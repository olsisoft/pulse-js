/**
 * B-170 / GA-gate #312 — opt-in retry policy (mirrors the Python reference).
 *
 * Each test drives a real request through PulseClient against a sequenced fake
 * `fetch` and asserts the attempt count.
 */
import { describe, expect, it } from 'vitest';

import { PulseClient, type PulseClientOptions } from '../src/client.js';
import { PulseAPIError, PulseNotFoundError } from '../src/errors.js';

type Step = { status: number; body?: unknown } | 'throw';

function seqFetch(steps: Step[]): { fetch: typeof fetch; calls: () => number } {
  let i = 0;
  let n = 0;
  const impl = async (_url: string | URL, _init?: RequestInit): Promise<Response> => {
    n += 1;
    const step = steps[Math.min(i, steps.length - 1)];
    i += 1;
    if (step === 'throw') throw new TypeError('network down');
    const body = step.body === undefined ? '' : JSON.stringify(step.body);
    return new Response(body, { status: step.status, headers: { 'content-type': 'application/json' } });
  };
  return { fetch: impl as unknown as typeof fetch, calls: () => n };
}

function makeClient(steps: Step[], opts: Partial<PulseClientOptions> = {}) {
  const f = seqFetch(steps);
  const c = new PulseClient({
    baseUrl: 'http://pulse.test:9090',
    token: 't',
    fetch: f.fetch,
    retryBackoffMs: 1,
    retryMaxBackoffMs: 2,
    ...opts,
  });
  return { c, f };
}

describe('opt-in retry (#312)', () => {
  it('is off by default — one attempt', async () => {
    const { c, f } = makeClient([{ status: 503 }, { status: 200, body: { ok: true } }]);
    await expect(c.version()).rejects.toBeInstanceOf(PulseAPIError);
    expect(f.calls()).toBe(1);
  });

  it('retries an idempotent GET on 5xx then succeeds', async () => {
    const { c, f } = makeClient(
      [{ status: 503 }, { status: 502 }, { status: 200, body: { v: 1 } }],
      { maxRetries: 2 },
    );
    await expect(c.version()).resolves.toEqual({ v: 1 });
    expect(f.calls()).toBe(3);
  });

  it('exhausts retries then throws', async () => {
    const { c, f } = makeClient([{ status: 503 }], { maxRetries: 2 });
    await expect(c.version()).rejects.toMatchObject({ statusCode: 503 });
    expect(f.calls()).toBe(3); // initial + 2 retries
  });

  it('retries 429 for a POST, honouring Retry-After', async () => {
    const { c, f } = makeClient(
      [{ status: 429, body: { retryAfterSeconds: 0 } }, { status: 201, body: { id: 'p1' } }],
      { maxRetries: 1 },
    );
    await expect(c.pipelines.create({ name: 'x' })).resolves.toEqual({ id: 'p1' });
    expect(f.calls()).toBe(2);
  });

  it('does NOT retry a POST on 5xx by default', async () => {
    const { c, f } = makeClient(
      [{ status: 503 }, { status: 201, body: { id: 'p1' } }],
      { maxRetries: 3 },
    );
    await expect(c.pipelines.create({ name: 'x' })).rejects.toMatchObject({ statusCode: 503 });
    expect(f.calls()).toBe(1);
  });

  it('retries a POST on 5xx when opted in', async () => {
    const { c, f } = makeClient(
      [{ status: 503 }, { status: 201, body: { id: 'p1' } }],
      { maxRetries: 2, retryNonIdempotent: true },
    );
    await expect(c.pipelines.create({ name: 'x' })).resolves.toEqual({ id: 'p1' });
    expect(f.calls()).toBe(2);
  });

  it('retries a transport error for an idempotent GET', async () => {
    const { c, f } = makeClient(['throw', { status: 200, body: { pipelines: [] } }], {
      maxRetries: 1,
    });
    await expect(c.pipelines.list()).resolves.toEqual([]);
    expect(f.calls()).toBe(2);
  });

  it('never retries a terminal 404', async () => {
    const { c, f } = makeClient([{ status: 404, body: { error: 'nope' } }], { maxRetries: 3 });
    await expect(c.pipelines.get('nope')).rejects.toBeInstanceOf(PulseNotFoundError);
    expect(f.calls()).toBe(1);
  });
});
