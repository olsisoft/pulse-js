/**
 * Smoke tests for PulseClient.
 *
 * Every test is offline — MSW (Mock Service Worker) intercepts fetch calls
 * and returns canned responses. The point is to pin the wire format the
 * client speaks against the Pulse OpenAPI spec, not to exercise a real
 * server.
 */

import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import {
  type IQResource,
  PulseAPIError,
  PulseAuthError,
  PulseClient,
  PulseNotFoundError,
  PulseRateLimitError,
  PulseValidationError,
} from '../src/index.js';

const BASE_URL = 'http://pulse.test:9090';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

function newClient(token?: string): PulseClient {
  return new PulseClient({ baseUrl: BASE_URL, token });
}

describe('PulseClient lifecycle', () => {
  it('exposes the token as a mutable property', () => {
    const client = newClient();
    expect(client.token).toBeUndefined();
    client.token = 'abc';
    expect(client.token).toBe('abc');
    client.token = undefined;
    expect(client.token).toBeUndefined();
  });

  it('strips trailing slashes from the base URL', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/version`, () =>
        HttpResponse.json({ version: '2.6.0' }),
      ),
    );
    const client = new PulseClient({ baseUrl: `${BASE_URL}//` });
    const result = await client.version();
    expect(result).toEqual({ version: '2.6.0' });
  });
});

describe('version()', () => {
  it('returns metadata without requiring a JWT', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/version`, () =>
        HttpResponse.json({ version: '2.6.0', edition: 'desktop' }),
      ),
    );
    const client = newClient();
    expect(client.token).toBeUndefined();
    const result = await client.version();
    expect(result).toEqual({ version: '2.6.0', edition: 'desktop' });
  });
});

describe('auth', () => {
  it('login caches the returned token on the client', async () => {
    server.use(
      http.post(`${BASE_URL}/api/auth/login`, async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>;
        expect(body).toEqual({ username: 'alice', password: 'secret' });
        return HttpResponse.json({
          token: 'new.jwt.token',
          refreshToken: 'refresh.token',
          activeOrg: { id: 'org1', name: 'Acme' },
        });
      }),
    );
    const client = newClient();
    const result = await client.auth.login('alice', 'secret');
    expect(client.token).toBe('new.jwt.token');
    expect(result.refreshToken).toBe('refresh.token');
  });

  it('login failure raises PulseAuthError and does not cache a token', async () => {
    server.use(
      http.post(`${BASE_URL}/api/auth/login`, () =>
        HttpResponse.json({ error: 'Invalid credentials' }, { status: 401 }),
      ),
    );
    const client = newClient();
    await expect(client.auth.login('alice', 'wrong')).rejects.toBeInstanceOf(PulseAuthError);
    expect(client.token).toBeUndefined();
  });

  it('refresh caches the new token', async () => {
    server.use(
      http.post(`${BASE_URL}/api/auth/refresh`, () =>
        HttpResponse.json({ token: 'refreshed.jwt' }),
      ),
    );
    const client = newClient();
    await client.auth.refresh('some-refresh-token');
    expect(client.token).toBe('refreshed.jwt');
  });

  it('organizations unwraps the envelope', async () => {
    server.use(
      http.get(`${BASE_URL}/api/auth/organizations`, () =>
        HttpResponse.json({ organizations: [{ id: 'o1', name: 'Acme' }] }),
      ),
    );
    const client = newClient('fake.jwt');
    const orgs = await client.auth.organizations();
    expect(orgs).toEqual([{ id: 'o1', name: 'Acme' }]);
  });

  it('switchOrg caches the new token', async () => {
    server.use(
      http.post(`${BASE_URL}/api/auth/switch-org`, () =>
        HttpResponse.json({ token: 'switched.jwt' }),
      ),
    );
    const client = newClient('fake.jwt');
    await client.auth.switchOrg('org2');
    expect(client.token).toBe('switched.jwt');
  });
});

describe('pipelines', () => {
  it('list unwraps the envelope', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines`, () =>
        HttpResponse.json({
          pipelines: [
            { id: 'p1', name: 'demo', nodes: [] },
            { id: 'p2', name: 'fraud', nodes: [] },
          ],
        }),
      ),
    );
    const client = newClient('fake.jwt');
    const pipelines = await client.pipelines.list();
    expect(pipelines).toHaveLength(2);
    expect(pipelines[0]).toMatchObject({ id: 'p1' });
  });

  it('list returns empty when the envelope is missing', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines`, () => HttpResponse.json({})),
    );
    const client = newClient('fake.jwt');
    expect(await client.pipelines.list()).toEqual([]);
  });

  it('get returns one pipeline', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines/p1`, () =>
        HttpResponse.json({ id: 'p1', name: 'demo', nodes: [] }),
      ),
    );
    const client = newClient('fake.jwt');
    const result = await client.pipelines.get('p1');
    expect(result.id).toBe('p1');
  });

  it('get on missing pipeline raises PulseNotFoundError', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines/nope`, () =>
        HttpResponse.json({ error: 'not found' }, { status: 404 }),
      ),
    );
    const client = newClient('fake.jwt');
    await expect(client.pipelines.get('nope')).rejects.toBeInstanceOf(PulseNotFoundError);
  });

  it('create returns the created pipeline', async () => {
    server.use(
      http.post(`${BASE_URL}/api/pulse/pipelines`, async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>;
        expect(body.name).toBe('new');
        return HttpResponse.json({ id: 'p3', name: 'new', nodes: [] }, { status: 201 });
      }),
    );
    const client = newClient('fake.jwt');
    const result = await client.pipelines.create({
      name: 'new',
      nodes: [{ id: 'n1', type: 'source' }],
    });
    expect(result.id).toBe('p3');
  });

  it('create with malformed body raises PulseValidationError', async () => {
    server.use(
      http.post(`${BASE_URL}/api/pulse/pipelines`, () =>
        HttpResponse.json({ error: 'Missing required field: nodes' }, { status: 400 }),
      ),
    );
    const client = newClient('fake.jwt');
    await expect(client.pipelines.create({ name: 'bad' })).rejects.toBeInstanceOf(
      PulseValidationError,
    );
  });

  it('delete returns undefined on 204', async () => {
    server.use(
      http.delete(`${BASE_URL}/api/pulse/pipelines/p1`, () =>
        new HttpResponse(null, { status: 204 }),
      ),
    );
    const client = newClient('fake.jwt');
    await expect(client.pipelines.delete('p1')).resolves.toBeUndefined();
  });

  it('url-encodes path-param ids that contain special chars', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines/foo%2Fbar`, () =>
        HttpResponse.json({ id: 'foo/bar' }),
      ),
    );
    const client = newClient('fake.jwt');
    const result = await client.pipelines.get('foo/bar');
    expect(result.id).toBe('foo/bar');
  });
});

describe('agents', () => {
  it('list unwraps the envelope', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/agents`, () =>
        HttpResponse.json({
          agents: [{ id: 'a1', name: 'fraud-detector', engineType: 'streaming' }],
        }),
      ),
    );
    const client = newClient('fake.jwt');
    const agents = await client.agents.list();
    expect(agents[0]).toMatchObject({ engineType: 'streaming' });
  });

  it('get returns one agent', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/agents/a1`, () =>
        HttpResponse.json({ id: 'a1', name: 'fraud-detector', engineType: 'streaming' }),
      ),
    );
    const client = newClient('fake.jwt');
    const result = await client.agents.get('a1');
    expect(result.id).toBe('a1');
  });

  it('update PUTs the full config and returns the fresh snapshot', async () => {
    let receivedBody: Record<string, unknown> | undefined;
    server.use(
      http.put(`${BASE_URL}/api/pulse/agents/a1`, async ({ request }) => {
        receivedBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({
          id: 'a1',
          name: 'fraud-detector-v2',
          engineType: 'rule-based',
          status: 'running',
        });
      }),
    );
    const client = newClient('fake.jwt');
    const newConfig = {
      name: 'fraud-detector-v2',
      engineType: 'rule-based',
      config: { rules: [{ if: 'amount > 5000', then: 'block' }] },
    };
    const result = await client.agents.update('a1', newConfig);
    expect(result.name).toBe('fraud-detector-v2');
    expect(receivedBody).toEqual(newConfig);
  });

  it('update raises validation error on self-loop 400', async () => {
    server.use(
      http.put(`${BASE_URL}/api/pulse/agents/a1`, () =>
        HttpResponse.json(
          {
            error: 'Agent would self-loop: outputTopic == inputTopic',
            unsafeFields: ['outputTopic'],
          },
          { status: 400 },
        ),
      ),
    );
    const client = newClient('fake.jwt');
    await expect(
      client.agents.update('a1', { name: 'x', inputTopic: 't', outputTopic: 't' }),
    ).rejects.toThrow(PulseValidationError);
  });

  it('update raises not-found on missing agent', async () => {
    server.use(
      http.put(`${BASE_URL}/api/pulse/agents/missing`, () =>
        HttpResponse.json({ error: 'Agent not found' }, { status: 404 }),
      ),
    );
    const client = newClient('fake.jwt');
    await expect(client.agents.update('missing', { name: 'x' })).rejects.toThrow(
      PulseNotFoundError,
    );
  });

  it('update URL-encodes the agent id', async () => {
    server.use(
      http.put(`${BASE_URL}/api/pulse/agents/tenant%2Fagent`, () =>
        HttpResponse.json({ id: 'tenant/agent', name: 'x' }),
      ),
    );
    const client = newClient('fake.jwt');
    const result = await client.agents.update('tenant/agent', { name: 'x' });
    expect(result.id).toBe('tenant/agent');
  });

  it('delete returns void on 204', async () => {
    let hit = false;
    server.use(
      http.delete(`${BASE_URL}/api/pulse/agents/a1`, () => {
        hit = true;
        return new HttpResponse(null, { status: 204 });
      }),
    );
    const client = newClient('fake.jwt');
    await expect(client.agents.delete('a1')).resolves.toBeUndefined();
    expect(hit).toBe(true);
  });

  it('delete raises not-found', async () => {
    server.use(
      http.delete(`${BASE_URL}/api/pulse/agents/missing`, () =>
        HttpResponse.json({ error: 'Agent not found' }, { status: 404 }),
      ),
    );
    const client = newClient('fake.jwt');
    await expect(client.agents.delete('missing')).rejects.toThrow(PulseNotFoundError);
  });

  it('update without token raises auth error synchronously', async () => {
    // No msw handler — if the client reached the wire, msw's strict mode
    // would throw. Reaching the assertion proves no fetch was attempted.
    const client = newClient();
    await expect(client.agents.update('a1', { name: 'x' })).rejects.toThrow(PulseAuthError);
  });

  it('delete without token raises auth error synchronously', async () => {
    const client = newClient();
    await expect(client.agents.delete('a1')).rejects.toThrow(PulseAuthError);
  });
});

describe('templates', () => {
  it('list unwraps the envelope', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/templates`, () =>
        HttpResponse.json({
          templates: [{ id: 'fraud-detection', name: 'Fraud Detection' }],
        }),
      ),
    );
    const client = newClient('fake.jwt');
    const templates = await client.templates.list();
    expect(templates[0]?.id).toBe('fraud-detection');
  });
});

describe('error handling', () => {
  it('rejects without calling the server when no token is set', async () => {
    // No MSW handler — if the client hits the server, MSW would fail
    // (onUnhandledRequest: 'error').
    const client = newClient();
    await expect(client.pipelines.list()).rejects.toThrow(/no token set/i);
  });

  it('parses retry-after-seconds from the 429 body', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines`, () =>
        HttpResponse.json(
          {
            error: 'Rate limit exceeded',
            errorCode: 'RATE_LIMITED',
            retryAfterSeconds: 60,
            limit: 120,
            remaining: 0,
          },
          { status: 429 },
        ),
      ),
    );
    const client = newClient('fake.jwt');
    try {
      await client.pipelines.list();
      throw new Error('should have rejected');
    } catch (err) {
      expect(err).toBeInstanceOf(PulseRateLimitError);
      expect((err as PulseRateLimitError).retryAfterSeconds).toBe(60);
    }
  });

  it('falls back to the Retry-After header when the body lacks retryAfterSeconds', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines`, () =>
        new HttpResponse('Too Many Requests', {
          status: 429,
          headers: { 'Retry-After': '30' },
        }),
      ),
    );
    const client = newClient('fake.jwt');
    try {
      await client.pipelines.list();
      throw new Error('should have rejected');
    } catch (err) {
      expect(err).toBeInstanceOf(PulseRateLimitError);
      expect((err as PulseRateLimitError).retryAfterSeconds).toBe(30);
    }
  });

  it('unknown 5xx raises the generic PulseAPIError', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines`, () =>
        HttpResponse.json(
          { error: 'Internal', errorClass: 'NPE' },
          { status: 500 },
        ),
      ),
    );
    const client = newClient('fake.jwt');
    try {
      await client.pipelines.list();
      throw new Error('should have rejected');
    } catch (err) {
      expect(err).toBeInstanceOf(PulseAPIError);
      // It's a generic one, NOT one of the specialised subclasses
      expect(err).not.toBeInstanceOf(PulseAuthError);
      expect(err).not.toBeInstanceOf(PulseNotFoundError);
      expect(err).not.toBeInstanceOf(PulseValidationError);
      expect(err).not.toBeInstanceOf(PulseRateLimitError);
      expect((err as PulseAPIError).statusCode).toBe(500);
    }
  });

  it('attaches the bearer token to outbound requests', async () => {
    let observedAuth: string | null = null;
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines`, ({ request }) => {
        observedAuth = request.headers.get('Authorization');
        return HttpResponse.json({ pipelines: [] });
      }),
    );
    const client = newClient('fake.jwt.token');
    await client.pipelines.list();
    expect(observedAuth).toBe('Bearer fake.jwt.token');
  });

  it('sets the User-Agent header so server-side logs identify the client', async () => {
    let observedUserAgent: string | null = null;
    server.use(
      http.get(`${BASE_URL}/api/pulse/pipelines`, ({ request }) => {
        observedUserAgent = request.headers.get('User-Agent');
        return HttpResponse.json({ pipelines: [] });
      }),
    );
    const client = newClient('fake.jwt');
    await client.pipelines.list();
    expect(observedUserAgent).toMatch(/pulse-client-js/);
  });
});

describe('events.stream() — B-098 Phase 7 SSE', () => {
  /**
   * Helper: build a fake fetch that returns the given SSE body as a
   * streaming Response. MSW doesn't model `ReadableStream`-streamed bodies
   * cleanly, so we bypass MSW and inject a custom fetch via the client
   * `options.fetch` hook.
   */
  function fakeStreamingFetch(sseBody: string, status = 200): typeof fetch {
    return (async (input: RequestInfo | URL, init?: RequestInit) => {
      void input;
      void init;
      const encoder = new TextEncoder();
      const stream = new ReadableStream({
        start(controller) {
          // Emit the body in two chunks to exercise the multi-chunk parser
          const mid = Math.floor(sseBody.length / 2);
          controller.enqueue(encoder.encode(sseBody.slice(0, mid)));
          controller.enqueue(encoder.encode(sseBody.slice(mid)));
          controller.close();
        },
      });
      return new Response(stream, {
        status,
        headers: { 'Content-Type': 'text/event-stream' },
      });
    }) as typeof fetch;
  }

  it('yields parsed JSON events', async () => {
    const sseBody =
      'data: {"type":"fraud_signal","payload":{"customerId":"c1"}}\n\n' +
      'data: {"type":"heartbeat"}\n\n';
    const client = new PulseClient({
      baseUrl: BASE_URL,
      token: 'fake.jwt',
      fetch: fakeStreamingFetch(sseBody),
    });
    const events: Record<string, unknown>[] = [];
    for await (const event of client.events.stream()) {
      events.push(event);
    }
    expect(events).toHaveLength(2);
    expect(events[0]).toMatchObject({ type: 'fraud_signal' });
    expect(events[1]).toMatchObject({ type: 'heartbeat' });
  });

  it('skips SSE comments and dispatches on blank lines', async () => {
    const sseBody =
      ': keep-alive\n\n' +
      'data: {"type":"a"}\n\n' +
      ': another keep-alive\n\n' +
      'data: {"type":"b"}\n\n';
    const client = new PulseClient({
      baseUrl: BASE_URL,
      token: 'fake.jwt',
      fetch: fakeStreamingFetch(sseBody),
    });
    const events: Record<string, unknown>[] = [];
    for await (const event of client.events.stream()) {
      events.push(event);
    }
    expect(events.map((e) => e.type)).toEqual(['a', 'b']);
  });

  it('falls back to {data:...} envelope for non-JSON payloads', async () => {
    const sseBody = 'data: not-json-here\n\n';
    const client = new PulseClient({
      baseUrl: BASE_URL,
      token: 'fake.jwt',
      fetch: fakeStreamingFetch(sseBody),
    });
    const events: Record<string, unknown>[] = [];
    for await (const event of client.events.stream()) {
      events.push(event);
    }
    expect(events).toEqual([{ data: 'not-json-here' }]);
  });

  it('throws PulseAuthError when no token is set', async () => {
    const client = new PulseClient({ baseUrl: BASE_URL });
    await expect(async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const _ of client.events.stream()) {
        // unreachable — the auth check fires before any network call
      }
    }).rejects.toBeInstanceOf(PulseAuthError);
  });

  it('throws PulseAuthError on a 401 response', async () => {
    const client = new PulseClient({
      baseUrl: BASE_URL,
      token: 'expired.jwt',
      fetch: fakeStreamingFetch('{"error":"expired"}', 401),
    });
    await expect(async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const _ of client.events.stream()) {
        // unreachable
      }
    }).rejects.toBeInstanceOf(PulseAuthError);
  });
});

describe('iq — B-106 Interactive Queries', () => {
  // ---- summary ----
  it('summary returns state metadata', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/fraud-detector/state`, () =>
        HttpResponse.json({
          agentId: 'fraud-detector',
          queryable: true,
          backend: 'rocksdb',
          hotSize: 1500,
          hotBytes: 32768,
          coldSize: 50000,
          coldBytes: 4194304,
          lastCheckpointId: 42,
          totalSize: 51500,
        }),
      ),
    );
    const summary = await newClient('fake.jwt').iq.summary('fraud-detector');
    expect(summary.queryable).toBe(true);
    expect(summary.backend).toBe('rocksdb');
    expect(summary.totalSize).toBe(51500);
  });

  it('summary handles non-queryable agent shape', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/rule-agent/state`, () =>
        HttpResponse.json({
          agentId: 'rule-agent',
          queryable: false,
          backend: 'none',
          hotSize: 0, hotBytes: 0, coldSize: 0, coldBytes: 0,
          lastCheckpointId: -1, totalSize: 0,
        }),
      ),
    );
    const result = await newClient('fake.jwt').iq.summary('rule-agent');
    expect(result.queryable).toBe(false);
    expect(result.lastCheckpointId).toBe(-1);
  });

  it('summary URL-encodes agent id with slash', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/tenant%2Fagent/state`, () =>
        HttpResponse.json({
          agentId: 'tenant/agent', queryable: true, backend: 'rocksdb',
          hotSize: 0, hotBytes: 0, coldSize: 0, coldBytes: 0,
          lastCheckpointId: 0, totalSize: 0,
        }),
      ),
    );
    const result = await newClient('fake.jwt').iq.summary('tenant/agent');
    expect(result.agentId).toBe('tenant/agent');
  });

  // ---- get ----
  it('get returns value at key', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/fraud-detector/state/value/customer-42`, () =>
        HttpResponse.json({
          agentId: 'fraud-detector',
          key: 'customer-42',
          value: { tx_count_60s: 7, total_amount_60s: 12500 },
        }),
      ),
    );
    const result = await newClient('fake.jwt').iq.get('fraud-detector', 'customer-42');
    expect(result.key).toBe('customer-42');
    expect((result.value as { tx_count_60s: number }).tx_count_60s).toBe(7);
  });

  it('get URL-encodes keys containing slash', async () => {
    server.use(
      http.get(
        `${BASE_URL}/api/pulse/iq/agents/sessions/state/value/user%3A123%2Forders`,
        () =>
          HttpResponse.json({
            agentId: 'sessions',
            key: 'user:123/orders',
            value: ['o1', 'o2', 'o3'],
          }),
      ),
    );
    const result = await newClient('fake.jwt').iq.get('sessions', 'user:123/orders');
    expect(result.value).toEqual(['o1', 'o2', 'o3']);
  });

  it('get returns null value when key is present-with-null', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/value/k1`, () =>
        HttpResponse.json({ agentId: 'a1', key: 'k1', value: null }),
      ),
    );
    const result = await newClient('fake.jwt').iq.get('a1', 'k1');
    expect(result.value).toBeNull();
  });

  it('get 404 key-not-found raises PulseNotFoundError', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/value/missing-key`, () =>
        HttpResponse.json(
          { error: 'Key not found', agentId: 'a1', key: 'missing-key' },
          { status: 404 },
        ),
      ),
    );
    try {
      await newClient('fake.jwt').iq.get('a1', 'missing-key');
      throw new Error('should have rejected');
    } catch (err) {
      expect(err).toBeInstanceOf(PulseNotFoundError);
      const body = (err as PulseNotFoundError).body as Record<string, unknown>;
      expect(body.error).toBe('Key not found');
      expect(body.key).toBe('missing-key');
    }
  });

  it('get 404 agent-not-queryable raises PulseNotFoundError with reason', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/value/k1`, () =>
        HttpResponse.json(
          {
            error: 'Agent has no queryable state',
            agentId: 'a1',
            reason: 'non-streaming or stopped',
          },
          { status: 404 },
        ),
      ),
    );
    try {
      await newClient('fake.jwt').iq.get('a1', 'k1');
      throw new Error('should have rejected');
    } catch (err) {
      expect(err).toBeInstanceOf(PulseNotFoundError);
      const body = (err as PulseNotFoundError).body as Record<string, unknown>;
      expect(body.reason).toBe('non-streaming or stopped');
    }
  });

  // ---- scan ----
  it('scan returns entries with default limit=100', async () => {
    let observedQS = '';
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/scan`, ({ request }) => {
        observedQS = new URL(request.url).search;
        return HttpResponse.json({
          agentId: 'a1',
          entries: [
            { key: 'k1', value: 1 },
            { key: 'k2', value: 2 },
          ],
          count: 2, truncated: false, limitApplied: 100,
        });
      }),
    );
    const result = await newClient('fake.jwt').iq.scan('a1');
    expect((result.entries as unknown[]).length).toBe(2);
    expect(observedQS).toBe('?limit=100');
  });

  it('scan passes through start/end/limit', async () => {
    let observedQS = '';
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/scan`, ({ request }) => {
        observedQS = new URL(request.url).search;
        return HttpResponse.json({
          agentId: 'a1', entries: [], count: 0,
          truncated: false, limitApplied: 50, start: 'alice', end: 'bob',
        });
      }),
    );
    await newClient('fake.jwt').iq.scan('a1', { start: 'alice', end: 'bob', limit: 50 });
    const params = new URLSearchParams(observedQS);
    expect(params.get('limit')).toBe('50');
    expect(params.get('start')).toBe('alice');
    expect(params.get('end')).toBe('bob');
  });

  it('scan 404 agent-not-queryable raises', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/scan`, () =>
        HttpResponse.json(
          { error: 'Agent has no queryable state', agentId: 'a1', reason: 'non-streaming or stopped' },
          { status: 404 },
        ),
      ),
    );
    await expect(newClient('fake.jwt').iq.scan('a1')).rejects.toBeInstanceOf(PulseNotFoundError);
  });

  // ---- listKeys ----
  it('listKeys returns keys array', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/keys`, () =>
        HttpResponse.json({
          agentId: 'a1',
          keys: ['alpha', 'beta', 'gamma'],
          count: 3, truncated: false, limitApplied: 100,
        }),
      ),
    );
    const result = await newClient('fake.jwt').iq.listKeys('a1');
    expect(result.keys).toEqual(['alpha', 'beta', 'gamma']);
  });

  // ---- query ----
  it('query flat with filter sends correct body shape', async () => {
    let sentBody: Record<string, unknown> | undefined;
    server.use(
      http.post(
        `${BASE_URL}/api/pulse/iq/agents/fraud-detector/state/query`,
        async ({ request }) => {
          sentBody = (await request.json()) as Record<string, unknown>;
          return HttpResponse.json({
            agentId: 'fraud-detector',
            entries: [
              { key: 'c1', value: { tx_count_60s: 8 } },
              { key: 'c5', value: { tx_count_60s: 12 } },
            ],
            count: 2, totalScanned: 1500, matchedCount: 2,
            truncated: false, limitApplied: 100,
          });
        },
      ),
    );
    const result = await newClient('fake.jwt').iq.query('fraud-detector', {
      filter: { field: 'tx_count_60s', op: 'gt', value: 5 },
    });
    expect(result.count).toBe(2);
    const filter = sentBody?.filter as Record<string, unknown>;
    expect(filter.field).toBe('tx_count_60s');
    expect(filter.op).toBe('gt');
  });

  it('query grouped returns groups shape', async () => {
    let sentBody: Record<string, unknown> | undefined;
    server.use(
      http.post(
        `${BASE_URL}/api/pulse/iq/agents/users/state/query`,
        async ({ request }) => {
          sentBody = (await request.json()) as Record<string, unknown>;
          return HttpResponse.json({
            agentId: 'users',
            groups: [
              { groupKey: 'free', count: 8420 },
              { groupKey: 'pro', count: 312 },
              { groupKey: 'enterprise', count: 47 },
            ],
            groupCount: 3, totalScanned: 8779, matchedCount: 8779,
            truncated: false, limitApplied: 100,
          });
        },
      ),
    );
    const result = await newClient('fake.jwt').iq.query('users', { groupBy: 'plan' });
    expect(result.groupCount).toBe(3);
    expect(sentBody?.groupBy).toBe('plan');
  });

  it('query empty options sends undefined body', async () => {
    let observedContentLength: string | null = null;
    server.use(
      http.post(`${BASE_URL}/api/pulse/iq/agents/a1/state/query`, ({ request }) => {
        observedContentLength = request.headers.get('content-length');
        return HttpResponse.json({
          agentId: 'a1', entries: [], count: 0,
          totalScanned: 0, matchedCount: 0,
          truncated: false, limitApplied: 100,
        });
      }),
    );
    await newClient('fake.jwt').iq.query('a1');
    // Empty options → no body sent (content-length absent or '0')
    expect(observedContentLength === null || observedContentLength === '0').toBe(true);
  });

  it('query 400 invalid filter raises PulseValidationError', async () => {
    server.use(
      http.post(`${BASE_URL}/api/pulse/iq/agents/a1/state/query`, () =>
        HttpResponse.json(
          { error: 'filter cannot mix discriminators (field/and/or/not) at the same level' },
          { status: 400 },
        ),
      ),
    );
    // Caller passes an intentionally-malformed filter (mixed discriminators)
    // to verify that the server's 400 propagates as PulseValidationError.
    // The TS type allows the shape (it's a structural union), the SERVER
    // rejects it. We don't try to enforce discriminator-exclusivity client
    // side — that would duplicate server validation and lock us to one
    // version of the rules.
    const badFilter = {
      field: 'a',
      and: [{ field: 'b', op: 'eq', value: 1 }],
    } as Parameters<IQResource['query']>[1];
    await expect(
      newClient('fake.jwt').iq.query('a1', { filter: badFilter as never }),
    ).rejects.toBeInstanceOf(PulseValidationError);
  });

  // ---- auth gating ----
  it('summary without token raises PulseAuthError before any HTTP call', async () => {
    await expect(newClient().iq.summary('a1')).rejects.toBeInstanceOf(PulseAuthError);
  });
});
