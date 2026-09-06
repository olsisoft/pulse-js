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
  deriveWsUrl,
  type IQResource,
  PulseAPIError,
  PulseAuthError,
  PulseClient,
  PulseNotFoundError,
  PulseRateLimitError,
  PulseValidationError,
  validateWasmModule,
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
          accessToken: 'new.jwt.token',
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

describe('events.replay() — B-113 time-travel change replay', () => {
  it('unwraps the changes list and sends from/to/limit', async () => {
    let observedQS = '';
    server.use(
      http.get(
        `${BASE_URL}/api/pulse/iq/agents/user-sessions/state/replay/u42`,
        ({ request }) => {
          observedQS = new URL(request.url).search;
          return HttpResponse.json({
            agentId: 'user-sessions',
            key: 'u42',
            count: 2,
            changes: [
              { timestamp: 1000, changeType: 'PUT', value: { v: 1 } },
              { timestamp: 2000, changeType: 'PUT', value: { v: 2 } },
            ],
          });
        },
      ),
    );
    const changes = await newClient('fake.jwt').events.replay({
      affectingState: 'user-sessions',
      key: 'u42',
      from: '2026-05-24T10:00:00Z',
      to: '2026-05-24T11:00:00Z',
    });
    expect(Array.isArray(changes)).toBe(true);
    expect(changes).toHaveLength(2);
    expect(changes[0].changeType).toBe('PUT');
    const params = new URLSearchParams(observedQS);
    expect(params.get('from')).toBe('2026-05-24T10:00:00Z');
    expect(params.get('to')).toBe('2026-05-24T11:00:00Z');
    expect(params.get('limit')).toBe('100');
  });

  it('defaults from=-1h, to=now, limit=100 and returns [] when changes absent', async () => {
    let observedQS = '';
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/replay/k1`, ({ request }) => {
        observedQS = new URL(request.url).search;
        return HttpResponse.json({ agentId: 'a1', key: 'k1', count: 0 });
      }),
    );
    const changes = await newClient('fake.jwt').events.replay({
      affectingState: 'a1',
      key: 'k1',
    });
    expect(changes).toEqual([]);
    const params = new URLSearchParams(observedQS);
    expect(params.get('from')).toBe('-1h');
    expect(params.get('to')).toBe('now');
    expect(params.get('limit')).toBe('100');
  });

  it('respects a custom limit', async () => {
    let observedQS = '';
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/replay/k1`, ({ request }) => {
        observedQS = new URL(request.url).search;
        return HttpResponse.json({ agentId: 'a1', key: 'k1', count: 0, changes: [] });
      }),
    );
    await newClient('fake.jwt').events.replay({ affectingState: 'a1', key: 'k1', limit: 25 });
    expect(new URLSearchParams(observedQS).get('limit')).toBe('25');
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

  // ---- B-113 time-travel: as_of / diff ----
  it('get as_of sends param and returns past value', async () => {
    let observedQS = '';
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/sessions/state/value/u42`, ({ request }) => {
        observedQS = new URL(request.url).search;
        return HttpResponse.json({
          agentId: 'sessions',
          key: 'u42',
          value: { pages: 1 },
          asOf: 1716559920000,
        });
      }),
    );
    const result = await newClient('fake.jwt').iq.get('sessions', 'u42', { asOf: '-1h' });
    expect((result.value as { pages: number }).pages).toBe(1);
    expect(result.asOf).toBe(1716559920000);
    expect(new URLSearchParams(observedQS).get('as_of')).toBe('-1h');
  });

  it('get without as_of sends no query param', async () => {
    let observedQS = '?unset';
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/value/k1`, ({ request }) => {
        observedQS = new URL(request.url).search;
        return HttpResponse.json({ agentId: 'a1', key: 'k1', value: 1 });
      }),
    );
    await newClient('fake.jwt').iq.get('a1', 'k1');
    expect(observedQS).toBe('');
  });

  it('diff sends from/to and returns changes', async () => {
    let observedQS = '';
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/sessions/state/diff/u42`, ({ request }) => {
        observedQS = new URL(request.url).search;
        return HttpResponse.json({
          agentId: 'sessions',
          key: 'u42',
          changes: { cart_value: { delta: 70, from: 0, to: 70 } },
        });
      }),
    );
    const result = await newClient('fake.jwt').iq.diff('sessions', 'u42', {
      from: '-1h',
      to: 'now',
    });
    const changes = result.changes as { cart_value: { delta: number } };
    expect(changes.cart_value.delta).toBe(70);
    const params = new URLSearchParams(observedQS);
    expect(params.get('from')).toBe('-1h');
    expect(params.get('to')).toBe('now');
  });

  it('diff defaults from=-1h and to=now', async () => {
    let observedQS = '';
    server.use(
      http.get(`${BASE_URL}/api/pulse/iq/agents/a1/state/diff/k1`, ({ request }) => {
        observedQS = new URL(request.url).search;
        return HttpResponse.json({ agentId: 'a1', key: 'k1', changes: {} });
      }),
    );
    await newClient('fake.jwt').iq.diff('a1', 'k1');
    const params = new URLSearchParams(observedQS);
    expect(params.get('from')).toBe('-1h');
    expect(params.get('to')).toBe('now');
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

// ---------------------------------------------------------------------------
// B-112 — client.models (embedded ML model registry)
// ---------------------------------------------------------------------------

describe('client.models', () => {
  it('upload from bytes sends multipart/form-data', async () => {
    let contentType: string | null = null;
    let rawBody = '';
    server.use(
      http.post(`${BASE_URL}/api/pulse/ml-models`, async ({ request }) => {
        contentType = request.headers.get('content-type');
        rawBody = await request.text();
        return HttpResponse.json(
          { name: 'fraud', runtime: 'onnx', version: 1, sizeBytes: 5 },
          { status: 201 },
        );
      }),
    );

    const meta = await newClient('fake.jwt').models.upload({
      name: 'fraud',
      data: new Uint8Array([0x08, 0x09, 0x6f, 0x6e, 0x6e]),
      inputSchema: { amount: 'float' },
      outputSchema: { score: 'float' },
    });

    expect(meta.name).toBe('fraud');
    expect(contentType).toMatch(/multipart\/form-data/);
    // The form carries the text fields + the file part named "model".
    expect(rawBody).toContain('name="name"');
    expect(rawBody).toContain('fraud');
    expect(rawBody).toContain('name="runtime"');
    expect(rawBody).toContain('name="inputSchema"');
    expect(rawBody).toContain('name="outputSchema"');
    expect(rawBody).toContain('name="model"');
  });

  it('upload requires exactly one of data/path', async () => {
    const client = newClient('fake.jwt');
    await expect(client.models.upload({ name: 'm' })).rejects.toThrow(/exactly one/);
    await expect(
      client.models.upload({ name: 'm', data: new Uint8Array([1]), path: 'x' }),
    ).rejects.toThrow(/exactly one/);
  });

  it('upload rejects empty bytes', async () => {
    await expect(
      newClient('fake.jwt').models.upload({ name: 'm', data: new Uint8Array(0) }),
    ).rejects.toThrow(/empty/);
  });

  it('upload rejects blank name', async () => {
    await expect(
      newClient('fake.jwt').models.upload({ name: '  ', data: new Uint8Array([1]) }),
    ).rejects.toThrow(/name/);
  });

  it('list unwraps the {models:[...]} envelope', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/ml-models`, () =>
        HttpResponse.json({ models: [{ name: 'fraud' }] }),
      ),
    );
    const models = await newClient('fake.jwt').models.list();
    expect(models[0]!.name).toBe('fraud');
  });

  it('get returns metadata', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/ml-models/fraud`, () =>
        HttpResponse.json({ name: 'fraud', version: 2 }),
      ),
    );
    const meta = await newClient('fake.jwt').models.get('fraud');
    expect(meta.version).toBe(2);
  });

  it('delete issues DELETE', async () => {
    let called = false;
    server.use(
      http.delete(`${BASE_URL}/api/pulse/ml-models/fraud`, () => {
        called = true;
        return HttpResponse.json({ deleted: 'fraud' });
      }),
    );
    await newClient('fake.jwt').models.delete('fraud');
    expect(called).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// B-110 — client.wasm (sandboxed WASM module registry)
// ---------------------------------------------------------------------------

/** Parse a hex string (spaces ignored) into a Uint8Array. */
function hex(s: string): Uint8Array {
  const clean = s.replace(/\s+/g, '');
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = Number.parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

// A conforming module: magic + version, then an export section listing
// alloc / process / memory, no import section.
const VALID_WASM = hex(
  '00 61 73 6d 01 00 00 00 ' +
    '07 1c ' + // export section, size 0x1c (28 bytes payload)
    '03 ' + // 3 exports
    '05 61 6c 6c 6f 63 00 00 ' + // "alloc"  kind 0 idx 0
    '07 70 72 6f 63 65 73 73 00 00 ' + // "process" kind 0 idx 0
    '06 6d 65 6d 6f 72 79 02 00', // "memory" kind 2 idx 0
);

// Same exports but preceded by an import section declaring 1 host import.
const WASM_WITH_IMPORT = hex(
  '00 61 73 6d 01 00 00 00 ' +
    '02 09 01 03 65 6e 76 01 66 00 00 ' + // import section: 1 import (env.f)
    '07 1c 03 05 61 6c 6c 6f 63 00 00 ' +
    '07 70 72 6f 63 65 73 73 00 00 ' +
    '06 6d 65 6d 6f 72 79 02 00',
);

// Export section listing only "alloc" (missing process + memory).
const WASM_MISSING_EXPORT = hex(
  '00 61 73 6d 01 00 00 00 ' +
    '07 09 01 05 61 6c 6c 6f 63 00 00', // export section, 1 export "alloc"
);

describe('validateWasmModule (B-110 client-side pre-upload validation)', () => {
  it('accepts a conforming module', () => {
    expect(() => validateWasmModule(VALID_WASM)).not.toThrow();
  });

  it('rejects an empty / too-short module', () => {
    expect(() => validateWasmModule(new Uint8Array(0))).toThrow(PulseValidationError);
    expect(() => validateWasmModule(new Uint8Array(0))).toThrow(/too short/);
    expect(() => validateWasmModule(new Uint8Array([0x00, 0x61, 0x73]))).toThrow(/too short/);
  });

  it('rejects bad magic / version', () => {
    const bad = hex('de ad be ef 01 00 00 00');
    expect(() => validateWasmModule(bad)).toThrow(PulseValidationError);
    expect(() => validateWasmModule(bad)).toThrow(/bad magic\/version/);
  });

  it('rejects a module that imports host functions', () => {
    expect(() => validateWasmModule(WASM_WITH_IMPORT)).toThrow(PulseValidationError);
    expect(() => validateWasmModule(WASM_WITH_IMPORT)).toThrow(/imports host functions/);
  });

  it('rejects a module missing required exports', () => {
    expect(() => validateWasmModule(WASM_MISSING_EXPORT)).toThrow(PulseValidationError);
    expect(() => validateWasmModule(WASM_MISSING_EXPORT)).toThrow(
      /must export alloc, process and memory/,
    );
  });
});

describe('client.wasm', () => {
  it('upload from bytes sends multipart/form-data', async () => {
    let contentType: string | null = null;
    let rawBody = '';
    server.use(
      http.post(`${BASE_URL}/api/pulse/wasm-modules`, async ({ request }) => {
        contentType = request.headers.get('content-type');
        rawBody = await request.text();
        return HttpResponse.json(
          { name: 'pii-redactor', sha256: 'abc', version: 1, sizeBytes: 4 },
          { status: 201 },
        );
      }),
    );

    const meta = await newClient('fake.jwt').wasm.upload({
      name: 'pii-redactor',
      data: VALID_WASM,
      description: 'redacts PII',
    });

    expect(meta.name).toBe('pii-redactor');
    expect(contentType).toMatch(/multipart\/form-data/);
    // The form carries the text fields + the file part named "module".
    expect(rawBody).toContain('name="name"');
    expect(rawBody).toContain('pii-redactor');
    expect(rawBody).toContain('name="description"');
    expect(rawBody).toContain('redacts PII');
    expect(rawBody).toContain('name="module"');
  });

  it('upload omits description when not provided', async () => {
    let rawBody = '';
    server.use(
      http.post(`${BASE_URL}/api/pulse/wasm-modules`, async ({ request }) => {
        rawBody = await request.text();
        return HttpResponse.json({ name: 'm', version: 1 }, { status: 201 });
      }),
    );
    await newClient('fake.jwt').wasm.upload({ name: 'm', data: VALID_WASM });
    expect(rawBody).not.toContain('name="description"');
  });

  it('upload requires exactly one of data/path', async () => {
    const client = newClient('fake.jwt');
    await expect(client.wasm.upload({ name: 'm' })).rejects.toThrow(/exactly one/);
    await expect(
      client.wasm.upload({ name: 'm', data: new Uint8Array([1]), path: 'x' }),
    ).rejects.toThrow(/exactly one/);
  });

  it('upload rejects a non-conforming module WITHOUT a network call', async () => {
    // No MSW handler registered — onUnhandledRequest:'error' makes any fetch
    // throw. Reaching the PulseValidationError assertion proves the client
    // rejected client-side, before the wire.
    const client = newClient('fake.jwt');
    // bad magic
    await expect(
      client.wasm.upload({ name: 'm', data: hex('de ad be ef 01 00 00 00') }),
    ).rejects.toBeInstanceOf(PulseValidationError);
    // host import
    await expect(
      client.wasm.upload({ name: 'm', data: WASM_WITH_IMPORT }),
    ).rejects.toThrow(/imports host functions/);
    // missing required exports
    await expect(
      client.wasm.upload({ name: 'm', data: WASM_MISSING_EXPORT }),
    ).rejects.toThrow(/must export alloc, process and memory/);
  });

  it('upload rejects empty bytes', async () => {
    await expect(
      newClient('fake.jwt').wasm.upload({ name: 'm', data: new Uint8Array(0) }),
    ).rejects.toThrow(/empty/);
  });

  it('upload rejects blank name', async () => {
    await expect(
      newClient('fake.jwt').wasm.upload({ name: '  ', data: new Uint8Array([1]) }),
    ).rejects.toThrow(/name/);
  });

  it('list unwraps the {modules:[...]} envelope', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/wasm-modules`, () =>
        HttpResponse.json({ modules: [{ name: 'pii-redactor' }] }),
      ),
    );
    const modules = await newClient('fake.jwt').wasm.list();
    expect(modules[0]!.name).toBe('pii-redactor');
  });

  it('list returns [] when the envelope is missing', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/wasm-modules`, () => HttpResponse.json({})),
    );
    const modules = await newClient('fake.jwt').wasm.list();
    expect(modules).toEqual([]);
  });

  it('get returns metadata', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/wasm-modules/pii-redactor`, () =>
        HttpResponse.json({ name: 'pii-redactor', version: 2 }),
      ),
    );
    const meta = await newClient('fake.jwt').wasm.get('pii-redactor');
    expect(meta.version).toBe(2);
  });

  it('delete issues DELETE', async () => {
    let called = false;
    server.use(
      http.delete(`${BASE_URL}/api/pulse/wasm-modules/pii-redactor`, () => {
        called = true;
        return HttpResponse.json({ deleted: 'pii-redactor' });
      }),
    );
    await newClient('fake.jwt').wasm.delete('pii-redactor');
    expect(called).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// B-114 — duplex WebSocket channel
// ---------------------------------------------------------------------------

describe('deriveWsUrl', () => {
  it('http port + 1', () => {
    expect(deriveWsUrl('http://localhost:9090', 'fraud', undefined)).toBe(
      'ws://localhost:9091/api/pulse/agents/fraud/duplex',
    );
  });

  it('https becomes wss', () => {
    expect(deriveWsUrl('https://pulse.example.com:8443', 'agent-x', undefined)).toBe(
      'wss://pulse.example.com:8444/api/pulse/agents/agent-x/duplex',
    );
  });

  it('token rides in the query', () => {
    expect(deriveWsUrl('http://h:9090', 'a', 'jwt.tok en')).toContain(
      '/api/pulse/agents/a/duplex?token=jwt.tok%20en',
    );
  });

  it('agent id is URL-encoded', () => {
    expect(deriveWsUrl('http://h:9090', 'team/agent', undefined)).toContain(
      'agents/team%2Fagent/duplex',
    );
  });

  it('client.duplex rejects a blank agent id', async () => {
    await expect(newClient().duplex('  ')).rejects.toThrow(/agentId/);
  });
});

describe('client.connectors (B-093 follow-up — catalogue parity)', () => {
  it('list() returns sinks and sources', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/connectors`, () =>
        HttpResponse.json({
          sinks: [{ subType: 'segment', displayName: 'Segment', configFields: [] }],
          sources: [{ subType: 'posthog-source', displayName: 'PostHog Source (poll)', configFields: [] }],
        })
      )
    );
    const client = newClient('jwt');
    const catalog = await client.connectors.list();
    expect((catalog.sinks as Record<string, unknown>[])[0].subType).toBe('segment');
    expect((catalog.sources as Record<string, unknown>[])[0].subType).toBe('posthog-source');
  });

  it('sinks() / sources() helpers and empty-key degrade', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/connectors`, () =>
        HttpResponse.json({ sinks: [{ subType: 'amplitude' }] })
      )
    );
    const client = newClient('jwt');
    expect((await client.connectors.sinks())[0].subType).toBe('amplitude');
    expect(await client.connectors.sources()).toEqual([]); // missing key -> []
  });
});
