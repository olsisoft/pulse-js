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
        HttpResponse.json({ version: '2.5.8' }),
      ),
    );
    const client = new PulseClient({ baseUrl: `${BASE_URL}//` });
    const result = await client.version();
    expect(result).toEqual({ version: '2.5.8' });
  });
});

describe('version()', () => {
  it('returns metadata without requiring a JWT', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/version`, () =>
        HttpResponse.json({ version: '2.5.8', edition: 'desktop' }),
      ),
    );
    const client = newClient();
    expect(client.token).toBeUndefined();
    const result = await client.version();
    expect(result).toEqual({ version: '2.5.8', edition: 'desktop' });
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
