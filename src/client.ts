/**
 * The main PulseClient and its resource accessors.
 *
 * Design mirrors `pulse-py` for cross-language consistency: each API surface
 * (auth, pipelines, agents, etc.) is its own small class accessed via a
 * property on the client. Shared HTTP transport via composition.
 *
 * Wire format: the Pulse REST API described by
 * `streamflow-pulse/src/main/resources/openapi/openapi.yaml`. When a new
 * endpoint lands in the spec, add a method here and a matching test.
 */

import {
  PulseAPIError,
  PulseAuthError,
  PulseClientError,
  type PulseErrorBody,
  PulseNotFoundError,
  PulseRateLimitError,
  PulseValidationError,
} from './errors.js';

const USER_AGENT = 'pulse-client-js/2.5.8';
const DEFAULT_TIMEOUT_MS = 30_000;

export interface PulseClientOptions {
  /** The Pulse server URL (e.g. http://localhost:9090). */
  baseUrl: string;
  /** Optional JWT to attach as `Authorization: Bearer <token>`. */
  token?: string;
  /** Per-request timeout in milliseconds. Default 30 000. */
  timeoutMs?: number;
  /**
   * Optional fetch implementation override. Used by tests + when running on
   * a runtime that doesn't expose a global `fetch` (older Node, edge runtimes).
   */
  fetch?: typeof fetch;
}

interface RequestOptions {
  method: string;
  path: string;
  body?: unknown;
  authenticated?: boolean;
}

/**
 * Synchronous-style (Promise-based) HTTP client for the Pulse REST API.
 *
 * @example
 * ```ts
 * import { PulseClient } from '@olsisoft/pulse-client';
 *
 * const client = new PulseClient({ baseUrl: 'http://localhost:9090' });
 * await client.auth.login('alice', 'secret');
 *
 * for (const pipeline of await client.pipelines.list()) {
 *   console.log(pipeline.name);
 * }
 * ```
 */
export class PulseClient {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;

  public token: string | undefined;

  public readonly auth: AuthResource;
  public readonly pipelines: PipelinesResource;
  public readonly agents: AgentsResource;
  public readonly templates: TemplatesResource;
  public readonly users: UsersResource;
  public readonly events: EventsResource;

  constructor(options: PulseClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, '');
    this.token = options.token;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    if (typeof this.fetchImpl !== 'function') {
      throw new PulseClientError(
        'No fetch implementation available. Use Node 20+ or pass options.fetch.',
      );
    }
    this.auth = new AuthResource(this);
    this.pipelines = new PipelinesResource(this);
    this.agents = new AgentsResource(this);
    this.templates = new TemplatesResource(this);
    this.users = new UsersResource(this);
    this.events = new EventsResource(this);
  }

  /** @internal — exposed so EventsResource can build its SSE URL. */
  public get baseUrlInternal(): string {
    return this.baseUrl;
  }

  /** @internal — exposed so EventsResource can use the configured fetch. */
  public get fetchInternal(): typeof fetch {
    return this.fetchImpl;
  }

  /** Returns the Pulse server's build + version metadata. Public — no JWT required. */
  public async version(): Promise<Record<string, unknown>> {
    return (await this.request({
      method: 'GET',
      path: '/api/pulse/version',
      authenticated: false,
    })) as Record<string, unknown>;
  }

  /** @internal — used by resource classes; intentionally not part of the public surface. */
  public async request(opts: RequestOptions): Promise<unknown> {
    const { method, path, body, authenticated = true } = opts;
    const headers: Record<string, string> = {
      'User-Agent': USER_AGENT,
    };
    if (body !== undefined) {
      headers['Content-Type'] = 'application/json';
    }
    if (authenticated) {
      if (!this.token) {
        throw new PulseAuthError(401, path, {
          error: 'No token set. Call client.auth.login() first or pass options.token.',
        });
      }
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);

    let response: Response;
    try {
      response = await this.fetchImpl(`${this.baseUrl}${path}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }

    if (response.status === 204) return undefined;

    if (response.status >= 200 && response.status < 300) {
      const text = await response.text();
      if (!text) return undefined;
      try {
        return JSON.parse(text);
      } catch {
        return text;
      }
    }

    // Error path — parse body, throw typed exception
    await this.raiseForError(response, path);
    // unreachable — raiseForError always throws
    throw new PulseAPIError(response.status, path);
  }

  private async raiseForError(response: Response, path: string): Promise<never> {
    const text = await response.text();
    let body: PulseErrorBody | string | null = null;
    if (text) {
      try {
        body = JSON.parse(text) as PulseErrorBody;
      } catch {
        body = text;
      }
    }

    switch (response.status) {
      case 401:
        throw new PulseAuthError(response.status, path, body);
      case 404:
        throw new PulseNotFoundError(response.status, path, body);
      case 400:
        throw new PulseValidationError(response.status, path, body);
      case 429: {
        let retryAfter: number | null = null;
        if (body && typeof body === 'object' && typeof body.retryAfterSeconds === 'number') {
          retryAfter = body.retryAfterSeconds;
        }
        if (retryAfter === null) {
          const header = response.headers.get('Retry-After');
          if (header !== null) {
            const parsed = Number.parseInt(header, 10);
            if (!Number.isNaN(parsed)) retryAfter = parsed;
          }
        }
        throw new PulseRateLimitError(response.status, path, body, retryAfter);
      }
      default:
        throw new PulseAPIError(response.status, path, body);
    }
  }
}

// ---------------------------------------------------------------------------
// Resource classes — one per OpenAPI tag.
// ---------------------------------------------------------------------------

abstract class Resource {
  constructor(protected readonly client: PulseClient) {}
}

/** client.auth — authentication + session management. */
export class AuthResource extends Resource {
  /**
   * POST /api/auth/login — exchanges credentials for a JWT.
   *
   * On success, the returned token is cached on the parent client so subsequent
   * calls authenticate automatically. The full response (including
   * `refreshToken` and `activeOrg`) is returned for downstream use.
   */
  public async login(username: string, password: string): Promise<Record<string, unknown>> {
    const response = (await this.client.request({
      method: 'POST',
      path: '/api/auth/login',
      body: { username, password },
      authenticated: false,
    })) as Record<string, unknown>;
    if (typeof response.token === 'string') {
      this.client.token = response.token;
    }
    return response;
  }

  /** POST /api/auth/refresh — exchanges a refresh token for a fresh JWT. */
  public async refresh(refreshToken: string): Promise<Record<string, unknown>> {
    const response = (await this.client.request({
      method: 'POST',
      path: '/api/auth/refresh',
      body: { refreshToken },
      authenticated: false,
    })) as Record<string, unknown>;
    if (typeof response.token === 'string') {
      this.client.token = response.token;
    }
    return response;
  }

  /** GET /api/auth/organizations — orgs the current user is a member of. */
  public async organizations(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/auth/organizations',
    })) as Record<string, unknown>;
    const orgs = result?.organizations;
    return Array.isArray(orgs) ? (orgs as Record<string, unknown>[]) : [];
  }

  /**
   * POST /api/auth/switch-org — switches the active organisation.
   *
   * The new JWT (with updated `orgId` claim) is cached on the parent client.
   */
  public async switchOrg(orgId: string): Promise<Record<string, unknown>> {
    const response = (await this.client.request({
      method: 'POST',
      path: '/api/auth/switch-org',
      body: { orgId },
    })) as Record<string, unknown>;
    if (typeof response.token === 'string') {
      this.client.token = response.token;
    }
    return response;
  }
}

/** client.pipelines — create / list / inspect / delete pipelines. */
export class PipelinesResource extends Resource {
  /** GET /api/pulse/pipelines — every pipeline in the current org. */
  public async list(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/pipelines',
    })) as Record<string, unknown>;
    const pipelines = result?.pipelines;
    return Array.isArray(pipelines) ? (pipelines as Record<string, unknown>[]) : [];
  }

  /** GET /api/pulse/pipelines/{id} — one pipeline by id. */
  public async get(pipelineId: string): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'GET',
      path: `/api/pulse/pipelines/${encodeURIComponent(pipelineId)}`,
    })) as Record<string, unknown>;
  }

  /**
   * POST /api/pulse/pipelines — creates + deploys a new pipeline.
   *
   * `definition` must follow the CreatePipelineRequest schema (see
   * openapi.yaml). At minimum: `name` + `nodes`.
   */
  public async create(definition: Record<string, unknown>): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'POST',
      path: '/api/pulse/pipelines',
      body: definition,
    })) as Record<string, unknown>;
  }

  /** DELETE /api/pulse/pipelines/{id} — tears down the pipeline. */
  public async delete(pipelineId: string): Promise<void> {
    await this.client.request({
      method: 'DELETE',
      path: `/api/pulse/pipelines/${encodeURIComponent(pipelineId)}`,
    });
  }
}

/** client.agents — inspect deployed agents (read-only). */
export class AgentsResource extends Resource {
  /** GET /api/pulse/agents — every deployed agent in the current org. */
  public async list(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/agents',
    })) as Record<string, unknown>;
    const agents = result?.agents;
    return Array.isArray(agents) ? (agents as Record<string, unknown>[]) : [];
  }

  /** GET /api/pulse/agents/{id} — one agent by id. */
  public async get(agentId: string): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'GET',
      path: `/api/pulse/agents/${encodeURIComponent(agentId)}`,
    })) as Record<string, unknown>;
  }
}

/** client.templates — first-party pipeline template catalog. */
export class TemplatesResource extends Resource {
  /** GET /api/pulse/templates — the 223+ first-party templates. */
  public async list(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/templates',
    })) as Record<string, unknown>;
    const templates = result?.templates;
    return Array.isArray(templates) ? (templates as Record<string, unknown>[]) : [];
  }
}

/** Options for {@link EventsResource.stream}. */
export interface StreamOptions {
  /** Optional AbortSignal to cancel the stream from the consumer side. */
  signal?: AbortSignal;
}

/**
 * client.events — live SSE stream of events flowing through the engine.
 *
 * @example
 * ```ts
 * for await (const event of client.events.stream()) {
 *   console.log(event.type, event.payload);
 * }
 *
 * // With cancellation:
 * const ac = new AbortController();
 * setTimeout(() => ac.abort(), 10_000);
 * for await (const event of client.events.stream({ signal: ac.signal })) {
 *   ...
 * }
 * ```
 *
 * Uses native `fetch` with a streaming response body — works in Node 20+
 * AND modern browsers without an `EventSource` polyfill (and without
 * EventSource's lack-of-Authorization-header limitation).
 */
export class EventsResource extends Resource {
  /** GET /api/pulse/events/stream — async iterable of parsed events. */
  public async *stream(options: StreamOptions = {}): AsyncIterableIterator<Record<string, unknown>> {
    if (!this.client.token) {
      throw new PulseAuthError(401, '/api/pulse/events/stream', {
        error: 'No token set for SSE stream',
      });
    }
    const url = `${this.client.baseUrlInternal}/api/pulse/events/stream`;
    const fetchImpl = this.client.fetchInternal;
    const response = await fetchImpl(url, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${this.client.token}`,
        Accept: 'text/event-stream',
        'Cache-Control': 'no-cache',
        'User-Agent': USER_AGENT,
      },
      signal: options.signal,
    });

    if (!response.ok) {
      const text = await response.text().catch(() => '');
      let body: PulseErrorBody | string | null = null;
      if (text) {
        try {
          body = JSON.parse(text) as PulseErrorBody;
        } catch {
          body = text;
        }
      }
      if (response.status === 401) {
        throw new PulseAuthError(response.status, '/api/pulse/events/stream', body);
      }
      throw new PulseAPIError(response.status, '/api/pulse/events/stream', body);
    }

    if (!response.body) {
      return;
    }

    // SSE parser — accumulate `data:` lines per event, dispatch on blank line.
    // See https://html.spec.whatwg.org/multipage/server-sent-events.html
    const reader = response.body.getReader();
    const decoder = new TextDecoder('utf-8');
    let buffer = '';
    let dataLines: string[] = [];

    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        let newlineIdx;
        while ((newlineIdx = buffer.indexOf('\n')) !== -1) {
          const rawLine = buffer.slice(0, newlineIdx).replace(/\r$/, '');
          buffer = buffer.slice(newlineIdx + 1);

          if (rawLine === '') {
            // Event boundary — assemble + yield
            if (dataLines.length > 0) {
              const payload = dataLines.join('\n');
              dataLines = [];
              try {
                yield JSON.parse(payload) as Record<string, unknown>;
              } catch {
                yield { data: payload };
              }
            }
            continue;
          }
          if (rawLine.startsWith(':')) continue; // comment / keep-alive
          if (rawLine.startsWith('data:')) {
            dataLines.push(rawLine.slice(5).trimStart());
          }
          // Other SSE fields (event:/id:/retry:) consumed but not surfaced.
        }
      }
    } finally {
      reader.releaseLock();
    }
  }
}

/** client.users — user management (admin only). */
export class UsersResource extends Resource {
  /**
   * GET /api/pulse/users — every user in the current org.
   *
   * Requires the caller to have the USERS_LIST permission atom (Owner or
   * Platform Admin personas by default — see B-105).
   */
  public async list(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/users',
    })) as Record<string, unknown>;
    const users = result?.users;
    return Array.isArray(users) ? (users as Record<string, unknown>[]) : [];
  }
}
