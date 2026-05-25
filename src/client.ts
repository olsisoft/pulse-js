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

import { DuplexChannel, type DuplexOptions, deriveWsUrl } from './duplex.js';
import {
  PulseAPIError,
  PulseAuthError,
  PulseClientError,
  type PulseErrorBody,
  PulseNotFoundError,
  PulseRateLimitError,
  PulseValidationError,
} from './errors.js';
import { StreamsResource } from './streams.js';

const USER_AGENT = 'pulse-client-js/2.6.0';
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
  public readonly iq: IQResource;
  public readonly streams: StreamsResource;
  public readonly models: ModelsResource;

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
    this.iq = new IQResource(this);
    this.streams = new StreamsResource(this);
    this.models = new ModelsResource(this);
  }

  /**
   * B-114 — open a bidirectional duplex channel to an agent.
   *
   * Returns a {@link DuplexChannel} that streams events IN and receives the
   * agent's correlated outputs OUT on a single WebSocket — the
   * synchronous-decision path (fraud, pricing, A/B assignment). Resolves once
   * the server's `connected` frame arrives (an `error` frame on open is
   * surfaced as a thrown {@link PulseAPIError}).
   *
   * The endpoint runs on the Pulse WebSocket port (REST port + 1); pass
   * `options.wsUrl` to override the derived URL.
   *
   * @example
   * ```ts
   * const ch = await client.duplex('fraud-detector');
   * const cid = await ch.send({ amount: 5000 }, 'tx-1');
   * const signal = await ch.recv();   // signal.correlationId === 'tx-1'
   * await ch.close();
   * ```
   */
  public async duplex(agentId: string, options: DuplexOptions = {}): Promise<DuplexChannel> {
    if (typeof agentId !== 'string' || !agentId.trim()) {
      throw new PulseClientError('agentId must be a non-empty string');
    }
    const url = options.wsUrl ?? deriveWsUrl(this.baseUrl, agentId, this.token);
    const channel = new DuplexChannel(url);
    await channel.open();
    return channel;
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

  /**
   * @internal — multipart/form-data POST, used by {@link ModelsResource.upload}.
   *
   * Lets the platform `fetch` set the `Content-Type` (with the generated
   * boundary) itself — we must NOT set it manually or the boundary is lost.
   */
  public async requestMultipart(path: string, form: FormData): Promise<unknown> {
    const headers: Record<string, string> = { 'User-Agent': USER_AGENT };
    if (!this.token) {
      throw new PulseAuthError(401, path, {
        error: 'No token set. Call client.auth.login() first or pass options.token.',
      });
    }
    headers['Authorization'] = `Bearer ${this.token}`;

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);

    let response: Response;
    try {
      response = await this.fetchImpl(`${this.baseUrl}${path}`, {
        method: 'POST',
        headers,
        body: form,
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
    await this.raiseForError(response, path);
    throw new PulseAPIError(response.status, path);
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

/** client.agents — list / get / update / delete deployed agents. */
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

  /**
   * B-115 Phase 1 — `PUT /api/pulse/agents/{id}`: replace the agent's config.
   *
   * `config` is the FULL agent config (not a partial merge) — at minimum
   * `name`. Optional fields (`engineType`, `inputTopic`, `outputTopic`,
   * `description`, `instances`, `monthlyBudget`, `config`) fall back to safe
   * defaults when omitted. See the `UpdateAgentRequest` schema in
   * `openapi.yaml`.
   *
   * Today this triggers a full stop + persist + start cycle on the engine
   * side — the agent is briefly unavailable while the swap happens.
   * Existing state in the agent's keyed store is preserved. Phase 2
   * (B-115-engine) will add atomic event-boundary swap so hot-reloadable
   * changes apply with no downtime.
   *
   * Returns the post-update agent snapshot (same shape as {@link get}).
   * Throws {@link PulseValidationError} on a bad config (self-loop, invalid
   * streaming operators), {@link PulseNotFoundError} if the agent doesn't
   * exist.
   */
  public async update(
    agentId: string,
    config: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'PUT',
      path: `/api/pulse/agents/${encodeURIComponent(agentId)}`,
      body: config,
    })) as Record<string, unknown>;
  }

  /**
   * `DELETE /api/pulse/agents/{id}` — stop the agent + remove its config row.
   *
   * The agent's keyed state store is also dropped. Requires the
   * `AGENT_DELETE` permission.
   */
  public async delete(agentId: string): Promise<void> {
    await this.client.request({
      method: 'DELETE',
      path: `/api/pulse/agents/${encodeURIComponent(agentId)}`,
    });
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

/** Options passed to {@link ModelsResource.upload}. */
export interface ModelUploadOptions {
  /** Model name referenced by `mlPredict({ model })`. */
  name: string;
  /** Raw model bytes (alternative to {@link path}). */
  data?: Uint8Array | Buffer;
  /** Filesystem path to the `.onnx` file (alternative to {@link data}). */
  path?: string;
  /** Model runtime — only `'onnx'` is supported today. Default `'onnx'`. */
  runtime?: string;
  /** Ordered feature-name → type map, used to pack the input tensor. */
  inputSchema?: Record<string, string>;
  /** Output-name → type map (informational). */
  outputSchema?: Record<string, string>;
}

/**
 * client.models — B-112 embedded ML model registry.
 *
 * Upload ONNX models that the streaming {@link StreamBuilder.mlPredict}
 * operator scores events against, in-process on the Pulse engine (no
 * model-server hop). Models are org-scoped; upload / delete require the
 * ADMIN role.
 *
 * @example
 * ```ts
 * await client.models.upload({
 *   name: 'fraud-classifier',
 *   path: './model.onnx',
 *   inputSchema: { amount: 'float', country: 'string' },
 *   outputSchema: { fraud_score: 'float', label: 'string' },
 * });
 * ```
 */
export class ModelsResource extends Resource {
  /**
   * POST /api/pulse/ml-models — upload (or replace) a model as
   * multipart/form-data. Supply the model either by `data` bytes or file
   * `path` (exactly one). Replacing an existing name hot-swaps the model with
   * no agent restart. Returns the persisted model metadata.
   */
  public async upload(options: ModelUploadOptions): Promise<Record<string, unknown>> {
    if (typeof options.name !== 'string' || !options.name.trim()) {
      throw new PulseClientError('name must be a non-empty string');
    }
    const hasData = options.data !== undefined;
    const hasPath = options.path !== undefined;
    if (hasData === hasPath) {
      throw new PulseClientError("provide exactly one of 'path' or 'data'");
    }

    let blob: Uint8Array;
    let filename: string;
    if (hasPath) {
      const { readFile } = await import('node:fs/promises');
      blob = await readFile(options.path as string);
      filename = (options.path as string).split('/').pop() ?? `${options.name}.onnx`;
    } else {
      blob = options.data as Uint8Array;
      filename = `${options.name}.onnx`;
    }
    if (blob.length === 0) {
      throw new PulseClientError('model bytes are empty');
    }

    const runtime = options.runtime ?? 'onnx';
    const form = new FormData();
    form.append('name', options.name);
    form.append('runtime', runtime);
    if (options.inputSchema !== undefined) {
      form.append('inputSchema', JSON.stringify(options.inputSchema));
    }
    if (options.outputSchema !== undefined) {
      form.append('outputSchema', JSON.stringify(options.outputSchema));
    }
    // Copy into a fresh ArrayBuffer so Blob gets a clean, exactly-sized buffer
    // regardless of any Buffer pooling / view offset on the input.
    const bytes = new Uint8Array(blob.length);
    bytes.set(blob);
    form.append(
      'model',
      new Blob([bytes], { type: 'application/octet-stream' }),
      filename,
    );

    return (await this.client.requestMultipart(
      '/api/pulse/ml-models',
      form,
    )) as Record<string, unknown>;
  }

  /** GET /api/pulse/ml-models — models registered for the caller's org. */
  public async list(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/ml-models',
    })) as Record<string, unknown>;
    const models = result?.models;
    return Array.isArray(models) ? (models as Record<string, unknown>[]) : [];
  }

  /** GET /api/pulse/ml-models/{name} — metadata for one model. */
  public async get(name: string): Promise<Record<string, unknown>> {
    if (typeof name !== 'string' || !name.trim()) {
      throw new PulseClientError('name must be a non-empty string');
    }
    return (await this.client.request({
      method: 'GET',
      path: `/api/pulse/ml-models/${encodeURIComponent(name)}`,
    })) as Record<string, unknown>;
  }

  /** DELETE /api/pulse/ml-models/{name} — remove a model (ADMIN). */
  public async delete(name: string): Promise<void> {
    if (typeof name !== 'string' || !name.trim()) {
      throw new PulseClientError('name must be a non-empty string');
    }
    await this.client.request({
      method: 'DELETE',
      path: `/api/pulse/ml-models/${encodeURIComponent(name)}`,
    });
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

  /**
   * GET /api/pulse/iq/agents/{affectingState}/state/replay/{key} — B-113.
   *
   * The ordered changes that touched a state key between two instants.
   * `options.affectingState` is the agent whose state store to inspect;
   * `options.key` is the state key. `options.from` / `options.to` accept the
   * same specs as {@link IQGetOptions.asOf} (defaults: `from='-1h'`,
   * `to='now'`); `options.limit` defaults to 100.
   *
   * Unwraps the `changes` array from the response — each change carries
   * `timestamp`, `changeType` (`PUT` / `DELETE`), the resulting `value`, and
   * `eventId` when known.
   *
   * @example
   * ```ts
   * const changes = await client.events.replay({
   *   affectingState: 'user-sessions', key: 'u42',
   *   from: '2026-05-24T10:00:00Z', to: '2026-05-24T11:00:00Z',
   * });
   * ```
   */
  public async replay(options: EventsReplayOptions): Promise<Record<string, unknown>[]> {
    const params = new URLSearchParams();
    params.set('from', options.from ?? '-1h');
    params.set('to', options.to ?? 'now');
    params.set('limit', String(options.limit ?? 100));
    const path =
      `/api/pulse/iq/agents/${encodeURIComponent(options.affectingState)}` +
      `/state/replay/${encodeURIComponent(options.key)}?${params.toString()}`;
    const result = (await this.client.request({ method: 'GET', path })) as Record<
      string,
      unknown
    >;
    const changes = result?.changes;
    return Array.isArray(changes) ? (changes as Record<string, unknown>[]) : [];
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

/** Optional range + limit for {@link IQResource.scan} / {@link IQResource.listKeys}. */
/** Optional inputs for {@link IQResource.get} (B-113 time-travel). */
export interface IQGetOptions {
  /**
   * Read the value as it was at a past instant instead of the live value.
   * Accepts `now`, a relative offset (`-1h`, `-30m`, `-7d`), an ISO-8601
   * instant, or epoch millis. Sent as the `?as_of=` query param.
   */
  asOf?: string;
}

/** Optional inputs for {@link IQResource.diff} (B-113 state diff). */
export interface IQDiffOptions {
  /** Lower-bound instant. Same specs as {@link IQGetOptions.asOf}. Default `-1h`. */
  from?: string;
  /** Upper-bound instant. Same specs as {@link IQGetOptions.asOf}. Default `now`. */
  to?: string;
}

/** Optional inputs for {@link EventsResource.replay} (B-113 change replay). */
export interface EventsReplayOptions {
  /** The agent whose state store to inspect. */
  affectingState: string;
  /** The state key to replay changes for. */
  key: string;
  /** Lower-bound instant. Same specs as {@link IQGetOptions.asOf}. Default `-1h`. */
  from?: string;
  /** Upper-bound instant. Same specs as {@link IQGetOptions.asOf}. Default `now`. */
  to?: string;
  /** Maximum number of changes to return. Default 100. */
  limit?: number;
}

export interface IQScanOptions {
  /** Inclusive lower bound on the key range. Omit for beginning. */
  start?: string;
  /** Exclusive upper bound on the key range. Omit for end. */
  end?: string;
  /** Page size; server clamps to [1, 1000]. Default 100. */
  limit?: number;
}

/**
 * Filter expression for {@link IQResource.query}. Recursive: each node MUST carry
 * exactly ONE discriminator — `field` (leaf), `and`, `or`, or `not`. Mixing in a
 * single node is rejected with HTTP 400.
 *
 * Use `'$value'` as `field` to test the value itself for scalar (non-map) states.
 */
export interface IQFilterExpression {
  field?: string;
  op?: 'eq' | 'neq' | 'exists' | 'notexists' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'in';
  value?: unknown;
  and?: IQFilterExpression[];
  or?: IQFilterExpression[];
  not?: IQFilterExpression;
}

/** Optional inputs for {@link IQResource.query}. */
export interface IQQueryOptions {
  start?: string;
  end?: string;
  limit?: number;
  filter?: IQFilterExpression;
  /** Field names to include in returned entries. Non-map values pass through. */
  projection?: string[];
  /**
   * Field name to group on. Switches the response shape from flat
   * `{entries, ...}` to grouped `{groups: [{groupKey, count}], ...}`.
   * Use `'$value'` for scalar states.
   */
  groupBy?: string;
}

/**
 * client.iq — B-106 Interactive Queries.
 *
 * Query the live state of streaming agents like a database from any
 * microservice. The killer use case is a synchronous decision service
 * (fraud, rate-limit, pricing) calling {@link get} on every request and
 * reading agent state from RAM with zero ingest-to-decision lag:
 *
 * @example
 * ```ts
 * const state = await client.iq.get('fraud-detector', 'customer-42');
 * if ((state.value as { tx_count_60s: number }).tx_count_60s > 5) {
 *   denyPayment();
 * }
 * ```
 *
 * All methods require the `AGENT_READ` permission (Owner, Platform Admin,
 * Developer, Auditor personas by default — see B-105).
 *
 * Responses are returned as raw `Record<string, unknown>` objects so callers
 * can paginate, inspect `truncated`/`limitApplied`/`totalScanned` metadata,
 * and read fields without going through a wrapper layer.
 */
export class IQResource extends Resource {
  /** GET /api/pulse/iq/agents/{id}/state — headline state summary. */
  public async summary(agentId: string): Promise<Record<string, unknown>> {
    const path = `/api/pulse/iq/agents/${encodeURIComponent(agentId)}/state`;
    return (await this.client.request({ method: 'GET', path })) as Record<string, unknown>;
  }

  /**
   * GET /api/pulse/iq/agents/{id}/state/value/{key} — point lookup.
   *
   * B-113 — pass `options.asOf` to read the value as it was at a past instant
   * (time-travel) instead of the live value. Accepts `now`, a relative offset
   * (`-1h`, `-30m`, `-7d`), an ISO-8601 instant, or epoch millis. The response
   * then also carries `asOf` (resolved epoch ms).
   *
   * @example
   * ```ts
   * const past = await client.iq.get('user-sessions', 'u42', { asOf: '-1h' });
   * ```
   *
   * @throws PulseNotFoundError when the key is absent OR the agent is not
   *   queryable. Inspect `error.body.error` ("Key not found" vs "Agent has
   *   no queryable state") to differentiate; `error.body.reason` carries
   *   the not-queryable cause.
   */
  public async get(
    agentId: string,
    key: string,
    options: IQGetOptions = {},
  ): Promise<Record<string, unknown>> {
    let path =
      `/api/pulse/iq/agents/${encodeURIComponent(agentId)}` +
      `/state/value/${encodeURIComponent(key)}`;
    if (options.asOf !== undefined) {
      const params = new URLSearchParams();
      params.set('as_of', options.asOf);
      path += `?${params.toString()}`;
    }
    return (await this.client.request({ method: 'GET', path })) as Record<string, unknown>;
  }

  /**
   * GET /api/pulse/iq/agents/{id}/state/diff/{key} — B-113 state diff.
   *
   * Field-level delta of `key`'s state between two instants. `options.from`
   * and `options.to` accept the same specs as {@link IQGetOptions.asOf}
   * (defaults: `from='-1h'`, `to='now'`). Returns the raw response dict
   * `{agentId, key, fromTs, toTs, changes}` where `changes` maps each changed
   * field to `{delta?, from, to}` (`delta` present for numeric fields), or
   * `{added}` / `{removed}`.
   *
   * @example
   * ```ts
   * const d = await client.iq.diff('user-sessions', 'u42', { from: '-1h', to: 'now' });
   * // d.changes.cart_value === { delta: 70, from: 0, to: 70 }
   * ```
   */
  public async diff(
    agentId: string,
    key: string,
    options: IQDiffOptions = {},
  ): Promise<Record<string, unknown>> {
    const params = new URLSearchParams();
    params.set('from', options.from ?? '-1h');
    params.set('to', options.to ?? 'now');
    const path =
      `/api/pulse/iq/agents/${encodeURIComponent(agentId)}` +
      `/state/diff/${encodeURIComponent(key)}?${params.toString()}`;
    return (await this.client.request({ method: 'GET', path })) as Record<string, unknown>;
  }

  /**
   * GET /api/pulse/iq/agents/{id}/state/scan — paginated range scan.
   *
   * Returns the raw response dict. Inspect `truncated` to decide if more
   * data exists; paginate by setting `start` on the next call to the last
   * returned key plus a sentinel suffix.
   *
   * @throws PulseNotFoundError when the agent is not queryable.
   */
  public async scan(
    agentId: string,
    options: IQScanOptions = {},
  ): Promise<Record<string, unknown>> {
    const path =
      `/api/pulse/iq/agents/${encodeURIComponent(agentId)}/state/scan` +
      buildIQScanQuery(options);
    return (await this.client.request({ method: 'GET', path })) as Record<string, unknown>;
  }

  /** GET /api/pulse/iq/agents/{id}/state/keys — keys-only range scan. */
  public async listKeys(
    agentId: string,
    options: IQScanOptions = {},
  ): Promise<Record<string, unknown>> {
    const path =
      `/api/pulse/iq/agents/${encodeURIComponent(agentId)}/state/keys` +
      buildIQScanQuery(options);
    return (await this.client.request({ method: 'GET', path })) as Record<string, unknown>;
  }

  /**
   * POST /api/pulse/iq/agents/{id}/state/query — filtered / projected / grouped query.
   *
   * When `options.groupBy` is set, the response shape is
   * `{groups: [{groupKey, count}], groupCount, ...}` instead of
   * `{entries: [...], count, ...}`.
   *
   * @throws PulseValidationError on invalid filter syntax (HTTP 400).
   * @throws PulseNotFoundError when the agent is not queryable.
   */
  public async query(
    agentId: string,
    options: IQQueryOptions = {},
  ): Promise<Record<string, unknown>> {
    const path = `/api/pulse/iq/agents/${encodeURIComponent(agentId)}/state/query`;
    // Only include keys the caller actually set so we don't lock the
    // server into echoing defaults that aren't part of the request.
    const body: Record<string, unknown> = {};
    if (options.start !== undefined) body.start = options.start;
    if (options.end !== undefined) body.end = options.end;
    if (options.limit !== undefined) body.limit = options.limit;
    if (options.filter !== undefined) body.filter = options.filter;
    if (options.projection !== undefined) body.projection = options.projection;
    if (options.groupBy !== undefined) body.groupBy = options.groupBy;
    return (await this.client.request({
      method: 'POST',
      path,
      body: Object.keys(body).length > 0 ? body : undefined,
    })) as Record<string, unknown>;
  }
}

/**
 * Builds the `?start=&end=&limit=N` query suffix for IQ scan / list-keys.
 * `limit` is always sent (default 100). Missing start/end are omitted so
 * the URL stays clean.
 */
function buildIQScanQuery(options: IQScanOptions): string {
  const params = new URLSearchParams();
  params.set('limit', String(options.limit ?? 100));
  if (options.start !== undefined) params.set('start', options.start);
  if (options.end !== undefined) params.set('end', options.end);
  return `?${params.toString()}`;
}
