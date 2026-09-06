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

const USER_AGENT = 'pulse-client-js/2.7.10';
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

  /**
   * Opt-in automatic retries. `0` (default) means retries are OFF — exactly one
   * attempt per request. When > 0, retries use bounded full-jitter exponential
   * backoff: 429 (rate limited) is retried for any method honouring
   * `Retry-After`; `retryOnStatus` 5xx and transport errors are retried only for
   * idempotent methods (unless `retryNonIdempotent`); terminal 4xx never retry.
   */
  maxRetries?: number;
  /** Base backoff in ms for the full-jitter exponential backoff. Default 200. */
  retryBackoffMs?: number;
  /** Per-attempt backoff cap in ms. Default 10000. */
  retryMaxBackoffMs?: number;
  /** Retryable 5xx statuses. Default `[502, 503, 504]`. */
  retryOnStatus?: number[];
  /** Also retry non-idempotent methods (POST/PATCH) on 5xx/transport. Default false. */
  retryNonIdempotent?: boolean;
}

const IDEMPOTENT_METHODS = new Set(['GET', 'HEAD', 'PUT', 'DELETE', 'OPTIONS']);

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

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

  private readonly maxRetries: number;
  private readonly retryBackoffMs: number;
  private readonly retryMaxBackoffMs: number;
  private readonly retryOnStatus: Set<number>;
  private readonly retryNonIdempotent: boolean;

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
  public readonly wasm: WasmResource;
  public readonly connectors: ConnectorsResource;
  public readonly pvsc: PvscResource;
  public readonly evals: EvalsResource;

  constructor(options: PulseClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, '');
    this.token = options.token;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.maxRetries = Math.max(0, options.maxRetries ?? 0);
    this.retryBackoffMs = options.retryBackoffMs ?? 200;
    this.retryMaxBackoffMs = options.retryMaxBackoffMs ?? 10_000;
    this.retryOnStatus = new Set(options.retryOnStatus ?? [502, 503, 504]);
    this.retryNonIdempotent = options.retryNonIdempotent ?? false;
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
    this.wasm = new WasmResource(this);
    this.connectors = new ConnectorsResource(this);
    this.pvsc = new PvscResource(this);
    this.evals = new EvalsResource(this);
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
  /**
   * Runs {@link requestOnce} under the opt-in retry policy. With retries off
   * (`maxRetries === 0`, the default) it makes exactly one attempt.
   */
  public async request(opts: RequestOptions): Promise<unknown> {
    const idempotent = IDEMPOTENT_METHODS.has(opts.method.toUpperCase());
    let attempt = 0;
    for (;;) {
      try {
        return await this.requestOnce(opts);
      } catch (err) {
        if (attempt >= this.maxRetries) throw err;
        const delay = this.retryDelay(err, idempotent, attempt);
        if (delay === null) throw err;
        await sleep(delay);
        attempt += 1;
      }
    }
  }

  /**
   * Returns the delay (ms) to wait before retrying `err` for an `idempotent`
   * method, or `null` when it must not be retried. 429 → any method (honour
   * Retry-After); `retryOnStatus` 5xx + transport → idempotent only (unless
   * `retryNonIdempotent`); terminal 4xx / client errors → never.
   */
  private retryDelay(err: unknown, idempotent: boolean, attempt: number): number | null {
    if (err instanceof PulseRateLimitError) {
      return err.retryAfterSeconds != null && err.retryAfterSeconds > 0
        ? err.retryAfterSeconds * 1000
        : this.backoffDelay(attempt);
    }
    if (!idempotent && !this.retryNonIdempotent) return null;
    if (err instanceof PulseAPIError) {
      return this.retryOnStatus.has(err.statusCode) ? this.backoffDelay(attempt) : null;
    }
    if (err instanceof PulseClientError) {
      // no-token / other client-side error — deterministic, never retried.
      return null;
    }
    // A non-Pulse rejection == the transport itself failed (network/abort) → retry.
    return this.backoffDelay(attempt);
  }

  /** Full-jitter exponential backoff: uniform in [0, min(max, base * 2^attempt)). */
  private backoffDelay(attempt: number): number {
    const ceiling = Math.min(this.retryMaxBackoffMs, this.retryBackoffMs * 2 ** attempt);
    return Math.random() * ceiling;
  }

  private async requestOnce(opts: RequestOptions): Promise<unknown> {
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
    if (typeof (response.accessToken ?? response.token) === 'string') {
      this.client.token = (response.accessToken ?? response.token) as string;
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
    if (typeof (response.accessToken ?? response.token) === 'string') {
      this.client.token = (response.accessToken ?? response.token) as string;
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
    if (typeof (response.accessToken ?? response.token) === 'string') {
      this.client.token = (response.accessToken ?? response.token) as string;
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
/**
 * `client.connectors` — the connector catalogue (the B-093 analytics family +
 * every native / bridged connector), the same list the Pipeline Studio palette
 * and `pulse connectors list` show. Each entry is
 * `{ subType, displayName, configFields }`; use the `subType` as a sink/source
 * node `type` in a pipeline definition deployed via `client.pipelines.deploy`.
 * Bridged connectors appear only when the enterprise bridge JAR is on the
 * server classpath.
 */
export class ConnectorsResource extends Resource {
  /** GET /api/pulse/connectors — `{ sources: [...], sinks: [...] }`. */
  public async list(): Promise<Record<string, unknown>> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/connectors',
    })) as Record<string, unknown>;
    return result ?? {};
  }

  /** Just the sink connectors. */
  public async sinks(): Promise<Record<string, unknown>[]> {
    return this.entries('sinks');
  }

  /** Just the source connectors. */
  public async sources(): Promise<Record<string, unknown>[]> {
    return this.entries('sources');
  }

  private async entries(kind: string): Promise<Record<string, unknown>[]> {
    const value = (await this.list())?.[kind];
    return Array.isArray(value) ? (value as Record<string, unknown>[]) : [];
  }
}

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

/** Options passed to {@link WasmResource.upload}. */
export interface WasmUploadOptions {
  /** Module name referenced by `wasm({ module })`. */
  name: string;
  /** Raw module bytes (alternative to {@link path}). */
  data?: Uint8Array | Buffer;
  /** Filesystem path to the `.wasm` file (alternative to {@link data}). */
  path?: string;
  /** Optional human-readable description stored alongside the module. */
  description?: string;
}

/**
 * Client-side pre-upload validation of a WASM module's bytes.
 *
 * Mirrors the server's `ChicoryWasmRunner.validateModule`: inspects the binary
 * (it does NOT execute it) and throws {@link PulseValidationError} on a module
 * that the server would reject — a pure sandbox that exports `alloc`,
 * `process`, and `memory` and imports no host functions. Rejecting locally
 * turns a cryptic server 400 / runtime trap into an actionable client error.
 *
 * The check is deliberately conservative: it only rejects modules it can prove
 * are non-conforming (bad magic, host imports, missing required exports). A
 * malformed binary that can't be walked throws "malformed WASM module".
 */
export function validateWasmModule(bytes: Uint8Array): void {
  const fail = (message: string): never => {
    throw new PulseValidationError(400, 'wasm.upload (client-side validation)', {
      error: message,
    });
  };

  if (bytes.length < 8) {
    fail('not a WASM module: too short');
  }

  // Magic "\0asm" + version 0x01 0x00 0x00 0x00.
  if (
    bytes[0] !== 0x00 ||
    bytes[1] !== 0x61 ||
    bytes[2] !== 0x73 ||
    bytes[3] !== 0x6d ||
    bytes[4] !== 0x01 ||
    bytes[5] !== 0x00 ||
    bytes[6] !== 0x00 ||
    bytes[7] !== 0x00
  ) {
    fail('not a WASM module (bad magic/version)');
  }

  /** Reads an unsigned LEB128 at `cursor.pos`, advancing it. Throws on overrun. */
  const readUleb = (cursor: { pos: number }): number => {
    let result = 0;
    let shift = 0;
    for (;;) {
      if (cursor.pos >= bytes.length) {
        fail('malformed WASM module');
      }
      const byte = bytes[cursor.pos++];
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) break;
      shift += 7;
      if (shift > 35) {
        fail('malformed WASM module');
      }
    }
    return result >>> 0;
  };

  const exportNames = new Set<string>();
  let offset = 8;
  while (offset < bytes.length) {
    const id = bytes[offset++];
    const cursor = { pos: offset };
    const size = readUleb(cursor);
    const payloadStart = cursor.pos;
    const payloadEnd = payloadStart + size;
    if (payloadEnd > bytes.length) {
      fail('malformed WASM module');
    }

    if (id === 2) {
      // Import section — any host import means the module is not a pure sandbox.
      const importCursor = { pos: payloadStart };
      const count = readUleb(importCursor);
      if (count > 0) {
        fail(
          'WASM module imports host functions; it must be a pure sandbox ' +
            '(build with no WASI/host imports)',
        );
      }
    } else if (id === 7) {
      // Export section — collect the exported names.
      const exportCursor = { pos: payloadStart };
      const count = readUleb(exportCursor);
      for (let i = 0; i < count; i++) {
        const nameLen = readUleb(exportCursor);
        const nameStart = exportCursor.pos;
        const nameEnd = nameStart + nameLen;
        if (nameEnd > payloadEnd) {
          fail('malformed WASM module');
        }
        let name = '';
        for (let j = nameStart; j < nameEnd; j++) {
          name += String.fromCharCode(bytes[j]);
        }
        exportCursor.pos = nameEnd;
        // 1 kind byte + uleb128 index.
        if (exportCursor.pos >= payloadEnd) {
          fail('malformed WASM module');
        }
        exportCursor.pos += 1; // kind
        readUleb(exportCursor); // index
        exportNames.add(name);
      }
    }

    offset = payloadEnd;
  }

  if (!exportNames.has('alloc') || !exportNames.has('process') || !exportNames.has('memory')) {
    fail('WASM module must export alloc, process and memory');
  }
}

/**
 * client.wasm — B-110 sandboxed WASM module registry.
 *
 * Upload WebAssembly modules that the streaming {@link StreamBuilder.wasm}
 * operator runs over events, sandboxed in pure-Java Chicory on the engine
 * (no host syscalls). Modules are org-scoped; upload / delete require the
 * ADMIN role.
 *
 * @example
 * ```ts
 * await client.wasm.upload({ name: 'pii-redactor', path: './redactor.wasm' });
 * builder.fromTopic('events').wasm({ module: 'pii-redactor' }).toTopic('clean');
 * ```
 */
export class WasmResource extends Resource {
  /**
   * POST /api/pulse/wasm-modules — upload (or replace) a module as
   * multipart/form-data. Supply the module either by `data` bytes or file
   * `path` (exactly one). The module is validated (must parse, import no host
   * functions, export alloc/process/memory) before persisting. Replacing an
   * existing name hot-swaps the module with no agent restart. Returns the
   * persisted module metadata.
   */
  public async upload(options: WasmUploadOptions): Promise<Record<string, unknown>> {
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
      filename = (options.path as string).split('/').pop() ?? `${options.name}.wasm`;
    } else {
      blob = options.data as Uint8Array;
      filename = `${options.name}.wasm`;
    }
    if (blob.length === 0) {
      throw new PulseClientError('module bytes are empty');
    }

    // Client-side pre-upload validation — reject a non-conforming module
    // locally (before the network round-trip) rather than as a cryptic
    // server 400 / runtime trap. Mirrors ChicoryWasmRunner.validateModule.
    validateWasmModule(blob);

    const form = new FormData();
    form.append('name', options.name);
    if (options.description !== undefined) {
      form.append('description', options.description);
    }
    // Copy into a fresh ArrayBuffer so Blob gets a clean, exactly-sized buffer
    // regardless of any Buffer pooling / view offset on the input.
    const bytes = new Uint8Array(blob.length);
    bytes.set(blob);
    form.append('module', new Blob([bytes], { type: 'application/wasm' }), filename);

    return (await this.client.requestMultipart(
      '/api/pulse/wasm-modules',
      form,
    )) as Record<string, unknown>;
  }

  /** GET /api/pulse/wasm-modules — modules registered for the caller's org. */
  public async list(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/wasm-modules',
    })) as Record<string, unknown>;
    const modules = result?.modules;
    return Array.isArray(modules) ? (modules as Record<string, unknown>[]) : [];
  }

  /** GET /api/pulse/wasm-modules/{name} — metadata for one module. */
  public async get(name: string): Promise<Record<string, unknown>> {
    if (typeof name !== 'string' || !name.trim()) {
      throw new PulseClientError('name must be a non-empty string');
    }
    return (await this.client.request({
      method: 'GET',
      path: `/api/pulse/wasm-modules/${encodeURIComponent(name)}`,
    })) as Record<string, unknown>;
  }

  /** DELETE /api/pulse/wasm-modules/{name} — remove a module (ADMIN). */
  public async delete(name: string): Promise<void> {
    if (typeof name !== 'string' || !name.trim()) {
      throw new PulseClientError('name must be a non-empty string');
    }
    await this.client.request({
      method: 'DELETE',
      path: `/api/pulse/wasm-modules/${encodeURIComponent(name)}`,
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

// ---------------------------------------------------------------------------
// PVSC — the governance surface
// ---------------------------------------------------------------------------

/**
 * One field's rule inside a topic schema.
 *
 * `grounding` is the one field worth explaining: it asks whether the value an
 * agent writes here must be traceable to what the agent was given.
 * `'required'` blocks an untraceable value, `'warn'` reports it, and the
 * default `'ignore'` does not look. It is what catches a figure that is
 * well-typed, in range, confidently asserted and invented — every other check
 * in the schema passes such a value.
 */
export interface PvscFieldRule {
  type: string;
  min?: number | null;
  max?: number | null;
  minLength?: number | null;
  maxLength?: number | null;
  allowedValues?: string[] | null;
  grounding?: 'ignore' | 'warn' | 'required';
  /**
   * Whether a figure the agent COMPUTED counts as traceable, or only one it
   * copied. `'deny'` is the default and the strict reading: a field marked for
   * grounding demands the literal. Use `'allow'` on fields the agent genuinely
   * calculates — a total, a difference, a ratio, a rate on a base, a rounding —
   * and every accepted derivation is named in the check's detail so it can be
   * judged rather than trusted.
   */
  derivation?: 'deny' | 'allow';
}

/** A topic's contract, as `PUT /api/pulse/pvsc/schemas` accepts it. */
export interface PvscTopicSchema {
  topic: string;
  requiredFields?: Record<string, PvscFieldRule>;
  optionalFields?: Record<string, PvscFieldRule>;
  allowExtraFields?: boolean;
  keyPolicy?: { required: boolean; pattern?: string | null; description?: string | null } | null;
}

/**
 * One arbitration stance. Attached to the guardian that votes, never read out
 * of what the vote says. Lower `precedence` wins — rank 1 outranks rank 2 —
 * and a `veto` stance blocks by construction rather than by count.
 */
export interface PvscStance {
  domain: string;
  precedence: number;
  veto: boolean;
}

/** client.pvsc — topic contracts, arbitration policy, guardians and the DLQ. */
export class PvscResource extends Resource {
  /** GET /api/pulse/pvsc/schemas — every registered topic contract. */
  public async schemas(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/pvsc/schemas',
    })) as Record<string, unknown>;
    const schemas = result?.schemas;
    return Array.isArray(schemas) ? (schemas as Record<string, unknown>[]) : [];
  }

  /**
   * PUT /api/pulse/pvsc/schemas — registers or replaces a topic's contract.
   *
   * The write REPLACES the schema, it does not merge into it: a field you
   * omit is gone, including its grounding policy. Read the current schema
   * first if you are changing one field of several.
   */
  public async saveSchema(schema: PvscTopicSchema): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'PUT',
      path: '/api/pulse/pvsc/schemas',
      body: schema,
    })) as Record<string, unknown>;
  }

  /** DELETE /api/pulse/pvsc/schemas — drops a topic's contract. */
  public async deleteSchema(topic: string): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'DELETE',
      path: '/api/pulse/pvsc/schemas',
      body: { topic },
    })) as Record<string, unknown>;
  }

  /** GET /api/pulse/pvsc/config — consensus, degradation and arbitration settings. */
  public async config(): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'GET',
      path: '/api/pulse/pvsc/config',
    })) as Record<string, unknown>;
  }

  /** PUT /api/pulse/pvsc/config — patches the settings named in `patch`. */
  public async updateConfig(patch: Record<string, unknown>): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'PUT',
      path: '/api/pulse/pvsc/config',
      body: patch,
    })) as Record<string, unknown>;
  }

  /**
   * Replaces the arbitration stances. Sugar over `updateConfig`, and the
   * shape most callers want: an empty list disables arbitration, so the
   * majority result stands.
   */
  public async setStances(stances: PvscStance[]): Promise<Record<string, unknown>> {
    return this.updateConfig({ arbitrationStances: stances });
  }

  /**
   * GET /api/pulse/pvsc/metrics — counters plus the quorum information yield
   * (`quorumInformationYield`, `quorumRedundantGuardianCalls`,
   * `quorumInterpretation`), which say whether consulting the quorum changed
   * any decision the first guardian would have made alone.
   */
  public async metrics(): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'GET',
      path: '/api/pulse/pvsc/metrics',
    })) as Record<string, unknown>;
  }

  /** GET /api/pulse/pvsc/guardians — the registered guardian pool. */
  public async guardians(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/pvsc/guardians',
    })) as Record<string, unknown>;
    const guardians = result?.guardians;
    return Array.isArray(guardians) ? (guardians as Record<string, unknown>[]) : [];
  }

  /** GET /api/pulse/pvsc/dlq — events the firewall turned away. */
  public async dlq(): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/pvsc/dlq',
    })) as Record<string, unknown>;
    const entries = result?.entries;
    return Array.isArray(entries) ? (entries as Record<string, unknown>[]) : [];
  }

  /** POST /api/pulse/pvsc/dlq/reinject — replays one blocked event. */
  public async reinject(eventId: string): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'POST',
      path: '/api/pulse/pvsc/dlq/reinject',
      body: { eventId },
    })) as Record<string, unknown>;
  }

  /** POST /api/pulse/pvsc/dlq/discard — drops one blocked event for good. */
  public async discard(eventId: string): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'POST',
      path: '/api/pulse/pvsc/dlq/discard',
      body: { eventId },
    })) as Record<string, unknown>;
  }
}

// ---------------------------------------------------------------------------
// Evals — golden cases with a ratcheting non-regression gate
// ---------------------------------------------------------------------------

/** One golden case: an input plus what its agent's output must satisfy. */
export interface EvalCase {
  caseId?: string;
  suiteId: string;
  name: string;
  agentKey: string;
  inputPayload: string;
  expectations: Record<string, unknown>;
  provenanceTrajectoryId?: string | null;
}

/**
 * The verdict of one run. `gate` is `HOLDING` / `IMPROVED` / `REGRESSION` /
 * `NO_BASELINE`, and `blocksRelease` is what a pipeline should branch on.
 */
export interface EvalReport {
  suiteId: string;
  total: number;
  passing: number;
  failing: number;
  baseline: number;
  gate: string;
  blocksRelease: boolean;
  summary: string;
  cases: Array<{
    caseId: string; name: string; verdict: string;
    failedPaths: string[]; detail: string;
  }>;
}

/**
 * client.evals — golden cases replayed against live agents.
 *
 * The gate counts PASSES against a recorded floor rather than counting
 * failures, so deleting an assertion cannot satisfy it. Cases run
 * node-isolated: nothing is persisted, published to a downstream topic, or
 * acted on, which is what makes it safe to run a suite against production
 * agents.
 */
export class EvalsResource extends Resource {
  /** GET /api/pulse/evals — the suite ids that have at least one case. */
  public async suites(): Promise<string[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: '/api/pulse/evals',
    })) as Record<string, unknown>;
    const suites = result?.suites;
    return Array.isArray(suites) ? (suites as string[]) : [];
  }

  /** GET /api/pulse/evals/cases?suite= — the cases in one suite. */
  public async cases(suiteId: string): Promise<Record<string, unknown>[]> {
    const result = (await this.client.request({
      method: 'GET',
      path: `/api/pulse/evals/cases?suite=${encodeURIComponent(suiteId)}`,
    })) as Record<string, unknown>;
    const cases = result?.cases;
    return Array.isArray(cases) ? (cases as Record<string, unknown>[]) : [];
  }

  /** POST /api/pulse/evals/cases — adds or replaces one case. */
  public async saveCase(evalCase: EvalCase): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'POST',
      path: '/api/pulse/evals/cases',
      body: evalCase,
    })) as Record<string, unknown>;
  }

  /**
   * POST /api/pulse/evals/run — replays every case in the suite.
   *
   * A REGRESSION comes back as a normal response with `blocksRelease: true`,
   * not as an error: the run succeeded and the gate's verdict is data. Branch
   * on `blocksRelease`, not on whether this call threw.
   */
  public async run(suiteId: string): Promise<EvalReport> {
    return (await this.client.request({
      method: 'POST',
      path: '/api/pulse/evals/run',
      body: { suiteId },
    })) as unknown as EvalReport;
  }

  /**
   * POST /api/pulse/evals/baseline — records the current passing count as the
   * floor future runs are held to. Call it after a run you are happy with;
   * calling it after a bad one ratchets the floor DOWN.
   */
  public async recordBaseline(suiteId: string): Promise<Record<string, unknown>> {
    return (await this.client.request({
      method: 'POST',
      path: '/api/pulse/evals/baseline',
      body: { suiteId },
    })) as Record<string, unknown>;
  }
}
