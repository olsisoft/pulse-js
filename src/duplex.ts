/**
 * B-114 — bidirectional duplex channel for synchronous decision agents.
 *
 * Opens ONE WebSocket to `/api/pulse/agents/{id}/duplex`: events are streamed
 * IN and the agent's correlated outputs come back OUT on the same connection,
 * matched by a correlation id. Eliminates the 2-connection publish-then-poll
 * pattern for decision microservices (fraud, pricing, A/B assignment).
 *
 * The duplex endpoint runs on the Pulse WebSocket port (REST port + 1 by
 * convention); {@link deriveWsUrl} derives it from the client's base URL.
 *
 * WebSocket source: the built-in global `WebSocket` (browsers, Node 22+) is
 * used when present; on Node < 22 (which has no global `WebSocket`) the
 * optional `ws` package is loaded lazily. Install it with `npm i ws` — it is
 * declared as an optional dependency so the core SDK stays dependency-light.
 *
 * @example
 * ```ts
 * const ch = await client.duplex('fraud-detector');
 * for (const tx of incoming) {
 *   await ch.send(tx, tx.id);
 *   const signal = await ch.recv();   // the agent's output for THIS input
 *   if ((signal.payload as { decision: string }).decision === 'DENY') { ... }
 * }
 * await ch.close();
 * ```
 *
 * @packageDocumentation
 */

import { PulseAPIError, PulseClientError } from './errors.js';

/** Options passed to `client.duplex()`. */
export interface DuplexOptions {
  /** Override the derived WebSocket URL (skips {@link deriveWsUrl}). */
  wsUrl?: string;
}

/**
 * Builds the duplex WebSocket URL from the client's REST base URL.
 *
 * `http`→`ws` / `https`→`wss`, host unchanged, port → REST port + 1 (the Pulse
 * WebSocket server convention). The JWT, when set, rides as a `token` query
 * param (the server reads it from the upgrade request line). The agent id is
 * URL-encoded.
 */
export function deriveWsUrl(baseUrl: string, agentId: string, token: string | undefined): string {
  const parts = new URL(baseUrl);
  const scheme = parts.protocol === 'https:' ? 'wss' : 'ws';
  const host = parts.hostname || 'localhost';
  let netloc = host;
  if (parts.port !== '') {
    netloc = `${host}:${Number.parseInt(parts.port, 10) + 1}`;
  }
  const path = `/api/pulse/agents/${encodeURIComponent(agentId)}/duplex`;
  const query = token ? `?token=${encodeURIComponent(token)}` : '';
  return `${scheme}://${netloc}${path}${query}`;
}

/** An agent output event delivered by {@link DuplexChannel.recv}. */
export interface DuplexOutput extends Record<string, unknown> {
  /** The correlation id of the input that produced this output. */
  correlationId: string | undefined;
}

interface PendingRecv {
  resolve: (output: DuplexOutput) => void;
  reject: (err: Error) => void;
}

/** The minimal WebSocket surface this channel uses — satisfied by both the
 *  global `WebSocket` and the `ws` package's WebSocket (which provides the same
 *  `addEventListener` / `MessageEvent.data` shim). */
interface DuplexSocket {
  addEventListener(type: string, listener: (event: { data?: unknown }) => void): void;
  removeEventListener(type: string, listener: (event: { data?: unknown }) => void): void;
  send(data: string): void;
  close(): void;
}

type WebSocketCtor = new (url: string) => DuplexSocket;

/**
 * Resolves a WebSocket constructor. Prefers the global `WebSocket` (browsers /
 * Node 22+); on Node < 22 falls back to the optional `ws` package, lazily
 * imported. Throws a clear error if neither is available.
 */
async function resolveWebSocketCtor(): Promise<WebSocketCtor> {
  const globalWs = (globalThis as { WebSocket?: WebSocketCtor }).WebSocket;
  if (typeof globalWs === 'function') {
    return globalWs;
  }
  try {
    const mod = (await import('ws')) as unknown as {
      default?: WebSocketCtor;
      WebSocket?: WebSocketCtor;
    };
    const ctor = mod.default ?? mod.WebSocket;
    if (typeof ctor !== 'function') {
      throw new Error('ws module did not export a WebSocket constructor');
    }
    return ctor;
  } catch {
    throw new PulseClientError(
      "duplex needs a WebSocket implementation: this runtime has no global WebSocket " +
        "(Node < 22) and the optional 'ws' package is not installed — run `npm i ws`.",
    );
  }
}

/**
 * An open duplex session over a single WebSocket.
 *
 * {@link send} publishes an event to the agent's input topic and returns its
 * correlation id; {@link recv} returns the next output event the agent
 * produced (each carries a `correlationId` matching the input that caused it).
 * Acknowledgement / keep-alive / connected frames are consumed transparently.
 */
export class DuplexChannel {
  private readonly url: string;
  private ws: DuplexSocket | undefined;
  /** Outputs received but not yet consumed by recv(). */
  private readonly outputs: DuplexOutput[] = [];
  /** recv() callers waiting on the next output. */
  private readonly waiters: PendingRecv[] = [];
  /** A terminal error (error frame / close) that fails pending + future recvs. */
  private failure: Error | undefined;

  constructor(url: string) {
    if (typeof url !== 'string' || !url.trim()) {
      throw new PulseClientError('DuplexChannel requires a non-empty url');
    }
    this.url = url;
  }

  /**
   * Opens the WebSocket and resolves once the server's `connected` frame
   * arrives. An `error` frame on open (unknown agent / disabled duplex) is
   * surfaced as a thrown {@link PulseAPIError}.
   */
  public async open(): Promise<void> {
    const WebSocketImpl = await resolveWebSocketCtor();
    const socket = new WebSocketImpl(this.url);
    socket.addEventListener('message', (event: { data?: unknown }) => this.onMessage(event));
    socket.addEventListener('close', () => this.onClose());

    await new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        socket.removeEventListener('message', onFirst);
        socket.removeEventListener('error', onError);
        socket.removeEventListener('close', onEarlyClose);
      };
      const onFirst = (event: { data?: unknown }) => {
        let msg: Record<string, unknown>;
        try {
          msg = JSON.parse(toText(event.data)) as Record<string, unknown>;
        } catch {
          msg = {};
        }
        cleanup();
        if (msg.type === 'error') {
          this.closeSocket(socket);
          reject(new PulseAPIError(400, this.url, msg as Record<string, unknown>));
          return;
        }
        // 'connected' (or anything that isn't an error) means the channel is up.
        this.ws = socket;
        resolve();
      };
      const onError = () => {
        cleanup();
        this.closeSocket(socket);
        reject(new PulseClientError(`duplex WebSocket failed to connect to ${this.url}`));
      };
      const onEarlyClose = () => {
        cleanup();
        reject(
          new PulseClientError(`duplex WebSocket to ${this.url} closed before the connected frame`),
        );
      };
      socket.addEventListener('message', onFirst);
      socket.addEventListener('error', onError);
      socket.addEventListener('close', onEarlyClose);
    });
  }

  /**
   * Publishes `payload` to the agent's input topic. Returns the correlation id
   * (generated via `crypto.randomUUID()` when not supplied) that the matching
   * output will carry.
   */
  public async send(payload: Record<string, unknown>, correlationId?: string): Promise<string> {
    if (this.ws === undefined) {
      throw new PulseClientError('duplex channel is not open');
    }
    const cid = correlationId ?? crypto.randomUUID();
    this.ws.send(JSON.stringify({ type: 'send', correlationId: cid, payload }));
    return cid;
  }

  /**
   * Resolves with the next agent output event (skips ack / pong / connected
   * frames). The returned object is the agent's output event (`id` / `topic` /
   * `type` / `key` / `payload`) plus a `correlationId` field identifying the
   * input that produced it. Rejects if an `error` frame arrives or the socket
   * closes before an output is delivered.
   */
  public async recv(): Promise<DuplexOutput> {
    if (this.ws === undefined && this.outputs.length === 0) {
      throw new PulseClientError('duplex channel is not open');
    }
    const queued = this.outputs.shift();
    if (queued !== undefined) {
      return queued;
    }
    if (this.failure !== undefined) {
      throw this.failure;
    }
    return new Promise<DuplexOutput>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  /** Closes the underlying WebSocket. Idempotent. */
  public async close(): Promise<void> {
    const socket = this.ws;
    this.ws = undefined;
    if (socket !== undefined) {
      this.closeSocket(socket);
    }
  }

  // ------------------------------------------------------------------
  // Internals
  // ------------------------------------------------------------------

  private onMessage(event: { data?: unknown }): void {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(toText(event.data)) as Record<string, unknown>;
    } catch {
      return;
    }
    const kind = msg.type;
    if (kind === 'output') {
      const rawEvent = msg.event;
      const output: DuplexOutput =
        rawEvent !== null && typeof rawEvent === 'object'
          ? ({ ...(rawEvent as Record<string, unknown>), correlationId: undefined } as DuplexOutput)
          : ({ value: rawEvent, correlationId: undefined } as DuplexOutput);
      output.correlationId = typeof msg.correlationId === 'string' ? msg.correlationId : undefined;
      this.deliver(output);
      return;
    }
    if (kind === 'error') {
      this.fail(new PulseAPIError(400, this.url, msg as Record<string, unknown>));
      return;
    }
    // ack / pong / connected → transparently skipped
  }

  private onClose(): void {
    // Only surface a failure to recv() callers who are still waiting; a clean
    // close after all outputs were consumed is not an error.
    if (this.waiters.length > 0) {
      this.fail(new PulseClientError('duplex channel closed before an output arrived'));
    }
    this.ws = undefined;
  }

  private deliver(output: DuplexOutput): void {
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter.resolve(output);
    } else {
      this.outputs.push(output);
    }
  }

  private fail(err: Error): void {
    this.failure = err;
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift();
      waiter?.reject(err);
    }
  }

  private closeSocket(socket: DuplexSocket): void {
    try {
      socket.close();
    } catch {
      // already closing / closed
    }
  }
}

/** Coerce a WebSocket message payload (string | Buffer | ArrayBuffer) to text. */
function toText(data: unknown): string {
  if (typeof data === 'string') return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) {
    return new TextDecoder().decode(data as ArrayBufferView);
  }
  return String(data);
}
