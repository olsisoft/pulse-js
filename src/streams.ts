/**
 * B-107 — Kafka-Streams-like declarative DSL that compiles to a Pulse pipeline.
 *
 * The DSL is **server-side execution, client-side declaration**: the operator
 * chain is built in TypeScript, compiled to the JSON pipeline shape that the
 * Pulse server's `StreamingOperatorValidator` accepts, and POSTed to
 * `/api/pulse/pipelines`. Stream processing then runs on the Pulse engine
 * (3.6 M evt/s native throughput), not in the client process.
 *
 * This is the opposite of Kafka Streams (which runs in the caller's JVM). The
 * trade-off: you can't do microsecond client-side compute, but you get
 * infinite-scale stateful streaming, durable replicated state queryable via
 * B-106 IQ, and the same DSL works from any of the 5 Pulse SDKs.
 *
 * @example
 * ```ts
 * import { PulseClient, StreamBuilder, windows, aggs } from '@olsisoft/pulse-client';
 *
 * const builder = new StreamBuilder('iot-temperature-aggregator')
 *   .fromTopic('sensor-readings', { sourceEngine: 'mqtt' })
 *   .keyBy('deviceId')
 *   .window(windows.tumbling('60s'), {
 *     aggregations: { avgTemp: aggs.avg('temperature') },
 *   })
 *   .filter('avgTemp > 75')
 *   .toTopic('sensor-minute-averages', { sinkChannel: 'email' });
 *
 * const client = new PulseClient({ baseUrl: 'http://localhost:9090', token: 'ey...' });
 * await client.streams.deploy(builder);
 * ```
 *
 * Supported operators (mirror the 11 validated by
 * `com.streamflow.pulse.streaming.StreamingOperatorValidator`):
 * `filter`, `map`, `flatMap`, `keyBy`, `window`, `branch`, `enrich`,
 * `enrichAsync`, `cep`, `broadcastJoin`, `cdcJoin`.
 *
 * Supported window specs: `tumbling`, `sliding`, `session`, `globalWin`,
 * `count`, `countSliding`.
 *
 * Supported aggregators: `count`, `sum`, `avg`, `min`, `max`, `collectList`,
 * `distinctCount`.
 *
 * Conditions and field-expressions are passed as **strings** (the Pulse
 * streaming runtime parses them server-side). Lambdas / closures are NOT
 * supported because they cannot be serialised to JSON.
 *
 * @packageDocumentation
 */

import type { PulseClient } from './client.js';

// ---------------------------------------------------------------------------
// Window specs — typed wrappers that compile to the string form the server
// parser (`WindowEngine.parseSpec`) accepts.
// ---------------------------------------------------------------------------

/**
 * A window specification. Compiled to the string form the server expects.
 *
 * Construct via {@link windows} factories — instantiate directly only if you've
 * already validated the raw string against `WindowEngine.parseSpec`.
 */
export class WindowSpec {
  public readonly spec: string;

  constructor(spec: string) {
    if (!spec || !spec.trim()) {
      throw new Error('WindowSpec requires a non-empty spec string');
    }
    this.spec = spec;
  }

  public toString(): string {
    return this.spec;
  }
}

/**
 * Factory namespace for the 6 window kinds the server understands.
 *
 * `globalWin` is the unbounded single-window variant (named `globalWin` here
 * to avoid clashing with the JavaScript `globalThis` / browser `global`).
 */
export const windows = {
  /** Non-overlapping fixed windows: `tumbling('60s')`, `tumbling('5m')`. */
  tumbling(size: string): WindowSpec {
    requireNonBlank('size', size);
    return new WindowSpec(`tumbling(${size})`);
  },

  /** Overlapping windows: `sliding('10m', '1m')` = size, slide step. */
  sliding(size: string, slide: string): WindowSpec {
    requireNonBlank('size', size);
    requireNonBlank('slide', slide);
    return new WindowSpec(`sliding(${size},${slide})`);
  },

  /** Inactivity-bounded windows: `session('30s')`. */
  session(timeout: string): WindowSpec {
    requireNonBlank('timeout', timeout);
    return new WindowSpec(`session(${timeout})`);
  },

  /** Single unbounded window. Use for global aggregates. */
  globalWin(): WindowSpec {
    return new WindowSpec('global');
  },

  /** Event-count tumbling: closes after `n` events. `count(100)`. */
  count(n: number): WindowSpec {
    if (!Number.isInteger(n) || n <= 0) {
      throw new Error(`count window size must be a positive integer, got ${n}`);
    }
    return new WindowSpec(`count(${n})`);
  },

  /** Event-count sliding: `countSliding(100, 10)` = window, slide. */
  countSliding(size: number, slide: number): WindowSpec {
    if (!Number.isInteger(size) || !Number.isInteger(slide) || size <= 0 || slide <= 0) {
      throw new Error(
        `countSliding requires positive integer size and slide, got ${size}, ${slide}`,
      );
    }
    return new WindowSpec(`count_sliding(${size},${slide})`);
  },
} as const;

// ---------------------------------------------------------------------------
// Aggregators — string-template builders that compile to the form the
// server parser (`Aggregators.parse`) accepts inside `window.aggregations`.
// ---------------------------------------------------------------------------

/**
 * Factory namespace for the 7 aggregation functions the server understands.
 *
 * Each returns the string template the server parses (`'avg(temperature)'`).
 * Use inside a window's `aggregations` map:
 * ```ts
 * windows.tumbling('60s'), { aggregations: { avgTemp: aggs.avg('temperature') } }
 * ```
 */
export const aggs = {
  /** Event count — no field required. */
  count(): string {
    return 'count()';
  },

  /** Sum of a numeric field: `aggs.sum('amount')`. */
  sum(field: string): string {
    requireNonBlank('field', field);
    return `sum(${field})`;
  },

  /** Average of a numeric field: `aggs.avg('price')`. */
  avg(field: string): string {
    requireNonBlank('field', field);
    return `avg(${field})`;
  },

  /** Minimum value of a numeric field. */
  min(field: string): string {
    requireNonBlank('field', field);
    return `min(${field})`;
  },

  /** Maximum value of a numeric field. */
  max(field: string): string {
    requireNonBlank('field', field);
    return `max(${field})`;
  },

  /** Collect every value of `field` into a list. */
  collectList(field: string): string {
    requireNonBlank('field', field);
    return `collect_list(${field})`;
  },

  /** Cardinality of distinct values of `field`. */
  distinctCount(field: string): string {
    requireNonBlank('field', field);
    return `distinct_count(${field})`;
  },
} as const;

// ---------------------------------------------------------------------------
// StreamBuilder option shapes
// ---------------------------------------------------------------------------

/** Options passed to {@link StreamBuilder.fromTopic}. */
export interface FromTopicOptions {
  /** Source subType (`'kafka'`, `'mqtt'`, `'webhook'`, …). Default `'kafka'`. */
  sourceEngine?: string;
  /** Extra config merged into the source node's `config` dict. */
  sourceConfig?: Record<string, unknown>;
  /** Display label for the source node. */
  label?: string;
}

/** Options passed to {@link StreamBuilder.toTopic}. */
export interface ToTopicOptions {
  /** Sink subType (`'kafka'`, `'telegram'`, `'email'`, …). Omit to skip the sink node. */
  sinkChannel?: string;
  /** Extra config merged into the sink node's `config` dict. */
  sinkConfig?: Record<string, unknown>;
  /** Display label for the sink node. */
  label?: string;
}

/** Options passed to {@link StreamBuilder.map}. */
export interface MapOptions {
  /** Output-field-name → source-expression string mapping. */
  fields?: Record<string, string>;
  /** Tag the output event with a `type` field. */
  targetType?: string;
}

/** Options passed to {@link StreamBuilder.window}. */
export interface WindowOptions {
  /** Map of output-field → aggregator-string (use {@link aggs} for the right-hand side). */
  aggregations?: Record<string, string>;
  /** Override for where window results go. */
  outputTopic?: string;
  /** Server-side trigger config (passed through opaquely). */
  trigger?: unknown;
}

/** A single branch of {@link StreamBuilder.branch}. */
export interface BranchSpec {
  condition: string;
  topic: string;
}

/** Options passed to {@link StreamBuilder.enrich}. */
export interface EnrichOptions {
  lookupTopic: string;
  keyField: string;
}

/** Options passed to {@link StreamBuilder.enrichAsync}. */
export interface EnrichAsyncOptions {
  url: string;
  parallelism?: number;
  queueSize?: number;
  timeoutMs?: number;
  maxRetries?: number;
  retryBackoffMs?: number;
  ordering?: 'PRESERVE_INPUT' | 'UNORDERED';
  onFailure?: 'EMIT_ERROR' | 'DROP' | 'PASS_THROUGH';
}

/** Options passed to {@link StreamBuilder.cep}. */
export interface CepOptions {
  within?: string;
  name?: string;
}

/** Options passed to {@link StreamBuilder.broadcastJoin}. */
export interface BroadcastJoinOptions {
  joinKeyField: string;
  streamingTopic?: string;
  name?: string;
  maxBytes?: number;
  refreshMode?: 'cdc' | 'periodic' | 'explicit';
  intervalMillis?: number;
}

/** Options passed to {@link StreamBuilder.cdcJoin}. */
export interface CdcJoinOptions {
  source: string;
  joinKey?: string;
  table?: string;
  stateBackend?: string;
}

/** B-109 — options passed to {@link StreamBuilder.mapLlm}. */
export interface MapLlmOptions {
  outputField: string;
  model?: string;
  temperature?: number;
  maxTokens?: number;
  parallelism?: number;
  ordering?: 'PRESERVE_INPUT' | 'UNORDERED';
  onFailure?: 'EMIT_ERROR' | 'DROP' | 'PASS_THROUGH';
  maxCallsPerSec?: number;
}

/** B-109 — options passed to {@link StreamBuilder.extract}. */
export interface ExtractOptions {
  instruction: string;
  schema: Record<string, string>;
  model?: string;
  temperature?: number;
  maxTokens?: number;
  onFailure?: 'EMIT_ERROR' | 'DROP' | 'PASS_THROUGH';
}

/** B-109 Phase 2 — options passed to {@link StreamBuilder.mcpCall}. */
export interface McpCallOptions {
  args?: Record<string, unknown>;
  outputField?: string;
  parallelism?: number;
  ordering?: 'PRESERVE_INPUT' | 'UNORDERED';
  onFailure?: 'EMIT_ERROR' | 'DROP' | 'PASS_THROUGH';
}

/** B-112 — options passed to {@link StreamBuilder.mlPredict}. */
export interface MlPredictOptions {
  /** Registered model name (see `client.models.upload`). */
  model: string;
  /** Feature names pulled from the event, in the model's input order. */
  inputFields: string[];
  /** Event field the prediction object is written to. */
  outputField: string;
  /** Max concurrent inferences. */
  parallelism?: number;
  ordering?: 'PRESERVE_INPUT' | 'UNORDERED';
  onFailure?: 'EMIT_ERROR' | 'DROP' | 'PASS_THROUGH';
}

/** B-110 — options passed to {@link StreamBuilder.wasm}. */
export interface WasmOptions {
  /** Registered module name (see `client.wasm.upload`). */
  module: string;
  /** Max concurrent module invocations. */
  parallelism?: number;
  ordering?: 'PRESERVE_INPUT' | 'UNORDERED';
  onFailure?: 'EMIT_ERROR' | 'DROP' | 'PASS_THROUGH';
}

/** Constructor options for {@link StreamBuilder}. */
export interface StreamBuilderOptions {
  description?: string;
  agentLabel?: string;
}

// ---------------------------------------------------------------------------
// StreamBuilder — fluent operator-chain → pipeline-JSON compiler.
// ---------------------------------------------------------------------------

/**
 * Fluent builder for a Pulse streaming pipeline.
 *
 * The chain order matters: operators are emitted in the order the methods are
 * called. Each method returns `this` so calls chain naturally.
 *
 * Compile via {@link build} (returns the JSON dict), or deploy via
 * `client.streams.deploy(builder)` (POSTs to the server).
 *
 * @example
 * ```ts
 * const builder = new StreamBuilder('fraud-detector')
 *   .fromTopic('payments')
 *   .filter('amount > 1000')
 *   .keyBy('customer_id')
 *   .window(windows.tumbling('60s'), { aggregations: { cnt: aggs.count() } })
 *   .filter('cnt > 5')
 *   .toTopic('fraud-alerts');
 * ```
 */
export class StreamBuilder {
  private nameValue: string | undefined;
  private descriptionValue: string | undefined;
  private agentLabelValue: string | undefined;
  private inputTopic: string | undefined;
  private sourceEngine: string | undefined;
  private sourceConfig: Record<string, unknown> = {};
  private sourceLabel: string | undefined;
  private outputTopic: string | undefined;
  private sinkChannel: string | undefined;
  private sinkConfig: Record<string, unknown> = {};
  private sinkLabel: string | undefined;
  private ops: Record<string, unknown>[] = [];

  constructor(name?: string, options?: StreamBuilderOptions) {
    if (name !== undefined) {
      requireNonBlank('name', name);
      this.nameValue = name;
    }
    this.descriptionValue = options?.description;
    this.agentLabelValue = options?.agentLabel;
  }

  // ------------------------------------------------------------------
  // Source
  // ------------------------------------------------------------------

  /**
   * Sets the input topic + source node config.
   *
   * @param topic - The topic name the streaming agent will consume from.
   * @param options - Source engine / extra config / label overrides.
   */
  public fromTopic(topic: string, options?: FromTopicOptions): this {
    requireNonBlank('topic', topic);
    this.inputTopic = topic;
    this.sourceEngine = options?.sourceEngine ?? 'kafka';
    this.sourceConfig = { ...(options?.sourceConfig ?? {}) };
    this.sourceLabel = options?.label;
    return this;
  }

  // ------------------------------------------------------------------
  // Operators — each appends one entry to this.ops
  // ------------------------------------------------------------------

  /** Filter operator. `condition` is a CEL-like expression string. */
  public filter(condition: string): this {
    requireNonBlank('condition', condition);
    this.ops.push({ type: 'filter', condition });
    return this;
  }

  /**
   * Map operator. Produces an output event from the input.
   * At least one of `fields` or `targetType` is required.
   */
  public map(options: MapOptions): this {
    if (options.fields === undefined && options.targetType === undefined) {
      throw new Error("map operator does nothing — provide 'fields' or 'targetType'");
    }
    const op: Record<string, unknown> = { type: 'map' };
    if (options.fields !== undefined) op.fields = { ...options.fields };
    if (options.targetType !== undefined) op.targetType = options.targetType;
    this.ops.push(op);
    return this;
  }

  /** Flat-map: explode an array-valued field into one event per element. */
  public flatMap(splitField: string): this {
    requireNonBlank('splitField', splitField);
    this.ops.push({ type: 'flatMap', splitField });
    return this;
  }

  /** Group the stream by a top-level field value. Required before stateful operators. */
  public keyBy(field: string): this {
    requireNonBlank('field', field);
    this.ops.push({ type: 'keyBy', field });
    return this;
  }

  /**
   * Window operator. Aggregates events inside a window.
   *
   * @param spec - A {@link WindowSpec} built via {@link windows}, or the raw string form.
   */
  public window(spec: WindowSpec | string, options?: WindowOptions): this {
    const specStr = spec instanceof WindowSpec ? spec.spec : spec;
    requireNonBlank('spec', specStr);
    const op: Record<string, unknown> = { type: 'window', spec: specStr };
    if (options?.aggregations !== undefined) op.aggregations = { ...options.aggregations };
    if (options?.outputTopic !== undefined) op.outputTopic = options.outputTopic;
    if (options?.trigger !== undefined) op.trigger = options.trigger;
    this.ops.push(op);
    return this;
  }

  /** Branch operator: route events to different topics by condition. */
  public branch(branches: BranchSpec[]): this {
    if (!branches || branches.length === 0) {
      throw new Error('branch operator requires at least one branch');
    }
    const normalised: BranchSpec[] = branches.map((b, i) => {
      if (typeof b.condition !== 'string' || !b.condition.trim()) {
        throw new Error(`branch[${i}] requires a non-empty 'condition'`);
      }
      if (typeof b.topic !== 'string' || !b.topic.trim()) {
        throw new Error(`branch[${i}] requires a non-empty 'topic'`);
      }
      return { condition: b.condition, topic: b.topic };
    });
    this.ops.push({ type: 'branch', branches: normalised });
    return this;
  }

  /** Synchronous enrichment: join the stream against a state-store topic. */
  public enrich(options: EnrichOptions): this {
    requireNonBlank('lookupTopic', options.lookupTopic);
    requireNonBlank('keyField', options.keyField);
    this.ops.push({
      type: 'enrich',
      lookupTopic: options.lookupTopic,
      keyField: options.keyField,
    });
    return this;
  }

  /**
   * Asynchronous HTTP enrichment. `url` supports `{field}` placeholders that
   * get substituted from the event payload before the call.
   */
  public enrichAsync(options: EnrichAsyncOptions): this {
    requireNonBlank('url', options.url);
    const op: Record<string, unknown> = { type: 'enrichAsync', url: options.url };
    if (options.parallelism !== undefined) op.parallelism = options.parallelism;
    if (options.queueSize !== undefined) op.queueSize = options.queueSize;
    if (options.timeoutMs !== undefined) op.timeoutMs = options.timeoutMs;
    if (options.maxRetries !== undefined) op.maxRetries = options.maxRetries;
    if (options.retryBackoffMs !== undefined) op.retryBackoffMs = options.retryBackoffMs;
    if (options.ordering !== undefined) op.ordering = options.ordering;
    if (options.onFailure !== undefined) op.onFailure = options.onFailure;
    this.ops.push(op);
    return this;
  }

  /** Complex Event Processing: match a sequence of conditions. */
  public cep(sequence: Record<string, unknown>[], options?: CepOptions): this {
    if (!sequence || sequence.length === 0) {
      throw new Error('cep operator requires a non-empty sequence');
    }
    const op: Record<string, unknown> = {
      type: 'cep',
      sequence: sequence.map((step) => ({ ...step })),
    };
    if (options?.within !== undefined) op.within = options.within;
    if (options?.name !== undefined) op.name = options.name;
    this.ops.push(op);
    return this;
  }

  /**
   * B-109 — enrich each event with an LLM completion. `prompt` supports
   * `{field}` placeholders (and `{__payload__}`) substituted from the event
   * server-side; the completion text lands on the event under `outputField`.
   */
  public mapLlm(prompt: string, options: MapLlmOptions): this {
    requireNonBlank('prompt', prompt);
    requireNonBlank('outputField', options.outputField);
    const op: Record<string, unknown> = {
      type: 'mapLlm',
      prompt,
      outputField: options.outputField,
    };
    if (options.model !== undefined) op.model = options.model;
    if (options.temperature !== undefined) op.temperature = options.temperature;
    if (options.maxTokens !== undefined) op.maxTokens = options.maxTokens;
    if (options.parallelism !== undefined) op.parallelism = options.parallelism;
    if (options.ordering !== undefined) op.ordering = options.ordering;
    if (options.onFailure !== undefined) op.onFailure = options.onFailure;
    if (options.maxCallsPerSec !== undefined) op.maxCallsPerSec = options.maxCallsPerSec;
    this.ops.push(op);
    return this;
  }

  /**
   * B-109 — LLM → typed structured fields merged into the event. The LLM is
   * asked for a JSON object keyed by `schema`'s fields; missing / malformed
   * fields become `null` server-side (never crashes on a bad response).
   */
  public extract(options: ExtractOptions): this {
    requireNonBlank('instruction', options.instruction);
    if (!options.schema || Object.keys(options.schema).length === 0) {
      throw new Error('extract operator requires a non-empty schema');
    }
    const op: Record<string, unknown> = {
      type: 'extract',
      instruction: options.instruction,
      schema: { ...options.schema },
    };
    if (options.model !== undefined) op.model = options.model;
    if (options.temperature !== undefined) op.temperature = options.temperature;
    if (options.maxTokens !== undefined) op.maxTokens = options.maxTokens;
    if (options.onFailure !== undefined) op.onFailure = options.onFailure;
    this.ops.push(op);
    return this;
  }

  /**
   * B-109 Phase 2 — invoke an MCP tool per event. `args` string values
   * support `{field}` substitution. On success the tool output is written to
   * `outputField` (omit for a fire-and-forget side effect).
   */
  public mcpCall(tool: string, options?: McpCallOptions): this {
    requireNonBlank('tool', tool);
    const op: Record<string, unknown> = { type: 'mcpCall', tool };
    if (options?.args !== undefined) op.args = { ...options.args };
    if (options?.outputField !== undefined) op.outputField = options.outputField;
    if (options?.parallelism !== undefined) op.parallelism = options.parallelism;
    if (options?.ordering !== undefined) op.ordering = options.ordering;
    if (options?.onFailure !== undefined) op.onFailure = options.onFailure;
    this.ops.push(op);
    return this;
  }

  /**
   * B-112 — score each event with an embedded ML model. Runs an uploaded ONNX
   * model in-process on the Pulse engine (no model-server hop). The named
   * `inputFields` are pulled from the event payload and fed to the model; the
   * model's output is written as a nested object under `outputField` so
   * downstream operators can branch on it (e.g.
   * `.filter('prediction.fraud_score > 0.8')`). Upload the model first with
   * `client.models.upload`.
   */
  public mlPredict(options: MlPredictOptions): this {
    requireNonBlank('model', options.model);
    requireNonBlank('outputField', options.outputField);
    if (
      !Array.isArray(options.inputFields) ||
      options.inputFields.length === 0 ||
      !options.inputFields.every((f) => typeof f === 'string' && f.trim() !== '')
    ) {
      throw new Error('inputFields must be a non-empty array of non-blank strings');
    }
    if (
      options.ordering !== undefined &&
      options.ordering !== 'PRESERVE_INPUT' &&
      options.ordering !== 'UNORDERED'
    ) {
      throw new Error(
        `ordering must be PRESERVE_INPUT or UNORDERED, got ${JSON.stringify(options.ordering)}`,
      );
    }
    if (
      options.onFailure !== undefined &&
      options.onFailure !== 'EMIT_ERROR' &&
      options.onFailure !== 'DROP' &&
      options.onFailure !== 'PASS_THROUGH'
    ) {
      throw new Error(
        `onFailure must be EMIT_ERROR, DROP, or PASS_THROUGH, got ${JSON.stringify(options.onFailure)}`,
      );
    }
    const op: Record<string, unknown> = {
      type: 'mlPredict',
      model: options.model,
      inputFields: [...options.inputFields],
      outputField: options.outputField,
    };
    if (options.parallelism !== undefined) op.parallelism = options.parallelism;
    if (options.ordering !== undefined) op.ordering = options.ordering;
    if (options.onFailure !== undefined) op.onFailure = options.onFailure;
    this.ops.push(op);
    return this;
  }

  /**
   * B-110 — run a sandboxed WASM module over each event.
   *
   * The uploaded module (see `client.wasm.upload`) receives the event payload
   * bytes and returns the new payload (transform / map) or drops the event
   * (filter), running in pure-Java Chicory on the engine — no host syscalls,
   * bounded linear memory. Any `wasm32` toolchain (Rust, TinyGo,
   * AssemblyScript, C) can author a module against the alloc/process ABI.
   */
  public wasm(options: WasmOptions): this {
    requireNonBlank('module', options.module);
    if (
      options.ordering !== undefined &&
      options.ordering !== 'PRESERVE_INPUT' &&
      options.ordering !== 'UNORDERED'
    ) {
      throw new Error(
        `ordering must be PRESERVE_INPUT or UNORDERED, got ${JSON.stringify(options.ordering)}`,
      );
    }
    if (
      options.onFailure !== undefined &&
      options.onFailure !== 'EMIT_ERROR' &&
      options.onFailure !== 'DROP' &&
      options.onFailure !== 'PASS_THROUGH'
    ) {
      throw new Error(
        `onFailure must be EMIT_ERROR, DROP, or PASS_THROUGH, got ${JSON.stringify(options.onFailure)}`,
      );
    }
    const op: Record<string, unknown> = { type: 'wasm', module: options.module };
    if (options.parallelism !== undefined) op.parallelism = options.parallelism;
    if (options.ordering !== undefined) op.ordering = options.ordering;
    if (options.onFailure !== undefined) op.onFailure = options.onFailure;
    this.ops.push(op);
    return this;
  }

  /** Broadcast join: enrich the stream against a fully-replicated table. */
  public broadcastJoin(options: BroadcastJoinOptions): this {
    requireNonBlank('joinKeyField', options.joinKeyField);
    const op: Record<string, unknown> = {
      type: 'broadcastJoin',
      joinKeyField: options.joinKeyField,
    };
    if (options.streamingTopic !== undefined) op.streamingTopic = options.streamingTopic;
    if (options.name !== undefined) op.name = options.name;
    if (options.maxBytes !== undefined) op.maxBytes = options.maxBytes;
    if (options.refreshMode !== undefined) op.refreshMode = options.refreshMode;
    if (options.intervalMillis !== undefined) op.intervalMillis = options.intervalMillis;
    this.ops.push(op);
    return this;
  }

  /** CDC join: stream-table join against a CDC-fed state table. */
  public cdcJoin(options: CdcJoinOptions): this {
    requireNonBlank('source', options.source);
    const op: Record<string, unknown> = { type: 'cdcJoin', source: options.source };
    if (options.joinKey !== undefined) op.joinKey = options.joinKey;
    if (options.table !== undefined) op.table = options.table;
    if (options.stateBackend !== undefined) op.stateBackend = options.stateBackend;
    this.ops.push(op);
    return this;
  }

  // ------------------------------------------------------------------
  // Sink
  // ------------------------------------------------------------------

  /**
   * Sets the output topic + optional sink node config. Omit `sinkChannel`
   * to leave the stream's tail at `outputTopic` (downstream consumers
   * subscribe themselves; no sink node is emitted).
   */
  public toTopic(topic: string, options?: ToTopicOptions): this {
    requireNonBlank('topic', topic);
    this.outputTopic = topic;
    this.sinkChannel = options?.sinkChannel;
    this.sinkConfig = { ...(options?.sinkConfig ?? {}) };
    this.sinkLabel = options?.label;
    return this;
  }

  /**
   * Terminate the stream in a connector sink (Segment, Kafka, Postgres, …) —
   * an ergonomic, connector-first alias for
   * `toTopic(topic, { sinkChannel, sinkConfig })`. `connectorType` is a subType
   * from `client.connectors.list()`; bridged connectors require the enterprise
   * bridge JAR on the server. `topic` defaults to `<connectorType>-sink-out`.
   */
  public toConnector(
    connectorType: string,
    config?: Record<string, unknown>,
    options?: { topic?: string; label?: string }
  ): this {
    requireNonBlank('connectorType', connectorType);
    const topic = options?.topic ?? `${connectorType}-sink-out`;
    return this.toTopic(topic, {
      sinkChannel: connectorType,
      sinkConfig: config,
      label: options?.label,
    });
  }

  /**
   * Terminate the stream in the agent's state store (queryable via the
   * B-106 IQ surface). No sink node is emitted and no output topic is set.
   */
  public toState(): this {
    this.outputTopic = undefined;
    this.sinkChannel = undefined;
    this.sinkConfig = {};
    this.sinkLabel = undefined;
    return this;
  }

  // ------------------------------------------------------------------
  // Metadata
  // ------------------------------------------------------------------

  /** Sets / overrides the pipeline name. */
  public named(name: string): this {
    requireNonBlank('name', name);
    this.nameValue = name;
    return this;
  }

  /** Sets the pipeline description. */
  public describedAs(description: string): this {
    this.descriptionValue = description;
    return this;
  }

  /** Sets the display label for the streaming agent node. */
  public withAgentLabel(label: string): this {
    requireNonBlank('label', label);
    this.agentLabelValue = label;
    return this;
  }

  // ------------------------------------------------------------------
  // Compilation
  // ------------------------------------------------------------------

  /** Returns a copy of the recorded operator chain (read-only view). */
  public operators(): Record<string, unknown>[] {
    return this.ops.map((op) => ({ ...op }));
  }

  /**
   * Compile the chain into a Pulse pipeline dict ready for POST.
   *
   * @param name - Override the pipeline name. Falls back to the value set at
   * construction or via {@link named}.
   */
  public build(name?: string): Record<string, unknown> {
    const pipelineName = name ?? this.nameValue;
    if (!pipelineName) {
      throw new Error(
        "pipeline name required — pass to new StreamBuilder(name) or .build('name')",
      );
    }
    if (!this.inputTopic) {
      throw new Error('no source — call .fromTopic(...) before .build()');
    }
    if (this.ops.length === 0) {
      throw new Error(
        'no operators — chain at least one of .filter/.map/.keyBy/... before .build()',
      );
    }

    const nodes: Record<string, unknown>[] = [];

    // Source node
    nodes.push({
      type: 'source',
      label: this.sourceLabel ?? `${this.sourceEngine} source`,
      config: {
        engine: this.sourceEngine,
        inputTopic: this.inputTopic,
        ...this.sourceConfig,
      },
    });

    // Agent node (the streaming processor)
    const agentConfig: Record<string, unknown> = {
      engine: 'streaming',
      inputTopic: this.inputTopic,
      operators: this.ops.map((op) => ({ ...op })),
    };
    if (this.outputTopic !== undefined) {
      agentConfig.outputTopic = this.outputTopic;
    }
    nodes.push({
      type: 'agent',
      label: this.agentLabelValue ?? pipelineName,
      config: agentConfig,
    });

    // Sink node — only when both an output topic AND a sink channel are set
    if (this.outputTopic !== undefined && this.sinkChannel !== undefined) {
      nodes.push({
        type: 'sink',
        label: this.sinkLabel ?? `${this.sinkChannel} sink`,
        config: {
          channel: this.sinkChannel,
          inputTopic: this.outputTopic,
          ...this.sinkConfig,
        },
      });
    }

    const pipeline: Record<string, unknown> = { name: pipelineName, nodes };
    if (this.descriptionValue !== undefined) {
      pipeline.description = this.descriptionValue;
    }
    return pipeline;
  }
}

// ---------------------------------------------------------------------------
// StreamsResource — the client.streams accessor.
// ---------------------------------------------------------------------------

/**
 * `client.streams` — compile + deploy {@link StreamBuilder} pipelines. Just
 * sugar over `client.pipelines.create()` — the compile happens client-side,
 * the deploy is the same POST.
 */
export class StreamsResource {
  private readonly client: PulseClient;

  constructor(client: PulseClient) {
    this.client = client;
  }

  /** Compile the builder to a pipeline dict WITHOUT deploying. */
  public compile(builder: StreamBuilder, name?: string): Record<string, unknown> {
    return builder.build(name);
  }

  /** Compile + POST to `/api/pulse/pipelines`. Returns the server response. */
  public async deploy(builder: StreamBuilder, name?: string): Promise<Record<string, unknown>> {
    const definition = builder.build(name);
    return this.client.pipelines.create(definition);
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function requireNonBlank(name: string, value: string | undefined): void {
  if (value === undefined || typeof value !== 'string' || !value.trim()) {
    throw new Error(`${name} must be a non-empty string, got ${JSON.stringify(value)}`);
  }
}
