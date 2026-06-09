/**
 * Official JavaScript / TypeScript client for StreamFlow Pulse.
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
 *
 * @packageDocumentation
 */

export {
  AgentsResource,
  AuthResource,
  ConnectorsResource,
  type EventsReplayOptions,
  EventsResource,
  type IQDiffOptions,
  type IQFilterExpression,
  type IQGetOptions,
  type IQQueryOptions,
  IQResource,
  type IQScanOptions,
  type ModelUploadOptions,
  ModelsResource,
  PipelinesResource,
  PulseClient,
  type PulseClientOptions,
  type StreamOptions,
  TemplatesResource,
  UsersResource,
  validateWasmModule,
  type WasmUploadOptions,
  WasmResource,
} from './client.js';

export {
  DuplexChannel,
  type DuplexOptions,
  type DuplexOutput,
  deriveWsUrl,
} from './duplex.js';

export {
  PulseAPIError,
  PulseAuthError,
  PulseClientError,
  type PulseErrorBody,
  PulseNotFoundError,
  PulseRateLimitError,
  PulseValidationError,
} from './errors.js';

export {
  aggs,
  type BranchSpec,
  type BroadcastJoinOptions,
  type CdcJoinOptions,
  type CepOptions,
  type EnrichAsyncOptions,
  type EnrichOptions,
  type ExtractOptions,
  type FromTopicOptions,
  type MapLlmOptions,
  type MapOptions,
  type McpCallOptions,
  type MlPredictOptions,
  StreamBuilder,
  type StreamBuilderOptions,
  StreamsResource,
  type ToTopicOptions,
  type WasmOptions,
  type WindowOptions,
  WindowSpec,
  windows,
} from './streams.js';

export const VERSION = '2.6.1';
