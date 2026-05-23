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
  EventsResource,
  PipelinesResource,
  PulseClient,
  type PulseClientOptions,
  type StreamOptions,
  TemplatesResource,
  UsersResource,
} from './client.js';

export {
  PulseAPIError,
  PulseAuthError,
  PulseClientError,
  type PulseErrorBody,
  PulseNotFoundError,
  PulseRateLimitError,
  PulseValidationError,
} from './errors.js';

export const VERSION = '2.5.8';
