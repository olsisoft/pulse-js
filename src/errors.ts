/**
 * Typed exception hierarchy for the Pulse JavaScript client.
 *
 * Every HTTP error from the server is translated into one of these. They all
 * inherit from {@link PulseClientError} so callers can catch the entire
 * client-side family with one `catch (e instanceof PulseClientError)`.
 */

/** Base class for every error raised by this library. */
export class PulseClientError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    // Preserve stack trace on V8 (Node + Chromium)
    if (typeof (Error as { captureStackTrace?: unknown }).captureStackTrace === 'function') {
      (Error as { captureStackTrace: (target: object, ctor: unknown) => void }).captureStackTrace(
        this,
        this.constructor,
      );
    }
  }
}

/** Body shape the Pulse server returns for errors (see openapi.yaml ApiError schema). */
export interface PulseErrorBody {
  error?: string;
  errorCode?: string;
  errorClass?: string;
  errorMessage?: string;
  retryAfterSeconds?: number;
  [key: string]: unknown;
}

/**
 * A non-2xx response from the Pulse server.
 *
 * Carries the status code, the parsed error body (if JSON), and the request
 * path so log lines + bug reports are actionable.
 */
export class PulseAPIError extends PulseClientError {
  public readonly statusCode: number;
  public readonly path: string;
  public readonly body: PulseErrorBody | string | null;

  constructor(statusCode: number, path: string, body: PulseErrorBody | string | null = null) {
    super(formatMessage(statusCode, path, body));
    this.statusCode = statusCode;
    this.path = path;
    this.body = body;
  }
}

function formatMessage(
  statusCode: number,
  path: string,
  body: PulseErrorBody | string | null,
): string {
  let msg = `HTTP ${statusCode} from ${path}`;
  if (body && typeof body === 'object') {
    const err = body.error ?? body.errorMessage ?? (body as { message?: string }).message;
    if (err) msg += ` — ${err}`;
  } else if (typeof body === 'string' && body) {
    msg += ` — ${body.slice(0, 200)}`;
  }
  return msg;
}

/** 401 — invalid / missing / expired JWT. */
export class PulseAuthError extends PulseAPIError {}

/** 404 — the resource does not exist. */
export class PulseNotFoundError extends PulseAPIError {}

/** 400 — the request body is malformed. */
export class PulseValidationError extends PulseAPIError {}

/**
 * 429 — per-user or per-IP rate limit hit.
 *
 * Carries {@link retryAfterSeconds} parsed from either the JSON body or the
 * `Retry-After` header, whichever is present.
 */
export class PulseRateLimitError extends PulseAPIError {
  public readonly retryAfterSeconds: number | null;

  constructor(
    statusCode: number,
    path: string,
    body: PulseErrorBody | string | null,
    retryAfterSeconds: number | null,
  ) {
    super(statusCode, path, body);
    this.retryAfterSeconds = retryAfterSeconds;
  }
}
