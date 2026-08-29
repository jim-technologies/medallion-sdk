import {
  isRetryableConnectError,
  MedallionError,
  medallionApiErrorFromEnvelope,
  sanitizedErrorCause,
} from "./errors.js";
import {
  type NormalizedTracing,
  normalizeTracing,
  setResponseSpanAttributes,
  type TracingConfig,
  traceRequest,
} from "./tracing.js";
import type { FetchLike, RetryOptions } from "./types.js";

export interface RequestClientOptions {
  baseUrl: string;
  apiKey?: string;
  accessToken?: string;
  workspaceId: string;
  fetch?: FetchLike;
  timeoutMs?: number;
  retry?: RetryOptions;
  tracing?: TracingConfig;
}

export interface JsonRequestOptions {
  method: "POST";
  path: string;
  body?: unknown;
  headers?: Record<string, string | undefined>;
  signal?: AbortSignal;
  timeoutMs?: number;
  connectProtocol?: boolean;
  retrySafe?: boolean;
  /**
   * SDK-controlled Idempotency-Key header value for whole-batch replay
   * protection. Caller-supplied headers can never set it.
   */
  idempotencyKey?: string;
}

export interface ResponseEnvelope<TBody> {
  body: TBody;
  requestId?: string;
}

interface SignalState {
  signal?: AbortSignal;
  didTimeout: () => boolean;
  cleanup: () => void;
}

interface NormalizedRetryOptions {
  maxAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
  jitterRatio: number;
}

const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_RETRY: NormalizedRetryOptions = {
  maxAttempts: 1,
  initialDelayMs: 200,
  maxDelayMs: 2_000,
  jitterRatio: 0.2,
};
// Node and browser timers clamp or overflow beyond a signed 32-bit delay.
// Keeping the request deadline within that bound also lets an oversized
// Retry-After wait be cancelled before it can trigger an early retry.
const MAX_TIMER_DELAY_MS = 2_147_483_647;
const MAX_CONNECT_TIMEOUT_MS = MAX_TIMER_DELAY_MS;
const MAX_RESPONSE_BODY_BYTES = 64 * 1024 * 1024;

export class RequestClient {
  private readonly baseUrl: string;
  readonly #credential: {
    kind: "accessToken" | "apiKey";
    value: string;
  };
  readonly #workspaceId: string;
  private readonly fetchImpl: FetchLike;
  private readonly timeoutMs: number;
  private readonly retry: NormalizedRetryOptions;
  private readonly tracing: NormalizedTracing;

  constructor(options: RequestClientOptions) {
    if (typeof options.baseUrl !== "string") {
      throw new MedallionError("baseUrl is required.", {
        code: "MEDALLION_INVALID_OPTIONS",
      });
    }
    if (containsAsciiControl(options.baseUrl)) {
      throw new MedallionError(
        "baseUrl must not contain ASCII control characters.",
        { code: "MEDALLION_INVALID_OPTIONS" },
      );
    }
    const baseUrl = options.baseUrl.trim();

    if (baseUrl.length === 0) {
      throw new MedallionError("baseUrl is required.", {
        code: "MEDALLION_INVALID_OPTIONS",
      });
    }
    let parsedBaseUrl: URL;
    try {
      parsedBaseUrl = new URL(baseUrl);
    } catch {
      throw new MedallionError("baseUrl must be an absolute HTTP(S) URL.", {
        code: "MEDALLION_INVALID_OPTIONS",
      });
    }
    if (
      (parsedBaseUrl.protocol !== "http:" &&
        parsedBaseUrl.protocol !== "https:") ||
      (parsedBaseUrl.protocol === "http:" &&
        !isLoopbackHostname(parsedBaseUrl.hostname)) ||
      parsedBaseUrl.host.length === 0 ||
      parsedBaseUrl.username.length > 0 ||
      parsedBaseUrl.password.length > 0 ||
      hasNonOriginSuffix(baseUrl) ||
      parsedBaseUrl.pathname !== "/" ||
      parsedBaseUrl.search.length > 0 ||
      parsedBaseUrl.hash.length > 0
    ) {
      throw new MedallionError(
        "baseUrl must be an absolute HTTPS origin without credentials, path, query, or fragment; plaintext HTTP is allowed only for loopback hosts.",
        { code: "MEDALLION_INVALID_OPTIONS" },
      );
    }

    const accessToken = nonBlank(options.accessToken);
    const apiKey = nonBlank(options.apiKey);
    if (accessToken !== undefined && apiKey !== undefined) {
      throw new MedallionError(
        "Configure exactly one of accessToken or apiKey, never both.",
        { code: "MEDALLION_AMBIGUOUS_CREDENTIALS" },
      );
    }
    if (accessToken === undefined && apiKey === undefined) {
      throw new MedallionError("apiKey or accessToken is required.", {
        code: "MEDALLION_INVALID_OPTIONS",
      });
    }

    const fetchImpl = options.fetch ?? globalThis.fetch?.bind(globalThis);
    if (typeof fetchImpl !== "function") {
      throw new MedallionError(
        "No fetch implementation is available. Use a supported Node.js runtime or pass a custom fetch.",
        { code: "MEDALLION_FETCH_UNAVAILABLE" },
      );
    }

    this.baseUrl = parsedBaseUrl.origin;
    this.#credential =
      accessToken !== undefined
        ? {
            kind: "accessToken",
            value: validatedCredential(accessToken, "accessToken"),
          }
        : {
            kind: "apiKey",
            value: validatedCredential(requiredCredential(apiKey), "apiKey"),
          };
    this.#workspaceId = requiredWorkspaceSelector(options.workspaceId);
    this.fetchImpl = fetchImpl;
    this.timeoutMs = normalizeTimeout(options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
    this.retry = normalizeRetry(options.retry);
    this.tracing = normalizeTracing(options.tracing);
  }

  async request<TResponse = unknown>(
    options: JsonRequestOptions,
  ): Promise<TResponse> {
    const response = await this.requestJson<TResponse>(options);
    return response.body;
  }

  async requestJson<TResponse = unknown>(
    options: JsonRequestOptions,
  ): Promise<ResponseEnvelope<TResponse>> {
    this.assertWorkspaceSelectorCompatible(options.body);
    const timeoutMs = normalizeTimeout(options.timeoutMs ?? this.timeoutMs);
    const headers = this.defaultHeaders(options, timeoutMs);
    headers.set("Accept", "application/json");

    let body: string | undefined;
    if (options.body !== undefined) {
      headers.set("Content-Type", "application/json");
      try {
        body = JSON.stringify(options.body);
      } catch {
        throw new MedallionError(
          "Medallion request body must be JSON serializable.",
          { code: "MEDALLION_INVALID_JSON_BODY" },
        );
      }
    }

    const redactions = [
      this.#credential.value,
      ...payloadSensitiveValues(body),
    ];

    return this.dispatch<TResponse>({
      method: options.method,
      path: options.path,
      body,
      headers,
      signal: options.signal,
      timeoutMs,
      retrySafe: options.retrySafe ?? false,
      redactions,
      parseBody: readResponseBody,
    });
  }

  private async dispatch<TResponse>(options: {
    method: JsonRequestOptions["method"];
    path: string;
    body?: BodyInit;
    headers: Headers;
    signal?: AbortSignal;
    timeoutMs: number;
    retrySafe: boolean;
    redactions: readonly string[];
    parseBody: (response: Response) => Promise<unknown>;
  }): Promise<ResponseEnvelope<TResponse>> {
    const url = this.buildUrl(options.path);
    const deadline = Date.now() + options.timeoutMs;
    const { signal, didTimeout, cleanup } = createSignal(
      options.signal,
      options.timeoutMs,
    );

    try {
      return await traceRequest(this.tracing, {
        method: options.method,
        path: options.path,
        headers: options.headers,
        run: async (span) => {
          for (
            let attempt = 1;
            attempt <= this.retry.maxAttempts;
            attempt += 1
          ) {
            throwIfAborted(signal);
            const attemptHeaders = new Headers(options.headers);
            if (attempt > 1 && attemptHeaders.has("Connect-Timeout-Ms")) {
              attemptHeaders.set(
                "Connect-Timeout-Ms",
                String(
                  Math.min(
                    MAX_CONNECT_TIMEOUT_MS,
                    Math.max(1, Math.ceil(deadline - Date.now())),
                  ),
                ),
              );
            }
            let response: Response;
            try {
              response = await this.fetchImpl(url, {
                method: options.method,
                headers: attemptHeaders,
                body: options.body,
                signal,
                redirect: "error",
              });
            } catch (error) {
              if (
                attempt < this.retry.maxAttempts &&
                options.retrySafe &&
                !isAbortLikeError(error) &&
                !signal?.aborted
              ) {
                await abortableDelay(this.retryDelay(attempt), signal);
                continue;
              }
              throw error;
            }

            if (response.redirected) {
              await cancelResponseBody(response.body);
              throw new MedallionError(
                "Medallion redirected a request; redirects are not permitted.",
                { code: "MEDALLION_REDIRECT_REJECTED" },
              );
            }

            const requestId = readRequestId(
              response.headers,
              options.redactions,
            );
            let responseBody: unknown;
            try {
              responseBody = await options.parseBody(response);
            } catch (error) {
              setResponseSpanAttributes(span, response.status, requestId);
              if (error instanceof ResponseTooLargeError) {
                throw new MedallionError(
                  `Medallion response exceeded the ${MAX_RESPONSE_BODY_BYTES}-byte safety limit.`,
                  {
                    code: "MEDALLION_RESPONSE_TOO_LARGE",
                    requestId,
                  },
                );
              }
              if (error instanceof InvalidJsonResponseError) {
                if (!response.ok) {
                  const apiError = medallionApiErrorFromEnvelope(
                    response.status,
                    requestId,
                    undefined,
                    undefined,
                    options.redactions,
                  );
                  if (
                    attempt < this.retry.maxAttempts &&
                    isRetryableConnectError(apiError, options.retrySafe)
                  ) {
                    await abortableDelay(
                      this.retryDelay(attempt, response.headers),
                      signal,
                    );
                    continue;
                  }
                  throw apiError;
                }
                throw new MedallionError("Medallion returned invalid JSON.", {
                  code: "MEDALLION_INVALID_JSON_RESPONSE",
                  requestId,
                });
              }
              if (
                attempt < this.retry.maxAttempts &&
                options.retrySafe &&
                (response.ok ||
                  isRetryableConnectError(
                    medallionApiErrorFromEnvelope(
                      response.status,
                      requestId,
                      undefined,
                      undefined,
                      options.redactions,
                    ),
                    true,
                  )) &&
                !isAbortLikeError(error) &&
                !signal?.aborted
              ) {
                await abortableDelay(
                  this.retryDelay(attempt, response.headers),
                  signal,
                );
                continue;
              }
              if (isAbortLikeError(error) || signal?.aborted) throw error;
              throw new MedallionError(
                "Medallion response body could not be read.",
                {
                  code: "MEDALLION_NETWORK_ERROR",
                  requestId,
                  cause: sanitizedErrorCause(error, options.redactions),
                },
              );
            }
            setResponseSpanAttributes(span, response.status, requestId);

            if (!response.ok) {
              const apiError = medallionApiErrorFromEnvelope(
                response.status,
                requestId,
                responseBody,
                undefined,
                options.redactions,
              );
              if (
                attempt < this.retry.maxAttempts &&
                isRetryableConnectError(apiError, options.retrySafe)
              ) {
                await abortableDelay(
                  this.retryDelay(attempt, response.headers),
                  signal,
                );
                continue;
              }
              throw apiError;
            }

            return {
              body: responseBody as TResponse,
              requestId,
            };
          }
          throw new MedallionError(
            "Medallion request attempts were exhausted.",
            {
              code: "MEDALLION_NETWORK_ERROR",
            },
          );
        },
      });
    } catch (error) {
      if (error instanceof MedallionError) {
        throw error;
      }

      if (isAbortLikeError(error) || signal?.aborted) {
        if (didTimeout()) {
          throw new MedallionError(
            `Medallion request timed out after ${options.timeoutMs} ms.`,
            {
              code: "MEDALLION_TIMEOUT",
              cause: sanitizedErrorCause(error, [this.#credential.value]),
            },
          );
        }
        throw new MedallionError("Medallion request was aborted.", {
          code: "MEDALLION_ABORTED",
          cause: sanitizedErrorCause(error, [this.#credential.value]),
        });
      }

      throw new MedallionError("Medallion request failed.", {
        code: "MEDALLION_NETWORK_ERROR",
        cause: sanitizedErrorCause(error, options.redactions),
      });
    } finally {
      cleanup();
    }
  }

  private defaultHeaders(
    options: Pick<
      JsonRequestOptions,
      "headers" | "connectProtocol" | "idempotencyKey"
    >,
    timeoutMs: number,
  ): Headers {
    const headers = new Headers();
    for (const [key, value] of Object.entries(options.headers ?? {})) {
      if (value !== undefined) {
        setValidatedHeader(headers, key, value, "request header");
      }
    }

    rejectProtectedHeaders(headers);
    if (this.#credential.kind === "accessToken") {
      setValidatedHeader(
        headers,
        "Authorization",
        `Bearer ${this.#credential.value}`,
        "credential",
      );
    } else {
      setValidatedHeader(
        headers,
        "X-Medallion-API-Key",
        this.#credential.value,
        "credential",
      );
    }

    setValidatedHeader(
      headers,
      "X-Medallion-Workspace-Id",
      this.#workspaceId,
      "workspaceId",
    );
    if (options.idempotencyKey !== undefined) {
      setValidatedHeader(
        headers,
        "Idempotency-Key",
        options.idempotencyKey,
        "idempotencyKey",
      );
    }
    if (options.connectProtocol) {
      headers.set("Connect-Protocol-Version", "1");
      headers.set("Connect-Timeout-Ms", String(timeoutMs));
    }
    return headers;
  }

  private assertWorkspaceSelectorCompatible(body: unknown): void {
    if (body === null || typeof body !== "object" || Array.isArray(body)) {
      return;
    }

    const record = body as Record<string, unknown>;
    const selectors = [record.workspaceId, record.workspace_id];
    if (
      selectors.some(
        (selector) =>
          typeof selector === "string" &&
          selector.length > 0 &&
          selector !== this.#workspaceId,
      )
    ) {
      throw new MedallionError(
        "The request workspaceId conflicts with this client's immutable workspace.",
        { code: "MEDALLION_WORKSPACE_SELECTOR_CONFLICT" },
      );
    }
  }

  private retryDelay(attempt: number, headers?: Headers): number {
    const retryAfter =
      headers === undefined ? undefined : retryAfterMs(headers);
    if (retryAfter !== undefined) {
      // Retry-After is a server minimum. The total request deadline will
      // cancel a wait that cannot fit; never retry earlier than requested.
      return retryAfter;
    }
    const base = Math.min(
      this.retry.initialDelayMs * 2 ** (attempt - 1),
      this.retry.maxDelayMs,
    );
    const spread = base * this.retry.jitterRatio;
    return Math.min(
      this.retry.maxDelayMs,
      Math.max(0, Math.round(base - spread + Math.random() * spread * 2)),
    );
  }

  private buildUrl(path: string): string {
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;
    return new URL(`${this.baseUrl}${normalizedPath}`).toString();
  }
}

function payloadSensitiveValues(body: string | undefined): string[] {
  if (body === undefined) return [];
  let request: unknown;
  try {
    request = JSON.parse(body);
  } catch {
    return [];
  }
  if (
    request === null ||
    typeof request !== "object" ||
    Array.isArray(request)
  ) {
    return [];
  }
  const events = (request as Record<string, unknown>).events;
  if (!Array.isArray(events)) return [];

  const values = new Set<string>();
  for (const event of events) {
    if (event === null || typeof event !== "object" || Array.isArray(event)) {
      continue;
    }
    const raw = (event as Record<string, unknown>).payloadJson;
    if (typeof raw !== "string" || raw.length === 0) continue;
    values.add(raw);
    try {
      collectSensitiveJsonValues(JSON.parse(raw), values, new Set());
    } catch {
      // The complete serialized payload remains sufficient to redact a
      // wholesale server echo.
    }
  }
  return [...values].toSorted((left, right) => right.length - left.length);
}

function collectSensitiveJsonValues(
  value: unknown,
  output: Set<string>,
  ancestors: Set<object>,
): void {
  if (typeof value === "string") {
    if (value.length >= 4) output.add(value);
    return;
  }
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number"
  ) {
    const encoded = JSON.stringify(value);
    if (encoded.length >= 4) output.add(encoded);
    return;
  }
  if (typeof value !== "object" || ancestors.has(value)) return;
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      for (const item of value)
        collectSensitiveJsonValues(item, output, ancestors);
      return;
    }
    for (const [key, item] of Object.entries(value)) {
      if (key.length >= 4) output.add(key);
      collectSensitiveJsonValues(item, output, ancestors);
    }
  } finally {
    ancestors.delete(value);
  }
}

function normalizeTimeout(value: number): number {
  if (!Number.isFinite(value) || value <= 0 || value > MAX_CONNECT_TIMEOUT_MS) {
    throw new MedallionError(
      `timeoutMs must be a positive number no greater than ${MAX_CONNECT_TIMEOUT_MS}.`,
      { code: "MEDALLION_INVALID_TIMEOUT" },
    );
  }
  return Math.ceil(value);
}

function isLoopbackHostname(hostname: string): boolean {
  const normalized = hostname.toLowerCase();
  return (
    normalized === "localhost" ||
    normalized === "[::1]" ||
    /^127(?:\.\d{1,3}){3}$/.test(normalized)
  );
}

function normalizeRetry(
  value: RetryOptions | undefined,
): NormalizedRetryOptions {
  const retry = { ...DEFAULT_RETRY, ...value };
  if (
    !Number.isInteger(retry.maxAttempts) ||
    retry.maxAttempts < 1 ||
    retry.maxAttempts > 5
  ) {
    throw new MedallionError(
      "retry.maxAttempts must be an integer from 1 through 5.",
      {
        code: "MEDALLION_INVALID_RETRY_OPTIONS",
      },
    );
  }
  if (!Number.isFinite(retry.initialDelayMs) || retry.initialDelayMs < 0) {
    throw new MedallionError("retry.initialDelayMs must be non-negative.", {
      code: "MEDALLION_INVALID_RETRY_OPTIONS",
    });
  }
  if (
    !Number.isFinite(retry.maxDelayMs) ||
    retry.maxDelayMs < retry.initialDelayMs ||
    retry.maxDelayMs > 5_000
  ) {
    throw new MedallionError(
      "retry.maxDelayMs must be at least retry.initialDelayMs and no greater than 5000 ms.",
      {
        code: "MEDALLION_INVALID_RETRY_OPTIONS",
      },
    );
  }
  if (
    !Number.isFinite(retry.jitterRatio) ||
    retry.jitterRatio < 0 ||
    retry.jitterRatio > 1
  ) {
    throw new MedallionError("retry.jitterRatio must be between 0 and 1.", {
      code: "MEDALLION_INVALID_RETRY_OPTIONS",
    });
  }
  return retry;
}

function createSignal(
  inputSignal: AbortSignal | undefined,
  timeoutMs: number,
): SignalState {
  const controller = new AbortController();
  let didTimeout = false;
  const abortFromInput = () => controller.abort(inputSignal?.reason);
  if (inputSignal !== undefined) {
    if (inputSignal.aborted) abortFromInput();
    else inputSignal.addEventListener("abort", abortFromInput, { once: true });
  }
  const timeout = setTimeout(() => {
    didTimeout = true;
    controller.abort(
      new DOMException("Medallion request timed out.", "TimeoutError"),
    );
  }, timeoutMs);

  return {
    signal: controller.signal,
    didTimeout: () => didTimeout,
    cleanup: () => {
      clearTimeout(timeout);
      inputSignal?.removeEventListener("abort", abortFromInput);
    },
  };
}

async function abortableDelay(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0) {
    throwIfAborted(signal);
    return;
  }
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => {
        signal?.removeEventListener("abort", abort);
        resolve();
      },
      Math.min(ms, MAX_TIMER_DELAY_MS),
    );
    const abort = () => {
      clearTimeout(timer);
      reject(signal?.reason ?? new DOMException("Aborted", "AbortError"));
    };
    if (signal?.aborted) abort();
    else signal?.addEventListener("abort", abort, { once: true });
  });
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException("Aborted", "AbortError");
  }
}

async function readResponseBody(response: Response): Promise<unknown> {
  if (response.status === 204 || response.status === 205) return undefined;
  const text = await readBoundedResponseText(response);
  if (text.length === 0) return undefined;
  // This transport always sends JSON. In unary Connect, the request codec
  // selects the successful response codec; Accept and response media-type
  // hints never switch decoding to another representation. Errors are JSON
  // envelopes as well.
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new InvalidJsonResponseError();
  }
}

class InvalidJsonResponseError extends Error {
  constructor() {
    super("Medallion returned invalid JSON.");
  }
}

class ResponseTooLargeError extends Error {
  constructor() {
    super("Medallion response exceeded the configured safety limit.");
  }
}

async function readBoundedResponseText(response: Response): Promise<string> {
  const declaredLength = response.headers.get("content-length")?.trim();
  if (
    declaredLength !== undefined &&
    /^\d+$/.test(declaredLength) &&
    BigInt(declaredLength) > BigInt(MAX_RESPONSE_BODY_BYTES)
  ) {
    await cancelResponseBody(response.body);
    throw new ResponseTooLargeError();
  }

  if (response.body === null) return "";
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const parts: string[] = [];
  let receivedBytes = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      receivedBytes += value.byteLength;
      if (receivedBytes > MAX_RESPONSE_BODY_BYTES) {
        try {
          await reader.cancel();
        } catch {
          // Preserve the bounded-response error even if stream cancellation
          // itself fails.
        }
        throw new ResponseTooLargeError();
      }
      parts.push(decoder.decode(value, { stream: true }));
    }
    parts.push(decoder.decode());
    return parts.join("");
  } finally {
    reader.releaseLock();
  }
}

async function cancelResponseBody(
  body: ReadableStream<Uint8Array> | null,
): Promise<void> {
  try {
    await body?.cancel();
  } catch {
    // A declared oversize response is terminal regardless of cancellation.
  }
}

function readRequestId(
  headers: Headers,
  redactions: readonly string[],
): string | undefined {
  let value =
    headers.get("x-request-id") ??
    headers.get("request-id") ??
    headers.get("x-correlation-id") ??
    undefined;
  if (value === undefined) return undefined;
  for (const secret of redactions) {
    if (secret.length > 0) value = value.split(secret).join("[REDACTED]");
  }
  const sanitized = Array.from(value)
    .filter((character) => {
      const code = character.charCodeAt(0);
      return code >= 32 && code !== 127;
    })
    .join("")
    .trim()
    .slice(0, 256);
  return sanitized.length > 0 ? sanitized : undefined;
}

function retryAfterMs(headers: Headers): number | undefined {
  const raw = headers.get("retry-after")?.trim();
  if (raw === undefined || raw.length === 0) return undefined;
  if (/^\d+$/.test(raw)) return Number(raw) * 1_000;
  const date = Date.parse(raw);
  return Number.isNaN(date) ? undefined : Math.max(0, date - Date.now());
}

function nonBlank(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

function requiredWorkspaceSelector(value: unknown): string {
  if (typeof value !== "string") {
    throw new MedallionError("workspaceId is required for ingestion.", {
      code: "MEDALLION_MISSING_WORKSPACE_ID",
    });
  }
  if (value.length === 0) {
    throw new MedallionError("workspaceId is required for ingestion.", {
      code: "MEDALLION_MISSING_WORKSPACE_ID",
    });
  }
  if (!/^ws_[0-9a-hjkmnp-tv-z]{26}$/.test(value)) {
    throw new MedallionError(
      "workspaceId must match ^ws_[0-9a-hjkmnp-tv-z]{26}$.",
      { code: "MEDALLION_INVALID_WORKSPACE_ID" },
    );
  }
  return value;
}

const PROTECTED_REQUEST_HEADERS = new Set([
  "accept",
  "authorization",
  "connect-protocol-version",
  "connect-timeout-ms",
  "content-length",
  "content-type",
  "host",
  "idempotency-key",
  "transfer-encoding",
  "x-medallion-api-key",
  "x-medallion-workspace-id",
]);

function rejectProtectedHeaders(headers: Headers): void {
  for (const name of headers.keys()) {
    if (PROTECTED_REQUEST_HEADERS.has(name.toLowerCase())) {
      throw new MedallionError(
        `The ${name} header is controlled by the Medallion SDK.`,
        { code: "MEDALLION_PROTECTED_HEADER" },
      );
    }
  }
}

function hasNonOriginSuffix(value: string): boolean {
  const authorityStart = value.indexOf("://") + 3;
  if (authorityStart < 3) return true;
  const suffixStart = value.slice(authorityStart).search(/[/?#]/);
  if (suffixStart < 0) return false;
  return value.slice(authorityStart + suffixStart) !== "/";
}

function containsAsciiControl(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) return true;
  }
  return false;
}

function validatedCredential(value: string, label: string): string {
  // API keys and bearer tokens are HTTP token values. Reject controls,
  // whitespace, and non-ASCII characters before a platform Headers object can
  // construct an exception that echoes the credential.
  if (!/^[\x21-\x7e]+$/.test(value)) {
    throw new MedallionError(
      `${label} contains characters that cannot be sent as an HTTP credential.`,
      { code: "MEDALLION_INVALID_CREDENTIAL" },
    );
  }
  return value;
}

function validatedHeaderValue(value: string, label: string): string {
  if (!/^[\x20-\x7e]*$/.test(value)) {
    throw new MedallionError(
      `${label} contains characters that cannot be sent in an HTTP header.`,
      { code: "MEDALLION_INVALID_HEADER_VALUE" },
    );
  }
  return value;
}

function setValidatedHeader(
  headers: Headers,
  name: string,
  value: string,
  label: string,
): void {
  validatedHeaderValue(value, label);
  try {
    headers.set(name, value);
  } catch {
    throw new MedallionError(
      "Medallion request header configuration is invalid.",
      {
        code: "MEDALLION_INVALID_HEADER_VALUE",
      },
    );
  }
}

function requiredCredential(value: string | undefined): string {
  if (value === undefined) {
    throw new MedallionError("apiKey or accessToken is required.", {
      code: "MEDALLION_INVALID_OPTIONS",
    });
  }
  return value;
}

function isAbortLikeError(error: unknown): boolean {
  return (
    (error instanceof DOMException &&
      (error.name === "AbortError" || error.name === "TimeoutError")) ||
    (error instanceof Error &&
      (error.name === "AbortError" || error.name === "TimeoutError"))
  );
}
