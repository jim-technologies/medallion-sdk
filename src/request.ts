import { MedallionApiError, MedallionError } from "./errors.js";
import {
  type NormalizedTracing,
  normalizeTracing,
  setResponseSpanAttributes,
  type TracingConfig,
  traceRequest,
} from "./tracing.js";
import type { FetchLike } from "./types.js";

export interface RequestClientOptions {
  baseUrl: string;
  apiKey?: string;
  accessToken?: string;
  fetch?: FetchLike;
  timeoutMs?: number;
  tracing?: TracingConfig;
}

export interface JsonRequestOptions {
  method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  path: string;
  query?: Record<string, string | number | boolean | undefined>;
  body?: unknown;
  idempotencyKey?: string;
  headers?: Record<string, string | undefined>;
  signal?: AbortSignal;
  timeoutMs?: number;
}

export interface RawRequestOptions {
  method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  path: string;
  query?: Record<string, string | number | boolean | undefined>;
  body?: BodyInit;
  idempotencyKey?: string;
  headers?: Record<string, string | undefined>;
  signal?: AbortSignal;
  timeoutMs?: number;
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

const DEFAULT_TIMEOUT_MS = 30_000;

export class RequestClient {
  private readonly baseUrl: string;
  private readonly bearerToken: string;
  private readonly fetchImpl: FetchLike;
  private readonly timeoutMs: number;
  private readonly tracing: NormalizedTracing;

  constructor(options: RequestClientOptions) {
    const baseUrl = options.baseUrl.trim();

    if (baseUrl.length === 0) {
      throw new MedallionError("baseUrl is required.", {
        code: "MEDALLION_INVALID_OPTIONS",
      });
    }
    let parsedBaseUrl: URL;
    try {
      parsedBaseUrl = new URL(baseUrl);
    } catch (error) {
      throw new MedallionError("baseUrl must be an absolute HTTP(S) URL.", {
        code: "MEDALLION_INVALID_OPTIONS",
        cause: error,
      });
    }
    if (
      (parsedBaseUrl.protocol !== "http:" &&
        parsedBaseUrl.protocol !== "https:") ||
      parsedBaseUrl.host.length === 0 ||
      parsedBaseUrl.username.length > 0 ||
      parsedBaseUrl.password.length > 0 ||
      parsedBaseUrl.search.length > 0 ||
      parsedBaseUrl.hash.length > 0
    ) {
      throw new MedallionError(
        "baseUrl must be an absolute HTTP(S) URL without credentials, query, or fragment.",
        { code: "MEDALLION_INVALID_OPTIONS" },
      );
    }

    const bearerToken = [options.accessToken, options.apiKey]
      .map((value) => value?.trim())
      .find((value) => value !== undefined && value.length > 0);
    if (bearerToken === undefined || bearerToken.length === 0) {
      throw new MedallionError("apiKey or accessToken is required.", {
        code: "MEDALLION_INVALID_OPTIONS",
      });
    }

    const fetchImpl = options.fetch ?? globalThis.fetch?.bind(globalThis);

    if (typeof fetchImpl !== "function") {
      throw new MedallionError(
        "No fetch implementation is available. Use Node.js 18+ or pass a custom fetch.",
        { code: "MEDALLION_FETCH_UNAVAILABLE" },
      );
    }

    this.baseUrl = parsedBaseUrl.toString().replace(/\/+$/, "");
    this.bearerToken = bearerToken;
    this.fetchImpl = fetchImpl;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
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
    const headers = this.defaultHeaders(
      options.idempotencyKey,
      options.headers,
    );
    headers.set("Accept", "application/json");

    let body: string | undefined;
    if (options.body !== undefined) {
      headers.set("Content-Type", "application/json");
      try {
        body = JSON.stringify(options.body);
      } catch (error) {
        throw new MedallionError(
          "Medallion request body must be JSON serializable.",
          { code: "MEDALLION_INVALID_JSON_BODY", cause: error },
        );
      }
    }

    return this.dispatch<TResponse>({
      method: options.method,
      path: options.path,
      query: options.query,
      body,
      headers,
      signal: options.signal,
      timeoutMs: options.timeoutMs,
      parseBody: readResponseBody,
    });
  }

  async requestRaw<TResponse = unknown>(
    options: RawRequestOptions,
  ): Promise<ResponseEnvelope<TResponse>> {
    const headers = this.defaultHeaders(
      options.idempotencyKey,
      options.headers,
    );
    headers.set("Accept", "application/json");

    return this.dispatch<TResponse>({
      method: options.method,
      path: options.path,
      query: options.query,
      body: options.body,
      headers,
      signal: options.signal,
      timeoutMs: options.timeoutMs,
      parseBody: readResponseBody,
    });
  }

  private async dispatch<TResponse>(options: {
    method: RawRequestOptions["method"];
    path: string;
    query?: RawRequestOptions["query"];
    body?: BodyInit;
    headers: Headers;
    signal?: AbortSignal;
    timeoutMs?: number;
    parseBody: (response: Response) => Promise<unknown>;
  }): Promise<ResponseEnvelope<TResponse>> {
    const url = this.buildUrl(options.path, options.query);
    const { signal, didTimeout, cleanup } = createSignal(
      options.signal,
      options.timeoutMs ?? this.timeoutMs,
    );

    try {
      return await traceRequest(this.tracing, {
        method: options.method,
        path: options.path,
        headers: options.headers,
        run: async (span) => {
          const response = await this.fetchImpl(url, {
            method: options.method,
            headers: options.headers,
            body: options.body,
            signal,
          });

          const requestId = readRequestId(response.headers);
          let responseBody: unknown;
          try {
            responseBody = await options.parseBody(response);
          } catch (error) {
            setResponseSpanAttributes(span, response.status, requestId);
            if (error instanceof InvalidJsonResponseError) {
              if (!response.ok) {
                throw new MedallionApiError(
                  buildApiErrorMessage(response.status, requestId, error.body),
                  {
                    status: response.status,
                    requestId,
                    responseBody: error.body,
                  },
                );
              }
              throw new MedallionError("Medallion returned invalid JSON.", {
                code: "MEDALLION_INVALID_JSON_RESPONSE",
                requestId,
                cause: error,
              });
            }
            throw error;
          }
          setResponseSpanAttributes(span, response.status, requestId);

          if (!response.ok) {
            throw new MedallionApiError(
              buildApiErrorMessage(response.status, requestId, responseBody),
              {
                status: response.status,
                requestId,
                responseBody,
              },
            );
          }

          return {
            body: responseBody as TResponse,
            requestId,
          };
        },
      });
    } catch (error) {
      if (error instanceof MedallionError) {
        throw error;
      }

      if (isAbortLikeError(error)) {
        if (didTimeout()) {
          throw new MedallionError(
            `Medallion request timed out after ${options.timeoutMs ?? this.timeoutMs} ms.`,
            { code: "MEDALLION_TIMEOUT", cause: error },
          );
        }

        throw new MedallionError("Medallion request was aborted.", {
          code: "MEDALLION_ABORTED",
          cause: error,
        });
      }

      throw new MedallionError("Medallion request failed.", {
        code: "MEDALLION_NETWORK_ERROR",
        cause: error,
      });
    } finally {
      cleanup();
    }
  }

  private defaultHeaders(
    idempotencyKey: string | undefined,
    extraHeaders: Record<string, string | undefined> | undefined,
  ): Headers {
    const headers = new Headers({
      Authorization: `Bearer ${this.bearerToken}`,
    });

    if (idempotencyKey !== undefined) {
      headers.set("Idempotency-Key", idempotencyKey);
    }

    for (const [key, value] of Object.entries(extraHeaders ?? {})) {
      if (value !== undefined) {
        headers.set(key, value);
      }
    }

    return headers;
  }

  private buildUrl(
    path: string,
    query?: Record<string, string | number | boolean | undefined>,
  ): string {
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;
    const url = new URL(`${this.baseUrl}${normalizedPath}`);

    for (const [key, value] of Object.entries(query ?? {})) {
      if (value !== undefined) {
        url.searchParams.set(key, String(value));
      }
    }

    return url.toString();
  }
}

function createSignal(
  inputSignal: AbortSignal | undefined,
  timeoutMs: number,
): SignalState {
  if (timeoutMs <= 0 && inputSignal === undefined) {
    return {
      didTimeout: () => false,
      cleanup: () => undefined,
    };
  }

  const controller = new AbortController();
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let didTimeout = false;

  const abortFromInput = () => {
    controller.abort(inputSignal?.reason);
  };

  if (inputSignal !== undefined) {
    if (inputSignal.aborted) {
      abortFromInput();
    } else {
      inputSignal.addEventListener("abort", abortFromInput, { once: true });
    }
  }

  if (timeoutMs > 0) {
    timeout = setTimeout(() => {
      didTimeout = true;
      controller.abort(new Error("Medallion request timed out."));
    }, timeoutMs);
  }

  return {
    signal: controller.signal,
    didTimeout: () => didTimeout,
    cleanup: () => {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }

      if (inputSignal !== undefined) {
        inputSignal.removeEventListener("abort", abortFromInput);
      }
    },
  };
}

async function readResponseBody(response: Response): Promise<unknown> {
  if (response.status === 204 || response.status === 205) {
    return undefined;
  }

  const text = await response.text();
  if (text.length === 0) {
    return undefined;
  }

  if (isJsonResponse(response.headers)) {
    try {
      return JSON.parse(text) as unknown;
    } catch (error) {
      throw new InvalidJsonResponseError(text, error);
    }
  }

  return text;
}

class InvalidJsonResponseError extends Error {
  constructor(
    readonly body: string,
    options: unknown,
  ) {
    super("Medallion returned invalid JSON.", { cause: options });
  }
}

function isJsonResponse(headers: Headers): boolean {
  return headers.get("content-type")?.toLowerCase().includes("json") ?? false;
}

function readRequestId(headers: Headers): string | undefined {
  return (
    headers.get("x-request-id") ??
    headers.get("request-id") ??
    headers.get("x-correlation-id") ??
    undefined
  );
}

function buildApiErrorMessage(
  status: number,
  requestId: string | undefined,
  responseBody: unknown,
): string {
  const detail = extractErrorDetail(responseBody);
  const requestPart =
    requestId === undefined ? "" : `, request id ${requestId}`;

  return `${detail ?? "Medallion API request failed"} (HTTP ${status}${requestPart}).`;
}

function extractErrorDetail(responseBody: unknown): string | undefined {
  if (typeof responseBody === "string" && responseBody.length > 0) {
    return responseBody;
  }

  if (responseBody !== null && typeof responseBody === "object") {
    const body = responseBody as Record<string, unknown>;

    if (typeof body.message === "string") {
      return body.message;
    }

    if (typeof body.error === "string") {
      return body.error;
    }

    if (
      body.error !== null &&
      typeof body.error === "object" &&
      typeof (body.error as Record<string, unknown>).message === "string"
    ) {
      return (body.error as Record<string, string>).message;
    }
  }

  return undefined;
}

function isAbortLikeError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error &&
      (error.name === "AbortError" || error.name === "TimeoutError"))
  );
}
