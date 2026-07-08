import { describe, expect, it, vi } from "vitest";
import type { Tracer } from "@opentelemetry/api";
import { MedallionApiError, MedallionError } from "../src/errors.js";
import { RequestClient } from "../src/request.js";

describe("RequestClient", () => {
  it("sends JSON, bearer auth, and idempotency headers", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });
    const client = new RequestClient({
      baseUrl: "https://api.example.com/",
      apiKey: "test_api_key",
      fetch,
    });

    await expect(
      client.request({
        method: "POST",
        path: "/v1/query",
        body: { type: "checkout.started" },
        idempotencyKey: "idem_123",
      }),
    ).resolves.toEqual({ ok: true });

    const [url, init] = fetch.mock.calls[0]!;
    const headers = init?.headers as Headers;

    expect(url).toBe("https://api.example.com/v1/query");
    expect(init?.method).toBe("POST");
    expect(headers.get("authorization")).toBe("Bearer test_api_key");
    expect(headers.get("content-type")).toBe("application/json");
    expect(headers.get("idempotency-key")).toBe("idem_123");
    expect(init?.body).toBe(JSON.stringify({ type: "checkout.started" }));
  });

  it("throws API errors with status and request ID", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      return new Response(JSON.stringify({ message: "invalid event" }), {
        status: 400,
        headers: {
          "content-type": "application/json",
          "x-request-id": "req_123",
        },
      });
    });
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      fetch,
    });

    await expect(
      client.request({ method: "POST", path: "/v1/query", body: {} }),
    ).rejects.toMatchObject({
      name: "MedallionApiError",
      status: 400,
      requestId: "req_123",
      responseBody: { message: "invalid event" },
    } satisfies Partial<MedallionApiError>);
  });

  it("rejects blank bearer credentials before sending requests", () => {
    expect(
      () =>
        new RequestClient({
          baseUrl: "https://api.example.com",
          apiKey: "   ",
          fetch: vi.fn(),
        }),
    ).toThrowError(/apiKey or accessToken is required/);
  });

  it("converts fetch aborts caused by timeout into timeout errors", async () => {
    vi.useFakeTimers();

    const fetch = vi.fn((_url: RequestInfo | URL, init?: RequestInit) => {
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("The operation was aborted.", "AbortError"));
        });
      });
    });
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      fetch,
      timeoutMs: 10,
    });

    const request = client.request({ method: "POST", path: "/v1/query" });
    const expectation = expect(request).rejects.toMatchObject({
      name: "MedallionError",
      code: "MEDALLION_TIMEOUT",
    } satisfies Partial<MedallionError>);

    await vi.advanceTimersByTimeAsync(10);
    await expectation;

    vi.useRealTimers();
  });

  it("creates OpenTelemetry spans when tracing is enabled", async () => {
    const spans: Array<{
      name: string;
      attributes: Record<string, unknown>;
      status: unknown;
      ended: boolean;
    }> = [];
    const tracer = {
      startSpan(name: string, options: { attributes?: Record<string, unknown> }) {
        const span = {
          name,
          attributes: { ...options.attributes },
          status: undefined as unknown,
          ended: false,
          setAttribute(key: string, value: unknown) {
            this.attributes[key] = value;
            return this;
          },
          setStatus(status: unknown) {
            this.status = status;
            return this;
          },
          recordException: vi.fn(),
          end() {
            this.ended = true;
          },
          addEvent: vi.fn().mockReturnThis(),
          addLink: vi.fn().mockReturnThis(),
          addLinks: vi.fn().mockReturnThis(),
          isRecording: () => true,
          spanContext: () => ({
            traceId: "0".repeat(32),
            spanId: "0".repeat(16),
            traceFlags: 1,
          }),
          updateName: vi.fn().mockReturnThis(),
        };
        spans.push(span);
        return span;
      },
    } as unknown as Tracer;
    const fetch = vi.fn(async () => {
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "x-request-id": "req_trace",
        },
      });
    });
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      fetch,
      tracing: { enabled: true, tracer, spanPrefix: "test-medallion" },
    });

    await client.request({ method: "POST", path: "/v1/query", body: {} });

    expect(spans).toHaveLength(1);
    expect(spans[0]).toMatchObject({
      name: "test-medallion POST /v1/query",
      ended: true,
    });
    expect(spans[0]!.attributes).toMatchObject({
      "medallion.sdk.language": "typescript",
      "medallion.request.path": "/v1/query",
      "http.request.method": "POST",
      "http.response.status_code": 200,
      "medallion.request_id": "req_trace",
    });
  });
});
