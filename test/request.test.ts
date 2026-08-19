import { propagation, type Tracer } from "@opentelemetry/api";
import { describe, expect, it, vi } from "vitest";
import type { MedallionApiError, MedallionError } from "../src/errors.js";
import { RequestClient } from "../src/request.js";

describe("RequestClient", () => {
  it("sends JSON and API-key headers with redirects disabled", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) => {
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
    );
    const client = new RequestClient({
      baseUrl: "https://api.example.com/",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch,
    });

    await expect(
      client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        body: { type: "checkout.started" },
        connectProtocol: true,
      }),
    ).resolves.toEqual({ ok: true });

    const [url, init] = fetch.mock.calls[0]!;
    const headers = init?.headers as Headers;

    expect(url).toBe(
      "https://api.example.com/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
    );
    expect(init?.method).toBe("POST");
    expect(init?.redirect).toBe("error");
    expect(headers.get("authorization")).toBeNull();
    expect(headers.get("x-medallion-api-key")).toBe("test_api_key");
    expect(headers.get("x-medallion-workspace-id")).toBe(
      "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
    );
    expect(headers.get("connect-protocol-version")).toBe("1");
    expect(headers.get("connect-timeout-ms")).toBe("30000");
    expect(headers.get("content-type")).toBe("application/json");
    expect(headers.get("idempotency-key")).toBeNull();
    expect(init?.body).toBe(JSON.stringify({ type: "checkout.started" }));
  });

  it("fails closed if a custom fetch implementation follows a redirect", async () => {
    const redirected = new Response("{}", {
      status: 200,
      headers: { "content-type": "application/json" },
    });
    Object.defineProperty(redirected, "redirected", { value: true });
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch: vi.fn(async () => redirected),
    });

    await expect(
      client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_REDIRECT_REJECTED" });
  });

  it("throws API errors with status and request ID", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) => {
        return new Response(JSON.stringify({ message: "invalid event" }), {
          status: 400,
          headers: {
            "content-type": "application/json",
            "x-request-id": "req_123",
          },
        });
      },
    );
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch,
    });

    await expect(
      client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        body: {},
      }),
    ).rejects.toMatchObject({
      name: "MedallionApiError",
      message:
        "Medallion API request failed with HTTP 400. Request ID: req_123.",
      status: 400,
      requestId: "req_123",
    } satisfies Partial<MedallionApiError>);
  });

  it("rejects blank bearer credentials before sending requests", () => {
    expect(
      () =>
        new RequestClient({
          baseUrl: "https://api.example.com",
          workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
          apiKey: "   ",
          fetch: vi.fn(),
        }),
    ).toThrowError(/apiKey or accessToken is required/);
  });

  it("rejects malformed credentials without echoing them", () => {
    const secret = "credential\nthat_must_not_escape";
    for (const credential of [{ apiKey: secret }, { accessToken: secret }]) {
      try {
        new RequestClient({
          baseUrl: "https://api.example.com",
          workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
          ...credential,
          fetch: vi.fn(),
        });
        throw new Error("expected invalid credential");
      } catch (error) {
        expect(error).toMatchObject({
          code: "MEDALLION_INVALID_CREDENTIAL",
        });
        expect((error as Error).message).not.toContain(secret);
      }
    }
  });

  it("rejects protected header overrides before network I/O", async () => {
    const fetch = vi.fn();
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch,
    });

    await expect(
      client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        headers: {
          "X-Medallion-Workspace-Id": "ws_01jz9q5g6rsf7r5ar4rah1b2c4",
        },
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_PROTECTED_HEADER" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("requires a canonical configured workspace", () => {
    for (const workspaceId of [
      "",
      "   ",
      "workspace private",
      "工作区",
      "w".repeat(29),
      "ws_01jz9q5g6rsf7r5ar4rah1b2ci",
      " ws_01jz9q5g6rsf7r5ar4rah1b2c3",
    ]) {
      expect(
        () =>
          new RequestClient({
            baseUrl: "https://api.example.com",
            apiKey: "test_api_key",
            workspaceId,
            fetch: vi.fn(),
          }),
      ).toThrowError(
        expect.objectContaining({
          code:
            workspaceId.length === 0
              ? "MEDALLION_MISSING_WORKSPACE_ID"
              : "MEDALLION_INVALID_WORKSPACE_ID",
        }),
      );
    }
  });

  it("rejects a body selector that disagrees with the immutable workspace", async () => {
    const fetch = vi.fn();
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch,
    });

    await expect(
      client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        body: { workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c4" },
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("falls back to a non-blank API key and rejects unsafe base URLs", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        new Response("{}", {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    );
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      accessToken: "   ",
      apiKey: "fallback_key",
      fetch,
    });

    await client.request({
      method: "POST",
      path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
    });
    const headers = fetch.mock.calls[0]?.[1]?.headers as Headers;
    expect(headers.get("authorization")).toBeNull();
    expect(headers.get("x-medallion-api-key")).toBe("fallback_key");

    expect(
      () =>
        new RequestClient({
          baseUrl: "https://user:secret@api.example.com?unexpected=true",
          workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
          apiKey: "test_api_key",
          fetch,
        }),
    ).toThrowError(/origin without credentials, path, query, or fragment/);

    const pathFetch = vi.fn();
    expect(
      () =>
        new RequestClient({
          baseUrl: "https://api.example.com/connect",
          workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
          apiKey: "test_api_key",
          fetch: pathFetch,
        }),
    ).toThrowError(/origin without credentials, path, query, or fragment/);
    expect(pathFetch).not.toHaveBeenCalled();

    const plaintextFetch = vi.fn();
    expect(
      () =>
        new RequestClient({
          baseUrl: "http://api.example.com",
          workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
          apiKey: "test_api_key",
          fetch: plaintextFetch,
        }),
    ).toThrowError(/plaintext HTTP is allowed only for loopback hosts/);
    expect(plaintextFetch).not.toHaveBeenCalled();
  });

  it("rejects raw base URLs that normalize into a different origin syntax", () => {
    const fetch = vi.fn();
    for (const baseUrl of [
      "https://api.example.com\n",
      "https://api.example.com\t/",
      "https://api.example.com?",
      "https://api.example.com#",
      "https://api.example.com/?",
      "https://api.example.com/#",
      "https://api.example.com/.",
      "https://api.example.com/..",
    ]) {
      expect(
        () =>
          new RequestClient({
            baseUrl,
            workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
            apiKey: "test_api_key",
            fetch,
          }),
      ).toThrowError(
        expect.objectContaining({ code: "MEDALLION_INVALID_OPTIONS" }),
      );
    }

    expect(
      () =>
        new RequestClient({
          baseUrl: "  https://api.example.com:443/  ",
          workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
          apiKey: "test_api_key",
          fetch,
        }),
    ).not.toThrow();
    expect(fetch).not.toHaveBeenCalled();
  });

  it("disables redirects for bearer-token requests", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        new Response("{}", {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    );
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      accessToken: "test_access_token",
      fetch,
    });

    await client.request({
      method: "POST",
      path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
    });
    const init = fetch.mock.calls[0]?.[1];
    const headers = init?.headers as Headers;
    expect(init?.redirect).toBe("error");
    expect(headers.get("authorization")).toBe("Bearer test_access_token");
    expect(headers.get("x-medallion-api-key")).toBeNull();
  });

  it("classifies malformed success and error JSON consistently", async () => {
    const successClient = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch: vi.fn(
        async () =>
          new Response("{", {
            status: 200,
            headers: {
              "content-type": "application/json",
              "x-request-id": "req_bad_success",
            },
          }),
      ),
    });

    await expect(
      successClient.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_INVALID_JSON_RESPONSE",
      requestId: "req_bad_success",
    });

    const errorClient = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch: vi.fn(
        async () =>
          new Response("not-json", {
            status: 502,
            headers: {
              "content-type": "application/json",
              "x-request-id": "req_bad_error",
            },
          }),
      ),
    });

    await expect(
      errorClient.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
      }),
    ).rejects.toMatchObject({
      name: "MedallionApiError",
      status: 502,
      requestId: "req_bad_error",
    });
  });

  it("uses the JSON request codec regardless of response media-type hints", async () => {
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch: vi.fn(
        async () =>
          new Response('{"ok":true}', {
            status: 200,
            headers: { "content-type": "application/octet-stream" },
          }),
      ),
    });

    await expect(
      client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        body: {},
      }),
    ).resolves.toEqual({ ok: true });
  });

  it("rejects responses whose declared length exceeds the safety limit", async () => {
    const fetch = vi.fn(
      async () =>
        new Response("{}", {
          status: 200,
          headers: {
            "content-length": String(64 * 1024 * 1024 + 1),
            "content-type": "application/json",
            "x-request-id": "req_declared_too_large",
          },
        }),
    );
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch,
    });

    await expect(
      client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_RESPONSE_TOO_LARGE",
      requestId: "req_declared_too_large",
    } satisfies Partial<MedallionError>);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("stops reading an error response that streams past the safety limit", async () => {
    const chunk = new Uint8Array(1024 * 1024);
    let chunksSent = 0;
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        chunksSent += 1;
        controller.enqueue(chunk);
      },
      cancel() {
        cancelled = true;
      },
    });
    const fetch = vi.fn(
      async () =>
        new Response(body, {
          status: 503,
          headers: {
            "content-type": "application/json",
            "x-request-id": "req_stream_too_large",
          },
        }),
    );
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch,
    });

    await expect(
      client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_RESPONSE_TOO_LARGE",
      requestId: "req_stream_too_large",
    } satisfies Partial<MedallionError>);
    // ReadableStream may keep one chunk queued ahead of the active read.
    expect(chunksSent).toBeGreaterThanOrEqual(65);
    expect(chunksSent).toBeLessThanOrEqual(66);
    expect(cancelled).toBe(true);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("retries response-stream failures only for retry-safe calls", async () => {
    const failedResponse = (status = 200) => {
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.error(new Error("mid-response disconnect"));
        },
      });
      return new Response(body, {
        status,
        headers: {
          "content-type": "application/json",
          "x-request-id": "req_read_failure",
        },
      });
    };
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(failedResponse())
      .mockResolvedValueOnce(
        new Response('{"ok":true}', {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      );
    const retrying = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch,
      retry: {
        maxAttempts: 2,
        initialDelayMs: 0,
        maxDelayMs: 0,
        jitterRatio: 0,
      },
    });

    await expect(
      retrying.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        retrySafe: true,
      }),
    ).resolves.toEqual({ ok: true });
    expect(fetch).toHaveBeenCalledTimes(2);

    const unsafeFetch = vi.fn(async () => failedResponse());
    const unsafe = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch: unsafeFetch,
      retry: { maxAttempts: 2 },
    });
    await expect(
      unsafe.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_NETWORK_ERROR",
      requestId: "req_read_failure",
    } satisfies Partial<MedallionError>);
    expect(unsafeFetch).toHaveBeenCalledTimes(1);

    const terminalFetch = vi.fn(async () => failedResponse(400));
    const terminal = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch: terminalFetch,
      retry: { maxAttempts: 5 },
    });
    await expect(
      terminal.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        retrySafe: true,
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_NETWORK_ERROR",
      requestId: "req_read_failure",
    } satisfies Partial<MedallionError>);
    expect(terminalFetch).toHaveBeenCalledTimes(1);
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
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "test_api_key",
      fetch,
      timeoutMs: 10,
    });

    const request = client.request({
      method: "POST",
      path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
    });
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
      startSpan(
        name: string,
        options: { attributes?: Record<string, unknown> },
      ) {
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
    let outgoingHeaders: Headers | undefined;
    const fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      outgoingHeaders = init?.headers as Headers;
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
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "trace_secret_api_key",
      fetch,
      tracing: { enabled: true, tracer, spanPrefix: "test-medallion" },
    });

    propagation.disable();
    propagation.setGlobalPropagator({
      inject(
        _context: unknown,
        carrier: unknown,
        setter: {
          set(target: unknown, key: string, value: string): void;
        },
      ) {
        setter.set(carrier, "authorization", "Bearer injected_attacker");
        setter.set(
          carrier,
          "x-medallion-workspace-id",
          "ws_01jz9q5g6rsf7r5ar4rah1b2c4",
        );
        setter.set(carrier, "content-type", "text/plain");
        setter.set(carrier, "x-untrusted-propagation", "blocked");
        setter.set(
          carrier,
          "traceparent",
          `00-${"1".repeat(32)}-${"2".repeat(16)}-01`,
        );
      },
      extract(context: unknown) {
        return context;
      },
      fields() {
        return [];
      },
    } as never);
    try {
      await client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        body: { sensitive: "trace_secret_payload" },
      });
    } finally {
      propagation.disable();
    }

    expect(spans).toHaveLength(1);
    expect(spans[0]).toMatchObject({
      name: "test-medallion POST /medallion.connect.v1.MedallionConnectService/ListCdcEvents",
      ended: true,
    });
    expect(spans[0]!.attributes).toMatchObject({
      "medallion.sdk.language": "typescript",
      "medallion.request.path":
        "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
      "http.request.method": "POST",
      "http.response.status_code": 200,
      "medallion.request_id": "req_trace",
    });
    expect(JSON.stringify(spans)).not.toContain("trace_secret_api_key");
    expect(JSON.stringify(spans)).not.toContain("trace_secret_payload");
    expect(outgoingHeaders?.get("authorization")).toBeNull();
    expect(outgoingHeaders?.get("x-medallion-api-key")).toBe(
      "trace_secret_api_key",
    );
    expect(outgoingHeaders?.get("x-medallion-workspace-id")).toBe(
      "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
    );
    expect(outgoingHeaders?.get("content-type")).toBe("application/json");
    expect(outgoingHeaders?.get("x-untrusted-propagation")).toBeNull();
    expect(outgoingHeaders?.get("traceparent")).toBe(
      `00-${"1".repeat(32)}-${"2".repeat(16)}-01`,
    );
  });

  it("never records arbitrary server error messages in tracing", async () => {
    const secret = "customer_payload_that_must_not_escape";
    const exceptions: unknown[] = [];
    const statuses: unknown[] = [];
    const tracer = {
      startSpan() {
        return {
          setAttribute: vi.fn().mockReturnThis(),
          setStatus(status: unknown) {
            statuses.push(status);
            return this;
          },
          recordException(error: unknown) {
            exceptions.push(error);
          },
          end: vi.fn(),
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
      },
    } as unknown as Tracer;
    const client = new RequestClient({
      baseUrl: "https://api.example.com",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      apiKey: "trace_api_key",
      tracing: { enabled: true, tracer },
      fetch: vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              code: "invalid_argument",
              message: `Invalid payload ${secret}`,
            }),
            {
              status: 400,
              headers: { "content-type": "application/json" },
            },
          ),
      ),
    });

    let thrown: unknown;
    try {
      await client.request({
        method: "POST",
        path: "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
        body: { private: secret },
      });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toMatchObject({
      name: "MedallionApiError",
      message: "Medallion API request failed with HTTP 400.",
    });
    expect(thrown).toBeInstanceOf(Error);
    const thrownError = thrown as Error & { cause?: unknown };
    expect(thrownError.message).not.toContain(secret);
    const causeMessage =
      thrownError.cause instanceof Error ? thrownError.cause.message : "";
    expect(causeMessage).not.toContain(secret);

    expect(exceptions).toEqual([
      {
        name: "MedallionApiError",
        message: "Medallion request failed.",
      },
    ]);
    expect(statuses.at(-1)).toEqual({ code: 2 });
    expect(JSON.stringify({ exceptions, statuses })).not.toContain(secret);
  });
});
