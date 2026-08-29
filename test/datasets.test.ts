import { Buffer } from "node:buffer";
import { describe, expect, it, vi } from "vitest";
import { MAX_QUERY_POLLS } from "../src/datasets.js";
import { MedallionClient } from "../src/index.js";

const INGEST_SERVICE = "medallion.ingest.v1.MedallionIngestService";
const UUID_TEXT =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function newClient(fetch: typeof globalThis.fetch) {
  return new MedallionClient({
    baseUrl: "https://api.example.com",
    apiKey: "scoped_api_key",
    workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
    fetch,
  });
}

function jsonResponse(
  body: unknown,
  status = 200,
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

function requestBody(init: RequestInit | undefined): Record<string, unknown> {
  return JSON.parse(String(init?.body)) as Record<string, unknown>;
}

describe("datasets.append", () => {
  it("sends JSON rows with an automatic Idempotency-Key header", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ accepted_rows: "2" }, 200, {
          "x-request-id": "req_append_1",
        }),
    );
    const client = newClient(fetch);

    const result = await client.datasets.append(
      "events",
      [
        { level: "info", count: 1 },
        { level: "warn", count: 2 },
      ],
      { insertIds: ["evt-1", undefined] },
    );

    const [url, init] = fetch.mock.calls[0]!;
    expect(url).toBe(`https://api.example.com/${INGEST_SERVICE}/Append`);
    const headers = init?.headers as Headers;
    expect(headers.get("idempotency-key")).toMatch(UUID_TEXT);
    expect(headers.get("x-medallion-api-key")).toBe("scoped_api_key");
    const body = requestBody(init);
    expect(body.datasetId).toBe("events");
    const rows = (body.jsonRows as { rows: Array<Record<string, unknown>> })
      .rows;
    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({
      insertId: "evt-1",
      json: '{"count":1,"level":"info"}',
    });
    expect(rows[1]).toEqual({ json: '{"count":2,"level":"warn"}' });
    expect(result.acceptedRows).toBe(2);
    expect(result.duplicate).toBe(false);
    expect(result.rowErrors).toEqual([]);
    expect(result.idempotencyKey).toMatch(UUID_TEXT);
    expect(result.requestId).toBe("req_append_1");
  });

  it("passes a caller batch key through and reports a replayed batch", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ accepted_rows: 0, duplicate: true }),
    );
    const client = newClient(fetch);

    const result = await client.datasets.append("events", [{ ok: true }], {
      idempotencyKey: "outbox:batch:42",
    });

    const headers = fetch.mock.calls[0]?.[1]?.headers as Headers;
    expect(headers.get("idempotency-key")).toBe("outbox:batch:42");
    expect(result.idempotencyKey).toBe("outbox:batch:42");
    expect(result.duplicate).toBe(true);
    expect(result.acceptedRows).toBe(0);
  });

  it("surfaces per-row errors from the acknowledgement", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          accepted_rows: 1,
          insert_errors: [
            { index: 1, reason: "TYPE_MISMATCH", message: "count is text" },
          ],
        }),
    );
    const client = newClient(fetch);

    const result = await client.datasets.append("events", [
      { count: 1 },
      { count: "oops" },
    ]);

    expect(result.acceptedRows).toBe(1);
    expect(result.rowErrors).toEqual([
      { index: 1, reason: "TYPE_MISMATCH", message: "count is text" },
    ]);
  });

  it("sends an Arrow IPC payload as one base64 record batch", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ accepted_rows: 3 }),
    );
    const client = newClient(fetch);
    const arrow = new Uint8Array([65, 82, 82, 79, 87, 49]);

    const result = await client.datasets.append("events", arrow);

    const body = requestBody(fetch.mock.calls[0]?.[1]);
    expect(body.arrowRows).toEqual({
      serializedRecordBatch: Buffer.from(arrow).toString("base64"),
    });
    expect(body).not.toHaveProperty("jsonRows");
    expect(result.acceptedRows).toBe(3);
  });

  it("rejects malformed batches before any network I/O", async () => {
    const fetch = vi.fn();
    const client = newClient(fetch as never);

    await expect(client.datasets.append("events", [])).rejects.toMatchObject({
      code: "MEDALLION_INVALID_BATCH_SIZE",
    });
    await expect(
      client.datasets.append("events", [[1, 2]] as never),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_ROW" });
    await expect(
      client.datasets.append("events", [{ ok: true }], {
        insertIds: ["a", "b"],
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_ROW" });
    await expect(
      client.datasets.append("events", new Uint8Array()),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_ROW" });
    await expect(
      client.datasets.append("", [{ ok: true }]),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_DATASET_ID" });
    await expect(
      client.datasets.append("events", [{ ok: true }], {
        idempotencyKey: "bad key with spaces",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_IDEMPOTENCY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("rejects acknowledgements that disagree with the submitted batch", async () => {
    const outOfRange = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          accepted_rows: 1,
          insert_errors: [{ index: 7, reason: "TYPE_MISMATCH" }],
        }),
    );
    await expect(
      newClient(outOfRange).datasets.append("events", [{ ok: true }]),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_INGEST_RESPONSE" });

    const overCounted = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ accepted_rows: 5 }),
    );
    await expect(
      newClient(overCounted).datasets.append("events", [{ ok: true }]),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_INGEST_RESPONSE" });
  });
});

describe("datasets.query", () => {
  it("returns parsed rows from a synchronously completed query", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          results: {
            completed: true,
            schema: {
              columns: [
                { name: "level", type: "String" },
                { name: "count", type: "UInt64" },
              ],
            },
            rows_json: ['{"level":"info","count":1}'],
            total_rows: "1",
            total_bytes_processed: "128",
          },
        }),
    );
    const client = newClient(fetch);

    const result = await client.datasets.query(
      "SELECT level, count FROM events",
      { serverTimeoutMs: 2_000, maxResults: 500 },
    );

    const [url, init] = fetch.mock.calls[0]!;
    expect(url).toBe(`https://api.example.com/${INGEST_SERVICE}/Query`);
    const headers = init?.headers as Headers;
    expect(headers.get("idempotency-key")).toBeNull();
    const body = requestBody(init);
    expect(body.query).toBe("SELECT level, count FROM events");
    expect(body.timeoutMs).toBe("2000");
    expect(body.maxResults).toBe(500);
    expect(result.columns).toEqual([
      { name: "level", type: "String" },
      { name: "count", type: "UInt64" },
    ]);
    expect(result.totalRows).toBe(1);
    expect(result.totalBytesProcessed).toBe(128);
    const rows = [];
    for await (const row of result) rows.push(row);
    expect(rows).toEqual([{ level: "info", count: 1 }]);
  });

  it("polls transparently while the query is still running", async () => {
    const fetch = vi
      .fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({}),
      )
      .mockResolvedValueOnce(
        jsonResponse({ results: { completed: false, query_id: "q_1" } }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ results: { completed: false, query_id: "q_1" } }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          results: {
            completed: true,
            query_id: "q_1",
            rows_json: ['{"n":1}'],
            total_rows: 1,
          },
        }),
      );
    const client = newClient(fetch);

    const result = await client.datasets.query("SELECT n FROM events");

    expect(fetch).toHaveBeenCalledTimes(3);
    expect(String(fetch.mock.calls[1]?.[0])).toBe(
      `https://api.example.com/${INGEST_SERVICE}/GetQueryResults`,
    );
    expect(requestBody(fetch.mock.calls[1]?.[1]).queryId).toBe("q_1");
    const rows = [];
    for await (const row of result.rows()) rows.push(row);
    expect(rows).toEqual([{ n: 1 }]);
  });

  it("paginates rows across pages without exposing page tokens", async () => {
    const fetch = vi
      .fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({}),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          results: {
            completed: true,
            query_id: "q_2",
            rows_json: ['{"n":1}', '{"n":2}'],
            next_page_token: "page-2",
            total_rows: 3,
          },
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          results: {
            completed: true,
            query_id: "q_2",
            rows_json: ['{"n":3}'],
          },
        }),
      );
    const client = newClient(fetch);

    const result = await client.datasets.query("SELECT n FROM events");
    const rows = [];
    for await (const row of result) rows.push(row);

    expect(rows).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
    expect(fetch).toHaveBeenCalledTimes(2);
    const pageBody = requestBody(fetch.mock.calls[1]?.[1]);
    expect(pageBody.queryId).toBe("q_2");
    expect(pageBody.pageToken).toBe("page-2");
  });

  it("stops on a repeated continuation token", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          results: {
            completed: true,
            query_id: "q_3",
            rows_json: ['{"n":1}'],
            next_page_token: "loop",
          },
        }),
    );
    const client = newClient(fetch);

    const result = await client.datasets.query("SELECT n FROM events");
    await expect(async () => {
      for await (const _row of result) {
        // drain
      }
    }).rejects.toMatchObject({ code: "MEDALLION_REPEATED_CURSOR" });
  });

  it("rejects a running acknowledgement without a query identifier", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ results: { completed: false } }),
    );
    const client = newClient(fetch);

    await expect(client.datasets.query("SELECT 1")).rejects.toMatchObject({
      code: "MEDALLION_INVALID_INGEST_RESPONSE",
    });
  });

  it("gives up polling after the poll budget", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ results: { completed: false, query_id: "q_slow" } }),
    );
    const client = newClient(fetch);

    await expect(
      client.datasets.query("SELECT sleep(3600)"),
    ).rejects.toMatchObject({ code: "MEDALLION_QUERY_POLL_LIMIT" });
    expect(fetch).toHaveBeenCalledTimes(MAX_QUERY_POLLS + 1);
  });

  it("reports a dry run without fetching rows", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          results: {
            completed: true,
            schema: { columns: [{ name: "n", type: "UInt8" }] },
            total_bytes_processed: "4096",
          },
        }),
    );
    const client = newClient(fetch);

    const result = await client.datasets.query("SELECT n FROM events", {
      dryRun: true,
    });

    expect(requestBody(fetch.mock.calls[0]?.[1]).dryRun).toBe(true);
    expect(result.dryRun).toBe(true);
    expect(result.totalBytesProcessed).toBe(4_096);
    const rows = [];
    for await (const row of result) rows.push(row);
    expect(rows).toEqual([]);
  });

  it("iterates Arrow pages when the arrow format is selected", async () => {
    const batch = Buffer.from("ARROW-IPC-STREAM").toString("base64");
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          results: {
            completed: true,
            arrow_rows: { serialized_record_batch: batch },
          },
        }),
    );
    const client = newClient(fetch);

    const result = await client.datasets.query("SELECT n FROM events", {
      format: "arrow",
    });

    expect(requestBody(fetch.mock.calls[0]?.[1]).format).toBe(
      "RESULT_FORMAT_ARROW_IPC",
    );
    const batches = [];
    for await (const bytes of result.arrowBatches()) batches.push(bytes);
    expect(batches).toHaveLength(1);
    expect(Buffer.from(batches[0]!).toString()).toBe("ARROW-IPC-STREAM");
  });

  it("consumes a result exactly once", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          results: { completed: true, rows_json: ['{"n":1}'] },
        }),
    );
    const client = newClient(fetch);

    const result = await client.datasets.query("SELECT n FROM events");
    for await (const _row of result) {
      // drain
    }
    await expect(async () => {
      for await (const _row of result) {
        // drain again
      }
    }).rejects.toMatchObject({ code: "MEDALLION_RESULT_CONSUMED" });
  });

  it("rejects an empty statement and an unknown format locally", async () => {
    const fetch = vi.fn();
    const client = newClient(fetch as never);

    await expect(client.datasets.query("   ")).rejects.toMatchObject({
      code: "MEDALLION_INVALID_QUERY",
    });
    await expect(
      client.datasets.query("SELECT 1", { format: "csv" as never }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_QUERY" });
    expect(fetch).not.toHaveBeenCalled();
  });
});

describe("datasets management", () => {
  it("creates a dataset with an Idempotency-Key header", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          dataset: {
            dataset_id: "events",
            description: "app events",
            create_time: "2026-08-29T00:00:00Z",
          },
        }),
    );
    const client = newClient(fetch);

    const dataset = await client.datasets.create({
      datasetId: "events",
      description: "app events",
    });

    const [url, init] = fetch.mock.calls[0]!;
    expect(url).toBe(`https://api.example.com/${INGEST_SERVICE}/CreateDataset`);
    expect(
      (init?.headers as Headers | undefined)?.get("idempotency-key"),
    ).toMatch(UUID_TEXT);
    expect(requestBody(init)).toEqual({
      datasetId: "events",
      description: "app events",
    });
    expect(dataset).toEqual({
      datasetId: "events",
      description: "app events",
      createTime: "2026-08-29T00:00:00Z",
    });
  });

  it("reads one dataset and rejects an acknowledgement without one", async () => {
    const fetch = vi
      .fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({}),
      )
      .mockResolvedValueOnce(
        jsonResponse({ dataset: { dataset_id: "events" } }),
      )
      .mockResolvedValueOnce(jsonResponse({}));
    const client = newClient(fetch);

    const dataset = await client.datasets.get("events");
    expect(dataset.datasetId).toBe("events");
    await expect(client.datasets.get("events")).rejects.toMatchObject({
      code: "MEDALLION_INVALID_INGEST_RESPONSE",
    });
  });

  it("iterates datasets across pages", async () => {
    const fetch = vi
      .fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({}),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          datasets: [{ dataset_id: "a" }],
          next_page_token: "p2",
        }),
      )
      .mockResolvedValueOnce(jsonResponse({ datasets: [{ dataset_id: "b" }] }));
    const client = newClient(fetch);

    const names = [];
    for await (const dataset of client.datasets.iterate({ pageSize: 1 })) {
      names.push(dataset.datasetId);
    }

    expect(names).toEqual(["a", "b"]);
    expect(requestBody(fetch.mock.calls[1]?.[1]).pageToken).toBe("p2");
  });
});
