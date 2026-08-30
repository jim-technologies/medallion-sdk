import { Buffer } from "node:buffer";
import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";
import { MAX_QUERY_POLLS } from "../src/tables.js";

const INGEST_SERVICE = "medallion.ingest.v1.MedallionIngestService";
const UUID_TEXT =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const QUERY_NAME = "queries/01jz9q5g6rsf7r5ar4rah1b2c3";

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

describe("tables.append", () => {
  it("sends JSON rows with an automatic batch idempotency key", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ acceptedRows: "2" }, 200, {
          "x-request-id": "req_append_1",
        }),
    );
    const client = newClient(fetch);

    const result = await client.tables.append(
      "events",
      [
        { level: "info", count: 1 },
        { level: "warn", count: 2 },
      ],
      { insertIds: ["evt-1", undefined] },
    );

    const [url, init] = fetch.mock.calls[0]!;
    expect(url).toBe(`https://api.example.com/${INGEST_SERVICE}/AppendRows`);
    const headers = init?.headers as Headers;
    expect(headers.get("idempotency-key")).toMatch(UUID_TEXT);
    expect(headers.get("x-medallion-api-key")).toBe("scoped_api_key");
    const body = requestBody(init);
    expect(body.table).toBe("tables/events");
    // The contract deduplicates on request_id, so the batch key is stamped
    // into the body as well as the header.
    expect(body.requestId).toMatch(UUID_TEXT);
    const rows = body.rows as Array<Record<string, unknown>>;
    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({
      insertId: "evt-1",
      json: { count: 1, level: "info" },
    });
    expect(rows[1]).toEqual({ json: { count: 2, level: "warn" } });
    expect(result.acceptedRows).toBe(2);
    expect(result.rowErrors).toEqual([]);
    expect(result.idempotencyKey).toMatch(UUID_TEXT);
    expect(result.requestId).toBe("req_append_1");
  });

  it("passes a caller batch key through as header and request_id", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ acceptedRows: "1" }),
    );
    const client = newClient(fetch);

    const result = await client.tables.append("events", [{ ok: true }], {
      idempotencyKey: "outbox:batch:42",
    });

    const [, init] = fetch.mock.calls[0]!;
    const headers = init?.headers as Headers;
    expect(headers.get("idempotency-key")).toBe("outbox:batch:42");
    expect(requestBody(init).requestId).toBe("outbox:batch:42");
    expect(result.idempotencyKey).toBe("outbox:batch:42");
    expect(result.acceptedRows).toBe(1);
  });

  it("surfaces per-row errors and the skip-invalid-rows flag", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          acceptedRows: "1",
          rowErrors: [
            {
              index: "1",
              error: { code: 3, message: "count expects an integer" },
            },
          ],
        }),
    );
    const client = newClient(fetch);

    const result = await client.tables.append(
      "events",
      [{ count: 1 }, { count: "oops" }],
      { skipInvalidRows: true },
    );

    expect(requestBody(fetch.mock.calls[0]?.[1]).skipInvalidRows).toBe(true);
    expect(result.acceptedRows).toBe(1);
    expect(result.rowErrors).toEqual([
      { index: 1, code: 3, message: "count expects an integer" },
    ]);
  });

  it("sends an Arrow IPC payload as one base64 record batch", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ acceptedRows: "3" }),
    );
    const client = newClient(fetch);
    const arrow = new Uint8Array([65, 82, 82, 79, 87, 49]);

    const result = await client.tables.append("events", arrow);

    const body = requestBody(fetch.mock.calls[0]?.[1]);
    expect(body.arrowRows).toEqual({
      serializedRecordBatch: Buffer.from(arrow).toString("base64"),
    });
    expect(body).not.toHaveProperty("rows");
    expect(result.acceptedRows).toBe(3);
  });

  it("rejects malformed batches before any network I/O", async () => {
    const fetch = vi.fn();
    const client = newClient(fetch as never);

    await expect(client.tables.append("events", [])).rejects.toMatchObject({
      code: "MEDALLION_INVALID_BATCH_SIZE",
    });
    await expect(
      client.tables.append("events", [[1, 2]] as never),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_ROW" });
    await expect(
      client.tables.append("events", [{ ok: true }], {
        insertIds: ["a", "b"],
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_ROW" });
    await expect(
      client.tables.append("events", new Uint8Array()),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_ROW" });
    await expect(
      client.tables.append("Events", [{ ok: true }]),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_TABLE_ID" });
    await expect(
      client.tables.append("events", [{ ok: true }], {
        idempotencyKey: "bad key with spaces",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_IDEMPOTENCY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("rejects acknowledgements that disagree with the submitted batch", async () => {
    const outOfRange = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          acceptedRows: "1",
          rowErrors: [{ index: "7", error: { code: 3 } }],
        }),
    );
    await expect(
      newClient(outOfRange).tables.append("events", [{ ok: true }]),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_INGEST_RESPONSE" });

    const overCounted = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ acceptedRows: "5" }),
    );
    await expect(
      newClient(overCounted).tables.append("events", [{ ok: true }]),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_INGEST_RESPONSE" });
  });
});

describe("tables.query", () => {
  it("returns rows from a synchronously succeeded query", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          name: QUERY_NAME,
          state: "SUCCEEDED",
          schema: {
            columns: [
              { name: "level", type: "STRING" },
              { name: "count", type: "INT64", nullable: true },
            ],
          },
          rows: [{ level: "info", count: 1 }],
          totalRows: "1",
        }),
    );
    const client = newClient(fetch);

    const result = await client.tables.query(
      "SELECT level, count FROM events",
      { serverTimeoutMs: 2_000, pageSize: 500 },
    );

    const [url, init] = fetch.mock.calls[0]!;
    expect(url).toBe(`https://api.example.com/${INGEST_SERVICE}/RunQuery`);
    const headers = init?.headers as Headers;
    expect(headers.get("idempotency-key")).toBeNull();
    const body = requestBody(init);
    expect(body.query).toBe("SELECT level, count FROM events");
    expect(body.timeoutMs).toBe("2000");
    expect(body.pageSize).toBe(500);
    expect(result.queryName).toBe(QUERY_NAME);
    expect(result.columns).toEqual([
      { name: "level", type: "STRING", nullable: false },
      { name: "count", type: "INT64", nullable: true },
    ]);
    expect(result.totalRows).toBe(1);
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
        jsonResponse({ name: QUERY_NAME, state: "RUNNING" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ name: QUERY_NAME, state: "RUNNING" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          name: QUERY_NAME,
          state: "SUCCEEDED",
          rows: [{ n: 1 }],
          totalRows: "1",
        }),
      );
    const client = newClient(fetch);

    const result = await client.tables.query("SELECT n FROM events");

    expect(fetch).toHaveBeenCalledTimes(3);
    expect(String(fetch.mock.calls[1]?.[0])).toBe(
      `https://api.example.com/${INGEST_SERVICE}/GetQueryResults`,
    );
    expect(requestBody(fetch.mock.calls[1]?.[1]).name).toBe(QUERY_NAME);
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
          name: QUERY_NAME,
          state: "SUCCEEDED",
          rows: [{ n: 1 }, { n: 2 }],
          nextPageToken: "page-2",
          totalRows: "3",
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          name: QUERY_NAME,
          state: "SUCCEEDED",
          rows: [{ n: 3 }],
        }),
      );
    const client = newClient(fetch);

    const result = await client.tables.query("SELECT n FROM events");
    const rows = [];
    for await (const row of result) rows.push(row);

    expect(rows).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
    expect(fetch).toHaveBeenCalledTimes(2);
    const pageBody = requestBody(fetch.mock.calls[1]?.[1]);
    expect(pageBody.name).toBe(QUERY_NAME);
    expect(pageBody.pageToken).toBe("page-2");
  });

  it("stops on a repeated continuation token", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          name: QUERY_NAME,
          state: "SUCCEEDED",
          rows: [{ n: 1 }],
          nextPageToken: "loop",
        }),
    );
    const client = newClient(fetch);

    const result = await client.tables.query("SELECT n FROM events");
    await expect(async () => {
      for await (const _row of result) {
        // drain
      }
    }).rejects.toMatchObject({ code: "MEDALLION_REPEATED_CURSOR" });
  });

  it("rejects a running acknowledgement without a resource name", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ state: "RUNNING" }),
    );
    const client = newClient(fetch);

    await expect(client.tables.query("SELECT 1")).rejects.toMatchObject({
      code: "MEDALLION_INVALID_INGEST_RESPONSE",
    });
  });

  it("raises the failure cause of a failed query", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          name: QUERY_NAME,
          state: "FAILED",
          error: { code: 13, message: "query was abandoned by its executor" },
        }),
    );
    const client = newClient(fetch);

    await expect(
      client.tables.query("SELECT sleep(3600)"),
    ).rejects.toMatchObject({
      code: "MEDALLION_QUERY_FAILED",
      message: "query was abandoned by its executor",
    });
  });

  it("gives up polling after the poll budget", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ name: QUERY_NAME, state: "RUNNING" }),
    );
    const client = newClient(fetch);

    await expect(
      client.tables.query("SELECT sleep(3600)"),
    ).rejects.toMatchObject({ code: "MEDALLION_QUERY_POLL_LIMIT" });
    expect(fetch).toHaveBeenCalledTimes(MAX_QUERY_POLLS + 1);
  });

  it("reports a dry run, which carries a schema and no resource name", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          state: "SUCCEEDED",
          schema: { columns: [{ name: "n", type: "INT64" }] },
        }),
    );
    const client = newClient(fetch);

    const result = await client.tables.query("SELECT n FROM events", {
      dryRun: true,
    });

    expect(requestBody(fetch.mock.calls[0]?.[1]).dryRun).toBe(true);
    expect(result.dryRun).toBe(true);
    expect(result.queryName).toBeUndefined();
    expect(result.columns).toEqual([
      { name: "n", type: "INT64", nullable: false },
    ]);
    const rows = [];
    for await (const row of result) rows.push(row);
    expect(rows).toEqual([]);
  });

  it("consumes a result exactly once", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          name: QUERY_NAME,
          state: "SUCCEEDED",
          rows: [{ n: 1 }],
        }),
    );
    const client = newClient(fetch);

    const result = await client.tables.query("SELECT n FROM events");
    for await (const _row of result) {
      // drain
    }
    await expect(async () => {
      for await (const _row of result) {
        // drain again
      }
    }).rejects.toMatchObject({ code: "MEDALLION_RESULT_CONSUMED" });
  });

  it("rejects an empty statement and an out-of-range page size locally", async () => {
    const fetch = vi.fn();
    const client = newClient(fetch as never);

    await expect(client.tables.query("   ")).rejects.toMatchObject({
      code: "MEDALLION_INVALID_QUERY",
    });
    await expect(
      client.tables.query("SELECT 1", { pageSize: 200_000 }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_PAGE_SIZE" });
    await expect(
      client.tables.query("SELECT 1", { serverTimeoutMs: 900_000 }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_TIMEOUT" });
    expect(fetch).not.toHaveBeenCalled();
  });
});

describe("table management", () => {
  it("declares a table with a schema and a batch idempotency key", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          table: {
            name: "tables/events",
            schema: {
              columns: [
                { name: "occurred_at", type: "TIMESTAMP" },
                { name: "level", type: "STRING" },
              ],
            },
            timeColumn: "occurred_at",
            sortColumns: ["occurred_at"],
            createTime: "2026-08-29T00:00:00Z",
          },
        }),
    );
    const client = newClient(fetch);

    const table = await client.tables.create({
      tableId: "events",
      columns: [
        { name: "occurred_at", type: "TIMESTAMP" },
        { name: "level", type: "STRING" },
      ],
      timeColumn: "occurred_at",
    });

    const [url, init] = fetch.mock.calls[0]!;
    expect(url).toBe(`https://api.example.com/${INGEST_SERVICE}/CreateTable`);
    expect(
      (init?.headers as Headers | undefined)?.get("idempotency-key"),
    ).toMatch(UUID_TEXT);
    const body = requestBody(init);
    expect(body.tableId).toBe("events");
    expect(body.requestId).toMatch(UUID_TEXT);
    expect(body.table).toEqual({
      schema: {
        columns: [
          { name: "occurred_at", type: "TIMESTAMP" },
          { name: "level", type: "STRING" },
        ],
      },
      timeColumn: "occurred_at",
    });
    expect(table).toEqual({
      tableId: "events",
      name: "tables/events",
      columns: [
        { name: "occurred_at", type: "TIMESTAMP", nullable: false },
        { name: "level", type: "STRING", nullable: false },
      ],
      timeColumn: "occurred_at",
      sortColumns: ["occurred_at"],
      createTime: "2026-08-29T00:00:00Z",
    });
  });

  it("evolves a schema additively with the full desired column list", async () => {
    const columns = [
      { name: "occurred_at", type: "TIMESTAMP" as const },
      { name: "level", type: "STRING" as const },
      { name: "trace_id", type: "STRING" as const, nullable: true },
    ];
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({
          table: {
            name: "tables/events",
            schema: { columns },
            timeColumn: "occurred_at",
          },
        }),
    );
    const client = newClient(fetch);

    const table = await client.tables.update({ tableId: "events", columns });

    const [url, init] = fetch.mock.calls[0]!;
    expect(url).toBe(`https://api.example.com/${INGEST_SERVICE}/UpdateTable`);
    const body = requestBody(init);
    expect((body.table as Record<string, unknown>).name).toBe("tables/events");
    expect(
      (body.table as { schema: { columns: unknown[] } }).schema.columns.at(-1),
    ).toEqual({ name: "trace_id", type: "STRING", nullable: true });
    expect(table.columns.at(-1)).toEqual({
      name: "trace_id",
      type: "STRING",
      nullable: true,
    });
  });

  it("rejects an undeclared schema before any network I/O", async () => {
    const fetch = vi.fn();
    const client = newClient(fetch as never);

    await expect(
      client.tables.create({
        tableId: "events",
        columns: [],
        timeColumn: "occurred_at",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_SCHEMA" });
    await expect(
      client.tables.create({
        tableId: "events",
        columns: [{ name: "occurred_at", type: "DATETIME" }],
        timeColumn: "occurred_at",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_SCHEMA" });
    await expect(
      client.tables.create({
        tableId: "events",
        columns: [{ name: "occurred_at", type: "TIMESTAMP" }],
        timeColumn: "missing",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_SCHEMA" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("reads one table and rejects an acknowledgement without one", async () => {
    const fetch = vi
      .fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({}),
      )
      .mockResolvedValueOnce(jsonResponse({ table: { name: "tables/events" } }))
      .mockResolvedValueOnce(jsonResponse({}));
    const client = newClient(fetch);

    const table = await client.tables.get("events");
    expect(table.tableId).toBe("events");
    expect(requestBody(fetch.mock.calls[0]?.[1]).name).toBe("tables/events");
    await expect(client.tables.get("events")).rejects.toMatchObject({
      code: "MEDALLION_INVALID_INGEST_RESPONSE",
    });
  });

  it("iterates tables across pages", async () => {
    const fetch = vi
      .fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({}),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          tables: [{ name: "tables/a" }],
          nextPageToken: "p2",
        }),
      )
      .mockResolvedValueOnce(jsonResponse({ tables: [{ name: "tables/b" }] }));
    const client = newClient(fetch);

    const names = [];
    for await (const table of client.tables.iterate({ pageSize: 1 })) {
      names.push(table.tableId);
    }

    expect(names).toEqual(["a", "b"]);
    expect(String(fetch.mock.calls[0]?.[0])).toBe(
      `https://api.example.com/${INGEST_SERVICE}/ListTables`,
    );
    expect(requestBody(fetch.mock.calls[1]?.[1]).pageToken).toBe("p2");
  });
});
