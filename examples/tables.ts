import { MedallionApiError, MedallionClient } from "@jimtech/medallion";

const runId = `example_${Date.now()}`;
const medallion = new MedallionClient({
  baseUrl: requiredEnv("MEDALLION_BASE_URL"),
  apiKey: requiredEnv("MEDALLION_API_KEY"),
  workspaceId: requiredEnv("MEDALLION_WORKSPACE_ID"),
  timeoutMs: 30_000,
});

try {
  // A table is one declared tabular collection in the client's workspace: an
  // ordered schema, a TIMESTAMP column carrying event time, and an optional
  // sort key. Re-declaring the same table is a safe retry.
  const table = await medallion.tables.create({
    tableId: "app_events",
    columns: [
      { name: "occurred_at", type: "TIMESTAMP" },
      { name: "run_id", type: "STRING" },
      { name: "seq", type: "INT64" },
      { name: "level", type: "STRING" },
      { name: "message", type: "STRING", nullable: true },
    ],
    timeColumn: "occurred_at",
  });
  console.log("table", table.tableId, table.createTime);

  // Schema evolution is additive only: resend the full desired schema with the
  // existing columns unchanged, then the new nullable ones.
  const evolved = await medallion.tables.update({
    tableId: "app_events",
    columns: [
      ...table.columns,
      { name: "trace_id", type: "STRING", nullable: true },
    ],
  });
  console.log(
    "columns",
    evolved.columns.map((column) => column.name),
  );

  // Append plain JSON rows. The SDK generates a batch idempotency key, sends
  // it as both the Idempotency-Key header and the request's request_id, and
  // returns it so the exact batch can be replayed safely after a crash or
  // timeout.
  const rows = [
    {
      occurred_at: new Date().toISOString(),
      run_id: runId,
      seq: 1,
      level: "info",
      message: "service started",
    },
    {
      occurred_at: new Date().toISOString(),
      run_id: runId,
      seq: 2,
      level: "warn",
      message: "cache is cold",
    },
  ];
  const appended = await medallion.tables.append("app_events", rows, {
    insertIds: [`${runId}:1`, `${runId}:2`],
  });
  console.log("appended", appended.acceptedRows, appended.idempotencyKey);
  for (const rowError of appended.rowErrors) {
    console.error("rejected row", rowError.index, rowError.message);
  }

  // Replaying the same batch under the same key is absorbed without
  // duplication and re-acknowledged with the original counts.
  const replay = await medallion.tables.append("app_events", rows, {
    idempotencyKey: appended.idempotencyKey,
  });
  console.log("replayed", replay.acceptedRows);

  // Plan first: dryRun validates the ClickHouse SQL and reports the result
  // schema without executing it.
  const planned = await medallion.tables.query(
    "SELECT level, count() AS events FROM app_events GROUP BY level",
    { dryRun: true },
  );
  console.log(
    "planned columns",
    planned.columns.map((column) => `${column.name} ${column.type}`),
  );

  // Queries are synchronous first; if the server is still running the
  // statement the SDK polls transparently, and iterating the result crosses
  // every page without exposing page tokens.
  const result = await medallion.tables.query(
    `SELECT run_id, seq, level, message FROM app_events WHERE run_id = '${runId}' ORDER BY seq`,
    { serverTimeoutMs: 10_000 },
  );
  console.log("total rows", result.totalRows);
  for await (const row of result) {
    console.log("row", row);
  }

  for await (const item of medallion.tables.iterate()) {
    console.log("workspace table", item.tableId);
  }
} catch (error) {
  if (error instanceof MedallionApiError) {
    console.error({
      connectCode: error.connectCode,
      reason: error.errorInfoReason,
      requestId: error.requestId,
    });
  }
  throw error;
}

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required.`);
  return value;
}
