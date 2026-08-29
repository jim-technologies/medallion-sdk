import { MedallionApiError, MedallionClient } from "@jimtech/medallion";

const runId = `example_${Date.now()}`;
const medallion = new MedallionClient({
  baseUrl: requiredEnv("MEDALLION_BASE_URL"),
  apiKey: requiredEnv("MEDALLION_API_KEY"),
  workspaceId: requiredEnv("MEDALLION_WORKSPACE_ID"),
  timeoutMs: 30_000,
});

try {
  // Datasets are named tabular collections in the client's workspace.
  const dataset = await medallion.datasets.create({
    datasetId: "app_events",
    description: "Application events appended by the quickstart",
  });
  console.log("dataset", dataset.datasetId, dataset.createTime);

  // Append plain JSON rows. The SDK sends a Stripe-style Idempotency-Key
  // header automatically and returns it, so the exact batch can be replayed
  // safely after a crash or timeout.
  const appended = await medallion.datasets.append(
    "app_events",
    [
      { run_id: runId, seq: 1, level: "info", message: "service started" },
      { run_id: runId, seq: 2, level: "warn", message: "cache is cold" },
    ],
    { insertIds: [`${runId}:1`, `${runId}:2`] },
  );
  console.log("appended", appended.acceptedRows, appended.idempotencyKey);
  for (const rowError of appended.rowErrors) {
    console.error("rejected row", rowError.index, rowError.reason);
  }

  // Replaying the same batch with the same key is acknowledged as duplicate.
  const replay = await medallion.datasets.append(
    "app_events",
    [
      { run_id: runId, seq: 1, level: "info", message: "service started" },
      { run_id: runId, seq: 2, level: "warn", message: "cache is cold" },
    ],
    { idempotencyKey: appended.idempotencyKey },
  );
  console.log("replayed", replay.duplicate);

  // Estimate first: dry_run validates the ClickHouse SQL and reports cost
  // without executing it.
  const estimate = await medallion.datasets.query(
    "SELECT level, count() AS events FROM app_events GROUP BY level",
    { dryRun: true },
  );
  console.log("estimated bytes", estimate.totalBytesProcessed);

  // Queries are synchronous first; if the server is still running the
  // statement the SDK polls transparently, and iterating the result crosses
  // every page without exposing page tokens.
  const result = await medallion.datasets.query(
    `SELECT run_id, seq, level, message FROM app_events WHERE run_id = '${runId}' ORDER BY seq`,
    { serverTimeoutMs: 10_000 },
  );
  console.log(
    "columns",
    result.columns.map((column) => `${column.name} ${column.type}`),
  );
  for await (const row of result) {
    console.log("row", row);
  }

  for await (const item of medallion.datasets.iterate()) {
    console.log("workspace dataset", item.datasetId);
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
