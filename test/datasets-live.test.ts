import { describe, expect, it } from "vitest";
import { MedallionClient } from "../src/index.js";

const requiredEnv = [
  "MEDALLION_SMOKE_BASE_URL",
  "MEDALLION_SMOKE_API_KEY",
  "MEDALLION_SMOKE_WORKSPACE_ID",
  "MEDALLION_SMOKE_INGEST_DATASET",
] as const;

const liveEnabled = requiredEnv.every((name) => process.env[name]?.trim());

describe.skipIf(!liveEnabled)("deployed datasets ingest", () => {
  it("appends rows idempotently and reads them back with SQL", async () => {
    const dataset = requiredEnvValue("MEDALLION_SMOKE_INGEST_DATASET");
    // The dataset name is spliced into SQL below, so restrict it to a safe
    // identifier instead of trusting the environment.
    expect(dataset).toMatch(/^[A-Za-z0-9_]+$/);
    const client = new MedallionClient({
      baseUrl: requiredEnvValue("MEDALLION_SMOKE_BASE_URL"),
      apiKey: requiredEnvValue("MEDALLION_SMOKE_API_KEY"),
      workspaceId: requiredEnvValue("MEDALLION_SMOKE_WORKSPACE_ID"),
      timeoutMs: 30_000,
    });
    const runId = `sdk_live_${Date.now()}`;

    const found = await client.datasets.get(dataset);
    expect(found.datasetId).toBe(dataset);

    const rows = [
      { run_id: runId, seq: 1, level: "info" },
      { run_id: runId, seq: 2, level: "warn" },
    ];
    const appended = await client.datasets.append(dataset, rows);
    expect(appended.rowErrors).toEqual([]);
    expect(appended.acceptedRows).toBe(2);

    const replay = await client.datasets.append(dataset, rows, {
      idempotencyKey: appended.idempotencyKey,
    });
    expect(replay.duplicate).toBe(true);

    const result = await client.datasets.query(
      `SELECT run_id, seq, level FROM ${dataset} WHERE run_id = '${runId}' ORDER BY seq`,
      { serverTimeoutMs: 20_000 },
    );
    const readBack = [];
    for await (const row of result) readBack.push(row);
    expect(readBack).toHaveLength(2);
    expect(readBack[0]).toMatchObject({ run_id: runId, level: "info" });

    const estimate = await client.datasets.query(
      `SELECT count() FROM ${dataset}`,
      { dryRun: true },
    );
    expect(estimate.dryRun).toBe(true);
  }, 120_000);
});

function requiredEnvValue(name: (typeof requiredEnv)[number]): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required for the live ingest test`);
  return value;
}
