import { describe, expect, it } from "vitest";
import { MedallionClient } from "../src/index.js";

const requiredEnv = [
  "MEDALLION_SMOKE_BASE_URL",
  "MEDALLION_SMOKE_API_KEY",
  "MEDALLION_SMOKE_WORKSPACE_ID",
  "MEDALLION_SMOKE_INGEST_TABLE",
] as const;

const liveEnabled = requiredEnv.every((name) => process.env[name]?.trim());

describe.skipIf(!liveEnabled)("deployed tables ingest", () => {
  it("appends rows idempotently and reads them back with SQL", async () => {
    const table = requiredEnvValue("MEDALLION_SMOKE_INGEST_TABLE");
    // The table name is spliced into SQL below, so restrict it to a safe
    // identifier instead of trusting the environment.
    expect(table).toMatch(/^[a-z][a-z0-9_]*$/);
    const client = new MedallionClient({
      baseUrl: requiredEnvValue("MEDALLION_SMOKE_BASE_URL"),
      apiKey: requiredEnvValue("MEDALLION_SMOKE_API_KEY"),
      workspaceId: requiredEnvValue("MEDALLION_SMOKE_WORKSPACE_ID"),
      timeoutMs: 30_000,
    });
    const runId = `sdk_live_${Date.now()}`;
    const occurredAt = new Date().toISOString();

    const found = await client.tables.get(table);
    expect(found.tableId).toBe(table);

    const rows = [
      { occurred_at: occurredAt, run_id: runId, seq: 1, level: "info" },
      { occurred_at: occurredAt, run_id: runId, seq: 2, level: "warn" },
    ];
    const appended = await client.tables.append(table, rows);
    expect(appended.rowErrors).toEqual([]);
    expect(appended.acceptedRows).toBe(2);

    // Replaying the exact batch under the same key is absorbed without
    // duplication and re-acknowledged with the original counts.
    const replay = await client.tables.append(table, rows, {
      idempotencyKey: appended.idempotencyKey,
    });
    expect(replay.acceptedRows).toBe(2);

    const result = await client.tables.query(
      `SELECT run_id, seq, level FROM ${table} WHERE run_id = '${runId}' ORDER BY seq`,
      { serverTimeoutMs: 20_000 },
    );
    const readBack = [];
    for await (const row of result) readBack.push(row);
    expect(readBack).toHaveLength(2);
    expect(readBack[0]).toMatchObject({ run_id: runId, level: "info" });

    const planned = await client.tables.query(`SELECT count() FROM ${table}`, {
      dryRun: true,
    });
    expect(planned.dryRun).toBe(true);
    expect(planned.columns.length).toBeGreaterThan(0);
  }, 120_000);
});

function requiredEnvValue(name: (typeof requiredEnv)[number]): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required for the live ingest test`);
  return value;
}
