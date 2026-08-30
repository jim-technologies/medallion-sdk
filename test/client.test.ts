import { inspect } from "node:util";
import { describe, expect, it, vi } from "vitest";
import { MedallionClient, ProtocolConnectClient } from "../src/index.js";

describe("MedallionClient", () => {
  it("exposes only the bounded customer-ingestion clients", () => {
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch: vi.fn(),
    });

    expect(Object.keys(client).sort()).toEqual([
      "audit",
      "cdc",
      "connect",
      "ingest",
      "tables",
    ]);
    expect(client.audit.record).toBeTypeOf("function");
    expect(client.cdc.record).toBeTypeOf("function");
    expect(client.tables.append).toBeTypeOf("function");
    expect(client.tables.query).toBeTypeOf("function");
    expect(Object.getOwnPropertyNames(ProtocolConnectClient.prototype)).toEqual(
      [
        "constructor",
        "publishCdcEvents",
        "listCdcEvents",
        "publishAuditEvents",
        "listAuditEvents",
      ],
    );
  });

  it("requires a canonical immutable workspace before network I/O", () => {
    const fetch = vi.fn();

    expect(
      () =>
        new MedallionClient({
          baseUrl: "https://api.example.com",
          apiKey: "test_api_key",
          workspaceId: "workspace_not_canonical",
          fetch,
        }),
    ).toThrowError(
      expect.objectContaining({
        code: "MEDALLION_INVALID_WORKSPACE_ID",
      }),
    );
    expect(fetch).not.toHaveBeenCalled();
  });

  it("does not expose credentials through serialization or inspection", () => {
    const secret = "api_key_that_must_remain_private";
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: secret,
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch: vi.fn(),
    });

    expect(JSON.stringify(client)).not.toContain(secret);
    expect(inspect(client, { depth: 10 })).not.toContain(secret);
  });
});
