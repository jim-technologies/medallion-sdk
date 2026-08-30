import { AuditClient } from "./audit.js";
import { CdcClient } from "./cdc.js";

import { ProtocolIngestClient } from "./ingest.js";
import { ProtocolConnectClient } from "./protocol.js";
import { RequestClient } from "./request.js";
import { TablesClient } from "./tables.js";
import type { MedallionClientOptions } from "./types.js";

export class MedallionClient {
  /** Tabular ingestion and query: declare tables, append rows, run SQL. */
  readonly tables: TablesClient;
  /** Low-level access to the seven medallion.ingest.v1 RPCs. */
  readonly ingest: ProtocolIngestClient;
  /** @deprecated medallion.connect.v1 audit publishing is deprecated. */
  readonly audit: AuditClient;
  /** @deprecated medallion.connect.v1 CDC publishing is deprecated. */
  readonly cdc: CdcClient;
  /**
   * Low-level access to the same four ingestion RPCs used by cdc and audit.
   * @deprecated medallion.connect.v1 is deprecated.
   */
  readonly connect: ProtocolConnectClient;

  constructor(options: MedallionClientOptions) {
    const requests = new RequestClient(options);
    this.ingest = new ProtocolIngestClient(requests);
    this.tables = new TablesClient(this.ingest);
    this.connect = new ProtocolConnectClient(requests, options.workspaceId);
    this.audit = new AuditClient(this.connect, {
      workspaceId: options.workspaceId,
      defaultConnectorId: options.defaultConnectorId,
    });
    this.cdc = new CdcClient(this.connect, {
      workspaceId: options.workspaceId,
      defaultConnectorId: options.defaultConnectorId,
    });
  }
}
