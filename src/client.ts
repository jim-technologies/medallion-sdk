import { ProtocolIngestClient } from "./ingest.js";
import { RequestClient } from "./request.js";
import { TablesClient } from "./tables.js";
import type { MedallionClientOptions } from "./types.js";

export class MedallionClient {
  /** Tabular ingestion and query: declare tables, append rows, run SQL. */
  readonly tables: TablesClient;
  /** Low-level access to the seven medallion.ingest.v1 RPCs. */
  readonly ingest: ProtocolIngestClient;

  constructor(options: MedallionClientOptions) {
    const requests = new RequestClient(options);
    this.ingest = new ProtocolIngestClient(requests);
    this.tables = new TablesClient(this.ingest);
  }
}
