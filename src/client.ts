import { AuditClient } from "./audit.js";
import { CdcClient } from "./cdc.js";
import { ProtocolConnectClient } from "./protocol.js";
import { RequestClient } from "./request.js";
import type { MedallionClientOptions } from "./types.js";

export class MedallionClient {
  readonly audit: AuditClient;
  readonly cdc: CdcClient;
  /** Low-level access to the same four ingestion RPCs used by cdc and audit. */
  readonly connect: ProtocolConnectClient;

  constructor(options: MedallionClientOptions) {
    const requests = new RequestClient(options);
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
