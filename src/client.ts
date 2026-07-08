import { AuditClient } from "./audit.js";
import { CdcClient } from "./cdc.js";
import { ConnectClient } from "./connect.js";
import { DatasourcesClient } from "./datasources.js";
import { EventsClient } from "./events.js";
import {
  ProtocolConnectClient,
  type ProtocolClients,
  ProtocolOntologyClient,
  ProtocolStorageClient,
} from "./protocol.js";
import { OntologyClient } from "./ontology.js";
import { RequestClient } from "./request.js";
import { StorageClient } from "./storage.js";
import type { MedallionClientOptions } from "./types.js";

export class MedallionClient {
  readonly audit: AuditClient;
  readonly events: EventsClient;
  readonly cdc: CdcClient;
  readonly connect: ConnectClient;
  readonly datasources: DatasourcesClient;
  readonly ontology: OntologyClient;
  readonly storage: StorageClient;
  readonly protocol: ProtocolClients;
  /** @deprecated Use protocol instead. */
  readonly generated: ProtocolClients;

  constructor(options: MedallionClientOptions) {
    const connectRequests = new RequestClient({
      ...options,
      baseUrl: options.connectBaseUrl ?? options.baseUrl,
    });
    const ontologyRequests = new RequestClient({
      ...options,
      baseUrl: options.ontologyBaseUrl ?? options.baseUrl,
    });
    const storageRequests = new RequestClient({
      ...options,
      baseUrl: options.storageBaseUrl ?? options.baseUrl,
    });

    const protocolConnect = new ProtocolConnectClient(connectRequests);
    const protocolOntology = new ProtocolOntologyClient(ontologyRequests);
    const protocolStorage = new ProtocolStorageClient(storageRequests);

    this.protocol = {
      connect: protocolConnect,
      ontology: protocolOntology,
      storage: protocolStorage,
    };
    this.generated = this.protocol;

    this.connect = new ConnectClient(protocolConnect, {
      organizationId: options.organizationId ?? options.tenantId,
    });
    this.audit = new AuditClient(protocolConnect, {
      organizationId: options.organizationId ?? options.tenantId,
      defaultConnectorId: options.defaultConnectorId,
    });
    this.events = new EventsClient(protocolConnect, {
      defaultConnectorId: options.defaultConnectorId,
    });
    this.cdc = new CdcClient(protocolConnect, {
      defaultConnectorId: options.defaultConnectorId,
    });
    this.datasources = new DatasourcesClient(this.connect);
    this.ontology = new OntologyClient(protocolOntology);
    this.storage = new StorageClient(protocolStorage);
  }
}
