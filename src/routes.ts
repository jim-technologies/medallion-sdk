export const CONNECT_SERVICE = "/medallion.connect.v1.MedallionConnectService";
export const ONTOLOGY_RPC_SERVICE =
  "/rpc/medallion.ontology.v1.MedallionOntologyService";
export const STORAGE_SERVICE = "/medallion.storage.v1.StorageService";

export const CONNECT_ROUTES = {
  registerConnector: `${CONNECT_SERVICE}/RegisterConnector`,
  listConnectors: `${CONNECT_SERVICE}/ListConnectors`,
  disableConnector: `${CONNECT_SERVICE}/DisableConnector`,
  registerConnectorAction: `${CONNECT_SERVICE}/RegisterConnectorAction`,
  listConnectorActions: `${CONNECT_SERVICE}/ListConnectorActions`,
  disableConnectorAction: `${CONNECT_SERVICE}/DisableConnectorAction`,
  executeConnectorAction: `${CONNECT_SERVICE}/ExecuteConnectorAction`,
  getActionExecution: `${CONNECT_SERVICE}/GetActionExecution`,
  publishCdcEvents: `${CONNECT_SERVICE}/PublishCdcEvents`,
  listCdcEvents: `${CONNECT_SERVICE}/ListCdcEvents`,
  publishAuditEvents: `${CONNECT_SERVICE}/PublishAuditEvents`,
  listAuditEvents: `${CONNECT_SERVICE}/ListAuditEvents`,
} as const;

export const ONTOLOGY_ROUTES = {
  query: "/v1/query",
  planAction: (actionName: string) =>
    `/v1/actions/${encodeURIComponent(actionName)}:plan`,
  executeAction: (actionName: string) =>
    `/v1/actions/${encodeURIComponent(actionName)}:execute`,
  listDatasources: "/v1/datasources",
  graph: "/v1/ontology/graph",
  entities: "/v1/ontology/entities",
  actions: "/v1/ontology/actions",
} as const;

export const STORAGE_ROUTES = {
  upload: "/upload",
  uploadRpc: `${STORAGE_SERVICE}/Upload`,
} as const;
