import { toJson, type DescField, type DescMessage } from "@bufbuild/protobuf";
import { Server, type Tool } from "@jim-technologies/invariant-protocol";

import { connectDescriptorBytes } from "./connect-descriptor.js";
import { MedallionError } from "./errors.js";
import { ontologyDescriptorBytes } from "./ontology-descriptor.js";
import type { RequestClient, ResponseEnvelope } from "./request.js";
import { storageDescriptorBytes } from "./storage-descriptor.js";
import type {
  ConnectListCdcEventsRequest,
  ConnectListCdcEventsResponse,
  ConnectPublishCdcEventsRequest,
  ConnectPublishCdcEventsResponse,
  ConnectRegisterConnectorRequest,
  ConnectRegisterConnectorResponse,
  RequestOptions,
} from "./types.js";

const CONNECT_JSON_HEADERS = {
  "Connect-Protocol-Version": "1",
};
const CONNECT_SERVICE_NAME = "medallion.connect.v1.MedallionConnectService";
const ONTOLOGY_SERVICE_NAME = "medallion.ontology.v1.MedallionOntologyService";
const STORAGE_SERVICE_NAME = "medallion.storage.v1.StorageService";

let connectRuntime: InvariantProtocolRuntime | undefined;
let ontologyRuntime: InvariantProtocolRuntime | undefined;
let storageRuntime: InvariantProtocolRuntime | undefined;

export class ProtocolConnectClient {
  private readonly runtime: InvariantProtocolRuntime;

  constructor(private readonly requests: RequestClient) {
    this.runtime = invariantConnectRuntime();
  }

  registerConnector(
    request: ConnectRegisterConnectorRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<ConnectRegisterConnectorResponse>> {
    return this.rpc("RegisterConnector", request, options);
  }

  publishCdcEvents(
    request: ConnectPublishCdcEventsRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<ConnectPublishCdcEventsResponse>> {
    return this.rpc("PublishCdcEvents", request, options, {
      idempotencyKey: request.events[0]?.idempotency_key,
    });
  }

  listCdcEvents(
    request: ConnectListCdcEventsRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<ConnectListCdcEventsResponse>> {
    return this.rpc("ListCdcEvents", request, options);
  }

  executeConnectorAction<TResponse = unknown>(
    request: Record<string, unknown>,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<TResponse>> {
    return this.rpc("ExecuteConnectorAction", request, options, {
      idempotencyKey:
        typeof request.idempotency_key === "string"
          ? request.idempotency_key
          : undefined,
    });
  }

  private async rpc<TResponse>(
    methodName: string,
    body: unknown,
    options: RequestOptions,
    requestOptions: { idempotencyKey?: string } = {},
  ): Promise<ResponseEnvelope<TResponse>> {
    const tool = this.runtime.tool(methodName);
    const response = await this.requests.requestJson<unknown>({
      method: "POST",
      path: this.runtime.path(tool),
      body: this.runtime.encodeInput(tool, body),
      idempotencyKey: requestOptions.idempotencyKey,
      headers: CONNECT_JSON_HEADERS,
      signal: options.signal,
      timeoutMs: options.timeoutMs,
    });

    return {
      requestId: response.requestId,
      body: this.runtime.decodeOutput<TResponse>(tool, response.body),
    };
  }
}

class InvariantProtocolRuntime {
  private readonly server: Server;
  private readonly toolPrefix: string;
  private readonly pathPrefix: string;

  constructor(options: {
    descriptor: Uint8Array;
    serviceName: string;
    pathPrefix?: string;
  }) {
    this.server = Server.fromBytes(options.descriptor);
    this.server.connectHttp("http://127.0.0.1", {
      serviceName: options.serviceName,
    });
    this.toolPrefix = `${options.serviceName.split(".").at(-1)}.`;
    this.pathPrefix = normalizePathPrefix(options.pathPrefix);
  }

  tool(methodName: string): Tool {
    const tool = this.server.tools.get(`${this.toolPrefix}${methodName}`);
    if (tool === undefined) {
      throw new MedallionError(
        `Method ${methodName} is not present in the vendored invariantprotocol descriptor.`,
        { code: "MEDALLION_PROTOCOL_METHOD_NOT_FOUND" },
      );
    }
    return tool;
  }

  path(tool: Tool): string {
    return `${this.pathPrefix}/${tool.serviceFullName}/${tool.methodName}`;
  }

  encodeInput(tool: Tool, input: unknown): unknown {
    try {
      const message = this.server.coerceMessage(
        tool.inputDesc,
        normalizeProtoJson(tool.inputDesc, input),
      );
      return toJson(tool.inputDesc, message, {
        registry: this.server.parsed.registry,
      });
    } catch (error) {
      throw new MedallionError(
        `Invalid request for ${tool.serviceFullName}.${tool.methodName}.`,
        { code: "MEDALLION_PROTOCOL_ENCODE_FAILED", cause: error },
      );
    }
  }

  decodeOutput<TResponse>(tool: Tool, output: unknown): TResponse {
    try {
      const message = this.server.coerceMessage(
        tool.outputDesc,
        normalizeProtoJson(tool.outputDesc, output ?? {}),
      );
      return this.server.toJson(tool, message) as TResponse;
    } catch (error) {
      throw new MedallionError(
        `Invalid response from ${tool.serviceFullName}.${tool.methodName}.`,
        { code: "MEDALLION_PROTOCOL_DECODE_FAILED", cause: error },
      );
    }
  }
}

function invariantConnectRuntime(): InvariantProtocolRuntime {
  connectRuntime ??= new InvariantProtocolRuntime({
    descriptor: connectDescriptorBytes(),
    serviceName: CONNECT_SERVICE_NAME,
  });
  return connectRuntime;
}

function invariantOntologyRuntime(): InvariantProtocolRuntime {
  ontologyRuntime ??= new InvariantProtocolRuntime({
    descriptor: ontologyDescriptorBytes(),
    serviceName: ONTOLOGY_SERVICE_NAME,
    pathPrefix: "/rpc",
  });
  return ontologyRuntime;
}

function invariantStorageRuntime(): InvariantProtocolRuntime {
  storageRuntime ??= new InvariantProtocolRuntime({
    descriptor: storageDescriptorBytes(),
    serviceName: STORAGE_SERVICE_NAME,
  });
  return storageRuntime;
}

function normalizePathPrefix(value: string | undefined): string {
  if (value === undefined || value.trim() === "" || value === "/") {
    return "";
  }
  return `/${value.replace(/^\/+|\/+$/g, "")}`;
}

function normalizeProtoJson(desc: DescMessage, value: unknown): unknown {
  if (!isRecord(value)) {
    return value;
  }

  const normalized: Record<string, unknown> = {};
  for (const [key, fieldValue] of Object.entries(value)) {
    if (fieldValue === undefined) {
      continue;
    }

    const field = desc.fields.find(
      (candidate) => candidate.name === key || candidate.jsonName === key,
    );

    if (field === undefined) {
      normalized[key] = fieldValue;
      continue;
    }

    normalized[field.jsonName] = normalizeFieldValue(field, fieldValue);
  }

  return normalized;
}

function normalizeFieldValue(field: DescField, value: unknown): unknown {
  if (value === undefined || value === null) {
    return value;
  }

  if (field.fieldKind === "message") {
    return normalizeProtoJson(field.message, value);
  }

  if (field.fieldKind === "list" && field.listKind === "message") {
    if (!Array.isArray(value)) {
      return value;
    }
    return value.map((item) => normalizeProtoJson(field.message, item));
  }

  if (field.fieldKind === "map" && field.mapKind === "message") {
    if (!isRecord(value)) {
      return value;
    }
    return Object.fromEntries(
      Object.entries(value).map(([mapKey, mapValue]) => [
        mapKey,
        normalizeProtoJson(field.message, mapValue),
      ]),
    );
  }

  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export class ProtocolOntologyClient {
  private readonly runtime: InvariantProtocolRuntime;

  constructor(private readonly requests: RequestClient) {
    this.runtime = invariantOntologyRuntime();
  }

  query<TResponse = unknown>(
    request: Record<string, unknown>,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<TResponse>> {
    return this.rpc("Query", request, options);
  }

  planAction<TResponse = unknown>(
    actionName: string,
    request: Record<string, unknown>,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<TResponse>> {
    return this.rpc("PlanAction", { ...request, action_name: actionName }, options);
  }

  executeAction<TResponse = unknown>(
    actionName: string,
    request: Record<string, unknown>,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<TResponse>> {
    return this.rpc(
      "ExecuteAction",
      { ...request, action_name: actionName },
      options,
      {
        idempotencyKey:
          typeof request.idempotency_key === "string"
            ? request.idempotency_key
            : undefined,
      },
    );
  }

  private async rpc<TResponse>(
    methodName: string,
    body: unknown,
    options: RequestOptions,
    requestOptions: { idempotencyKey?: string } = {},
  ): Promise<ResponseEnvelope<TResponse>> {
    const tool = this.runtime.tool(methodName);
    const response = await this.requests.requestJson<unknown>({
      method: "POST",
      path: this.runtime.path(tool),
      body: this.runtime.encodeInput(tool, body),
      idempotencyKey: requestOptions.idempotencyKey,
      headers: CONNECT_JSON_HEADERS,
      signal: options.signal,
      timeoutMs: options.timeoutMs,
    });

    return {
      requestId: response.requestId,
      body: this.runtime.decodeOutput<TResponse>(tool, response.body),
    };
  }
}

export class ProtocolStorageClient {
  private readonly runtime: InvariantProtocolRuntime;

  constructor(private readonly requests: RequestClient) {
    this.runtime = invariantStorageRuntime();
  }

  upload<TResponse = unknown>(
    request: Record<string, unknown>,
    options: RequestOptions & {
      idempotencyKey?: string;
    } = {},
  ): Promise<ResponseEnvelope<TResponse>> {
    return this.rpc("Upload", request, options, {
      idempotencyKey: options.idempotencyKey,
    });
  }

  private async rpc<TResponse>(
    methodName: string,
    body: unknown,
    options: RequestOptions,
    requestOptions: { idempotencyKey?: string } = {},
  ): Promise<ResponseEnvelope<TResponse>> {
    const tool = this.runtime.tool(methodName);
    const response = await this.requests.requestJson<unknown>({
      method: "POST",
      path: this.runtime.path(tool),
      body: this.runtime.encodeInput(tool, body),
      idempotencyKey: requestOptions.idempotencyKey,
      headers: CONNECT_JSON_HEADERS,
      signal: options.signal,
      timeoutMs: options.timeoutMs,
    });

    return {
      requestId: response.requestId,
      body: this.runtime.decodeOutput<TResponse>(tool, response.body),
    };
  }
}

export interface ProtocolClients {
  connect: ProtocolConnectClient;
  ontology: ProtocolOntologyClient;
  storage: ProtocolStorageClient;
}
