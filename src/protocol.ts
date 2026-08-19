import {
  type DescField,
  type DescMessage,
  type DescMethod,
  fromJson,
  type JsonValue,
  toJson,
} from "@bufbuild/protobuf";
import { ParsedDescriptor } from "@jim-technologies/invariant-protocol";

import { auditEventFromWire } from "./audit.js";
import { cdcEventFromWire } from "./cdc.js";
import { connectDescriptorBytes } from "./connect-descriptor.js";
import { MedallionError } from "./errors.js";
import { batchResponse, optionalListText } from "./ingestion.js";
import { preflightConnectRequest } from "./protocol-preflight.js";
import type { RequestClient, ResponseEnvelope } from "./request.js";
import type {
  ConnectAuditEvent,
  ConnectCdcEvent,
  ConnectListAuditEventsRequest,
  ConnectListAuditEventsResponse,
  ConnectListCdcEventsRequest,
  ConnectListCdcEventsResponse,
  ConnectPublishAuditEventsRequest,
  ConnectPublishAuditEventsResponse,
  ConnectPublishCdcEventsRequest,
  ConnectPublishCdcEventsResponse,
  RequestOptions,
} from "./types.js";

const CONNECT_SERVICE_NAME = "medallion.connect.v1.MedallionConnectService";

let connectRuntime: InvariantProtocolRuntime | undefined;

export class ProtocolConnectClient {
  readonly #requests: RequestClient;
  readonly #workspaceId: string;
  private readonly runtime: InvariantProtocolRuntime;

  constructor(requests: RequestClient, workspaceId: string) {
    this.#requests = requests;
    this.#workspaceId = workspaceId;
    this.runtime = invariantConnectRuntime();
  }

  publishCdcEvents(
    request: ConnectPublishCdcEventsRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<ConnectPublishCdcEventsResponse>> {
    return this.#rpc("PublishCdcEvents", request, options, true);
  }

  listCdcEvents(
    request: ConnectListCdcEventsRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<ConnectListCdcEventsResponse>> {
    return this.#rpc("ListCdcEvents", request, options, true);
  }

  publishAuditEvents(
    request: ConnectPublishAuditEventsRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<ConnectPublishAuditEventsResponse>> {
    return this.#rpc("PublishAuditEvents", request, options, true);
  }

  listAuditEvents(
    request: ConnectListAuditEventsRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<ConnectListAuditEventsResponse>> {
    return this.#rpc("ListAuditEvents", request, options, true);
  }

  async #rpc<TResponse>(
    methodName: string,
    body: unknown,
    options: RequestOptions,
    retrySafe: boolean,
  ): Promise<ResponseEnvelope<TResponse>> {
    const method = this.runtime.method(methodName);
    const normalizedBody = this.runtime.normalizeInput(method, body);
    preflightConnectRequest(methodName, normalizedBody);
    const encodedBody = this.runtime.encodeInput(method, normalizedBody);
    const response = await this.#requests.requestJson<unknown>({
      method: "POST",
      path: this.runtime.path(method),
      body: encodedBody,
      signal: options.signal,
      timeoutMs: options.timeoutMs,
      connectProtocol: true,
      retrySafe,
    });

    const decoded = this.runtime.decodeOutput<TResponse>(
      method,
      response.body,
      response.requestId,
    );
    validateProtocolResponse(
      methodName,
      encodedBody,
      decoded,
      this.#workspaceId,
      response.requestId,
    );
    return {
      requestId: response.requestId,
      body: decoded,
    };
  }
}

function validateProtocolResponse(
  methodName: string,
  request: unknown,
  response: unknown,
  workspaceId: string,
  requestId?: string,
): void {
  if (
    methodName === "PublishCdcEvents" ||
    methodName === "PublishAuditEvents"
  ) {
    const events =
      request !== null && typeof request === "object"
        ? (request as { events?: Array<{ idempotencyKey?: unknown }> }).events
        : undefined;
    const expectedKeys = (events ?? []).map((event) =>
      String(event.idempotencyKey),
    );
    void batchResponse(
      response as
        | ConnectPublishCdcEventsResponse
        | ConnectPublishAuditEventsResponse,
      expectedKeys,
      requestId,
    );
    return;
  }
  if (methodName !== "ListCdcEvents" && methodName !== "ListAuditEvents") {
    return;
  }
  if (response === null || typeof response !== "object") return;
  const events = (response as { events?: unknown }).events;
  if (events === undefined) return;
  if (!Array.isArray(events)) {
    throw new MedallionError("Medallion returned a malformed event list.", {
      code: "MEDALLION_INVALID_LIST_RESPONSE",
      requestId,
    });
  }
  optionalListText(
    (response as { next_page_cursor?: unknown }).next_page_cursor,
    `${methodName}.nextCursor`,
    2_048,
    requestId,
  );
  for (const [index, event] of events.entries()) {
    if (event === null || typeof event !== "object") {
      throw new MedallionError(
        `Medallion returned a malformed event at ${methodName}.events[${index}].`,
        { code: "MEDALLION_INVALID_LIST_RESPONSE", requestId },
      );
    }
    if (methodName === "ListCdcEvents") {
      cdcEventFromWire(event as ConnectCdcEvent, workspaceId, requestId);
    } else {
      auditEventFromWire(event as ConnectAuditEvent, workspaceId, requestId);
    }
  }
}

class InvariantProtocolRuntime {
  private readonly parsed: ParsedDescriptor;
  private readonly pathPrefix: string;
  private readonly serviceName: string;
  private readonly ignoreUnknownResponseFields: boolean;

  constructor(options: {
    descriptor: Uint8Array;
    serviceName: string;
    pathPrefix?: string;
    ignoreUnknownResponseFields?: boolean;
  }) {
    this.parsed = ParsedDescriptor.fromBytes(options.descriptor);
    this.serviceName = options.serviceName;
    this.pathPrefix = normalizePathPrefix(options.pathPrefix);
    this.ignoreUnknownResponseFields =
      options.ignoreUnknownResponseFields ?? false;

    if (!this.parsed.services.has(this.serviceName)) {
      throw new MedallionError(
        `Service ${this.serviceName} is not present in the vendored invariantprotocol descriptor.`,
        { code: "MEDALLION_PROTOCOL_METHOD_NOT_FOUND" },
      );
    }
  }

  method(methodName: string): DescMethod {
    const method = this.parsed.services
      .get(this.serviceName)
      ?.methods.get(methodName)?.desc;
    if (method === undefined) {
      throw new MedallionError(
        `Method ${methodName} is not present in the vendored invariantprotocol descriptor.`,
        { code: "MEDALLION_PROTOCOL_METHOD_NOT_FOUND" },
      );
    }
    if (method.methodKind !== "unary") {
      throw new MedallionError(
        `Method ${this.serviceName}.${method.name} is streaming and is not supported by this SDK client.`,
        { code: "MEDALLION_PROTOCOL_METHOD_UNSUPPORTED" },
      );
    }
    return method;
  }

  path(method: DescMethod): string {
    return `${this.pathPrefix}/${this.serviceName}/${method.name}`;
  }

  encodeInput(method: DescMethod, input: unknown): unknown {
    try {
      const message = fromJson(
        method.input,
        normalizeProtoJson(method.input, input) as JsonValue,
        { registry: this.parsed.registry },
      );
      return toJson(method.input, message, {
        registry: this.parsed.registry,
      });
    } catch {
      throw new MedallionError(
        `Invalid request for ${this.serviceName}.${method.name}.`,
        { code: "MEDALLION_PROTOCOL_ENCODE_FAILED" },
      );
    }
  }

  normalizeInput(method: DescMethod, input: unknown): unknown {
    return normalizeProtoJson(method.input, input);
  }

  decodeOutput<TResponse>(
    method: DescMethod,
    output: unknown,
    requestId?: string,
  ): TResponse {
    try {
      if (output === null || output === undefined) {
        throw new TypeError("RPC response body is required.");
      }
      const message = fromJson(
        method.output,
        normalizeProtoJson(method.output, output) as JsonValue,
        {
          registry: this.parsed.registry,
          ignoreUnknownFields: this.ignoreUnknownResponseFields,
        },
      );
      return toJson(method.output, message, {
        registry: this.parsed.registry,
        useProtoFieldName: true,
      }) as TResponse;
    } catch {
      throw new MedallionError(
        `Invalid response from ${this.serviceName}.${method.name}.`,
        {
          code: "MEDALLION_PROTOCOL_DECODE_FAILED",
          requestId,
        },
      );
    }
  }
}

function invariantConnectRuntime(): InvariantProtocolRuntime {
  connectRuntime ??= new InvariantProtocolRuntime({
    descriptor: connectDescriptorBytes(),
    serviceName: CONNECT_SERVICE_NAME,
    ignoreUnknownResponseFields: true,
  });
  return connectRuntime;
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
