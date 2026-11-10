import {
  type DescField,
  type DescMessage,
  type DescMethod,
  fromJson,
  type JsonValue,
  toJson,
} from "@bufbuild/protobuf";
import { ParsedDescriptor } from "@jim-technologies/invariant-protocol";

import { MedallionError } from "./errors.js";

export class InvariantProtocolRuntime {
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
