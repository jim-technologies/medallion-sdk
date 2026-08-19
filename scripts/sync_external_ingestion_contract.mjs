#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  createFileRegistry,
  fromBinary,
  fromJson,
  getOption,
  toJson,
} from "@bufbuild/protobuf";
import {
  FieldDescriptorProtoSchema,
  FileDescriptorSetSchema,
} from "@bufbuild/protobuf/wkt";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
export const SDK_ROOT = path.resolve(SCRIPT_DIR, "..");

export const CONNECT_PACKAGE = "medallion.connect.v1";
export const CONNECT_SERVICE = "medallion.connect.v1.MedallionConnectService";
export const PROFILE_ID = "external_ingestion_sdk_v1";
export const EXPECTED_PROFILE_METHODS = Object.freeze([
  "PublishCdcEvents",
  "PublishAuditEvents",
  "ListCdcEvents",
  "ListAuditEvents",
]);
export const EXPECTED_ERROR_REASONS = Object.freeze([
  "AUTHORIZATION_DEPENDENCY_UNAVAILABLE",
  "BACKPRESSURE",
  "CAPABILITY_UNAVAILABLE",
  "FEATURE_NOT_ENTITLED",
  "IDEMPOTENCY_MISMATCH",
  "INTEGRATION_UNAVAILABLE",
  "WORKSPACE_SELECTOR_CONFLICT",
]);

const VENDOR_RELATIVE = "proto/external-ingestion-contract/v1";
const MANIFEST_NAME = "external_ingestion_sdk_v1.json";
const LOCAL_DESCRIPTOR_RELATIVE =
  "proto/external-ingestion-v1.descriptor.binpb";
const LOCAL_PROTO_RELATIVE = "proto/medallion/connect/v1/connect.proto";
const VALIDATION_PROTO_RELATIVE = "proto/buf/validate/validate.proto";
const ALLOWLIST_RELATIVE = "proto/external-ingestion-v1.json";
const SUPPORTED_ROUTES_RELATIVE = "proto/client-facing-routes.json";
const TYPESCRIPT_ERROR_POLICY_RELATIVE = "src/error-policy.ts";
const GO_ERROR_POLICY_RELATIVE = "go/error_policy_generated.go";
const PYTHON_ERROR_POLICY_RELATIVE =
  "python/src/medallion/error_policy_generated.py";
const SDK_GO_PACKAGE =
  "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1;connectv1";
const ERROR_INFO_DOMAIN = "medallion.jimtech.io";
const BUF = process.env.BUF || "buf";
const RETIRED_SCOPE_FIELD = ["organization", "id"].join("_");

const ARTIFACT_ROLES = Object.freeze({
  "conformance/external-sdk-ingestion.json": "conformance_fixture",
  "conformance/schemas/fixture-document.schema.json": "schema",
  "connect-idempotency-policy.json": "idempotency_policy",
  "descriptor.binpb": "descriptor",
  "error-reasons.json": "error_registry",
  [MANIFEST_NAME]: "consumer_manifest",
  "release-attestation.json": "release_attestation",
  "schemas/error-reasons.schema.json": "schema",
  "schemas/external-ingestion-sdk-v1.schema.json": "schema",
});

const TEXT_FORBIDDEN_PARTS = Object.freeze([
  ["Medallion", ["Onto", "logy"].join(""), "Service"],
  ["medallion", ["onto", "logy"].join(""), "v1"],
  [["onto", "logy"].join(""), "idempotency", "policy"],
  [["ter", "minal"].join(""), ["com", "pass"].join("")],
  ["MEDALLION", ["ONTO", "LOGY"].join(""), "ROOT"],
  ["medallion", ["onto", "logy"].join("")],
]);
const RETIRED_IMPLEMENTATION_MARKER = ["medallion", "connect"].join("-");
const FORBIDDEN_EXPORT_MARKERS = Object.freeze([
  ["source", "repository"].join("_"),
  `${["consumer", "profiles"].join("-")}.json`,
  ["contracts", "public", "v1"].join("/"),
  ["..", "medallion", ["onto", "logy"].join("")].join("/"),
]);

function fail(message) {
  throw new Error(`external ingestion contract: ${message}`);
}

export function sha256(payload) {
  return createHash("sha256").update(payload).digest("hex");
}

function normalizeJson(value) {
  if (typeof value === "bigint") return value.toString();
  if (Array.isArray(value)) return value.map(normalizeJson);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, item]) => item !== undefined)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, item]) => [key, normalizeJson(item)]),
    );
  }
  return value;
}

export function renderJson(value) {
  return `${JSON.stringify(normalizeJson(value), null, 2)}\n`;
}

function parseJson(payload, label) {
  try {
    return JSON.parse(payload.toString("utf8"));
  } catch (error) {
    fail(`${label} is not valid JSON: ${error.message}`);
  }
}

function assertObject(value, label) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    fail(`${label} must be an object`);
  }
}

function assertExactKeys(value, expected, label) {
  assertObject(value, label);
  const actual = Object.keys(value).toSorted();
  const wanted = [...expected].toSorted();
  if (renderJson(actual) !== renderJson(wanted)) {
    fail(
      `${label} keys differ: expected ${wanted.join(", ")}; got ${actual.join(", ")}`,
    );
  }
}

function safeArtifactPath(value, label) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.includes("\\") ||
    path.posix.isAbsolute(value) ||
    path.posix.normalize(value) !== value ||
    value === ".." ||
    value.startsWith("../")
  ) {
    fail(`${label} contains unsafe artifact path ${String(value)}`);
  }
}

function parseSingleChecksum(payload, expectedName, label) {
  const match = /^([0-9a-f]{64}) {2}([^\n]+)\n$/.exec(
    payload.toString("ascii"),
  );
  if (!match || match[2] !== expectedName) {
    fail(`${label} must contain exactly one checksum for ${expectedName}`);
  }
  return match[1];
}

function atomicWrite(destination, payload) {
  mkdirSync(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.tmp-${process.pid}`;
  try {
    writeFileSync(temporary, payload);
    renameSync(temporary, destination);
  } finally {
    rmSync(temporary, { force: true });
  }
}

function recursiveFileInventory(root, prefix = "") {
  const directory = path.join(root, prefix);
  if (!existsSync(directory)) return [];
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const relative = path.posix.join(prefix, entry.name);
    if (entry.isDirectory()) {
      files.push(...recursiveFileInventory(root, relative));
    } else if (entry.isFile()) {
      files.push(relative);
    }
  }
  return files.toSorted();
}

function scanBoundary(payload, label) {
  const source = payload.toString("latin1").toLowerCase();
  if (source.includes(RETIRED_IMPLEMENTATION_MARKER)) {
    fail(`${label} contains an implementation-only repository marker`);
  }
  if (FORBIDDEN_EXPORT_MARKERS.some((marker) => source.includes(marker))) {
    fail(`${label} contains non-SDK contract provenance or profile metadata`);
  }
  for (const parts of TEXT_FORBIDDEN_PARTS) {
    const candidates = [
      parts.join(""),
      parts.join("."),
      parts.join("-"),
      parts.join("_"),
    ].map((value) => value.toLowerCase());
    if (candidates.some((value) => source.includes(value))) {
      fail(`${label} contains an implementation-only contract reference`);
    }
  }
}

function readBundle(directory, label) {
  const bundlePath = path.join(directory, "bundle.json");
  const checksumPath = path.join(directory, "bundle.sha256");
  if (!existsSync(bundlePath) || !existsSync(checksumPath)) {
    fail(`${label} must contain bundle.json and bundle.sha256`);
  }
  const bundlePayload = readFileSync(bundlePath);
  const checksumPayload = readFileSync(checksumPath);
  const pinnedHash = parseSingleChecksum(
    checksumPayload,
    "bundle.json",
    `${label}/bundle.sha256`,
  );
  if (sha256(bundlePayload) !== pinnedHash) {
    fail(`${label}/bundle.json does not match bundle.sha256`);
  }
  const bundle = parseJson(bundlePayload, `${label}/bundle.json`);
  assertExactKeys(
    bundle,
    [
      "artifacts",
      "bundle_id",
      "contract_version",
      "release_status",
      "schema_version",
    ],
    `${label}/bundle.json`,
  );
  if (
    bundle.schema_version !== 1 ||
    bundle.bundle_id !== PROFILE_ID ||
    !["released", "unreleased_candidate"].includes(bundle.release_status) ||
    !/^sdkc_[0-9a-z]{26}$/.test(bundle.contract_version) ||
    !Array.isArray(bundle.artifacts)
  ) {
    fail(`${label}/bundle.json has unsupported release metadata`);
  }

  const expectedPaths = Object.keys(ARTIFACT_ROLES).toSorted();
  const actualPaths = [];
  const artifacts = new Map();
  for (const entry of bundle.artifacts) {
    assertExactKeys(
      entry,
      ["bytes", "path", "role", "sha256"],
      "bundle artifact",
    );
    safeArtifactPath(entry.path, "bundle.json");
    if (
      !Number.isSafeInteger(entry.bytes) ||
      entry.bytes < 0 ||
      !/^[0-9a-f]{64}$/.test(entry.sha256) ||
      entry.role !== ARTIFACT_ROLES[entry.path]
    ) {
      fail(`bundle artifact ${entry.path} has invalid evidence or role`);
    }
    actualPaths.push(entry.path);
    const filename = path.join(directory, ...entry.path.split("/"));
    if (!existsSync(filename)) {
      fail(`${label} is missing ${entry.path}`);
    }
    const payload = readFileSync(filename);
    if (payload.length !== entry.bytes || sha256(payload) !== entry.sha256) {
      fail(`${label}/${entry.path} differs from its bundle evidence`);
    }
    scanBoundary(payload, `${label}/${entry.path}`);
    artifacts.set(entry.path, payload);
  }
  if (renderJson(actualPaths) !== renderJson(expectedPaths)) {
    fail("bundle must contain exactly the minimal SDK artifact inventory");
  }
  const expectedFiles = [
    "bundle.json",
    "bundle.sha256",
    ...expectedPaths,
  ].toSorted();
  if (
    renderJson(recursiveFileInventory(directory)) !== renderJson(expectedFiles)
  ) {
    fail(`${label} contains stale or undeclared files`);
  }
  scanBoundary(bundlePayload, `${label}/bundle.json`);
  return {
    artifacts,
    bundle,
    bundleHash: pinnedHash,
    bundlePayload,
    checksumPayload,
    directory,
  };
}

function artifact(state, name) {
  const payload = state.artifacts.get(name);
  if (!payload) fail(`bundle is missing ${name}`);
  return payload;
}

function normalizeTypeName(name) {
  return name.startsWith(".") ? name.slice(1) : name;
}

function addUnique(index, name, value, label) {
  if (index.has(name)) fail(`${label} contains duplicate symbol ${name}`);
  index.set(name, value);
}

function indexDescriptor(payload, label) {
  let descriptor;
  try {
    descriptor = fromBinary(FileDescriptorSetSchema, payload);
  } catch (error) {
    fail(`${label} is not a valid FileDescriptorSet: ${error.message}`);
  }
  const files = new Map();
  const services = new Map();
  const messages = new Map();
  const enums = new Map();
  const addEnum = (enumeration, prefix) =>
    addUnique(enums, `${prefix}.${enumeration.name}`, enumeration, label);
  const addMessage = (message, prefix) => {
    const name = `${prefix}.${message.name}`;
    addUnique(messages, name, message, label);
    for (const enumeration of message.enumType) addEnum(enumeration, name);
    for (const nested of message.nestedType) addMessage(nested, name);
  };
  for (const file of descriptor.file) {
    addUnique(files, file.name, file, label);
    if (file.sourceCodeInfo !== undefined) {
      fail(`${label} must omit source comments and source paths`);
    }
    for (const dependency of file.dependency) {
      if (!descriptor.file.some((candidate) => candidate.name === dependency)) {
        fail(`${label} is not dependency-closed; missing ${dependency}`);
      }
    }
    for (const enumeration of file.enumType) addEnum(enumeration, file.package);
    for (const message of file.messageType) addMessage(message, file.package);
    for (const service of file.service) {
      addUnique(services, `${file.package}.${service.name}`, service, label);
    }
  }
  return { descriptor, enums, files, messages, services };
}

function methodProjection(serviceName, method) {
  return {
    client_streaming: method.clientStreaming,
    name: method.name,
    path: `/${serviceName}/${method.name}`,
    request_type: normalizeTypeName(method.inputType),
    response_type: normalizeTypeName(method.outputType),
    server_streaming: method.serverStreaming,
  };
}

function validateDescriptor(payload) {
  const index = indexDescriptor(payload, "descriptor.binpb");
  if (
    renderJson([...index.services.keys()]) !== renderJson([CONNECT_SERVICE])
  ) {
    fail("descriptor must contain only MedallionConnectService");
  }
  const service = index.services.get(CONNECT_SERVICE);
  const methodNames = service.method.map((method) => method.name);
  if (
    renderJson(methodNames.toSorted()) !==
      renderJson([...EXPECTED_PROFILE_METHODS].toSorted()) ||
    service.method.some(
      (method) => method.clientStreaming || method.serverStreaming,
    )
  ) {
    fail("descriptor must contain exactly the four unary ingestion RPCs");
  }

  const pending = service.method.flatMap((method) => [
    normalizeTypeName(method.inputType),
    normalizeTypeName(method.outputType),
  ]);
  const reachableMessages = new Set();
  const reachableEnums = new Set();
  while (pending.length > 0) {
    const name = pending.pop();
    if (
      reachableMessages.has(name) ||
      !name.startsWith(`${CONNECT_PACKAGE}.`)
    ) {
      continue;
    }
    const message = index.messages.get(name);
    if (!message) fail(`descriptor is missing reachable message ${name}`);
    reachableMessages.add(name);
    for (const field of message.field) {
      const typeName = normalizeTypeName(field.typeName);
      if (index.messages.has(typeName)) pending.push(typeName);
      if (index.enums.has(typeName)) reachableEnums.add(typeName);
      if (
        /^(organization|tenant)_id$/.test(field.name) ||
        /^(organization|tenant)Id$/.test(field.jsonName)
      ) {
        fail(`active legacy scope field remains in ${name}.${field.name}`);
      }
    }
  }
  const localMessages = [...index.messages.keys()]
    .filter((name) => name.startsWith(`${CONNECT_PACKAGE}.`))
    .toSorted();
  const localEnums = [...index.enums.keys()]
    .filter((name) => name.startsWith(`${CONNECT_PACKAGE}.`))
    .toSorted();
  if (
    renderJson(localMessages) !==
      renderJson([...reachableMessages].toSorted()) ||
    renderJson(localEnums) !== renderJson([...reachableEnums].toSorted())
  ) {
    fail("descriptor contains symbols outside the four-RPC wire closure");
  }
  if (reachableMessages.size !== 12 || reachableEnums.size !== 3) {
    fail("descriptor wire closure has unexpected size");
  }
  const expectedWorkspaceFields = new Map([
    [`${CONNECT_PACKAGE}.CdcEvent`, [17, 2]],
    [`${CONNECT_PACKAGE}.AuditEvent`, [18, 2]],
    [`${CONNECT_PACKAGE}.ListCdcEventsRequest`, [13, 1]],
    [`${CONNECT_PACKAGE}.ListAuditEventsRequest`, [15, 1]],
  ]);
  for (const [name, [fieldNumber, reservedNumber]] of expectedWorkspaceFields) {
    const message = index.messages.get(name);
    const workspace = message?.field.find(
      (field) => field.name === "workspace_id",
    );
    if (
      workspace?.number !== fieldNumber ||
      !message.reservedName.includes(RETIRED_SCOPE_FIELD) ||
      !message.reservedRange.some(
        (range) => range.start <= reservedNumber && range.end > reservedNumber,
      )
    ) {
      fail(`${name} does not preserve the workspace-only wire contract`);
    }
  }
  return {
    enumCount: reachableEnums.size,
    index,
    messageCount: reachableMessages.size,
    methods: service.method,
    wireClosureSha256: sha256(
      Buffer.from(
        renderJson({
          enums: [...reachableEnums].toSorted(),
          messages: [...reachableMessages].toSorted(),
          methods: service.method.map((method) =>
            methodProjection(CONNECT_SERVICE, method),
          ),
          service: CONNECT_SERVICE,
        }),
      ),
    ),
  };
}

function validateManifest(payload, descriptorPayload, descriptorState) {
  const manifest = parseJson(payload, MANIFEST_NAME);
  assertExactKeys(
    manifest,
    [
      "$schema",
      "descriptor",
      "descriptor_sha256",
      "methods",
      "package",
      "profile",
      "schema_version",
      "service",
    ],
    MANIFEST_NAME,
  );
  if (
    manifest.$schema !== "schemas/external-ingestion-sdk-v1.schema.json" ||
    manifest.schema_version !== 1 ||
    manifest.profile !== PROFILE_ID ||
    manifest.package !== CONNECT_PACKAGE ||
    manifest.service !== CONNECT_SERVICE ||
    manifest.descriptor !== "descriptor.binpb" ||
    manifest.descriptor_sha256 !== sha256(descriptorPayload) ||
    !Array.isArray(manifest.methods)
  ) {
    fail(`${MANIFEST_NAME} has unsupported metadata`);
  }
  const descriptorMethods = new Map(
    descriptorState.methods.map((method) => [method.name, method]),
  );
  const expected = EXPECTED_PROFILE_METHODS.map((name) => {
    const method = descriptorMethods.get(name);
    if (!method) fail(`descriptor is missing ${name}`);
    return methodProjection(CONNECT_SERVICE, method);
  });
  if (renderJson(manifest.methods) !== renderJson(expected)) {
    fail(`${MANIFEST_NAME} differs from descriptor.binpb`);
  }
  return manifest;
}

function validateIdempotencyPolicy(payload) {
  const policy = parseJson(payload, "connect-idempotency-policy.json");
  assertExactKeys(
    policy,
    ["classifications", "schema_version", "service"],
    "Connect idempotency policy",
  );
  if (
    policy.schema_version !== 1 ||
    policy.service !== CONNECT_SERVICE ||
    !policy.classifications ||
    Array.isArray(policy.classifications)
  ) {
    fail("Connect idempotency policy has unsupported metadata");
  }
  assertExactKeys(
    policy.classifications,
    ["read_only", "record_idempotent_batch"],
    "Connect idempotency classifications",
  );
  const expected = {
    record_idempotent_batch: ["PublishCdcEvents", "PublishAuditEvents"],
    read_only: ["ListCdcEvents", "ListAuditEvents"],
  };
  for (const [classification, methods] of Object.entries(expected)) {
    const entry = policy.classifications[classification];
    assertExactKeys(
      entry,
      ["methods", "rationale"],
      `Connect idempotency ${classification}`,
    );
    if (
      typeof entry.rationale !== "string" ||
      entry.rationale.length === 0 ||
      renderJson(entry.methods) !== renderJson(methods)
    ) {
      fail(`Connect idempotency ${classification} differs from the profile`);
    }
  }
  return policy;
}

export function validateErrorReasonPolicy(policy) {
  assertExactKeys(
    policy,
    ["$schema", "domain", "reasons", "schema_version", "unknown_reason_policy"],
    "error reason registry",
  );
  if (
    policy.$schema !== "schemas/error-reasons.schema.json" ||
    policy.schema_version !== 1 ||
    policy.domain !== ERROR_INFO_DOMAIN ||
    policy.unknown_reason_policy !== "preserve" ||
    !Array.isArray(policy.reasons)
  ) {
    fail("error reason registry has unsupported metadata");
  }
  const seen = new Set();
  for (const entry of policy.reasons) {
    assertExactKeys(
      entry,
      ["consumer_category", "grpc_code", "reason", "retry_policy"],
      `error reason ${entry?.reason ?? "<missing>"}`,
    );
    assertExactKeys(
      entry.retry_policy,
      ["classification", "reuse_idempotency_key"],
      `error reason ${entry.reason} retry policy`,
    );
    if (
      seen.has(entry.reason) ||
      !/^[A-Z][A-Z0-9_]*$/.test(entry.reason) ||
      !/^[A-Z][A-Z0-9_]*$/.test(entry.grpc_code) ||
      typeof entry.consumer_category !== "string" ||
      entry.consumer_category.length === 0 ||
      typeof entry.retry_policy.classification !== "string" ||
      entry.retry_policy.classification.length === 0 ||
      typeof entry.retry_policy.reuse_idempotency_key !== "boolean"
    ) {
      fail(`error reason ${String(entry.reason)} has invalid policy values`);
    }
    seen.add(entry.reason);
  }
  if (
    renderJson([...seen].toSorted()) !==
    renderJson([...EXPECTED_ERROR_REASONS].toSorted())
  ) {
    fail("error registry must contain exactly the SDK-relevant reasons");
  }
  return policy;
}

function roundTripJson(schema, value, label) {
  try {
    const decoded = fromJson(schema, value, { ignoreUnknownFields: false });
    const encoded = toJson(schema, decoded);
    if (renderJson(encoded) !== renderJson(value)) {
      fail(`${label} is not canonical protobuf JSON`);
    }
  } catch (error) {
    if (String(error.message).startsWith("external ingestion contract:")) {
      throw error;
    }
    fail(`${label} is invalid protobuf JSON: ${error.message}`);
  }
}

function resolveSchemaReference(rootSchema, reference, label) {
  if (typeof reference !== "string" || !reference.startsWith("#/")) {
    fail(
      `${label} uses unsupported JSON Schema reference ${String(reference)}`,
    );
  }
  let current = rootSchema;
  for (const encoded of reference.slice(2).split("/")) {
    const key = encoded.replaceAll("~1", "/").replaceAll("~0", "~");
    if (
      current === null ||
      typeof current !== "object" ||
      Array.isArray(current) ||
      !Object.hasOwn(current, key)
    ) {
      fail(`${label} contains unresolved JSON Schema reference ${reference}`);
    }
    current = current[key];
  }
  return current;
}

function jsonSchemaTypeMatches(value, type) {
  if (type === "array") return Array.isArray(value);
  if (type === "object") {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }
  if (type === "integer") return Number.isInteger(value);
  if (type === "number")
    return typeof value === "number" && Number.isFinite(value);
  if (type === "null") return value === null;
  return typeof value === type;
}

function validateJsonSchema(value, schema, rootSchema, label, location = "$") {
  assertObject(schema, `${label} schema at ${location}`);
  if (schema.$ref !== undefined) {
    return validateJsonSchema(
      value,
      resolveSchemaReference(rootSchema, schema.$ref, label),
      rootSchema,
      label,
      location,
    );
  }
  if (
    schema.type !== undefined &&
    (typeof schema.type !== "string" ||
      !jsonSchemaTypeMatches(value, schema.type))
  ) {
    fail(`${label} ${location} must have JSON Schema type ${schema.type}`);
  }
  if (
    schema.const !== undefined &&
    renderJson(value) !== renderJson(schema.const)
  ) {
    fail(`${label} ${location} differs from its JSON Schema const`);
  }
  if (
    schema.enum !== undefined &&
    (!Array.isArray(schema.enum) ||
      !schema.enum.some((item) => renderJson(item) === renderJson(value)))
  ) {
    fail(`${label} ${location} is not allowed by its JSON Schema enum`);
  }
  if (typeof value === "string") {
    if (
      Number.isInteger(schema.minLength) &&
      [...value].length < schema.minLength
    ) {
      fail(`${label} ${location} is shorter than its JSON Schema minimum`);
    }
    if (schema.pattern !== undefined) {
      let pattern;
      try {
        pattern = new RegExp(schema.pattern, "u");
      } catch (error) {
        fail(
          `${label} schema has invalid pattern at ${location}: ${error.message}`,
        );
      }
      if (!pattern.test(value)) {
        fail(`${label} ${location} does not match its JSON Schema pattern`);
      }
    }
  }
  if (Array.isArray(value)) {
    if (Number.isInteger(schema.minItems) && value.length < schema.minItems) {
      fail(`${label} ${location} has too few JSON Schema items`);
    }
    if (Number.isInteger(schema.maxItems) && value.length > schema.maxItems) {
      fail(`${label} ${location} has too many JSON Schema items`);
    }
    if (
      schema.uniqueItems === true &&
      new Set(value.map((item) => renderJson(item))).size !== value.length
    ) {
      fail(`${label} ${location} has duplicate JSON Schema items`);
    }
    if (schema.items !== undefined) {
      for (const [index, item] of value.entries()) {
        validateJsonSchema(
          item,
          schema.items,
          rootSchema,
          label,
          `${location}[${index}]`,
        );
      }
    }
  }
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const properties = schema.properties ?? {};
    assertObject(properties, `${label} schema properties at ${location}`);
    if (schema.required !== undefined) {
      if (!Array.isArray(schema.required)) {
        fail(`${label} schema required at ${location} must be an array`);
      }
      for (const key of schema.required) {
        if (typeof key !== "string" || !Object.hasOwn(value, key)) {
          fail(
            `${label} ${location} is missing required property ${String(key)}`,
          );
        }
      }
    }
    if (schema.additionalProperties === false) {
      const unknown = Object.keys(value).filter(
        (key) => !Object.hasOwn(properties, key),
      );
      if (unknown.length > 0) {
        fail(
          `${label} ${location} has undeclared properties: ${unknown.join(", ")}`,
        );
      }
    }
    for (const [key, propertySchema] of Object.entries(properties)) {
      if (Object.hasOwn(value, key)) {
        validateJsonSchema(
          value[key],
          propertySchema,
          rootSchema,
          label,
          `${location}.${key}`,
        );
      }
    }
  }
}

function validateConformance(payload, descriptorState) {
  const document = parseJson(
    payload,
    "conformance/external-sdk-ingestion.json",
  );
  if (
    document.$schema !== "schemas/fixture-document.schema.json" ||
    document.schemaVersion !== 1 ||
    document.category !== "external_sdk_ingestion" ||
    !Array.isArray(document.fixtures) ||
    document.fixtures.length !== 2
  ) {
    fail("external SDK conformance fixture has unsupported metadata");
  }
  const registry = createFileRegistry(descriptorState.index.descriptor);
  const methods = new Map(
    descriptorState.methods.map((method) => [method.name, method]),
  );
  const expectedFixtureMethods = ["PublishCdcEvents", "PublishAuditEvents"];
  for (const [index, fixture] of document.fixtures.entries()) {
    const methodName = expectedFixtureMethods[index];
    const method = methods.get(methodName);
    if (
      fixture.protocol?.service !== CONNECT_SERVICE ||
      fixture.protocol?.method !== methodName ||
      fixture.protocol?.path !== `/${CONNECT_SERVICE}/${methodName}` ||
      fixture.kind !== "transport_exchange"
    ) {
      fail(`conformance fixture ${fixture.id ?? index} has invalid RPC scope`);
    }
    const requestSchema = registry.getMessage(
      normalizeTypeName(method.inputType),
    );
    const responseSchema = registry.getMessage(
      normalizeTypeName(method.outputType),
    );
    roundTripJson(requestSchema, fixture.input?.body, `${fixture.id} request`);
    roundTripJson(
      responseSchema,
      fixture.expected?.body,
      `${fixture.id} response`,
    );
    const receipts = fixture.expected?.body?.events;
    if (
      fixture.expected?.receiptDispositions?.join(",") !==
        "accepted,duplicate" ||
      fixture.input?.headers?.["x-medallion-workspace-id"] === undefined ||
      !Array.isArray(receipts) ||
      receipts.length !== 2 ||
      receipts[0].duplicate !== undefined ||
      receipts[1].duplicate !== true ||
      !receipts.every((receipt) => typeof receipt.eventId === "string")
    ) {
      fail(
        `conformance fixture ${fixture.id} lacks required ingestion evidence`,
      );
    }
  }
  return document.fixtures.length;
}

function validateSchemas(state) {
  const fixtureSchema = parseJson(
    artifact(state, "conformance/schemas/fixture-document.schema.json"),
    "fixture document schema",
  );
  const protocol = fixtureSchema.$defs?.fixture?.properties?.protocol;
  if (
    fixtureSchema.properties?.category?.const !== "external_sdk_ingestion" ||
    renderJson(protocol?.properties?.service?.enum) !==
      renderJson([CONNECT_SERVICE]) ||
    protocol?.properties?.path?.pattern !==
      "^/medallion\\.connect\\.v1\\.MedallionConnectService/(PublishCdcEvents|PublishAuditEvents|ListCdcEvents|ListAuditEvents)$" ||
    renderJson(protocol?.properties?.method?.enum) !==
      renderJson(EXPECTED_PROFILE_METHODS)
  ) {
    fail("fixture schema must be Connect-ingestion-only");
  }
  const manifestSchema = parseJson(
    artifact(state, "schemas/external-ingestion-sdk-v1.schema.json"),
    "external ingestion manifest schema",
  );
  if (
    manifestSchema.properties?.profile?.const !== PROFILE_ID ||
    manifestSchema.properties?.service?.const !== CONNECT_SERVICE
  ) {
    fail("external ingestion manifest schema has invalid scope");
  }
  const errorSchema = parseJson(
    artifact(state, "schemas/error-reasons.schema.json"),
    "error reason schema",
  );
  if (errorSchema.properties?.domain?.const !== ERROR_INFO_DOMAIN) {
    fail("error reason schema has invalid domain");
  }
  return { errorSchema, fixtureSchema, manifestSchema };
}

function attestedArtifacts(state) {
  return state.bundle.artifacts
    .filter((entry) => entry.path !== "release-attestation.json")
    .map((entry) => ({ path: entry.path, sha256: entry.sha256 }))
    .toSorted((left, right) => left.path.localeCompare(right.path));
}

function validateReleaseAttestation(state) {
  const attestation = parseJson(
    artifact(state, "release-attestation.json"),
    "release-attestation.json",
  );
  assertExactKeys(
    attestation,
    [
      "artifact_set_sha256",
      "artifacts",
      "attestation",
      "contract_version",
      "immutable",
      "release_id",
      "released",
      "schema_version",
    ],
    "release attestation",
  );
  const expectedArtifacts = attestedArtifacts(state);
  const commonEvidenceIsInvalid =
    attestation.schema_version !== 1 ||
    attestation.contract_version !== state.bundle.contract_version ||
    renderJson(attestation.artifacts) !== renderJson(expectedArtifacts) ||
    attestation.artifact_set_sha256 !==
      sha256(Buffer.from(renderJson(expectedArtifacts)));
  if (commonEvidenceIsInvalid) {
    fail("contract attestation does not authenticate this export");
  }
  const releaseEvidenceIsValid =
    state.bundle.release_status === "released" &&
    attestation.attestation === "immutable_release" &&
    attestation.immutable === true &&
    attestation.released === true &&
    typeof attestation.release_id === "string" &&
    /^sdkrel_[0-9a-z]{26}$/.test(attestation.release_id);
  const candidateEvidenceIsValid =
    state.bundle.release_status === "unreleased_candidate" &&
    attestation.attestation === "unreleased_candidate" &&
    attestation.immutable === false &&
    attestation.released === false &&
    attestation.release_id === null;
  if (!releaseEvidenceIsValid && !candidateEvidenceIsValid) {
    fail("release attestation disagrees with the bundle release status");
  }
  return attestation;
}

function runBuf(args, label) {
  const result = spawnSync(BUF, args, {
    cwd: SDK_ROOT,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.error) {
    fail(`cannot run ${BUF}: ${result.error.message}; activate Flox`);
  }
  if (result.status !== 0) {
    fail(`${label} failed: ${(result.stderr || result.stdout).trim()}`);
  }
  return result.stdout;
}

const PROTO_SCALAR_TYPES = Object.freeze({
  1: "double",
  2: "float",
  3: "int64",
  4: "uint64",
  5: "int32",
  6: "fixed64",
  7: "fixed32",
  8: "bool",
  9: "string",
  12: "bytes",
  13: "uint32",
  15: "sfixed32",
  16: "sfixed64",
  17: "sint32",
  18: "sint64",
});

function protoType(field, packageName) {
  if (field.type === 11 || field.type === 14) {
    const typeName = normalizeTypeName(field.typeName);
    return typeName.startsWith(`${packageName}.`)
      ? typeName.slice(packageName.length + 1)
      : typeName;
  }
  const scalar = PROTO_SCALAR_TYPES[field.type];
  if (!scalar) fail(`cannot render protobuf scalar type ${field.type}`);
  return scalar;
}

function collectValidationProjection(descriptorState) {
  const validationFile = descriptorState.index.files.get(
    "buf/validate/validate.proto",
  );
  if (!validationFile) fail("descriptor lacks buf.validate source metadata");
  const registry = createFileRegistry(descriptorState.index.descriptor);
  const validationExtension = registry.getExtension("buf.validate.field");
  if (!validationExtension) fail("descriptor lacks buf.validate.field");
  const selected = new Map([["FieldRules", new Set()]]);
  for (const message of descriptorState.index.files.get(
    "medallion/connect/v1/connect.proto",
  ).messageType) {
    const reflectionMessage = registry.getMessage(
      `${CONNECT_PACKAGE}.${message.name}`,
    );
    for (const reflectionField of reflectionMessage.fields) {
      const value = getOption(reflectionField, validationExtension);
      if (value === undefined) continue;
      const json = toJson(validationExtension.message, value);
      for (const [fieldName, fieldValue] of Object.entries(json)) {
        selected.get("FieldRules").add(fieldName);
        if (
          fieldValue === null ||
          typeof fieldValue !== "object" ||
          Array.isArray(fieldValue)
        ) {
          continue;
        }
        const field = validationFile.messageType
          .find((message) => message.name === "FieldRules")
          ?.field.find(
            (candidate) =>
              candidate.name === fieldName || candidate.jsonName === fieldName,
          );
        const typeName = normalizeTypeName(field?.typeName ?? "");
        if (!typeName.startsWith("buf.validate.")) {
          fail(`cannot project validation rule ${fieldName}`);
        }
        const messageName = typeName.slice("buf.validate.".length);
        const fields = selected.get(messageName) ?? new Set();
        for (const nestedName of Object.keys(fieldValue))
          fields.add(nestedName);
        selected.set(messageName, fields);
      }
    }
  }
  return { selected, validationFile };
}

function protoFieldLine(field, packageName, indentation, includeLabel) {
  const labels = { 1: "optional", 2: "required", 3: "repeated" };
  const label = includeLabel ? `${labels[field.label]} ` : "";
  if (includeLabel && labels[field.label] === undefined) {
    fail(`cannot render protobuf label ${field.label}`);
  }
  return `${" ".repeat(indentation)}${label}${protoType(field, packageName)} ${field.name} = ${field.number};`;
}

function explicitOneofIndex(field) {
  const json = toJson(FieldDescriptorProtoSchema, field);
  return Object.hasOwn(json, "oneofIndex") ? field.oneofIndex : undefined;
}

function renderProjectedValidationMessage(
  message,
  selectedFields,
  packageName,
) {
  const selected = message.field.filter(
    (field) =>
      selectedFields.has(field.name) || selectedFields.has(field.jsonName),
  );
  if (selected.length !== selectedFields.size) {
    fail(`cannot resolve every projected validation field in ${message.name}`);
  }
  const units = selected
    .filter((field) => explicitOneofIndex(field) === undefined)
    .map((field) => ({ fields: [field], number: field.number, oneof: null }));
  for (const [index, oneof] of message.oneofDecl.entries()) {
    const fields = selected.filter(
      (field) => explicitOneofIndex(field) === index,
    );
    if (fields.length > 0) {
      units.push({
        fields,
        number: Math.min(...fields.map((field) => field.number)),
        oneof: oneof.name,
      });
    }
  }
  units.sort((left, right) => left.number - right.number);
  const lines = [`message ${message.name} {`, ...renderReserved(message)];
  for (const unit of units) {
    if (unit.oneof === null) {
      lines.push(protoFieldLine(unit.fields[0], packageName, 2, true));
      continue;
    }
    lines.push(`  oneof ${unit.oneof} {`);
    for (const field of unit.fields.toSorted(
      (left, right) => left.number - right.number,
    )) {
      lines.push(protoFieldLine(field, packageName, 4, false));
    }
    lines.push("  }");
  }
  lines.push("}", "");
  return lines;
}

function renderValidationProtoSource(descriptorState) {
  const { selected, validationFile } =
    collectValidationProjection(descriptorState);
  const fieldExtension = validationFile.extension.find(
    (extension) => extension.name === "field",
  );
  if (
    fieldExtension?.extendee !== ".google.protobuf.FieldOptions" ||
    normalizeTypeName(fieldExtension.typeName) !== "buf.validate.FieldRules"
  ) {
    fail("descriptor has an unsupported buf.validate.field extension");
  }
  const lines = [
    "// Code generated by scripts/sync_external_ingestion_contract.mjs. DO NOT EDIT.",
    "// This is the exact validation-rule projection used by the ingestion messages.",
    'syntax = "proto2";',
    "",
    `package ${validationFile.package};`,
    "",
    'import "google/protobuf/descriptor.proto";',
    "",
    `option go_package = ${JSON.stringify(validationFile.options?.goPackage)};`,
    "",
  ];
  for (const message of validationFile.messageType) {
    const selectedFields = selected.get(message.name);
    if (selectedFields !== undefined) {
      lines.push(
        ...renderProjectedValidationMessage(
          message,
          selectedFields,
          validationFile.package,
        ),
      );
    }
  }
  if (
    [...selected.keys()].some(
      (name) =>
        !validationFile.messageType.some((message) => message.name === name),
    )
  ) {
    fail("descriptor is missing a projected validation rule message");
  }
  lines.push(
    "extend google.protobuf.FieldOptions {",
    protoFieldLine(fieldExtension, validationFile.package, 2, true),
    "}",
    "",
  );
  return Buffer.from(formatProtoSource(`${lines.join("\n")}\n`));
}

function protoScalarLiteral(field, value) {
  if (field.fieldKind === "enum") return String(value);
  if (field.scalar === 9 || field.scalar === 12) return JSON.stringify(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(value);
}

function renderMessageLiteral(schema, value, indentation) {
  const lines = [];
  for (const [jsonName, item] of Object.entries(value)) {
    const field = schema.fields.find(
      (candidate) =>
        candidate.jsonName === jsonName || candidate.name === jsonName,
    );
    if (!field) {
      fail(`cannot render unknown option field ${schema.typeName}.${jsonName}`);
    }
    if (field.fieldKind === "message") {
      lines.push(`${" ".repeat(indentation)}${field.name}: {`);
      lines.push(renderMessageLiteral(field.message, item, indentation + 2));
      lines.push(`${" ".repeat(indentation)}}`);
    } else if (field.fieldKind === "list") {
      for (const listItem of item) {
        lines.push(
          `${" ".repeat(indentation)}${field.name}: ${protoScalarLiteral(field, listItem)}`,
        );
      }
    } else {
      lines.push(
        `${" ".repeat(indentation)}${field.name}: ${protoScalarLiteral(field, item)}`,
      );
    }
  }
  return lines.join("\n");
}

function renderValidationOption(reflectionField, validationExtension) {
  const value = getOption(reflectionField, validationExtension);
  if (value === undefined) return "";
  const json = toJson(validationExtension.message, value);
  if (Object.keys(json).length === 0) return "";
  return ` [(buf.validate.field) = {\n${renderMessageLiteral(validationExtension.message, json, 4)}\n  }]`;
}

function renderReserved(message) {
  const lines = [];
  for (const range of message.reservedRange) {
    const end = range.end - 1;
    lines.push(
      `  reserved ${range.start === end ? range.start : `${range.start} to ${end}`};`,
    );
  }
  if (message.reservedName.length > 0) {
    lines.push(
      `  reserved ${message.reservedName.map((name) => JSON.stringify(name)).join(", ")};`,
    );
  }
  return lines;
}

function formatProtoSource(source) {
  const temporaryDirectory = mkdtempSync(
    path.join(tmpdir(), "medallion-sdk-proto-"),
  );
  try {
    const filename = path.join(temporaryDirectory, "connect.proto");
    writeFileSync(filename, source);
    return runBuf(["format", filename], "generated proto formatting");
  } finally {
    rmSync(temporaryDirectory, { force: true, recursive: true });
  }
}

function renderProtoSource(descriptorState) {
  const file = descriptorState.index.files.get(
    "medallion/connect/v1/connect.proto",
  );
  if (!file) fail("descriptor lacks Connect source file metadata");
  const registry = createFileRegistry(descriptorState.index.descriptor);
  const validationExtension = registry.getExtension("buf.validate.field");
  if (!validationExtension) fail("descriptor lacks buf.validate.field");
  const lines = [
    "// Code generated by scripts/sync_external_ingestion_contract.mjs. DO NOT EDIT.",
    'syntax = "proto3";',
    "",
    `package ${file.package};`,
    "",
  ];
  for (const dependency of file.dependency) {
    lines.push(`import "${dependency}";`);
  }
  lines.push("", `option go_package = "${SDK_GO_PACKAGE}";`, "");
  for (const enumeration of file.enumType) {
    lines.push(`enum ${enumeration.name} {`);
    for (const value of enumeration.value) {
      lines.push(`  ${value.name} = ${value.number};`);
    }
    lines.push("}", "");
  }
  for (const message of file.messageType) {
    const reflectionMessage = registry.getMessage(
      `${file.package}.${message.name}`,
    );
    if (!reflectionMessage) fail(`cannot reflect ${message.name}`);
    lines.push(`message ${message.name} {`, ...renderReserved(message));
    for (const field of message.field.toSorted(
      (left, right) => left.number - right.number,
    )) {
      const reflectionField = reflectionMessage.fields.find(
        (candidate) => candidate.number === field.number,
      );
      const label = field.label === 3 ? "repeated " : "";
      const option = renderValidationOption(
        reflectionField,
        validationExtension,
      );
      lines.push(
        `  ${label}${protoType(field, file.package)} ${field.name} = ${field.number}${option};`,
      );
    }
    lines.push("}", "");
  }
  for (const service of file.service) {
    lines.push(`service ${service.name} {`);
    for (const method of service.method) {
      const input = normalizeTypeName(method.inputType).replace(
        `${file.package}.`,
        "",
      );
      const output = normalizeTypeName(method.outputType).replace(
        `${file.package}.`,
        "",
      );
      lines.push(`  rpc ${method.name}(${input}) returns (${output});`);
    }
    lines.push("}", "");
  }
  return Buffer.from(formatProtoSource(`${lines.join("\n")}\n`));
}

function renderTypeScriptErrorPolicyModule(errorPolicy) {
  const entries = errorPolicy.reasons
    .toSorted((left, right) => left.reason.localeCompare(right.reason))
    .map(
      (entry) =>
        `  ${entry.reason}: {\n` +
        `    consumerCategory: ${JSON.stringify(entry.consumer_category)},\n` +
        `    grpcCode: ${JSON.stringify(entry.grpc_code)},\n` +
        `    retryClassification: ${JSON.stringify(entry.retry_policy.classification)},\n` +
        `    reuseIdempotencyKey: ${entry.retry_policy.reuse_idempotency_key},\n` +
        "  },",
    )
    .join("\n");
  return Buffer.from(
    `// Code generated by scripts/sync_external_ingestion_contract.mjs. DO NOT EDIT.\n` +
      `// Source: sealed external-ingestion error registry.\n\n` +
      `export const MEDALLION_ERROR_INFO_DOMAIN = ${JSON.stringify(errorPolicy.domain)} as const;\n\n` +
      `export const MEDALLION_ERROR_REASON_POLICY = {\n${entries}\n} as const;\n\n` +
      `export type MedallionKnownErrorReason =\n` +
      `  keyof typeof MEDALLION_ERROR_REASON_POLICY;\n`,
  );
}

function reasonIdentifier(reason) {
  return reason
    .split("_")
    .map((part) => `${part[0]}${part.slice(1).toLowerCase()}`)
    .join("");
}

function formatGoSource(source) {
  const result = spawnSync("gofmt", [], {
    cwd: SDK_ROOT,
    encoding: "utf8",
    input: source,
    stdio: ["pipe", "pipe", "pipe"],
  });
  if (result.status !== 0) {
    fail(`generated Go formatting failed: ${result.stderr.trim()}`);
  }
  return Buffer.from(result.stdout);
}

function renderGoErrorPolicyModule(errorPolicy) {
  const reasons = errorPolicy.reasons.toSorted((left, right) =>
    left.reason.localeCompare(right.reason),
  );
  const constants = reasons
    .map(
      (entry) =>
        `\tErrorReason${reasonIdentifier(entry.reason)} KnownErrorReason = ${JSON.stringify(entry.reason)}`,
    )
    .join("\n");
  const entries = reasons
    .map(
      (entry) =>
        `\tErrorReason${reasonIdentifier(entry.reason)}: {\n` +
        `\t\tConsumerCategory:       ${JSON.stringify(entry.consumer_category)},\n` +
        `\t\tGRPCCode:               ${JSON.stringify(entry.grpc_code)},\n` +
        `\t\tRetryClassification:    ${JSON.stringify(entry.retry_policy.classification)},\n` +
        `\t\tReuseIdempotencyKey:    ${entry.retry_policy.reuse_idempotency_key},\n` +
        "\t},",
    )
    .join("\n");
  return formatGoSource(
    `// Code generated by scripts/sync_external_ingestion_contract.mjs. DO NOT EDIT.\n\n` +
      `package medallion\n\n` +
      `// KnownErrorReason is a stable ErrorInfo reason from the external-ingestion contract.\n` +
      `type KnownErrorReason string\n\n` +
      `const (\n` +
      `\t// ErrorInfoDomain is the canonical google.rpc.ErrorInfo domain.\n` +
      `\tErrorInfoDomain = ${JSON.stringify(errorPolicy.domain)}\n\n` +
      `${constants}\n` +
      `)\n\n` +
      `type generatedErrorReasonPolicy struct {\n` +
      `\tConsumerCategory       string\n` +
      `\tGRPCCode               string\n` +
      `\tRetryClassification    string\n` +
      `\tReuseIdempotencyKey    bool\n` +
      `}\n\n` +
      `var generatedErrorReasonPolicies = map[KnownErrorReason]generatedErrorReasonPolicy{\n` +
      `${entries}\n` +
      `}\n`,
  );
}

function renderPythonErrorPolicyModule(errorPolicy) {
  const reasons = errorPolicy.reasons.toSorted((left, right) =>
    left.reason.localeCompare(right.reason),
  );
  const enumEntries = reasons
    .map((entry) => `    ${entry.reason} = ${JSON.stringify(entry.reason)}`)
    .join("\n");
  const policyEntries = reasons
    .map(
      (entry) =>
        `    KnownErrorReason.${entry.reason}: ErrorReasonPolicy(\n` +
        `        consumer_category=${JSON.stringify(entry.consumer_category)},\n` +
        `        grpc_code=${JSON.stringify(entry.grpc_code)},\n` +
        `        retry_classification=${JSON.stringify(entry.retry_policy.classification)},\n` +
        `        reuse_idempotency_key=${entry.retry_policy.reuse_idempotency_key ? "True" : "False"},\n` +
        `    ),`,
    )
    .join("\n");
  return Buffer.from(
    `# Code generated by scripts/sync_external_ingestion_contract.mjs. DO NOT EDIT.\n\n` +
      `from enum import StrEnum\n` +
      `from typing import Final, NamedTuple\n\n\n` +
      `class KnownErrorReason(StrEnum):\n${enumEntries}\n\n\n` +
      `class ErrorReasonPolicy(NamedTuple):\n` +
      `    consumer_category: str\n` +
      `    grpc_code: str\n` +
      `    retry_classification: str\n` +
      `    reuse_idempotency_key: bool\n\n\n` +
      `KNOWN_ERROR_DOMAIN: Final = ${JSON.stringify(errorPolicy.domain)}\n\n` +
      `REASON_POLICIES: Final[dict[KnownErrorReason, ErrorReasonPolicy]] = {\n` +
      `${policyEntries}\n` +
      `}\n`,
  );
}

function deriveArtifacts(validated) {
  const allowlist = {
    methods: validated.manifest.methods,
    profile: PROFILE_ID,
    schema_version: 1,
    service: CONNECT_SERVICE,
  };
  return {
    allowlist: Buffer.from(renderJson(allowlist)),
    descriptor: artifact(validated.state, "descriptor.binpb"),
    errorPolicyGo: renderGoErrorPolicyModule(validated.errorPolicy),
    errorPolicyPython: renderPythonErrorPolicyModule(validated.errorPolicy),
    errorPolicyTypeScript: renderTypeScriptErrorPolicyModule(
      validated.errorPolicy,
    ),
    proto: renderProtoSource(validated.descriptorState),
    validationProto: renderValidationProtoSource(validated.descriptorState),
    routes: Buffer.from(
      renderJson({
        connect: {
          methods: EXPECTED_PROFILE_METHODS,
          service: CONNECT_SERVICE,
        },
      }),
    ),
  };
}

function validateBundleState(state) {
  const descriptorPayload = artifact(state, "descriptor.binpb");
  const descriptorState = validateDescriptor(descriptorPayload);
  const schemas = validateSchemas(state);
  const manifestPayload = artifact(state, MANIFEST_NAME);
  validateJsonSchema(
    parseJson(manifestPayload, MANIFEST_NAME),
    schemas.manifestSchema,
    schemas.manifestSchema,
    MANIFEST_NAME,
  );
  const manifest = validateManifest(
    manifestPayload,
    descriptorPayload,
    descriptorState,
  );
  const idempotencyPolicy = validateIdempotencyPolicy(
    artifact(state, "connect-idempotency-policy.json"),
  );
  const errorPolicyDocument = parseJson(
    artifact(state, "error-reasons.json"),
    "error-reasons.json",
  );
  validateJsonSchema(
    errorPolicyDocument,
    schemas.errorSchema,
    schemas.errorSchema,
    "error-reasons.json",
  );
  const errorPolicy = validateErrorReasonPolicy(errorPolicyDocument);
  const conformancePayload = artifact(
    state,
    "conformance/external-sdk-ingestion.json",
  );
  validateJsonSchema(
    parseJson(conformancePayload, "conformance/external-sdk-ingestion.json"),
    schemas.fixtureSchema,
    schemas.fixtureSchema,
    "conformance/external-sdk-ingestion.json",
  );
  const fixtureCount = validateConformance(conformancePayload, descriptorState);
  const attestation = validateReleaseAttestation(state);
  return {
    attestation,
    descriptorState,
    errorPolicy,
    fixtureCount,
    idempotencyPolicy,
    manifest,
    state,
  };
}

function assertBytes(actual, expected, label) {
  if (!actual.equals(expected)) {
    fail(`${label} has drifted; run make contract-sync`);
  }
}

function readRequired(filename, label) {
  if (!existsSync(filename)) fail(`missing ${label}`);
  return readFileSync(filename);
}

function writeVendoredExport(state, destination) {
  const parent = path.dirname(destination);
  mkdirSync(parent, { recursive: true });
  const staging = mkdtempSync(path.join(parent, ".contract-stage-"));
  const previous = `${destination}.previous-${process.pid}`;
  try {
    writeFileSync(path.join(staging, "bundle.json"), state.bundlePayload);
    writeFileSync(path.join(staging, "bundle.sha256"), state.checksumPayload);
    for (const entry of state.bundle.artifacts) {
      const filename = path.join(staging, ...entry.path.split("/"));
      mkdirSync(path.dirname(filename), { recursive: true });
      writeFileSync(filename, state.artifacts.get(entry.path));
    }
    if (existsSync(destination)) renameSync(destination, previous);
    renameSync(staging, destination);
    rmSync(previous, { force: true, recursive: true });
  } catch (error) {
    if (!existsSync(destination) && existsSync(previous)) {
      renameSync(previous, destination);
    }
    throw error;
  } finally {
    rmSync(staging, { force: true, recursive: true });
    rmSync(previous, { force: true, recursive: true });
  }
}

function validateGeneratedScopeSurfaces(sdkRoot) {
  for (const relative of [
    "go/gen/medallion/connect/v1/connect.pb.go",
    "python/src/medallion/connect/v1/connect_pb2.py",
  ]) {
    const filename = path.join(sdkRoot, relative);
    if (!existsSync(filename)) continue;
    const source = readFileSync(filename, "utf8");
    const retired = ["Organization", "Tenant"].flatMap((scope) => [
      `${scope}Id`,
      `Get${scope}Id`,
    ]);
    if (new RegExp(`\\b(?:${retired.join("|")})\\b`).test(source)) {
      fail(`${relative} exposes an active legacy scope field`);
    }
    scanBoundary(Buffer.from(source), relative);
  }
}

function resultFrom(validated) {
  return {
    attestationSha256: sha256(
      artifact(validated.state, "release-attestation.json"),
    ),
    bundleSha256: validated.state.bundleHash,
    connectPolicySha256: sha256(
      artifact(validated.state, "connect-idempotency-policy.json"),
    ),
    contractVersion: validated.state.bundle.contract_version,
    descriptorSha256: sha256(artifact(validated.state, "descriptor.binpb")),
    enumCount: validated.descriptorState.enumCount,
    errorReasonPolicySha256: sha256(
      artifact(validated.state, "error-reasons.json"),
    ),
    fixtureCount: validated.fixtureCount,
    fixtureSha256: sha256(
      artifact(validated.state, "conformance/external-sdk-ingestion.json"),
    ),
    manifestSha256: sha256(artifact(validated.state, MANIFEST_NAME)),
    messageCount: validated.descriptorState.messageCount,
    methods: [...EXPECTED_PROFILE_METHODS],
    profile: PROFILE_ID,
    releaseStatus: validated.state.bundle.release_status,
    service: CONNECT_SERVICE,
    wireClosureSha256: validated.descriptorState.wireClosureSha256,
  };
}

export function syncContract(contractRoot, sdkRoot = SDK_ROOT) {
  const sourceDirectory = path.resolve(contractRoot);
  const sourceState = readBundle(sourceDirectory, "sanitized contract export");
  const validated = validateBundleState(sourceState);
  const derived = deriveArtifacts(validated);
  const vendorDirectory = path.join(sdkRoot, VENDOR_RELATIVE);
  writeVendoredExport(sourceState, vendorDirectory);
  atomicWrite(
    path.join(sdkRoot, LOCAL_DESCRIPTOR_RELATIVE),
    derived.descriptor,
  );
  atomicWrite(path.join(sdkRoot, LOCAL_PROTO_RELATIVE), derived.proto);
  atomicWrite(
    path.join(sdkRoot, VALIDATION_PROTO_RELATIVE),
    derived.validationProto,
  );
  atomicWrite(path.join(sdkRoot, ALLOWLIST_RELATIVE), derived.allowlist);
  atomicWrite(path.join(sdkRoot, SUPPORTED_ROUTES_RELATIVE), derived.routes);
  atomicWrite(
    path.join(sdkRoot, TYPESCRIPT_ERROR_POLICY_RELATIVE),
    derived.errorPolicyTypeScript,
  );
  atomicWrite(
    path.join(sdkRoot, GO_ERROR_POLICY_RELATIVE),
    derived.errorPolicyGo,
  );
  atomicWrite(
    path.join(sdkRoot, PYTHON_ERROR_POLICY_RELATIVE),
    derived.errorPolicyPython,
  );
  return checkContract(sdkRoot);
}

export function checkContract(sdkRoot = SDK_ROOT) {
  const vendorDirectory = path.join(sdkRoot, VENDOR_RELATIVE);
  const state = readBundle(vendorDirectory, "vendored SDK contract");
  const validated = validateBundleState(state);
  const derived = deriveArtifacts(validated);
  for (const [relative, expected] of [
    [LOCAL_DESCRIPTOR_RELATIVE, derived.descriptor],
    [LOCAL_PROTO_RELATIVE, derived.proto],
    [VALIDATION_PROTO_RELATIVE, derived.validationProto],
    [ALLOWLIST_RELATIVE, derived.allowlist],
    [SUPPORTED_ROUTES_RELATIVE, derived.routes],
    [TYPESCRIPT_ERROR_POLICY_RELATIVE, derived.errorPolicyTypeScript],
    [GO_ERROR_POLICY_RELATIVE, derived.errorPolicyGo],
    [PYTHON_ERROR_POLICY_RELATIVE, derived.errorPolicyPython],
  ]) {
    assertBytes(
      readRequired(path.join(sdkRoot, relative), relative),
      expected,
      relative,
    );
  }
  validateGeneratedScopeSurfaces(sdkRoot);
  return resultFrom(validated);
}

export function checkReleaseContract(sdkRoot = SDK_ROOT) {
  const result = checkContract(sdkRoot);
  if (result.releaseStatus !== "released") {
    fail(`release check blocked by ${result.releaseStatus} contract`);
  }
  return result;
}

function parseArguments(argv) {
  let mode = null;
  let contractRoot = null;
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (["--check", "--check-release", "--sync"].includes(argument)) {
      if (mode !== null) {
        fail("choose exactly one of --check, --check-release, or --sync");
      }
      mode = argument.slice(2);
    } else if (argument === "--contract-root") {
      contractRoot = argv[++index];
      if (!contractRoot) fail("--contract-root requires a path");
    } else {
      fail(`unknown argument ${argument}`);
    }
  }
  if (mode === null) {
    fail("choose exactly one of --check, --check-release, or --sync");
  }
  if (mode !== "sync" && contractRoot !== null) {
    fail("offline checks do not accept --contract-root");
  }
  return { contractRoot, mode };
}

function printResult(action, result) {
  process.stdout.write(
    `${[
      `External ingestion contract ${action}: ${result.methods.length} RPCs (${result.profile})`,
      `contract_version=${result.contractVersion}`,
      `bundle=${result.bundleSha256} (${result.releaseStatus})`,
      `descriptor=${result.descriptorSha256}`,
      `manifest=${result.manifestSha256}`,
      `connect_policy=${result.connectPolicySha256}`,
      `error_registry=${result.errorReasonPolicySha256}`,
      `conformance_fixture=${result.fixtureSha256} (${result.fixtureCount} cases)`,
      `release_attestation=${result.attestationSha256}`,
      `wire_closure=${result.wireClosureSha256} (${result.messageCount} messages, ${result.enumCount} enums)`,
      `public_rpcs=${result.service}=4`,
    ].join("\n")}\n`,
  );
}

async function main() {
  const { contractRoot, mode } = parseArguments(process.argv.slice(2));
  if (mode === "check") return printResult("is current", checkContract());
  if (mode === "check-release") {
    return printResult("is release-ready", checkReleaseContract());
  }
  const configuredRoot =
    contractRoot ??
    process.env.MEDALLION_SDK_CONTRACT_ROOT ??
    path.join(SDK_ROOT, VENDOR_RELATIVE);
  return printResult(
    "synchronized",
    syncContract(path.resolve(configuredRoot)),
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
