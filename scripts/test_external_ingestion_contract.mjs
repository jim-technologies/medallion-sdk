#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

import { fromBinary } from "@bufbuild/protobuf";
import { FileDescriptorSetSchema } from "@bufbuild/protobuf/wkt";

import {
  checkContract,
  checkReleaseContract,
  EXPECTED_ERROR_REASONS,
  EXPECTED_PROFILE_METHODS,
  PROFILE_ID,
  renderJson,
  SDK_ROOT,
  sha256,
  syncContract,
} from "./sync_external_ingestion_contract.mjs";

const VENDOR_RELATIVE = "proto/external-ingestion-contract/v1";
const LOCAL_DESCRIPTOR_RELATIVE =
  "proto/external-ingestion-v1.descriptor.binpb";
const LOCAL_PROTO_RELATIVE = "proto/medallion/connect/v1/connect.proto";
const VALIDATION_PROTO_RELATIVE = "proto/buf/validate/validate.proto";
const ALLOWLIST_RELATIVE = "proto/external-ingestion-v1.json";
const ROUTES_RELATIVE = "proto/client-facing-routes.json";
const TYPESCRIPT_ERROR_POLICY_RELATIVE = "src/error-policy.ts";
const GO_ERROR_POLICY_RELATIVE = "go/error_policy_generated.go";
const PYTHON_ERROR_POLICY_RELATIVE =
  "python/src/medallion/error_policy_generated.py";
const CONNECT_SERVICE = "medallion.connect.v1.MedallionConnectService";
const RETIRED_SCOPE_FIELD = ["organization", "id"].join("_");
const DERIVED_RELATIVES = [
  LOCAL_DESCRIPTOR_RELATIVE,
  LOCAL_PROTO_RELATIVE,
  VALIDATION_PROTO_RELATIVE,
  ALLOWLIST_RELATIVE,
  ROUTES_RELATIVE,
  TYPESCRIPT_ERROR_POLICY_RELATIVE,
  GO_ERROR_POLICY_RELATIVE,
  PYTHON_ERROR_POLICY_RELATIVE,
];
const FORBIDDEN = [
  ["Medallion", ["Onto", "logy"].join(""), "Service"].join(""),
  ["medallion", ["onto", "logy"].join(""), "v1"].join("."),
  [["onto", "logy"].join(""), "idempotency", "policy"].join("-"),
  [["ter", "minal"].join(""), ["com", "pass"].join("")].join("_"),
  ["MEDALLION", ["ONTO", "LOGY"].join(""), "ROOT"].join("_"),
  ["medallion", ["onto", "logy"].join("")].join("-"),
  ["medallion", "connect"].join("-"),
];
let fixtureNumber = 0;

function readJson(filename) {
  return JSON.parse(readFileSync(filename, "utf8"));
}

function createSdkFixture() {
  fixtureNumber += 1;
  const fixture = mkdtempSync(
    path.join(tmpdir(), `external-ingestion-contract-${fixtureNumber}-`),
  );
  for (const relative of [VENDOR_RELATIVE, ...DERIVED_RELATIVES]) {
    const source = path.join(SDK_ROOT, relative);
    const destination = path.join(fixture, relative);
    mkdirSync(path.dirname(destination), { recursive: true });
    cpSync(source, destination, { recursive: true });
  }
  return fixture;
}

function withSdkFixture(callback) {
  const fixture = createSdkFixture();
  try {
    return callback(fixture);
  } finally {
    rmSync(fixture, { force: true, recursive: true });
  }
}

function createContractExport(sdkFixture) {
  const exportRoot = path.join(sdkFixture, "sanitized-export");
  cpSync(path.join(sdkFixture, VENDOR_RELATIVE), exportRoot, {
    recursive: true,
  });
  return exportRoot;
}

function resealExport(exportRoot, transform = () => {}) {
  const bundlePath = path.join(exportRoot, "bundle.json");
  const attestationPath = path.join(exportRoot, "release-attestation.json");
  const bundle = readJson(bundlePath);
  const attestation = readJson(attestationPath);
  transform({ attestation, bundle });
  for (const entry of attestation.artifacts) {
    entry.sha256 = sha256(readFileSync(path.join(exportRoot, entry.path)));
  }
  attestation.artifact_set_sha256 = sha256(
    Buffer.from(renderJson(attestation.artifacts)),
  );
  writeFileSync(attestationPath, renderJson(attestation));
  for (const entry of bundle.artifacts) {
    const payload = readFileSync(path.join(exportRoot, entry.path));
    entry.bytes = payload.length;
    entry.sha256 = sha256(payload);
  }
  writeFileSync(bundlePath, renderJson(bundle));
  writeFileSync(
    path.join(exportRoot, "bundle.sha256"),
    `${sha256(readFileSync(bundlePath))}  bundle.json\n`,
  );
}

function recursiveFiles(root, prefix = "") {
  const files = [];
  for (const entry of readdirSync(path.join(root, prefix), {
    withFileTypes: true,
  })) {
    const relative = path.posix.join(prefix, entry.name);
    if (entry.isDirectory()) files.push(...recursiveFiles(root, relative));
    if (entry.isFile()) files.push(relative);
  }
  return files.toSorted();
}

function snapshot(fixture) {
  const files = [
    ...DERIVED_RELATIVES,
    ...recursiveFiles(path.join(fixture, VENDOR_RELATIVE)).map((relative) =>
      path.posix.join(VENDOR_RELATIVE, relative),
    ),
  ].toSorted();
  return new Map(
    files.map((relative) => [
      relative,
      readFileSync(path.join(fixture, relative)),
    ]),
  );
}

function assertSnapshotsEqual(actual, expected) {
  assert.deepEqual([...actual.keys()], [...expected.keys()]);
  for (const [relative, payload] of actual) {
    assert.deepEqual(payload, expected.get(relative), relative);
  }
}

function bundleState(root = SDK_ROOT) {
  const directory = path.join(root, VENDOR_RELATIVE);
  const bundle = readJson(path.join(directory, "bundle.json"));
  return {
    bundle,
    directory,
    entries: new Map(bundle.artifacts.map((entry) => [entry.path, entry])),
  };
}

test("offline check authenticates the standalone four-RPC contract", () => {
  const result = checkContract();
  const state = bundleState();
  assert.equal(result.profile, PROFILE_ID);
  assert.deepEqual(result.methods, EXPECTED_PROFILE_METHODS);
  assert.equal(result.releaseStatus, "unreleased_candidate");
  assert.match(result.contractVersion, /^sdkc_[0-9a-z]{26}$/);
  assert.equal(
    result.bundleSha256,
    sha256(readFileSync(path.join(state.directory, "bundle.json"))),
  );
  assert.equal(
    result.descriptorSha256,
    state.entries.get("descriptor.binpb").sha256,
  );
  assert.equal(
    result.manifestSha256,
    state.entries.get("external_ingestion_sdk_v1.json").sha256,
  );
  assert.equal(result.messageCount, 12);
  assert.equal(result.enumCount, 3);
});

test("bundle contains exactly the minimal sealed export", () => {
  const { bundle, directory } = bundleState();
  const expected = [
    "conformance/external-sdk-ingestion.json",
    "conformance/schemas/fixture-document.schema.json",
    "connect-idempotency-policy.json",
    "descriptor.binpb",
    "error-reasons.json",
    "external_ingestion_sdk_v1.json",
    "release-attestation.json",
    "schemas/error-reasons.schema.json",
    "schemas/external-ingestion-sdk-v1.schema.json",
  ];
  assert.deepEqual(
    bundle.artifacts.map((entry) => entry.path),
    expected,
  );
  for (const entry of bundle.artifacts) {
    const payload = readFileSync(path.join(directory, entry.path));
    assert.equal(payload.length, entry.bytes, entry.path);
    assert.equal(sha256(payload), entry.sha256, entry.path);
  }
  assert.equal(
    readFileSync(path.join(directory, "bundle.sha256"), "ascii"),
    `${sha256(readFileSync(path.join(directory, "bundle.json")))}  bundle.json\n`,
  );
  assert.deepEqual(
    recursiveFiles(directory),
    ["bundle.json", "bundle.sha256", ...expected].toSorted(),
  );
});

test("candidate attestation pins every normative artifact without claiming release", () => {
  const { bundle, directory } = bundleState();
  const attestation = readJson(
    path.join(directory, "release-attestation.json"),
  );
  assert.equal(attestation.attestation, "unreleased_candidate");
  assert.equal(attestation.immutable, false);
  assert.equal(attestation.released, false);
  assert.equal(attestation.contract_version, bundle.contract_version);
  assert.equal(attestation.release_id, null);
  assert.equal(Object.hasOwn(attestation, "source"), false);
  assert.equal(Object.hasOwn(attestation, "repository"), false);
  const expected = bundle.artifacts
    .filter((entry) => entry.path !== "release-attestation.json")
    .map((entry) => ({ path: entry.path, sha256: entry.sha256 }));
  assert.deepEqual(attestation.artifacts, expected);
});

test("descriptor is the exact comment-free workspace-only wire closure", () => {
  const descriptor = fromBinary(
    FileDescriptorSetSchema,
    readFileSync(path.join(SDK_ROOT, LOCAL_DESCRIPTOR_RELATIVE)),
  );
  assert.ok(descriptor.file.every((file) => file.sourceCodeInfo === undefined));
  const connect = descriptor.file.find(
    (file) => file.name === "medallion/connect/v1/connect.proto",
  );
  assert.ok(connect);
  assert.deepEqual(
    descriptor.file
      .flatMap((file) => file.service)
      .map((service) => service.name),
    ["MedallionConnectService"],
  );
  assert.deepEqual(
    connect.service[0].method.map((method) => method.name).toSorted(),
    EXPECTED_PROFILE_METHODS.toSorted(),
  );
  assert.equal(connect.messageType.length, 12);
  assert.equal(connect.enumType.length, 3);
  for (const [messageName, fieldNumber, reservedNumber] of [
    ["CdcEvent", 17, 2],
    ["AuditEvent", 18, 2],
    ["ListCdcEventsRequest", 13, 1],
    ["ListAuditEventsRequest", 15, 1],
  ]) {
    const message = connect.messageType.find(
      (candidate) => candidate.name === messageName,
    );
    assert.equal(
      message.field.find((field) => field.name === "workspace_id")?.number,
      fieldNumber,
    );
    assert.equal(
      message.field.some((field) =>
        /^(organization|tenant)_id$/.test(field.name),
      ),
      false,
    );
    assert.ok(message.reservedName.includes(RETIRED_SCOPE_FIELD));
    assert.ok(
      message.reservedRange.some(
        (range) => range.start <= reservedNumber && range.end > reservedNumber,
      ),
    );
  }
});

test("vendored and derived surfaces contain no implementation-only references", () => {
  const files = [
    ...recursiveFiles(path.join(SDK_ROOT, VENDOR_RELATIVE)).map((relative) =>
      path.join(SDK_ROOT, VENDOR_RELATIVE, relative),
    ),
    ...DERIVED_RELATIVES.map((relative) => path.join(SDK_ROOT, relative)),
  ];
  for (const filename of files) {
    const source = readFileSync(filename).toString("latin1").toLowerCase();
    for (const forbidden of FORBIDDEN) {
      assert.equal(source.includes(forbidden.toLowerCase()), false, filename);
    }
  }
});

test("fixture schema and manifest are Connect-ingestion-only", () => {
  const state = bundleState();
  const schema = readJson(
    path.join(
      state.directory,
      "conformance/schemas/fixture-document.schema.json",
    ),
  );
  assert.equal(schema.properties.category.const, "external_sdk_ingestion");
  assert.deepEqual(
    schema.$defs.fixture.properties.protocol.properties.service.enum,
    [CONNECT_SERVICE],
  );
  assert.deepEqual(
    schema.$defs.fixture.properties.protocol.properties.method.enum,
    EXPECTED_PROFILE_METHODS,
  );
  const manifest = readJson(
    path.join(state.directory, "external_ingestion_sdk_v1.json"),
  );
  assert.equal(manifest.service, CONNECT_SERVICE);
  assert.deepEqual(
    manifest.methods.map((method) => method.name),
    EXPECTED_PROFILE_METHODS,
  );
});

test("official external-ingestion fixtures execute offline", () => {
  const result = checkContract();
  const state = bundleState();
  const fixture = state.entries.get("conformance/external-sdk-ingestion.json");
  assert.equal(result.fixtureCount, 2);
  assert.equal(
    sha256(readFileSync(path.join(state.directory, fixture.path))),
    fixture.sha256,
  );
});

test("generated error policies contain only the seven relevant reasons", () => {
  const state = bundleState();
  const registry = readJson(path.join(state.directory, "error-reasons.json"));
  assert.deepEqual(
    registry.reasons.map((entry) => entry.reason).toSorted(),
    EXPECTED_ERROR_REASONS.toSorted(),
  );
  for (const relative of [
    TYPESCRIPT_ERROR_POLICY_RELATIVE,
    GO_ERROR_POLICY_RELATIVE,
    PYTHON_ERROR_POLICY_RELATIVE,
  ]) {
    const generated = readFileSync(path.join(SDK_ROOT, relative), "utf8");
    for (const entry of registry.reasons) {
      assert.match(generated, new RegExp(`\\b${entry.reason}\\b`), relative);
      assert.match(generated, new RegExp(entry.grpc_code), relative);
      assert.match(
        generated,
        new RegExp(entry.retry_policy.classification),
        relative,
      );
    }
    assert.doesNotMatch(generated, /PROVIDER_UNAVAILABLE|REVISION_CONFLICT/);
  }
});

test("generated-code check detects language binding drift", () => {
  const fixture = mkdtempSync(
    path.join(tmpdir(), `external-ingestion-generated-${process.pid}-`),
  );
  const required = [
    "buf.gen.yaml",
    "go.mod",
    "go.sum",
    LOCAL_DESCRIPTOR_RELATIVE,
    "go/gen/medallion/connect/v1/connect.pb.go",
    "go/gen/medallion/ingest/v1/ingest.pb.go",
    "proto/ingest-v1.descriptor.binpb",
    "proto/medallion/ingest/v1/ingest.proto",
    "python/src/buf/validate/validate_pb2.py",
    "python/src/medallion/connect/v1/connect_pb2.py",
    "python/src/medallion/ingest/v1/ingest_pb2.py",
    "src/connect-descriptor.ts",
    "src/ingest-descriptor.ts",
  ];
  try {
    for (const relative of required) {
      const destination = path.join(fixture, relative);
      mkdirSync(path.dirname(destination), { recursive: true });
      cpSync(path.join(SDK_ROOT, relative), destination);
    }
    const runCheck = () =>
      spawnSync(path.join(SDK_ROOT, "scripts/check_generated.sh"), [], {
        cwd: SDK_ROOT,
        encoding: "utf8",
        env: { ...process.env, MEDALLION_GENERATED_ROOT: fixture },
      });
    const current = runCheck();
    assert.equal(current.status, 0, current.stderr || current.stdout);
    const generatedGo = path.join(
      fixture,
      "go/gen/medallion/connect/v1/connect.pb.go",
    );
    writeFileSync(
      generatedGo,
      Buffer.concat([readFileSync(generatedGo), Buffer.from("\n")]),
    );
    const stale = runCheck();
    assert.notEqual(stale.status, 0);
    assert.match(
      `${stale.stdout}\n${stale.stderr}`,
      /connect\.pb\.go is stale/,
    );
  } finally {
    rmSync(fixture, { force: true, recursive: true });
  }
});

test("synchronization from a generic export is deterministic", () =>
  withSdkFixture((fixture) => {
    const exportRoot = createContractExport(fixture);
    const first = syncContract(exportRoot, fixture);
    const firstBytes = snapshot(fixture);
    const second = syncContract(exportRoot, fixture);
    const secondBytes = snapshot(fixture);
    assertSnapshotsEqual(secondBytes, firstBytes);
    assert.equal(first.contractVersion, second.contractVersion);
    assert.equal(first.releaseStatus, "unreleased_candidate");
  }));

test("source export tampering fails before any SDK destination is rewritten", () =>
  withSdkFixture((fixture) => {
    const exportRoot = createContractExport(fixture);
    const before = snapshot(fixture);
    const policyPath = path.join(exportRoot, "connect-idempotency-policy.json");
    writeFileSync(policyPath, `${readFileSync(policyPath, "utf8")}\n`);
    assert.throws(
      () => syncContract(exportRoot, fixture),
      /differs from its bundle evidence/,
    );
    assertSnapshotsEqual(snapshot(fixture), before);
  }));

test("schema-invalid error policy is rejected even when every checksum is resealed", () =>
  withSdkFixture((fixture) => {
    const exportRoot = createContractExport(fixture);
    const policyPath = path.join(exportRoot, "error-reasons.json");
    const policy = readJson(policyPath);
    policy.reasons[0].retry_policy.classification =
      "schema_forbidden_but_nonempty";
    writeFileSync(policyPath, renderJson(policy));
    resealExport(exportRoot);
    assert.throws(
      () => syncContract(exportRoot, fixture),
      /error-reasons\.json.*retry_policy\.classification.*JSON Schema enum/,
    );
  }));

for (const [relative, pattern] of [
  [LOCAL_DESCRIPTOR_RELATIVE, /descriptor.*drifted/i],
  [LOCAL_PROTO_RELATIVE, /connect\.proto.*drifted/i],
  [VALIDATION_PROTO_RELATIVE, /validate\.proto.*drifted/i],
  [ALLOWLIST_RELATIVE, /external-ingestion-v1\.json.*drifted/i],
  [ROUTES_RELATIVE, /client-facing-routes\.json.*drifted/i],
  [TYPESCRIPT_ERROR_POLICY_RELATIVE, /error-policy\.ts.*drifted/i],
  [GO_ERROR_POLICY_RELATIVE, /error_policy_generated\.go.*drifted/i],
  [PYTHON_ERROR_POLICY_RELATIVE, /error_policy_generated\.py.*drifted/i],
]) {
  test(`offline check detects derived drift in ${relative}`, () =>
    withSdkFixture((fixture) => {
      const filename = path.join(fixture, relative);
      writeFileSync(
        filename,
        Buffer.concat([readFileSync(filename), Buffer.from("\n")]),
      );
      assert.throws(() => checkContract(fixture), pattern);
    }));
}

for (const artifact of [
  "descriptor.binpb",
  "external_ingestion_sdk_v1.json",
  "connect-idempotency-policy.json",
  "error-reasons.json",
  "conformance/external-sdk-ingestion.json",
  "release-attestation.json",
]) {
  test(`offline check detects sealed artifact drift in ${artifact}`, () =>
    withSdkFixture((fixture) => {
      const filename = path.join(fixture, VENDOR_RELATIVE, artifact);
      writeFileSync(
        filename,
        Buffer.concat([readFileSync(filename), Buffer.from("\n")]),
      );
      assert.throws(
        () => checkContract(fixture),
        /differs from its bundle evidence/,
      );
    }));
}

test("release check blocks the neutral candidate attestation", () => {
  assert.throws(
    () => checkReleaseContract(),
    /release check blocked by unreleased_candidate contract/,
  );
});

test("release check accepts a producer-shaped immutable released export", () =>
  withSdkFixture((fixture) => {
    const exportRoot = createContractExport(fixture);
    resealExport(exportRoot, ({ attestation, bundle }) => {
      bundle.release_status = "released";
      attestation.attestation = "immutable_release";
      attestation.immutable = true;
      attestation.release_id = "sdkrel_01k1ef9a3n7r0w5t8v4x6y2z1b";
      attestation.released = true;
    });
    const synced = syncContract(exportRoot, fixture);
    assert.equal(synced.releaseStatus, "released");
    assert.equal(checkReleaseContract(fixture).releaseStatus, "released");
  }));

test("normal checks use only committed files and perform no writes", () =>
  withSdkFixture((fixture) => {
    const before = snapshot(fixture);
    const result = checkContract(fixture);
    assert.equal(result.methods.length, 4);
    assertSnapshotsEqual(snapshot(fixture), before);
  }));

test("synchronizer exposes only a neutral contract-root input", () => {
  const source = readFileSync(
    path.join(SDK_ROOT, "scripts/sync_external_ingestion_contract.mjs"),
    "utf8",
  );
  assert.match(source, /MEDALLION_SDK_CONTRACT_ROOT/);
  assert.match(source, /--contract-root/);
  for (const forbidden of FORBIDDEN) {
    assert.equal(source.toLowerCase().includes(forbidden.toLowerCase()), false);
  }
});
