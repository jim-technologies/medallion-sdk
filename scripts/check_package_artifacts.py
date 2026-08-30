#!/usr/bin/env python3
"""Verify the exact Git-install package payloads for every SDK artifact."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
PYTHON_ROOT = ROOT / "python"
EXTERNAL_CONTRACT_ROOT = Path("proto/external-ingestion-contract/v1")
MINIMAL_CONTRACT_FILES = frozenset(
    {
        "bundle.json",
        "bundle.sha256",
        "conformance/external-sdk-ingestion.json",
        "conformance/schemas/fixture-document.schema.json",
        "connect-idempotency-policy.json",
        "descriptor.binpb",
        "error-reasons.json",
        "external_ingestion_sdk_v1.json",
        "release-attestation.json",
        "schemas/error-reasons.schema.json",
        "schemas/external-ingestion-sdk-v1.schema.json",
    }
)

# Construct private implementation tokens so this boundary checker can itself be
# shipped in the root Go module without placing those tokens in the public tree.
# These are implementation/repository identifiers, not public protocol names.
PRIVATE_IMPLEMENTATION_MARKERS = (
    b"onto" + b"logy",
    b"medallion-" + b"ter" + b"minal",
    b"medallion-" + b"storage",
    b"medallion-" + b"cloud",
    b"medallion-" + b"connect",
    b"MEDALLION_" + b"ONTO" + b"LOGY_ROOT",
    b"consumer-" + b"profiles",
    b"ter" + b"minal-" + b"com" + b"pass",
    b"source_" + b"repository",
)
ALLOWED_JIMTECH_REPOSITORIES = frozenset(
    {
        "invariantprotocol",
        "medallion-sdk",
    }
)
FORBIDDEN_INGESTION_SYMBOLS = (
    b"PublishPlatformAuditEvents",
    b"publishPlatformAuditEvents",
    b"publish_platform_audit_events",
    b"RegisterConnector",
    b"registerConnector",
    b"register_connector",
    b"RegisterDatasource",
    b"registerDatasource",
    b"register_datasource",
    b"ExecuteConnectorAction",
    b"executeConnectorAction",
    b"execute_connector_action",
    b"ProtocolStorageClient",
    b"StorageClient",
    b"DatasourcesClient",
    b"DatasourceRegistrationInput",
    b"StorageUploadInput",
    b"ExecuteActionInput",
    b"QueryInput",
)
TYPESCRIPT_INGESTION_METHODS = (
    b"publishCdcEvents(",
    b"publishAuditEvents(",
    b"listCdcEvents(",
    b"listAuditEvents(",
)
PYTHON_INGESTION_METHODS = (
    b"def publish_cdc_events(",
    b"def publish_audit_events(",
    b"def list_cdc_events(",
    b"def list_audit_events(",
)
PYTHON_CANONICAL_RPC_PATHS = (
    b"/medallion.connect.v1.MedallionConnectService/PublishCdcEvents",
    b"/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
    b"/medallion.connect.v1.MedallionConnectService/PublishAuditEvents",
    b"/medallion.connect.v1.MedallionConnectService/ListAuditEvents",
)
CONNECT_RPC_METHODS = frozenset(
    {
        "PublishCdcEvents",
        "PublishAuditEvents",
        "ListCdcEvents",
        "ListAuditEvents",
    }
)
INGEST_RPC_METHODS = frozenset(
    {
        "CreateTable",
        "GetTable",
        "ListTables",
        "UpdateTable",
        "AppendRows",
        "RunQuery",
        "GetQueryResults",
    }
)
ACTIVE_LEGACY_SCOPE_MARKERS = (
    b"tenant" + b"_id",
    b"tenant" + b"Id",
    b"Tenant" + b"ID",
    b"organization" + b"_id",
    b"organization" + b"Id",
    b"Organization" + b"ID",
    b"X-Jimtech-" + b"Tenant-Id",
)

TYPESCRIPT_DIST_MODULES = frozenset(
    {
        "audit",
        "cdc",
        "client",
        "connect-descriptor",
        "error-policy",
        "errors",
        "ids",
        "index",
        "ingest",
        "ingest-descriptor",
        "ingestion",
        "payload",
        "protocol-preflight",
        "protocol",
        "request",
        "tables",
        "tracing",
        "types",
    }
)

PYTHON_RUNTIME_FILES = frozenset(
    {
        "buf/__init__.py",
        "buf/validate/__init__.py",
        "buf/validate/validate_pb2.py",
        "medallion/__init__.py",
        "medallion/client.py",
        "medallion/error_policy_generated.py",
        "medallion/errors.py",
        "medallion/ids.py",
        "medallion/py.typed",
        "medallion/request.py",
        "medallion/tables.py",
        "medallion/tracing.py",
        "medallion/types.py",
        "medallion/connect/__init__.py",
        "medallion/connect/v1/__init__.py",
        "medallion/connect/v1/connect_pb2.py",
        "medallion/ingest/__init__.py",
        "medallion/ingest/v1/__init__.py",
        "medallion/ingest/v1/ingest_pb2.py",
    }
)


def fail(message: str) -> None:
    raise AssertionError(message)


def require_subset(actual: set[str], expected: set[str], artifact: str) -> None:
    missing = sorted(expected - actual)
    if missing:
        fail(f"{artifact} is missing required files: {missing}")


def require_exact_files(actual: set[str], expected: set[str], artifact: str) -> None:
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        fail(
            f"{artifact} has an unexpected payload; missing={missing}, "
            f"unexpected={unexpected}"
        )


def reject_private_implementation_references(
    payload: bytes,
    artifact: str,
) -> None:
    lowered = payload.lower()
    for marker in PRIVATE_IMPLEMENTATION_MARKERS:
        if marker.lower() in lowered:
            fail(f"{artifact} contains a private implementation reference")

    text = payload.decode("utf-8", errors="ignore")
    for match in re.finditer(
        r"(?:github:|github\.com[:/]|codeload\.github\.com/)"
        r"jim-technologies/"
        r"([a-z0-9_.-]+)",
        text,
        flags=re.IGNORECASE,
    ):
        repository = match.group(1).removesuffix(".git").lower()
        if repository not in ALLOWED_JIMTECH_REPOSITORIES:
            fail(f"{artifact} references a non-public Jim Technologies repository")


def scan_distribution_tree(root: Path, artifact: str) -> None:
    if not root.is_dir():
        fail(f"{artifact} root does not exist: {root}")
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        reject_private_implementation_references(
            relative.encode("utf-8"),
            f"{artifact} path {relative}",
        )
        if path.is_symlink():
            fail(f"{artifact} contains a symbolic link: {relative}")
        if path.is_file():
            reject_private_implementation_references(
                path.read_bytes(),
                f"{artifact} file {relative}",
            )


def check_minimal_contract_tree(contract_root: Path, artifact: str) -> None:
    if not contract_root.is_dir():
        fail(f"{artifact} is missing")
    contract_files = {
        path.relative_to(contract_root).as_posix()
        for path in contract_root.rglob("*")
        if path.is_file()
    }
    require_exact_files(
        contract_files,
        set(MINIMAL_CONTRACT_FILES),
        artifact,
    )
    scan_distribution_tree(contract_root, artifact)


def check_dependency_and_ci_boundaries(
    root: Path = ROOT,
    python_root: Path | None = None,
) -> None:
    python_root = root / "python" if python_root is None else python_root
    files = {
        root / "Makefile",
        root / "buf.gen.yaml",
        root / "buf.yaml",
        root / "go.mod",
        root / "go.sum",
        root / "package.json",
        root / "pnpm-lock.yaml",
        root / "pnpm-workspace.yaml",
        python_root / "pyproject.toml",
        python_root / "uv.lock",
    }
    for configuration_root in (root / ".github", root / ".flox", root / ".ci"):
        files.update(
            path
            for path in configuration_root.rglob("*")
            if path.is_file()
            and not {
                "cache",
                "log",
                "run",
            }.intersection(path.relative_to(configuration_root).parts)
        )
    for path in sorted(files):
        if not path.is_file():
            fail(f"missing manifest, lock, or CI file: {path.relative_to(root)}")
        reject_private_implementation_references(
            path.read_bytes(),
            f"repository boundary {path.relative_to(root)}",
        )


def reject_forbidden_ingestion_symbols(payload: bytes, artifact: str) -> None:
    for symbol in FORBIDDEN_INGESTION_SYMBOLS:
        if symbol in payload:
            fail(f"{artifact} exposes prohibited ingestion symbol {symbol.decode()}")


def reject_active_legacy_scope(payload: bytes, artifact: str) -> None:
    for marker in ACTIVE_LEGACY_SCOPE_MARKERS:
        if marker in payload:
            fail(f"{artifact} exposes a retired tenant/organization scope")


def require_ingestion_symbols(
    payload: bytes,
    expected: tuple[bytes, ...],
    artifact: str,
) -> None:
    missing = [symbol.decode() for symbol in expected if symbol not in payload]
    if missing:
        fail(f"{artifact} is missing required ingestion symbols: {missing}")


def require_exact_rpc_methods(
    payload: bytes,
    pattern: bytes,
    expected: set[str] | frozenset[str],
    artifact: str,
) -> None:
    methods = {
        match.decode("ascii")
        for match in re.findall(pattern, payload, flags=re.MULTILINE)
    }
    require_exact_files(methods, set(expected), artifact)


def require_private_python_transport(payload: bytes, artifact: str) -> None:
    for forbidden in (b"class RequestClient", b"def post_json("):
        if forbidden in payload:
            fail(
                f"{artifact} exposes public or generic Python transport surface "
                f"{forbidden.decode()}"
            )
    required = (
        b"class _RequestClient",
        b"_CANONICAL_RPC_PATHS",
        b"canonical_path not in _CANONICAL_RPC_PATHS",
        *PYTHON_CANONICAL_RPC_PATHS,
    )
    missing = [marker.decode() for marker in required if marker not in payload]
    if missing:
        fail(f"{artifact} is missing the private transport boundary: {missing}")


def metadata_license_fields(raw: bytes, artifact: str) -> None:
    text = raw.decode("utf-8")
    if "License-Expression: Apache-2.0\n" not in text:
        fail(f"{artifact} is missing License-Expression: Apache-2.0")
    fields = {
        line.removeprefix("License-File: ")
        for line in text.splitlines()
        if line.startswith("License-File: ")
    }
    if fields != {"LICENSE", "NOTICE"}:
        fail(f"{artifact} has unexpected License-File fields: {sorted(fields)}")


def npm_pack_description(payload: object) -> dict[str, object]:
    """Normalize npm 11's array and npm 12's package-name-keyed JSON shapes."""
    if isinstance(payload, list) and len(payload) == 1 and isinstance(payload[0], dict):
        return payload[0]
    if isinstance(payload, dict):
        if isinstance(payload.get("files"), list):
            return payload
        if len(payload) == 1:
            description = next(iter(payload.values()))
            if isinstance(description, dict):
                return description
    fail("npm pack did not return exactly one package description")


def expected_npm_files() -> set[str]:
    files = {
        "LICENSE",
        "NOTICE",
        "README.md",
        "VERSION",
        "package.json",
        "dist/THIRD_PARTY_LICENSES.txt",
        "dist/index.cjs",
        "dist/index.cjs.map",
        "dist/index.js",
        "dist/index.js.map",
    }
    for module in TYPESCRIPT_DIST_MODULES:
        files.add(f"dist/{module}.d.ts")
        files.add(f"dist/{module}.d.ts.map")
    return files


def check_npm() -> None:
    subprocess.run(
        ["node", "scripts/generate_third_party_licenses.mjs", "--check"],
        cwd=ROOT,
        check=True,
    )
    result = subprocess.run(
        ["npm", "pack", "--dry-run", "--json", "--ignore-scripts"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    description = npm_pack_description(payload)
    files = {
        item["path"]
        for item in description.get("files", [])
        if isinstance(item, dict) and isinstance(item.get("path"), str)
    }
    require_exact_files(files, expected_npm_files(), "npm Git package")
    for relative in sorted(files):
        packaged = ROOT / relative
        if not packaged.is_file():
            fail(f"npm Git package file is unavailable locally: {relative}")
        payload = packaged.read_bytes()
        reject_private_implementation_references(
            payload,
            f"npm Git package {relative}",
        )
        if relative.startswith("dist/"):
            reject_forbidden_ingestion_symbols(
                payload,
                f"npm Git package {relative}",
            )
            reject_active_legacy_scope(payload, f"npm Git package {relative}")
    protocol_declarations = (ROOT / "dist/protocol.d.ts").read_bytes()
    require_ingestion_symbols(
        protocol_declarations,
        TYPESCRIPT_INGESTION_METHODS,
        "npm Git package dist/protocol.d.ts",
    )
    require_exact_rpc_methods(
        protocol_declarations,
        rb"^    ([a-z][A-Za-z0-9]*)\(request:",
        {
            "publishCdcEvents",
            "publishAuditEvents",
            "listCdcEvents",
            "listAuditEvents",
        },
        "npm Git package ProtocolConnectClient methods",
    )
    for prohibited in (b"\n    invoke(", b"\n    call("):
        if prohibited in protocol_declarations:
            fail("npm Git package exposes a generic Connect dispatcher")


def check_go() -> None:
    descriptors = {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "proto").glob("*.descriptor.binpb")
    }
    require_exact_files(
        descriptors,
        {
            "proto/external-ingestion-v1.descriptor.binpb",
            "proto/ingest-v1.descriptor.binpb",
        },
        "Go Git package descriptors",
    )

    contract_directories = {
        path.name
        for path in (ROOT / "proto").iterdir()
        if path.is_dir() and "contract" in path.name
    }
    require_exact_files(
        contract_directories,
        {"external-ingestion-contract"},
        "Go Git package contract directories",
    )
    check_minimal_contract_tree(
        ROOT / EXTERNAL_CONTRACT_ROOT,
        "Go Git package external-ingestion contract",
    )

    go_runtime_sources = [
        source
        for source in sorted((ROOT / "go").glob("*.go"))
        if not source.name.endswith("_test.go")
    ]
    for source in sorted((ROOT / "go").rglob("*.go")):
        if source.name.endswith("_test.go"):
            continue
        reject_private_implementation_references(
            source.read_bytes(),
            f"Go Git package {source.relative_to(ROOT)}",
        )
        if source in go_runtime_sources:
            reject_active_legacy_scope(
                source.read_bytes(),
                f"Go Git package {source.relative_to(ROOT)}",
            )
        reject_forbidden_ingestion_symbols(
            source.read_bytes(),
            f"Go Git package {source.relative_to(ROOT)}",
        )

    generated = ROOT / "go/gen/medallion/connect/v1/connect.pb.go"
    payload = generated.read_bytes()
    reject_forbidden_ingestion_symbols(payload, "Go generated Connect binding")
    require_ingestion_symbols(
        payload,
        tuple(
            method.encode()
            for method in (
                "PublishCdcEvents",
                "PublishAuditEvents",
                "ListCdcEvents",
                "ListAuditEvents",
            )
        ),
        "Go generated Connect binding",
    )
    require_exact_rpc_methods(
        b"\n".join(source.read_bytes() for source in go_runtime_sources),
        rb"^func \(c \*ConnectClient\) ([A-Z][A-Za-z0-9]*)\(",
        CONNECT_RPC_METHODS,
        "Go ConnectClient methods",
    )

    generated_ingest = ROOT / "go/gen/medallion/ingest/v1/ingest.pb.go"
    ingest_payload = generated_ingest.read_bytes()
    reject_forbidden_ingestion_symbols(ingest_payload, "Go generated ingest binding")
    require_ingestion_symbols(
        ingest_payload,
        tuple(method.encode() for method in sorted(INGEST_RPC_METHODS)),
        "Go generated ingest binding",
    )
    require_exact_rpc_methods(
        b"\n".join(source.read_bytes() for source in go_runtime_sources),
        rb"^func \(c \*IngestClient\) ([A-Z][A-Za-z0-9]*)\(",
        INGEST_RPC_METHODS,
        "Go IngestClient methods",
    )


def check_python(version: str) -> None:
    license_bytes = (ROOT / "LICENSE").read_bytes()
    notice_bytes = (ROOT / "NOTICE").read_bytes()
    if (PYTHON_ROOT / "LICENSE").read_bytes() != license_bytes:
        fail("python/LICENSE is not byte-identical to root LICENSE")
    if (PYTHON_ROOT / "NOTICE").read_bytes() != notice_bytes:
        fail("python/NOTICE is not byte-identical to root NOTICE")

    wheel = PYTHON_ROOT / "dist" / f"medallion-{version}-py3-none-any.whl"
    sdist = PYTHON_ROOT / "dist" / f"medallion-{version}.tar.gz"
    for artifact in (wheel, sdist):
        if not artifact.is_file():
            fail(f"missing Python artifact: {artifact.relative_to(ROOT)}")

    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        prefix = f"medallion-{version}.dist-info"
        expected_wheel_files = set(PYTHON_RUNTIME_FILES)
        expected_wheel_files.update(
            {
                f"{prefix}/licenses/LICENSE",
                f"{prefix}/licenses/NOTICE",
                f"{prefix}/METADATA",
                f"{prefix}/RECORD",
                f"{prefix}/WHEEL",
            }
        )
        require_exact_files(
            names,
            expected_wheel_files,
            "Python wheel",
        )
        if archive.read(f"{prefix}/licenses/LICENSE") != license_bytes:
            fail("wheel LICENSE bytes differ from root LICENSE")
        if archive.read(f"{prefix}/licenses/NOTICE") != notice_bytes:
            fail("wheel NOTICE bytes differ from root NOTICE")
        metadata_license_fields(archive.read(f"{prefix}/METADATA"), "wheel METADATA")
        require_ingestion_symbols(
            archive.read("medallion/client.py"),
            PYTHON_INGESTION_METHODS,
            "Python wheel medallion/client.py",
        )
        require_private_python_transport(
            archive.read("medallion/request.py"),
            "Python wheel medallion/request.py",
        )
        python_client = archive.read("medallion/client.py")
        connect_client_match = re.search(
            rb"^class ConnectClient:\n(.*?)(?=^class [A-Za-z])",
            python_client,
            flags=re.MULTILINE | re.DOTALL,
        )
        if connect_client_match is None:
            fail("Python wheel is missing ConnectClient")
        require_exact_rpc_methods(
            connect_client_match.group(1),
            rb"^    def ([a-z][a-z0-9_]*)\(",
            {
                "workspace_id",
                "publish_cdc_events",
                "publish_audit_events",
                "list_cdc_events",
                "list_audit_events",
            },
            "Python wheel ConnectClient methods",
        )
        for name in sorted(names):
            payload = archive.read(name)
            reject_private_implementation_references(
                payload,
                f"Python wheel {name}",
            )
            if name.endswith(".py"):
                reject_forbidden_ingestion_symbols(
                    payload,
                    f"Python wheel {name}",
                )
                if name != "medallion/connect/v1/connect_pb2.py":
                    reject_active_legacy_scope(payload, f"Python wheel {name}")

    distribution_root = f"medallion-{version}"
    with tarfile.open(sdist, "r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                fail(f"sdist contains unsafe path: {member.name!r}")
        by_name = {member.name: member for member in members}
        expected_sdist_files = {
            f"{distribution_root}/.gitignore",
            f"{distribution_root}/LICENSE",
            f"{distribution_root}/NOTICE",
            f"{distribution_root}/PKG-INFO",
            f"{distribution_root}/README.md",
            f"{distribution_root}/pyproject.toml",
        }
        expected_sdist_files.update(
            f"{distribution_root}/src/{relative}" for relative in PYTHON_RUNTIME_FILES
        )
        require_exact_files(set(by_name), expected_sdist_files, "Python sdist")
        for name, expected in (
            (f"{distribution_root}/LICENSE", license_bytes),
            (f"{distribution_root}/NOTICE", notice_bytes),
        ):
            member = by_name[name]
            if not member.isfile():
                fail(f"sdist {name} is not a regular file")
            extracted = archive.extractfile(member)
            if extracted is None or extracted.read() != expected:
                fail(f"sdist {name} bytes differ from the root file")
        metadata = archive.extractfile(by_name[f"{distribution_root}/PKG-INFO"])
        if metadata is None:
            fail("sdist PKG-INFO could not be read")
        metadata_license_fields(metadata.read(), "sdist PKG-INFO")
        client = archive.extractfile(
            by_name[f"{distribution_root}/src/medallion/client.py"]
        )
        if client is None:
            fail("sdist medallion/client.py could not be read")
        require_ingestion_symbols(
            client.read(),
            PYTHON_INGESTION_METHODS,
            "Python sdist medallion/client.py",
        )
        request_transport = archive.extractfile(
            by_name[f"{distribution_root}/src/medallion/request.py"]
        )
        if request_transport is None:
            fail("sdist medallion/request.py could not be read")
        require_private_python_transport(
            request_transport.read(),
            "Python sdist medallion/request.py",
        )
        public_package_prefix = f"{distribution_root}/src/medallion/"
        for name, member in by_name.items():
            if member.isfile():
                extracted = archive.extractfile(member)
                if extracted is not None:
                    payload = extracted.read()
                    reject_private_implementation_references(
                        payload,
                        f"Python sdist {name}",
                    )
                    if name.startswith(public_package_prefix) and name.endswith(".py"):
                        reject_forbidden_ingestion_symbols(
                            payload,
                            f"Python sdist {name}",
                        )
                        if not name.endswith("/medallion/connect/v1/connect_pb2.py"):
                            reject_active_legacy_scope(
                                payload,
                                f"Python sdist {name}",
                            )

    with tempfile.TemporaryDirectory() as temporary:
        subprocess.run(
            ["uv", "build", "--wheel", "--out-dir", temporary],
            cwd=PYTHON_ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        direct_wheel = Path(temporary) / wheel.name
        if not direct_wheel.is_file():
            fail("direct Python wheel build did not produce the expected filename")
        if (
            hashlib.sha256(direct_wheel.read_bytes()).digest()
            != hashlib.sha256(wheel.read_bytes()).digest()
        ):
            fail("direct wheel differs from the wheel rebuilt through the sdist")


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    try:
        if arguments:
            if len(arguments) != 2 or arguments[0] not in {
                "--check-contract-tree",
                "--scan-tree",
            }:
                fail(
                    "usage: check_package_artifacts.py "
                    "[--scan-tree PATH | --check-contract-tree PATH]"
                )
            target = Path(arguments[1])
            if arguments[0] == "--scan-tree":
                scan_distribution_tree(target, "distribution tree")
                print("Distribution tree contains no private implementation references")
            else:
                check_minimal_contract_tree(target, "minimal contract tree")
                print("Minimal external-ingestion contract inventory passed")
            return 0

        version = (ROOT / "VERSION").read_text().strip()
        check_dependency_and_ci_boundaries()
        check_npm()
        check_go()
        check_python(version)
    except (
        AssertionError,
        OSError,
        subprocess.CalledProcessError,
        ValueError,
    ) as error:
        print(f"package artifact check failed: {error}", file=sys.stderr)
        return 1
    print(
        "Closed npm, Go, Python wheel, and Python sdist boundaries contain "
        "only the required ingestion runtime, contract, metadata, and licenses"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
