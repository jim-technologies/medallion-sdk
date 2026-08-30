#!/usr/bin/env python3
"""Unit tests for package-artifact compatibility helpers."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from check_package_artifacts import (
    MINIMAL_CONTRACT_FILES,
    PYTHON_INGESTION_METHODS,
    TYPESCRIPT_INGESTION_METHODS,
    check_dependency_and_ci_boundaries,
    check_minimal_contract_tree,
    expected_npm_files,
    npm_pack_description,
    reject_forbidden_ingestion_symbols,
    reject_active_legacy_scope,
    reject_private_implementation_references,
    require_exact_files,
    require_exact_rpc_methods,
    require_ingestion_symbols,
    require_private_python_transport,
    scan_distribution_tree,
)


class NpmPackDescriptionTest(unittest.TestCase):
    def test_accepts_npm_11_array_shape(self) -> None:
        description = {"name": "@jimtech/medallion", "files": []}
        self.assertIs(npm_pack_description([description]), description)

    def test_accepts_npm_12_package_keyed_shape(self) -> None:
        description = {"name": "@jimtech/medallion", "files": []}
        self.assertIs(
            npm_pack_description({"@jimtech/medallion": description}),
            description,
        )

    def test_accepts_direct_description_shape(self) -> None:
        description = {"name": "@jimtech/medallion", "files": []}
        self.assertIs(npm_pack_description(description), description)

    def test_rejects_ambiguous_or_empty_payloads(self) -> None:
        for payload in (
            [],
            {},
            {"first": {"files": []}, "second": {"files": []}},
            "not JSON metadata",
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(AssertionError):
                    npm_pack_description(payload)


class IngestionArtifactBoundaryTest(unittest.TestCase):
    def test_accepts_the_reviewed_ingestion_surface(self) -> None:
        reject_forbidden_ingestion_symbols(
            b"PublishCdcEvents PublishAuditEvents ListCdcEvents ListAuditEvents",
            "fixture",
        )

        require_ingestion_symbols(
            b" ".join(TYPESCRIPT_INGESTION_METHODS),
            TYPESCRIPT_INGESTION_METHODS,
            "fixture",
        )
        require_ingestion_symbols(
            b" ".join(PYTHON_INGESTION_METHODS),
            PYTHON_INGESTION_METHODS,
            "fixture",
        )

    def test_rejects_out_of_scope_publication_and_control_symbols(self) -> None:
        for symbol in (
            b"PublishPlatformAuditEvents",
            b"RegisterConnector",
            b"ProtocolStorageClient",
            b"StorageClient",
        ):
            with self.subTest(symbol=symbol.decode()):
                with self.assertRaisesRegex(
                    AssertionError,
                    f"prohibited ingestion symbol {symbol.decode()}",
                ):
                    reject_forbidden_ingestion_symbols(
                        b"generated " + symbol + b" client",
                        "fixture",
                    )

    def test_rejects_every_retired_scope_spelling_and_header(self) -> None:
        markers = (
            b"tenant" + b"_id",
            b"tenant" + b"Id",
            b"Tenant" + b"ID",
            b"organization" + b"_id",
            b"organization" + b"Id",
            b"Organization" + b"ID",
            b"X-Jimtech-" + b"Tenant-Id",
        )
        for marker in markers:
            with self.subTest(marker=marker):
                with self.assertRaisesRegex(
                    AssertionError,
                    "retired tenant/organization scope",
                ):
                    reject_active_legacy_scope(marker, "fixture")

    def test_rejects_a_missing_stable_method(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            r"missing required ingestion symbols: \['listAuditEvents\('\]",
        ):
            require_ingestion_symbols(
                b" ".join(TYPESCRIPT_INGESTION_METHODS[:-1]),
                TYPESCRIPT_INGESTION_METHODS,
                "fixture",
            )

    def test_requires_private_allowlisted_python_transport(self) -> None:
        valid = b"\n".join(
            (
                b"class _RequestClient:",
                b"_CANONICAL_RPC_PATHS = frozenset()",
                b"if canonical_path not in _CANONICAL_RPC_PATHS:",
                b"/medallion.connect.v1.MedallionConnectService/PublishCdcEvents",
                b"/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
                b"/medallion.connect.v1.MedallionConnectService/PublishAuditEvents",
                b"/medallion.connect.v1.MedallionConnectService/ListAuditEvents",
            )
        )
        require_private_python_transport(valid, "fixture")
        for invalid in (
            valid.replace(b"class _RequestClient", b"class RequestClient"),
            valid + b"\n    def post_json(self): pass",
            valid.replace(b"canonical_path not in _CANONICAL_RPC_PATHS", b"True"),
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(AssertionError):
                    require_private_python_transport(invalid, "fixture")


class PrivateImplementationBoundaryTest(unittest.TestCase):
    def test_rejects_private_implementation_and_provenance_markers(self) -> None:
        markers = (
            b"medallion-" + b"onto" + b"logy",
            b"medallion." + b"onto" + b"logy.v1",
            b"Medallion" + b"Onto" + b"logyService",
            b"onto" + b"logy-idempotency-policy",
            b"MEDALLION_" + b"ONTO" + b"LOGY_ROOT",
            b"consumer-" + b"profiles",
            b"ter" + b"minal-" + b"com" + b"pass",
            b'"source_' + b'repository": "private"',
        )
        for marker in markers:
            with self.subTest(marker=marker):
                with self.assertRaisesRegex(
                    AssertionError,
                    "private implementation reference",
                ):
                    reject_private_implementation_references(marker, "fixture")

    def test_rejects_unapproved_jimtech_repository_urls(self) -> None:
        private_org = b"jim-" + b"technologies"
        private_url = (
            b"https" + b"://github.com/" + private_org + b"/medallion-" + b"private.git"
        )
        for reference in (
            private_url,
            b"github:" + private_org + b"/private-sdk",
            b"git" + b"@github.com:" + private_org + b"/private-sdk.git",
        ):
            with self.subTest(reference=reference):
                with self.assertRaisesRegex(
                    AssertionError,
                    "non-public Jim Technologies repository",
                ):
                    reject_private_implementation_references(reference, "fixture")

        for public_url in (
            b"https://github.com/jim-technologies/medallion-sdk.git",
            b"https://codeload.github.com/jim-technologies/invariantprotocol/tar.gz/sha",
            b"github:jim-technologies/invariantprotocol",
        ):
            reject_private_implementation_references(public_url, "fixture")

    def test_ci_flox_lock_is_part_of_the_private_dependency_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            required = (
                "Makefile",
                "buf.gen.yaml",
                "buf.yaml",
                "go.mod",
                "go.sum",
                "package.json",
                "pnpm-lock.yaml",
                "pnpm-workspace.yaml",
                "python/pyproject.toml",
                "python/uv.lock",
            )
            for relative in required:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("public")
            ci_lock = root / ".ci/node-26/.flox/env/manifest.lock"
            ci_lock.parent.mkdir(parents=True, exist_ok=True)
            ci_lock.write_bytes(b"medallion-" + b"onto" + b"logy")
            with self.assertRaisesRegex(
                AssertionError,
                "private implementation reference",
            ):
                check_dependency_and_ci_boundaries(root)

    def test_distribution_tree_scans_paths_and_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "safe.txt").write_text("medallion.connect.v1")
            scan_distribution_tree(root, "fixture")

            private = root / ("medallion-" + "onto" + "logy")
            private.write_text("opaque")
            with self.assertRaisesRegex(
                AssertionError,
                "private implementation reference",
            ):
                scan_distribution_tree(root, "fixture")

    def test_minimal_contract_tree_has_a_closed_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for relative in MINIMAL_CONTRACT_FILES:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("opaque public contract")
            check_minimal_contract_tree(root, "fixture")

            (root / "unrelated-profile.json").write_text("{}")
            with self.assertRaisesRegex(AssertionError, "unexpected payload"):
                check_minimal_contract_tree(root, "fixture")

    def test_exact_payload_rejects_missing_and_extra_files(self) -> None:
        require_exact_files({"runtime"}, {"runtime"}, "fixture")
        with self.assertRaisesRegex(AssertionError, "unexpected payload"):
            require_exact_files({"runtime", "private"}, {"runtime"}, "fixture")

    def test_exact_rpc_surface_rejects_an_extra_dispatcher(self) -> None:
        valid = b"\n".join(
            (
                b"    publishCdcEvents(request: unknown): unknown;",
                b"    publishAuditEvents(request: unknown): unknown;",
                b"    listCdcEvents(request: unknown): unknown;",
                b"    listAuditEvents(request: unknown): unknown;",
            )
        )
        expected = {
            "publishCdcEvents",
            "publishAuditEvents",
            "listCdcEvents",
            "listAuditEvents",
        }
        pattern = rb"^    ([a-z][A-Za-z0-9]*)\(request:"
        require_exact_rpc_methods(valid, pattern, expected, "fixture")
        with self.assertRaisesRegex(AssertionError, "unexpected payload"):
            require_exact_rpc_methods(
                valid + b"\n    invoke(request: unknown): unknown;",
                pattern,
                expected,
                "fixture",
            )

    def test_typescript_package_inventory_is_closed(self) -> None:
        files = expected_npm_files()
        self.assertIn("dist/index.js", files)
        self.assertIn("dist/protocol.d.ts", files)
        self.assertNotIn("proto/external-ingestion-v1.json", files)


if __name__ == "__main__":
    unittest.main()
    (check_minimal_contract_tree,)
