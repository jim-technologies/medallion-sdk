from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

from google.protobuf import descriptor_pb2

ROOT = Path(__file__).resolve().parents[2]


def load_generated_connect_module():
    generated_path = ROOT / "python/src/medallion/connect/v1/connect_pb2.py"
    spec = importlib.util.spec_from_file_location(
        "generated_connect_pb2", generated_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load generated module at {generated_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


connect_pb2 = load_generated_connect_module()

EXPECTED_METHODS = (
    "PublishCdcEvents",
    "ListCdcEvents",
    "PublishAuditEvents",
    "ListAuditEvents",
)
EXPECTED_MESSAGES = (
    "CdcEvent",
    "AuditEvent",
    "PublishCdcEventsRequest",
    "PublishedCdcEvent",
    "PublishCdcEventsResponse",
    "ListCdcEventsRequest",
    "ListCdcEventsResponse",
    "PublishAuditEventsRequest",
    "PublishedAuditEvent",
    "PublishAuditEventsResponse",
    "ListAuditEventsRequest",
    "ListAuditEventsResponse",
)
EXPECTED_ENUMS = (
    "CdcOperation",
    "AuditEventOrigin",
    "AuditEventOutcome",
)


class DescriptorContractTest(unittest.TestCase):
    def test_connect_descriptor_has_only_the_bounded_ingestion_contract(self) -> None:
        descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(
            (ROOT / "proto/external-ingestion-v1.descriptor.binpb").read_bytes()
        )
        connect_files = [
            file
            for file in descriptor_set.file
            if file.package == "medallion.connect.v1"
        ]

        self.assertEqual(len(connect_files), 1)
        descriptor = connect_files[0]
        self.assertEqual(
            [service.name for service in descriptor.service],
            ["MedallionConnectService"],
        )
        self.assertEqual(
            [method.name for method in descriptor.service[0].method],
            list(EXPECTED_METHODS),
        )
        self.assertEqual(
            [message.name for message in descriptor.message_type],
            list(EXPECTED_MESSAGES),
        )
        self.assertEqual(
            [enum.name for enum in descriptor.enum_type],
            list(EXPECTED_ENUMS),
        )

    def test_generated_python_descriptor_matches_the_committed_inventory(self) -> None:
        descriptor = connect_pb2.DESCRIPTOR

        self.assertEqual(
            list(descriptor.services_by_name),
            ["MedallionConnectService"],
        )
        self.assertEqual(
            [
                method.name
                for method in descriptor.services_by_name[
                    "MedallionConnectService"
                ].methods
            ],
            list(EXPECTED_METHODS),
        )
        self.assertEqual(
            list(descriptor.message_types_by_name), list(EXPECTED_MESSAGES)
        )
        self.assertEqual(list(descriptor.enum_types_by_name), list(EXPECTED_ENUMS))

    def test_retired_scope_name_is_reserved_only_and_absent_from_authored_python(
        self,
    ) -> None:
        descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(
            (ROOT / "proto/external-ingestion-v1.descriptor.binpb").read_bytes()
        )
        descriptor = next(
            file
            for file in descriptor_set.file
            if file.package == "medallion.connect.v1"
        )
        retired_name = "organization" + "_id"
        selected = {
            "CdcEvent",
            "AuditEvent",
            "ListCdcEventsRequest",
            "ListAuditEventsRequest",
        }
        for message in descriptor.message_type:
            if message.name not in selected:
                continue
            with self.subTest(message=message.name):
                self.assertIn(retired_name, message.reserved_name)
                self.assertNotIn(retired_name, {field.name for field in message.field})

        forbidden = (
            retired_name,
            "organization" + "Id",
            "tenant" + "_id",
            "tenant" + "Id",
            "X-Jimtech-" + "Tenant-Id",
        )
        authored = [
            path
            for path in (ROOT / "python").rglob("*")
            if path.is_file()
            and path.suffix in {".py", ".md"}
            and path.name not in {"connect_pb2.py", "validate_pb2.py"}
            # Installed third-party environments (.venv) and tool caches are
            # not authored surface; a dependency may legitimately spell a
            # token this repository has retired.
            and not any(part.startswith(".") for part in path.relative_to(ROOT).parts)
        ]
        for path in authored:
            text = path.read_text()
            for token in forbidden:
                with self.subTest(path=path.relative_to(ROOT), token=token):
                    self.assertNotIn(token, text)


if __name__ == "__main__":
    unittest.main()
