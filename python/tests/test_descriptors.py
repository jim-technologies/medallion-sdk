from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

from google.protobuf import descriptor_pb2

from medallion import ingest_pb2

ROOT = Path(__file__).resolve().parents[2]
METHODS = (
    "CreateTable",
    "GetTable",
    "ListTables",
    "UpdateTable",
    "AppendRows",
    "RunQuery",
    "GetQueryResults",
)


class DescriptorContractTest(unittest.TestCase):
    def test_active_descriptor_has_exactly_seven_ingest_methods(self) -> None:
        descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(
            (ROOT / "proto/ingest-v1.descriptor.binpb").read_bytes()
        )
        services = [
            (file.package, service)
            for file in descriptor_set.file
            for service in file.service
        ]
        self.assertEqual(len(services), 1)
        package, service = services[0]
        self.assertEqual(package, "medallion.ingest.v1")
        self.assertEqual(service.name, "MedallionIngestService")
        self.assertEqual(tuple(method.name for method in service.method), METHODS)
        generated = ingest_pb2.DESCRIPTOR.services_by_name["MedallionIngestService"]
        self.assertEqual(tuple(method.name for method in generated.methods), METHODS)

    def test_retired_namespace_is_absent_from_installed_package(self) -> None:
        self.assertIsNone(importlib.util.find_spec("medallion.connect"))


if __name__ == "__main__":
    unittest.main()
