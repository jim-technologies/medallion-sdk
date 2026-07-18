from __future__ import annotations

import unittest
from pathlib import Path

from google.protobuf import descriptor_pb2

from buf.validate import validate_pb2

ROOT = Path(__file__).resolve().parents[2]


class DescriptorContractTest(unittest.TestCase):
    def test_storage_zero_bound_validation_rules_keep_presence(self) -> None:
        descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(
            (ROOT / "proto/medallion-storage.descriptor.binpb").read_bytes()
        )
        messages = {
            message.name: message
            for file in descriptor_set.file
            if file.package == "medallion.storage.v1"
            for message in file.message_type
        }

        catalog_size = messages["CatalogEntry"].field[2]
        catalog_rules = catalog_size.options.Extensions[validate_pb2.field].int64
        self.assertTrue(catalog_rules.HasField("gte"))
        self.assertEqual(catalog_rules.gte, 0)

        chunk_size = messages["ChunkRef"].field[2]
        chunk_rules = chunk_size.options.Extensions[validate_pb2.field].int64
        self.assertTrue(chunk_rules.HasField("gt"))
        self.assertEqual(chunk_rules.gt, 0)


if __name__ == "__main__":
    unittest.main()
