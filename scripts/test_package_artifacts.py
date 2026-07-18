#!/usr/bin/env python3
"""Unit tests for package-artifact compatibility helpers."""

from __future__ import annotations

import unittest

from check_package_artifacts import npm_pack_description


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


if __name__ == "__main__":
    unittest.main()
