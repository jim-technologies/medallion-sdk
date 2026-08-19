from __future__ import annotations

import json
import unittest
from pathlib import Path

from medallion import KNOWN_ERROR_DOMAIN, KnownErrorReason, MedallionAPIError
from medallion.errors import REASON_POLICIES

ROOT = Path(__file__).resolve().parents[2]
AUTOMATIC_RETRY_CLASSIFICATIONS = {
    "bounded_transient_retry",
    "bounded_transient_retry_idempotent_operations_only",
}


class ErrorPolicyTests(unittest.TestCase):
    def test_runtime_policy_exactly_matches_the_vendored_registry(self) -> None:
        registry = json.loads(
            (
                ROOT / "proto/external-ingestion-contract/v1/error-reasons.json"
            ).read_text()
        )
        expected = {
            KnownErrorReason(item["reason"]): item for item in registry["reasons"]
        }
        self.assertEqual(registry["domain"], KNOWN_ERROR_DOMAIN)
        self.assertEqual(set(KnownErrorReason), set(expected))

        for reason, item in expected.items():
            with self.subTest(reason=reason):
                policy = REASON_POLICIES[reason]
                self.assertEqual(policy.consumer_category, item["consumer_category"])
                self.assertEqual(policy.grpc_code, item["grpc_code"])
                self.assertEqual(
                    policy.retry_classification,
                    item["retry_policy"]["classification"],
                )
                self.assertEqual(
                    policy.reuse_idempotency_key,
                    item["retry_policy"]["reuse_idempotency_key"],
                )
                error = MedallionAPIError(
                    "safe",
                    status=503,
                    connect_code=policy.grpc_code.lower(),
                    error_info_domain=KNOWN_ERROR_DOMAIN,
                    reason=reason,
                )
                self.assertEqual(
                    error.is_retryable(idempotent=True),
                    policy.retry_classification in AUTOMATIC_RETRY_CLASSIFICATIONS
                    and policy.reuse_idempotency_key,
                )
                self.assertFalse(error.is_retryable(idempotent=False))

    def test_unknown_or_inconsistent_error_info_is_never_retried(self) -> None:
        cases = (
            MedallionAPIError(
                "safe",
                status=503,
                connect_code="unavailable",
                error_info_domain=KNOWN_ERROR_DOMAIN,
                reason="FUTURE_REASON",
            ),
            MedallionAPIError(
                "safe",
                status=503,
                connect_code="unavailable",
                error_info_domain="future.example",
                reason="BACKPRESSURE",
            ),
            MedallionAPIError(
                "safe",
                status=503,
                connect_code="unavailable",
                error_info_domain=KNOWN_ERROR_DOMAIN,
                reason="BACKPRESSURE",
            ),
            MedallionAPIError(
                "safe",
                status=503,
                connect_code="unavailable",
                error_info_domain=KNOWN_ERROR_DOMAIN,
                reason="PROVIDER_UNAVAILABLE",
            ),
            MedallionAPIError(
                "safe",
                status=503,
                connect_code="aborted",
                error_info_domain=KNOWN_ERROR_DOMAIN,
                reason="REVISION_CONFLICT",
            ),
        )
        for error in cases:
            with self.subTest(error=error.reason):
                self.assertFalse(error.is_retryable(idempotent=True))
        self.assertEqual(cases[0].reason, "FUTURE_REASON")
        self.assertIsNone(cases[0].known_reason)

    def test_transport_fallbacks_apply_only_without_error_info(self) -> None:
        for connect_code in (
            "deadline_exceeded",
            "resource_exhausted",
            "unavailable",
        ):
            with self.subTest(connect_code=connect_code):
                self.assertTrue(
                    MedallionAPIError(
                        "safe",
                        status=400,
                        connect_code=connect_code,
                    ).is_retryable(idempotent=True)
                )
        for connect_code in ("aborted", "permission_denied", "unknown"):
            with self.subTest(connect_code=connect_code):
                self.assertFalse(
                    MedallionAPIError(
                        "safe",
                        status=503,
                        connect_code=connect_code,
                    ).is_retryable(idempotent=True)
                )
        for status in (408, 429, 502, 503, 504):
            with self.subTest(status=status):
                self.assertTrue(
                    MedallionAPIError("safe", status=status).is_retryable(
                        idempotent=True
                    )
                )


if __name__ == "__main__":
    unittest.main()
