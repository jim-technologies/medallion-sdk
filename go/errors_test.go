package medallion

import (
	"testing"
)

func TestGeneratedErrorReasonRetryPolicy(t *testing.T) {
	if len(generatedErrorReasonPolicies) != 7 {
		t.Fatalf("generated error reason policy count = %d, want 7", len(generatedErrorReasonPolicies))
	}

	for reason, policy := range generatedErrorReasonPolicies {
		if reason == "" || policy.ConsumerCategory == "" || policy.GRPCCode == "" || policy.RetryClassification == "" {
			t.Fatalf("incomplete generated policy for %q: %#v", reason, policy)
		}
		wantRetryable := policy.ReuseIdempotencyKey && (policy.RetryClassification == "bounded_transient_retry" || policy.RetryClassification == "bounded_transient_retry_idempotent_operations_only")
		error := &APIError{
			Status:    503,
			Code:      policy.GRPCCode,
			ErrorInfo: &ConnectErrorInfo{Reason: string(reason), Domain: ErrorInfoDomain},
		}
		if got := error.Retryable(true); got != wantRetryable {
			t.Errorf("Retryable(true) for %s = %t, want %t", reason, got, wantRetryable)
		}
		if error.Retryable(false) {
			t.Errorf("Retryable(false) for %s = true, want false", reason)
		}
		error.Code = "UNKNOWN"
		if error.Retryable(true) {
			t.Errorf("Retryable(true) for %s ignored mismatched gRPC code", reason)
		}
	}
}

func TestRetryablePreservesUnknownReasonAndFailsClosed(t *testing.T) {
	for _, reason := range []string{"FUTURE_REASON", "PROVIDER_UNAVAILABLE", "REVISION_CONFLICT"} {
		error := &APIError{
			Status: 503,
			Code:   "unavailable",
			ErrorInfo: &ConnectErrorInfo{
				Reason: reason,
				Domain: ErrorInfoDomain,
			},
		}
		if error.ErrorInfo.Reason != reason {
			t.Fatalf("reason = %q, want %q", error.ErrorInfo.Reason, reason)
		}
		if error.Retryable(true) {
			t.Errorf("unknown structured reason %q must not be retried automatically", reason)
		}
	}
}

func TestRetryableRequiresCanonicalErrorInfoDomain(t *testing.T) {
	error := &APIError{
		Status: 503,
		Code:   "unavailable",
		ErrorInfo: &ConnectErrorInfo{
			Reason: string(ErrorReasonBackpressure),
			Domain: "example.invalid",
		},
	}
	if error.Retryable(true) {
		t.Fatal("a known reason from a non-canonical domain must not control retry policy")
	}
}

func TestRetryableFallbacksAreConservative(t *testing.T) {
	for _, code := range []string{"deadline_exceeded", "resource_exhausted", "unavailable"} {
		if !(&APIError{Code: code}).Retryable(true) {
			t.Errorf("code %s should be retryable", code)
		}
	}
	for _, code := range []string{"aborted", "permission_denied", "unknown"} {
		if (&APIError{Status: 503, Code: code}).Retryable(true) {
			t.Errorf("code %s must override retryable HTTP status", code)
		}
	}
	for _, status := range []int{408, 429, 502, 503, 504} {
		if !(&APIError{Status: status}).Retryable(true) {
			t.Errorf("HTTP status %d should be retryable without a structured reason or Connect code", status)
		}
	}
	for _, status := range []int{400, 409, 500} {
		if (&APIError{Status: status}).Retryable(true) {
			t.Errorf("HTTP status %d should not be retryable", status)
		}
	}
}
