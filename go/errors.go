package medallion

import (
	"encoding/base64"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

type Error struct {
	Code      string
	RequestID string
	Message   string
	Cause     error
}

func (e *Error) Error() string {
	return e.Message
}

func (e *Error) Unwrap() error {
	return e.Cause
}

type APIError struct {
	Status    int
	Code      string
	RequestID string
	Details   []ConnectErrorDetail
	ErrorInfo *ConnectErrorInfo
	Message   string
}

func (e *APIError) Error() string {
	return e.Message
}

// ConnectErrorDetail is the lossless wire representation of one Connect error
// detail. Value contains the decoded protobuf bytes.
type ConnectErrorDetail struct {
	Type  string
	Value []byte
}

// ConnectErrorInfo is the stable google.rpc.ErrorInfo projection returned by
// Medallion. Callers should branch on Reason rather than human message text.
type ConnectErrorInfo struct {
	Reason   string
	Domain   string
	Metadata map[string]string
}

// Retryable reports whether the failure is transient only when the caller has
// independently established that replaying the exact operation is safe. Pass
// false for mutations without complete idempotency protection.
func (e *APIError) Retryable(safelyIdempotent bool) bool {
	if e == nil || !safelyIdempotent {
		return false
	}
	if e.ErrorInfo != nil && e.ErrorInfo.Domain == ErrorInfoDomain {
		if policy, known := generatedErrorReasonPolicies[KnownErrorReason(e.ErrorInfo.Reason)]; known {
			if !policy.ReuseIdempotencyKey || !strings.EqualFold(e.Code, policy.GRPCCode) {
				return false
			}
			switch policy.RetryClassification {
			case "bounded_transient_retry", "bounded_transient_retry_idempotent_operations_only":
				return true
			default:
				return false
			}
		}
		// Preserve unknown future reasons for callers but never turn one into a
		// blind retry through less-specific transport metadata.
		if e.ErrorInfo.Reason != "" {
			return false
		}
	}
	if e.ErrorInfo != nil && (e.ErrorInfo.Domain != "" || e.ErrorInfo.Reason != "") {
		// A non-canonical domain cannot opt into this registry's retry policy.
		return false
	}
	if code := strings.ToLower(strings.TrimSpace(e.Code)); code != "" {
		switch code {
		case "deadline_exceeded", "resource_exhausted", "unavailable":
			return true
		default:
			return false
		}
	}
	switch e.Status {
	case 408, 429, 502, 503, 504:
		return true
	default:
		return false
	}
}

func connectErrorDetails(body map[string]any) ([]ConnectErrorDetail, *ConnectErrorInfo) {
	rawDetails, ok := body["details"].([]any)
	if !ok {
		return nil, nil
	}
	details := make([]ConnectErrorDetail, 0, len(rawDetails))
	var info *ConnectErrorInfo
	for _, rawDetail := range rawDetails {
		record, ok := rawDetail.(map[string]any)
		if !ok {
			continue
		}
		typeName, _ := record["type"].(string)
		encoded, _ := record["value"].(string)
		value, err := decodeConnectDetail(encoded)
		if typeName == "" || err != nil {
			continue
		}
		details = append(details, ConnectErrorDetail{Type: typeName, Value: value})
		if info == nil && (typeName == "google.rpc.ErrorInfo" || strings.HasSuffix(typeName, "/google.rpc.ErrorInfo")) {
			if decoded, ok := decodeErrorInfo(value); ok {
				info = decoded
			}
		}
	}
	return details, info
}

func decodeConnectDetail(value string) ([]byte, error) {
	for _, encoding := range []*base64.Encoding{
		base64.RawStdEncoding,
		base64.StdEncoding,
		base64.RawURLEncoding,
		base64.URLEncoding,
	} {
		if decoded, err := encoding.DecodeString(value); err == nil {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("invalid Connect error detail encoding")
}

func decodeErrorInfo(raw []byte) (*ConnectErrorInfo, bool) {
	info := &ConnectErrorInfo{Metadata: map[string]string{}}
	for len(raw) > 0 {
		number, wireType, size := protowire.ConsumeTag(raw)
		if size < 0 {
			return nil, false
		}
		raw = raw[size:]
		switch {
		case (number == 1 || number == 2) && wireType == protowire.BytesType:
			value, size := protowire.ConsumeBytes(raw)
			if size < 0 {
				return nil, false
			}
			if number == 1 {
				info.Reason = string(value)
			} else {
				info.Domain = string(value)
			}
			raw = raw[size:]
		case number == 3 && wireType == protowire.BytesType:
			value, size := protowire.ConsumeBytes(raw)
			if size < 0 {
				return nil, false
			}
			key, metadataValue, ok := decodeStringMapEntry(value)
			if !ok {
				return nil, false
			}
			info.Metadata[key] = metadataValue
			raw = raw[size:]
		default:
			size := protowire.ConsumeFieldValue(number, wireType, raw)
			if size < 0 {
				return nil, false
			}
			raw = raw[size:]
		}
	}
	if info.Reason == "" && info.Domain == "" && len(info.Metadata) == 0 {
		return nil, false
	}
	return info, true
}

func decodeStringMapEntry(raw []byte) (string, string, bool) {
	var key, value string
	for len(raw) > 0 {
		number, wireType, size := protowire.ConsumeTag(raw)
		if size < 0 {
			return "", "", false
		}
		raw = raw[size:]
		if (number == 1 || number == 2) && wireType == protowire.BytesType {
			field, size := protowire.ConsumeBytes(raw)
			if size < 0 {
				return "", "", false
			}
			if number == 1 {
				key = string(field)
			} else {
				value = string(field)
			}
			raw = raw[size:]
			continue
		}
		size = protowire.ConsumeFieldValue(number, wireType, raw)
		if size < 0 {
			return "", "", false
		}
		raw = raw[size:]
	}
	return key, value, key != ""
}

func invalidOptions(message string) error {
	return &Error{Code: "MEDALLION_INVALID_OPTIONS", Message: message}
}

func missingOption(code, message string) error {
	return &Error{Code: code, Message: message}
}

func invalidID(path string) error {
	return &Error{
		Code:    "MEDALLION_INVALID_ID",
		Message: fmt.Sprintf("invalid ID at %s: expected string or integer", path),
	}
}
