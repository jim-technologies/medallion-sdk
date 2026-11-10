package medallion

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

var namespaceURLUUID = [16]byte{0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}

// StableIdempotencyKey derives a deterministic UUIDv5 key from a namespace and
// durable source identity. Prefer an existing outbox or source event ID when
// one is already available, and never generate a random key inside a retry
// loop. The algorithm matches the other Medallion SDKs.
func StableIdempotencyKey(namespace string, sourceIdentity ...IDInput) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" || len(sourceIdentity) == 0 {
		return "", &Error{
			Code:    "MEDALLION_INVALID_IDEMPOTENCY_KEY",
			Message: "namespace and at least one stable source identity are required",
		}
	}
	parts := make([]string, 0, len(sourceIdentity)+1)
	parts = append(parts, namespace)
	for index, value := range sourceIdentity {
		normalized, err := normalizeID(value, fmt.Sprintf("sourceIdentity[%d]", index))
		if err != nil {
			return "", err
		}
		parts = append(parts, normalized)
	}
	logicalIdentity := strings.Join(parts, "\x1f")
	input := make([]byte, 0, len(namespaceURLUUID)+len(logicalIdentity))
	input = append(input, namespaceURLUUID[:]...)
	input = append(input, logicalIdentity...)
	digest := sha1.Sum(input)
	digest[6] = (digest[6] & 0x0f) | 0x50
	digest[8] = (digest[8] & 0x3f) | 0x80
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", digest[0:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16])
	return requiredEventIdempotencyKey(namespace+":"+uuid, "idempotency key")
}

func normalizeID(value any, path string) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case json.Number:
		parsed, ok := new(big.Int).SetString(v.String(), 10)
		if !ok {
			return "", invalidID(path)
		}
		return parsed.String(), nil
	case big.Int:
		return v.String(), nil
	case *big.Int:
		if v == nil {
			return "", invalidID(path)
		}
		return v.String(), nil
	default:
		return "", invalidID(path)
	}
}

func requiredEventIdempotencyKey(value, field string) (string, error) {
	if value == "" {
		return "", &Error{
			Code:    "MEDALLION_MISSING_IDEMPOTENCY_KEY",
			Message: field + " is required for retry-safe delivery",
		}
	}
	if len(value) > 512 || !utf8.ValidString(value) {
		return "", &Error{
			Code:    "MEDALLION_INVALID_IDEMPOTENCY_KEY",
			Message: field + " must be valid UTF-8 between 1 and 512 bytes",
		}
	}
	return value, nil
}
