package medallion

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

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

func normalizeActor(actor ActorRef) (map[string]string, error) {
	id, err := normalizeID(actor.ID, "actor.id")
	if err != nil {
		return nil, err
	}
	out := map[string]string{"id": id}
	if actor.Type != "" {
		out["type"] = actor.Type
	}
	if actor.Provider != "" {
		out["provider"] = actor.Provider
	}
	return out, nil
}

func actorPrincipalFromRef(actor map[string]string) string {
	parts := make([]string, 0, 3)
	if value := actor["type"]; value != "" {
		parts = append(parts, value)
	}
	if value := actor["provider"]; value != "" {
		parts = append(parts, value)
	}
	parts = append(parts, actor["id"])
	return strings.Join(parts, ":")
}

func actorFromPrincipal(value string) *ActorRef {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ":")
	if len(parts) < 2 {
		return &ActorRef{ID: value}
	}
	id := parts[len(parts)-1]
	kind := parts[0]
	provider := ""
	if len(parts) > 2 {
		provider = strings.Join(parts[1:len(parts)-1], ":")
	}
	return &ActorRef{ID: id, Type: kind, Provider: provider}
}

func normalizeResource(resource ResourceRef) (map[string]string, error) {
	if resource.Type == "" {
		return nil, &Error{Code: "MEDALLION_INVALID_ID", Message: "resource.type is required"}
	}
	id, err := normalizeID(resource.ID, "resource.id")
	if err != nil {
		return nil, err
	}
	return map[string]string{"type": resource.Type, "id": id}, nil
}

func normalizeIDRecord(values map[string]IDInput, path string) (map[string]string, error) {
	out := make(map[string]string, len(values))
	for key, value := range values {
		normalized, err := normalizeID(value, fmt.Sprintf("%s.%s", path, key))
		if err != nil {
			return nil, err
		}
		out[key] = normalized
	}
	return out, nil
}

func sameActor(left *ActorRef, right map[string]string) bool {
	if left == nil {
		return false
	}
	id, err := normalizeID(left.ID, "actor.id")
	if err != nil {
		return false
	}
	return left != nil &&
		id == right["id"] &&
		left.Type == right["type"] &&
		left.Provider == right["provider"]
}
