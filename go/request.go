package medallion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type requestClient struct {
	baseURL string
	token   string
	http    HTTPDoer
	tracing TracingConfig
}

type protoEnvelope struct {
	requestID string
}

func newRequestClient(cfg ClientConfig) (*requestClient, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(firstNonEmpty(cfg.ConnectBaseURL, cfg.BaseURL)), "/")
	if baseURL == "" {
		return nil, invalidOptions("base URL is required")
	}
	token := strings.TrimSpace(firstNonEmpty(cfg.AccessToken, cfg.APIKey))
	if token == "" {
		return nil, invalidOptions("API key or access token is required")
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &requestClient{baseURL: baseURL, token: token, http: httpClient, tracing: cfg.Tracing}, nil
}

func (c *requestClient) postProto(ctx context.Context, path string, req proto.Message, resp proto.Message, idempotencyKey string) (protoEnvelope, error) {
	ctx, span := c.tracing.startRequestSpan(ctx, http.MethodPost, path)
	if span != nil {
		defer span.End()
	}

	raw, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(req)
	if err != nil {
		recordRequestSpanError(span, err)
		return protoEnvelope{}, &Error{Code: "MEDALLION_INVALID_JSON_BODY", Message: "request body must be protobuf JSON serializable"}
	}

	url := c.baseURL + ensureLeadingSlash(path)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		recordRequestSpanError(span, err)
		return protoEnvelope{}, err
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Connect-Protocol-Version", "1")
	httpReq.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		httpReq.Header.Set("Idempotency-Key", idempotencyKey)
	}
	c.tracing.inject(ctx, httpReq.Header)

	httpResp, err := c.http.Do(httpReq)
	if err != nil {
		recordRequestSpanError(span, err)
		return protoEnvelope{}, &Error{Code: "MEDALLION_NETWORK_ERROR", Message: "Medallion request failed"}
	}
	defer httpResp.Body.Close()

	responseRaw, readErr := io.ReadAll(httpResp.Body)
	if readErr != nil {
		recordRequestSpanError(span, readErr)
		return protoEnvelope{}, readErr
	}
	requestID := httpResp.Header.Get("x-request-id")
	setRequestSpanResponse(span, httpResp.StatusCode, requestID)
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		responseBody := readJSONBody(bytes.NewReader(responseRaw))
		return protoEnvelope{}, &APIError{
			Status:       httpResp.StatusCode,
			RequestID:    requestID,
			ResponseBody: responseBody,
			Message:      apiErrorMessage(httpResp.StatusCode, requestID, responseBody),
		}
	}
	if len(responseRaw) > 0 && resp != nil {
		if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(responseRaw, resp); err != nil {
			recordRequestSpanError(span, err)
			return protoEnvelope{}, &Error{Code: "MEDALLION_INVALID_JSON_RESPONSE", Message: "Medallion response was not valid protobuf JSON"}
		}
	}
	setRequestSpanOK(span)
	return protoEnvelope{requestID: requestID}, nil
}

func readJSONBody(body io.Reader) map[string]any {
	raw, err := io.ReadAll(body)
	if err != nil || len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err == nil && out != nil {
		return out
	}
	return map[string]any{"body": string(raw)}
}

func apiErrorMessage(status int, requestID string, body map[string]any) string {
	if message, ok := body["message"].(string); ok && message != "" {
		return message
	}
	if requestID != "" {
		return fmt.Sprintf("Medallion API request failed with HTTP %d. Request ID: %s.", status, requestID)
	}
	return fmt.Sprintf("Medallion API request failed with HTTP %d.", status)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func ensureLeadingSlash(path string) string {
	if strings.HasPrefix(path, "/") {
		return path
	}
	return "/" + path
}
