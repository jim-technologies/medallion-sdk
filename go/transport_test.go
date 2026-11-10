package medallion

// Shared transport behavior, exercised through the retained ingest RPCs.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	ingestv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/ingest/v1"
	"google.golang.org/protobuf/encoding/protowire"
)

const otherWorkspaceID = "ws_01jz9q5g6rsf7r5ar4rah1b2c4"

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type failingReadCloser struct {
	err error
}

func (reader failingReadCloser) Read([]byte) (int, error) {
	return 0, reader.err
}

func (failingReadCloser) Close() error {
	return nil
}

func listTables(ctx context.Context, client *Client) error {
	_, _, err := client.Ingest.ListTables(ctx, &ingestv1.ListTablesRequest{})
	return err
}

func TestAuthenticationWorkspaceAndTimeoutHeaders(t *testing.T) {
	var headers []http.Header
	var bodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		headers = append(headers, request.Header.Clone())
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode list request: %v", err)
		}
		bodies = append(bodies, body)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"tables":[]}`))
	}))
	defer server.Close()

	apiClient, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		APIKey:      "service-key",
		WorkspaceID: testWorkspaceID,
	})
	if err != nil {
		t.Fatalf("new API-key client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := listTables(ctx, apiClient); err != nil {
		t.Fatalf("list with API key: %v", err)
	}
	if got := headers[0].Get("X-Medallion-API-Key"); got != "service-key" {
		t.Fatalf("API key header = %q", got)
	}
	if got := headers[0].Get("Authorization"); got != "" {
		t.Fatalf("API-key request authorization = %q", got)
	}
	if got := headers[0].Get("X-Medallion-Workspace-Id"); got != testWorkspaceID {
		t.Fatalf("workspace = %q", got)
	}
	if got := headers[0].Get("Connect-Protocol-Version"); got != "1" {
		t.Fatalf("Connect-Protocol-Version = %q", got)
	}
	if got := headers[0].Get("Idempotency-Key"); got != "" {
		t.Fatalf("read request carried Idempotency-Key %q", got)
	}
	if _, ok := bodies[0]["workspaceId"]; ok {
		t.Fatalf("workspace identity leaked into the body: %#v", bodies[0])
	}
	assertConnectTimeout(t, headers[0].Get("Connect-Timeout-Ms"))

	jwtClient, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		AccessToken: "jwt-token",
		WorkspaceID: otherWorkspaceID,
	})
	if err != nil {
		t.Fatalf("new JWT client: %v", err)
	}
	if err := listTables(context.Background(), jwtClient); err != nil {
		t.Fatalf("list with JWT: %v", err)
	}
	if got := headers[1].Get("Authorization"); got != "Bearer jwt-token" {
		t.Fatalf("JWT authorization = %q", got)
	}
	if got := headers[1].Get("X-Medallion-API-Key"); got != "" {
		t.Fatalf("JWT request API key = %q", got)
	}
	if got := headers[1].Get("X-Medallion-Workspace-Id"); got != otherWorkspaceID {
		t.Fatalf("JWT workspace = %q", got)
	}

	_, err = NewClient(ClientConfig{BaseURL: server.URL, APIKey: "key"})
	assertErrorCode(t, err, "MEDALLION_MISSING_WORKSPACE_ID")
	if len(headers) != 2 {
		t.Fatalf("missing workspace sent an HTTP request; calls = %d", len(headers))
	}
}

func TestRedirectsNeverForwardAPIKeyOrJWTHeaders(t *testing.T) {
	for _, auth := range []struct {
		name   string
		config ClientConfig
		header string
		value  string
	}{
		{
			name:   "API key",
			config: ClientConfig{APIKey: "fixture-api-key"},
			header: "X-Medallion-API-Key",
			value:  "fixture-api-key",
		},
		{
			name:   "JWT",
			config: ClientConfig{AccessToken: "fixture-jwt"},
			header: "Authorization",
			value:  "Bearer fixture-jwt",
		},
	} {
		for _, customClient := range []bool{false, true} {
			name := auth.name + "/default client"
			if customClient {
				name = auth.name + "/custom client"
			}
			t.Run(name, func(t *testing.T) {
				var redirectedHeaders []http.Header
				target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					redirectedHeaders = append(redirectedHeaders, request.Header.Clone())
					_, _ = w.Write([]byte(`{}`))
				}))
				defer target.Close()

				var sourceHeaders []http.Header
				source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					sourceHeaders = append(sourceHeaders, request.Header.Clone())
					w.Header().Set("Location", target.URL+"/credential-sink")
					w.WriteHeader(http.StatusTemporaryRedirect)
				}))
				defer source.Close()

				config := auth.config
				config.BaseURL = source.URL
				config.WorkspaceID = testWorkspaceID
				redirectCallbacks := 0
				transportCalls := 0
				if customClient {
					config.HTTPClient = &http.Client{
						Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
							transportCalls++
							return http.DefaultTransport.RoundTrip(request)
						}),
						CheckRedirect: func(*http.Request, []*http.Request) error {
							redirectCallbacks++
							return nil
						},
					}
				}
				client, err := NewClient(config)
				if err != nil {
					t.Fatalf("new client: %v", err)
				}
				err = listTables(context.Background(), client)
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.Status != http.StatusTemporaryRedirect {
					t.Fatalf("redirect error = %#v", err)
				}
				if len(sourceHeaders) != 1 || sourceHeaders[0].Get(auth.header) != auth.value {
					t.Fatalf("source authentication header = %#v", sourceHeaders)
				}
				if len(redirectedHeaders) != 0 {
					t.Fatalf("redirect target received credential-bearing request: %#v", redirectedHeaders)
				}
				if redirectCallbacks != 0 {
					t.Fatalf("caller redirect policy ran %d times", redirectCallbacks)
				}
				if customClient && transportCalls != 1 {
					t.Fatalf("custom RoundTripper calls = %d, want 1", transportCalls)
				}
			})
		}
	}
}

func TestBaseURLMustBeAnOrigin(t *testing.T) {
	for _, baseURL := range []string{
		"https://api.example.com",
		"https://api.example.com/",
		"http://localhost:8080",
		"http://127.99.1.2:8080",
		"http://[::1]:8080",
	} {
		t.Run("valid "+baseURL, func(t *testing.T) {
			client, err := newRequestClient(ClientConfig{BaseURL: baseURL, APIKey: "key", WorkspaceID: testWorkspaceID})
			if err != nil {
				t.Fatalf("new request client: %v", err)
			}
			if strings.HasSuffix(client.baseURL, "/") {
				t.Fatalf("normalized base URL retains trailing slash: %q", client.baseURL)
			}
		})
	}

	for _, baseURL := range []string{
		"https://api.example.com/not-an-origin",
		"https://api.example.com/not-an-origin/",
		"https://api.example.com//",
		"https://api.example.com/%2F",
		"http://api.example.com",
		"http://localhost.example.com",
		"http://128.0.0.1",
		"http://[::2]",
		"https://api.example.com?",
		"https://api.example.com#",
		"https://:443",
		"https://api.example.com:not-a-port",
		"https://exam\nple.com",
		"https://exam\tple.com",
	} {
		t.Run("invalid "+baseURL, func(t *testing.T) {
			_, err := newRequestClient(ClientConfig{BaseURL: baseURL, APIKey: "key", WorkspaceID: testWorkspaceID})
			assertErrorCode(t, err, "MEDALLION_INVALID_OPTIONS")
		})
	}
}

func TestWorkspaceIDsMustBeCanonicalAtConstruction(t *testing.T) {
	for _, test := range []struct {
		name   string
		config ClientConfig
		code   string
	}{
		{name: "missing", config: ClientConfig{}, code: "MEDALLION_MISSING_WORKSPACE_ID"},
		{name: "leading space", config: ClientConfig{WorkspaceID: " " + testWorkspaceID}, code: "MEDALLION_INVALID_WORKSPACE_ID"},
		{name: "uppercase", config: ClientConfig{WorkspaceID: "ws_01JZ9Q5G6RSF7R5AR4RAH1B2C3"}, code: "MEDALLION_INVALID_WORKSPACE_ID"},
		{name: "forbidden i", config: ClientConfig{WorkspaceID: "ws_01jz9q5g6rsf7r5ar4rah1b2ci"}, code: "MEDALLION_INVALID_WORKSPACE_ID"},
		{name: "wrong prefix", config: ClientConfig{WorkspaceID: "wk_01jz9q5g6rsf7r5ar4rah1b2c3"}, code: "MEDALLION_INVALID_WORKSPACE_ID"},
		{name: "too short", config: ClientConfig{WorkspaceID: "ws_01jz9q5g6rsf7r5ar4rah1b2c"}, code: "MEDALLION_INVALID_WORKSPACE_ID"},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.config.BaseURL = "https://api.example.com"
			test.config.APIKey = "key"
			_, err := NewClient(test.config)
			assertErrorCode(t, err, test.code)
		})
	}
}

func TestOversizedSuccessAndErrorResponsesAreTypedAndNeverRetried(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		for _, declared := range []bool{true, false} {
			t.Run(fmt.Sprintf("status_%d/declared_%t", status, declared), func(t *testing.T) {
				calls := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls++
					w.Header().Set("content-type", "application/json")
					w.Header().Set("x-request-id", "req_oversized")
					if declared {
						w.Header().Set("Content-Length", "65")
					}
					w.WriteHeader(status)
					if !declared {
						w.(http.Flusher).Flush()
					}
					_, _ = io.WriteString(w, strings.Repeat("x", 65))
				}))
				defer server.Close()

				client, err := NewClient(ClientConfig{
					BaseURL:     server.URL,
					APIKey:      "key",
					WorkspaceID: testWorkspaceID,
					Retry:       RetryConfig{MaxAttempts: 2},
				})
				if err != nil {
					t.Fatalf("new client: %v", err)
				}
				client.Ingest.requests.responseMax = 64
				err = listTables(context.Background(), client)
				assertErrorCode(t, err, "MEDALLION_RESPONSE_TOO_LARGE")
				var sdkErr *Error
				if !errors.As(err, &sdkErr) || sdkErr.RequestID != "req_oversized" || !errors.Is(err, errResponseTooLarge) {
					t.Fatalf("oversized response error = %#v", err)
				}
				if calls != 1 {
					t.Fatalf("oversized response made %d calls, want 1", calls)
				}
			})
		}
	}
}

func TestTerminalHTTPResponseReadFailuresAreNeverRetried(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"X-Request-Id": []string{"req_read_failure"}},
					Body:       failingReadCloser{err: errors.New("response read failed")},
					Request:    request,
				}, nil
			})
			client, err := NewClient(ClientConfig{
				BaseURL:     "https://api.example.com",
				APIKey:      "key",
				WorkspaceID: testWorkspaceID,
				HTTPClient:  &http.Client{Transport: transport},
				Retry: RetryConfig{
					MaxAttempts:    3,
					InitialBackoff: time.Millisecond,
					MaxBackoff:     time.Millisecond,
				},
			})
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			err = listTables(context.Background(), client)
			assertErrorCode(t, err, "MEDALLION_NETWORK_ERROR")
			if !errors.Is(err, errResponseReadFailure) || strings.Contains(fmt.Sprintf("%v", errors.Unwrap(err)), "response read failed") {
				t.Fatalf("response-read cause was not replaced with the safe SDK sentinel: %#v", err)
			}
			if calls != 1 {
				t.Fatalf("terminal status response-read failure made %d calls, want 1", calls)
			}
		})
	}
}

func TestTransportErrorCauseIsSanitizedWithoutLosingClassification(t *testing.T) {
	const sensitiveTransportText = "customer-payload-from-round-tripper"
	client, err := NewClient(ClientConfig{
		BaseURL:     "https://api.example.com",
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New(sensitiveTransportText)
		})},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	err = listTables(context.Background(), client)
	assertErrorCode(t, err, "MEDALLION_NETWORK_ERROR")
	if !errors.Is(err, errTransportFailure) {
		t.Fatalf("transport error does not retain the safe classification sentinel: %#v", err)
	}
	if strings.Contains(fmt.Sprintf("%+v", err), sensitiveTransportText) || strings.Contains(fmt.Sprintf("%+v", errors.Unwrap(err)), sensitiveTransportText) {
		t.Fatalf("arbitrary transport error text leaked through the public error: %#v", err)
	}
}

func TestEveryIngestRPCRejectsEmptySuccessBodies(t *testing.T) {
	schema := &ingestv1.TableSchema{Columns: []*ingestv1.ColumnSchema{
		{Name: "occurred_at", Type: "TIMESTAMP"},
	}}
	for _, status := range []int{http.StatusOK, http.StatusNoContent} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("content-type", "application/json")
				w.Header().Set("x-request-id", "req_empty")
				w.WriteHeader(status)
			}))
			defer server.Close()
			client, err := NewClient(ClientConfig{
				BaseURL:     server.URL,
				APIKey:      "key",
				WorkspaceID: testWorkspaceID,
			})
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			ctx := context.Background()
			checks := []struct {
				name string
				call func() error
			}{
				{"CreateTable", func() error {
					_, _, err := client.Ingest.CreateTable(ctx, &ingestv1.CreateTableRequest{
						TableId: "events",
						Table:   &ingestv1.Table{Schema: schema, TimeColumn: "occurred_at"},
					})
					return err
				}},
				{"GetTable", func() error {
					_, _, err := client.Ingest.GetTable(ctx, &ingestv1.GetTableRequest{Name: "tables/events"})
					return err
				}},
				{"ListTables", func() error {
					return listTables(ctx, client)
				}},
				{"UpdateTable", func() error {
					_, _, err := client.Ingest.UpdateTable(ctx, &ingestv1.UpdateTableRequest{
						Table: &ingestv1.Table{Name: "tables/events", Schema: schema},
					})
					return err
				}},
				{"AppendRows", func() error {
					_, _, err := client.Ingest.AppendRows(ctx, &ingestv1.AppendRowsRequest{
						Table: "tables/events",
						Rows:  []*ingestv1.Row{jsonRow(t, map[string]any{"level": "info"})},
					})
					return err
				}},
				{"RunQuery", func() error {
					_, _, err := client.Ingest.RunQuery(ctx, &ingestv1.RunQueryRequest{Query: "SELECT 1"})
					return err
				}},
				{"GetQueryResults", func() error {
					_, _, err := client.Ingest.GetQueryResults(ctx, &ingestv1.GetQueryResultsRequest{Name: testQueryName})
					return err
				}},
			}
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					err := check.call()
					assertErrorCode(t, err, "MEDALLION_INVALID_JSON_RESPONSE")
					var sdkErr *Error
					if !errors.As(err, &sdkErr) || sdkErr.RequestID != "req_empty" {
						t.Fatalf("empty response error = %#v", err)
					}
				})
			}
		})
	}
}

func TestAppendRowsRetryIsOptInBoundedAndReusesExactBody(t *testing.T) {
	var bodies []string
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		raw, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		bodies = append(bodies, string(raw))
		keys = append(keys, request.Header.Get("Idempotency-Key"))
		w.Header().Set("content-type", "application/json")
		if len(bodies) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			writeConnectError(t, w, "resource_exhausted", "BACKPRESSURE", map[string]string{"retry_scope": "table"})
			return
		}
		_, _ = w.Write([]byte(`{"acceptedRows":"1"}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
		Retry: RetryConfig{
			MaxAttempts:    2,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("new retry client: %v", err)
	}
	ctx := WithIngestIdempotencyKey(context.Background(), "outbox:batch:7")
	response, _, err := client.Ingest.AppendRows(ctx, &ingestv1.AppendRowsRequest{
		Table: "tables/events",
		Rows:  []*ingestv1.Row{jsonRow(t, map[string]any{"level": "info"})},
	})
	if err != nil {
		t.Fatalf("append with retry: %v", err)
	}
	if response.GetAcceptedRows() != 1 {
		t.Fatalf("accepted rows = %d", response.GetAcceptedRows())
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] {
		t.Fatalf("retry bodies differ: %#v", bodies)
	}
	if keys[0] != "outbox:batch:7" || keys[1] != keys[0] {
		t.Fatalf("retry idempotency keys = %#v", keys)
	}
	if !strings.Contains(bodies[0], `"requestId":"outbox:batch:7"`) || strings.Contains(bodies[0], "workspaceId") {
		t.Fatalf("append body = %s", bodies[0])
	}

	// Without an opt-in retry budget the same transient failure is final.
	calls := 0
	single := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeConnectError(t, w, "resource_exhausted", "BACKPRESSURE", nil)
	}))
	defer single.Close()
	defaultClient, err := NewClient(ClientConfig{BaseURL: single.URL, APIKey: "key", WorkspaceID: testWorkspaceID})
	if err != nil {
		t.Fatalf("new default client: %v", err)
	}
	_, _, err = defaultClient.Ingest.AppendRows(context.Background(), &ingestv1.AppendRowsRequest{
		Table: "tables/events",
		Rows:  []*ingestv1.Row{jsonRow(t, map[string]any{"level": "info"})},
	})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || calls != 1 {
		t.Fatalf("default append error = %#v; calls = %d", err, calls)
	}
}

func TestUnknownErrorInfoIsStructuredAndNotRetried(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_denied")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeConnectError(t, w, "unavailable", "POLICY_DENIED", map[string]string{"policy": "workspace-boundary"})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
		Retry: RetryConfig{
			MaxAttempts:    5,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	err = listTables(context.Background(), client)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %#v", err)
	}
	if calls != 1 || apiErr.Code != "unavailable" || apiErr.RequestID != "req_denied" || apiErr.Retryable(true) {
		t.Fatalf("API error = %#v; calls = %d", apiErr, calls)
	}
	if apiErr.ErrorInfo == nil || apiErr.ErrorInfo.Reason != "POLICY_DENIED" || apiErr.ErrorInfo.Domain != ErrorInfoDomain || apiErr.ErrorInfo.Metadata["policy"] != "workspace-boundary" {
		t.Fatalf("ErrorInfo = %#v", apiErr.ErrorInfo)
	}
	if len(apiErr.Details) != 1 || apiErr.Details[0].Type != "google.rpc.ErrorInfo" || len(apiErr.Details[0].Value) == 0 {
		t.Fatalf("details = %#v", apiErr.Details)
	}
	if _, retained := reflect.TypeOf(*apiErr).FieldByName("ResponseBody"); retained {
		t.Fatal("APIError must not retain raw response bodies")
	}
	serialized, err := json.Marshal(apiErr)
	if err != nil {
		t.Fatalf("marshal API error: %v", err)
	}
	if strings.Contains(string(serialized), "secret-provider-token") {
		t.Fatalf("API error retained a sensitive raw body field: %s", serialized)
	}
}

func TestAPIErrorRedactsConfiguredCredentialsEverywhere(t *testing.T) {
	for _, test := range []struct {
		name       string
		configure  func(*ClientConfig)
		credential string
	}{
		{
			name:       "API key",
			credential: testCredential("api-key"),
			configure: func(config *ClientConfig) {
				config.APIKey = testCredential("api-key")
			},
		},
		{
			name:       "access token",
			credential: testCredential("jwt"),
			configure: func(config *ClientConfig) {
				config.AccessToken = testCredential("jwt")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("content-type", "application/json")
				w.Header().Set("x-request-id", "req-"+test.credential)
				w.WriteHeader(http.StatusServiceUnavailable)
				raw := protowire.AppendTag(nil, 1, protowire.BytesType)
				raw = protowire.AppendString(raw, "BACKPRESSURE")
				raw = protowire.AppendTag(raw, 2, protowire.BytesType)
				raw = protowire.AppendString(raw, ErrorInfoDomain)
				entry := protowire.AppendTag(nil, 1, protowire.BytesType)
				entry = protowire.AppendString(entry, "credential")
				entry = protowire.AppendTag(entry, 2, protowire.BytesType)
				entry = protowire.AppendString(entry, test.credential)
				raw = protowire.AppendTag(raw, 3, protowire.BytesType)
				raw = protowire.AppendBytes(raw, entry)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":    "unavailable",
					"message": "provider echoed " + test.credential,
					"details": []any{map[string]any{
						"type":  "google.rpc.ErrorInfo",
						"value": base64.RawStdEncoding.EncodeToString(raw),
					}},
				})
			}))
			defer server.Close()
			config := ClientConfig{
				BaseURL:     server.URL,
				WorkspaceID: testWorkspaceID,
			}
			test.configure(&config)
			client, err := NewClient(config)
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			err = listTables(context.Background(), client)
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("error = %#v", err)
			}
			serialized, marshalErr := json.Marshal(apiErr)
			if marshalErr != nil {
				t.Fatalf("marshal error: %v", marshalErr)
			}
			if strings.Contains(string(serialized), test.credential) {
				t.Fatalf("credential leaked through APIError: %s", serialized)
			}
			if !strings.Contains(apiErr.RequestID, "[REDACTED]") || apiErr.ErrorInfo == nil || apiErr.ErrorInfo.Metadata["credential"] != "[REDACTED]" {
				t.Fatalf("redacted API error = %#v", apiErr)
			}
			if len(apiErr.Details) != 0 {
				t.Fatalf("credential-bearing raw detail was retained: %#v", apiErr.Details)
			}
		})
	}
}

func TestAPIErrorRetryGuidanceRequiresExplicitOperationSafety(t *testing.T) {
	retryable := &APIError{Status: http.StatusServiceUnavailable, Code: "unavailable"}
	if retryable.Retryable(false) {
		t.Fatal("transient transport status must not imply that an unsafe operation can be replayed")
	}
	if !retryable.Retryable(true) {
		t.Fatal("safe idempotent operation should expose transient retry guidance")
	}
}

func TestAPIErrorMessageAndCodeAreSanitized(t *testing.T) {
	message := apiErrorMessage(http.StatusBadRequest, "req_123")
	if message != "Medallion API request failed with HTTP 400. Request ID: req_123." {
		t.Fatalf("generic message = %q", message)
	}
	if got := sanitizeConnectCode(" UNAVAILABLE "); got != "unavailable" {
		t.Fatalf("sanitized code = %q", got)
	}
	if got := sanitizeConnectCode("unavailable\r\nInjected"); got != "" {
		t.Fatalf("unsafe code = %q", got)
	}
}

func TestRetriesAreDisabledByDefaultAndStrictlyBounded(t *testing.T) {
	client, err := newRequestClient(ClientConfig{BaseURL: "https://api.example.com", APIKey: "key", WorkspaceID: testWorkspaceID})
	if err != nil {
		t.Fatalf("new request client: %v", err)
	}
	if client.retry.MaxAttempts != 1 {
		t.Fatalf("default attempts = %d, want 1", client.retry.MaxAttempts)
	}
	withRetries, err := newRequestClient(ClientConfig{
		BaseURL:     "https://api.example.com",
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
		Retry:       RetryConfig{MaxAttempts: 2},
	})
	if err != nil || withRetries.retry.JitterRatio != 0.2 {
		t.Fatalf("default retry jitter = %v, %v", withRetries.retry.JitterRatio, err)
	}
	_, err = NewClient(ClientConfig{
		BaseURL:     "https://api.example.com",
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
		Retry:       RetryConfig{MaxAttempts: maxRetryAttempts + 1},
	})
	assertErrorCode(t, err, "MEDALLION_INVALID_OPTIONS")
	_, err = NewClient(ClientConfig{
		BaseURL:     "https://api.example.com",
		APIKey:      "key",
		AccessToken: "token",
		WorkspaceID: testWorkspaceID,
	})
	assertErrorCode(t, err, "MEDALLION_INVALID_OPTIONS")
	_, err = NewClient(ClientConfig{
		BaseURL:     "https://api.example.com",
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
		Retry:       RetryConfig{MaxAttempts: 2, JitterRatio: 1.1},
	})
	assertErrorCode(t, err, "MEDALLION_INVALID_OPTIONS")
}

func TestCancellationInterruptsRetryBackoff(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeConnectError(t, w, "resource_exhausted", "BACKPRESSURE", nil)
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
		Retry: RetryConfig{
			MaxAttempts:    5,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(5*time.Millisecond, cancel)
	err = listTables(ctx, client)
	assertErrorCode(t, err, "MEDALLION_ABORTED")
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("error = %#v; calls = %d", err, calls)
	}
}

func TestRetryAfterIsNotCappedOrJittered(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	client := &requestClient{
		retry:       RetryConfig{InitialBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, JitterRatio: 0.2},
		now:         func() time.Time { return fixedNow },
		randomFloat: func() float64 { return 0 },
	}
	if got := client.retryDelay(1, "2"); got != 2*time.Second {
		t.Fatalf("delta Retry-After = %s, want 2s", got)
	}
	if got := client.retryDelay(1, fixedNow.Add(3*time.Second).Format(http.TimeFormat)); got != 3*time.Second {
		t.Fatalf("date Retry-After = %s, want 3s", got)
	}
	if got := client.retryDelay(1, ""); got != 8*time.Millisecond {
		t.Fatalf("lower jitter boundary = %s, want 8ms", got)
	}
	client.randomFloat = func() float64 { return 1 }
	if got := client.retryDelay(1, "invalid"); got != 12*time.Millisecond {
		t.Fatalf("upper jitter boundary = %s, want 12ms", got)
	}
	if got := client.retryDelay(2, ""); got != 20*time.Millisecond {
		t.Fatalf("bounded jitter at max backoff = %s, want 20ms", got)
	}
}

func TestRetryAfterThatCannotFitDeadlineDeclinesRetry(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("content-type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeConnectError(t, w, "resource_exhausted", "BACKPRESSURE", nil)
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
		Retry: RetryConfig{
			MaxAttempts:    3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = listTables(ctx, client)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || calls != 1 {
		t.Fatalf("error = %#v; calls = %d", err, calls)
	}
}

func TestSuccessfulResponseAllowsAdditiveFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"tables":[],"nextPageToken":"","futurePageMetadata":{"version":2}}`))
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{
		BaseURL:     server.URL,
		APIKey:      "key",
		WorkspaceID: testWorkspaceID,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	page, _, err := client.Ingest.ListTables(context.Background(), &ingestv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("list with additive response field: %v", err)
	}
	if len(page.GetTables()) != 0 || page.GetNextPageToken() != "" {
		t.Fatalf("page = %#v", page)
	}
}

func TestStableIdempotencyKeyMatchesCrossLanguageVector(t *testing.T) {
	// The same vector is pinned in the TypeScript and Python SDKs.
	first, err := StableIdempotencyKey(" orders ", "partition-1", json.Number("42"))
	if err != nil {
		t.Fatalf("stable key: %v", err)
	}
	const expected = "orders:103ec0f8-cc69-5f19-81d9-08f2d641a5e4"
	if first != expected {
		t.Fatalf("stable key = %q, want %q", first, expected)
	}
	second, err := StableIdempotencyKey("orders", "partition-1", int64(42))
	if err != nil || second != first {
		t.Fatalf("determinism = %q, %v", second, err)
	}
	different, err := StableIdempotencyKey("orders", "partition-1", int64(43))
	if err != nil || different == first {
		t.Fatalf("different identity key = %q, %v", different, err)
	}
	_, err = StableIdempotencyKey("orders")
	assertErrorCode(t, err, "MEDALLION_INVALID_IDEMPOTENCY_KEY")
}

func assertConnectTimeout(t *testing.T, value string) {
	t.Helper()
	if value == "" || len(value) > 10 {
		t.Fatalf("Connect-Timeout-Ms = %q", value)
	}
	milliseconds, err := strconv.ParseUint(value, 10, 64)
	if err != nil || milliseconds == 0 || milliseconds > 2000 {
		t.Fatalf("Connect-Timeout-Ms = %q, %v", value, err)
	}
}

func writeConnectError(t *testing.T, writer http.ResponseWriter, code, reason string, metadata map[string]string) {
	t.Helper()
	raw := protowire.AppendTag(nil, 1, protowire.BytesType)
	raw = protowire.AppendString(raw, reason)
	raw = protowire.AppendTag(raw, 2, protowire.BytesType)
	raw = protowire.AppendString(raw, ErrorInfoDomain)
	for key, value := range metadata {
		entry := protowire.AppendTag(nil, 1, protowire.BytesType)
		entry = protowire.AppendString(entry, key)
		entry = protowire.AppendTag(entry, 2, protowire.BytesType)
		entry = protowire.AppendString(entry, value)
		raw = protowire.AppendTag(raw, 3, protowire.BytesType)
		raw = protowire.AppendBytes(raw, entry)
	}
	if err := json.NewEncoder(writer).Encode(map[string]any{
		"code":    code,
		"message": "request failed",
		"private": "secret-provider-token",
		"details": []any{map[string]any{
			"type":  "google.rpc.ErrorInfo",
			"value": base64.RawStdEncoding.EncodeToString(raw),
		}},
	}); err != nil {
		t.Errorf("encode Connect error: %v", err)
	}
}

func testCredential(kind string) string {
	return kind + "-fixture-not-a-real-credential"
}

func assertErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var sdkErr *Error
	if !errors.As(err, &sdkErr) || sdkErr.Code != code {
		t.Fatalf("error = %#v, want code %q", err, code)
	}
}
