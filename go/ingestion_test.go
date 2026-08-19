package medallion

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

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

type failingReadCloser struct {
	err error
}

const (
	testWorkspaceID  = "ws_01jz9q5g6rsf7r5ar4rah1b2c3"
	otherWorkspaceID = "ws_01jz9q5g6rsf7r5ar4rah1b2c4"
)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func (reader failingReadCloser) Read([]byte) (int, error) {
	return 0, reader.err
}

func (failingReadCloser) Close() error {
	return nil
}

func TestGoSurfaceSupportsExactlyFourIngestionRPCs(t *testing.T) {
	expected := map[string]struct{}{
		"PublishCdcEvents":   {},
		"ListCdcEvents":      {},
		"PublishAuditEvents": {},
		"ListAuditEvents":    {},
	}
	clientType := reflect.TypeOf((*ConnectClient)(nil))
	if clientType.NumMethod() != len(expected) {
		t.Fatalf("ConnectClient exported method count = %d, want %d", clientType.NumMethod(), len(expected))
	}
	for index := 0; index < clientType.NumMethod(); index++ {
		method := clientType.Method(index)
		if _, ok := expected[method.Name]; !ok {
			t.Fatalf("unsupported low-level method is exposed: %s", method.Name)
		}
	}

	service := connectv1.File_medallion_connect_v1_connect_proto.Services().ByName("MedallionConnectService")
	if service == nil {
		t.Fatal("generated MedallionConnectService descriptor is missing")
	}
	for method := range expected {
		if service.Methods().ByName(protoreflect.Name(method)) == nil {
			t.Fatalf("generated descriptor is missing %s", method)
		}
	}
	if service.Methods().ByName("PublishPlatformAuditEvents") != nil {
		t.Fatal("system-only PublishPlatformAuditEvents leaked into the public generated binding")
	}

	configType := reflect.TypeOf(ClientConfig{})
	wantConfigFields := map[string]struct{}{
		"BaseURL": {}, "APIKey": {}, "AccessToken": {}, "WorkspaceID": {},
		"DefaultConnectorID": {}, "Timeout": {}, "Retry": {}, "HTTPClient": {}, "Tracing": {},
	}
	if configType.NumField() != len(wantConfigFields) {
		t.Fatalf("ClientConfig field count = %d, want %d", configType.NumField(), len(wantConfigFields))
	}
	for index := 0; index < configType.NumField(); index++ {
		if _, allowed := wantConfigFields[configType.Field(index).Name]; !allowed {
			t.Fatalf("unexpected ClientConfig field: %s", configType.Field(index).Name)
		}
	}
}

func TestConnectAuthenticationWorkspaceAndTimeoutHeaders(t *testing.T) {
	var headers []http.Header
	var bodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		headers = append(headers, request.Header.Clone())
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode list request: %v", err)
		}
		bodies = append(bodies, body)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{}`))
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
	if _, _, err := apiClient.Connect.ListCdcEvents(ctx, &connectv1.ListCdcEventsRequest{}); err != nil {
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
	if got := bodies[0]["workspaceId"]; got != testWorkspaceID {
		t.Fatalf("API-key body workspace = %#v", got)
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
	if _, _, err := jwtClient.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{}); err != nil {
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
	if got := bodies[1]["workspaceId"]; got != otherWorkspaceID {
		t.Fatalf("JWT body workspace = %#v", got)
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
				_, _, err = client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{})
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

func TestWorkspaceSelectorConflictsFailBeforeNetwork(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls++
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "key",
		WorkspaceID:        testWorkspaceID,
		DefaultConnectorID: "connector_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	checks := []struct {
		name string
		call func() error
	}{
		{
			name: "low-level CDC",
			call: func() error {
				_, _, err := client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{WorkspaceId: otherWorkspaceID})
				return err
			},
		},
		{
			name: "low-level audit",
			call: func() error {
				_, _, err := client.Connect.ListAuditEvents(context.Background(), &connectv1.ListAuditEventsRequest{WorkspaceId: otherWorkspaceID})
				return err
			},
		},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			assertErrorCode(t, check.call(), "MEDALLION_WORKSPACE_SELECTOR_CONFLICT")
		})
	}
	if calls != 0 {
		t.Fatalf("selector conflicts sent %d HTTP requests", calls)
	}
}

func TestListResponsesFailClosedOnWorkspaceMismatch(t *testing.T) {
	for _, test := range []struct {
		name string
		path string
		body string
		code string
		call func(*Client) error
	}{
		{
			name: "CDC mismatch",
			path: listCdcEventsPath,
			body: `{"events":[{"workspace_id":"ws_01jz9q5g6rsf7r5ar4rah1b2c4"}]}`,
			code: "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
			call: func(client *Client) error {
				_, _, err := client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{})
				return err
			},
		},
		{
			name: "audit mismatch",
			path: listAuditEventsPath,
			body: `{"events":[{"workspace_id":"ws_01jz9q5g6rsf7r5ar4rah1b2c4"}]}`,
			code: "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
			call: func(client *Client) error {
				_, _, err := client.Connect.ListAuditEvents(context.Background(), &connectv1.ListAuditEventsRequest{})
				return err
			},
		},
		{
			name: "CDC omitted",
			path: listCdcEventsPath,
			body: `{"events":[{}]}`,
			code: "MEDALLION_INVALID_LIST_RESPONSE",
			call: func(client *Client) error {
				_, _, err := client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{})
				return err
			},
		},
		{
			name: "audit omitted",
			path: listAuditEventsPath,
			body: `{"events":[{}]}`,
			code: "MEDALLION_INVALID_LIST_RESPONSE",
			call: func(client *Client) error {
				_, _, err := client.Connect.ListAuditEvents(context.Background(), &connectv1.ListAuditEventsRequest{})
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != test.path {
					t.Fatalf("path = %q, want %q", request.URL.Path, test.path)
				}
				w.Header().Set("content-type", "application/json")
				w.Header().Set("x-request-id", "req_workspace")
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "key", WorkspaceID: testWorkspaceID})
			if err != nil {
				t.Fatal(err)
			}
			err = test.call(client)
			assertErrorCode(t, err, test.code)
			var sdkErr *Error
			if !errors.As(err, &sdkErr) || sdkErr.RequestID != "req_workspace" {
				t.Fatalf("error = %#v", err)
			}
		})
	}
}

func TestLowLevelListsValidateCompleteEventsAndCursors(t *testing.T) {
	for _, test := range []struct {
		name string
		path string
		body map[string]any
		call func(*Client) error
	}{
		{
			name: "CDC event",
			path: listCdcEventsPath,
			body: map[string]any{"events": []any{map[string]any{
				"id": "1", "workspaceId": testWorkspaceID, "entityType": "order", "entityId": "1",
				"operation": "CDC_OPERATION_INSERT", "idempotencyKey": "cdc-low-level", "payloadJson": "{}",
			}}},
			call: func(client *Client) error {
				_, _, err := client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{})
				return err
			},
		},
		{
			name: "audit event",
			path: listAuditEventsPath,
			body: map[string]any{"events": []any{map[string]any{
				"id": "1", "workspaceId": testWorkspaceID, "resourceType": "order", "resourceId": "1",
				"idempotencyKey": "audit-low-level", "payloadJson": "{}",
				"origin": "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER", "outcome": "AUDIT_EVENT_OUTCOME_SUCCEEDED",
			}}},
			call: func(client *Client) error {
				_, _, err := client.Connect.ListAuditEvents(context.Background(), &connectv1.ListAuditEventsRequest{})
				return err
			},
		},
		{
			name: "CDC cursor",
			path: listCdcEventsPath,
			body: map[string]any{"nextPageCursor": strings.Repeat("c", maxListCursorBytes+1)},
			call: func(client *Client) error {
				_, _, err := client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{})
				return err
			},
		},
		{
			name: "audit cursor",
			path: listAuditEventsPath,
			body: map[string]any{"nextPageCursor": strings.Repeat("c", maxListCursorBytes+1)},
			call: func(client *Client) error {
				_, _, err := client.Connect.ListAuditEvents(context.Background(), &connectv1.ListAuditEventsRequest{})
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != test.path {
					t.Fatalf("path = %q, want %q", request.URL.Path, test.path)
				}
				w.Header().Set("content-type", "application/json")
				w.Header().Set("x-request-id", "req_low_level_list")
				_ = json.NewEncoder(w).Encode(test.body)
			}))
			defer server.Close()
			client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "key", WorkspaceID: testWorkspaceID})
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			err = test.call(client)
			var sdkErr *Error
			if !errors.As(err, &sdkErr) || sdkErr.Code != "MEDALLION_INVALID_LIST_RESPONSE" || sdkErr.RequestID != "req_low_level_list" {
				t.Fatalf("low-level list error = %#v", err)
			}
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
					Retry: RetryConfig{
						MaxAttempts: 2,
					},
				})
				if err != nil {
					t.Fatalf("new client: %v", err)
				}
				client.Connect.requests.responseMax = 64
				_, _, err = client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{})
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
			_, _, err = client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{})
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
	_, _, err = client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{})
	assertErrorCode(t, err, "MEDALLION_NETWORK_ERROR")
	if !errors.Is(err, errTransportFailure) {
		t.Fatalf("transport error does not retain the safe classification sentinel: %#v", err)
	}
	if strings.Contains(fmt.Sprintf("%+v", err), sensitiveTransportText) || strings.Contains(fmt.Sprintf("%+v", errors.Unwrap(err)), sensitiveTransportText) {
		t.Fatalf("arbitrary transport error text leaked through the public error: %#v", err)
	}
}

func TestAllConnectRPCsRejectEmptySuccessBodies(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNoContent} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("content-type", "application/json")
				w.Header().Set("x-request-id", "req_empty")
				w.WriteHeader(status)
			}))
			defer server.Close()
			client, err := NewClient(ClientConfig{
				BaseURL:            server.URL,
				APIKey:             "key",
				WorkspaceID:        testWorkspaceID,
				DefaultConnectorID: "connector_123",
			})
			if err != nil {
				t.Fatalf("new client: %v", err)
			}

			checks := []struct {
				name string
				call func() error
			}{
				{
					name: "PublishCdcEvents",
					call: func() error {
						_, _, err := client.Connect.PublishCdcEvents(context.Background(), &connectv1.PublishCdcEventsRequest{Events: []*connectv1.CdcEvent{{
							StreamName: "orders", EntityType: "order", EntityId: "1", Operation: connectv1.CdcOperation_CDC_OPERATION_INSERT, IdempotencyKey: "cdc_empty", PayloadJson: "{}",
						}}})
						return err
					},
				},
				{
					name: "ListCdcEvents",
					call: func() error {
						_, _, err := client.Connect.ListCdcEvents(context.Background(), &connectv1.ListCdcEventsRequest{})
						return err
					},
				},
				{
					name: "PublishAuditEvents",
					call: func() error {
						_, _, err := client.Connect.PublishAuditEvents(context.Background(), &connectv1.PublishAuditEventsRequest{Events: []*connectv1.AuditEvent{{
							ResourceType: "order", ResourceId: "1", Action: "read", Outcome: connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED, IdempotencyKey: "audit_empty", PayloadJson: "{}",
						}}})
						return err
					},
				},
				{
					name: "ListAuditEvents",
					call: func() error {
						_, _, err := client.Connect.ListAuditEvents(context.Background(), &connectv1.ListAuditEventsRequest{})
						return err
					},
				},
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

func TestLowLevelPublishRPCsRejectMalformedAcknowledgements(t *testing.T) {
	malformations := []struct {
		name string
		body string
	}{
		{
			name: "aggregate count does not cover request",
			body: `{"acceptedCount":1,"events":[` +
				`{"idempotencyKey":"first","eventId":"1"},` +
				`{"idempotencyKey":"second","eventId":"2"}]}`,
		},
		{
			name: "results are out of request order",
			body: `{"acceptedCount":2,"events":[` +
				`{"idempotencyKey":"second","eventId":"1"},` +
				`{"idempotencyKey":"first","eventId":"2"}]}`,
		},
		{
			name: "event ID is not positive",
			body: `{"acceptedCount":2,"events":[` +
				`{"idempotencyKey":"first","eventId":"0"},` +
				`{"idempotencyKey":"second","eventId":"2"}]}`,
		},
		{
			name: "duplicate flags disagree with aggregates",
			body: `{"acceptedCount":2,"events":[` +
				`{"idempotencyKey":"first","eventId":"1","duplicate":true},` +
				`{"idempotencyKey":"second","eventId":"2"}]}`,
		},
		{
			name: "uint32 aggregates cannot wrap",
			body: `{"acceptedCount":4294967295,"duplicateCount":3,"events":[` +
				`{"idempotencyKey":"first","eventId":"1"},` +
				`{"idempotencyKey":"second","eventId":"2"}]}`,
		},
	}

	families := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "CDC",
			call: func(client *Client) error {
				_, _, err := client.Connect.PublishCdcEvents(context.Background(), &connectv1.PublishCdcEventsRequest{Events: []*connectv1.CdcEvent{
					{StreamName: "orders", EntityType: "order", EntityId: "1", Operation: connectv1.CdcOperation_CDC_OPERATION_INSERT, IdempotencyKey: "first", PayloadJson: "{}"},
					{StreamName: "orders", EntityType: "order", EntityId: "2", Operation: connectv1.CdcOperation_CDC_OPERATION_UPDATE, IdempotencyKey: "second", PayloadJson: "{}"},
				}})
				return err
			},
		},
		{
			name: "audit",
			call: func(client *Client) error {
				_, _, err := client.Connect.PublishAuditEvents(context.Background(), &connectv1.PublishAuditEventsRequest{Events: []*connectv1.AuditEvent{
					{ResourceType: "order", ResourceId: "1", Action: "read", Outcome: connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED, IdempotencyKey: "first", PayloadJson: "{}"},
					{ResourceType: "order", ResourceId: "2", Action: "read", Outcome: connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED, IdempotencyKey: "second", PayloadJson: "{}"},
				}})
				return err
			},
		},
	}

	for _, family := range families {
		family := family
		for _, malformation := range malformations {
			malformation := malformation
			t.Run(family.name+"/"+malformation.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("content-type", "application/json")
					w.Header().Set("x-request-id", "req_malformed_receipt")
					_, _ = io.WriteString(w, malformation.body)
				}))
				defer server.Close()

				client, err := NewClient(ClientConfig{
					BaseURL:            server.URL,
					APIKey:             "key",
					WorkspaceID:        testWorkspaceID,
					DefaultConnectorID: "connector_123",
				})
				if err != nil {
					t.Fatalf("new client: %v", err)
				}
				err = family.call(client)
				var sdkErr *Error
				if !errors.As(err, &sdkErr) || sdkErr.Code != "MEDALLION_INVALID_PUBLISH_RESPONSE" || sdkErr.RequestID != "req_malformed_receipt" {
					t.Fatalf("malformed low-level acknowledgement error = %#v", err)
				}
			})
		}
	}
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

func TestPublishRetryIsOptInBoundedAndReusesExactBody(t *testing.T) {
	var bodies []string
	var workspaceHeaders []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		raw, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		bodies = append(bodies, string(raw))
		workspaceHeaders = append(workspaceHeaders, request.Header.Get("X-Medallion-Workspace-Id"))
		w.Header().Set("content-type", "application/json")
		if len(bodies) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			writeConnectError(t, w, "resource_exhausted", "BACKPRESSURE", map[string]string{"retry_scope": "connector"})
			return
		}
		_, _ = w.Write([]byte(`{"accepted_count":1,"events":[{"idempotency_key":"cdc_retry","event_id":"9223372036854775807"}]}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "key",
		WorkspaceID:        testWorkspaceID,
		DefaultConnectorID: "conn_default",
		Retry: RetryConfig{
			MaxAttempts:    2,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("new retry client: %v", err)
	}
	request := &connectv1.PublishCdcEventsRequest{Events: []*connectv1.CdcEvent{{
		StreamName:     "orders",
		EntityType:     "order",
		EntityId:       "order_1",
		Operation:      connectv1.CdcOperation_CDC_OPERATION_UPDATE,
		IdempotencyKey: "cdc_retry",
		PayloadJson:    `{"large":9223372036854775807}`,
	}}}
	response, _, err := client.Connect.PublishCdcEvents(context.Background(), request)
	if err != nil {
		t.Fatalf("publish with retry: %v", err)
	}
	if response.GetEvents()[0].GetEventId() != int64(^uint64(0)>>1) {
		t.Fatalf("event ID = %d", response.GetEvents()[0].GetEventId())
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] {
		t.Fatalf("retry bodies differ: %#v", bodies)
	}
	if workspaceHeaders[0] != testWorkspaceID || workspaceHeaders[1] != testWorkspaceID {
		t.Fatalf("retry workspace headers = %#v", workspaceHeaders)
	}
	if strings.Contains(bodies[0], "workspaceId") {
		t.Fatalf("publish body included server-derived workspace: %s", bodies[0])
	}
	if strings.Contains(bodies[0], "conn_default") == false {
		t.Fatalf("default connector was not projected: %s", bodies[0])
	}
	if request.GetConnectorId() != "" {
		t.Fatalf("caller request was mutated: %#v", request)
	}
}

func TestEventIdempotencyKeysAcceptAnyBoundedUTF8(t *testing.T) {
	calls := 0
	validKeys := []string{
		" \tclé\n識別子\x00 ",
		strings.Repeat("é", 256), // exactly 512 UTF-8 bytes
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls++
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		key := body["events"].([]any)[0].(map[string]any)["idempotencyKey"].(string)
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accepted_count": 1,
			"events": []any{map[string]any{
				"idempotency_key": key,
				"event_id":        "1",
			}},
		})
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "key",
		WorkspaceID:        testWorkspaceID,
		DefaultConnectorID: "conn_123",
		Retry: RetryConfig{
			MaxAttempts:    3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	assertLocalKeyError := func(t *testing.T, err error, code string) {
		t.Helper()
		assertErrorCode(t, err, code)
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			t.Fatalf("local key validation was classified as a network API error: %#v", err)
		}
	}
	invalidKeys := []struct {
		name string
		key  string
		code string
	}{
		{name: "empty", key: "", code: "MEDALLION_MISSING_IDEMPOTENCY_KEY"},
		{name: "too long", key: strings.Repeat("k", 513), code: "MEDALLION_INVALID_IDEMPOTENCY_KEY"},
		{name: "too many UTF-8 bytes", key: strings.Repeat("é", 257), code: "MEDALLION_INVALID_IDEMPOTENCY_KEY"},
		{name: "invalid UTF-8", key: string([]byte{0xff}), code: "MEDALLION_INVALID_IDEMPOTENCY_KEY"},
	}
	for _, test := range invalidKeys {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.CDC.Record(context.Background(), CDCEvent{
				StreamName:     "orders",
				EntityType:     "order",
				EntityID:       "1",
				Operation:      "insert",
				IdempotencyKey: test.key,
				Payload:        map[string]any{},
			})
			assertLocalKeyError(t, err, test.code)
		})
	}

	_, _, err = client.Connect.PublishAuditEvents(context.Background(), &connectv1.PublishAuditEventsRequest{Events: []*connectv1.AuditEvent{
		{ResourceType: "order", ResourceId: "1", Action: "read", IdempotencyKey: "audit-valid", PayloadJson: "{}", Outcome: connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED},
		{ResourceType: "order", ResourceId: "2", Action: "read", IdempotencyKey: string([]byte{0xff}), PayloadJson: "{}", Outcome: connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED},
	}})
	assertLocalKeyError(t, err, "MEDALLION_INVALID_IDEMPOTENCY_KEY")
	if calls != 0 {
		t.Fatalf("invalid event keys sent %d HTTP requests despite retry configuration", calls)
	}

	for index, validKey := range validKeys {
		result, err := client.CDC.Record(context.Background(), CDCEvent{
			StreamName:     "orders",
			EntityType:     "order",
			EntityID:       strconv.Itoa(index + 1),
			Operation:      "insert",
			IdempotencyKey: validKey,
			Payload:        map[string]any{},
		})
		if err != nil || result.IdempotencyKey != validKey {
			t.Fatalf("valid UTF-8 key result = %#v, error = %v", result, err)
		}
	}
	if calls != len(validKeys) {
		t.Fatalf("valid key calls = %d, want %d", calls, len(validKeys))
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
	_, _, err = client.Connect.ListAuditEvents(context.Background(), &connectv1.ListAuditEventsRequest{})
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
			_, _, err = client.Connect.ListAuditEvents(context.Background(), &connectv1.ListAuditEventsRequest{})
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
			if !strings.Contains(apiErr.Message, "[REDACTED]") || !strings.Contains(apiErr.RequestID, "[REDACTED]") || apiErr.ErrorInfo == nil || apiErr.ErrorInfo.Metadata["credential"] != "[REDACTED]" {
				t.Fatalf("redacted API error = %#v", apiErr)
			}
			if len(apiErr.Details) != 0 {
				t.Fatalf("credential-bearing raw detail was retained: %#v", apiErr.Details)
			}
		})
	}
}

func TestAPIErrorRedactsCustomerPayloadValuesAndDropsSensitiveDetails(t *testing.T) {
	const (
		payloadSecret = "customer-payload-secret"
		payloadNumber = "98765432123456789"
	)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req-"+payloadSecret)
		w.WriteHeader(http.StatusBadRequest)

		errorInfo := protowire.AppendTag(nil, 1, protowire.BytesType)
		errorInfo = protowire.AppendString(errorInfo, "IDEMPOTENCY_MISMATCH")
		errorInfo = protowire.AppendTag(errorInfo, 2, protowire.BytesType)
		errorInfo = protowire.AppendString(errorInfo, ErrorInfoDomain)
		for key, value := range map[string]string{"echo": payloadSecret, "number": payloadNumber} {
			entry := protowire.AppendTag(nil, 1, protowire.BytesType)
			entry = protowire.AppendString(entry, key)
			entry = protowire.AppendTag(entry, 2, protowire.BytesType)
			entry = protowire.AppendString(entry, value)
			errorInfo = protowire.AppendTag(errorInfo, 3, protowire.BytesType)
			errorInfo = protowire.AppendBytes(errorInfo, entry)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code":    "invalid_argument",
			"message": "server echoed " + payloadSecret + " and " + payloadNumber,
			"details": []any{
				map[string]any{
					"type":  "google.rpc.ErrorInfo",
					"value": base64.RawStdEncoding.EncodeToString(errorInfo),
				},
				map[string]any{
					"type":  "example.future.Detail",
					"value": base64.RawStdEncoding.EncodeToString([]byte("echo " + payloadSecret)),
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "key",
		WorkspaceID:        testWorkspaceID,
		DefaultConnectorID: "connector_123",
		Retry: RetryConfig{
			MaxAttempts:    3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.CDC.Record(context.Background(), CDCEvent{
		StreamName:     "orders",
		EntityType:     "order",
		EntityID:       "1",
		Operation:      "insert",
		IdempotencyKey: "cdc-payload-redaction",
		Payload: map[string]any{
			"private":        payloadSecret,
			"numericPrivate": json.Number(payloadNumber),
		},
	})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %#v", err)
	}
	if calls != 1 {
		t.Fatalf("terminal idempotency mismatch made %d calls, want 1", calls)
	}
	if apiErr.ErrorInfo == nil || apiErr.ErrorInfo.Metadata["echo"] != "[REDACTED]" || apiErr.ErrorInfo.Metadata["number"] != "[REDACTED]" {
		t.Fatalf("redacted ErrorInfo = %#v", apiErr.ErrorInfo)
	}
	if len(apiErr.Details) != 0 {
		t.Fatalf("payload-bearing details were retained: %#v", apiErr.Details)
	}
	serialized, marshalErr := json.Marshal(apiErr)
	if marshalErr != nil {
		t.Fatalf("marshal API error: %v", marshalErr)
	}
	for _, sensitive := range []string{payloadSecret, payloadNumber} {
		if strings.Contains(string(serialized), sensitive) {
			t.Fatalf("payload value leaked through APIError: %s", serialized)
		}
	}
	if apiErr.Message != "Medallion API request failed with HTTP 400. Request ID: req-[REDACTED]." {
		t.Fatalf("generic API message = %q", apiErr.Message)
	}
}

func testCredential(kind string) string {
	return kind + "-fixture-not-a-real-credential"
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
	_, _, err = client.Connect.ListCdcEvents(ctx, &connectv1.ListCdcEventsRequest{})
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
	_, _, err = client.Connect.ListCdcEvents(ctx, &connectv1.ListCdcEventsRequest{})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || calls != 1 {
		t.Fatalf("error = %#v; calls = %d", err, calls)
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
		t.Fatalf("encode Connect error: %v", err)
	}
}

func TestSuccessfulResponseAllowsAdditiveFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"events":[],"next_page_cursor":"","future_page_metadata":{"version":2}}`))
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
	page, err := client.CDC.List(context.Background(), CDCListQuery{})
	if err != nil {
		t.Fatalf("list with additive response field: %v", err)
	}
	if len(page.Events) != 0 || page.NextCursor != "" {
		t.Fatalf("page = %#v", page)
	}
}

func TestStableIdempotencyKeyMatchesCrossLanguageVector(t *testing.T) {
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

func TestModernSingleBatchAndRawJSONInputs(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls++
		w.Header().Set("content-type", "application/json")
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		events := body["events"].([]any)
		for _, rawEvent := range events {
			event := rawEvent.(map[string]any)
			for _, field := range []string{"id", "workspaceId", "connectorId", "observedAt", "sourceSystem", "ingestedByPrincipal", "origin"} {
				if _, exists := event[field]; exists {
					t.Fatalf("customer event leaked server-derived %s: %#v", field, event)
				}
			}
		}
		switch request.URL.Path {
		case publishCdcEventsPath:
			event := events[0].(map[string]any)
			if event["streamName"] != "orders" || event["entityType"] != "order" || event["entityId"] != "42" || event["payloadJson"] != `{"amount":42,"status":"paid"}` {
				t.Fatalf("modern CDC event = %#v", event)
			}
			_, _ = w.Write([]byte(`{"accepted_count":1,"events":[{"idempotency_key":"cdc-modern-1","event_id":"101"}]}`))
		case publishAuditEventsPath:
			if len(events) != 2 {
				t.Fatalf("audit event count = %d", len(events))
			}
			first := events[0].(map[string]any)
			second := events[1].(map[string]any)
			if first["resourceType"] != "invoice" || first["resourceId"] != "invoice_1" || first["payloadJson"] != "{}" {
				t.Fatalf("modern audit event = %#v", first)
			}
			if second["payloadJson"] != `{"duplicate":1,"duplicate":2}` {
				t.Fatalf("raw payload JSON was not preserved: %#v", second["payloadJson"])
			}
			_, _ = w.Write([]byte(`{"accepted_count":2,"events":[{"idempotency_key":"audit-modern-1","event_id":"201"},{"idempotency_key":"audit-modern-2","event_id":"202"}]}`))
		default:
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "key",
		WorkspaceID:        testWorkspaceID,
		DefaultConnectorID: "connector_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	cdcResult, err := client.CDC.Record(context.Background(), CDCEvent{
		StreamName:     "orders",
		EntityType:     "order",
		EntityID:       int64(42),
		Operation:      "update",
		IdempotencyKey: "cdc-modern-1",
		Payload:        map[string]any{"status": "paid", "amount": 42},
	})
	if err != nil || cdcResult.Events[0].EventID != "101" {
		t.Fatalf("modern CDC result = %#v, %v", cdcResult, err)
	}
	auditResult, err := client.Audit.RecordBatch(context.Background(), []AuditRecord{
		{
			ResourceType:   "invoice",
			ResourceID:     "invoice_1",
			Action:         "approve",
			Outcome:        AuditOutcomeSucceeded,
			IdempotencyKey: "audit-modern-1",
		},
		{
			ResourceType:   "invoice",
			ResourceID:     "invoice_2",
			Action:         "approve",
			Outcome:        AuditOutcomeSucceeded,
			IdempotencyKey: "audit-modern-2",
			PayloadJSON:    `{"duplicate":1,"duplicate":2}`,
		},
	})
	if err != nil || len(auditResult.Events) != 2 {
		t.Fatalf("modern audit result = %#v, %v", auditResult, err)
	}

	requestsBeforeValidation := calls
	_, err = client.CDC.Record(context.Background(), CDCEvent{
		StreamName: "orders", EntityType: "order", EntityID: "1", Operation: "insert", IdempotencyKey: "ambiguous",
		Payload: map[string]any{"a": 1}, PayloadJSON: `{}`,
	})
	assertErrorCode(t, err, "MEDALLION_AMBIGUOUS_JSON_PAYLOAD")
	_, err = client.Audit.Record(context.Background(), AuditRecord{
		ResourceType: "invoice", ResourceID: "1", Action: "read", Outcome: AuditOutcomeSucceeded, IdempotencyKey: "invalid-json", PayloadJSON: `{`,
	})
	assertErrorCode(t, err, "MEDALLION_INVALID_JSON_BODY")
	if calls != requestsBeforeValidation {
		t.Fatalf("invalid modern inputs sent HTTP requests: before=%d after=%d", requestsBeforeValidation, calls)
	}
}

func TestLowLevelPublishRejectsServerDerivedFields(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer server.Close()
	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "key",
		WorkspaceID:        testWorkspaceID,
		DefaultConnectorID: "connector_123",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, _, err = client.Connect.PublishCdcEvents(context.Background(), &connectv1.PublishCdcEventsRequest{Events: []*connectv1.CdcEvent{{
		WorkspaceId: testWorkspaceID, StreamName: "orders", EntityType: "order", EntityId: "1", Operation: connectv1.CdcOperation_CDC_OPERATION_INSERT, IdempotencyKey: "cdc-server-field", PayloadJson: "{}",
	}}})
	assertErrorCode(t, err, "MEDALLION_SERVER_DERIVED_FIELD")
	_, _, err = client.Connect.PublishAuditEvents(context.Background(), &connectv1.PublishAuditEventsRequest{Events: []*connectv1.AuditEvent{{
		WorkspaceId: testWorkspaceID, ResourceType: "invoice", ResourceId: "1", Action: "read", Outcome: connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED, Origin: connectv1.AuditEventOrigin_AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER, IdempotencyKey: "audit-server-field", PayloadJson: "{}",
	}}})
	assertErrorCode(t, err, "MEDALLION_SERVER_DERIVED_FIELD")
	if calls != 0 {
		t.Fatalf("server-derived inputs sent %d HTTP requests", calls)
	}
}

func TestPublishRejectsDuplicateEventKeysAcrossEntireBatch(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer server.Close()
	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "key",
		WorkspaceID:        testWorkspaceID,
		DefaultConnectorID: "connector_123",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = client.Connect.PublishCdcEvents(context.Background(), &connectv1.PublishCdcEventsRequest{Events: []*connectv1.CdcEvent{
		{StreamName: "orders", EntityType: "order", EntityId: "1", Operation: connectv1.CdcOperation_CDC_OPERATION_INSERT, IdempotencyKey: "duplicate-key", PayloadJson: "{}"},
		{StreamName: "customers", EntityType: "customer", EntityId: "2", Operation: connectv1.CdcOperation_CDC_OPERATION_UPDATE, IdempotencyKey: "duplicate-key", PayloadJson: "{}"},
	}})
	assertErrorCode(t, err, "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY")

	_, _, err = client.Connect.PublishAuditEvents(context.Background(), &connectv1.PublishAuditEventsRequest{Events: []*connectv1.AuditEvent{
		{ResourceType: "invoice", ResourceId: "1", Action: "read", Outcome: connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED, IdempotencyKey: "duplicate-key", PayloadJson: "{}"},
		{ResourceType: "invoice", ResourceId: "2", Action: "read", Outcome: connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED, IdempotencyKey: "duplicate-key", PayloadJson: "{}"},
	}})
	assertErrorCode(t, err, "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY")
	if calls != 0 {
		t.Fatalf("duplicate batches sent %d HTTP requests", calls)
	}
}

func TestBatchConnectorScopeBelongsOnlyToTheRequest(t *testing.T) {
	for name, eventType := range map[string]reflect.Type{
		"CDCEvent":    reflect.TypeOf(CDCEvent{}),
		"AuditRecord": reflect.TypeOf(AuditRecord{}),
	} {
		if _, nestedConnector := eventType.FieldByName("ConnectorID"); nestedConnector {
			t.Fatalf("%s exposes nested connector scope", name)
		}
	}

	wantConnector := map[string]string{
		publishCdcEventsPath:   "connector_cdc_request",
		publishAuditEventsPath: "connector_audit_request",
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls++
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode publish batch: %v", err)
		}
		if got := body["connectorId"]; got != wantConnector[request.URL.Path] {
			t.Fatalf("request connector = %#v, want %q", got, wantConnector[request.URL.Path])
		}
		events, ok := body["events"].([]any)
		if !ok || len(events) != 1 {
			t.Fatalf("publish events = %#v", body["events"])
		}
		event := events[0].(map[string]any)
		if _, nestedConnector := event["connectorId"]; nestedConnector {
			t.Fatalf("nested event owns connector scope: %#v", event)
		}
		key := event["idempotencyKey"].(string)
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accepted_count": 1,
			"events": []any{map[string]any{
				"idempotency_key": key,
				"event_id":        "1",
			}},
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "key", WorkspaceID: testWorkspaceID})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.CDC.PublishBatch(context.Background(), CDCBatchInput{
		ConnectorID: "connector_cdc_request",
		Events: []CDCEvent{{
			StreamName: "orders", EntityType: "order", EntityID: "1", Operation: "insert", IdempotencyKey: "cdc-request-scope",
		}},
	})
	if err != nil {
		t.Fatalf("publish request-scoped CDC batch: %v", err)
	}
	_, err = client.Audit.PublishBatch(context.Background(), AuditBatchInput{
		ConnectorID: "connector_audit_request",
		Events: []AuditRecord{{
			ResourceType: "order", ResourceID: "1", Action: "read", Outcome: AuditOutcomeSucceeded, IdempotencyKey: "audit-request-scope",
		}},
	})
	if err != nil {
		t.Fatalf("publish request-scoped audit batch: %v", err)
	}
	clientWithDefault, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "key",
		WorkspaceID:        testWorkspaceID,
		DefaultConnectorID: "connector_default",
	})
	if err != nil {
		t.Fatalf("new client with default connector: %v", err)
	}
	_, err = clientWithDefault.CDC.PublishBatch(context.Background(), CDCBatchInput{
		ConnectorID: "connector_cdc_request",
		Events: []CDCEvent{{
			StreamName: "orders", EntityType: "order", EntityID: "override", Operation: "insert", IdempotencyKey: "cdc-request-override",
		}},
	})
	if err != nil {
		t.Fatalf("explicit request connector did not override default: %v", err)
	}

	_, err = client.CDC.RecordBatch(context.Background(), []CDCEvent{{
		StreamName: "orders", EntityType: "order", EntityID: "2", Operation: "insert", IdempotencyKey: "cdc-missing-scope",
	}})
	assertErrorCode(t, err, "MEDALLION_MISSING_CONNECTOR_ID")
	_, err = client.Audit.RecordBatch(context.Background(), []AuditRecord{{
		ResourceType: "order", ResourceID: "2", Action: "read", Outcome: AuditOutcomeSucceeded, IdempotencyKey: "audit-missing-scope",
	}})
	assertErrorCode(t, err, "MEDALLION_MISSING_CONNECTOR_ID")
	if calls != 3 {
		t.Fatalf("request-level connector test made %d HTTP calls, want 3", calls)
	}
}

func TestRecordBatchAndLosslessLists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_ingestion")
		switch request.URL.Path {
		case publishAuditEventsPath:
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatalf("decode batch: %v", err)
			}
			if body["connectorId"] != "conn_default" || len(body["events"].([]any)) != 2 {
				t.Fatalf("batch body = %#v", body)
			}
			_, _ = w.Write([]byte(`{
				"accepted_count":1,
				"duplicate_count":1,
				"events":[
					{"idempotency_key":"audit_1","event_id":"9223372036854775807"},
					{"idempotency_key":"audit_2","event_id":"42","duplicate":true}
				]
			}`))
		case listCdcEventsPath:
			_, _ = w.Write([]byte(`{
				"events":[{
					"id":"9223372036854775807",
					"workspace_id":"ws_01jz9q5g6rsf7r5ar4rah1b2c3",
					"connector_id":"conn_default",
					"stream_name":"orders",
					"entity_type":"order",
					"entity_id":"order_1",
					"idempotency_key":"cdc_list_1",
					"operation":"CDC_OPERATION_UPDATE",
					"payload_json":"{\"integer\":9223372036854775807,\"decimal\":1234567890.123456789}",
					"occurred_at":"2026-08-01T01:02:03.123456789Z",
					"observed_at":"2026-08-01T01:02:04.123456789Z"
				}]
			}`))
		default:
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		BaseURL:            server.URL,
		APIKey:             "key",
		WorkspaceID:        testWorkspaceID,
		DefaultConnectorID: "conn_default",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	batch, err := client.Audit.RecordBatch(context.Background(), []AuditRecord{
		{Actor: ActorRef{Type: "user", ID: int64(1)}, Action: "read", Outcome: AuditOutcomeSucceeded, Resource: ResourceRef{Type: "order", ID: uint64(1)}, IdempotencyKey: "audit_1"},
		{Actor: ActorRef{Type: "user", ID: int64(2)}, Action: "update", Outcome: AuditOutcomeFailed, Resource: ResourceRef{Type: "order", ID: uint64(2)}, IdempotencyKey: "audit_2"},
	})
	if err != nil {
		t.Fatalf("record batch: %v", err)
	}
	if batch.Result != "mixed" || batch.Duplicate || batch.AcceptedCount != 1 || batch.DuplicateCount != 1 || batch.Events[0].EventID != "9223372036854775807" {
		t.Fatalf("batch response = %#v", batch)
	}

	page, err := client.CDC.List(context.Background(), CDCListQuery{})
	if err != nil {
		t.Fatalf("list CDC: %v", err)
	}
	if len(page.Events) != 1 || page.Events[0].ID != "9223372036854775807" || page.Events[0].OccurredAt != "2026-08-01T01:02:03.123456789Z" {
		t.Fatalf("CDC page = %#v", page)
	}
	payload, ok := page.Events[0].Payload.(map[string]any)
	if !ok {
		t.Fatalf("payload = %#v", page.Events[0].Payload)
	}
	if payload["integer"].(json.Number).String() != "9223372036854775807" || payload["decimal"].(json.Number).String() != "1234567890.123456789" {
		t.Fatalf("lossy payload = %#v", payload)
	}
}

func TestListProjectionEnforcesRequiredContractFieldsAndByteLimits(t *testing.T) {
	newCDCEvent := func() *connectv1.CdcEvent {
		return &connectv1.CdcEvent{
			Id:             1,
			WorkspaceId:    testWorkspaceID,
			StreamName:     "orders",
			EntityType:     "order",
			EntityId:       "1",
			Operation:      connectv1.CdcOperation_CDC_OPERATION_INSERT,
			IdempotencyKey: "cdc-list-1",
			PayloadJson:    "{}",
		}
	}
	cdcCases := []struct {
		name   string
		field  string
		mutate func(*connectv1.CdcEvent)
	}{
		{name: "missing stream name", field: ".streamName", mutate: func(event *connectv1.CdcEvent) { event.StreamName = "" }},
		{name: "blank stream name", field: ".streamName", mutate: func(event *connectv1.CdcEvent) { event.StreamName = " \t " }},
		{name: "oversized stream name", field: ".streamName", mutate: func(event *connectv1.CdcEvent) { event.StreamName = strings.Repeat("\u00e9", 129) }},
		{name: "missing entity type", field: ".entityType", mutate: func(event *connectv1.CdcEvent) { event.EntityType = "" }},
		{name: "oversized entity type", field: ".entityType", mutate: func(event *connectv1.CdcEvent) { event.EntityType = strings.Repeat("t", 257) }},
		{name: "missing entity ID", field: ".entityId", mutate: func(event *connectv1.CdcEvent) { event.EntityId = "" }},
		{name: "oversized entity ID", field: ".entityId", mutate: func(event *connectv1.CdcEvent) { event.EntityId = strings.Repeat("i", 1025) }},
		{name: "missing idempotency key", field: ".idempotencyKey", mutate: func(event *connectv1.CdcEvent) { event.IdempotencyKey = "" }},
		{name: "oversized idempotency key", field: ".idempotencyKey", mutate: func(event *connectv1.CdcEvent) { event.IdempotencyKey = strings.Repeat("k", 513) }},
		{name: "missing workspace ID", field: ".workspaceId", mutate: func(event *connectv1.CdcEvent) { event.WorkspaceId = "" }},
		{name: "invalid workspace ID", field: ".workspaceId", mutate: func(event *connectv1.CdcEvent) { event.WorkspaceId = "workspace_invalid" }},
		{name: "oversized connector ID", field: ".connectorId", mutate: func(event *connectv1.CdcEvent) { event.ConnectorId = strings.Repeat("c", 129) }},
		{name: "oversized source event ID", field: ".sourceEventId", mutate: func(event *connectv1.CdcEvent) { event.SourceEventId = strings.Repeat("s", 1025) }},
		{name: "oversized actor principal", field: ".actorPrincipal", mutate: func(event *connectv1.CdcEvent) { event.ActorPrincipal = strings.Repeat("a", 513) }},
		{name: "oversized description", field: ".description", mutate: func(event *connectv1.CdcEvent) { event.Description = strings.Repeat("d", 4097) }},
		{name: "oversized source system", field: ".sourceSystem", mutate: func(event *connectv1.CdcEvent) { event.SourceSystem = strings.Repeat("s", 257) }},
		{name: "oversized ingester principal", field: ".ingestedByPrincipal", mutate: func(event *connectv1.CdcEvent) { event.IngestedByPrincipal = strings.Repeat("i", 513) }},
	}
	for _, test := range cdcCases {
		t.Run("CDC "+test.name, func(t *testing.T) {
			event := newCDCEvent()
			test.mutate(event)
			_, err := cdcListResponse(&connectv1.ListCdcEventsResponse{Events: []*connectv1.CdcEvent{event}}, "req_cdc_contract", testWorkspaceID)
			var sdkErr *Error
			if !errors.As(err, &sdkErr) || sdkErr.Code != "MEDALLION_INVALID_LIST_RESPONSE" || sdkErr.RequestID != "req_cdc_contract" || !strings.Contains(sdkErr.Message, test.field) {
				t.Fatalf("CDC projection error = %#v", err)
			}
		})
	}

	newAuditEvent := func() *connectv1.AuditEvent {
		return &connectv1.AuditEvent{
			Id:             1,
			WorkspaceId:    testWorkspaceID,
			ResourceType:   "order",
			ResourceId:     "1",
			Action:         "read",
			IdempotencyKey: "audit-list-1",
			PayloadJson:    "{}",
			Origin:         connectv1.AuditEventOrigin_AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER,
			Outcome:        connectv1.AuditEventOutcome_AUDIT_EVENT_OUTCOME_SUCCEEDED,
		}
	}
	auditCases := []struct {
		name   string
		field  string
		mutate func(*connectv1.AuditEvent)
	}{
		{name: "missing resource type", field: ".resourceType", mutate: func(event *connectv1.AuditEvent) { event.ResourceType = "" }},
		{name: "blank resource type", field: ".resourceType", mutate: func(event *connectv1.AuditEvent) { event.ResourceType = " \t " }},
		{name: "oversized resource type", field: ".resourceType", mutate: func(event *connectv1.AuditEvent) { event.ResourceType = strings.Repeat("\u00e9", 129) }},
		{name: "missing resource ID", field: ".resourceId", mutate: func(event *connectv1.AuditEvent) { event.ResourceId = "" }},
		{name: "oversized resource ID", field: ".resourceId", mutate: func(event *connectv1.AuditEvent) { event.ResourceId = strings.Repeat("i", 1025) }},
		{name: "missing action", field: ".action", mutate: func(event *connectv1.AuditEvent) { event.Action = "" }},
		{name: "oversized action", field: ".action", mutate: func(event *connectv1.AuditEvent) { event.Action = strings.Repeat("a", 257) }},
		{name: "missing idempotency key", field: ".idempotencyKey", mutate: func(event *connectv1.AuditEvent) { event.IdempotencyKey = "" }},
		{name: "oversized idempotency key", field: ".idempotencyKey", mutate: func(event *connectv1.AuditEvent) { event.IdempotencyKey = strings.Repeat("k", 513) }},
		{name: "missing workspace ID", field: ".workspaceId", mutate: func(event *connectv1.AuditEvent) { event.WorkspaceId = "" }},
		{name: "invalid workspace ID", field: ".workspaceId", mutate: func(event *connectv1.AuditEvent) { event.WorkspaceId = "workspace_invalid" }},
		{name: "oversized connector ID", field: ".connectorId", mutate: func(event *connectv1.AuditEvent) { event.ConnectorId = strings.Repeat("c", 129) }},
		{name: "oversized source event ID", field: ".sourceEventId", mutate: func(event *connectv1.AuditEvent) { event.SourceEventId = strings.Repeat("s", 1025) }},
		{name: "oversized actor principal", field: ".actorPrincipal", mutate: func(event *connectv1.AuditEvent) { event.ActorPrincipal = strings.Repeat("a", 513) }},
		{name: "oversized description", field: ".description", mutate: func(event *connectv1.AuditEvent) { event.Description = strings.Repeat("d", 4097) }},
		{name: "oversized source system", field: ".sourceSystem", mutate: func(event *connectv1.AuditEvent) { event.SourceSystem = strings.Repeat("s", 257) }},
		{name: "oversized ingester principal", field: ".ingestedByPrincipal", mutate: func(event *connectv1.AuditEvent) { event.IngestedByPrincipal = strings.Repeat("i", 513) }},
	}
	for _, test := range auditCases {
		t.Run("audit "+test.name, func(t *testing.T) {
			event := newAuditEvent()
			test.mutate(event)
			_, err := auditListResponse(&connectv1.ListAuditEventsResponse{Events: []*connectv1.AuditEvent{event}}, "req_audit_contract", testWorkspaceID, "")
			var sdkErr *Error
			if !errors.As(err, &sdkErr) || sdkErr.Code != "MEDALLION_INVALID_LIST_RESPONSE" || sdkErr.RequestID != "req_audit_contract" || !strings.Contains(sdkErr.Message, test.field) {
				t.Fatalf("audit projection error = %#v", err)
			}
		})
	}

	cdcBoundary := newCDCEvent()
	cdcBoundary.StreamName = strings.Repeat("\u00e9", 128)
	cdcBoundary.EntityType = strings.Repeat("\u00e9", 128)
	cdcBoundary.EntityId = strings.Repeat("\u00e9", 512)
	cdcBoundary.IdempotencyKey = strings.Repeat("\u00e9", 256)
	if _, err := cdcListResponse(&connectv1.ListCdcEventsResponse{Events: []*connectv1.CdcEvent{cdcBoundary}}, "req_cdc_boundary", testWorkspaceID); err != nil {
		t.Fatalf("CDC projection rejected exact byte boundaries: %v", err)
	}
	auditBoundary := newAuditEvent()
	auditBoundary.ResourceType = strings.Repeat("\u00e9", 128)
	auditBoundary.ResourceId = strings.Repeat("\u00e9", 512)
	auditBoundary.Action = strings.Repeat("\u00e9", 128)
	auditBoundary.IdempotencyKey = strings.Repeat("\u00e9", 256)
	if _, err := auditListResponse(&connectv1.ListAuditEventsResponse{Events: []*connectv1.AuditEvent{auditBoundary}}, "req_audit_boundary", testWorkspaceID, ""); err != nil {
		t.Fatalf("audit projection rejected exact byte boundaries: %v", err)
	}
}

func TestListRejectsInvalidPayloadAndIteratorRejectsRepeatedCursor(t *testing.T) {
	t.Run("invalid payload", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("content-type", "application/json")
			w.Header().Set("x-request-id", "req_invalid_payload")
			_, _ = w.Write([]byte(`{"events":[{"id":"1","workspace_id":"ws_01jz9q5g6rsf7r5ar4rah1b2c3","stream_name":"orders","entity_type":"order","entity_id":"1","operation":"CDC_OPERATION_INSERT","idempotency_key":"cdc_list_1","payload_json":"{"}]}`))
		}))
		defer server.Close()
		client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "key", WorkspaceID: testWorkspaceID})
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		_, err = client.CDC.List(context.Background(), CDCListQuery{})
		var sdkErr *Error
		if !errors.As(err, &sdkErr) || sdkErr.Code != "MEDALLION_INVALID_LIST_RESPONSE" || sdkErr.RequestID != "req_invalid_payload" {
			t.Fatalf("error = %#v", err)
		}
	})

	t.Run("repeated cursor", func(t *testing.T) {
		calls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			calls++
			w.Header().Set("content-type", "application/json")
			if calls == 1 {
				_, _ = w.Write([]byte(`{
					"events":[
						{"id":"1","workspace_id":"ws_01jz9q5g6rsf7r5ar4rah1b2c3","stream_name":"orders","entity_type":"order","entity_id":"1","operation":"CDC_OPERATION_INSERT","idempotency_key":"cdc_list_1","payload_json":"{}"},
						{"id":"2","workspace_id":"ws_01jz9q5g6rsf7r5ar4rah1b2c3","stream_name":"orders","entity_type":"order","entity_id":"2","operation":"CDC_OPERATION_UPDATE","idempotency_key":"cdc_list_2","payload_json":"{}"}
					],
					"next_page_cursor":"cursor_same"
				}`))
				return
			}
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatalf("decode cursor request: %v", err)
			}
			if body["pageCursor"] != "cursor_same" {
				t.Fatalf("page cursor = %#v", body["pageCursor"])
			}
			_, _ = w.Write([]byte(`{"next_page_cursor":"cursor_same"}`))
		}))
		defer server.Close()
		client, err := NewClient(ClientConfig{BaseURL: server.URL, APIKey: "key", WorkspaceID: testWorkspaceID})
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		iterator := client.CDC.Iterate(context.Background(), CDCListQuery{})
		var ids []string
		for iterator.Next() {
			ids = append(ids, iterator.Event().ID)
		}
		if strings.Join(ids, ",") != "1,2" || calls != 2 {
			t.Fatalf("IDs = %#v; calls = %d", ids, calls)
		}
		assertErrorCode(t, iterator.Err(), "MEDALLION_REPEATED_CURSOR")
		if strings.Contains(iterator.Err().Error(), "cursor_same") {
			t.Fatalf("confidential cursor leaked through iterator error: %v", iterator.Err())
		}
	})
}

func TestIteratorsEnforcePageBudgetBeforeAnotherRequest(t *testing.T) {
	cdc := &CDCIterator{pages: maxIteratorPages, index: -1}
	if cdc.Next() {
		t.Fatal("CDC iterator advanced beyond its page budget")
	}
	assertErrorCode(t, cdc.Err(), "MEDALLION_PAGINATION_LIMIT")

	audit := &AuditIterator{pages: maxIteratorPages, index: -1}
	if audit.Next() {
		t.Fatal("audit iterator advanced beyond its page budget")
	}
	assertErrorCode(t, audit.Err(), "MEDALLION_PAGINATION_LIMIT")
}

func TestAnalyticalTimestampRange(t *testing.T) {
	valid := []string{
		"1900-01-01T00:00:00Z",
		"2262-04-11T23:47:16.854775807Z",
	}
	for _, value := range valid {
		if _, err := timestampFromString(value, "event.occurredAt"); err != nil {
			t.Fatalf("timestamp %q: %v", value, err)
		}
	}
	for _, value := range []string{
		"1899-12-31T23:59:59.999999999Z",
		"2262-04-11T23:47:16.854775808Z",
	} {
		_, err := timestampFromString(value, "event.occurredAt")
		assertErrorCode(t, err, "MEDALLION_TIMESTAMP_OUT_OF_RANGE")
	}
	_, err := timestampFromString("not-a-time", "event.occurredAt")
	assertErrorCode(t, err, "MEDALLION_INVALID_TIMESTAMP")
}

func assertErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var sdkErr *Error
	if !errors.As(err, &sdkErr) || sdkErr.Code != code {
		t.Fatalf("error = %#v, want code %q", err, code)
	}
}
