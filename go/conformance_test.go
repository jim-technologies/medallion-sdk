package medallion

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type externalIngestionFixtureDocument struct {
	Fixtures []externalIngestionFixture `json:"fixtures"`
}

type externalIngestionFixture struct {
	ID       string                    `json:"id"`
	Input    externalIngestionExchange `json:"input"`
	Expected externalIngestionExchange `json:"expected"`
	Protocol struct {
		Method string `json:"method"`
		Path   string `json:"path"`
	} `json:"protocol"`
}

type externalIngestionExchange struct {
	Body       json.RawMessage   `json:"body"`
	Headers    map[string]string `json:"headers"`
	HTTPStatus int               `json:"httpStatus"`
}

func TestOfficialExternalIngestionFixturesExecuteThroughLowLevelClient(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve conformance test path")
	}
	fixturePath := filepath.Join(filepath.Dir(filename), "..", "proto", "external-ingestion-contract", "v1", "conformance", "external-sdk-ingestion.json")
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read official ingestion fixture: %v", err)
	}
	var document externalIngestionFixtureDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode official ingestion fixture: %v", err)
	}
	if len(document.Fixtures) != 2 {
		t.Fatalf("official ingestion fixture count = %d, want 2", len(document.Fixtures))
	}

	for _, fixture := range document.Fixtures {
		fixture := fixture
		t.Run(fixture.ID, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != fixture.Protocol.Path {
					t.Errorf("canonical path = %q, want %q", request.URL.Path, fixture.Protocol.Path)
				}
				if request.Method != http.MethodPost {
					t.Errorf("HTTP method = %q, want POST", request.Method)
				}
				if got, want := request.Header.Get("X-Medallion-Workspace-Id"), fixture.Input.Headers["x-medallion-workspace-id"]; got != want {
					t.Errorf("workspace header = %q, want %q", got, want)
				}
				if got := request.Header.Get("X-Medallion-API-Key"); got != "FIXTURE_NON_SECRET_API_KEY" {
					t.Errorf("API key header = %q", got)
				}
				if got, want := request.Header.Get("Content-Type"), fixture.Input.Headers["content-type"]; got != want {
					t.Errorf("content type = %q, want %q", got, want)
				}
				requestBody, readErr := io.ReadAll(request.Body)
				if readErr != nil {
					t.Errorf("read request body: %v", readErr)
				} else {
					assertCanonicalJSONBody(t, requestBody, fixture.Input.Body)
				}
				writer.Header().Set("content-type", fixture.Expected.Headers["content-type"])
				writer.Header().Set("x-request-id", "req_fixture")
				writer.WriteHeader(fixture.Expected.HTTPStatus)
				_, _ = writer.Write(fixture.Expected.Body)
			}))
			defer server.Close()

			client, err := NewClient(ClientConfig{
				BaseURL:     server.URL,
				APIKey:      "FIXTURE_NON_SECRET_API_KEY",
				WorkspaceID: fixture.Input.Headers["x-medallion-workspace-id"],
			})
			if err != nil {
				t.Fatalf("construct fixture client: %v", err)
			}

			switch fixture.Protocol.Method {
			case "PublishCdcEvents":
				request := &connectv1.PublishCdcEventsRequest{}
				if err := (protojson.UnmarshalOptions{}).Unmarshal(fixture.Input.Body, request); err != nil {
					t.Fatalf("decode CDC fixture request: %v", err)
				}
				want := &connectv1.PublishCdcEventsResponse{}
				if err := (protojson.UnmarshalOptions{}).Unmarshal(fixture.Expected.Body, want); err != nil {
					t.Fatalf("decode CDC fixture receipt: %v", err)
				}
				got, requestID, err := client.Connect.PublishCdcEvents(context.Background(), request)
				if err != nil {
					t.Fatalf("execute CDC fixture: %v", err)
				}
				if requestID != "req_fixture" || !proto.Equal(got, want) {
					t.Fatalf("decoded CDC receipt = %#v, request ID %q; want %#v", got, requestID, want)
				}
			case "PublishAuditEvents":
				request := &connectv1.PublishAuditEventsRequest{}
				if err := (protojson.UnmarshalOptions{}).Unmarshal(fixture.Input.Body, request); err != nil {
					t.Fatalf("decode audit fixture request: %v", err)
				}
				want := &connectv1.PublishAuditEventsResponse{}
				if err := (protojson.UnmarshalOptions{}).Unmarshal(fixture.Expected.Body, want); err != nil {
					t.Fatalf("decode audit fixture receipt: %v", err)
				}
				got, requestID, err := client.Connect.PublishAuditEvents(context.Background(), request)
				if err != nil {
					t.Fatalf("execute audit fixture: %v", err)
				}
				if requestID != "req_fixture" || !proto.Equal(got, want) {
					t.Fatalf("decoded audit receipt = %#v, request ID %q; want %#v", got, requestID, want)
				}
			default:
				t.Fatalf("unsupported official fixture method %q", fixture.Protocol.Method)
			}
		})
	}
}

func assertCanonicalJSONBody(t *testing.T, got, want []byte) {
	t.Helper()
	decode := func(raw []byte) any {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatalf("decode canonical JSON body: %v", err)
		}
		return value
	}
	if gotValue, wantValue := decode(got), decode(want); !jsonValuesEqual(gotValue, wantValue) {
		t.Fatalf("canonical request body = %s, want %s", got, want)
	}
}

func jsonValuesEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
