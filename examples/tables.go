// Tables quickstart for the thin Go bindings: declare a table, evolve its
// schema additively, append a batch idempotently, and read it back with one
// ClickHouse SQL statement. Run from a trusted server; never ship the API key
// to a browser.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	medallion "github.com/jim-technologies/medallion-sdk/go"
	ingestv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/ingest/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func main() {
	client, err := medallion.NewClient(medallion.ClientConfig{
		BaseURL:     requiredEnv("MEDALLION_BASE_URL"),
		APIKey:      requiredEnv("MEDALLION_API_KEY"),
		WorkspaceID: requiredEnv("MEDALLION_WORKSPACE_ID"),
		Timeout:     30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	runID := fmt.Sprintf("example_%d", time.Now().UnixMilli())
	occurredAt := time.Now().UTC().Format(time.RFC3339)

	// A table is one declared tabular collection: an ordered schema, a
	// TIMESTAMP time column, and an optional sort key. Re-declaring the same
	// table returns the existing one, so retries are safe.
	columns := []*ingestv1.ColumnSchema{
		{Name: "occurred_at", Type: "TIMESTAMP"},
		{Name: "run_id", Type: "STRING"},
		{Name: "seq", Type: "INT64"},
		{Name: "level", Type: "STRING"},
	}
	created, _, err := client.Ingest.CreateTable(ctx, &ingestv1.CreateTableRequest{
		TableId: "app_events",
		Table: &ingestv1.Table{
			Schema:     &ingestv1.TableSchema{Columns: columns},
			TimeColumn: "occurred_at",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("table", created.GetTable().GetName())

	// Schema evolution is additive only: send the FULL desired schema with the
	// existing columns unchanged, then the new nullable ones.
	evolved := append(columns, &ingestv1.ColumnSchema{Name: "trace_id", Type: "STRING", Nullable: true})
	if _, _, err = client.Ingest.UpdateTable(ctx, &ingestv1.UpdateTableRequest{
		Table: &ingestv1.Table{
			Name:   created.GetTable().GetName(),
			Schema: &ingestv1.TableSchema{Columns: evolved},
		},
	}); err != nil {
		log.Fatal(err)
	}

	// Append JSON rows. The SDK generates a batch key, sends it as the
	// Idempotency-Key header, and stamps it into request_id; pin your own with
	// WithIngestIdempotencyKey to make an exact replay of the same batch safe.
	appendCtx := medallion.WithIngestIdempotencyKey(ctx, "quickstart:"+runID)
	appended, _, err := client.Ingest.AppendRows(appendCtx, &ingestv1.AppendRowsRequest{
		Table: "tables/app_events",
		Rows: []*ingestv1.Row{
			{InsertId: runID + ":1", Json: row(occurredAt, runID, 1, "info")},
			{InsertId: runID + ":2", Json: row(occurredAt, runID, 2, "warn")},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("appended", appended.GetAcceptedRows())
	for _, rowError := range appended.GetRowErrors() {
		fmt.Println("rejected row", rowError.GetIndex(), rowError.GetError().GetMessage())
	}

	// Queries are synchronous first; poll GetQueryResults while the state is
	// RUNNING, then follow next_page_token until it is empty.
	response, _, err := client.Ingest.RunQuery(ctx, &ingestv1.RunQueryRequest{
		Query:     fmt.Sprintf("SELECT run_id, seq, level FROM app_events WHERE run_id = '%s' ORDER BY seq", runID),
		TimeoutMs: 10_000,
	})
	if err != nil {
		log.Fatal(err)
	}
	name, state := response.GetName(), response.GetState()
	rows, nextPageToken := response.GetRows(), response.GetNextPageToken()
	for state == "RUNNING" {
		poll, _, err := client.Ingest.GetQueryResults(ctx, &ingestv1.GetQueryResultsRequest{Name: name})
		if err != nil {
			log.Fatal(err)
		}
		state, rows, nextPageToken = poll.GetState(), poll.GetRows(), poll.GetNextPageToken()
	}
	if state == "FAILED" {
		log.Fatal(response.GetError().GetMessage())
	}
	for {
		for _, value := range rows {
			fmt.Println("row", value.AsMap())
		}
		if nextPageToken == "" {
			break
		}
		page, _, err := client.Ingest.GetQueryResults(ctx, &ingestv1.GetQueryResultsRequest{
			Name:      name,
			PageToken: nextPageToken,
		})
		if err != nil {
			log.Fatal(err)
		}
		rows, nextPageToken = page.GetRows(), page.GetNextPageToken()
	}
}

func row(occurredAt string, runID string, seq int, level string) *structpb.Struct {
	values, err := structpb.NewStruct(map[string]any{
		"occurred_at": occurredAt,
		"run_id":      runID,
		"seq":         seq,
		"level":       level,
	})
	if err != nil {
		log.Fatal(err)
	}
	return values
}

func requiredEnv(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}
