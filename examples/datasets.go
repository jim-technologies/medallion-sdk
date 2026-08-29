// Datasets quickstart for the thin Go bindings: create a dataset, append a
// batch idempotently, and read it back with one ClickHouse SQL statement.
// Run from a trusted server; never ship the API key to a browser.
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

	created, _, err := client.Ingest.CreateDataset(ctx, &ingestv1.CreateDatasetRequest{
		DatasetId:   "app_events",
		Description: "Application events appended by the quickstart",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("dataset", created.GetDataset().GetDatasetId())

	// Append JSON rows. The SDK sends a generated Stripe-style
	// Idempotency-Key header; pin one with WithIngestIdempotencyKey to make
	// an exact replay of the same batch safe.
	appendCtx := medallion.WithIngestIdempotencyKey(ctx, "quickstart:"+runID)
	appended, _, err := client.Ingest.Append(appendCtx, &ingestv1.AppendRequest{
		DatasetId: "app_events",
		Rows: &ingestv1.AppendRequest_JsonRows{JsonRows: &ingestv1.JsonRows{Rows: []*ingestv1.Row{
			{InsertId: runID + ":1", Json: fmt.Sprintf(`{"run_id":%q,"seq":1,"level":"info"}`, runID)},
			{InsertId: runID + ":2", Json: fmt.Sprintf(`{"run_id":%q,"seq":2,"level":"warn"}`, runID)},
		}}},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("appended", appended.GetAcceptedRows(), "duplicate:", appended.GetDuplicate())
	for _, rowError := range appended.GetInsertErrors() {
		fmt.Println("rejected row", rowError.GetIndex(), rowError.GetReason())
	}

	// Queries are synchronous first; poll GetQueryResults while completed is
	// false, then follow next_page_token until it is empty.
	response, _, err := client.Ingest.Query(ctx, &ingestv1.QueryRequest{
		Query:     fmt.Sprintf("SELECT run_id, seq, level FROM app_events WHERE run_id = '%s' ORDER BY seq", runID),
		TimeoutMs: 10_000,
	})
	if err != nil {
		log.Fatal(err)
	}
	results := response.GetResults()
	for !results.GetCompleted() {
		poll, _, err := client.Ingest.GetQueryResults(ctx, &ingestv1.GetQueryResultsRequest{
			QueryId:   results.GetQueryId(),
			TimeoutMs: 10_000,
		})
		if err != nil {
			log.Fatal(err)
		}
		results = poll.GetResults()
	}
	for {
		for _, row := range results.GetRowsJson() {
			fmt.Println("row", row)
		}
		if results.GetNextPageToken() == "" {
			break
		}
		page, _, err := client.Ingest.GetQueryResults(ctx, &ingestv1.GetQueryResultsRequest{
			QueryId:   results.GetQueryId(),
			PageToken: results.GetNextPageToken(),
		})
		if err != nil {
			log.Fatal(err)
		}
		results = page.GetResults()
	}
}

func requiredEnv(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}
