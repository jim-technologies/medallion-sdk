package medallion_test

import (
	"context"
	"time"

	medallion "github.com/jim-technologies/medallion-sdk/go"
)

// ExampleClient is compiled with the test suite so the documented initial
// ingestion surface cannot silently drift.
func ExampleClient() {
	client, err := medallion.NewClient(medallion.ClientConfig{
		BaseURL:            "https://api.example.com",
		APIKey:             "service-key",
		WorkspaceID:        "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
		DefaultConnectorID: "connector_123",
		Timeout:            20 * time.Second,
	})
	if err != nil {
		return
	}

	ctx := context.Background()
	key, err := medallion.StableIdempotencyKey("orders", "partition-3", "lsn-9A/BC")
	if err != nil {
		return
	}
	_, _ = client.CDC.Record(ctx, medallion.CDCEvent{
		StreamName:     "orders",
		EntityType:     "order",
		EntityID:       int64(42),
		Operation:      "update",
		IdempotencyKey: key,
		Payload:        map[string]any{"status": "paid"},
	})
	_, _ = client.Audit.RecordBatch(ctx, []medallion.AuditRecord{{
		Actor:          medallion.ActorRef{Type: "user", ID: "user_123"},
		Action:         "approve",
		Outcome:        medallion.AuditOutcomeSucceeded,
		ResourceType:   "invoice",
		ResourceID:     "invoice_42",
		IdempotencyKey: "billing-audit:evt-9921",
		PayloadJSON:    `{"approvalPolicy":"four-eyes"}`,
	}})
	_, _ = client.CDC.List(ctx, medallion.CDCListQuery{StreamName: "orders", Limit: 100})
}
