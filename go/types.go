package medallion

import (
	"net/http"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type IDInput any

type ClientConfig struct {
	BaseURL     string
	APIKey      string
	AccessToken string
	WorkspaceID string
	Timeout     time.Duration
	Retry       RetryConfig
	HTTPClient  *http.Client
	Tracing     TracingConfig
}

// RetryConfig enables a small, bounded retry budget for requests that are
// intrinsically read-only or carry complete batch idempotency keys.
// The zero value disables retries.
type RetryConfig struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// JitterRatio is the proportional random spread applied to client
	// exponential backoff. Zero selects the safe default of 0.2 when retries
	// are enabled.
	JitterRatio float64
}

type TracingConfig struct {
	Enabled    bool
	Tracer     trace.Tracer
	TracerName string
	SpanPrefix string
}
