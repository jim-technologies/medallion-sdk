package medallion

import "google.golang.org/protobuf/proto"

type Client struct {
	// Ingest is the thin client for the medallion.ingest.v1 tabular surface.
	Ingest *IngestClient
}

func NewClient(cfg ClientConfig) (*Client, error) {
	requests, err := newRequestClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{Ingest: &IngestClient{requests: requests}}, nil
}

func cloneMessage[T proto.Message](message T) T {
	if any(message) == nil {
		return message
	}
	return proto.Clone(message).(T)
}
