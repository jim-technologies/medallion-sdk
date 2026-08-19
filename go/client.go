package medallion

import (
	"context"
	"strings"

	connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
	"google.golang.org/protobuf/proto"
)

const connectService = "/medallion.connect.v1.MedallionConnectService"

const (
	publishCdcEventsPath   = connectService + "/PublishCdcEvents"
	listCdcEventsPath      = connectService + "/ListCdcEvents"
	publishAuditEventsPath = connectService + "/PublishAuditEvents"
	listAuditEventsPath    = connectService + "/ListAuditEvents"
)

type Client struct {
	Connect *ConnectClient
	Audit   *AuditClient
	CDC     *CDCClient
}

func NewClient(cfg ClientConfig) (*Client, error) {
	requests, err := newRequestClient(cfg)
	if err != nil {
		return nil, err
	}
	connect := &ConnectClient{requests: requests, defaultConnectorID: strings.TrimSpace(cfg.DefaultConnectorID)}
	return &Client{
		Connect: connect,
		Audit:   &AuditClient{connect: connect, defaultConnectorID: cfg.DefaultConnectorID},
		CDC:     &CDCClient{connect: connect, defaultConnectorID: cfg.DefaultConnectorID},
	}, nil
}

type ConnectClient struct {
	requests           *requestClient
	defaultConnectorID string
}

func (c *ConnectClient) PublishCdcEvents(ctx context.Context, request *connectv1.PublishCdcEventsRequest) (*connectv1.PublishCdcEventsResponse, string, error) {
	request, err := c.preparePublishCdcEventsRequest(request)
	if err != nil {
		return nil, "", err
	}
	response := &connectv1.PublishCdcEventsResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, publishCdcEventsPath, request, response, postOptions{
		retrySafe: true,
	})
	if err != nil {
		return nil, "", err
	}
	if err := validateCdcPublishAcknowledgement(response, cdcEventKeys(request.GetEvents()), envelope.requestID); err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

func (c *ConnectClient) ListCdcEvents(ctx context.Context, request *connectv1.ListCdcEventsRequest) (*connectv1.ListCdcEventsResponse, string, error) {
	request, err := c.prepareListCdcEventsRequest(request)
	if err != nil {
		return nil, "", err
	}
	response := &connectv1.ListCdcEventsResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, listCdcEventsPath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	if err := validateCdcListResponse(response, c.requests.workspaceID, envelope.requestID); err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

func (c *ConnectClient) PublishAuditEvents(ctx context.Context, request *connectv1.PublishAuditEventsRequest) (*connectv1.PublishAuditEventsResponse, string, error) {
	request, err := c.preparePublishAuditEventsRequest(request)
	if err != nil {
		return nil, "", err
	}
	response := &connectv1.PublishAuditEventsResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, publishAuditEventsPath, request, response, postOptions{
		retrySafe: true,
	})
	if err != nil {
		return nil, "", err
	}
	if err := validateAuditPublishAcknowledgement(response, auditEventKeys(request.GetEvents()), envelope.requestID); err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

func (c *ConnectClient) ListAuditEvents(ctx context.Context, request *connectv1.ListAuditEventsRequest) (*connectv1.ListAuditEventsResponse, string, error) {
	request, err := c.prepareListAuditEventsRequest(request)
	if err != nil {
		return nil, "", err
	}
	response := &connectv1.ListAuditEventsResponse{}
	envelope, err := c.requests.postProtoWithOptions(ctx, listAuditEventsPath, request, response, postOptions{retrySafe: true})
	if err != nil {
		return nil, "", err
	}
	if err := validateAuditListResponse(response, c.requests.workspaceID, envelope.requestID); err != nil {
		return nil, "", err
	}
	return response, envelope.requestID, nil
}

type AuditClient struct {
	connect            *ConnectClient
	defaultConnectorID string
}

func (c *AuditClient) Record(ctx context.Context, input AuditRecord) (AuditRecordResponse, error) {
	return c.RecordBatch(ctx, []AuditRecord{input})
}

func (c *AuditClient) Trail(ctx context.Context, input AuditTrailQuery) (AuditTrailResponse, error) {
	return c.list(ctx, input, true)
}

type CDCClient struct {
	connect            *ConnectClient
	defaultConnectorID string
}

func (c *CDCClient) Record(ctx context.Context, input CDCEvent) (EventRecordResponse, error) {
	return c.RecordBatch(ctx, []CDCEvent{input})
}

func cloneMessage[T proto.Message](message T) T {
	if any(message) == nil {
		return message
	}
	return proto.Clone(message).(T)
}
