package medallion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type requestClient struct {
	baseURL     string
	accessToken string
	apiKey      string
	workspaceID string
	timeout     time.Duration
	retry       RetryConfig
	http        *http.Client
	tracing     TracingConfig
	randomFloat func() float64
	now         func() time.Time
	responseMax int64
}

type protoEnvelope struct {
	requestID string
}

type postOptions struct {
	retrySafe bool
}

const (
	defaultRequestTimeout = 30 * time.Second
	maxRetryAttempts      = 5
	maxRetryBackoff       = 5 * time.Second
	maxResponseBytes      = 64 << 20
)

var (
	errResponseTooLarge    = errors.New("Medallion response exceeded the safety limit")
	errResponseReadFailure = errors.New("Medallion response body could not be read")
	errInvalidJSONResponse = errors.New("Medallion response was not valid protobuf JSON")
	errTransportFailure    = errors.New("Medallion transport request failed")
)

func newRequestClient(cfg ClientConfig) (*requestClient, error) {
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		return nil, invalidOptions("base URL is required")
	}
	parsedBaseURL, err := url.Parse(baseURL)
	if err != nil ||
		!baseURLSchemeAllowed(parsedBaseURL) ||
		parsedBaseURL.Host == "" ||
		parsedBaseURL.Hostname() == "" ||
		parsedBaseURL.User != nil ||
		(parsedBaseURL.EscapedPath() != "" && parsedBaseURL.EscapedPath() != "/") ||
		parsedBaseURL.RawQuery != "" ||
		parsedBaseURL.ForceQuery ||
		parsedBaseURL.Fragment != "" ||
		strings.Contains(baseURL, "#") {
		return nil, invalidOptions("base URL must be an HTTPS origin (or loopback HTTP) without credentials, path, query, or fragment")
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	accessToken := strings.TrimSpace(cfg.AccessToken)
	apiKey := strings.TrimSpace(cfg.APIKey)
	if accessToken == "" && apiKey == "" {
		return nil, invalidOptions("API key or access token is required")
	}
	if accessToken != "" && apiKey != "" {
		return nil, invalidOptions("configure exactly one of API key or access token")
	}
	if err := validateConfiguredHeaderValue(firstNonBlank(accessToken, apiKey), "credential", 0); err != nil {
		return nil, err
	}
	workspaceID := cfg.WorkspaceID
	if err := validateWorkspaceID(workspaceID); err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout < 0 {
		return nil, invalidOptions("timeout must not be negative")
	}
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	retry, err := normalizeRetryConfig(cfg.Retry)
	if err != nil {
		return nil, err
	}
	httpClient := redirectRejectingHTTPClient(cfg.HTTPClient, timeout)
	return &requestClient{
		baseURL:     baseURL,
		accessToken: accessToken,
		apiKey:      apiKey,
		workspaceID: workspaceID,
		timeout:     timeout,
		retry:       retry,
		http:        httpClient,
		tracing:     cfg.Tracing,
		randomFloat: rand.Float64,
		now:         time.Now,
		responseMax: maxResponseBytes,
	}, nil
}

func baseURLSchemeAllowed(parsed *url.URL) bool {
	if strings.EqualFold(parsed.Scheme, "https") {
		return true
	}
	if !strings.EqualFold(parsed.Scheme, "http") {
		return false
	}
	host := parsed.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		return ipv4[0] == 127
	}
	return ip.Equal(net.IPv6loopback)
}

func redirectRejectingHTTPClient(configured *http.Client, timeout time.Duration) *http.Client {
	if configured == nil {
		return &http.Client{
			Timeout:       timeout,
			CheckRedirect: rejectRedirect,
		}
	}
	// Do not mutate a caller-owned client. In particular, an application may
	// use the same client for unrelated traffic with a different redirect
	// policy. Restricting ClientConfig to *http.Client ensures this policy is
	// always applied while still allowing custom RoundTrippers.
	clone := *configured
	clone.CheckRedirect = rejectRedirect
	return &clone
}

func rejectRedirect(*http.Request, []*http.Request) error {
	// Returning ErrUseLastResponse exposes the 3xx response to the normal safe
	// error decoder without issuing a second credential-bearing request.
	return http.ErrUseLastResponse
}

func (c *requestClient) postProtoWithOptions(ctx context.Context, path string, req proto.Message, resp proto.Message, options postOptions) (protoEnvelope, error) {
	workspaceID := c.workspaceID
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	ctx, span := c.tracing.startRequestSpan(ctx, http.MethodPost, path)
	if span != nil {
		defer span.End()
	}

	raw, err := protojson.MarshalOptions{
		EmitUnpopulated: false,
	}.Marshal(req)
	if err != nil {
		recordRequestSpanError(span, err)
		return protoEnvelope{}, &Error{Code: "MEDALLION_INVALID_JSON_BODY", Message: "request body must be protobuf JSON serializable"}
	}
	redactions := c.redactionValues(raw)

	requestURL := c.baseURL + ensureLeadingSlash(path)
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(raw))
		if err != nil {
			recordRequestSpanError(span, err)
			return protoEnvelope{}, &Error{Code: "MEDALLION_INVALID_OPTIONS", Message: "Medallion base URL is invalid", Cause: err}
		}
		c.setRequestHeaders(httpReq, ctx, workspaceID)

		httpResp, requestErr := c.http.Do(httpReq)
		if requestErr != nil {
			classified := classifyTransportError(requestErr)
			if options.retrySafe && attempt < c.retry.MaxAttempts && retryableTransportError(requestErr, ctx) {
				delay := c.retryDelay(attempt, "")
				if !canWaitForRetry(ctx, delay) {
					recordRequestSpanError(span, requestErr)
					return protoEnvelope{}, classified
				}
				if waitErr := waitForRetry(ctx, delay); waitErr == nil {
					continue
				} else {
					recordRequestSpanError(span, waitErr)
					return protoEnvelope{}, classifyTransportError(waitErr)
				}
			}
			recordRequestSpanError(span, requestErr)
			return protoEnvelope{}, classified
		}

		requestID := sanitizeText(redactText(httpResp.Header.Get("x-request-id"), redactions), 256)
		responseLimit := c.responseMax
		if responseLimit <= 0 {
			responseLimit = maxResponseBytes
		}
		responseRaw, readErr := readBoundedResponse(httpResp.Body, httpResp.ContentLength, responseLimit)
		closeErr := httpResp.Body.Close()
		if readErr == nil {
			readErr = closeErr
		}
		setRequestSpanResponse(span, httpResp.StatusCode, requestID)
		if readErr != nil {
			if errors.Is(readErr, errResponseTooLarge) {
				error := &Error{
					Code:      "MEDALLION_RESPONSE_TOO_LARGE",
					RequestID: requestID,
					Message:   fmt.Sprintf("Medallion response exceeded the %d-byte safety limit", responseLimit),
					Cause:     readErr,
				}
				recordRequestSpanError(span, error)
				return protoEnvelope{}, error
			}
			if options.retrySafe && attempt < c.retry.MaxAttempts && retryableResponseReadFailure(httpResp.StatusCode) {
				delay := c.retryDelay(attempt, httpResp.Header.Get("Retry-After"))
				if !canWaitForRetry(ctx, delay) {
					recordRequestSpanError(span, readErr)
					return protoEnvelope{}, classifyResponseReadError(readErr, ctx, requestID)
				}
				if waitErr := waitForRetry(ctx, delay); waitErr == nil {
					continue
				} else {
					recordRequestSpanError(span, waitErr)
					return protoEnvelope{}, classifyTransportError(waitErr)
				}
			}
			recordRequestSpanError(span, readErr)
			return protoEnvelope{}, classifyResponseReadError(readErr, ctx, requestID)
		}
		if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
			responseBody := readJSONBody(bytes.NewReader(responseRaw))
			details, errorInfo := connectErrorDetails(responseBody)
			details = redactConnectDetails(details, redactions)
			errorInfo = redactErrorInfo(errorInfo, redactions)
			connectCode, _ := responseBody["code"].(string)
			apiErr := &APIError{
				Status:    httpResp.StatusCode,
				Code:      redactText(sanitizeConnectCode(connectCode), redactions),
				RequestID: requestID,
				Details:   details,
				ErrorInfo: errorInfo,
				Message:   apiErrorMessage(httpResp.StatusCode, requestID),
			}
			if apiErr.Retryable(options.retrySafe) && attempt < c.retry.MaxAttempts {
				delay := c.retryDelay(attempt, httpResp.Header.Get("Retry-After"))
				if !canWaitForRetry(ctx, delay) {
					return protoEnvelope{}, apiErr
				}
				if waitErr := waitForRetry(ctx, delay); waitErr == nil {
					continue
				} else {
					recordRequestSpanError(span, waitErr)
					return protoEnvelope{}, classifyTransportError(waitErr)
				}
			}
			return protoEnvelope{}, apiErr
		}
		if resp != nil && len(responseRaw) == 0 {
			error := &Error{
				Code:      "MEDALLION_INVALID_JSON_RESPONSE",
				RequestID: requestID,
				Message:   "Medallion response body was empty",
			}
			recordRequestSpanError(span, error)
			return protoEnvelope{}, error
		}
		if len(responseRaw) > 0 && resp != nil {
			proto.Reset(resp)
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(responseRaw, resp); err != nil {
				recordRequestSpanError(span, err)
				return protoEnvelope{}, &Error{Code: "MEDALLION_INVALID_JSON_RESPONSE", RequestID: requestID, Message: "Medallion response was not valid protobuf JSON", Cause: errInvalidJSONResponse}
			}
		}
		setRequestSpanOK(span)
		return protoEnvelope{requestID: requestID}, nil
	}
	return protoEnvelope{}, &Error{Code: "MEDALLION_NETWORK_ERROR", Message: "Medallion request exhausted its retry budget"}
}

func (c *requestClient) setRequestHeaders(request *http.Request, ctx context.Context, workspaceID string) {
	// Trace propagation is intentionally applied first. The SDK-owned transport
	// boundary below then overwrites or removes every protected header, so a
	// custom propagator cannot replace credentials, scope, protocol, timeout, or
	// idempotency metadata.
	c.tracing.inject(ctx, request.Header)
	for _, name := range []string{
		"Accept",
		"Authorization",
		"Connect-Protocol-Version",
		"Connect-Timeout-Ms",
		"Content-Encoding",
		"Content-Length",
		"Content-Type",
		"Transfer-Encoding",
		"X-Medallion-API-Key",
		"X-Medallion-Workspace-Id",
	} {
		request.Header.Del(name)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Connect-Protocol-Version", "1")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Medallion-Workspace-Id", workspaceID)
	if c.accessToken != "" {
		request.Header.Set("Authorization", "Bearer "+c.accessToken)
	} else {
		request.Header.Set("X-Medallion-API-Key", c.apiKey)
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 {
			milliseconds := (remaining + time.Millisecond - 1) / time.Millisecond
			if milliseconds <= 9_999_999_999 {
				request.Header.Set("Connect-Timeout-Ms", strconv.FormatInt(int64(milliseconds), 10))
			}
		}
	}
}

func normalizeRetryConfig(config RetryConfig) (RetryConfig, error) {
	if config.MaxAttempts < 0 || config.MaxAttempts > maxRetryAttempts {
		return RetryConfig{}, invalidOptions("retry max attempts must be between 0 and 5")
	}
	if config.InitialBackoff < 0 || config.MaxBackoff < 0 {
		return RetryConfig{}, invalidOptions("retry backoff durations must not be negative")
	}
	if math.IsNaN(config.JitterRatio) || math.IsInf(config.JitterRatio, 0) || config.JitterRatio < 0 || config.JitterRatio > 1 {
		return RetryConfig{}, invalidOptions("retry jitter ratio must be between 0 and 1")
	}
	if config.MaxAttempts <= 1 {
		return RetryConfig{MaxAttempts: 1}, nil
	}
	if config.InitialBackoff == 0 {
		config.InitialBackoff = 100 * time.Millisecond
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = 2 * time.Second
	}
	if config.JitterRatio == 0 {
		config.JitterRatio = 0.2
	}
	if config.InitialBackoff > config.MaxBackoff || config.MaxBackoff > maxRetryBackoff {
		return RetryConfig{}, invalidOptions("retry backoff must be ordered and capped at 5 seconds")
	}
	return config, nil
}

func (c *requestClient) retryDelay(attempt int, retryAfter string) time.Duration {
	if serverDelay, ok := c.retryAfterDelay(retryAfter); ok {
		// Retry-After is a server minimum. The total request deadline decides
		// whether the retry can happen; never shorten it to client backoff.
		return serverDelay
	}
	delay := c.retry.InitialBackoff
	for index := 1; index < attempt && delay < c.retry.MaxBackoff; index++ {
		delay *= 2
		if delay > c.retry.MaxBackoff {
			delay = c.retry.MaxBackoff
		}
	}
	if delay <= 0 || c.retry.JitterRatio <= 0 {
		return delay
	}
	randomValue := rand.Float64()
	if c.randomFloat != nil {
		randomValue = c.randomFloat()
	}
	randomValue = math.Max(0, math.Min(1, randomValue))
	spread := float64(delay) * c.retry.JitterRatio
	jittered := float64(delay) - spread + randomValue*(2*spread)
	if jittered < 0 {
		return 0
	}
	if maximum := float64(c.retry.MaxBackoff); jittered > maximum {
		jittered = maximum
	}
	return time.Duration(jittered)
}

func (c *requestClient) retryAfterDelay(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		const maxDuration = time.Duration(1<<63 - 1)
		if seconds > uint64(maxDuration/time.Second) {
			return maxDuration, true
		}
		return time.Duration(seconds) * time.Second, true
	}
	retryAt, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	now := time.Now()
	if c.now != nil {
		now = c.now()
	}
	if !retryAt.After(now) {
		return 0, true
	}
	return retryAt.Sub(now), true
}

func canWaitForRetry(ctx context.Context, delay time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return !ok || delay < time.Until(deadline)
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryableTransportError(err error, ctx context.Context) bool {
	return ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func retryableResponseReadFailure(status int) bool {
	if status >= 200 && status < 300 {
		// A truncated successful response leaves a retry-safe operation's result
		// unknown, so replaying the exact serialized request is permitted.
		return true
	}
	switch status {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func classifyTransportError(err error) error {
	code := "MEDALLION_NETWORK_ERROR"
	message := "Medallion request failed"
	cause := errTransportFailure
	var netErr net.Error
	switch {
	case errors.Is(err, context.Canceled):
		code = "MEDALLION_ABORTED"
		message = "Medallion request was aborted"
		cause = context.Canceled
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		code = "MEDALLION_TIMEOUT"
		message = "Medallion request timed out"
		cause = context.DeadlineExceeded
	}
	return &Error{Code: code, Message: message, Cause: cause}
}

func classifyResponseReadError(err error, ctx context.Context, requestID string) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = ctxErr
	}
	classified := classifyTransportError(err).(*Error)
	classified.RequestID = requestID
	if classified.Code == "MEDALLION_NETWORK_ERROR" {
		classified.Message = "Medallion response body could not be read"
		classified.Cause = errResponseReadFailure
	}
	return classified
}

func readBoundedResponse(body io.Reader, contentLength, limit int64) ([]byte, error) {
	if contentLength > limit {
		return nil, errResponseTooLarge
	}
	limited := io.LimitReader(body, limit+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, errResponseTooLarge
	}
	return raw, nil
}

func validateWorkspaceID(value string) error {
	if value == "" {
		return missingOption("MEDALLION_MISSING_WORKSPACE_ID", "workspace ID is required")
	}
	if !isCanonicalWorkspaceID(value) {
		return &Error{Code: "MEDALLION_INVALID_WORKSPACE_ID", Message: "workspace ID must match ws_[26 canonical base32 characters]"}
	}
	return nil
}

func isCanonicalWorkspaceID(value string) bool {
	if len(value) != 29 || !strings.HasPrefix(value, "ws_") {
		return false
	}
	for index := 3; index < len(value); index++ {
		character := value[index]
		if !((character >= '0' && character <= '9') ||
			(character >= 'a' && character <= 'h') ||
			(character >= 'j' && character <= 'k') ||
			(character >= 'm' && character <= 'n') ||
			(character >= 'p' && character <= 't') ||
			(character >= 'v' && character <= 'z')) {
			return false
		}
	}
	return true
}

func validateConfiguredHeaderValue(value, name string, maximum int) error {
	if maximum > 0 && len(value) > maximum {
		return invalidOptions(fmt.Sprintf("%s must not exceed %d bytes", name, maximum))
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] == 0x7f {
			return invalidOptions(name + " contains invalid header characters")
		}
	}
	return nil
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
	return map[string]any{}
}

func apiErrorMessage(status int, requestID string) string {
	if requestID != "" {
		return fmt.Sprintf("Medallion API request failed with HTTP %d. Request ID: %s.", status, requestID)
	}
	return fmt.Sprintf("Medallion API request failed with HTTP %d.", status)
}

func sanitizeConnectCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(value) > 64 {
		return ""
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character < 'a' || character > 'z') && character != '_' {
			return ""
		}
	}
	return value
}

func sanitizeText(value string, maximumBytes int) string {
	var sanitized strings.Builder
	sanitized.Grow(min(len(value), maximumBytes))
	for _, character := range value {
		if unicode.IsControl(character) {
			character = ' '
		}
		if sanitized.Len()+utf8.RuneLen(character) > maximumBytes {
			break
		}
		sanitized.WriteRune(character)
	}
	return strings.TrimSpace(sanitized.String())
}

func (c *requestClient) redactionValues(requestRaw []byte) []string {
	values := make(map[string]struct{})
	addRedactionValue(values, c.apiKey, true)
	addRedactionValue(values, c.accessToken, true)

	decoder := json.NewDecoder(bytes.NewReader(requestRaw))
	decoder.UseNumber()
	var requestBody map[string]any
	if err := decoder.Decode(&requestBody); err == nil {
		if events, ok := requestBody["events"].([]any); ok {
			for _, rawEvent := range events {
				event, ok := rawEvent.(map[string]any)
				if !ok {
					continue
				}
				payloadJSON, ok := event["payloadJson"].(string)
				if !ok || payloadJSON == "" {
					continue
				}
				addRedactionValue(values, payloadJSON, false)
				payloadDecoder := json.NewDecoder(strings.NewReader(payloadJSON))
				payloadDecoder.UseNumber()
				var payload any
				if err := payloadDecoder.Decode(&payload); err == nil {
					collectJSONRedactionValues(payload, values)
				}
			}
		}
	}

	redactions := make([]string, 0, len(values))
	for value := range values {
		redactions = append(redactions, value)
	}
	sort.Slice(redactions, func(left, right int) bool {
		return len(redactions[left]) > len(redactions[right])
	})
	return redactions
}

func addRedactionValue(values map[string]struct{}, value string, allowShort bool) {
	if value == "" || (!allowShort && len(value) < 4) {
		return
	}
	values[value] = struct{}{}
}

func collectJSONRedactionValues(value any, values map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			addRedactionValue(values, key, false)
			collectJSONRedactionValues(child, values)
		}
	case []any:
		for _, child := range typed {
			collectJSONRedactionValues(child, values)
		}
	case string:
		addRedactionValue(values, typed, false)
	case json.Number:
		addRedactionValue(values, typed.String(), false)
	}
}

func redactText(value string, redactions []string) string {
	for _, sensitive := range redactions {
		value = strings.ReplaceAll(value, sensitive, "[REDACTED]")
	}
	return value
}

func redactErrorInfo(info *ConnectErrorInfo, redactions []string) *ConnectErrorInfo {
	if info == nil {
		return nil
	}
	redacted := &ConnectErrorInfo{
		Reason:   sanitizeText(redactText(info.Reason, redactions), 256),
		Domain:   sanitizeText(redactText(info.Domain, redactions), 256),
		Metadata: make(map[string]string, len(info.Metadata)),
	}
	for key, value := range info.Metadata {
		redactedKey := sanitizeText(redactText(key, redactions), 256)
		redacted.Metadata[redactedKey] = sanitizeText(redactText(value, redactions), 4096)
	}
	return redacted
}

func redactConnectDetails(details []ConnectErrorDetail, redactions []string) []ConnectErrorDetail {
	redacted := make([]ConnectErrorDetail, 0, len(details))
	for _, detail := range details {
		containsSensitiveValue := false
		for _, sensitive := range redactions {
			if bytes.Contains(detail.Value, []byte(sensitive)) {
				containsSensitiveValue = true
				break
			}
		}
		if containsSensitiveValue {
			continue
		}
		detail.Type = sanitizeText(redactText(detail.Type, redactions), 512)
		redacted = append(redacted, detail)
	}
	return redacted
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
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
