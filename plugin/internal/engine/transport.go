package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	pluginv1 "github.com/wangyunjeff/sub2api-state-kit/plugin/internal/pluginapi/v1"
)

const (
	// Only explicitly enabled accounts are buffered for model inspection. Match
	// typical large code/image prompts while bounding per-request memory use.
	maxTicketRequestBody = 64 << 20
	maxForwardDuration   = 10 * time.Minute
	maxPooledClients     = 32
)

type pooledClient struct {
	client *http.Client
	used   uint64
}
type clientPool struct {
	mu      sync.Mutex
	clients map[[32]byte]pooledClient
	clock   uint64
}

func newClientPool() *clientPool { return &clientPool{clients: make(map[[32]byte]pooledClient)} }

func (p *clientPool) client(proxyURL string) (*http.Client, error) {
	return p.chainedClient(proxyURL, "")
}
func (p *clientPool) chainedClient(proxyURL, outer string) (*http.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := sha256.Sum256([]byte(proxyURL + "\x00" + outer))
	p.clock++
	if item, ok := p.clients[key]; ok {
		item.used = p.clock
		p.clients[key] = item
		return item.client, nil
	}
	client, err := chainedHTTPClient(proxyURL, outer, false)
	if err != nil {
		return nil, err
	}
	if len(p.clients) >= maxPooledClients {
		var oldestKey [32]byte
		oldest := ^uint64(0)
		for k, item := range p.clients {
			if item.used < oldest {
				oldestKey, oldest = k, item.used
			}
		}
		p.clients[oldestKey].client.CloseIdleConnections()
		delete(p.clients, oldestKey)
	}
	p.clients[key] = pooledClient{client: client, used: p.clock}
	return client, nil
}

func (p *clientPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, item := range p.clients {
		item.client.CloseIdleConnections()
		delete(p.clients, key)
	}
}

func freshProbeClient(proxyURL string) (*http.Client, error) { return makeHTTPClient(proxyURL, true) }

func makeHTTPClient(proxyURL string, fresh bool) (*http.Client, error) {
	var proxy func(*http.Request) (*url.URL, error)
	if proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil || u.Hostname() == "" || u.Fragment != "" || u.RawQuery != "" || (u.Path != "" && u.Path != "/") {
			return nil, errors.New("invalid proxy configuration")
		}
		switch u.Scheme {
		case "http", "https", "socks5", "socks5h":
		default:
			return nil, errors.New("unsupported proxy protocol")
		}
		if u.Port() != "" {
			port, err := strconv.Atoi(u.Port())
			if err != nil || port < 1 || port > 65535 {
				return nil, errors.New("invalid proxy port")
			}
		}
		proxy = http.ProxyURL(u)
	}
	transport := &http.Transport{
		Proxy:                 proxy,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true,
		DisableKeepAlives:     fresh,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	timeout := maxForwardDuration
	if fresh {
		timeout = 60 * time.Second
	}
	return &http.Client{
		Transport: transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

func (e *Engine) Forward(stream pluginv1.TransportPlugin_ForwardServer) error {
	started := time.Now()
	ctx, cancel := context.WithTimeout(stream.Context(), maxForwardDuration)
	defer cancel()
	if e.ctx != nil {
		stop := context.AfterFunc(e.ctx, cancel)
		defer stop()
	}
	first, err := recvFrame(ctx, stream)
	if err != nil {
		return sendForwardError(stream, "invalid_request", "Request start is missing", false)
	}
	start := first.GetStart()
	if start == nil {
		return sendForwardError(stream, "invalid_request", "The first frame must be a request start", false)
	}
	u, err := url.Parse(start.Url)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return sendForwardError(stream, "invalid_request", "Invalid upstream URL", false)
	}
	var body io.Reader
	var pipe *io.PipeReader
	var ticket *receipt
	var model string
	if e.accountEnabled(start) {
		data, readErr := readConfiguredBody(ctx, stream, start.HasBody)
		if readErr != nil {
			return sendUnavailable(stream, started)
		}
		var request struct {
			Model string `json:"model"`
		}
		if json.Unmarshal(data, &request) != nil || strings.TrimSpace(request.Model) == "" {
			return sendUnavailable(stream, started)
		}
		e.mu.Lock()
		forceModel := e.config.ForceModel
		e.mu.Unlock()
		model = effectiveModel(Config{ForceModel: forceModel}, request.Model)
		if forceModel != "" {
			data, err = rewriteModelField(data, forceModel)
			if err != nil {
				return sendUnavailable(stream, started)
			}
		}
		ticket, err = e.ticketForRequest(ctx, start, model)
		if err != nil {
			return sendUnavailable(stream, started)
		}
		if start.HasBody {
			body = bytes.NewReader(data)
		}
	} else if start.HasBody {
		var writer *io.PipeWriter
		pipe, writer = io.Pipe()
		defer pipe.Close()
		body = pipe
		go receiveBody(stream, writer)
	}
	req, err := http.NewRequestWithContext(ctx, start.Method, start.Url, body)
	if err != nil {
		return sendForwardError(stream, "invalid_request", "Invalid upstream request", false)
	}
	for name, values := range start.Headers {
		if values == nil {
			continue
		}
		req.Header[name] = append([]string(nil), values.Values...)
	}
	if start.Host != "" {
		req.Host = start.Host
	}
	req.ContentLength = start.ContentLength
	// Do not make a buffered business POST eligible for transport-level replay.
	req.GetBody = nil
	if !start.HasBody {
		req.ContentLength = 0
	}
	if ticket != nil {
		// Remove differently cased map keys as well; Set alone canonicalizes only
		// the new key and could leave a caller-supplied duplicate header intact.
		for key := range req.Header {
			if strings.EqualFold(key, StateHeader) || strings.EqualFold(key, "Accept-Encoding") {
				delete(req.Header, key)
			}
		}
		req.Header.Set(StateHeader, ticket.State)
		// Completion inspection observes the bytes actually forwarded to the host.
		// Negotiate an uncompressed response only when we inject a ticket; normal
		// passthrough preserves the caller's compression preferences and raw bytes.
		req.Header.Set("Accept-Encoding", "identity")
	}
	e.mu.Lock()
	businessConfig := e.config
	_, listed := findAccount(businessConfig, start.AccountId)
	e.mu.Unlock()
	outer := ""
	if listed && start.Platform == "openai" && start.AccountType == "oauth" {
		outer, err = e.resolveBusinessFront(ctx, businessConfig, start.ProxyUrl)
		if err != nil {
			return sendForwardError(stream, "invalid_proxy", "Business front proxy unavailable", false)
		}
	}
	client, err := e.clients.chainedClient(start.ProxyUrl, outer)
	if err != nil {
		return sendForwardError(stream, "invalid_proxy", "Invalid proxy configuration", false)
	}
	response, err := client.Do(req)
	if err != nil {
		return sendForwardError(stream, "upstream_transport", "Upstream transport failed", true)
	}
	defer response.Body.Close()
	if pipe != nil {
		_ = pipe.Close()
	}
	headers := make(map[string]*pluginv1.HeaderValues, len(response.Header))
	for name, values := range response.Header {
		headers[name] = &pluginv1.HeaderValues{Values: append([]string(nil), values...)}
	}
	if err := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Start{Start: &pluginv1.ForwardResponseStart{
		StatusCode: int32(response.StatusCode), Status: response.Status, Protocol: response.Proto,
		ProtocolMajor: int32(response.ProtoMajor), ProtocolMinor: int32(response.ProtoMinor),
		Headers: headers, ContentLength: response.ContentLength,
	}}}); err != nil {
		return err
	}
	var observer *completionObserver
	if ticket != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
		if validState(response.Header.Get(StateHeader), 312) {
			e.invalidate(ticket, "state_312")
		}
		observer = newCompletionObserver(model)
	}
	buffer := make([]byte, 32*1024)
	var received int64
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			chunk := append([]byte(nil), buffer[:n]...)
			if observer != nil {
				observer.Write(chunk)
			}
			if err := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_BodyChunk{BodyChunk: chunk}}); err != nil {
				return err
			}
			received += int64(n)
			// Consumers may close the transport as soon as response.completed is
			// delivered. Observe that completion now rather than waiting for EOF.
			if observer != nil {
				if complete, matches := observer.Result(); complete && !matches {
					e.invalidate(ticket, "model_mismatch")
					observer = nil
				}
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				return sendForwardError(stream, "upstream_read", "Upstream response was interrupted", true)
			}
			break
		}
	}
	if observer != nil {
		observer.Finish()
		if complete, matches := observer.Result(); complete && !matches {
			e.invalidate(ticket, "model_mismatch")
		}
	}
	return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_End{End: &pluginv1.ForwardResponseEnd{
		BytesReceived: received, DurationMs: time.Since(started).Milliseconds(),
	}}})
}

// rewriteModelField changes only the top-level JSON model field. The plugin
// buffers enabled-account requests already, so this keeps the forced-model
// behavior bounded to the same inspection path as STATE injection.
func rewriteModelField(data []byte, model string) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	payload["model"], _ = json.Marshal(model)
	return json.Marshal(payload)
}

func readConfiguredBody(ctx context.Context, stream pluginv1.TransportPlugin_ForwardServer, hasBody bool) ([]byte, error) {
	if !hasBody {
		return nil, nil
	}
	var data []byte
	for {
		frame, err := recvFrame(ctx, stream)
		if err != nil {
			return nil, errors.New("incomplete request body")
		}
		switch value := frame.Frame.(type) {
		case *pluginv1.ForwardRequest_BodyChunk:
			if len(value.BodyChunk) > maxTicketRequestBody-len(data) {
				return nil, errors.New("request body exceeds inspection limit")
			}
			data = append(data, value.BodyChunk...)
		case *pluginv1.ForwardRequest_BodyEnd:
			if !value.BodyEnd {
				return nil, errors.New("invalid body end")
			}
			return data, nil
		default:
			return nil, errors.New("unexpected request frame")
		}
	}
}

// Recv itself is unblocked by gRPC when Forward returns. The buffered result
// channel lets the deadline terminate Forward even if the host stalls mid-frame.
func recvFrame(ctx context.Context, stream pluginv1.TransportPlugin_ForwardServer) (*pluginv1.ForwardRequest, error) {
	type result struct {
		frame *pluginv1.ForwardRequest
		err   error
	}
	ch := make(chan result, 1)
	go func() { frame, err := stream.Recv(); ch <- result{frame, err} }()
	select {
	case result := <-ch:
		return result.frame, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func receiveBody(stream pluginv1.TransportPlugin_ForwardServer, writer *io.PipeWriter) {
	defer writer.Close()
	for {
		frame, err := stream.Recv()
		if err != nil {
			_ = writer.CloseWithError(errors.New("incomplete request body"))
			return
		}
		switch value := frame.Frame.(type) {
		case *pluginv1.ForwardRequest_BodyChunk:
			if _, err := writer.Write(value.BodyChunk); err != nil {
				return
			}
		case *pluginv1.ForwardRequest_BodyEnd:
			if !value.BodyEnd {
				_ = writer.CloseWithError(errors.New("invalid body end"))
			}
			return
		default:
			_ = writer.CloseWithError(errors.New("unexpected request frame"))
			return
		}
	}
}

func sendForwardError(stream pluginv1.TransportPlugin_ForwardServer, code, message string, sent bool) error {
	return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Error{Error: &pluginv1.ForwardResponseError{
		Code: code, Message: message, RequestSent: sent,
	}}})
}

func sendUnavailable(stream pluginv1.TransportPlugin_ForwardServer, started time.Time) error {
	body := []byte(`{"error":{"message":"STATE ticket is not ready; retry after recovery completes","type":"state_ticket_unavailable"}}`)
	if err := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Start{Start: &pluginv1.ForwardResponseStart{
		StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable", Protocol: "HTTP/1.1", ProtocolMajor: 1, ProtocolMinor: 1,
		ContentLength: int64(len(body)), Headers: map[string]*pluginv1.HeaderValues{
			"Content-Type": {Values: []string{"application/json"}}, "Retry-After": {Values: []string{"10"}},
		},
	}}}); err != nil {
		return err
	}
	if err := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_BodyChunk{BodyChunk: body}}); err != nil {
		return err
	}
	return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_End{End: &pluginv1.ForwardResponseEnd{
		BytesReceived: int64(len(body)), DurationMs: time.Since(started).Milliseconds(),
	}}})
}
