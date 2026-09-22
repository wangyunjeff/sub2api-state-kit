package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	pluginv1 "github.com/wangyunjeff/sub2api-state-kit/plugin/internal/pluginapi/v1"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const maxManualText = 256 * 1024

type proxyTestConfig struct {
	DynamicProxyURL string `json:"dynamic_proxy_url"`
	Mode            string `json:"harvest_dial_proxy_mode"`
	ProxyID         int64  `json:"harvest_dial_proxy_id"`
	URL             string `json:"harvest_dial_proxy_url"`
}

type manualAction struct {
	Proxy      *proxyTestConfig `json:"proxy,omitempty"`
	Kind       string           `json:"kind"`
	RequestID  string           `json:"request_id"`
	AccountID  int64            `json:"account_id"`
	Model      string           `json:"model"`
	Prompt     string           `json:"prompt"`
	UseState   bool             `json:"use_state"`
	Route      string           `json:"route"`
	ExpectedIP string           `json:"expected_ip"`
}
type manualResult struct {
	ProxySessionExpiresAt string `json:"proxy_session_expires_at,omitempty"`
	ID                    string `json:"id"`
	Kind                  string `json:"kind"`
	AccountID             int64  `json:"account_id"`
	Model                 string `json:"model"`
	ActualModel           string `json:"actual_model"`
	Running               bool   `json:"running"`
	Complete              bool   `json:"complete"`
	Matches               bool   `json:"matches"`
	Injected              bool   `json:"injected"`
	Route                 string `json:"route"`
	ExitIP                string `json:"exit_ip"`
	CountryCode           string `json:"country_code"`
	IPMatches             *bool  `json:"ip_matches,omitempty"`
	StateBytes            int    `json:"state_bytes"`
	HTTPStatus            int    `json:"http_status"`
	Text                  string `json:"text"`
	Error                 string `json:"error"`
	IPError               bool   `json:"ip_error"`
	StartedAt             string `json:"started_at"`
	DurationMS            int64  `json:"duration_ms"`
}

func (e *Engine) RunAction(_ context.Context, req *pluginv1.RunActionRequest) (*pluginv1.RunActionResponse, error) {
	reject := func(message string) (*pluginv1.RunActionResponse, error) {
		return &pluginv1.RunActionResponse{Message: message}, nil
	}
	if req == nil || len(req.ActionJson) > 32768 {
		return reject("动作参数过大或为空")
	}
	var a manualAction
	d := json.NewDecoder(bytes.NewReader(req.ActionJson))
	d.DisallowUnknownFields()
	if d.Decode(&a) != nil || d.Decode(new(any)) != io.EOF || a.RequestID == "" || len(a.RequestID) > 128 {
		return reject("动作参数无效")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return reject("插件已停止")
	}
	for _, id := range e.actionIDs {
		if id == a.RequestID {
			return &pluginv1.RunActionResponse{Accepted: true, Message: "该操作已接收，请查看状态"}, nil
		}
	}
	if a.Kind == "cancel_test" {
		if e.manual == nil || !e.manual.Running {
			return reject("当前没有正在运行的测试")
		}
		if e.manualCancel != nil {
			e.manualCancel()
		}
		return &pluginv1.RunActionResponse{Accepted: true, Message: "已请求停止测试"}, nil
	}
	if e.host == nil || (a.Kind != "proxy_test" && a.Kind != "refresh_all" && !e.directory[a.AccountID]) {
		return reject("账号不可用，请先确认宿主账号状态")
	}
	message := "操作已开始，进度会显示在对应账号或测试区域"
	switch a.Kind {
	case "refresh", "refresh_all":
		if !e.config.Enabled {
			return reject("请先保存并开启总开关")
		}
		targets := []AccountConfig{}
		if a.Kind == "refresh" {
			c, ok := findAccount(e.config, a.AccountID)
			if !ok || !c.Enabled {
				return reject("请先保存并开启此账号开关")
			}
			targets = append(targets, c)
		} else {
			for _, c := range e.config.Accounts {
				if c.Enabled && e.directory[c.AccountID] {
					targets = append(targets, c)
				}
			}
			if len(targets) == 0 {
				return reject("没有已开启且可用的账号")
			}
		}
		started := 0
		trigger := "manual"
		if a.Kind == "refresh_all" {
			trigger = "manual_all"
		}
		for _, c := range targets {
			accountStarted := false
			for _, model := range effectiveModels(e.config, c) {
				if e.startCollectLocked(c, model, true, trigger) {
					accountStarted = true
				}
			}
			if accountStarted {
				started++
			}
		}
		if a.Kind == "refresh" {
			if started == 0 {
				message = "此账号已在获取票据，没有重复启动；其他账号不受影响"
			} else {
				message = "已仅启动此账号，请查看该行进度"
			}
		} else {
			message = fmt.Sprintf("已启动 %d 个账号；%d 个已在运行，未重复启动。关闭的账号不参与", started, len(targets)-started)
		}
	case "test", "ip", "proxy_test":
		if e.manual != nil && e.manual.Running {
			return reject("已有测试正在运行，请等待或先停止")
		}
		testConfig := e.config
		if a.Kind == "proxy_test" {
			if a.Proxy == nil {
				return reject("请填写要测试的代理配置")
			}
			c := DefaultConfig()
			c.DynamicProxyURL = a.Proxy.DynamicProxyURL
			c.HarvestDialProxyMode = a.Proxy.Mode
			c.HarvestDialProxyID = a.Proxy.ProxyID
			c.HarvestDialProxyURL = a.Proxy.URL
			var err error
			testConfig, err = ParseConfig([]byte(jsonText(c)))
			if err != nil || testConfig.DynamicProxyURL == "" {
				return reject("动态或前置代理配置无效")
			}
			e.testedProxy = nil
			a.Route = "dynamic"
		}
		if a.Kind == "test" {
			if !modelPattern.MatchString(a.Model) || strings.TrimSpace(a.Prompt) == "" || len(a.Prompt) > 24000 {
				return reject("请输入模型和提示词（最多 24000 字节）")
			}
			a.Route = "business"
		} else if a.Route != "business" && a.Route != "dynamic" && a.Route != "direct" {
			return reject("请选择业务、动态或直连出口")
		}
		if a.ExpectedIP != "" && net.ParseIP(a.ExpectedIP) == nil {
			return reject("预期 IP 格式无效")
		}
		timeout := 3 * time.Minute
		if a.Kind == "proxy_test" {
			timeout = 30 * time.Second
		}
		ctx, cancel := context.WithTimeout(e.ctx, timeout)
		e.manualCancel = cancel
		e.manual = &manualResult{ID: a.RequestID, Kind: a.Kind, AccountID: a.AccountID, Model: a.Model, Route: a.Route, Running: true, StartedAt: time.Now().UTC().Format(time.RFC3339)}
		e.wg.Add(1)
		go e.runManual(ctx, cancel, a, testConfig, e.host)
	default:
		return reject("不支持的操作")
	}
	e.actionIDs = append(e.actionIDs, a.RequestID)
	if len(e.actionIDs) > 100 {
		e.actionIDs = e.actionIDs[1:]
	}
	return &pluginv1.RunActionResponse{Accepted: true, Message: message}, nil
}
func (e *Engine) manualUpdate(id string, update func(*manualResult)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.manual != nil && e.manual.ID == id {
		update(e.manual)
	}
}
func (e *Engine) runManual(ctx context.Context, cancel context.CancelFunc, a manualAction, c Config, host pluginv1.HostServiceClient) {
	defer e.wg.Done()
	defer cancel()
	started := time.Now()
	defer e.manualUpdate(a.RequestID, func(r *manualResult) {
		r.Running = false
		e.manualCancel = nil
		r.DurationMS = time.Since(started).Milliseconds()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			r.Error = "测试超时，请检查出口后重试"
		} else if errors.Is(ctx.Err(), context.Canceled) {
			r.Error = "测试已取消"
		}
	})
	fail := func(message string) { e.manualUpdate(a.RequestID, func(r *manualResult) { r.Error = message }) }
	identity := &pluginv1.ResolveOutboundIdentityResponse{}
	var err error
	if a.Kind != "proxy_test" {
		identity, err = resolveIdentity(ctx, host, a.AccountID)
	}
	if err != nil {
		fail("无法取得账号授权和业务代理")
		return
	}
	route, outer := identity.ProxyUrl, ""
	if a.Route == "business" {
		if _, listed := findAccount(c, a.AccountID); listed {
			outer, err = e.resolveBusinessFront(ctx, c, route)
			if err != nil {
				fail("业务前置代理不可用，请检查前置代理配置")
				return
			}
		}
	}
	if a.Route == "direct" {
		route = ""
	}
	if a.Route == "dynamic" {
		if c.DynamicProxyURL == "" {
			fail("请先保存动态代理配置")
			return
		}
		route, err = rotateProxy(c.DynamicProxyURL)
		if err == nil {
			outer, err = e.resolveFrontProxy(ctx, c)
		}
		if err != nil {
			fail("动态或前置代理配置不可用")
			return
		}
	}
	client, err := harvestClient(route, outer)
	if err != nil {
		fail("代理连接配置无效")
		return
	}
	defer client.CloseIdleConnections()
	client.Timeout = 3 * time.Minute
	ip, iperr := detectExitIP(ctx, client, e.exitIPURL)
	country := e.countryForIP(ctx, ip)
	e.manualUpdate(a.RequestID, func(r *manualResult) {
		r.ExitIP = ip
		r.CountryCode = country
		r.IPError = iperr != nil
		if a.ExpectedIP != "" && iperr == nil {
			v := net.ParseIP(a.ExpectedIP).Equal(net.ParseIP(ip))
			r.IPMatches = &v
		}
	})
	if a.Kind == "ip" || a.Kind == "proxy_test" {
		if iperr != nil {
			fail("出口 IP 检测失败")
		} else {
			expires := ""
			if a.Kind == "proxy_test" && ctx.Err() == nil {
				expires = e.rememberTestedProxy(a.RequestID, c, route, outer, started)
			}
			e.manualUpdate(a.RequestID, func(r *manualResult) {
				r.Complete = true
				r.ProxySessionExpiresAt = expires
			})
		}
		return
	}
	var ticket *receipt
	requestModel := effectiveModel(c, a.Model)
	if a.UseState {
		ticket, err = e.ticketForRequest(ctx, &pluginv1.ForwardRequestStart{Platform: "openai", AccountType: "oauth", AccountId: a.AccountID, ProxyUrl: identity.ProxyUrl, Headers: identity.Headers}, a.Model)
		if err != nil {
			fail("当前无可用票据，且无票据放行开关已关闭")
			return
		}
	}
	payload := map[string]any{"model": requestModel, "store": false, "stream": true, "instructions": "Follow the user's request.", "input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_text", "text": a.Prompt}}}}}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.probeURL, bytes.NewReader(body))
	if err != nil {
		fail("请求构造失败")
		return
	}
	for k, v := range identity.Headers {
		if v != nil && !strings.EqualFold(k, StateHeader) {
			req.Header[k] = append([]string{}, v.Values...)
		}
	}
	req.Header.Set("Authorization", "Bearer "+identity.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("OpenAI-Beta", "responses=experimental")
	req.Header.Set("session_id", randomID())
	req.Header.Set("version", "0.153.4")
	req.Header.Set("User-Agent", "codex_cli_rs/0.153.4")
	req.Header.Set("originator", "codex_cli_rs")
	if ticket != nil {
		req.Header.Set(StateHeader, ticket.State)
	}
	e.manualUpdate(a.RequestID, func(r *manualResult) { r.Injected = ticket != nil })
	response, err := client.Do(req)
	if err != nil {
		fail("模型请求失败：" + transportCode(err))
		return
	}
	defer response.Body.Close()
	state := strings.TrimSpace(response.Header.Get(StateHeader))
	e.manualUpdate(a.RequestID, func(r *manualResult) { r.HTTPStatus = response.StatusCode; r.StateBytes = len(state) })
	if validState(state, 312) && ticket != nil {
		e.invalidate(ticket, "state_312")
	}
	if response.StatusCode != 200 {
		switch response.StatusCode {
		case http.StatusUnauthorized:
			fail("已收到上游响应，但账号授权被拒绝（401）；请检查或重新授权该账号")
		case http.StatusForbidden:
			fail("已收到上游响应，但访问被拒绝（403）；请检查账号或出口限制")
		case http.StatusTooManyRequests:
			fail("已收到上游响应，但触发限流（429）；请稍后重试")
		default:
			fail("已收到上游响应，但返回非 200 状态，请查看 HTTP 状态码")
		}
		return
	}
	obs := newCompletionObserver(requestModel)
	text := ""
	upstreamFailure := ""
	safe := func(s string) string {
		for _, secret := range []string{identity.Token, state, route, outer} {
			if secret != "" {
				s = strings.ReplaceAll(s, secret, "[敏感值已隐藏]")
			}
		}
		if ticket != nil {
			s = strings.ReplaceAll(s, ticket.State, "[票据已隐藏]")
		}
		return s
	}
	consume := func(data []byte) error {
		obs.inspect(data)
		if message := manualUpstreamFailure(data); message != "" {
			upstreamFailure = message
		}
		var event struct {
			Type   string `json:"type"`
			Delta  string `json:"delta"`
			Output []struct {
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			} `json:"output"`
			Response *struct {
				Output []struct {
					Content []struct {
						Text string `json:"text"`
					} `json:"content"`
				} `json:"output"`
			} `json:"response"`
		}
		if json.Unmarshal(data, &event) != nil {
			return nil
		}
		if event.Type == "response.output_text.delta" {
			text += event.Delta
		}
		output := event.Output
		if event.Type == "response.completed" && event.Response != nil {
			output = event.Response.Output
		}
		full := ""
		for _, item := range output {
			for _, content := range item.Content {
				full += content.Text
			}
		}
		if full != "" {
			text = full
		}
		if len(text) > maxManualText {
			return errors.New("output too large")
		}
		e.manualUpdate(a.RequestID, func(r *manualResult) { r.Text = safe(text) })
		return nil
	}
	err = readManualResponse(response, consume)
	if upstreamFailure != "" {
		fail(upstreamFailure)
		return
	}
	if err != nil {
		fail("响应中断、超时或输出超过大小限制")
		return
	}
	complete, matches := obs.Result()
	e.manualUpdate(a.RequestID, func(r *manualResult) {
		r.Complete = complete
		r.Matches = matches
		r.ActualModel = obs.actual
		if !complete {
			r.Error = "未收到完整成功响应，不能判断模型是否匹配"
		}
	})
	if ticket != nil && complete && !matches {
		e.invalidate(ticket, "model_mismatch")
	}
}

// HTTP 200 can still contain a failed Responses stream. Report an allowlisted
// reason rather than exposing arbitrary upstream text or calling it a timeout.
func manualUpstreamFailure(data []byte) string {
	type responseError struct {
		Code string `json:"code"`
		Type string `json:"type"`
	}
	var event struct {
		Type     string         `json:"type"`
		Status   string         `json:"status"`
		Error    *responseError `json:"error"`
		Response *struct {
			Status string         `json:"status"`
			Error  *responseError `json:"error"`
		} `json:"response"`
	}
	if json.Unmarshal(data, &event) != nil {
		return ""
	}
	upstream := event.Error
	failed := event.Type == "error" || event.Type == "response.failed" || event.Type == "response.incomplete" || event.Status == "failed" || event.Status == "incomplete"
	if event.Response != nil {
		failed = failed || event.Response.Status == "failed" || event.Response.Status == "incomplete"
		if upstream == nil {
			upstream = event.Response.Error
		}
	}
	if upstream != nil {
		switch upstream.Code {
		case "server_is_overloaded":
			return "已连通上游，但上游服务器过载（server_is_overloaded）；请稍后重试"
		case "rate_limit_exceeded":
			return "已连通上游，但上游返回限流（rate_limit_exceeded）；请稍后重试"
		}
		if upstream.Type == "service_unavailable_error" {
			return "已连通上游，但上游服务暂不可用；请稍后重试"
		}
		failed = true
	}
	if failed {
		return "已连通上游，但上游返回失败或未完成响应；本次无法判断模型是否匹配"
	}
	return ""
}

func readManualResponse(response *http.Response, consume func([]byte) error) error {
	limited := io.LimitReader(response.Body, 8*1024*1024+1)
	if strings.Contains(response.Header.Get("Content-Type"), "application/json") {
		b, err := io.ReadAll(limited)
		if err != nil {
			return err
		}
		if len(b) > maxObservedFrame {
			return errors.New("large JSON")
		}
		return consume(b)
	}
	scan := bufio.NewScanner(limited)
	scan.Buffer(make([]byte, 4096), maxObservedFrame)
	var event []byte
	total := 0
	for scan.Scan() {
		line := scan.Bytes()
		total += len(line) + 1
		if total > 8*1024*1024 {
			return errors.New("large response")
		}
		if len(line) == 0 {
			if len(event) > 0 {
				if err := consume(event); err != nil {
					return err
				}
			}
			event = nil
		} else if bytes.HasPrefix(line, []byte("data:")) {
			data := bytes.TrimSpace(line[5:])
			if len(event)+len(data) > maxObservedFrame {
				return errors.New("large event")
			}
			event = append(event, data...)
			event = append(event, '\n')
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if len(event) > 0 {
		return consume(event)
	}
	return nil
}
