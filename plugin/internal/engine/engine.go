// Package engine implements the public Sub2API transport plugin contract.
package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	hcplugin "github.com/hashicorp/go-plugin"
	pluginv1 "github.com/wangyunjeff/sub2api-state-kit/plugin/internal/pluginapi/v1"
	"google.golang.org/grpc"
)

type Engine struct {
	countryURL    string
	countryCache  map[string]countryEntry
	countryClient *http.Client
	forceRefresh  map[string]bool
	manual        *manualResult
	manualCancel  context.CancelFunc
	testedProxy   *testedProxySession
	actionIDs     []string
	pluginv1.UnimplementedTransportPluginServer
	mu               sync.Mutex
	persistMu        sync.Mutex
	config           Config
	generation       uint64
	ctx              context.Context
	cancel           context.CancelFunc
	generationCtx    context.Context
	generationCancel context.CancelFunc
	wake             chan struct{}
	done             chan struct{}
	wg               sync.WaitGroup
	closed           bool
	broker           *hcplugin.GRPCBroker
	host             pluginv1.HostServiceClient
	hostConn         *grpc.ClientConn
	hostReady        bool
	resources        *pluginv1.ListResourcesResponse
	directory        map[int64]bool
	directoryAt      time.Time
	directoryError   string
	tickets          map[string]*ticket
	records          map[string]*jobRecord
	jobs             map[string]uint64
	revoked          map[string]string
	semaphore        chan struct{}
	clients          *clientPool
	probeURL         string
	exitIPURL        string
	events           []activityEvent
	eventSeq         uint64
	tick             time.Duration
	warmup           time.Duration
	activeAfter      time.Time
}
type jobRecord struct {
	RoundID       string
	Trigger       string
	ExitIP        string
	CountryCode   string
	Attempts      int
	LastError     string
	CooldownUntil time.Time
}
type ticket struct {
	AccountID           int64     `json:"account_id"`
	Model               string    `json:"model"`
	Plan                string    `json:"plan"`
	State               string    `json:"state"`
	Version             string    `json:"version"`
	ConfigFingerprint   string    `json:"config_fingerprint"`
	FixedFingerprint    string    `json:"fixed_fingerprint"`
	IdentityFingerprint string    `json:"identity_fingerprint"`
	CapturedAt          time.Time `json:"captured_at"`
	ExpiresAt           time.Time `json:"expires_at"`
}
type receipt struct {
	State, Version, Key, ConfigFingerprint string
	Generation                             uint64
}
type statusTicket struct {
	Trigger          string `json:"trigger"`
	ExitIP           string `json:"exit_ip"`
	CountryCode      string `json:"country_code"`
	AccountID        int64  `json:"account_id"`
	Model            string `json:"model"`
	Plan             string `json:"plan"`
	State            string `json:"state"`
	Status           string `json:"status"`
	RemainingSeconds int64  `json:"remaining_seconds"`
	ExpiresAt        string `json:"expires_at"`
	LastError        string `json:"last_error"`
	Attempts         int    `json:"attempts"`
}
type statusSnapshot struct {
	ActionsReady   bool                       `json:"actions_ready"`
	ManualTest     *manualResult              `json:"manual_test,omitempty"`
	ResourcesReady bool                       `json:"resources_ready"`
	Accounts       []*pluginv1.AccountSummary `json:"accounts"`
	Proxies        []*pluginv1.ProxySummary   `json:"proxies"`
	Events         []activityEvent            `json:"events"`
	HostReady      bool                       `json:"host_ready"`
	AccountIDs     []int64                    `json:"account_ids"`
	Tickets        []statusTicket             `json:"tickets"`
	Message        string                     `json:"message"`
}

func New() *Engine {
	e := newEngine(nil, "https://chatgpt.com/backend-api/codex/responses", time.Second)
	e.mu.Lock()
	e.countryURL = "https://api.country.is/"
	e.mu.Unlock()
	return e
}

// Tests may inject a host and loopback probe endpoint; production configuration
// deliberately has no endpoint override and never receives OAuth tokens from UI.
func newEngine(host pluginv1.HostServiceClient, probeURL string, tick time.Duration) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	gc, gcancel := context.WithCancel(ctx)
	e := &Engine{countryCache: map[string]countryEntry{}, countryClient: &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{}}, forceRefresh: map[string]bool{}, config: DefaultConfig(), ctx: ctx, cancel: cancel, generationCtx: gc, generationCancel: gcancel, wake: make(chan struct{}, 1), done: make(chan struct{}), host: host, hostReady: host != nil, directory: map[int64]bool{}, tickets: map[string]*ticket{}, records: map[string]*jobRecord{}, jobs: map[string]uint64{}, revoked: map[string]string{}, semaphore: make(chan struct{}, 4), clients: newClientPool(), probeURL: probeURL, tick: tick, warmup: 5 * time.Second}
	e.exitIPURL = "https://api.ipify.org?format=json"
	go e.loop()
	return e
}
func (e *Engine) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		<-e.done
		return
	}
	e.closed = true
	e.cancel()
	e.generationCancel()
	conn := e.hostConn
	e.mu.Unlock()
	<-e.done
	e.wg.Wait()
	e.clients.Close()
	e.countryClient.CloseIdleConnections()
	if conn != nil {
		_ = conn.Close()
	}
}
func (e *Engine) SetHostBroker(b *hcplugin.GRPCBroker) { e.mu.Lock(); e.broker = b; e.mu.Unlock() }
func (e *Engine) InitHostServices(ctx context.Context, r *pluginv1.InitHostServicesRequest) (*pluginv1.InitHostServicesResponse, error) {
	if r == nil || r.HostServiceApiVersion != pluginv1.HostServiceAPIVersion {
		return &pluginv1.InitHostServicesResponse{Message: "unsupported host service API"}, nil
	}
	e.mu.Lock()
	b := e.broker
	closed := e.closed
	e.mu.Unlock()
	if b == nil || closed {
		return &pluginv1.InitHostServicesResponse{Message: "host broker unavailable"}, nil
	}
	conn, err := b.Dial(r.HostServiceId)
	if err != nil {
		return &pluginv1.InitHostServicesResponse{Message: "host service connection failed"}, nil
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		conn.Close()
		return &pluginv1.InitHostServicesResponse{Message: "plugin stopped"}, nil
	}
	old := e.hostConn
	e.hostConn = conn
	e.host = pluginv1.NewHostServiceClient(conn)
	e.hostReady = true
	e.directoryAt = time.Time{}
	e.mu.Unlock()
	if old != nil {
		old.Close()
	}
	e.notify()
	return &pluginv1.InitHostServicesResponse{Ready: true, Message: "host services connected"}, nil
}
func (e *Engine) GetInfo(context.Context, *pluginv1.GetInfoRequest) (*pluginv1.GetInfoResponse, error) {
	return &pluginv1.GetInfoResponse{PluginId: PluginID, PluginVersion: Version, ProtocolVersion: 1, TransportApiVersion: 1, Capabilities: []string{"openai.oauth.outbound_transport.v1"}}, nil
}
func (e *Engine) Health(context.Context, *pluginv1.HealthRequest) (*pluginv1.HealthResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.snapshotLocked(time.Now())
	return &pluginv1.HealthResponse{Healthy: !e.closed, Message: s.Message, StatusJson: jsonText(s)}, nil
}
func (e *Engine) ValidateConfig(_ context.Context, r *pluginv1.ValidateConfigRequest) (*pluginv1.ValidateConfigResponse, error) {
	if r == nil {
		r = &pluginv1.ValidateConfigRequest{}
	}
	c, err := ParseConfig(r.ConfigJson)
	if err != nil {
		return &pluginv1.ValidateConfigResponse{Message: err.Error()}, nil
	}
	return &pluginv1.ValidateConfigResponse{Valid: true, Message: "configuration valid", NormalizedConfigJson: []byte(jsonText(c))}, nil
}
func (e *Engine) ApplyConfig(_ context.Context, r *pluginv1.ApplyConfigRequest) (*pluginv1.ApplyConfigResponse, error) {
	if r == nil {
		r = &pluginv1.ApplyConfigRequest{}
	}
	c, err := ParseConfig(r.ConfigJson)
	if err != nil {
		return &pluginv1.ApplyConfigResponse{Message: err.Error()}, nil
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return &pluginv1.ApplyConfigResponse{Message: "plugin stopped"}, nil
	}
	if jsonText(c) == jsonText(e.config) {
		e.mu.Unlock()
		return &pluginv1.ApplyConfigResponse{Applied: true, Message: "configuration unchanged"}, nil
	}
	e.generationCancel()
	e.generationCtx, e.generationCancel = context.WithCancel(e.ctx)
	e.generation++
	if e.testedProxy != nil && e.testedProxy.ConfigFingerprint != networkFingerprint(c) {
		e.testedProxy = nil
	}
	e.config = c
	e.activeAfter = time.Now().Add(e.warmup)
	e.jobs = map[string]uint64{}
	e.forceRefresh = map[string]bool{}
	e.records = map[string]*jobRecord{}
	// Remove memory entries no longer matching configuration. Persisted entries are
	// keyed by fingerprint and expire naturally; switching configuration cannot use them.
	for k, t := range e.tickets {
		a, ok := findAccount(c, t.AccountID)
		if !ok || !contains(effectiveModels(c, a), t.Model) || t.ConfigFingerprint != configFingerprint(c, a, t.Model) {
			delete(e.tickets, k)
		}
	}
	e.mu.Unlock()
	e.clients.Close()
	e.notify()
	return &pluginv1.ApplyConfigResponse{Applied: true, Message: "configuration applied"}, nil
}
func (e *Engine) TestConfig(_ context.Context, r *pluginv1.TestConfigRequest) (*pluginv1.TestConfigResponse, error) {
	if r == nil {
		r = &pluginv1.TestConfigRequest{}
	}
	c, err := ParseConfig(r.ConfigJson)
	if err != nil {
		return &pluginv1.TestConfigResponse{Message: err.Error()}, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.snapshotLocked(time.Now())
	result := &pluginv1.TestConfigResponse{StatusJson: jsonText(s)}
	if jsonText(c) != jsonText(e.config) {
		result.Message = "save configuration before checking"
		return result, nil
	}
	if !e.hostReady {
		result.Message = "host services not ready"
		return result, nil
	}
	if c.Enabled {
		if frontProxyMode(c) == "managed" {
			found := false
			if e.resources != nil {
				for _, p := range e.resources.Proxies {
					if p.Id == c.HarvestDialProxyID {
						found = true
					}
				}
			}
			if !found {
				result.Message = "选中的前置代理不可用，请检查 IP 管理或宿主适配"
				return result, nil
			}
		}
		for _, a := range c.Accounts {
			if a.Enabled && !e.directory[a.AccountID] {
				result.Message = "an enabled account is not in the host account directory"
				return result, nil
			}
		}
	}
	result.Success = true
	result.Message = "saved configuration and cached host account bindings are valid; no proxy or model request was sent"
	return result, nil
}
func findAccount(c Config, id int64) (AccountConfig, bool) {
	for _, a := range c.Accounts {
		if a.AccountID == id {
			return a, true
		}
	}
	return AccountConfig{}, false
}
func contains(a []string, s string) bool {
	for _, x := range a {
		if x == s {
			return true
		}
	}
	return false
}
func keyFor(id int64, model string) string { return fmt.Sprintf("%d.%s", id, model) }
func kvKey(id int64, model, fp string) string {
	return fmt.Sprintf("ticket.%d.%s", id, digest(model, fp))
}
func proxyFingerprint(raw string) string { return digest("business-proxy-v1", strings.TrimSpace(raw)) }
func (e *Engine) accountEnabled(start *pluginv1.ForwardRequestStart) bool {
	if start == nil || start.Platform != "openai" || start.AccountType != "oauth" {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	a, ok := findAccount(e.config, start.AccountId)
	return !e.closed && e.config.Enabled && ok && a.Enabled
}
func (e *Engine) ticketForRequest(_ context.Context, start *pluginv1.ForwardRequestStart, model string) (*receipt, error) {
	if start == nil || start.Platform != "openai" || start.AccountType != "oauth" {
		return nil, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	a, ok := findAccount(e.config, start.AccountId)
	if !e.config.Enabled || !ok || !a.Enabled {
		return nil, nil
	}
	model = effectiveModel(e.config, model)
	if model == "" {
		if e.config.AllowWithoutTicket {
			return nil, nil
		}
		return nil, errors.New("configured account request has no inspectable model")
	}
	if !contains(effectiveModels(e.config, a), model) {
		return nil, nil
	}
	k := keyFor(start.AccountId, model)
	t := e.tickets[k]
	if t != nil && (t.FixedFingerprint != proxyFingerprint(start.ProxyUrl) || t.IdentityFingerprint != stableHeaders(start.AccountId, start.Headers)) {
		delete(e.tickets, k)
		t = nil
	}

	if !e.closed && e.hostReady && e.directory[start.AccountId] && validTicket(t, e.config, a, model, time.Now()) && t.FixedFingerprint == proxyFingerprint(start.ProxyUrl) && t.IdentityFingerprint == stableHeaders(start.AccountId, start.Headers) && e.revoked[k] != t.Version {
		return &receipt{State: t.State, Version: t.Version, Key: k, ConfigFingerprint: t.ConfigFingerprint, Generation: e.generation}, nil
	}
	e.notify()
	if e.config.AllowWithoutTicket {
		return nil, nil
	}
	return nil, errors.New("verified STATE unavailable; acquisition is running in the background")
}
func validTicket(t *ticket, c Config, a AccountConfig, model string, now time.Time) bool {
	return t != nil && t.AccountID == a.AccountID && t.Model == model && t.Plan == a.Plan && t.ConfigFingerprint == configFingerprint(c, a, model) && validState(t.State, targetLength(a.Plan)) && t.Version != "" && t.FixedFingerprint != "" && t.IdentityFingerprint != "" && !t.CapturedAt.IsZero() && !t.CapturedAt.After(now.Add(time.Minute)) && t.ExpiresAt.After(t.CapturedAt) && t.ExpiresAt.Sub(t.CapturedAt) <= time.Duration(c.TTLMinutes)*time.Minute && now.Before(t.ExpiresAt)
}
func (e *Engine) invalidate(r *receipt, reason string) {
	if r == nil || (reason != "model_mismatch" && reason != "state_312") {
		return
	}
	e.mu.Lock()
	t := e.tickets[r.Key]
	if e.closed || !e.config.Enabled || e.generation != r.Generation || t == nil || t.Version != r.Version || t.ConfigFingerprint != r.ConfigFingerprint {
		e.mu.Unlock()
		return
	}
	delete(e.tickets, r.Key)
	e.revoked[r.Key] = r.Version
	rec := e.records[r.Key]
	if rec == nil {
		rec = &jobRecord{}
		e.records[r.Key] = rec
	}
	rec.LastError = reason
	e.eventLocked(activityEvent{AccountID: t.AccountID, Model: t.Model, Phase: "watchdog", Result: reason})
	host := e.host
	e.mu.Unlock()
	e.notify()
	if host == nil {
		return
	}
	// Serialize persistence mutations without holding the business/status mutex.
	// A newly committed ticket wins over an old response waiting to delete KV.
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	e.mu.Lock()
	current := e.tickets[r.Key]
	newer := current != nil && current.Version != r.Version
	e.mu.Unlock()
	if newer {
		return
	}
	ctx, cancel := context.WithTimeout(e.ctx, 2*time.Second)
	_, err := host.KVDelete(ctx, &pluginv1.KVDeleteRequest{Namespace: namespace, Key: kvKey(t.AccountID, t.Model, t.ConfigFingerprint)})
	cancel()
	if err != nil {
		e.mu.Lock()
		if rec := e.records[r.Key]; rec != nil && e.tickets[r.Key] == nil {
			rec.LastError = reason + "_persistence_failed"
		}
		e.mu.Unlock()
	}
}

func (e *Engine) notify() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}
func (e *Engine) snapshotLocked(now time.Time) statusSnapshot {
	s := statusSnapshot{HostReady: e.hostReady, AccountIDs: []int64{}, Tickets: []statusTicket{}, Message: "STATE disabled; requests use the account business proxy"}
	s.ActionsReady = e.resources != nil && e.resources.ActionsSupported
	if e.manual != nil {
		copy := *e.manual
		s.ManualTest = &copy
	}
	s.Events = append([]activityEvent{}, e.events...)
	if e.resources != nil {
		s.ResourcesReady = true
		s.Accounts = e.resources.Accounts
		s.Proxies = e.resources.Proxies
	}
	for id := range e.directory {
		s.AccountIDs = append(s.AccountIDs, id)
	}
	sort.Slice(s.AccountIDs, func(i, j int) bool { return s.AccountIDs[i] < s.AccountIDs[j] })
	if e.config.Enabled {
		s.Message = "STATE active only for explicitly enabled account/model pairs"
	}
	if !e.hostReady {
		s.Message = "waiting for host services"
	}
	if e.directoryError != "" {
		s.Message = e.directoryError
	}
	for _, a := range e.config.Accounts {
		for _, model := range effectiveModels(e.config, a) {
			k := keyFor(a.AccountID, model)
			t := e.tickets[k]
			r := e.records[k]
			row := statusTicket{AccountID: a.AccountID, Model: model, Plan: a.Plan, State: "queued"}
			valid := validTicket(t, e.config, a, model, now)
			if t != nil {
				row.ExpiresAt = t.ExpiresAt.UTC().Format(time.RFC3339)
				if valid {
					row.RemainingSeconds = int64(t.ExpiresAt.Sub(now).Seconds())
				}
			}
			if r != nil {
				row.Attempts = r.Attempts
				row.LastError = r.LastError
				row.Trigger = r.Trigger
				row.ExitIP = r.ExitIP
				row.CountryCode = r.CountryCode
			}
			switch {
			case !e.config.Enabled || !a.Enabled:
				row.State = "disabled"
			case !e.hostReady:
				row.State = "waiting_host"
			case !e.directory[a.AccountID]:
				row.State = "waiting_account"
			case e.jobs[k] == e.generation && e.generation != 0:
				if r != nil && strings.HasPrefix(r.LastError, "checking_") {
					row.State = "checking_proxy"
					break
				}
				if valid {
					row.State = "renewing"
				} else {
					row.State = "harvesting"
				}
			case valid:
				row.State = "ready"
			case r != nil && r.CooldownUntil.After(now):
				row.State = "cooldown"
			case t != nil:
				row.State = "expired"
			case !e.config.AutoHarvest:
				row.State = "manual_idle"
			}
			row.Status = row.State
			s.Tickets = append(s.Tickets, row)
		}
	}
	return s
}
