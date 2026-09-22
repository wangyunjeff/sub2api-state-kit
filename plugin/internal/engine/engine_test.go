package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pluginv1 "github.com/wangyunjeff/sub2api-state-kit/plugin/internal/pluginapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type fakeHost struct {
	pluginv1.HostServiceClient // optional resource RPCs overridden below for stock-host fallback

	mu                            sync.Mutex
	values                        map[string][]byte
	accounts                      map[int64]*pluginv1.ResolveOutboundIdentityResponse
	gets, sets, deletes, resolves int
}

func testHost(ids ...int64) *fakeHost {
	h := &fakeHost{values: map[string][]byte{}, accounts: map[int64]*pluginv1.ResolveOutboundIdentityResponse{}}
	for _, id := range ids {
		h.accounts[id] = &pluginv1.ResolveOutboundIdentityResponse{Found: true, AccountId: id, Platform: "openai", AccountType: "oauth", Token: fmt.Sprintf("test-token-%d", id), Headers: map[string]*pluginv1.HeaderValues{"Chatgpt-Account-Id": {Values: []string{fmt.Sprint(id)}}}}
	}
	return h
}

func TestForceModelConfigAndRewrite(t *testing.T) {
	c, err := ParseConfig([]byte(`{"force_model":"gpt-6-astra","accounts":[{"account_id":1,"models":["gpt-5.6-luna"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := effectiveModels(c, c.Accounts[0]); len(got) != 1 || got[0] != "gpt-6-astra" {
		t.Fatalf("effective models = %#v", got)
	}
	data, err := rewriteModelField([]byte(`{"model":"gpt-5.6-luna","input":[]}`), "gpt-6-astra")
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["model"] != "gpt-6-astra" {
		t.Fatalf("rewritten model = %#v", payload["model"])
	}
}
func (h *fakeHost) KVGet(_ context.Context, r *pluginv1.KVGetRequest, _ ...grpc.CallOption) (*pluginv1.KVGetResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gets++
	b, ok := h.values[r.Namespace+"/"+r.Key]
	return &pluginv1.KVGetResponse{Found: ok, Value: append([]byte(nil), b...)}, nil
}
func (h *fakeHost) KVSet(_ context.Context, r *pluginv1.KVSetRequest, _ ...grpc.CallOption) (*pluginv1.KVSetResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sets++
	h.values[r.Namespace+"/"+r.Key] = append([]byte(nil), r.Value...)
	return &pluginv1.KVSetResponse{}, nil
}
func (h *fakeHost) KVDelete(_ context.Context, r *pluginv1.KVDeleteRequest, _ ...grpc.CallOption) (*pluginv1.KVDeleteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deletes++
	delete(h.values, r.Namespace+"/"+r.Key)
	return &pluginv1.KVDeleteResponse{}, nil
}
func (h *fakeHost) KVList(context.Context, *pluginv1.KVListRequest, ...grpc.CallOption) (*pluginv1.KVListResponse, error) {
	return &pluginv1.KVListResponse{}, nil
}
func (h *fakeHost) ListAccounts(context.Context, *pluginv1.ListAccountsRequest, ...grpc.CallOption) (*pluginv1.ListAccountsResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := &pluginv1.ListAccountsResponse{}
	for id := range h.accounts {
		r.AccountIds = append(r.AccountIds, id)
	}
	return r, nil
}
func (h *fakeHost) ResolveOutboundIdentity(_ context.Context, r *pluginv1.ResolveOutboundIdentityRequest, _ ...grpc.CallOption) (*pluginv1.ResolveOutboundIdentityResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resolves++
	original := h.accounts[r.AccountId]
	if original == nil {
		return &pluginv1.ResolveOutboundIdentityResponse{}, nil
	}
	return proto.Clone(original).(*pluginv1.ResolveOutboundIdentityResponse), nil
}
func testState(n int) string { return "gAAAAA" + strings.Repeat("A", n-6) }
func completed(w http.ResponseWriter, model string) {
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprintf(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"model\":%q}}\n\n", model)
}
func testConfig(proxy string, ids ...int64) Config {
	c := DefaultConfig()
	c.AllowWithoutTicket = false
	c.Enabled = true
	c.DynamicProxyURL = proxy
	c.MaxAttempts = 1
	for _, id := range ids {
		c.Accounts = append(c.Accounts, AccountConfig{AccountID: id, Enabled: true, Plan: "pro", Models: []string{"gpt-6-astra"}})
	}
	return c
}
func testEngine(t *testing.T, h *fakeHost, url string) *Engine {
	t.Helper()
	e := newEngine(h, url, 5*time.Millisecond)
	e.mu.Lock()
	e.warmup = 0
	e.mu.Unlock()
	t.Cleanup(e.Close)
	return e
}
func apply(t *testing.T, e *Engine, c Config) {
	t.Helper()
	r, err := e.ApplyConfig(context.Background(), &pluginv1.ApplyConfigRequest{ConfigJson: []byte(jsonText(c))})
	if err != nil || !r.Applied {
		t.Fatalf("apply: %v %+v", err, r)
	}
}
func waitFor(t *testing.T, e *Engine, state string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		r, _ := e.Health(context.Background(), &pluginv1.HealthRequest{})
		var s statusSnapshot
		json.Unmarshal([]byte(r.StatusJson), &s)
		if len(s.Tickets) > 0 && s.Tickets[0].State == state {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	r, _ := e.Health(context.Background(), &pluginv1.HealthRequest{})
	t.Fatalf("waiting %s: %s", state, r.StatusJson)
}
func testStart(id int64) *pluginv1.ForwardRequestStart {
	return &pluginv1.ForwardRequestStart{AccountId: id, Platform: "openai", AccountType: "oauth", Headers: map[string]*pluginv1.HeaderValues{"Chatgpt-Account-Id": {Values: []string{fmt.Sprint(id)}}}}
}

func TestConfigStrictIsolation(t *testing.T) {
	c, err := ParseConfig([]byte(`{}`))
	if err != nil || c.Enabled || c.TTLMinutes != 60 || len(c.Accounts) != 0 {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	bad := []string{`{"unknown":true}`, `null`, `{"ttl_minutes":61}`, `{"ttl_minutes":5,"refresh_before_minutes":5}`, `{"max_attempts":33}`, `{"enabled":true,"accounts":[{"account_id":1,"enabled":true}]}`, `{"accounts":[{"account_id":1},{"account_id":1}]}`, `{"accounts":[{"account_id":1,"models":["gpt-6-astra","gpt-6-astra"]}]}`, `{"dynamic_proxy_url":"file:///tmp/a"}`, `{"dynamic_proxy_url":"http://host/secret?token=x"}`, `{"accounts":[{"account_id":1,"plan":"wrong"}]}`}
	for _, raw := range bad {
		if _, err := ParseConfig([]byte(raw)); err == nil {
			t.Errorf("accepted invalid config %s", raw)
		}
	}
	c2, _ := ParseConfig([]byte(`{"accounts":[{"account_id":1}]}`))
	c2.Accounts[0].Models[0] = "gpt-other"
	c3, _ := ParseConfig([]byte(`{"accounts":[{"account_id":1}]}`))
	if c3.Accounts[0].Models[0] != "gpt-6-astra" {
		t.Fatal("default slices shared")
	}
}
func TestCollectFixedProxyValidationAndPersistence(t *testing.T) {
	h := testHost(42)
	var dynamic, fixed atomic.Int32
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dynamic.Add(1)
		if r.Header.Get("Authorization") != "Bearer test-token-42" || r.Header.Get(StateHeader) != "" {
			t.Error("dynamic identity or STATE incorrect")
		}
		w.Header().Set(StateHeader, testState(292))
		completed(w, "gpt-6-astra")
	}))
	defer pool.Close()
	business := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixed.Add(1)
		if r.Header.Get(StateHeader) != testState(292) || r.Header.Get("Authorization") != "Bearer test-token-42" {
			t.Error("fixed validation lost identity/state")
		}
		completed(w, "gpt-6-astra")
	}))
	defer business.Close()
	e := testEngine(t, h, business.URL)
	c := testConfig(pool.URL, 42)
	apply(t, e, c)
	waitFor(t, e, "ready")
	if dynamic.Load() != 1 || fixed.Load() != 1 {
		t.Fatalf("requests dynamic=%d fixed=%d", dynamic.Load(), fixed.Load())
	}
	r, err := e.ticketForRequest(context.Background(), testStart(42), "gpt-6-astra")
	if err != nil || r == nil {
		t.Fatalf("ticket missing %v", err)
	}
	if r, err = e.ticketForRequest(context.Background(), testStart(99), "gpt-6-astra"); err != nil || r != nil {
		t.Fatal("unlisted account not isolated")
	}
	if r, err = e.ticketForRequest(context.Background(), testStart(42), "gpt-5.6-sol"); err != nil || r != nil {
		t.Fatal("untargeted model not isolated")
	}
	start := testStart(42)
	start.ProxyUrl = "http://127.0.0.1:9"
	if _, err = e.ticketForRequest(context.Background(), start, "gpt-6-astra"); err == nil {
		t.Fatal("ticket reused on changed fixed proxy")
	}
	start = testStart(42)
	start.Headers["Chatgpt-Account-Id"].Values = []string{"another-account"}
	if _, err = e.ticketForRequest(context.Background(), start, "gpt-6-astra"); err == nil {
		t.Fatal("ticket reused on changed account identity")
	}
	health, _ := e.Health(context.Background(), &pluginv1.HealthRequest{})
	if strings.Contains(health.StatusJson, testState(292)) || strings.Contains(health.StatusJson, "test-token") {
		t.Fatal("secret in health")
	}
	h.mu.Lock()
	for _, value := range h.values {
		if strings.Contains(string(value), "test-token") || strings.Contains(string(value), pool.URL) {
			t.Error("credentials stored with ticket")
		}
	}
	h.mu.Unlock()
	apply(t, e, c)
	if dynamic.Load() != 1 {
		t.Fatal("identical config started duplicate harvest")
	}
	// Recreate engine using the same host KV. Restoration revalidates on fixed IP
	// and does not request another dynamic IP.
	e.Close()
	e2 := testEngine(t, h, business.URL)
	apply(t, e2, c)
	waitFor(t, e2, "ready")
	if dynamic.Load() != 1 || fixed.Load() != 2 {
		t.Fatalf("restart did not revalidate KV ticket: dynamic=%d fixed=%d", dynamic.Load(), fixed.Load())
	}
}
func TestPlanLengthMismatchAndRoutingFailure(t *testing.T) {
	for _, tc := range []struct {
		name, plan, actual string
		length             int
		want               string
	}{
		{"team332", "team", "gpt-6-astra", 332, "ready"}, {"teamReject292", "team", "gpt-6-astra", 292, "cooldown"}, {"proReject332", "pro", "gpt-6-astra", 332, "cooldown"}, {"lengthAloneInsufficient", "pro", "gpt-5.6-luna", 292, "cooldown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHost(42)
			pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(StateHeader, testState(tc.length))
				completed(w, tc.actual)
			}))
			defer pool.Close()
			business := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { completed(w, "gpt-6-astra") }))
			defer business.Close()
			e := testEngine(t, h, business.URL)
			c := testConfig(pool.URL, 42)
			c.Accounts[0].Plan = tc.plan
			apply(t, e, c)
			waitFor(t, e, tc.want)
		})
	}
}
func TestAuthAndRateLimitStopRound(t *testing.T) {
	for _, code := range []int{401, 403, 429} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			h := testHost(42)
			var calls atomic.Int32
			pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(code) }))
			defer pool.Close()
			e := testEngine(t, h, pool.URL)
			c := testConfig(pool.URL, 42)
			c.MaxAttempts = 8
			apply(t, e, c)
			waitFor(t, e, "cooldown")
			if calls.Load() != 1 {
				t.Fatalf("did not stop: %d", calls.Load())
			}
		})
	}
}
func TestFailedRenewalRetainsTicketAndLateWatchdogCannotRevokeNew(t *testing.T) {
	h := testHost(42)
	var fail atomic.Bool
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(429)
			return
		}
		w.Header().Set(StateHeader, testState(292))
		completed(w, "gpt-6-astra")
	}))
	defer pool.Close()
	business := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { completed(w, "gpt-6-astra") }))
	defer business.Close()
	e := testEngine(t, h, business.URL)
	c := testConfig(pool.URL, 42)
	apply(t, e, c)
	waitFor(t, e, "ready")
	old, _ := e.ticketForRequest(context.Background(), testStart(42), "gpt-6-astra")
	fail.Store(true)
	e.mu.Lock()
	current := e.tickets[keyFor(42, "gpt-6-astra")]
	current.ExpiresAt = time.Now().Add(5 * time.Minute)
	e.mu.Unlock()
	e.notify()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		r := e.records[old.Key]
		failed := r != nil && r.LastError == "upstream_rate_limited"
		e.mu.Unlock()
		if failed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, err := e.ticketForRequest(context.Background(), testStart(42), "gpt-6-astra")
	if err != nil || got.Version != old.Version {
		t.Fatal("failed renewal discarded still-valid ticket")
	}
	e.mu.Lock()
	replacement := *e.tickets[old.Key]
	replacement.Version = "new-version"
	e.tickets[old.Key] = &replacement
	e.mu.Unlock()
	e.invalidate(old, "model_mismatch")
	got, err = e.ticketForRequest(context.Background(), testStart(42), "gpt-6-astra")
	if err != nil || got.Version != "new-version" {
		t.Fatal("late response revoked a new ticket")
	}
	e.invalidate(got, "state_312")
	if _, err = e.ticketForRequest(context.Background(), testStart(42), "gpt-6-astra"); err == nil {
		t.Fatal("current rejected ticket still available")
	}
}
func TestApplyCancellationAndPassiveHealth(t *testing.T) {
	h := testHost(42)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set(StateHeader, testState(292))
		completed(w, "gpt-6-astra")
	}))
	defer pool.Close()
	e := testEngine(t, h, pool.URL)
	c := testConfig(pool.URL, 42)
	apply(t, e, c)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("harvest not started")
	}
	c.Enabled = false
	apply(t, e, c)
	close(release)
	time.Sleep(25 * time.Millisecond)
	e.mu.Lock()
	if len(e.tickets) != 0 {
		t.Error("old config job committed after disable")
	}
	e.mu.Unlock()
	h.mu.Lock()
	before := h.resolves
	h.mu.Unlock()
	for i := 0; i < 10; i++ {
		e.Health(context.Background(), &pluginv1.HealthRequest{})
	}
	h.mu.Lock()
	after := h.resolves
	h.mu.Unlock()
	if before != after {
		t.Fatal("Health caused credential reads")
	}
	if r, err := e.ticketForRequest(context.Background(), testStart(42), "gpt-6-astra"); r != nil || err != nil {
		t.Fatal("disabled account did not pass through")
	}
}
func TestProxySessionRotation(t *testing.T) {
	raw := "socks5h://example-region-Rand-sid-old-t-5:fake@us.1024proxy.io:3000"
	a, _ := rotateProxy(raw)
	b, _ := rotateProxy(raw)
	if a == b || strings.Contains(a, "sid-old") {
		t.Fatal("1024 session not rotated")
	}
	a, err := rotateProxy("http://user-{sid}:fake-{random}@127.0.0.1:3128")
	if err != nil || strings.Contains(a, "{") {
		t.Fatal("template not expanded")
	}
}

func TestFixedProxyChangeTriggersCollectionBeforeExpiry(t *testing.T) {
	h := testHost(42)
	var dynamic atomic.Int32
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dynamic.Add(1)
		w.Header().Set(StateHeader, testState(292))
		completed(w, "gpt-6-astra")
	}))
	defer pool.Close()
	business := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { completed(w, "gpt-6-astra") }))
	defer business.Close()
	newFixed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(StateHeader) != testState(292) {
			t.Error("new fixed proxy validation lacks state")
		}
		completed(w, "gpt-6-astra")
	}))
	defer newFixed.Close()
	e := testEngine(t, h, business.URL)
	c := testConfig(pool.URL, 42)
	apply(t, e, c)
	waitFor(t, e, "ready")
	h.mu.Lock()
	h.accounts[42].ProxyUrl = newFixed.URL
	h.mu.Unlock()
	start := testStart(42)
	start.ProxyUrl = newFixed.URL
	if _, err := e.ticketForRequest(context.Background(), start, "gpt-6-astra"); err == nil {
		t.Fatal("changed fixed proxy used old ticket")
	}
	waitFor(t, e, "ready")
	if dynamic.Load() != 2 {
		t.Fatalf("did not immediately recollect for changed binding: %d", dynamic.Load())
	}
	if _, err := e.ticketForRequest(context.Background(), start, "gpt-6-astra"); err != nil {
		t.Fatalf("new fixed proxy ticket unavailable: %v", err)
	}
}

func TestFixedRevalidationRejectsRoutingAnd312(t *testing.T) {
	for _, mode := range []string{"mismatch", "312", "incomplete"} {
		t.Run(mode, func(t *testing.T) {
			h := testHost(42)
			pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(StateHeader, testState(292))
				completed(w, "gpt-6-astra")
			}))
			defer pool.Close()
			business := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "mismatch":
					completed(w, "gpt-5.6-luna")
				case "312":
					w.Header().Set(StateHeader, testState(312))
					completed(w, "gpt-6-astra")
				case "incomplete":
					fmt.Fprint(w, `data: {"type":"response.created","response":{"model":"gpt-6-astra"}}`)
				}
			}))
			defer business.Close()
			e := testEngine(t, h, business.URL)
			apply(t, e, testConfig(pool.URL, 42))
			waitFor(t, e, "cooldown")
			if _, err := e.ticketForRequest(context.Background(), testStart(42), "gpt-6-astra"); err == nil {
				t.Fatal("unverified candidate exposed")
			}
			h.mu.Lock()
			n := len(h.values)
			h.mu.Unlock()
			if n != 0 {
				t.Fatal("unverified candidate persisted")
			}
		})
	}
}

type slowKVHost struct {
	*fakeHost
	entered chan struct{}
}

func (h *slowKVHost) KVSet(ctx context.Context, r *pluginv1.KVSetRequest, opts ...grpc.CallOption) (*pluginv1.KVSetResponse, error) {
	close(h.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestSlowPersistenceDoesNotBlockHealthOrConfig(t *testing.T) {
	h := &slowKVHost{fakeHost: testHost(42), entered: make(chan struct{})}
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(StateHeader, testState(292))
		completed(w, "gpt-6-astra")
	}))
	defer pool.Close()
	business := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { completed(w, "gpt-6-astra") }))
	defer business.Close()
	e := newEngine(h, business.URL, 5*time.Millisecond)
	defer e.Close()
	e.mu.Lock()
	e.warmup = 0
	e.mu.Unlock()
	c := testConfig(pool.URL, 42)
	apply(t, e, c)
	select {
	case <-h.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("KV write not reached")
	}
	started := time.Now()
	e.Health(context.Background(), &pluginv1.HealthRequest{})
	c.Enabled = false
	apply(t, e, c)
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("slow KV held global business/status mutex")
	}
	time.Sleep(20 * time.Millisecond)
	e.mu.Lock()
	n := len(e.tickets)
	e.mu.Unlock()
	if n != 0 {
		t.Fatal("cancelled persistence committed stale ticket")
	}
}

func (h *fakeHost) ListResources(context.Context, *pluginv1.ListResourcesRequest, ...grpc.CallOption) (*pluginv1.ListResourcesResponse, error) {
	return nil, errors.New("unimplemented")
}
func (h *fakeHost) ResolveProxy(context.Context, *pluginv1.ResolveProxyRequest, ...grpc.CallOption) (*pluginv1.ResolveProxyResponse, error) {
	return nil, errors.New("unimplemented")
}
