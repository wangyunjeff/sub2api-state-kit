package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	pluginv1 "github.com/wangyunjeff/sub2api-state-kit/plugin/internal/pluginapi/v1"
)

var errProbeModelMismatch = errors.New("probe completed with another model")

func (e *Engine) loop() {
	defer close(e.done)
	timer := time.NewTicker(e.tick)
	defer timer.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-timer.C:
		case <-e.wake:
		}
		e.refreshDirectory()
		e.schedule()
	}
}
func (e *Engine) refreshDirectory() {
	e.mu.Lock()
	host := e.host
	due := time.Since(e.directoryAt) >= 30*time.Second
	e.mu.Unlock()
	if host == nil || !due {
		return
	}
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	res, err := host.ListAccounts(ctx, &pluginv1.ListAccountsRequest{Platform: "openai", AccountType: "oauth"})
	cancel()
	ctx, cancel = context.WithTimeout(e.ctx, 5*time.Second)
	resources, resourceErr := host.ListResources(ctx, &pluginv1.ListResourcesRequest{})
	cancel()
	if resourceErr != nil {
		resources = nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.directoryAt = time.Now()
	e.resources = resources
	if err != nil || res == nil {
		e.directory = map[int64]bool{}
		e.directoryError = "host account directory unavailable"
		return
	}
	d := map[int64]bool{}
	for _, id := range res.AccountIds {
		if id > 0 {
			d[id] = true
		}
	}
	e.directory = d
	e.directoryError = ""
}
func (e *Engine) schedule() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || !e.config.Enabled || !e.config.AutoHarvest || e.host == nil || time.Now().Before(e.activeAfter) {
		return
	}
	now := time.Now()
	for _, a := range e.config.Accounts {
		if !a.Enabled || !e.directory[a.AccountID] {
			continue
		}
		for _, model := range effectiveModels(e.config, a) {
			k := keyFor(a.AccountID, model)
			if _, running := e.jobs[k]; running {
				continue
			}
			r := e.records[k]
			if r != nil && r.CooldownUntil.After(now) {
				continue
			}
			t := e.tickets[k]
			if !e.forceRefresh[k] && validTicket(t, e.config, a, model, now) && t.ExpiresAt.Sub(now) > time.Duration(e.config.RefreshBeforeMinutes)*time.Minute {
				continue
			}
			force := e.forceRefresh[k]
			delete(e.forceRefresh, k)
			e.startCollectLocked(a, model, force, "automatic")
		}
	}
}

// Caller holds e.mu. Targeted manual starts never wake the all-account scheduler.
func (e *Engine) startCollectLocked(a AccountConfig, model string, force bool, trigger string) bool {
	k := keyFor(a.AccountID, model)
	if e.closed || !e.config.Enabled || !a.Enabled || !e.directory[a.AccountID] {
		return false
	}
	if _, running := e.jobs[k]; running {
		return false
	}
	if force {
		delete(e.records, k)
	}
	if e.records[k] == nil {
		e.records[k] = &jobRecord{}
	}
	e.records[k].Trigger = trigger
	e.records[k].RoundID = randomID()
	e.records[k].ExitIP = ""
	e.records[k].CountryCode = ""
	gen := e.generation
	e.jobs[k] = gen
	e.wg.Add(1)
	e.eventLocked(activityEvent{AccountID: a.AccountID, Model: model, Phase: "collection", Result: "started", Trigger: trigger, RoundID: e.records[k].RoundID})
	go e.collect(e.generationCtx, e.host, e.config, a, model, k, gen, force)
	return true
}

func (e *Engine) activeLocked(k string, gen uint64) bool {
	current, ok := e.jobs[k]
	return !e.closed && e.generation == gen && ok && current == gen
}
func (e *Engine) note(k string, gen uint64, attempt int, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.activeLocked(k, gen) {
		return
	}
	r := e.records[k]
	if r == nil {
		r = &jobRecord{}
		e.records[k] = r
	}
	if r.Attempts != attempt {
		r.ExitIP = ""
		r.CountryCode = ""
	}
	r.Attempts = attempt
	r.LastError = reason
}
func (e *Engine) collect(ctx context.Context, host pluginv1.HostServiceClient, c Config, a AccountConfig, model, k string, gen uint64, force bool) {
	defer e.wg.Done()
	success := false
	reason := "attempts_exhausted"
	defer func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if !e.activeLocked(k, gen) {
			return
		}
		event := activityEvent{AccountID: a.AccountID, Model: model, Phase: "collection", Result: reason}
		if success {
			event.Result = "ready"
		} else if ctx.Err() != nil {
			event.Result = "cancelled"
		}
		if r := e.records[k]; r != nil {
			event.Trigger = r.Trigger
			event.RoundID = r.RoundID
		}
		e.eventLocked(event)
		delete(e.jobs, k)
		r := e.records[k]
		if r == nil {
			r = &jobRecord{}
			e.records[k] = r
		}
		if success {
			r.CooldownUntil = time.Time{}
			r.LastError = ""
		} else if ctx.Err() == nil {
			r.LastError = reason
			r.CooldownUntil = time.Now().Add(time.Duration(c.CooldownSeconds) * time.Second)
		}
	}()
	select {
	case e.semaphore <- struct{}{}:
		defer func() { <-e.semaphore }()
	case <-ctx.Done():
		return
	}
	identity, err := resolveIdentity(ctx, host, a.AccountID)
	if err != nil {
		reason = "identity_unavailable"
		return
	}
	if force {
		e.note(k, gen, 0, "checking_business_proxy")
		outer, err := e.resolveBusinessFront(ctx, c, identity.ProxyUrl)
		if err != nil {
			reason = "managed_proxy_unavailable"
			return
		}
		client, err := harvestClient(identity.ProxyUrl, outer)
		if err != nil {
			reason = "business_connectivity_failed"
			return
		}
		ok, _, _ := e.checkProxyConnectivity(ctx, client, k, gen, a.AccountID, model, 0, "business", outer != "")
		client.CloseIdleConnections()
		if !ok {
			reason = "business_connectivity_failed"
			return
		}
	}
	fp := configFingerprint(c, a, model)
	// Restored tickets are revalidated through the current business proxy before
	// reuse. This also prevents a failed persistence deletion reviving a bad ticket.
	if !force {
		if restored, status := e.restore(ctx, host, c, a, model, k, gen, identity, fp); restored {
			success = true
			return
		} else if isStopStatus(status) {
			reason = stopReason(status)
			return
		}
	}
	for attempt := 1; attempt <= c.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}
		e.note(k, gen, attempt, "")
		if attempt > 1 {
			select {
			case <-time.After(time.Duration(c.AttemptIntervalSeconds) * time.Second):
			case <-ctx.Done():
				return
			}
		}
		identity, err = resolveIdentity(ctx, host, a.AccountID)
		if err != nil {
			reason = "identity_unavailable"
			return
		}
		rotating, err := rotateProxy(c.DynamicProxyURL)
		if err != nil {
			reason = "invalid_dynamic_proxy"
			return
		}
		captured := time.Now()
		candidate, status, err := e.observedProbeChecked(ctx, c, identity, model, rotating, "", k, gen, attempt, "harvest", force)
		if err != nil {
			reason = "harvest_failed"
			if errors.Is(err, errProbeModelMismatch) {
				reason = "model_mismatch"
			}
			if errors.Is(err, errConnectivityCheck) {
				reason = "dynamic_connectivity_failed"
				// A single rotating exit failing does not invalidate the pool.
				// Respect the configured attempt/interval bound and try another.
				continue
			}
			if errors.Is(err, errManagedProxyUnavailable) {
				reason = "managed_proxy_unavailable"
				return
			}
			if isStopStatus(status) {
				reason = stopReason(status)
				return
			}
			continue
		}
		if !validState(candidate, targetLength(a.Plan)) {
			reason = "unexpected_state_length"
			e.activity(k, gen, activityEvent{AccountID: a.AccountID, Model: model, Attempt: attempt, Phase: "harvest", Result: reason, StateBytes: len(candidate)})
			continue
		}
		// Resolve fresh credentials again, and validate using the host's current
		// business proxy. Dynamic proxy credentials are never copied into business.
		fixed, err := resolveIdentity(ctx, host, a.AccountID)
		if err != nil {
			reason = "identity_unavailable"
			return
		}
		if stableIdentity(identity) != stableIdentity(fixed) {
			reason = "identity_changed"
			continue
		}
		returned, status, err := e.observedProbe(ctx, c, fixed, model, fixed.ProxyUrl, candidate, k, gen, attempt, "validate")
		if err != nil || validState(returned, 312) {
			reason = "fixed_proxy_validation_failed"
			if isStopStatus(status) {
				reason = stopReason(status)
				return
			}
			continue
		}
		t := &ticket{AccountID: a.AccountID, Model: model, Plan: a.Plan, State: candidate, Version: randomID(), ConfigFingerprint: fp, FixedFingerprint: proxyFingerprint(fixed.ProxyUrl), IdentityFingerprint: stableIdentity(fixed), CapturedAt: captured, ExpiresAt: captured.Add(time.Duration(c.TTLMinutes) * time.Minute)}
		if e.commit(ctx, host, c, a, model, k, gen, t, true) {
			success = true
			return
		}
		reason = "ticket_persistence_failed"
		return
	}
}
func isStopStatus(status int) bool { return status == 401 || status == 403 || status == 429 }
func stopReason(status int) string {
	switch status {
	case 401:
		return "upstream_unauthorized"
	case 403:
		return "upstream_forbidden"
	case 429:
		return "upstream_rate_limited"
	}
	return "upstream_rejected"
}
func resolveIdentity(ctx context.Context, host pluginv1.HostServiceClient, id int64) (*pluginv1.ResolveOutboundIdentityResponse, error) {
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r, err := host.ResolveOutboundIdentity(c, &pluginv1.ResolveOutboundIdentityRequest{AccountId: id})
	if err != nil || r == nil || !r.Found || r.AccountId != id || r.Platform != "openai" || r.AccountType != "oauth" || r.Token == "" {
		return nil, errors.New("account identity unavailable")
	}
	if err = validateProxy(r.ProxyUrl); err != nil {
		return nil, errors.New("business proxy invalid")
	}
	return r, nil
}
func stableIdentity(r *pluginv1.ResolveOutboundIdentityResponse) string {
	if r == nil {
		return ""
	}
	return stableHeaders(r.AccountId, r.Headers)
}
func stableHeaders(id int64, headers map[string]*pluginv1.HeaderValues) string {
	var account string
	for k, v := range headers {
		if strings.EqualFold(k, "Chatgpt-Account-Id") && v != nil {
			account = strings.Join(v.Values, "\x00")
		}
	}
	return digest(fmt.Sprint(id), account)
}

func (e *Engine) restore(ctx context.Context, host pluginv1.HostServiceClient, c Config, a AccountConfig, model, k string, gen uint64, identity *pluginv1.ResolveOutboundIdentityResponse, fp string) (bool, int) {
	e.mu.Lock()
	current := e.tickets[k]
	revoked := e.revoked[k]
	e.mu.Unlock()
	if validTicket(current, c, a, model, time.Now()) {
		return false, 0
	} // Renewal keeps the old usable ticket throughout.
	cc, cancel := context.WithTimeout(ctx, 3*time.Second)
	r, err := host.KVGet(cc, &pluginv1.KVGetRequest{Namespace: namespace, Key: kvKey(a.AccountID, model, fp)})
	cancel()
	if err != nil || r == nil || !r.Found || len(r.Value) > 16*1024 {
		return false, 0
	}
	var t ticket
	if json.Unmarshal(r.Value, &t) != nil || !validTicket(&t, c, a, model, time.Now()) || t.Version == revoked || t.FixedFingerprint != proxyFingerprint(identity.ProxyUrl) || t.IdentityFingerprint != stableIdentity(identity) {
		return false, 0
	}
	returned, status, err := e.observedProbe(ctx, c, identity, model, identity.ProxyUrl, t.State, k, gen, 0, "restore")
	if err != nil || validState(returned, 312) {
		return false, status
	}
	return e.commit(ctx, host, c, a, model, k, gen, &t, false), status
}
func (e *Engine) commit(ctx context.Context, host pluginv1.HostServiceClient, c Config, a AccountConfig, model, k string, gen uint64, t *ticket, persist bool) bool {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	valid := func() bool {
		return e.activeLocked(k, gen) && ctx.Err() == nil && e.directory[a.AccountID] && validTicket(t, c, a, model, time.Now()) && e.revoked[k] != t.Version
	}
	e.mu.Lock()
	ok := valid()
	e.mu.Unlock()
	if !ok {
		return false
	}
	if persist {
		cc, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := host.KVSet(cc, &pluginv1.KVSetRequest{Namespace: namespace, Key: kvKey(a.AccountID, model, t.ConfigFingerprint), Value: []byte(jsonText(t)), TtlSeconds: max(1, int64(time.Until(t.ExpiresAt).Seconds()))})
		cancel()
		if err != nil {
			return false
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !valid() {
		return false
	}
	e.tickets[k] = t
	delete(e.revoked, k)
	return true
}

func (e *Engine) probeWithClient(ctx context.Context, identity *pluginv1.ResolveOutboundIdentityResponse, model, state string, client *http.Client, detail *probeDetail) (string, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	payload := map[string]any{"model": model, "store": false, "stream": true, "instructions": "Reply with exactly: pong", "input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_text", "text": "ping"}}}}}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.probeURL, bytes.NewReader(body))
	if err != nil {
		return "", 0, errors.New("probe construction failed")
	}
	for name, values := range identity.Headers {
		if values == nil {
			continue
		}
		for _, v := range values.Values {
			req.Header.Add(name, v)
		}
	}
	req.Header.Set("Authorization", "Bearer "+identity.Token)
	req.Header.Del(StateHeader)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("OpenAI-Beta", "responses=experimental")
	req.Header.Set("session_id", randomID())
	req.Header.Set("version", "0.153.4")
	req.Header.Set("User-Agent", "codex_cli_rs/0.153.4")
	req.Header.Set("originator", "codex_cli_rs")
	if state != "" {
		req.Header.Set(StateHeader, state)
	}
	req.Close = true
	response, err := client.Do(req)
	if err != nil {
		detail.Result = transportCode(err)
		return "", 0, errors.New("probe transport failed")
	}
	defer response.Body.Close()
	status := response.StatusCode
	detail.StateBytes = len(strings.TrimSpace(response.Header.Get(StateHeader)))
	if status != http.StatusOK {
		detail.Result = "upstream_rejected"
		return "", status, errors.New("probe request rejected")
	}
	observer := newCompletionObserver(model)
	buf := make([]byte, 16*1024)
	total := 0
	for {
		n, readErr := response.Body.Read(buf)
		if n > 0 {
			total += n
			if total > 4*1024*1024 {
				return "", status, errors.New("probe response too large")
			}
			observer.Write(buf[:n])
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", status, errors.New("probe response interrupted")
		}
	}
	observer.Finish()
	complete, matches := observer.Result()
	detail.ActualModel = observer.actual
	detail.Result = "model_matched"
	if !complete || !matches {
		detail.Result = "model_mismatch"
		if !complete {
			detail.Result = "incomplete_response"
			return "", status, errors.New("probe did not complete")
		}
		return "", status, errProbeModelMismatch
	}
	return strings.TrimSpace(response.Header.Get(StateHeader)), status, nil
}

var sidPattern = regexp.MustCompile(`(?i)(-sid-)[^-]+`)

func rotateProxy(raw string) (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(900000))
	if err != nil {
		return "", errors.New("random session unavailable")
	}
	sid := fmt.Sprint(n.Int64() + 100000)
	expanded := strings.NewReplacer("{sid}", sid, "{random}", sid).Replace(raw)
	u, err := url.Parse(expanded)
	if err != nil {
		return "", errors.New("invalid proxy")
	}
	// 1024proxy's sticky-session username is explicitly rotated once per attempt.
	if (strings.HasSuffix(strings.ToLower(u.Hostname()), ".1024proxy.io") || strings.EqualFold(u.Hostname(), "1024proxy.io")) && u.User != nil {
		name := u.User.Username()
		if sidPattern.MatchString(name) {
			name = sidPattern.ReplaceAllString(name, "${1}"+sid)
		} else {
			name += "-sid-" + sid
		}
		if pass, ok := u.User.Password(); ok {
			u.User = url.UserPassword(name, pass)
		} else {
			u.User = url.User(name)
		}
	}
	if err = validateProxy(u.String()); err != nil {
		return "", err
	}
	return u.String(), nil
}
func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("cryptographic randomness unavailable")
	}
	return hex.EncodeToString(b[:])
}
