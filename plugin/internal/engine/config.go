package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const PluginID = "io.github.wangyunjeff.sub2api-state-kit"
const Version = "0.3.4"
const StateHeader = "x-codex-turn-state"
const namespace = "state-kit-v1"

// Config contains no OAuth credentials. The host owns credential refresh.
type Config struct {
	ForceModel             string          `json:"force_model"`
	BusinessUseFront       bool            `json:"business_use_front"`
	AutoHarvest            bool            `json:"auto_harvest"`
	AllowWithoutTicket     bool            `json:"allow_without_ticket"`
	HarvestDialProxyMode   string          `json:"harvest_dial_proxy_mode"`
	HarvestDialProxyID     int64           `json:"harvest_dial_proxy_id"`
	HarvestDialProxyURL    string          `json:"harvest_dial_proxy_url"`
	ObserveExitIP          bool            `json:"observe_exit_ip"`
	Enabled                bool            `json:"enabled"`
	DynamicProxyURL        string          `json:"dynamic_proxy_url"`
	TTLMinutes             int             `json:"ttl_minutes"`
	RefreshBeforeMinutes   int             `json:"refresh_before_minutes"`
	MaxAttempts            int             `json:"max_attempts"`
	AttemptIntervalSeconds int             `json:"attempt_interval_seconds"`
	CooldownSeconds        int             `json:"cooldown_seconds"`
	Accounts               []AccountConfig `json:"accounts"`
}
type AccountConfig struct {
	AccountID int64    `json:"account_id"`
	Enabled   bool     `json:"enabled"`
	Plan      string   `json:"plan"`
	Models    []string `json:"models"`
}

func DefaultConfig() Config {
	return Config{AutoHarvest: true, AllowWithoutTicket: true, TTLMinutes: 60, RefreshBeforeMinutes: 10, MaxAttempts: 8, AttemptIntervalSeconds: 10, CooldownSeconds: 300, Accounts: []AccountConfig{}}
}

// effectiveModels returns the models that the plugin should collect and serve.
// A forced model replaces the account's requested model while preserving the
// user's account/model configuration for easy rollback.
func effectiveModels(c Config, a AccountConfig) []string {
	if c.ForceModel != "" {
		return []string{c.ForceModel}
	}
	return a.Models
}

func effectiveModel(c Config, requested string) string {
	if c.ForceModel != "" {
		return c.ForceModel
	}
	return requested
}

var modelPattern = regexp.MustCompile(`^gpt-[A-Za-z0-9][A-Za-z0-9._-]{0,94}$`)

// ParseConfig rejects unknown fields, trailing JSON, and invalid ranges without
// echoing user input (which can include authenticated proxy URLs).
func ParseConfig(raw []byte) (Config, error) {
	c := DefaultConfig()
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	if len(raw) > 1<<20 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return c, errors.New("configuration must be a JSON object under 1 MiB")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, errors.New("invalid configuration JSON or unknown field")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return c, errors.New("configuration has trailing JSON")
	}
	if c.ForceModel != "" && !modelPattern.MatchString(c.ForceModel) {
		return c, errors.New("force_model must be a valid gpt model ID")
	}
	c.DynamicProxyURL = strings.TrimSpace(c.DynamicProxyURL)
	c.HarvestDialProxyURL = strings.TrimSpace(c.HarvestDialProxyURL)
	c.HarvestDialProxyMode = frontProxyMode(c)
	switch c.HarvestDialProxyMode {
	case "direct":
		c.HarvestDialProxyURL = ""
		c.HarvestDialProxyID = 0
	case "manual":
		c.HarvestDialProxyID = 0
		if c.HarvestDialProxyURL == "" {
			return c, errors.New("manual front proxy URL is required")
		}
	case "managed":
		c.HarvestDialProxyURL = ""
		if c.HarvestDialProxyID <= 0 {
			return c, errors.New("managed front proxy ID must be positive")
		}
	default:
		return c, errors.New("invalid harvest_dial_proxy_mode")
	}
	if err := validateProxy(c.HarvestDialProxyURL); err != nil {
		return c, errors.New("invalid harvest_dial_proxy_url")
	}
	if strings.ContainsAny(c.HarvestDialProxyURL, "{}") {
		return c, errors.New("harvest_dial_proxy_url cannot contain placeholders")
	}
	if c.TTLMinutes < 1 || c.TTLMinutes > 60 {
		return c, errors.New("ttl_minutes must be 1..60")
	}
	if c.RefreshBeforeMinutes < 0 || c.RefreshBeforeMinutes >= c.TTLMinutes {
		return c, errors.New("refresh_before_minutes must be nonnegative and less than ttl_minutes")
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > 32 {
		return c, errors.New("max_attempts must be 1..32")
	}
	if c.AttemptIntervalSeconds < 1 || c.AttemptIntervalSeconds > 300 {
		return c, errors.New("attempt_interval_seconds must be 1..300")
	}
	if c.CooldownSeconds < 30 || c.CooldownSeconds > 3600 {
		return c, errors.New("cooldown_seconds must be 30..3600")
	}
	if err := validateProxy(c.DynamicProxyURL); err != nil {
		return c, err
	}
	if len(c.Accounts) > 256 {
		return c, errors.New("at most 256 accounts are supported")
	}
	if c.Accounts == nil {
		c.Accounts = []AccountConfig{}
	}
	seen := map[int64]bool{}
	anyEnabled := false
	total := 0
	for i := range c.Accounts {
		a := &c.Accounts[i]
		if a.AccountID <= 0 || seen[a.AccountID] {
			return c, errors.New("account_id must be positive and unique")
		}
		seen[a.AccountID] = true
		if a.Plan == "" {
			a.Plan = "pro"
		}
		if a.Plan != "pro" && a.Plan != "team" {
			return c, errors.New("plan must be pro or team")
		}
		if a.Models == nil {
			a.Models = []string{"gpt-6-astra"}
		}
		if len(a.Models) == 0 || len(a.Models) > 16 {
			return c, errors.New("each account must have 1..16 models")
		}
		models := map[string]bool{}
		for _, m := range a.Models {
			if !modelPattern.MatchString(m) || models[m] {
				return c, errors.New("models must contain unique exact gpt model IDs")
			}
			models[m] = true
		}
		total += len(a.Models)
		anyEnabled = anyEnabled || a.Enabled
	}
	if total > 1024 {
		return c, errors.New("at most 1024 account/model pairs are supported")
	}
	if c.Enabled && anyEnabled && c.DynamicProxyURL == "" {
		return c, errors.New("dynamic_proxy_url is required for enabled accounts")
	}
	return c, nil
}
func validateProxy(raw string) error {
	if raw == "" {
		return nil
	}
	if len(raw) > 4096 || strings.ContainsAny(raw, "\r\n\t") {
		return errors.New("invalid proxy URL")
	}
	// Placeholders are accepted in credentials/host and expanded before use.
	replaced := strings.NewReplacer("{sid}", "123456", "{random}", "123456").Replace(raw)
	if strings.ContainsAny(replaced, "{}") {
		return errors.New("only {sid} and {random} proxy placeholders are supported")
	}
	u, err := url.Parse(replaced)
	if err != nil || u.Hostname() == "" || u.Fragment != "" || u.RawQuery != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("invalid proxy URL")
	}
	if u.Port() != "" {
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 {
			return errors.New("proxy port must be 1..65535")
		}
	}
	switch u.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return errors.New("proxy scheme must be http, https, socks5, or socks5h")
	}
	return nil
}
func digest(parts ...string) string {
	h := sha256.New()
	for _, s := range parts {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
func configFingerprint(c Config, a AccountConfig, model string) string {
	dynamicRoute := c.DynamicProxyURL
	if c.BusinessUseFront {
		dynamicRoute = digest("business-front-v1", dynamicRoute)
	}
	if frontProxyMode(c) == "managed" {
		dynamicRoute = digest("managed-v1", dynamicRoute, strconv.FormatInt(c.HarvestDialProxyID, 10))
	} else if frontProxyMode(c) == "manual" {
		dynamicRoute = digest("chained-v1", dynamicRoute, c.HarvestDialProxyURL)
	}
	return digest("v2", dynamicRoute, c.ForceModel, a.Plan, model, jsonText(struct {
		ID  int64
		TTL int
	}{a.AccountID, c.TTLMinutes}))
}
func jsonText(v any) string { b, _ := json.Marshal(v); return string(b) }
func targetLength(plan string) int {
	if plan == "team" {
		return 332
	}
	return 292
}

// STATE is opaque: only token-safe bytes and expected length are checked.
func validState(s string, n int) bool {
	if len(s) != n || !strings.HasPrefix(s, "gAAAAA") {
		return false
	}
	for _, c := range s {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '=') {
			return false
		}
	}
	return true
}

// Configs created before 0.3.2 use an empty URL for direct and a URL for manual.
func frontProxyMode(c Config) string {
	if c.HarvestDialProxyMode != "" {
		return c.HarvestDialProxyMode
	}
	if c.HarvestDialProxyURL != "" {
		return "manual"
	}
	return "direct"
}
