package dalgo2snowflake

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Client struct {
	endpoint     string
	options      Options
	transport    http.RoundTripper
	owned        *http.Transport
	mu           sync.Mutex
	closed       bool
	active       map[*operation]struct{}
	stateChanged chan struct{}
}

func name(s string) bool {
	return s != "" && len(s) <= 4096 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func bounded(v *int, def, max int) bool {
	if *v == 0 {
		*v = def
	}
	return *v > 0 && *v <= max
}

// New is lazy: no credential, network or schema calls occur here.
func New(endpoint string, options Options) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || !strings.HasSuffix(strings.ToLower(u.Hostname()), ".snowflakecomputing.com") {
		return nil, failure("config")
	}
	if !name(options.Database) || !name(options.Schema) || options.Token == nil || (options.Warehouse != "" && !name(options.Warehouse)) || (options.Role != "" && !name(options.Role)) || len(options.Tables) == 0 || len(options.Tables) > 128 {
		return nil, failure("config")
	}
	if options.Timeout == 0 {
		options.Timeout = 60 * time.Second
	}
	if options.Timeout <= 0 || options.Timeout > 120*time.Second {
		return nil, failure("config")
	}
	if !bounded(&options.MaxConcurrent, 4, 16) || !bounded(&options.MaxKeys, 100, 1000) || !bounded(&options.MaxRows, 10000, 100000) || !bounded(&options.MaxRequestBytes, 64*1024, 1024*1024) || !bounded(&options.MaxResponseBytes, 16*1024*1024, 32*1024*1024) || !bounded(&options.MaxPageBytes, 4*1024*1024, 16*1024*1024) || !bounded(&options.MaxRequests, 512, 2048) || !bounded(&options.MaxPartitions, 128, 1024) || !bounded(&options.MaxColumns, 512, 4096) || !bounded(&options.MaxNodes, 65536, 262144) || !bounded(&options.MaxPolls, 240, 1000) {
		return nil, failure("config")
	}
	if options.PollInterval == 0 && !options.ImmediatePolling {
		options.PollInterval = 250 * time.Millisecond
	}
	if options.PollInterval < 0 || options.PollInterval > options.Timeout {
		return nil, failure("config")
	}
	tables := make(map[string]Table, len(options.Tables))
	for alias, t := range options.Tables {
		if !name(alias) || !name(t.Name) || !name(t.IDColumn) {
			return nil, failure("config")
		}
		tables[alias] = t
	}
	options.Tables = tables
	var t *http.Transport
	if options.Transport != nil {
		t = options.Transport.Clone()
	} else {
		t = &http.Transport{DialContext: (&net.Dialer{Timeout: options.Timeout}).DialContext}
	}
	if t.TLSClientConfig == nil {
		t.TLSClientConfig = &tls.Config{}
	} else {
		t.TLSClientConfig = t.TLSClientConfig.Clone()
		if t.TLSClientConfig.RootCAs != nil {
			t.TLSClientConfig.RootCAs = t.TLSClientConfig.RootCAs.Clone()
		}
	}
	if t.DialTLS != nil || t.DialTLSContext != nil {
		return nil, failure("config")
	}
	if t.TLSClientConfig.InsecureSkipVerify || t.TLSClientConfig.ServerName != "" && !strings.EqualFold(t.TLSClientConfig.ServerName, u.Hostname()) {
		return nil, failure("config")
	}
	t.TLSClientConfig.MinVersion = max(t.TLSClientConfig.MinVersion, tls.VersionTLS12)
	t.TLSClientConfig.NextProtos = []string{"http/1.1"}
	t.Protocols = new(http.Protocols)
	t.Protocols.SetHTTP1(true)
	t.Proxy = nil
	t.DisableKeepAlives = true
	t.DisableCompression = true
	t.ForceAttemptHTTP2 = false
	t.MaxResponseHeaderBytes = 64 * 1024
	t.MaxConnsPerHost = options.MaxConcurrent * 2
	t.ResponseHeaderTimeout = options.Timeout
	t.TLSHandshakeTimeout = options.Timeout
	return &Client{endpoint: "https://" + u.Host, options: options, transport: t, owned: t, active: make(map[*operation]struct{}), stateChanged: make(chan struct{})}, nil
}

type operation struct {
	client                                             *Client
	parent, ctx                                        context.Context
	cancel                                             context.CancelFunc
	deadline                                           time.Time
	physical                                           int
	finished                                           bool
	requests, requestBytes, responseBytes, rows, nodes int
}

func (c *Client) begin(parent context.Context) (*operation, error) {
	if parent == nil {
		return nil, failure("input")
	}
	if err := safeContext(parent); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, c.options.Timeout)
	deadline, _ := ctx.Deadline()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		cancel()
		return nil, failure("closed")
	}
	if len(c.active) >= c.options.MaxConcurrent {
		cancel()
		return nil, failure("busy")
	}
	o := &operation{client: c, parent: parent, ctx: ctx, cancel: cancel, deadline: deadline}
	c.active[o] = struct{}{}
	return o, nil
}
func safeContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		if err == context.DeadlineExceeded {
			return context.DeadlineExceeded
		}
		return context.Canceled
	}
	return nil
}
func (o *operation) check() error {
	if err := safeContext(o.parent); err != nil {
		return err
	}
	if !time.Now().Before(o.deadline) {
		return context.DeadlineExceeded
	}
	o.client.mu.Lock()
	closed := o.client.closed
	o.client.mu.Unlock()
	if closed {
		return failure("closed")
	}
	return safeContext(o.ctx)
}
func (o *operation) releaseLocked() {
	if o.finished && o.physical == 0 {
		if _, present := o.client.active[o]; present {
			delete(o.client.active, o)
			// The empty snapshot and this generation are always observed under
			// the same mutex. Repeated finish calls cannot fabricate progress.
			close(o.client.stateChanged)
			o.client.stateChanged = make(chan struct{})
		}
	}
}
func (o *operation) track() { o.client.mu.Lock(); o.physical++; o.client.mu.Unlock() }
func (o *operation) settled() {
	o.client.mu.Lock()
	o.physical--
	o.releaseLocked()
	o.client.mu.Unlock()
}
func (o *operation) finish() {
	o.client.mu.Lock()
	o.finished = true
	o.releaseLocked()
	o.client.mu.Unlock()
	o.cancel()
}

type foreignResult[T any] struct {
	value T
	err   error
}

// Only adapter-owned staging can be passed here. Caller record/query callbacks
// stay inline. Ignored cancellation retains admission until physical settlement.
func foreign[T any](o *operation, ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	o.track()
	ch := make(chan foreignResult[T], 1)
	go func() { v, e := fn(); o.settled(); ch <- foreignResult[T]{v, e} }()
	select {
	case <-ctx.Done():
		return zero, safeContext(ctx)
	case r := <-ch:
		return r.value, r.err
	}
}
func (c *Client) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		for o := range c.active {
			o.cancel()
		}
	}
	c.mu.Unlock()
	c.owned.CloseIdleConnections()
	return nil
}
func (o *operation) auth() (Credential, error) {
	if e := o.check(); e != nil {
		return Credential{}, e
	}
	credential, e := foreign(o, o.ctx, func() (Credential, error) {
		if e := o.check(); e != nil {
			return Credential{}, e
		}
		return o.client.options.Token(o.ctx)
	})
	if final := o.check(); final != nil {
		return Credential{}, final
	}
	if e != nil {
		return Credential{}, failure("auth")
	}
	if (credential.kind != "OAUTH" && credential.kind != "KEYPAIR_JWT") || len(credential.raw) == 0 || len(credential.raw) > 16384 {
		return Credential{}, failure("auth")
	}
	for _, r := range credential.raw {
		if r < 33 || r > 126 {
			return Credential{}, failure("auth")
		}
	}
	return credential, nil
}
