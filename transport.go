package dalgo2snowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var handlePattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func handle(v any) (string, error) {
	s, ok := v.(string)
	if !ok || !handlePattern.MatchString(s) {
		return "", failure("protocol")
	}
	return strings.ToLower(s), nil
}

type statement struct {
	mu                   sync.Mutex
	handle               string
	credential           Credential
	completed, cancelled bool
}

func (s *statement) setHandle(v any) error {
	h, e := handle(v)
	if e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handle != "" && s.handle != h {
		return failure("protocol")
	}
	s.handle = h
	return nil
}
func (s *statement) id() string { s.mu.Lock(); defer s.mu.Unlock(); return s.handle }
func (s *statement) complete()  { s.mu.Lock(); s.completed = true; s.mu.Unlock() }

func (o *operation) cancelStatement(s *statement) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.completed || s.cancelled || s.handle == "" {
		s.mu.Unlock()
		return
	}
	s.cancelled = true
	id := s.handle
	credential := s.credential
	s.mu.Unlock()
	// Retain before returning: the ordinary slot cannot be bypassed while the
	// independent cleanup or an ignored physical cancel is still unsettled.
	o.track()
	go func() {
		defer o.settled()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = foreign(o, ctx, func() (httpReply, error) {
			reply, e := o.http(ctx, http.MethodPost, "/api/v2/statements/"+id+"/cancel", nil, credential, 16384, 4096, nil)
			if e != nil {
				return reply, e
			}
			if reply.status != 200 {
				return reply, failure("cancel-unknown")
			}
			confirmed, e := handle(reply.payload["statementHandle"])
			if e != nil || confirmed != id {
				return reply, failure("cancel-unknown")
			}
			return reply, nil
		})
		// Protocol confirmation is intentionally not a public success flag. Even
		// HTTP200/matching handle is not proof of remote termination/credit bounds.
	}()
}

type httpReply struct {
	status       int
	payload      map[string]any
	bytes, nodes int
}

// http owns only this request/response. The caller races this worker, so an
// uncooperative token/transport/read/Close cannot block the public deadline.
// Watcher and worker remain physically admitted until both actually settle.
func (o *operation) http(ctx context.Context, method, path string, body []byte, credential Credential, maxBytes, maxNodes int, pending *statement) (httpReply, error) {
	var result httpReply
	if e := safeContext(ctx); e != nil {
		return result, e
	}
	request, e := http.NewRequestWithContext(ctx, method, o.client.endpoint+path, bytes.NewReader(body))
	if e != nil {
		return result, failure("transport")
	}
	request.Header.Set("Authorization", "Bearer "+credential.raw)
	request.Header.Set("X-Snowflake-Authorization-Token-Type", credential.kind)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("User-Agent", "dalgo2snowflake/0.1.0")
	response, e := o.client.transport.RoundTrip(request)
	if e != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return result, failure("transport")
	}
	if response == nil || response.Body == nil {
		return result, failure("protocol")
	}
	var once sync.Once
	var closeErr error
	closeBody := func() { once.Do(func() { closeErr = response.Body.Close() }) }
	done := make(chan struct{})
	watchDone := make(chan struct{})
	o.track()
	go func() {
		defer o.settled()
		defer close(watchDone)
		select {
		case <-ctx.Done():
			o.cancelStatement(pending)
			closeBody()
		case <-done:
		}
	}()
	defer func() { close(done); <-watchDone }()
	defer closeBody()
	if e := safeContext(ctx); e != nil {
		o.cancelStatement(pending)
		return result, e
	}
	result.status = response.StatusCode
	if result.status != 200 && result.status != 202 && result.status != 429 {
		return result, &Error{Code: "remote", Status: result.status}
	}
	if response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" {
		return result, failure("protocol")
	}
	headerBytes, headerCount := 0, 0
	for k, values := range response.Header {
		for _, v := range values {
			headerCount++
			headerBytes += len(k) + len(v)
			if headerCount > 1024 || headerBytes > 65536 {
				return result, failure("bounds")
			}
		}
	}
	if maxBytes <= 0 || maxNodes <= 0 || response.ContentLength > int64(maxBytes) {
		return result, failure("bounds")
	}
	raw, e := io.ReadAll(io.LimitReader(response.Body, int64(maxBytes)+1))
	result.bytes = len(raw)
	if e != nil {
		return result, failure("transport")
	}
	if len(raw) > maxBytes {
		return result, failure("bounds")
	}
	closeBody()
	if closeErr != nil {
		return result, failure("transport")
	}
	if e := safeContext(ctx); e != nil {
		return result, e
	}
	value, n, e := strictJSON(raw, maxNodes, func() error { return safeContext(ctx) })
	result.nodes = n
	if e != nil {
		return result, e
	}
	payload, ok := value.(map[string]any)
	if !ok {
		return result, failure("protocol")
	}
	result.payload = payload
	return result, nil
}
func (o *operation) request(method, path string, body []byte, credential Credential, s *statement) (httpReply, error) {
	if e := o.check(); e != nil {
		return httpReply{}, e
	}
	if o.requests >= o.client.options.MaxRequests {
		return httpReply{}, failure("bounds")
	}
	o.requests++
	remaining := min(o.client.options.MaxPageBytes, o.client.options.MaxResponseBytes-o.responseBytes)
	remainingNodes := o.client.options.MaxNodes - o.nodes
	if remaining <= 0 || remainingNodes <= 0 {
		return httpReply{}, failure("bounds")
	}
	r, e := foreign(o, o.ctx, func() (httpReply, error) {
		return o.http(o.ctx, method, path, body, credential, remaining, remainingNodes, s)
	})
	o.responseBytes += r.bytes
	o.nodes += r.nodes
	if final := o.check(); final != nil {
		return httpReply{}, final
	}
	return r, e
}

func (o *operation) body(p plan) ([]byte, error) {
	// These closed values cannot invoke custom serializers. Admission counts
	// encoding/json escaping before the first potentially large allocation.
	n := 512
	for _, s := range []string{p.sql, o.client.options.Database, o.client.options.Schema, o.client.options.Warehouse, o.client.options.Role} {
		cost, e := jsonStringSize(s)
		if e != nil {
			return nil, e
		}
		if cost > o.client.options.MaxRequestBytes-n {
			return nil, failure("bounds")
		}
		n += cost
	}
	for key, b := range p.bindings {
		for _, s := range []string{key, b.Type, b.Value} {
			cost, e := jsonStringSize(s)
			if e != nil {
				return nil, e
			}
			if cost > o.client.options.MaxRequestBytes-n {
				return nil, failure("bounds")
			}
			n += cost
		}
		n += 32
		if n > o.client.options.MaxRequestBytes {
			return nil, failure("bounds")
		}
	}
	if n > o.client.options.MaxRequestBytes-o.requestBytes {
		return nil, failure("bounds")
	}
	envelope := map[string]any{"statement": p.sql, "bindings": p.bindings, "database": o.client.options.Database, "schema": o.client.options.Schema, "timeout": max(1, int(time.Until(o.deadline).Seconds()+0.999999999))}
	if o.client.options.Warehouse != "" {
		envelope["warehouse"] = o.client.options.Warehouse
	}
	if o.client.options.Role != "" {
		envelope["role"] = o.client.options.Role
	}
	data, e := json.Marshal(envelope)
	if e != nil {
		return nil, failure("input")
	}
	if len(data) > o.client.options.MaxRequestBytes-o.requestBytes {
		return nil, failure("bounds")
	}
	return data, nil
}
func object(v any) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, failure("protocol")
	}
	return m, nil
}
func integer(v any) (int64, error) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, failure("protocol")
	}
	i, e := strconv.ParseInt(string(n), 10, 64)
	if e != nil {
		return 0, failure("protocol")
	}
	return i, nil
}
func optionalInt(m map[string]any, key string) (*int64, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, nil
	}
	n, e := integer(v)
	if e != nil {
		return nil, e
	}
	return &n, nil
}
func columns(v any, maxColumns int) ([]Column, error) {
	items, ok := v.([]any)
	if !ok || len(items) == 0 || len(items) > maxColumns {
		return nil, failure("protocol")
	}
	out := make([]Column, 0, len(items))
	seen := make(map[string]bool)
	for _, item := range items {
		m, e := object(item)
		if e != nil {
			return nil, e
		}
		n, ok := m["name"].(string)
		if !ok || !name(n) || seen[n] {
			return nil, failure("protocol")
		}
		seen[n] = true
		t, ok := m["type"].(string)
		if !ok || !name(t) {
			return nil, failure("protocol")
		}
		c := Column{Name: n, Type: t}
		for key, target := range map[string]**int64{"precision": &c.Precision, "scale": &c.Scale, "length": &c.Length, "byteLength": &c.ByteLength} {
			*target, e = optionalInt(m, key)
			if e != nil {
				return nil, e
			}
		}
		if v, present := m["nullable"]; present && v != nil {
			nullable, ok := v.(bool)
			if !ok {
				return nil, failure("protocol")
			}
			c.Nullable = &nullable
		}
		out = append(out, c)
	}
	return out, nil
}
func decodeRows(v any, columns []Column, expected int, o *operation) ([]Row, error) {
	items, ok := v.([]any)
	if !ok || len(items) != expected {
		return nil, failure("protocol")
	}
	out := make([]Row, 0, len(items))
	for _, item := range items {
		if e := o.check(); e != nil {
			return nil, e
		}
		cells, ok := item.([]any)
		if !ok || len(cells) != len(columns) {
			return nil, failure("protocol")
		}
		row := make(Row, len(columns))
		for i, column := range columns {
			if cells[i] == nil {
				row[column.Name] = nil
			} else {
				s, ok := cells[i].(string)
				if !ok {
					return nil, failure("protocol")
				}
				row[column.Name] = &s
			}
		}
		out = append(out, row)
	}
	return out, nil
}

func (c *Client) execute(o *operation, p plan) (result *Result, err error) {
	if e := o.check(); e != nil {
		return nil, e
	}
	_, e := o.body(p)
	if e != nil {
		return nil, e
	}
	credential, e := o.auth()
	if e != nil {
		return nil, e
	}
	body, e := o.body(p)
	if e != nil {
		return nil, e
	}
	o.requestBytes += len(body)
	s := &statement{credential: credential}
	defer func() {
		if err != nil {
			o.cancelStatement(s)
		}
	}()
	reply, e := o.request(http.MethodPost, "/api/v2/statements", body, credential, s)
	if e != nil {
		return nil, e
	}
	if value, present := reply.payload["statementHandle"]; present {
		if e = s.setHandle(value); e != nil {
			return nil, e
		}
	}
	if reply.status != 200 && s.id() == "" {
		return nil, failure("protocol")
	}
	if reply.status == 429 {
		return nil, &Error{Code: "remote", Status: 429}
	}
	polls := 0
	for reply.status != 200 {
		polls++
		if polls > c.options.MaxPolls {
			return nil, failure("bounds")
		}
		timer := time.NewTimer(c.options.PollInterval)
		select {
		case <-o.ctx.Done():
			timer.Stop()
			return nil, o.check()
		case <-timer.C:
		}
		credential, e = o.auth()
		if e != nil {
			return nil, e
		}
		reply, e = o.request(http.MethodGet, "/api/v2/statements/"+s.id(), nil, credential, s)
		if e != nil {
			return nil, e
		}
		if value, present := reply.payload["statementHandle"]; present {
			if e = s.setHandle(value); e != nil {
				return nil, e
			}
		}
	}
	s.complete() // Server reports completion: mapping failures must not cancel it.
	metadata, e := object(reply.payload["resultSetMetaData"])
	if e != nil {
		return nil, e
	}
	cols, e := columns(metadata["rowType"], c.options.MaxColumns)
	if e != nil {
		return nil, e
	}
	count, e := integer(metadata["numRows"])
	if e != nil || count < 0 {
		return nil, failure("protocol")
	}
	if count > int64(p.limit) || count > int64(c.options.MaxRows-o.rows) {
		return nil, failure("bounds")
	}
	parts, ok := metadata["partitionInfo"].([]any)
	if !ok || len(parts) == 0 || len(parts) > c.options.MaxPartitions {
		return nil, failure("protocol")
	}
	counts := make([]int, len(parts))
	total := int64(0)
	for i, part := range parts {
		m, e := object(part)
		if e != nil {
			return nil, e
		}
		n, e := integer(m["rowCount"])
		if e != nil || n < 0 || n > count-total {
			return nil, failure("protocol")
		}
		total += n
		counts[i] = int(n)
	}
	if total != count || len(parts) > 1 && s.id() == "" {
		return nil, failure("protocol")
	}
	out := &Result{Rows: make([]Row, 0, int(count)), Columns: cols}
	for i, n := range counts {
		payload := reply.payload
		if i > 0 {
			credential, e = o.auth()
			if e != nil {
				return nil, e
			}
			part, e := o.request(http.MethodGet, "/api/v2/statements/"+s.id()+"?partition="+strconv.Itoa(i), nil, credential, s)
			if e != nil {
				return nil, e
			}
			if part.status != 200 {
				return nil, failure("protocol")
			}
			if v, present := part.payload["statementHandle"]; present {
				if e = s.setHandle(v); e != nil {
					return nil, e
				}
			}
			payload = part.payload
		}
		rows, e := decodeRows(payload["data"], cols, n, o)
		if e != nil {
			return nil, e
		}
		out.Rows = append(out.Rows, rows...)
	}
	if p.requireID {
		seen := make(map[string]bool)
		for _, row := range out.Rows {
			id, ok := row[p.table.IDColumn]
			if !ok || id == nil || *id == "" || seen[*id] {
				return nil, failure("identity")
			}
			seen[*id] = true
		}
	}
	o.rows += len(out.Rows)
	if e := o.check(); e != nil {
		return nil, e
	}
	return out, nil
}
