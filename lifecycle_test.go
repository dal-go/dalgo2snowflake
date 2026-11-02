package dalgo2snowflake

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAfterDispatchCallerDeadlineOwnedCancelAndReopen(t *testing.T) {
	for _, mode := range []string{"caller", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			var phase atomic.Int64
			var credentialCalls atomic.Int64
			dispatch := make(chan struct{})
			disconnected := make(chan struct{})
			cancelled := make(chan struct{})
			var cancelOnce sync.Once
			c, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/cancel") {
					if r.Header.Get("Authorization") != "Bearer principal-1" {
						t.Error("cancel changed submit authority")
					}
					cancelOnce.Do(func() { close(cancelled) })
					writeJSON(t, w, 200, map[string]any{"statementHandle": statementID})
					return
				}
				if r.Method == http.MethodPost {
					var v any
					if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
						t.Error(e)
					}
					if phase.CompareAndSwap(0, 1) {
						writeJSON(t, w, 202, map[string]any{"statementHandle": statementID})
						return
					}
					writeJSON(t, w, 200, payload([][]any{{"a", "v", nil}}))
					return
				}
				close(dispatch)
				<-r.Context().Done()
				close(disconnected)
			}, func(o *Options) {
				o.Token = func(context.Context) (Credential, error) {
					return OAuthToken(fmt.Sprintf("principal-%d", credentialCalls.Add(1))), nil
				}
				if mode == "deadline" {
					o.Timeout = 150 * time.Millisecond
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, e := c.Get(ctx, "users", "a"); done <- e }()
			wait(t, dispatch)
			start := time.Now()
			if mode == "caller" {
				cancel()
			}
			var e error
			select {
			case e = <-done:
			case <-time.After(750 * time.Millisecond):
				t.Fatal("public cancellation not prompt")
			}
			if time.Since(start) > 750*time.Millisecond {
				t.Fatal("public cancellation exceeded budget")
			}
			expected := context.Canceled
			if mode == "deadline" {
				expected = context.DeadlineExceeded
			}
			if !errors.Is(e, expected) {
				t.Fatal("wrong cancellation classification", e)
			}
			wait(t, disconnected)
			wait(t, cancelled)
			waitIdle(t, c)
			row, e := c.Get(context.Background(), "users", "a")
			if e != nil || row == nil {
				t.Fatal("subsequent read failed", e)
			}
		})
	}
}
func TestIgnoredTokenRetainsPhysicalAdmission(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { closeSignal(release) })
	var first atomic.Bool
	var calls atomic.Int64
	c, count := newFixture(t, nativeHandler(t), func(o *Options) {
		o.MaxConcurrent = 1
		o.Token = func(context.Context) (Credential, error) {
			calls.Add(1)
			if first.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
			return OAuthToken("synthetic"), nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := c.Get(ctx, "users", "a"); done <- e }()
	wait(t, entered)
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("ignored token blocked public cancellation")
	}
	for i := 0; i < 3; i++ {
		if _, e := c.Get(context.Background(), "users", "a"); e == nil {
			t.Fatal("physical lease bypassed")
		}
	}
	if calls.Load() != 1 || count.Load() != 0 {
		t.Fatal("new physical work admitted")
	}
	close(release)
	waitIdle(t, c)
	if _, e := c.Get(context.Background(), "users", "a"); e != nil {
		t.Fatal(e)
	}
}

type stalledBody struct {
	entered, closed, release chan struct{}
	readOnce, closeOnce      sync.Once
}

func (b *stalledBody) Read([]byte) (int, error) {
	b.readOnce.Do(func() { close(b.entered) })
	<-b.release
	return 0, io.EOF
}
func (b *stalledBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	<-b.release
	return nil
}
func TestIgnoredReadAndBodyCloseStayAdmittedAfterPublicAbort(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), func(o *Options) { o.MaxConcurrent = 1 })
	body := &stalledBody{entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	original := c.transport
	t.Cleanup(func() { closeSignal(body.release) })
	c.transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, ContentLength: -1}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.Get(ctx, "users", "a"); done <- e }()
	wait(t, body.entered)
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("ignored native read blocked caller")
	}
	wait(t, body.closed)
	if _, e := c.Get(context.Background(), "users", "a"); e == nil {
		t.Fatal("read/close lease released early")
	}
	close(body.release)
	waitIdle(t, c)
	c.transport = original
	if _, e := c.Get(context.Background(), "users", "a"); e != nil {
		t.Fatal("reopen after physical settlement", e)
	}
}
func TestOwnedCancellationStartsBeforeStalledReaderClose(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), func(o *Options) { o.MaxConcurrent = 1 })
	body := &stalledBody{entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	cancelStart := make(chan struct{})
	cancelRelease := make(chan struct{})
	var ordinary atomic.Int64
	t.Cleanup(func() { closeSignal(body.release); closeSignal(cancelRelease) })
	c.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			close(cancelStart)
			<-cancelRelease
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"statementHandle":"` + statementID + `"}`)), ContentLength: -1}, nil
		}
		if ordinary.Add(1) == 1 {
			return &http.Response{StatusCode: 202, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"statementHandle":"` + statementID + `"}`)), ContentLength: -1}, nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, ContentLength: -1}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.Get(ctx, "users", "a"); done <- e }()
	wait(t, body.entered)
	cancel()
	wait(t, cancelStart)
	wait(t, body.closed)
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("owned cancel or Close blocked public failure")
	}
	if _, e := c.Get(context.Background(), "users", "a"); e == nil {
		t.Fatal("cleanup lease bypassed")
	}
	close(body.release)
	close(cancelRelease)
	waitIdle(t, c)
}
func TestPreAbortCloseAndCARefusals(t *testing.T) {
	var auth atomic.Int64
	c, count := newFixture(t, nativeHandler(t), func(o *Options) {
		o.Token = func(context.Context) (Credential, error) { auth.Add(1); return OAuthToken("synthetic"), nil }
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.Get(ctx, "users", "a"); !errors.Is(e, context.Canceled) || auth.Load() != 0 || count.Load() != 0 {
		t.Fatal("preabort performed work", e)
	}
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Get(context.Background(), "users", "a"); e == nil {
		t.Fatal("closed client reopened")
	}
	bad, requests := newFixture(t, nativeHandler(t), func(o *Options) { o.Transport.TLSClientConfig.RootCAs = x509.NewCertPool() })
	if row, e := bad.Get(context.Background(), "users", "a"); e == nil || row != nil || requests.Load() != 0 {
		t.Fatal("untrusted certificate accepted", e)
	}
}

type notifyingBody struct {
	io.Reader
	closed chan struct{}
	calls  atomic.Int64
}

func (b *notifyingBody) Close() error {
	if b.calls.Add(1) == 1 {
		close(b.closed)
	}
	return nil
}
func TestLateHeadersRetainAdmissionAndCloseExactOwnedBody(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), func(o *Options) { o.MaxConcurrent = 1 })
	entered, release := make(chan struct{}), make(chan struct{})
	body := &notifyingBody{Reader: strings.NewReader(`{}`), closed: make(chan struct{})}
	original := c.transport
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	c.transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, ContentLength: -1}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.Get(ctx, "users", "a"); done <- e }()
	wait(t, entered)
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("ignored headers blocked public abort")
	}
	if _, e := c.Get(context.Background(), "users", "a"); e == nil {
		t.Fatal("late header physical lease bypassed")
	}
	close(release)
	wait(t, body.closed)
	waitIdle(t, c)
	if body.calls.Load() != 1 {
		t.Fatal("late body not closed exactly once")
	}
	c.transport = original
	if _, e := c.Get(context.Background(), "users", "a"); e != nil {
		t.Fatal("reopen after late cleanup", e)
	}
}
