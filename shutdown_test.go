package dalgo2snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

// Done is observed only when Shutdown actually enters its nonempty-state
// select. This is a positive wait-boundary ACK, not an Err-call counter.
type observedShutdownContext struct {
	context.Context
	entered chan struct{}
	onWait  func()
	once    sync.Once
}

func (c *observedShutdownContext) Done() <-chan struct{} {
	c.once.Do(func() {
		if c.onWait != nil {
			c.onWait()
		}
		close(c.entered)
	})
	return c.Context.Done()
}
func observeShutdownWait(t *testing.T, entered <-chan struct{}, done <-chan error) {
	t.Helper()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("Shutdown returned before nonempty work settled: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not enter its bounded wait")
	}
}
func awaitShutdown(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case e := <-done:
		return e
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown exceeded finite fixture bound")
		return nil
	}
}
func waitLogicalOnly(t *testing.T, c *Client) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		c.mu.Lock()
		logical := len(c.active)
		physical := 0
		for op := range c.active {
			physical += op.physical
		}
		c.mu.Unlock()
		if logical == 1 && physical == 0 {
			return
		}
		if logical != 1 {
			t.Fatal("held caller logical scope disappeared", logical)
		}
		select {
		case <-timer.C:
			t.Fatal("prior HTTP physical work did not settle")
		default:
			runtime.Gosched()
		}
	}
}

func TestShutdownIdleConcurrentAndTerminal(t *testing.T) {
	var auth atomic.Int64
	c, count := newFixture(t, nativeHandler(t), func(o *Options) {
		o.Token = func(context.Context) (Credential, error) { auth.Add(1); return OAuthToken("synthetic"), nil }
	})
	done := make(chan error, 8)
	for range 8 {
		go func() { done <- c.Shutdown(context.Background()) }()
	}
	for range 8 {
		if e := awaitShutdown(t, done); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := c.Get(context.Background(), "users", "a"); e == nil || auth.Load() != 0 || count.Load() != 0 {
		t.Fatal("Shutdown did not terminally fence new work", e)
	}
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	if e := c.Shutdown(context.Background()); e != nil {
		t.Fatal("idempotent shutdown", e)
	}
}

func TestShutdownStillClosesWithNilPreAbortedAndExpiredContexts(t *testing.T) {
	for _, mode := range []string{"nil", "caller", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			c, count := newFixture(t, nativeHandler(t), nil)
			var ctx context.Context
			var expected error
			if mode == "caller" {
				var cancel context.CancelCauseFunc
				ctx, cancel = context.WithCancelCause(context.Background())
				cancel(errors.New("SECRET_CAUSE"))
				expected = context.Canceled
			}
			if mode == "deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				expected = context.DeadlineExceeded
			}
			e := c.Shutdown(ctx)
			if mode == "nil" {
				var problem *Error
				if !errors.As(e, &problem) || problem.Code != "input" {
					t.Fatal("nil shutdown context", e)
				}
			} else if !errors.Is(e, expected) {
				t.Fatal("wrong wait-context error", e)
			}
			if e == nil || strings.Contains(e.Error(), "SECRET") || errors.Unwrap(e) != nil {
				t.Fatal("shutdown leaked cause or returned late success", e)
			}
			if _, e := c.Get(context.Background(), "users", "a"); e == nil || count.Load() != 0 {
				t.Fatal("pre-aborted Shutdown did not close", e)
			}
		})
	}
}

func TestShutdownWaitsForInlineCallerCallbackWithNoPhysicalWorker(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { closeSignal(release) })
	var once sync.Once
	target := map[string]any{}
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("users", "a"), &target), data: func() any { once.Do(func() { close(entered) }); <-release; return &target }}
	readDone := make(chan error, 1)
	go func() { readDone <- c.DB().Get(context.Background(), r) }()
	wait(t, entered)
	waitLogicalOnly(t, c)
	waitCtx := &observedShutdownContext{Context: context.Background(), entered: make(chan struct{})}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- c.Shutdown(waitCtx) }()
	observeShutdownWait(t, waitCtx.entered, shutdownDone)
	select {
	case e := <-shutdownDone:
		t.Fatal("held caller callback was force-finished", e)
	default:
	}
	close(release)
	if e := awaitShutdown(t, shutdownDone); e != nil {
		t.Fatal(e)
	}
	e := awaitShutdown(t, readDone)
	var problem *Error
	if !errors.As(e, &problem) || problem.Code != "closed" || len(target) != 0 {
		t.Fatal("inline callback was abandoned or mapped after Close", e)
	}
}

func TestShutdownRetainsDeferredOwnedCancelUntilActualBodyEOF(t *testing.T) {
	credentialEntered, credentialRelease, credentialReturned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	cancelHeaders, cancelBodyRelease := make(chan struct{}), make(chan struct{})
	var credentials, cancels, polls atomic.Int64
	c, count := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			cancels.Add(1)
			if r.Method != http.MethodPost || r.URL.Path != "/api/v2/statements/"+statementID+"/cancel" || r.Header.Get("Authorization") != "Bearer captured-submit-1" || r.Header.Get("X-Snowflake-Authorization-Token-Type") != "OAUTH" {
				t.Error("owned cancel changed submission authority")
			}
			body, e := io.ReadAll(io.LimitReader(r.Body, 10))
			if e != nil || len(body) != 0 {
				t.Error("unexpected cancel body")
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			close(cancelHeaders)
			<-cancelBodyRelease
			if e := json.NewEncoder(w).Encode(map[string]any{"statementHandle": statementID}); e != nil {
				t.Error("owned cancel acknowledgement write failed")
			}
			return
		}
		if r.Method == http.MethodGet {
			polls.Add(1)
			t.Error("poll dispatched after held credential was cancelled")
			w.WriteHeader(400)
			return
		}
		var body any
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
			t.Error(e)
		}
		writeJSON(t, w, 202, map[string]any{"statementHandle": statementID})
	}, func(o *Options) {
		o.MaxConcurrent = 1
		o.Token = func(context.Context) (Credential, error) {
			if credentials.Add(1) == 2 {
				close(credentialEntered)
				<-credentialRelease
				close(credentialReturned)
				return OAuthToken("different-poll-principal"), nil
			}
			return OAuthToken("captured-submit-1"), nil
		}
	})
	t.Cleanup(func() { closeSignal(credentialRelease); closeSignal(cancelBodyRelease) })
	readDone := make(chan error, 1)
	go func() { _, e := c.Get(context.Background(), "users", "a"); readDone <- e }()
	wait(t, credentialEntered)
	waitCtx := &observedShutdownContext{Context: context.Background(), entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- c.Shutdown(waitCtx) }()
	observeShutdownWait(t, waitCtx.entered, done)
	wait(t, cancelHeaders)
	var problem *Error
	if e := awaitShutdown(t, readDone); !errors.As(e, &problem) || problem.Code != "closed" {
		t.Fatal("original read classification changed", e)
	}
	close(credentialRelease)
	wait(t, credentialReturned)
	select {
	case e := <-done:
		t.Fatal("Shutdown returned at cancel headers before body EOF", e)
	default:
	}
	close(cancelBodyRelease)
	if e := awaitShutdown(t, done); e != nil {
		t.Fatal("owned cancel/body work did not quiesce", e)
	}
	if credentials.Load() != 2 || cancels.Load() != 1 || polls.Load() != 0 || count.Load() != 2 {
		t.Fatal("cancel reauth/retry or lost physical admission", credentials.Load(), cancels.Load(), polls.Load(), count.Load())
	}
}

type shutdownTestBody struct {
	read  func([]byte) (int, error)
	close func() error
}

func (b *shutdownTestBody) Read(p []byte) (int, error) { return b.read(p) }
func (b *shutdownTestBody) Close() error               { return b.close() }
func TestShutdownCallerTimeoutNeverReleasesIgnoredPhysicalWork(t *testing.T) {
	for _, kind := range []string{"token", "roundtrip", "read", "close"} {
		t.Run(kind, func(t *testing.T) {
			entered, release, settled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			t.Cleanup(func() { closeSignal(release) })
			var enteredOnce, settledOnce sync.Once
			markEntered := func() { enteredOnce.Do(func() { close(entered) }) }
			markSettled := func() { settledOnce.Do(func() { close(settled) }) }
			c, count := newFixture(t, nativeHandler(t), func(o *Options) {
				o.MaxConcurrent = 1
				if kind == "token" {
					o.Token = func(context.Context) (Credential, error) {
						markEntered()
						<-release
						markSettled()
						return OAuthToken("synthetic"), nil
					}
				}
			})
			if kind != "token" {
				c.transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
					if kind == "roundtrip" {
						markEntered()
						<-release
					}
					reader := strings.NewReader(`{}`)
					var closeOnce sync.Once
					body := &shutdownTestBody{read: func(p []byte) (int, error) {
						if kind == "read" {
							markEntered()
							<-release
							return 0, io.EOF
						}
						return reader.Read(p)
					}, close: func() error {
						closeOnce.Do(func() {
							if kind == "close" {
								markEntered()
								<-release
							}
							markSettled()
						})
						return nil
					}}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, ContentLength: -1}, nil
				})
			}
			readDone := make(chan error, 1)
			go func() { _, e := c.Get(context.Background(), "users", "a"); readDone <- e }()
			wait(t, entered)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if e := c.Shutdown(ctx); !errors.Is(e, context.DeadlineExceeded) {
				t.Fatal("ignored physical work did not bound Shutdown", e)
			}
			var problem *Error
			if e := awaitShutdown(t, readDone); !errors.As(e, &problem) || problem.Code != "closed" {
				t.Fatal("original read error changed", e)
			}
			c.mu.Lock()
			active := len(c.active)
			physical := 0
			for op := range c.active {
				physical += op.physical
			}
			c.mu.Unlock()
			if active != 1 || physical < 1 {
				t.Fatal("timed-out Shutdown forced physical credit release", active, physical)
			}
			if _, e := c.Get(context.Background(), "users", "a"); e == nil || count.Load() != 0 {
				t.Fatal("terminal client admitted new work", e)
			}
			close(release)
			wait(t, settled)
			if e := c.Shutdown(context.Background()); e != nil {
				t.Fatal("later Shutdown did not observe genuine settlement", e)
			}
		})
	}
}

func TestShutdownUnclosedReadersRemainLogicalOperations(t *testing.T) {
	for _, kind := range []string{"records", "recordset"} {
		t.Run(kind, func(t *testing.T) {
			c, count := newFixture(t, nativeHandler(t), nil)
			var reader dal.Reader
			var e error
			if kind == "records" {
				reader, e = c.DB().ExecuteQueryToRecordsReader(context.Background(), query())
			} else {
				reader, e = c.DB().ExecuteQueryToRecordsetReader(context.Background(), query())
			}
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = reader.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
			defer cancel()
			if e = c.Shutdown(ctx); !errors.Is(e, context.DeadlineExceeded) {
				t.Fatal("unclosed reader falsely quiescent", e)
			}
			c.mu.Lock()
			active := len(c.active)
			physical := 0
			for op := range c.active {
				physical += op.physical
			}
			c.mu.Unlock()
			if active != 1 || physical != 0 {
				t.Fatal("reader logical scope forced away", active, physical)
			}
			if e = reader.Close(); e != nil {
				t.Fatal(e)
			}
			if e = c.Shutdown(context.Background()); e != nil {
				t.Fatal(e)
			}
			if count.Load() != 1 {
				t.Fatal("completed reader caused remote cancellation")
			}
		})
	}
}

func TestShutdownBackgroundHasPrivateTwoSecondCap(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	reader, e := c.DB().ExecuteQueryToRecordsReader(context.Background(), query())
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	start := time.Now()
	e = c.Shutdown(context.Background())
	elapsed := time.Since(start)
	if !errors.Is(e, context.DeadlineExceeded) || elapsed < 2*time.Second || elapsed > 2500*time.Millisecond {
		t.Fatal("private Shutdown cap not honored", elapsed, e)
	}
	c.mu.Lock()
	active := len(c.active)
	c.mu.Unlock()
	if active != 1 {
		t.Fatal("private timeout fabricated reader completion")
	}
	if e = reader.Close(); e != nil {
		t.Fatal(e)
	}
	if e = c.Shutdown(context.Background()); e != nil {
		t.Fatal(e)
	}
}

func TestShutdownConcurrentWaitersShareOneRealDeletion(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	reader, e := c.DB().ExecuteQueryToRecordsReader(context.Background(), query())
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	done := make(chan error, 8)
	waiters := make([]*observedShutdownContext, 8)
	for i := range waiters {
		waiters[i] = &observedShutdownContext{Context: context.Background(), entered: make(chan struct{})}
		go func(ctx context.Context) { done <- c.Shutdown(ctx) }(waiters[i])
	}
	for _, waiter := range waiters {
		observeShutdownWait(t, waiter.entered, done)
	}
	if e = reader.Close(); e != nil {
		t.Fatal(e)
	}
	if e = reader.Close(); e != nil {
		t.Fatal(e)
	}
	for range waiters {
		if e := awaitShutdown(t, done); e != nil {
			t.Fatal("concurrent waiter missed deletion", e)
		}
	}
}

func TestShutdownCallerCancellationWinsSimultaneousEmptyWake(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	reader, e := c.DB().ExecuteQueryToRecordsReader(context.Background(), query())
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var closeError error
	waitCtx := &observedShutdownContext{Context: ctx, entered: make(chan struct{}), onWait: func() { closeError = reader.Close(); cancel(errors.New("SECRET_CALLER_CAUSE")) }}
	e = c.Shutdown(waitCtx)
	if closeError != nil || !errors.Is(e, context.Canceled) || strings.Contains(e.Error(), "SECRET") || errors.Unwrap(e) != nil {
		t.Fatal("empty deletion wake overrode actual caller cancellation", e)
	}
	wait(t, waitCtx.entered)
	c.mu.Lock()
	empty := len(c.active) == 0
	c.mu.Unlock()
	if !empty {
		t.Fatal("fixture did not actually close the reader")
	}
	if e = c.Shutdown(context.Background()); e != nil {
		t.Fatal(e)
	}
}
