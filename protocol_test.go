package dalgo2snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoll429UsesBoundedOwnedPath(t *testing.T) {
	var polls atomic.Int64
	c, count := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var input any
			if e := json.NewDecoder(r.Body).Decode(&input); e != nil {
				t.Error(e)
			}
			writeJSON(t, w, 202, map[string]any{"statementHandle": statementID, "statementStatusUrl": "https://foreign.invalid/SECRET"})
			return
		}
		if r.URL.Path != "/api/v2/statements/"+statementID {
			t.Error("status URL escaped owned handle")
		}
		if polls.Add(1) == 1 {
			writeJSON(t, w, 429, map[string]any{"statementHandle": statementID})
			return
		}
		writeJSON(t, w, 200, payload([][]any{{"a", "v", nil}}))
	}, nil)
	row, e := c.Get(context.Background(), "users", "a")
	if e != nil || row == nil || count.Load() != 3 || polls.Load() != 2 {
		t.Fatal("polling failed", e, count.Load())
	}
}
func TestPollLimitCancelRejectKeepsOriginalErrorAndCapturedAuthority(t *testing.T) {
	var tokens, cancels atomic.Int64
	cancelled := make(chan struct{})
	var once sync.Once
	c, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			cancels.Add(1)
			if r.Header.Get("Authorization") != "Bearer first-1" {
				t.Error("cleanup refreshed principal")
			}
			once.Do(func() { close(cancelled) })
			writeJSON(t, w, 401, map[string]any{"message": "SECRET"})
			return
		}
		if r.Method == http.MethodPost {
			var v any
			if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
				t.Error(e)
			}
			writeJSON(t, w, 202, map[string]any{"statementHandle": statementID})
			return
		}
		writeJSON(t, w, 429, map[string]any{"statementHandle": statementID})
	}, func(o *Options) {
		o.MaxPolls = 1
		o.Token = func(context.Context) (Credential, error) {
			return OAuthToken(fmt.Sprintf("first-%d", tokens.Add(1))), nil
		}
	})
	row, e := c.Get(context.Background(), "users", "a")
	var failure *Error
	if row != nil || !errors.As(e, &failure) || failure.Code != "bounds" || strings.Contains(e.Error(), "SECRET") {
		t.Fatal("cancel changed original failure", e)
	}
	wait(t, cancelled)
	waitIdle(t, c)
	if cancels.Load() != 1 || tokens.Load() != 2 {
		t.Fatal("cancel retry/new auth", cancels.Load(), tokens.Load())
	}
}
func TestMismatchedPendingHandleCancelsOnlyOriginal(t *testing.T) {
	cancelled := make(chan struct{})
	c, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			if r.URL.Path != "/api/v2/statements/"+statementID+"/cancel" {
				t.Error("cancelled non-owned handle")
			}
			close(cancelled)
			writeJSON(t, w, 200, map[string]any{"statementHandle": statementID})
			return
		}
		if r.Method == http.MethodPost {
			var v any
			if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
				t.Error(e)
			}
			writeJSON(t, w, 202, map[string]any{"statementHandle": statementID})
			return
		}
		writeJSON(t, w, 202, map[string]any{"statementHandle": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"})
	}, nil)
	if row, e := c.Get(context.Background(), "users", "a"); e == nil || row != nil {
		t.Fatal("mismatched owner delivered")
	}
	wait(t, cancelled)
	waitIdle(t, c)
}
func TestInvalidUnownedHandleNeverCreatesCancel(t *testing.T) {
	c, count := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var v any
		if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
			t.Error(e)
		}
		writeJSON(t, w, 202, map[string]any{"statementHandle": "../not-owned"})
	}, nil)
	if row, e := c.Get(context.Background(), "users", "a"); e == nil || row != nil {
		t.Fatal("invalid pending handle delivered")
	}
	waitIdle(t, c)
	if count.Load() != 1 {
		t.Fatal("guessed cleanup handle")
	}
}
func TestCompletedMappingFailureDoesNotCancel(t *testing.T) {
	c, count := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var v any
		if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
			t.Error(e)
		}
		body := payload([][]any{{"a", "v", nil}})
		body["data"] = [][]any{{"wrong", "v", nil}}
		writeJSON(t, w, 200, body)
	}, nil)
	if row, e := c.Get(context.Background(), "users", "a"); e == nil || row != nil {
		t.Fatal("wrong point identity delivered")
	}
	waitIdle(t, c)
	if count.Load() != 1 {
		t.Fatal("completed statement cancelled due to mapping")
	}
}
func TestSharedBatchDeadlineHasNoSuccessfulPrefix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var tokens atomic.Int64
	c, count := newFixture(t, nativeHandler(t), func(o *Options) {
		o.Token = func(context.Context) (Credential, error) {
			if tokens.Add(1) == 2 {
				cancel()
			}
			return OAuthToken("synthetic"), nil
		}
	})
	rows, e := c.GetMany(ctx, "users", []string{"a", "b"})
	if rows != nil || !errors.Is(e, context.Canceled) || count.Load() != 1 {
		t.Fatal("batch returned prefix or dispatched after cancel", e, count.Load())
	}
}
func TestAuthFailureNoAnonymousFallbackAndNoCause(t *testing.T) {
	for _, kind := range []string{"error", "invalid"} {
		t.Run(kind, func(t *testing.T) {
			c, count := newFixture(t, nativeHandler(t), func(o *Options) {
				o.Token = func(context.Context) (Credential, error) {
					if kind == "error" {
						return Credential{}, errors.New("SECRET custom cause")
					}
					return OAuthToken("SECRET\nheader"), nil
				}
			})
			_, e := c.Get(context.Background(), "users", "a")
			if e == nil || strings.Contains(e.Error(), "SECRET") || errors.Unwrap(e) != nil || count.Load() != 0 {
				t.Fatal("credential failure escaped or anonymous fallback", e)
			}
		})
	}
}
func TestCallerCancelCauseIsNotReturned(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("SECRET"))
	c, count := newFixture(t, nativeHandler(t), nil)
	if _, e := c.Get(ctx, "users", "a"); !errors.Is(e, context.Canceled) || strings.Contains(e.Error(), "SECRET") || count.Load() != 0 {
		t.Fatal("unsafe caller cause", e)
	}
}
func TestOriginalDeadlineIncludesCredentialWait(t *testing.T) {
	entered := make(chan struct{})
	c, count := newFixture(t, nativeHandler(t), func(o *Options) {
		o.Timeout = 100 * time.Millisecond
		o.Token = func(ctx context.Context) (Credential, error) {
			close(entered)
			<-ctx.Done()
			return OAuthToken("synthetic"), nil
		}
	})
	start := time.Now()
	_, e := c.Get(context.Background(), "users", "a")
	if !errors.Is(e, context.DeadlineExceeded) || time.Since(start) > 750*time.Millisecond || count.Load() != 0 {
		t.Fatal("credential wait escaped original deadline", e)
	}
	wait(t, entered)
	waitIdle(t, c)
}
