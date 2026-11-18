package dalgo2snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNativePointBatchAndMetadata(t *testing.T) {
	c, count := newFixture(t, nativeHandler(t), nil)
	row, e := c.Get(context.Background(), "users", "9007199254740993")
	if e != nil {
		t.Fatal(e)
	}
	if row == nil || row.ID != "9007199254740993" || *row.Row["VALUE"] != "9223372036854775807.123456789" || row.Row["NIL"] != nil {
		t.Fatal("native STRING/null changed")
	}
	column := row.Columns[1]
	if column.Precision == nil || *column.Precision != 38 || column.Scale == nil || *column.Scale != 9 || column.Nullable != nil || row.Columns[0].Nullable == nil || *row.Columns[0].Nullable {
		t.Fatal("native nullable/unknown metadata changed")
	}
	if v, e := c.Exists(context.Background(), "users", "missing"); e != nil || v {
		t.Fatal(v, e)
	}
	rows, e := c.GetMany(context.Background(), "users", []string{"b", "missing", "a", "a"})
	if e != nil {
		t.Fatal(e)
	}
	if len(rows) != 4 || rows[0].ID != "b" || rows[1] != nil || rows[2].ID != "a" || rows[3].ID != "a" {
		t.Fatal("batch order/missing/duplicates")
	}
	*rows[2].Row["VALUE"] = "changed"
	if *rows[3].Row["VALUE"] == "changed" {
		t.Fatal("duplicate alias")
	}
	before := count.Load()
	empty, e := c.GetMany(context.Background(), "users", nil)
	if e != nil || len(empty) != 0 || count.Load() != before {
		t.Fatal("empty batch used credentials/HTTP")
	}
	metadata := c.ConfiguredMetadata()
	delete(metadata.Tables, "users")
	if len(c.ConfiguredMetadata().Tables) != 1 || metadata.DynamicDiscovery {
		t.Fatal("configuration metadata escaped snapshot")
	}
}

func TestNativeTextNullWholePartitionsAndRotatingToken(t *testing.T) {
	var tokens atomic.Int64
	values := []any{"9223372036854775807", "0.000000000000000000123", "1e+100", "1700000000.123456789", "00FF", "{\"n\":9223372036854775807}", "null", "", nil}
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("missing token")
		}
		if r.Method == http.MethodPost {
			var v map[string]any
			if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
				t.Error(e)
			}
			writeJSON(t, w, 202, map[string]any{"statementHandle": statementID, "statementStatusUrl": "https://foreign.invalid/never"})
			return
		}
		if r.URL.Path != "/api/v2/statements/"+statementID {
			t.Error("foreign status authority used")
		}
		if r.URL.Query().Get("partition") == "1" {
			writeJSON(t, w, 200, map[string]any{"statementHandle": statementID, "data": []any{values}})
			return
		}
		cols := make([]any, len(values))
		for i := range cols {
			cols[i] = map[string]any{"name": fmt.Sprint("c", i), "type": "text"}
		}
		writeJSON(t, w, 200, map[string]any{"statementHandle": statementID, "resultSetMetaData": map[string]any{"numRows": 1, "rowType": cols, "partitionInfo": []any{map[string]any{"rowCount": 0}, map[string]any{"rowCount": 1}}}, "data": []any{}})
	}
	c, count := newFixture(t, handler, func(o *Options) {
		o.Token = func(context.Context) (Credential, error) {
			return KeyPairJWT(fmt.Sprintf("synthetic-%d", tokens.Add(1))), nil
		}
	})
	result, e := c.Query(context.Background(), query())
	if e != nil {
		t.Fatal(e)
	}
	if len(result.Rows) != 1 || count.Load() != 3 || tokens.Load() != 3 {
		t.Fatal("missing partitions or token rotation")
	}
	for i, v := range values {
		got := result.Rows[0][fmt.Sprint("c", i)]
		if v == nil {
			if got != nil {
				t.Fatal("null coerced")
			}
		} else if got == nil || *got != v.(string) {
			t.Fatal("wire lexeme changed", i)
		}
	}
}

func TestProtocolWholeResultRefusals(t *testing.T) {
	cases := map[string]func(map[string]any){
		"width":        func(v map[string]any) { v["data"] = [][]any{{"a"}} },
		"numeric-cell": func(v map[string]any) { v["data"] = [][]any{{"a", 12, nil}} },
		"missing-id":   func(v map[string]any) { v["data"] = [][]any{{nil, "value", nil}} },
		"duplicate-id": func(v map[string]any) {
			v["data"] = [][]any{{"a", "v", nil}, {"a", "v", nil}}
			m := v["resultSetMetaData"].(map[string]any)
			m["numRows"] = 2
			m["partitionInfo"] = []any{map[string]any{"rowCount": 2}}
		},
		"row-count": func(v map[string]any) { v["resultSetMetaData"].(map[string]any)["numRows"] = 2 },
		"duplicate-column": func(v map[string]any) {
			v["resultSetMetaData"].(map[string]any)["rowType"].([]any)[1].(map[string]any)["name"] = "ID"
		},
		"precision-fraction": func(v map[string]any) {
			v["resultSetMetaData"].(map[string]any)["rowType"].([]any)[0].(map[string]any)["precision"] = 1.1
		},
		"nullable-type": func(v map[string]any) {
			v["resultSetMetaData"].(map[string]any)["rowType"].([]any)[0].(map[string]any)["nullable"] = "false"
		},
		"bad-handle": func(v map[string]any) { v["statementHandle"] = "../secret" },
	}
	for n, change := range cases {
		t.Run(n, func(t *testing.T) {
			c, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
				var input any
				if e := json.NewDecoder(r.Body).Decode(&input); e != nil {
					t.Error(e)
				}
				v := payload([][]any{{"a", "v", nil}})
				change(v)
				writeJSON(t, w, 200, v)
			}, nil)
			result, e := c.Get(context.Background(), "users", "a")
			if e == nil || result != nil {
				t.Fatal("malformed whole result delivered")
			}
		})
	}
}
func TestHTTPFailureRedactionAndNoRedirectRetry(t *testing.T) {
	for _, status := range []int{301, 401, 403, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			c, count := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "https://foreign.invalid/SECRET")
				writeJSON(t, w, status, map[string]any{"message": "SECRET token SQL"})
			}, nil)
			r, e := c.Get(context.Background(), "users", "SECRET")
			if e == nil || r != nil || count.Load() != 1 || strings.Contains(e.Error(), "SECRET") || errors.Unwrap(e) != nil {
				t.Fatal("unsafe failure/retry", e, count.Load())
			}
		})
	}
}
func TestJSONDuplicateUnicodeTrailingAndNodeBounds(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `{"a":"\ud800"}`, `{"a":1} {}`, `[1,2,3]`} {
		maximum := 20
		if strings.HasPrefix(raw, "[") {
			maximum = 2
		}
		if _, _, e := strictJSON([]byte(raw), maximum, func() error { return nil }); e == nil {
			t.Fatal("malformed or oversized JSON admitted", raw)
		}
	}
	v, _, e := strictJSON([]byte(`{"__proto__":"native","constructor":"own","prototype":"own","emoji":"\ud83d\ude00"}`), 30, func() error { return nil })
	if e != nil || v.(map[string]any)["emoji"] != "😀" {
		t.Fatal(v, e)
	}
}
func TestRequestAndAggregateResponseBounds(t *testing.T) {
	var tokens atomic.Int64
	c, count := newFixture(t, nativeHandler(t), func(o *Options) {
		o.MaxRequestBytes = 2048
		o.Token = func(context.Context) (Credential, error) { tokens.Add(1); return OAuthToken("synthetic"), nil }
	})
	if _, e := c.Get(context.Background(), "users", strings.Repeat("\x00", 500)); e == nil || tokens.Load() != 0 || count.Load() != 0 {
		t.Fatal("escaping bypassed pre-auth admission")
	}
	c2, requests := newFixture(t, nativeHandler(t), func(o *Options) { o.MaxResponseBytes = 500 })
	if out, e := c2.GetMany(context.Background(), "users", []string{"a", "b", "c"}); e == nil || out != nil || requests.Load() > 2 {
		t.Fatal("aggregate response budget bypass", e, requests.Load())
	}
}
func TestCredentialFormattingAndConfiguration(t *testing.T) {
	secret := OAuthToken("SECRET")
	for _, s := range []string{fmt.Sprint(secret), fmt.Sprintf("%+v", secret), fmt.Sprintf("%#v", secret)} {
		if strings.Contains(s, "SECRET") {
			t.Fatal("credential formatted")
		}
	}
	for _, endpoint := range []string{"http://owned.snowflakecomputing.com", "https://evil.invalid", "https://owned.snowflakecomputing.com/path", "https://user:pass@owned.snowflakecomputing.com", "https://owned.snowflakecomputing.com?token=SECRET"} {
		_, e := New(endpoint, Options{})
		if e == nil || strings.Contains(e.Error(), "SECRET") {
			t.Fatal("invalid origin admitted", e)
		}
	}
	c, _ := newFixture(t, nativeHandler(t), nil)
	if c.owned.Proxy != nil || !c.owned.DisableKeepAlives || c.owned.Protocols.HTTP2() || !c.owned.Protocols.HTTP1() {
		t.Fatal("owned no-proxy/no-hidden-retry HTTP1 policy")
	}
	if !reflect.DeepEqual(c.options.Tables, map[string]Table{"users": {Name: "USERS", IDColumn: "ID"}}) {
		t.Fatal("mapping changed")
	}
}
