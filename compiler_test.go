package dalgo2snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func TestCompilerClosedNativeSQLAndBindings(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), func(o *Options) { o.Tables["users"] = Table{Name: `table"; DROP TABLE x; --`, IDColumn: "ID"} })
	o, e := c.begin(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer o.finish()
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where(dal.NewGroupCondition(dal.And,
		dal.Comparison{Left: dal.DocumentID(), Operator: dal.Equal, Right: dal.Constant{Value: "x' OR 1=1"}},
		dal.Comparison{Left: dal.Field("__name__"), Operator: dal.Operator("!="), Right: dal.Constant{Value: nil}},
		dal.Comparison{Left: dal.Field("VALUE"), Operator: dal.In, Right: dal.Array{Value: []any{int64(math.MaxInt64), json.Number("12345678901234567890.0001")}}},
	)).OrderBy(dal.DescendingField("VALUE")).Limit(2).Offset(7).SelectIntoRecordset()
	p, e := c.compile(o, q, false)
	if e != nil {
		t.Fatal(e)
	}
	for _, part := range []string{`FROM "table""; DROP TABLE x; --"`, `"ID" = ?`, `"__name__" IS NOT NULL`, `"VALUE" IN (?, ?)`, `ORDER BY "VALUE" DESC NULLS LAST, "ID" ASC NULLS LAST LIMIT 2 OFFSET 7`} {
		if !strings.Contains(p.sql, part) {
			t.Fatal("compiler mismatch", p.sql, part)
		}
	}
	if strings.Contains(p.sql, "OR 1=1") || p.bindings["1"].Value != "x' OR 1=1" || p.bindings["2"].Value != "9223372036854775807" || p.bindings["3"].Value != "12345678901234567890.0001" {
		t.Fatal("parameter fidelity/injection")
	}
}

type forbiddenCondition struct{}

func (forbiddenCondition) String() string { panic("must not render arbitrary condition") }
func TestCompilerRefusalsBeforeCredential(t *testing.T) {
	var auth atomic.Int64
	c, count := newFixture(t, nativeHandler(t), func(o *Options) {
		o.Token = func(context.Context) (Credential, error) { auth.Add(1); return OAuthToken("synthetic"), nil }
	})
	cases := []dal.Query{
		dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where(forbiddenCondition{}).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().WhereField("x", dal.In, []any{}).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().WhereField("x", dal.In, []any{nil}).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().WhereField("x", dal.Equal, math.Inf(1)).SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where().GroupBy(dal.Field("x")).SelectIntoRecordset(),
		dal.From(dal.NewQualifiedRootCollectionRef("different", "users", "")).NewQuery().Where().SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where().StartAfter("portable").SelectIntoRecordset(),
		dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where().Limit(-1).SelectIntoRecordset(),
	}
	for i, q := range cases {
		if result, e := c.Query(context.Background(), q); e == nil || result != nil {
			t.Fatal("unsupported query delivered", i)
		}
	}
	if auth.Load() != 0 || count.Load() != 0 {
		t.Fatal("refusal performed I/O")
	}
}

type cancelQuery struct {
	dal.StructuredQuery
	cancel      context.CancelFunc
	whereCalled *atomic.Bool
}

func (q cancelQuery) Columns() []dal.Column { q.cancel(); return nil }
func (q cancelQuery) Where() dal.Condition {
	q.whereCalled.Store(true)
	return q.StructuredQuery.Where()
}
func TestCompilerGetterCancellationStopsLaterCallback(t *testing.T) {
	c, count := newFixture(t, nativeHandler(t), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var called atomic.Bool
	r, e := c.Query(ctx, cancelQuery{StructuredQuery: query(), cancel: cancel, whereCalled: &called})
	if !errors.Is(e, context.Canceled) || r != nil || called.Load() || count.Load() != 0 {
		t.Fatal("callback boundary crossed", e)
	}
}
func TestExplicitProjectionAndOverflowFailWhole(t *testing.T) {
	c, count := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var request any
		if e := json.NewDecoder(r.Body).Decode(&request); e != nil {
			t.Error(e)
		}
		writeJSON(t, w, 200, payload([][]any{{"a", "v", nil}, {"b", "w", nil}}))
	}, func(o *Options) { o.MaxRows = 1 })
	if result, e := c.Query(context.Background(), query()); e == nil || result != nil {
		t.Fatal("overflow truncated")
	}
	before := count.Load()
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where().SelectColumns(dal.Column{Expression: dal.Field("VALUE")})
	if r, e := c.DB().ExecuteQueryToRecordsReader(context.Background(), q); !errors.Is(e, dal.ErrNotSupported) || r != nil || count.Load() != before {
		t.Fatal("records projection invented hidden identity", e)
	}
}
