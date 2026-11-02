package dalgo2snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

type callbackRecord struct {
	record.Record
	key  func() *record.Key
	data func() any
	set  func(error)
}

func (r *callbackRecord) Key() *record.Key {
	if r.key != nil {
		return r.key()
	}
	return r.Record.Key()
}
func (r *callbackRecord) Data() any {
	if r.data != nil {
		return r.data()
	}
	return r.Record.Data()
}
func (r *callbackRecord) SetError(e error) record.Record {
	r.Record.SetError(e)
	if r.set != nil {
		r.set(e)
	}
	return r
}

func TestDALNativeMapAndRowBridge(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	key := record.NewKeyWithID("users", "a")
	data := map[string]any{}
	r := record.NewRecordWithData(key, &data)
	if e := c.DB().Get(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	if data["VALUE"] != "9223372036854775807.123456789" || data["NIL"] != nil || r.Key() != key || !r.Exists() {
		t.Fatal("native map bridge changed")
	}
	var row Row
	r2 := record.NewRecordWithData(record.NewKeyWithID("users", "a"), &row)
	if e := c.DB().Get(context.Background(), r2); e != nil {
		t.Fatal(e)
	}
	if row["VALUE"] == nil || *row["VALUE"] != data["VALUE"] {
		t.Fatal("Row native bridge changed")
	}
	missing := record.NewRecordWithData(record.NewKeyWithID("users", "missing"), &map[string]any{})
	if e := c.DB().Get(context.Background(), missing); e == nil || !record.IsNotFound(e) || missing.Exists() {
		t.Fatal("DAL missing state", e)
	}
}
func TestDALCapturesSliceAndKeysBeforeCallbacks(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	aData, bData, replacementData := map[string]any{}, map[string]any{}, map[string]any{}
	a := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("users", "a"), &aData)}
	b := record.NewRecordWithData(record.NewKeyWithID("users", "b"), &bData)
	replacement := record.NewRecordWithData(record.NewKeyWithID("users", "replacement"), &replacementData)
	records := []record.Record{a, b}
	a.set = func(e error) {
		if e == nil {
			records[1] = replacement
			b.Key().ID = "substituted"
			a.Key().ID = "different"
		}
	}
	if e := c.DB().GetMulti(context.Background(), records); e != nil {
		t.Fatal(e)
	}
	if aData["ID"] != "a" || bData["ID"] != "b" || len(replacementData) != 0 {
		t.Fatal("admitted record or key substituted")
	}
}
func TestDALKeyCancellationStopsLaterCallbacks(t *testing.T) {
	c, count := newFixture(t, nativeHandler(t), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var laterKey, data atomic.Bool
	first := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("users", "a"), &map[string]any{})}
	first.key = func() *record.Key { cancel(); return first.Record.Key() }
	second := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("users", "b"), &map[string]any{}), key: func() *record.Key { laterKey.Store(true); return record.NewKeyWithID("users", "b") }, data: func() any { data.Store(true); return nil }}
	if e := c.DB().GetMulti(ctx, []record.Record{first, second}); !errors.Is(e, context.Canceled) || laterKey.Load() || data.Load() || count.Load() != 0 {
		t.Fatal("later callback/IO after cancellation", e)
	}
}
func TestDALClearErrorCancellationStopsData(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var data atomic.Bool
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("users", "a"), &map[string]any{}), set: func(e error) {
		if e == nil {
			cancel()
		}
	}, data: func() any { data.Store(true); return nil }}
	if e := c.DB().Get(ctx, r); !errors.Is(e, context.Canceled) || data.Load() || !errors.Is(r.Error(), context.Canceled) {
		t.Fatal("Data called after SetError cancellation", e)
	}
}
func TestDALDataCancellationNeverMapsIntoTarget(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	data := map[string]any{}
	var entered atomic.Bool
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("users", "a"), &data), data: func() any { entered.Store(true); cancel(); return &data }}
	if e := c.DB().Get(ctx, r); !errors.Is(e, context.Canceled) || !entered.Load() || len(data) != 0 || !errors.Is(r.Error(), context.Canceled) {
		t.Fatal("Data callback cancellation was followed by mutation", e)
	}
}
func TestDALFinalErrorCallbackPromotionConverges(t *testing.T) {
	c, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 403, map[string]any{"message": "SECRET"})
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var closed, caller atomic.Bool
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("users", "a"), &map[string]any{})}
	r.set = func(e error) {
		var x *Error
		if errors.As(e, &x) && x.Code == "remote" {
			closed.Store(true)
			_ = c.Close()
		}
		if errors.As(e, &x) && x.Code == "closed" {
			caller.Store(true)
			cancel()
		}
	}
	e := c.DB().Get(ctx, r)
	if !errors.Is(e, context.Canceled) || !errors.Is(r.Error(), context.Canceled) || !closed.Load() || !caller.Load() {
		t.Fatal("last classification callback ignored", e, r.Error())
	}
}
func TestDALDeadlineInsideFinalPopulatedCallback(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	data := map[string]any{}
	var entered atomic.Bool
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("users", "a"), &data)}
	r.set = func(e error) {
		if e == nil && data["ID"] == "a" {
			entered.Store(true)
			<-ctx.Done()
		}
	}
	if e := c.DB().Get(ctx, r); !errors.Is(e, context.DeadlineExceeded) || !entered.Load() || !errors.Is(r.Error(), context.DeadlineExceeded) {
		t.Fatal("final callback deadline not observed", e)
	}
}
func TestRecordsAndRecordsetReadersNativeColumns(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	r, e := c.DB().ExecuteQueryToRecordsReader(context.Background(), query())
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	row, e := r.Next()
	if e != nil {
		t.Fatal(e)
	}
	if row.Key().ID != "a" {
		t.Fatal("reader native key")
	}
	data := row.Data().(*map[string]any)
	if (*data)["VALUE"] != "9223372036854775807.123456789" {
		t.Fatal("reader lexeme")
	}
	if _, e = r.Next(); !errors.Is(e, dal.ErrNoMoreRecords) {
		t.Fatal(e)
	}
	if _, e = r.Cursor(); !errors.Is(e, dal.ErrNotSupported) {
		t.Fatal("portable cursor claimed")
	}
	rs, e := c.DB().ExecuteQueryToRecordsetReader(context.Background(), query(), recordset.WithName("observed"))
	if e != nil {
		t.Fatal(e)
	}
	defer rs.Close()
	rr, set, e := rs.Next()
	if e != nil {
		t.Fatal(e)
	}
	values, e := rr.Data(set)
	if e != nil || !reflect.DeepEqual(values, []any{"a", "9223372036854775807.123456789", nil}) || set.GetColumnByName("VALUE").DbType() != "fixed" || set.Name() != "observed" {
		t.Fatal("recordset native strings/types", values, e)
	}
	columns := rs.(interface{ Columns() []Column }).Columns()
	*columns[1].Precision = 1
	if *rs.(interface{ Columns() []Column }).Columns()[1].Precision != 38 {
		t.Fatal("metadata alias")
	}
}
func TestReaderFactoryCancellationDoesNotOpenReader(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var factory atomic.Bool
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where().SelectIntoRecord(func() record.Record { factory.Store(true); cancel(); return nil })
	r, e := c.DB().ExecuteQueryToRecordsReader(ctx, q)
	if !errors.Is(e, context.Canceled) || r != nil || !factory.Load() {
		t.Fatal("factory cancellation delivered reader", e)
	}
	waitIdle(t, c)
}
func TestReaderNextLateFactoryKeepsUnacknowledgedIndex(t *testing.T) {
	c, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var v any
		if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
			t.Error(e)
		}
		writeJSON(t, w, 200, payload([][]any{{"a", "v", nil}, {"b", "w", nil}}))
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int64
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where().SelectIntoRecord(func() record.Record {
		if calls.Add(1) == 2 {
			cancel()
		}
		return nil
	})
	reader, e := c.DB().ExecuteQueryToRecordsReader(ctx, q)
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	if _, e = reader.Next(); e != nil {
		t.Fatal(e)
	}
	if row, e := reader.Next(); row != nil || !errors.Is(e, context.Canceled) {
		t.Fatal("late factory row delivered", e)
	}
	if reader.(*recordReader).index != 1 {
		t.Fatal("failed row ACK advanced")
	}
}
func TestNativePercentIDReadAndDALgoKeyLimitAreSeparate(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	row, e := c.Get(context.Background(), "users", "a%2Fb")
	if e != nil || row.ID != "a%2Fb" {
		t.Fatal("native ID was rewritten", e)
	}
}
func TestReaderLifetimeCancellationHasNoAcknowledgedPrefix(t *testing.T) {
	c, _ := newFixture(t, nativeHandler(t), nil)
	ctx, cancel := context.WithCancel(context.Background())
	reader, e := c.DB().ExecuteQueryToRecordsReader(ctx, query())
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	cancel()
	if row, e := reader.Next(); row != nil || !errors.Is(e, context.Canceled) || reader.(*recordReader).index != 0 {
		t.Fatal("canceled reader acknowledged data", e)
	}
}

func TestRecordsetZeroMultipleNullableRowsAndIndependentReaders(t *testing.T) {
	for _, size := range []int{0, 2} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			want := [][]any{}
			if size == 2 {
				want = [][]any{{"a", "9223372036854775807.123456789", nil}, {"b", nil, "null"}}
			}
			c, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
				var input any
				if e := json.NewDecoder(r.Body).Decode(&input); e != nil {
					t.Error(e)
				}
				writeJSON(t, w, 200, payload(want))
			}, nil)
			open := func() dal.RecordsetReader {
				reader, e := c.DB().ExecuteQueryToRecordsetReader(context.Background(), query())
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() {
					if e := reader.Close(); e != nil {
						t.Error(e)
					}
				})
				return reader
			}
			first, second := open(), open()
			rs := first.Recordset()
			if rs.RowsCount() != size || rs.ColumnsCount() != 3 || rs.GetColumnByName("VALUE").DbType() != "fixed" {
				t.Fatal("recordset dimensions or native type changed")
			}
			for _, col := range rs.Columns() {
				if len(col.Values()) != size {
					t.Fatal("column did not admit each row")
				}
			}
			for i, expected := range want {
				row, set, e := first.Next()
				if e != nil {
					t.Fatal(e)
				}
				got, e := row.Data(set)
				if e != nil || !reflect.DeepEqual(got, expected) {
					t.Fatal("STRING/null cells changed", i, got, e)
				}
			}
			if row, _, e := first.Next(); row != nil || !errors.Is(e, dal.ErrNoMoreRecords) {
				t.Fatal("incorrect recordset EOF", e)
			}
			columns := first.(interface{ Columns() []Column }).Columns()
			if columns[1].Precision == nil || *columns[1].Precision != 38 || columns[1].Scale == nil || *columns[1].Scale != 9 || columns[1].Nullable != nil {
				t.Fatal("native descriptors changed")
			}
			*columns[1].Precision = 1
			if *second.(interface{ Columns() []Column }).Columns()[1].Precision != 38 {
				t.Fatal("independent reader metadata aliased")
			}
			if size > 0 {
				if e := rs.GetRow(0).SetValueByName("VALUE", "changed", rs); e != nil {
					t.Fatal(e)
				}
				row, set, e := second.Next()
				if e != nil {
					t.Fatal(e)
				}
				got, e := row.Data(set)
				if e != nil || !reflect.DeepEqual(got, want[0]) {
					t.Fatal("independent reader values aliased", got, e)
				}
			}
		})
	}
}
