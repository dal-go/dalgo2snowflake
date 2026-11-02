package dalgo2snowflake

import (
	"encoding/json"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

type binding struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}
type plan struct {
	collection string
	table      Table
	sql        string
	bindings   map[string]binding
	limit      int
	requireID  bool
}

func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

var numberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func scalar(value any) (binding, error) {
	switch v := value.(type) {
	case string:
		if !utf8.ValidString(v) {
			return binding{}, failure("input")
		}
		return binding{"TEXT", v}, nil
	case bool:
		return binding{"BOOLEAN", strconv.FormatBool(v)}, nil
	case int:
		return binding{"FIXED", strconv.FormatInt(int64(v), 10)}, nil
	case int8:
		return scalar(int64(v))
	case int16:
		return scalar(int64(v))
	case int32:
		return scalar(int64(v))
	case int64:
		return binding{"FIXED", strconv.FormatInt(v, 10)}, nil
	case uint:
		return scalar(uint64(v))
	case uint8:
		return scalar(uint64(v))
	case uint16:
		return scalar(uint64(v))
	case uint32:
		return scalar(uint64(v))
	case uint64:
		return binding{"FIXED", strconv.FormatUint(v, 10)}, nil
	case json.Number:
		if len(v) > 128 || !numberPattern.MatchString(string(v)) {
			return binding{}, failure("input")
		}
		return binding{"FIXED", string(v)}, nil
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return binding{}, failure("input")
		}
		return binding{"REAL", strconv.FormatFloat(float64(v), 'g', -1, 32)}, nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return binding{}, failure("input")
		}
		return binding{"REAL", strconv.FormatFloat(v, 'g', -1, 64)}, nil
	default:
		return binding{}, dal.ErrNotSupported
	}
}

// observe keeps arbitrary caller getters inline and checks before any later
// callback, credential or I/O. No caller code is abandoned on a goroutine.
func observe[T any](o *operation, f func() T) (T, error) {
	var zero T
	if e := o.check(); e != nil {
		return zero, e
	}
	v := f()
	if e := o.check(); e != nil {
		return zero, e
	}
	return v, nil
}
func (c *Client) compile(o *operation, query dal.Query, requireID bool) (plan, error) {
	q, ok := query.(dal.StructuredQuery)
	if !ok || q == nil {
		return plan{}, dal.ErrNotSupported
	}
	from, e := observe(o, q.From)
	if e != nil {
		return plan{}, e
	}
	if from == nil {
		return plan{}, dal.ErrNotSupported
	}
	base, e := observe(o, from.Base)
	if e != nil {
		return plan{}, e
	}
	joins, e := observe(o, from.Joins)
	if e != nil {
		return plan{}, e
	}
	if len(joins) != 0 {
		return plan{}, dal.ErrNotSupported
	}
	ref, ok := base.(dal.CollectionRef)
	if !ok || ref.Parent() != nil || ref.Schema() != "" || ref.Database() != "" || ref.Alias() != "" || ref.ScanLimit() != 0 || len(ref.ScanOrders()) != 0 {
		return plan{}, dal.ErrNotSupported
	}
	t, ok := c.options.Tables[ref.Name()]
	if !ok {
		return plan{}, failure("mapping")
	}
	p := plan{collection: ref.Name(), table: t, bindings: make(map[string]binding), requireID: requireID}
	groups, e := observe(o, q.GroupBy)
	if e != nil {
		return p, e
	}
	having, e := observe(o, q.Having)
	if e != nil {
		return p, e
	}
	start, e := observe(o, q.StartFrom)
	if e != nil {
		return p, e
	}
	after, e := observe(o, q.StartAfter)
	if e != nil {
		return p, e
	}
	if len(groups) != 0 || having != nil || start != "" || after != "" {
		return p, dal.ErrNotSupported
	}
	limit, e := observe(o, q.Limit)
	if e != nil {
		return p, e
	}
	offset, e := observe(o, q.Offset)
	if e != nil {
		return p, e
	}
	if limit < 0 || limit > c.options.MaxRows || offset < 0 || offset > 1000000000 {
		return p, failure("bounds")
	}
	p.limit = limit
	if p.limit == 0 {
		p.limit = c.options.MaxRows
	}
	sqlLimit := p.limit
	if limit == 0 {
		sqlLimit++
	}
	columns, e := observe(o, q.Columns)
	if e != nil {
		return p, e
	}
	if len(columns) > c.options.MaxColumns {
		return p, failure("bounds")
	}
	columns = append([]dal.Column(nil), columns...)
	sqlBytes := 128 + len(t.Name) + strings.Count(t.Name, `"`) + len(t.IDColumn) + strings.Count(t.IDColumn, `"`)
	field := func(expr dal.Expression) (string, error) {
		f, ok := expr.(dal.FieldRef)
		if !ok || f.Source() != "" {
			return "", dal.ErrNotSupported
		}
		n := f.Name()
		if f.IsID() {
			n = t.IDColumn
		}
		if !name(n) {
			return "", failure("input")
		}
		cost := len(n) + strings.Count(n, `"`) + 32
		if cost > c.options.MaxRequestBytes-sqlBytes {
			return "", failure("bounds")
		}
		sqlBytes += cost
		return n, nil
	}
	selection := "*"
	hasID := len(columns) == 0
	if len(columns) > 0 {
		selected := make([]string, 0, len(columns))
		seen := make(map[string]bool)
		for _, column := range columns {
			n, e := field(column.Expression)
			if e != nil {
				return p, e
			}
			if column.Alias != "" && column.Alias != n {
				return p, dal.ErrNotSupported
			}
			if seen[n] {
				return p, failure("input")
			}
			seen[n] = true
			hasID = hasID || n == t.IDColumn
			selected = append(selected, quote(n))
		}
		selection = strings.Join(selected, ", ")
	}
	if requireID && !hasID {
		return p, dal.ErrNotSupported
	}
	// Count each escaped binding occurrence before copying/marshalling; repeated
	// large shared values are charged repeatedly, not once by Go pointer identity.
	used := 0
	bind := func(value any) (string, error) {
		if raw, ok := value.(string); ok && len(raw) > c.options.MaxRequestBytes-used-64 {
			return "", failure("bounds")
		}
		b, e := scalar(value)
		if e != nil {
			return "", e
		}
		n, e := jsonStringSize(b.Value)
		if e != nil {
			return "", e
		}
		if n > c.options.MaxRequestBytes-used-64 {
			return "", failure("bounds")
		}
		used += n + 64
		if len(p.bindings) >= 256 {
			return "", failure("bounds")
		}
		id := strconv.Itoa(len(p.bindings) + 1)
		p.bindings[id] = b
		return "?", nil
	}
	condition, e := observe(o, q.Where)
	if e != nil {
		return p, e
	}
	nodes := 0
	var walk func(dal.Condition, int) (string, error)
	walk = func(condition dal.Condition, depth int) (string, error) {
		if e := o.check(); e != nil {
			return "", e
		}
		nodes++
		if nodes > 128 || depth > 8 {
			return "", failure("bounds")
		}
		switch v := condition.(type) {
		case nil:
			return "", nil
		case dal.GroupCondition:
			if v.Operator() != dal.And && v.Operator() != dal.Or {
				return "", dal.ErrNotSupported
			}
			children := v.Conditions()
			if len(children) == 0 || len(children) > 128 {
				return "", failure("input")
			}
			children = append([]dal.Condition(nil), children...)
			parts := make([]string, 0, len(children))
			for _, child := range children {
				if child == nil {
					return "", failure("input")
				}
				s, e := walk(child, depth+1)
				if e != nil {
					return "", e
				}
				parts = append(parts, s)
			}
			return "(" + strings.Join(parts, " "+string(v.Operator())+" ") + ")", nil
		case dal.Comparison:
			n, e := field(v.Left)
			if e != nil {
				return "", e
			}
			lhs := quote(n)
			if v.Operator == dal.In || v.Operator == dal.NotIn {
				a, ok := v.Right.(dal.Array)
				if !ok {
					return "", dal.ErrNotSupported
				}
				rv := reflect.ValueOf(a.Value)
				if !rv.IsValid() || rv.Kind() != reflect.Slice || rv.Len() == 0 || rv.Len() > 256 {
					return "", failure("input")
				}
				values := make([]any, rv.Len())
				for i := range values {
					values[i] = rv.Index(i).Interface()
				}
				terms := make([]string, 0, len(values))
				for _, v := range values {
					if v == nil {
						return "", failure("input")
					}
					s, e := bind(v)
					if e != nil {
						return "", e
					}
					terms = append(terms, s)
				}
				op := " IN "
				if v.Operator == dal.NotIn {
					op = " NOT IN "
				}
				return lhs + op + "(" + strings.Join(terms, ", ") + ")", nil
			}
			v2, ok := v.Right.(dal.Constant)
			if !ok {
				return "", dal.ErrNotSupported
			}
			op := ""
			switch v.Operator {
			case dal.Equal:
				op = "="
			case dal.Operator("!="):
				op = "<>"
			case dal.GreaterThen:
				op = ">"
			case dal.GreaterOrEqual:
				op = ">="
			case dal.LessThen:
				op = "<"
			case dal.LessOrEqual:
				op = "<="
			default:
				return "", dal.ErrNotSupported
			}
			if v2.Value == nil {
				if op == "=" {
					return lhs + " IS NULL", nil
				}
				if op == "<>" {
					return lhs + " IS NOT NULL", nil
				}
				return "", failure("input")
			}
			s, e := bind(v2.Value)
			return lhs + " " + op + " " + s, e
		default:
			return "", dal.ErrNotSupported
		}
	}
	where, e := walk(condition, 0)
	if e != nil {
		return p, e
	}
	orders, e := observe(o, q.OrderBy)
	if e != nil {
		return p, e
	}
	if len(orders) > 16 {
		return p, failure("bounds")
	}
	orders = append([]dal.OrderExpression(nil), orders...)
	order := make([]string, 0, len(orders)+1)
	ordered := make(map[string]bool)
	for _, v := range orders {
		if v == nil {
			return p, dal.ErrNotSupported
		}
		expr, e := observe(o, v.Expression)
		if e != nil {
			return p, e
		}
		n, e := field(expr)
		if e != nil {
			return p, e
		}
		desc, e := observe(o, v.Descending)
		if e != nil {
			return p, e
		}
		if ordered[n] {
			return p, failure("input")
		}
		ordered[n] = true
		direction := " ASC NULLS LAST"
		if desc {
			direction = " DESC NULLS LAST"
		}
		order = append(order, quote(n)+direction)
	}
	if !ordered[t.IDColumn] {
		order = append(order, quote(t.IDColumn)+" ASC NULLS LAST")
	}
	p.sql = "SELECT " + selection + " FROM " + quote(t.Name)
	if where != "" {
		p.sql += " WHERE " + where
	}
	p.sql += " ORDER BY " + strings.Join(order, ", ") + " LIMIT " + strconv.Itoa(sqlLimit) + " OFFSET " + strconv.Itoa(offset)
	if len(p.sql) > c.options.MaxRequestBytes {
		return p, failure("bounds")
	}
	if e := o.check(); e != nil {
		return p, e
	}
	return p, nil
}

func (c *Client) pointPlan(o *operation, collection, id string) (plan, error) {
	if e := o.check(); e != nil {
		return plan{}, e
	}
	t, ok := c.options.Tables[collection]
	if !ok {
		return plan{}, failure("mapping")
	}
	if id == "" || len(id) > c.options.MaxRequestBytes || !utf8.ValidString(id) {
		return plan{}, failure("key")
	}
	n, e := jsonStringSize(id)
	if e != nil || n > c.options.MaxRequestBytes-1024 {
		return plan{}, failure("bounds")
	}
	return plan{collection: collection, table: t, sql: "SELECT * FROM " + quote(t.Name) + " WHERE " + quote(t.IDColumn) + " = ? LIMIT 2", bindings: map[string]binding{"1": {"TEXT", id}}, limit: 2, requireID: true}, nil
}
