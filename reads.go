package dalgo2snowflake

import (
	"context"
	"github.com/dal-go/dalgo/dal"
)

func (c *Client) point(o *operation, p plan, id string) (*Document, error) {
	result, e := c.execute(o, p)
	if e != nil {
		return nil, e
	}
	if len(result.Rows) == 0 {
		return nil, nil
	}
	if len(result.Rows) != 1 {
		return nil, failure("identity")
	}
	nativeID := result.Rows[0][p.table.IDColumn]
	if nativeID == nil || *nativeID != id {
		return nil, failure("identity")
	}
	return &Document{ID: id, Row: result.Rows[0], Columns: result.Columns}, nil
}
func (c *Client) Get(ctx context.Context, collection, id string) (*Document, error) {
	o, e := c.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer o.finish()
	p, e := c.pointPlan(o, collection, id)
	if e != nil {
		return nil, e
	}
	row, e := c.point(o, p, id)
	if final := o.check(); final != nil {
		return nil, final
	}
	return row, e
}
func (c *Client) Exists(ctx context.Context, collection, id string) (bool, error) {
	row, e := c.Get(ctx, collection, id)
	return row != nil, e
}
func (c *Client) GetMany(ctx context.Context, collection string, ids []string) ([]*Document, error) {
	o, e := c.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer o.finish()
	if len(ids) > c.options.MaxKeys {
		return nil, failure("bounds")
	}
	ids = append([]string(nil), ids...)
	plans := make([]plan, len(ids))
	// Complete batch admission precedes the first credential; malformed later
	// keys cannot cause a successful prefix or a partial network journey.
	if _, ok := c.options.Tables[collection]; !ok {
		return nil, failure("mapping")
	}
	cost := 0
	for i, id := range ids {
		p, e := c.pointPlan(o, collection, id)
		if e != nil {
			return nil, e
		}
		body, e := o.body(p)
		if e != nil {
			return nil, e
		}
		if len(body) > c.options.MaxRequestBytes-cost {
			return nil, failure("bounds")
		}
		cost += len(body)
		plans[i] = p
	}
	rows := make([]*Document, len(ids))
	for i, id := range ids {
		rows[i], e = c.point(o, plans[i], id)
		if e != nil {
			return nil, e
		}
	}
	if e := o.check(); e != nil {
		return nil, e
	}
	return rows, nil
}

// Query accepts only the documented closed StructuredQuery subset. Result rows
// are fully materialized across every partition before a public success.
func (c *Client) Query(ctx context.Context, query dal.Query) (*Result, error) {
	o, e := c.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer o.finish()
	p, e := c.compile(o, query, false)
	if e != nil {
		return nil, e
	}
	result, e := c.execute(o, p)
	if final := o.check(); final != nil {
		return nil, final
	}
	return result, e
}
