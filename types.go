// Package dalgo2snowflake provides bounded read-only access to existing tables
// using Snowflake's SQL API. Values remain the original JSONv2 STRING or NULL.
package dalgo2snowflake

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Error deliberately has no underlying cause, URL, SQL, token or server body.
type Error struct {
	Code   string
	Status int
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("snowflake: %s (HTTP %d)", e.Code, e.Status)
	}
	return "snowflake: " + e.Code
}
func failure(code string) error { return &Error{Code: code} }

// Credential can only be constructed as an explicitly typed supported token.
// Formatting never discloses its value; signing and private keys are not handled.
type Credential struct{ raw, kind string }

func (Credential) String() string      { return "[Snowflake credential]" }
func (Credential) GoString() string    { return "[Snowflake credential]" }
func OAuthToken(raw string) Credential { return Credential{raw: raw, kind: "OAUTH"} }
func KeyPairJWT(raw string) Credential { return Credential{raw: raw, kind: "KEYPAIR_JWT"} }

type TokenProvider func(context.Context) (Credential, error)

// Table maps a trusted application alias to one native table and ID column.
// Names are quoted as exact identifiers; dots do not imply qualification.
type Table struct{ Name, IDColumn string }
type Options struct {
	Database, Schema, Warehouse, Role string
	Tables                            map[string]Table
	Token                             TokenProvider
	// Transport is cloned and owned. TLS verification is mandatory. A custom
	// DialContext may map the fixed account authority to an owned test listener.
	// Proxy, cookies, redirects, compression and connection reuse are disabled.
	Transport                                                                *http.Transport
	Timeout                                                                  time.Duration
	MaxConcurrent, MaxKeys, MaxRows, MaxRequestBytes, MaxResponseBytes       int
	MaxPageBytes, MaxRequests, MaxPartitions, MaxColumns, MaxNodes, MaxPolls int
	// Zero selects 250ms. ImmediatePolling is an explicit fixture option.
	PollInterval     time.Duration
	ImmediatePolling bool
}

type Column struct {
	Name                                 string
	Type                                 string
	Precision, Scale, Length, ByteLength *int64
	Nullable                             *bool
}

// Row contains independently owned exact strings; nil is SQL NULL.
type Row map[string]*string
type Result struct {
	Rows    []Row
	Columns []Column
}
type Document struct {
	ID      string
	Row     Row
	Columns []Column
}
type Metadata struct {
	Tables           map[string]Table
	Provenance       string
	DynamicDiscovery bool
}

func (c *Client) ConfiguredMetadata() Metadata {
	tables := make(map[string]Table, len(c.options.Tables))
	for k, v := range c.options.Tables {
		tables[k] = v
	}
	return Metadata{Tables: tables, Provenance: "trusted configuration; not schema discovery or permission proof"}
}

func copyColumns(columns []Column) []Column {
	out := append([]Column(nil), columns...)
	for i := range out {
		x := &out[i]
		for _, p := range []**int64{&x.Precision, &x.Scale, &x.Length, &x.ByteLength} {
			if *p != nil {
				v := **p
				*p = &v
			}
		}
		if x.Nullable != nil {
			v := *x.Nullable
			x.Nullable = &v
		}
	}
	return out
}
func copyRow(row Row) Row {
	out := make(Row, len(row))
	for k, p := range row {
		if p == nil {
			out[k] = nil
		} else {
			v := *p
			out[k] = &v
		}
	}
	return out
}
