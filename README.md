# DALgo Snowflake native reads

Read existing Snowflake tables through the SQL API. This Go adapter does not
create a managed document layout or use the vendor Go/Arrow driver. Its public
dependencies are `github.com/dal-go/dalgo v0.97.0` and
`github.com/dal-go/record v0.1.5`.

## Configuration and reads

```go
client, err := dalgo2snowflake.New("https://your-account.snowflakecomputing.com", dalgo2snowflake.Options{
    Database: "EXISTING_DB", Schema: "PUBLIC",
    Warehouse: "READ_WH", Role: "READER",
    Tables: map[string]dalgo2snowflake.Table{
        "users": {Name: "EXISTING_USERS", IDColumn: "ID"},
    },
    Token: func(ctx context.Context) (dalgo2snowflake.Credential, error) {
        // Obtain a bounded read-scoped credential through trusted application
        // infrastructure. Respect ctx. No signing/private keys are handled here.
        return dalgo2snowflake.OAuthToken(os.Getenv("SNOWFLAKE_READ_TOKEN")), nil
    },
})
if err != nil { return err }
defer client.Close()
document, err := client.Get(ctx, "users", "9007199254740993")
```

`Get(ctx, collection, stringID)` returns an owned `*Document`, or nil for a
successful empty result. `Exists` uses the same point SELECT. `GetMany` preserves
order, duplicate inputs and missing entries; duplicate results are independent
copies. Empty batches perform no credential or HTTP calls. Keys and every batch
member are admitted before the first credential. IDs use their original string
value, including wide numeric-looking IDs. Point reads select at most two rows;
nonunique or mismatched identities fail the whole call.

Mappings are trusted configuration, not credential enforcement. A mapping
does not certify role privileges or prevent expensive server scans. Use an
appropriately restricted Snowflake role/token. `ConfiguredMetadata()` reports
only copied local mappings, explicitly without dynamic discovery or permission
verification. Construction and metadata access perform no I/O.

## Structured queries and readers

`Query(ctx, dal.Query)` accepts a closed `dal.StructuredQuery` subset. For example:

```go
q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().
    Where(dal.Comparison{
        Left: dal.DocumentID(), Operator: dal.Equal,
        Right: dal.Constant{Value: "9007199254740993"},
    }).OrderBy(dal.DescendingField("CREATED_AT")).Limit(20).
    SelectIntoRecordset()
result, err := client.Query(ctx, q)
```

The compiler quotes exact native table/field identifiers and parameterizes
values. Identifier dots are literal characters, not qualification. It supports
scalar comparisons, `!=`, AND/OR groups, bounded IN/NOT IN, equality/inequality
with NULL, plain field projections, ascending/descending `NULLS LAST`, and
checked nonnegative LIMIT/OFFSET. `dal.DocumentID()` maps to the configured ID
column; an application field literally named `__name__` stays that native field.
An ID tie-breaker is appended to ordering when absent. Empty or NULL-containing
membership, nonfinite binding numbers, arbitrary expressions/String methods,
joins, grouping, subqueries, relation scans and portable cursor input are refused
before auth. Explicit aliases must equal the selected native field name.

Bindings support string, bool, integer types, validated `json.Number` decimal
text and finite float values. Existing float precision cannot be recovered;
use integer or `json.Number` input when exact decimal text matters. No maps,
arbitrary serializers, date objects or custom scalar codecs are executed.

An absent limit selects `maxRows + 1` for overflow detection. Explicit limits
must fit `maxRows`; over-return fails rather than truncating. Cross-call OFFSET
does not establish a snapshot. All declared partitions, including empty
partitions, are materialized and validated before success. Counts, widths,
duplicate column names, identity consistency and required metadata fail closed.

`client.DB()` implements DALgo Get/Exists/GetMulti and real records/recordset
readers through this same compiler. Records readers require the ID in the
selected columns and validate unique string IDs. They do not silently add hidden
columns. A factory is prepared for the first row before the reader opens, then
called inline for later rows. Returned record keys reflect the original native
ID; factory targets retain their own data/error callbacks. Missing DALgo records
receive the ordinary DALgo not-found error/state.

The facade accepts `Row`, `*Row`, `map[string]any` and `*map[string]any` data
targets. It does not implicitly JSON-map structs. Native `Get` permits string
IDs containing `%`; DALgo's `record v0.1.5` key representation refuses that
reserved escape marker, so records-reader/key bridging has that separate core
limitation. Native point IDs are never rewritten to avoid it.

Readers retain the original deadline and client lifetime through `Next` and
caller callbacks. Always `Close` a reader to release its admission slot. A failed
`Next` does not advance its row index. `Cursor()` returns `dal.ErrNotSupported`:
there is no fabricated resumable cursor. Readers are for sequential use, not
concurrent Next/Close. A recordset is already materialized at successful open;
its native column types and ordinary `recordset.WithName` are preserved.

## Native fidelity

Snowflake JSONv2 represents result cells as STRING or NULL. `Row` is exactly
`map[string]*string`: nil is SQL NULL. No automatic integer, decimal, boolean,
timestamp, binary or VARIANT parsing occurs. Native values such as
`9223372036854775807`, decimal/exponent lexemes, nanosecond timestamp text,
binary encodings, JSON text, an empty string and the string `"null"` remain
unchanged and distinct from NULL. Map targets and recordsets expose string/nil,
never implicit float64 values.

`Result.Columns`/`Document.Columns` preserve native column name/type spelling
and optional precision/scale/length/byteLength int64 descriptors and nullable
bool. Unprovided or JSON-null metadata stays nil, not inferred zero/false.
Readers also expose copied descriptors through `interface{ Columns() []Column }`;
recordset `DbType()` preserves the native type. These are per-result observed
descriptors, not dynamic schema discovery or broad logical type support.

## Transport, cancellation and bounds

Only a configured HTTPS `*.snowflakecomputing.com` account origin is admitted.
No URL credentials, query token, path, explicit port, alternate status URL,
redirect, cookie jar, ambient proxy, compression, automatic retry or session
statement is used. The client clones an optional trusted `*http.Transport` and
TLS configuration; the original transport is not closed. TLS verification and
minimum TLS1.2 are mandatory. Custom DialTLS bypasses are refused. A trusted
DialContext may map the fixed authority to an owned fixture without disabling
certificate verification. Owned transport uses HTTP/1 and disables keep-alives
so idempotent GET retry on reused connections cannot hide physical attempts.

Credentials are explicit OAuth or pre-generated key-pair JWT values. They have
redacted formatting and are acquired per ordinary request under the original
operation. There is no anonymous/admin credential fallback. Transport, token,
body and remote errors expose only a safe code/optional HTTP status; no original
cause, server body, SQL, URL, key, credentials or custom cancellation cause is
included. Caller/deadline errors preserve `errors.Is` context sentinels.

One monotonic original timeout covers input/query callbacks, auth, submit,
polling/429, partitions, batch reads, conversion, body Close and final delivery.
Server timeout is the positive ceiling of remaining seconds; zero is never
sent. Caller cancellation has priority over local timeout, then client Close.
Caller record/query/factory callbacks remain synchronous and are checked after
each boundary, including every error-restoration callback. A callback that
blocks arbitrarily cannot safely be preempted; no worker mutates caller records
after return. Existing caller side effects are not rolled back.

For a validated owned pending UUID only, failure starts at most one fixed-origin
cancel POST using the **captured submit credential**, never a newly rotated
principal. It starts before outstanding response-body cleanup. A separate 2s
private scope bounds cleanup independently of the original caller. Matching
HTTP200 CancelStatus is only server-reported protocol confirmation; failure or
unknown cleanup never replaces the original local error. No handle is guessed,
and completed statements are not cancelled because later mapping failed.

Ignored token, RoundTrip, read, body Close or private-cancel work retains the
ordinary admission lease until actual physical settlement. Public cancellation
returns promptly; the slot remains busy instead of allowing unbounded detached
work. Only adapter-owned staging runs asynchronously. Client Close cancels owned
active operations and owned idle connections, without destroying the supplied
transport or waiting for hostile work.

Defaults (maximum configurable values in parentheses): timeout60s (120s),
operations4 (16), keys100 (1000), aggregate rows10000 (100000), aggregate request
bodies64KiB (1MiB), aggregate response bodies16MiB (32MiB), page4MiB (16MiB),
requests512 (2048), partitions128 (1024), columns512 (4096), JSON nodes65536
(262144), depth32, headers64KiB/1024, polls240 (1000) at250ms. Immediate polling
is an explicit fixture option. Byte admission counts escaped JSON before marshal
and every wire occurrence of repeated values. Oversize responses, exhausted
budgets and incomplete results fail as a whole; no successful prefix is returned.

These bounds do not cap warehouse scans/credits, server memory, TLS/transport
internal allocations or total process RAM. Caller-controlled synchronous work
and trusted transport internals remain outside preallocation guarantees.

## Scope and validation

Writes, callback transactions, arbitrary SQL, DDL/DML, multi-statements, schema
discovery, wider typed IDs, formulas/joins/grouping and portable snapshots are
**not implemented by this adapter**. Snowflake supports additional semantics;
these are not claims of engine limitations. Go DALgo signals absent operations
with its ordinary `dal.ErrNotSupported` interface sentinel.

Account-free tests use an owned verified-TLS SQL API fixture with synthetic
credentials and fixed-account DialContext mapping. They cover closed compiler
refusals/parameter fidelity, exact native rows/metadata/whole partitions,
after-dispatch caller/deadline cancellation, captured authority, real server
disconnect/reopen, ignored physical work, callback cancellation and final reader
delivery. This is protocol-contract proof, not hosted Snowflake execution,
warehouse cost, provider-role enforcement, browser/CORS, vendor-driver parity,
installed CLI or release evidence. CI runs race tests, vet, module integrity,
ordinary tidy-diff and formatting with pinned actions/Go; no automatic releases.

Official contracts: [submission](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests),
[JSONv2 responses](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses),
[SQL API reference](https://docs.snowflake.com/en/developer-guide/sql-api/reference),
[authentication](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating),
[owned cancellation](https://docs.snowflake.com/en/developer-guide/sql-api/cancelling-requests).
