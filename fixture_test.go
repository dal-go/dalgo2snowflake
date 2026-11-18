package dalgo2snowflake

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

const account = "https://owned.snowflakecomputing.com"
const statementID = "11111111-2222-3333-4444-555555555555"

func newFixture(t *testing.T, handler http.HandlerFunc, configure func(*Options)) (*Client, *atomic.Int64) {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "owned.snowflakecomputing.com"}, DNSNames: []string{"owned.snowflakecomputing.com"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	cert, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	count := new(atomic.Int64)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		if r.Host != "owned.snowflakecomputing.com" || r.TLS == nil {
			t.Error("fixed verified authority changed")
		}
		handler(w, r)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	server.StartTLS()
	t.Cleanup(server.Close)
	dialer := &net.Dialer{Timeout: time.Second}
	options := Options{Database: "DB", Schema: "PUBLIC", Tables: map[string]Table{"users": {Name: "USERS", IDColumn: "ID"}}, Token: func(context.Context) (Credential, error) { return OAuthToken("synthetic-read-token"), nil }, Timeout: 4 * time.Second, ImmediatePolling: true, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr != "owned.snowflakecomputing.com:443" {
			t.Error("foreign dial authority")
		}
		return dialer.DialContext(ctx, network, server.Listener.Addr().String())
	}}}
	if configure != nil {
		configure(&options)
	}
	client, e := New(account, options)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := client.Close(); e != nil {
			t.Error(e)
		}
	})
	return client, count
}
func writeJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if e := json.NewEncoder(w).Encode(v); e != nil {
		t.Error("owned fixture JSON write failed")
	}
}
func payload(rows [][]any) map[string]any {
	return map[string]any{"statementHandle": statementID, "resultSetMetaData": map[string]any{"numRows": len(rows), "rowType": []any{map[string]any{"name": "ID", "type": "text", "nullable": false}, map[string]any{"name": "VALUE", "type": "fixed", "precision": 38, "scale": 9, "nullable": nil}, map[string]any{"name": "NIL", "type": "text"}}, "partitionInfo": []any{map[string]any{"rowCount": len(rows)}}}, "data": rows}
}
func nativeHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v2/statements" || r.Header.Get("Authorization") == "" {
			t.Error("unexpected native request")
			w.WriteHeader(400)
			return
		}
		var body struct {
			Statement, Database, Schema string
			Bindings                    map[string]binding
			Timeout                     int
		}
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
			t.Error(e)
			return
		}
		if body.Timeout <= 0 || body.Database != "DB" || body.Schema != "PUBLIC" {
			t.Error("fixed request scope")
		}
		id := "a"
		if b, ok := body.Bindings["1"]; ok {
			id = b.Value
		}
		rows := [][]any{{id, "9223372036854775807.123456789", nil}}
		if id == "missing" {
			rows = [][]any{}
		}
		writeJSON(t, w, 200, payload(rows))
	}
}
func query() dal.StructuredQuery {
	return dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().Where().SelectIntoRecordset()
}
func waitIdle(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		n := len(c.active)
		c.mu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("owned physical work did not settle")
		default:
			runtime.Gosched()
		}
	}
}
func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("owned event was not observed")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func closeSignal(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
