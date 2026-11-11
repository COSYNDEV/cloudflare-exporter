package cloudflare

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func server(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return New(srv.URL, "secret-token", "cloudflare-exporter/test")
}

func TestGraphQL(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{"happy path", 200, `{"data":{"viewer":{"ok":true}},"errors":null}`, ""},
		{"200 with errors", 200, `{"data":null,"errors":[{"message":"zone does not have access to the path"}]}`, "zone does not have access"},
		{"200 with errors and data", 200, `{"data":{"viewer":{"ok":true}},"errors":[{"message":"partial"}]}`, "partial"},
		{"non-200", 403, `{"success":false}`, "HTTP 403"},
		{"null data", 200, `{"data":null}`, "empty data"},
		{"bad json", 200, `not json`, "decode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := server(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/graphql" {
					t.Errorf("%s %s", r.Method, r.URL.Path)
				}
				if r.Header.Get("Authorization") != "Bearer secret-token" || r.Header.Get("User-Agent") != "cloudflare-exporter/test" {
					t.Errorf("headers = %v", r.Header)
				}
				var req struct {
					Query     string         `json:"query"`
					Variables map[string]any `json:"variables"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Query != "q" || req.Variables["a"] != "b" {
					t.Errorf("request = %+v, %v", req, err)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			})
			var out struct {
				Viewer struct {
					OK bool `json:"ok"`
				} `json:"viewer"`
			}
			err := c.GraphQL(context.Background(), "q", map[string]any{"a": "b"}, &out)
			if tt.wantErr == "" {
				if err != nil || !out.Viewer.OK {
					t.Fatalf("err=%v out=%+v", err, out)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "secret-token") {
				t.Fatal("token leaked into error")
			}
		})
	}
}

func TestZonesPaginates(t *testing.T) {
	c := server(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/zones" || q.Get("account.id") != "acc" || q.Get("per_page") != "50" {
			t.Errorf("request %s", r.URL)
		}
		page := q.Get("page")
		_, _ = fmt.Fprintf(w, `{"success":true,"result":[{"id":"id%s","name":"zone%s.dev","plan":{"name":"Free"}}],"result_info":{"page":%s,"total_pages":2}}`, page, page, page)
	})
	zones, err := c.Zones(context.Background(), "acc")
	if err != nil {
		t.Fatal(err)
	}
	want := []Zone{{"id1", "zone1.dev"}, {"id2", "zone2.dev"}}
	if fmt.Sprint(zones) != fmt.Sprint(want) {
		t.Fatalf("zones = %v", zones)
	}
}

func TestProxiedHostnames(t *testing.T) {
	c := server(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/zones/z1/dns_records" || q.Get("proxied") != "true" || q.Get("per_page") != "100" {
			t.Errorf("request %s", r.URL)
		}
		records := map[string]string{
			"1": `{"name":"WWW.vanessabarragao.com","type":"A","proxied":true},{"name":"mail.vanessabarragao.com","type":"A","proxied":false}`,
			"2": `{"name":"*.vanessabarragao.com","type":"CNAME","proxied":true}`,
		}[q.Get("page")]
		_, _ = fmt.Fprintf(w, `{"success":true,"errors":[],"result":[%s],"result_info":{"page":%s,"total_pages":2}}`, records, q.Get("page"))
	})
	got, err := c.ProxiedHostnames(context.Background(), "z1")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[www.vanessabarragao.com *.vanessabarragao.com]" {
		t.Fatalf("hosts = %v", got)
	}
}

func TestProxiedHostnamesAuthError(t *testing.T) {
	c := server(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":false,"errors":[{"code":10000,"message":"Authentication error"}],"result":null}`))
	})
	if _, err := c.ProxiedHostnames(context.Background(), "z1"); err == nil || !strings.Contains(err.Error(), "10000: Authentication error") {
		t.Fatalf("err = %v", err)
	}
}

func TestZonesFailure(t *testing.T) {
	c := server(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"success":false,"errors":[{"code":10000,"message":"Authentication error"}]}`, http.StatusForbidden)
	})
	if _, err := c.Zones(context.Background(), "acc"); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("err = %v", err)
	}
}

func TestZoneQueriesChunkByTen(t *testing.T) {
	var chunks [][]string
	c := server(t, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Variables struct {
				Zones []string `json:"zones"`
			} `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		chunks = append(chunks, req.Variables.Zones)
		var zones []string
		for _, z := range req.Variables.Zones {
			zones = append(zones, fmt.Sprintf(`{"zoneTag":%q,"rows":[{"action":"block","source":"waf"}]}`, z))
		}
		_, _ = fmt.Fprintf(w, `{"data":{"viewer":{"zones":[%s]}}}`, strings.Join(zones, ","))
	})
	var tags []string
	for i := range 11 {
		tags = append(tags, fmt.Sprintf("z%d", i))
	}
	from := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	got, truncated, err := c.FirewallEvents(context.Background(), tags, from, from.Add(time.Minute))
	if err != nil || truncated {
		t.Fatalf("err=%v truncated=%v", err, truncated)
	}
	if len(chunks) != 2 || len(chunks[0]) != 10 || len(chunks[1]) != 1 || len(got) != 11 {
		t.Fatalf("chunks=%v results=%d", chunks, len(got))
	}
}

func TestAccountNotVisible(t *testing.T) {
	c := server(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"viewer":{"accounts":[]}}}`))
	})
	if _, _, err := c.WorkersInvocations(context.Background(), "acc", time.Now(), time.Now()); err == nil {
		t.Fatal("expected error for missing account")
	}
}
