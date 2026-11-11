package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cosyndev/cloudflare-exporter/internal/cloudflare"
)

func TestParseConfig(t *testing.T) {
	base := map[string]string{"CF_API_TOKEN": "t", "CF_ACCOUNT_ID": "a"}
	tests := []struct {
		name    string
		env     map[string]string
		want    config
		wantErr []string
	}{
		{
			name: "defaults",
			env:  base,
			want: config{token: "t", account: "a", listen: ":9184", interval: time.Minute, delay: 5 * time.Minute},
		},
		{
			name: "all set",
			env: map[string]string{
				"CF_API_TOKEN": "t", "CF_ACCOUNT_ID": "a", "CF_ZONES": " Cosyn.dev, ,easygpu.ai,cosyn.dev",
				"LISTEN_ADDRESS": ":9000", "COLLECT_INTERVAL": "30s", "COLLECT_DELAY": "10m",
			},
			want: config{
				token: "t", account: "a", zones: []string{"cosyn.dev", "easygpu.ai"},
				listen: ":9000", interval: 30 * time.Second, delay: 10 * time.Minute,
			},
		},
		{name: "missing required", env: map[string]string{}, wantErr: []string{"CF_API_TOKEN", "CF_ACCOUNT_ID"}},
		{name: "bad interval", env: with(base, "COLLECT_INTERVAL", "soon"), wantErr: []string{"COLLECT_INTERVAL"}},
		{name: "zero interval", env: with(base, "COLLECT_INTERVAL", "0s"), wantErr: []string{"COLLECT_INTERVAL must be positive"}},
		{name: "negative delay", env: with(base, "COLLECT_DELAY", "-1m"), wantErr: []string{"COLLECT_DELAY"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConfig(func(k string) string { return tt.env[k] })
			if len(tt.wantErr) == 0 {
				if err != nil || !reflect.DeepEqual(got, tt.want) {
					t.Fatalf("got %+v, %v", got, err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			for _, w := range tt.wantErr {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q missing %q", err, w)
				}
			}
		})
	}
}

func with(env map[string]string, k, v string) map[string]string {
	out := map[string]string{k: v}
	for key, val := range env {
		if key != k {
			out[key] = val
		}
	}
	return out
}

func TestSelectZones(t *testing.T) {
	all := []cloudflare.Zone{{ID: "1", Name: "cosyn.dev"}, {ID: "2", Name: "Easygpu.ai"}}
	if got, err := selectZones(all, nil); err != nil || len(got) != 2 {
		t.Fatalf("all: %v %v", got, err)
	}
	if got, err := selectZones(all, []string{"easygpu.ai"}); err != nil || len(got) != 1 || got[0].ID != "2" {
		t.Fatalf("subset: %v %v", got, err)
	}
	if _, err := selectZones(all, []string{"cosyn.dev", "nope.dev"}); err == nil || !strings.Contains(err.Error(), "nope.dev") {
		t.Fatalf("unknown: %v", err)
	}
}

var fixtures = map[string]string{
	"httpRequestsAdaptiveGroups": `{"data":{"viewer":{"zones":[{"zoneTag":"z1","rows":[{"count":2,"dimensions":{"cacheStatus":"dynamic","clientRequestHTTPHost":"www.cosyn.dev:2086","edgeResponseStatus":521},"sum":{"edgeResponseBytes":14678}}]}]}}}`,
	"firewallEventsAdaptive":     `{"data":{"viewer":{"zones":[{"zoneTag":"z1","rows":[{"action":"block","source":"firewallManaged"}]}]}}}`,
	"workersInvocationsAdaptive": `{"data":{"viewer":{"accounts":[{"rows":[{"dimensions":{"scriptName":"edge","status":"success"},"sum":{"cpuTimeUs":565574,"errors":0,"requests":382,"subrequests":382}}]}]}}}`,
	"r2OperationsAdaptiveGroups": `{"data":{"viewer":{"accounts":[{"rows":[{"dimensions":{"actionType":"PutObject","bucketName":"backups","responseStatusCode":200},"sum":{"requests":1}}]}]}}}`,
	"r2StorageAdaptiveGroups":    `{"data":{"viewer":{"accounts":[{"rows":[{"dimensions":{"bucketName":"backups","datetime":"2026-11-11T13:50:00Z","storageClass":"Standard"},"max":{"objectCount":618,"payloadSize":17960558}}]}]}}}`,
}

func fakeCloudflare(t *testing.T, dnsForbidden bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/zones/z1/dns_records" {
			if dnsForbidden {
				http.Error(w, `{"success":false,"errors":[{"code":10000,"message":"Authentication error"}]}`, http.StatusForbidden)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"result":[{"name":"www.cosyn.dev","type":"A","proxied":true}],"result_info":{"page":1,"total_pages":1}}`))
			return
		}
		if r.URL.Path == "/zones" {
			_, _ = w.Write([]byte(`{"success":true,"result":[{"id":"z1","name":"cosyn.dev"},{"id":"z2","name":"other.dev"}],"result_info":{"page":1,"total_pages":1}}`))
			return
		}
		var req struct{ Query string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		for key, body := range fixtures {
			if strings.Contains(req.Query, key) {
				_, _ = w.Write([]byte(body))
				return
			}
		}
		http.Error(w, "unexpected query", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = res.Body.Close() }()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(body)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func TestRunServesMetricsAndShutsDown(t *testing.T) {
	api := fakeCloudflare(t, false)
	addr := freeAddr(t)
	env := map[string]string{"CF_API_TOKEN": "secret-token", "CF_ACCOUNT_ID": "acc", "CF_ZONES": "cosyn.dev", "LISTEN_ADDRESS": addr, "COLLECT_INTERVAL": "100ms"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, slog.New(slog.DiscardHandler), func(k string) string { return env[k] }, api.URL)
	}()

	var body string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if code, b := get(t, "http://"+addr+"/metrics"); code == http.StatusOK && strings.Contains(b, `cloudflare_worker_requests_total{script="edge",status="success"} 382`) {
			body = b
			break
		}
	}
	for _, want := range []string{
		`cloudflare_zone_requests_total{cache_status="dynamic",host="www.cosyn.dev",status="521",zone="cosyn.dev"} 2`,
		`cloudflare_zone_response_bytes_total{cache_status="dynamic",host="www.cosyn.dev",status="521",zone="cosyn.dev"} 14678`,
		`cloudflare_zone_firewall_events_total{action="block",source="firewallManaged",zone="cosyn.dev"} 1`,
		`cloudflare_worker_requests_total{script="edge",status="success"} 382`,
		`cloudflare_worker_cpu_seconds_total{script="edge",status="success"} 0.565574`,
		`cloudflare_r2_operations_total{action="PutObject",bucket="backups",status_code="200"} 1`,
		`cloudflare_r2_stored_bytes{bucket="backups",storage_class="Standard"} 1.7960558e+07`,
		`cloudflare_r2_objects{bucket="backups",storage_class="Standard"} 618`,
		`cloudflare_exporter_build_info{goversion="go`,
		`cloudflare_exporter_watermark_timestamp_seconds{source="http_requests"}`,
		`cloudflare_exporter_last_success_timestamp_seconds{source="r2_storage"}`,
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %s", want)
		}
	}
	if strings.Contains(body, "secret-token") {
		t.Error("/metrics leaked the token")
	}
	for _, src := range []string{"http_requests", "firewall_events", "workers", "r2_operations", "r2_storage"} {
		if !strings.Contains(body, `cloudflare_exporter_errors_total{source="`+src+`"} 0`) {
			t.Errorf("errors_total{source=%q} is not 0", src)
		}
	}
	if code, _ := get(t, "http://"+addr+"/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d", code)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not shut down")
	}
}

func TestRunFailsFastOnUnknownZone(t *testing.T) {
	api := fakeCloudflare(t, false)
	env := map[string]string{"CF_API_TOKEN": "secret-token", "CF_ACCOUNT_ID": "acc", "CF_ZONES": "missing.dev"}
	err := run(context.Background(), slog.New(slog.DiscardHandler), func(k string) string { return env[k] }, api.URL)
	if err == nil || !strings.Contains(err.Error(), "missing.dev") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunFailsFastWithoutDNSRead(t *testing.T) {
	api := fakeCloudflare(t, true)
	env := map[string]string{"CF_API_TOKEN": "secret-token", "CF_ACCOUNT_ID": "acc", "CF_ZONES": "cosyn.dev"}
	err := run(context.Background(), slog.New(slog.DiscardHandler), func(k string) string { return env[k] }, api.URL)
	for _, want := range []string{"zone cosyn.dev", "Zone > DNS > Read", "HTTP 403", "Authentication error"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to contain %q", err, want)
		}
	}
}
