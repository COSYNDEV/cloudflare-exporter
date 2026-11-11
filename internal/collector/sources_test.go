package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cosyndev/cloudflare-exporter/internal/cloudflare"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type gqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

func fakeAPI(t *testing.T, responses map[string]string) (*cloudflare.Client, *[]gqlRequest) {
	t.Helper()
	var seen []gqlRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req gqlRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		seen = append(seen, req)
		for key, body := range responses {
			if strings.Contains(req.Query, key) {
				_, _ = w.Write([]byte(body))
				return
			}
		}
		http.Error(w, "unexpected query", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	return cloudflare.New(srv.URL, "token", "test"), &seen
}

var testZones = newZoneSet([]Zone{
	{ID: "z1", Name: "cosyn.dev", ProxiedHosts: []string{"www.cosyn.dev"}},
	{ID: "z2", Name: "vanessabarragao.com", ProxiedHosts: []string{"vanessabarragao.com"}},
})

var testWindow = window{at(12, 0), at(12, 1)}

func apply(deltas []increment) {
	for _, d := range deltas {
		d.counter.WithLabelValues(d.labels...).Add(d.value)
	}
}

func newCounter(name string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: name}, labels)
}

func TestHTTPRequestsFetch(t *testing.T) {
	cf, seen := fakeAPI(t, map[string]string{"httpRequestsAdaptiveGroups": `{"data":{"viewer":{"zones":[
		{"zoneTag":"z1","rows":[
			{"count":2,"dimensions":{"cacheStatus":"dynamic","clientRequestHTTPHost":"www.cosyn.dev:2086","edgeResponseStatus":521},"sum":{"edgeResponseBytes":14678}},
			{"count":5,"dimensions":{"cacheStatus":"hit","clientRequestHTTPHost":"WWW.cosyn.dev","edgeResponseStatus":200},"sum":{"edgeResponseBytes":100}},
			{"count":7,"dimensions":{"cacheStatus":"hit","clientRequestHTTPHost":"scanner.example","edgeResponseStatus":200},"sum":{"edgeResponseBytes":1}}]},
		{"zoneTag":"z2","rows":[
			{"count":1,"dimensions":{"cacheStatus":"miss","clientRequestHTTPHost":"vanessabarragao.com","edgeResponseStatus":404},"sum":{"edgeResponseBytes":9}},
			{"count":3,"dimensions":{"cacheStatus":"miss","clientRequestHTTPHost":"www.cosyn.dev","edgeResponseStatus":404},"sum":{"edgeResponseBytes":9}}]}
	]}}}`})
	src := httpRequests{
		cf: cf, zones: testZones,
		requests: newCounter("req", "zone", "host", "status", "cache_status"),
		bytes:    newCounter("bytes", "zone", "host", "status", "cache_status"),
	}
	deltas, truncated, err := src.fetch(context.Background(), testWindow)
	if err != nil || truncated {
		t.Fatalf("err=%v truncated=%v", err, truncated)
	}
	apply(deltas)
	for _, c := range []struct {
		vec    *prometheus.CounterVec
		labels []string
		want   float64
	}{
		{src.requests, []string{"cosyn.dev", "www.cosyn.dev", "521", "dynamic"}, 2},
		{src.bytes, []string{"cosyn.dev", "www.cosyn.dev", "521", "dynamic"}, 14678},
		{src.requests, []string{"cosyn.dev", "www.cosyn.dev", "200", "hit"}, 5},
		{src.requests, []string{"cosyn.dev", "other", "200", "hit"}, 7},
		{src.requests, []string{"vanessabarragao.com", "vanessabarragao.com", "404", "miss"}, 1},
		{src.requests, []string{"vanessabarragao.com", "other", "404", "miss"}, 3},
	} {
		if got := testutil.ToFloat64(c.vec.WithLabelValues(c.labels...)); got != c.want {
			t.Errorf("%v = %v, want %v", c.labels, got, c.want)
		}
	}
	vars := (*seen)[0].Variables
	if vars["from"] != "2026-11-11T12:00:00Z" || vars["to"] != "2026-11-11T12:01:00Z" || vars["limit"] != float64(cloudflare.Limit) {
		t.Errorf("variables = %v", vars)
	}
}

func TestFirewallEventsFetch(t *testing.T) {
	cf, seen := fakeAPI(t, map[string]string{"firewallEventsAdaptive": `{"data":{"viewer":{"zones":[
		{"zoneTag":"z2","rows":[
			{"action":"block","source":"firewallManaged","datetime":"2026-11-11T03:44:53Z","clientRequestHTTPHost":"cpcalendars.vanessabarragao.com"},
			{"action":"block","source":"firewallManaged","sampleInterval":10},
			{"action":"managed_challenge","source":"securitylevel","sampleInterval":0}]}
	]}}}`})
	src := firewallEvents{cf: cf, zones: testZones, events: newCounter("fw", "zone", "action", "source")}
	deltas, _, err := src.fetch(context.Background(), testWindow)
	if err != nil {
		t.Fatal(err)
	}
	apply(deltas)
	if got := testutil.ToFloat64(src.events.WithLabelValues("vanessabarragao.com", "block", "firewallManaged")); got != 11 {
		t.Errorf("block = %v", got)
	}
	if got := testutil.ToFloat64(src.events.WithLabelValues("vanessabarragao.com", "managed_challenge", "securitylevel")); got != 1 {
		t.Errorf("challenge = %v", got)
	}
	if !strings.Contains((*seen)[0].Query, "sampleInterval") {
		t.Error("query does not select sampleInterval")
	}
}

func TestWorkersFetch(t *testing.T) {
	cf, seen := fakeAPI(t, map[string]string{"workersInvocationsAdaptive": `{"data":{"viewer":{"accounts":[{"rows":[
		{"dimensions":{"scriptName":"deploy-platform-edge-easygpu","status":"success"},"sum":{"cpuTimeUs":565574,"errors":0,"requests":382,"subrequests":382}}
	]}]}}}`})
	l := []string{"script", "status"}
	src := workers{cf: cf, account: "acc", requests: newCounter("r", l...), errors: newCounter("e", l...), subrequests: newCounter("s", l...), cpu: newCounter("c", l...)}
	deltas, _, err := src.fetch(context.Background(), testWindow)
	if err != nil {
		t.Fatal(err)
	}
	apply(deltas)
	labels := []string{"deploy-platform-edge-easygpu", "success"}
	for vec, want := range map[*prometheus.CounterVec]float64{src.requests: 382, src.errors: 0, src.subrequests: 382, src.cpu: 0.565574} {
		if got := testutil.ToFloat64(vec.WithLabelValues(labels...)); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	if (*seen)[0].Variables["account"] != "acc" {
		t.Errorf("account variable = %v", (*seen)[0].Variables["account"])
	}
}

func TestR2OperationsFetch(t *testing.T) {
	cf, _ := fakeAPI(t, map[string]string{"r2OperationsAdaptiveGroups": `{"data":{"viewer":{"accounts":[{"rows":[
		{"dimensions":{"actionType":"PutObject","bucketName":"eu_metabase-backups","responseStatusCode":200},"sum":{"requests":1}},
		{"dimensions":{"actionType":"ListBuckets","bucketName":"","responseStatusCode":200},"sum":{"requests":3}}
	]}]}}}`})
	src := r2Operations{cf: cf, account: "acc", operations: newCounter("ops", "bucket", "action", "status_code")}
	deltas, _, err := src.fetch(context.Background(), testWindow)
	if err != nil {
		t.Fatal(err)
	}
	apply(deltas)
	if got := testutil.ToFloat64(src.operations.WithLabelValues("eu_metabase-backups", "PutObject", "200")); got != 1 {
		t.Errorf("put = %v", got)
	}
	if got := testutil.ToFloat64(src.operations.WithLabelValues("", "ListBuckets", "200")); got != 3 {
		t.Errorf("list = %v", got)
	}
}

func TestFetchReportsTruncationAtLimit(t *testing.T) {
	rows := strings.Repeat(`{"dimensions":{"actionType":"GetObject","bucketName":"b","responseStatusCode":200},"sum":{"requests":1}},`, cloudflare.Limit)
	cf, _ := fakeAPI(t, map[string]string{"r2OperationsAdaptiveGroups": fmt.Sprintf(`{"data":{"viewer":{"accounts":[{"rows":[%s]}]}}}`, strings.TrimSuffix(rows, ","))})
	src := r2Operations{cf: cf, account: "acc", operations: newCounter("ops", "bucket", "action", "status_code")}
	deltas, truncated, err := src.fetch(context.Background(), testWindow)
	if err != nil || !truncated || len(deltas) != cloudflare.Limit {
		t.Fatalf("err=%v truncated=%v deltas=%d", err, truncated, len(deltas))
	}
}

func TestR2StorageKeepsLatestAndDropsAbsentBuckets(t *testing.T) {
	responses := map[string]string{"r2StorageAdaptiveGroups": `{"data":{"viewer":{"accounts":[{"rows":[
		{"dimensions":{"bucketName":"eu_vaultwarden-backups","datetime":"2026-11-11T13:40:00Z","storageClass":"Standard"},"max":{"objectCount":600,"payloadSize":17000000}},
		{"dimensions":{"bucketName":"eu_vaultwarden-backups","datetime":"2026-11-11T13:50:00Z","storageClass":"Standard"},"max":{"objectCount":618,"payloadSize":17960558}},
		{"dimensions":{"bucketName":"eu_metabase-backups","datetime":"2026-11-11T13:50:00Z","storageClass":"InfrequentAccess"},"max":{"objectCount":3,"payloadSize":42}}
	]}]}}}`}
	cf, seen := fakeAPI(t, responses)
	clk := &clock{at(14, 0)}
	src := &r2Storage{cf: cf, account: "acc", now: clk.now}
	if _, err := src.refresh(context.Background(), clk.t); err != nil {
		t.Fatal(err)
	}
	if (*seen)[0].Variables["from"] != "2026-11-11T08:00:00Z" {
		t.Errorf("lookback from = %v", (*seen)[0].Variables["from"])
	}
	want := `
# HELP cloudflare_r2_objects Objects stored per R2 bucket and storage class.
# TYPE cloudflare_r2_objects gauge
cloudflare_r2_objects{bucket="eu_metabase-backups",storage_class="InfrequentAccess"} 3
cloudflare_r2_objects{bucket="eu_vaultwarden-backups",storage_class="Standard"} 618
# HELP cloudflare_r2_stored_bytes Bytes stored per R2 bucket and storage class.
# TYPE cloudflare_r2_stored_bytes gauge
cloudflare_r2_stored_bytes{bucket="eu_metabase-backups",storage_class="InfrequentAccess"} 42
cloudflare_r2_stored_bytes{bucket="eu_vaultwarden-backups",storage_class="Standard"} 1.7960558e+07
`
	if err := testutil.CollectAndCompare(src, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}

	responses["r2StorageAdaptiveGroups"] = `{"data":{"viewer":{"accounts":[{"rows":[
		{"dimensions":{"bucketName":"eu_vaultwarden-backups","datetime":"2026-11-11T14:00:00Z","storageClass":"Standard"},"max":{"objectCount":619,"payloadSize":17960600}}
	]}]}}}`
	clk.add(time.Minute)
	if _, err := src.refresh(context.Background(), clk.t); err != nil {
		t.Fatal(err)
	}
	want = `
# HELP cloudflare_r2_objects Objects stored per R2 bucket and storage class.
# TYPE cloudflare_r2_objects gauge
cloudflare_r2_objects{bucket="eu_vaultwarden-backups",storage_class="Standard"} 619
`
	if err := testutil.CollectAndCompare(src, strings.NewReader(want), "cloudflare_r2_objects"); err != nil {
		t.Fatal(err)
	}

	responses["r2StorageAdaptiveGroups"] = `{"errors":[{"message":"rate limited"}]}`
	clk.add(time.Minute)
	if _, err := src.refresh(context.Background(), clk.t); err == nil {
		t.Fatal("expected error")
	}
	if err := testutil.CollectAndCompare(src, strings.NewReader(want), "cloudflare_r2_objects"); err != nil {
		t.Fatalf("failed refresh must keep the previous snapshot: %v", err)
	}
}

func TestR2StorageHidesStaleSnapshot(t *testing.T) {
	cf, _ := fakeAPI(t, map[string]string{"r2StorageAdaptiveGroups": `{"data":{"viewer":{"accounts":[{"rows":[
		{"dimensions":{"bucketName":"b","datetime":"2026-11-11T13:50:00Z","storageClass":"Standard"},"max":{"objectCount":1,"payloadSize":2}}
	]}]}}}`})
	clk := &clock{at(14, 0)}
	src := &r2Storage{cf: cf, account: "acc", now: clk.now}
	if _, err := src.refresh(context.Background(), clk.t); err != nil {
		t.Fatal(err)
	}
	clk.add(15 * time.Minute)
	if n := testutil.CollectAndCount(src); n != 2 {
		t.Fatalf("15m-old snapshot exported %d series, want 2", n)
	}
	clk.add(time.Second)
	if n := testutil.CollectAndCount(src); n != 0 {
		t.Fatalf("stale snapshot exported %d series", n)
	}
}

func TestR2StorageUsesTruncatedResponse(t *testing.T) {
	rows := strings.Repeat(`{"dimensions":{"bucketName":"b","datetime":"2026-11-11T13:50:00Z","storageClass":"Standard"},"max":{"objectCount":1,"payloadSize":2}},`, cloudflare.Limit)
	cf, _ := fakeAPI(t, map[string]string{"r2StorageAdaptiveGroups": fmt.Sprintf(`{"data":{"viewer":{"accounts":[{"rows":[%s]}]}}}`, strings.TrimSuffix(rows, ","))})
	clk := &clock{at(14, 0)}
	src := &r2Storage{cf: cf, account: "acc", now: clk.now}
	truncated, err := src.refresh(context.Background(), clk.t)
	if err != nil || !truncated || testutil.CollectAndCount(src) != 2 {
		t.Fatalf("err=%v truncated=%v series=%d", err, truncated, testutil.CollectAndCount(src))
	}
}

func TestHostMatcher(t *testing.T) {
	m := newHostMatcher([]string{"WWW.Cosyn.dev", "cosyn.dev", "*.easygpu.ai", "*.api.easygpu.ai", "[::1]"})
	tests := map[string]string{
		"www.cosyn.dev":         "www.cosyn.dev",
		"www.cosyn.dev:8443":    "www.cosyn.dev",
		"WWW.Cosyn.Dev:2086":    "www.cosyn.dev",
		"www.cosyn.dev.":        "www.cosyn.dev",
		"cosyn.dev":             "cosyn.dev",
		"smtp.cosyn.dev":        "other",
		"x.easygpu.ai":          "*.easygpu.ai",
		"a.b.easygpu.ai:443":    "*.easygpu.ai",
		"v1.api.easygpu.ai":     "*.api.easygpu.ai",
		"api.easygpu.ai":        "*.easygpu.ai",
		"easygpu.ai":            "other",
		"noteasygpu.ai":         "other",
		"x.easygpu.ai.evil.com": "other",
		"":                      "other",
		"[::1]:8080":            "[::1]",
	}
	for raw, want := range tests {
		if got := m.label(raw); got != want {
			t.Errorf("label(%q) = %q, want %q", raw, got, want)
		}
	}
	if got := newHostMatcher(nil).label("www.cosyn.dev"); got != "other" {
		t.Errorf("no records label = %q", got)
	}
}

func TestHostsMatchPerZone(t *testing.T) {
	zones := newZoneSet([]Zone{
		{ID: "a", Name: "a.dev", ProxiedHosts: []string{"*.a.dev"}},
		{ID: "b", Name: "b.dev", ProxiedHosts: []string{"www.b.dev"}},
	})
	if got := zones.hosts["a"].label("www.b.dev"); got != "other" {
		t.Errorf("zone a labeled zone b's host as %q", got)
	}
	if got := zones.hosts["b"].label("x.a.dev"); got != "other" {
		t.Errorf("zone b labeled zone a's host as %q", got)
	}
	if got := zones.hosts["a"].label("x.a.dev"); got != "*.a.dev" {
		t.Errorf("zone a label = %q", got)
	}
}
