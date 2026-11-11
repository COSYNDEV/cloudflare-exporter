package collector

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/cosyndev/cloudflare-exporter/internal/cloudflare"
	"github.com/prometheus/client_golang/prometheus"
)

type window struct{ from, to time.Time }

type increment struct {
	counter *prometheus.CounterVec
	labels  []string
	value   float64
}

// fetch must not touch metrics: the scheduler applies deltas only after the whole window succeeds.
type windowedSource interface {
	name() string
	fetch(ctx context.Context, w window) (deltas []increment, truncated bool, err error)
}

type snapshotSource interface {
	name() string
	refresh(ctx context.Context, now time.Time) (truncated bool, err error)
}

type Zone struct {
	ID           string
	Name         string
	ProxiedHosts []string
}

type zoneSet struct {
	tags  []string
	names map[string]string
	hosts map[string]hostMatcher
}

func newZoneSet(zones []Zone) zoneSet {
	s := zoneSet{names: map[string]string{}, hosts: map[string]hostMatcher{}}
	for _, z := range zones {
		s.tags = append(s.tags, z.ID)
		s.names[z.ID] = z.Name
		s.hosts[z.ID] = newHostMatcher(z.ProxiedHosts)
	}
	return s
}

type httpRequests struct {
	cf       *cloudflare.Client
	zones    zoneSet
	requests *prometheus.CounterVec
	bytes    *prometheus.CounterVec
}

func (httpRequests) name() string { return "http_requests" }

func (s httpRequests) fetch(ctx context.Context, w window) ([]increment, bool, error) {
	zones, truncated, err := s.cf.HTTPRequests(ctx, s.zones.tags, w.from, w.to)
	if err != nil {
		return nil, false, err
	}
	var out []increment
	for _, z := range zones {
		for _, r := range z.Rows {
			l := []string{s.zones.names[z.ZoneTag], s.zones.hosts[z.ZoneTag].label(r.Dimensions.Host), strconv.Itoa(r.Dimensions.Status), r.Dimensions.CacheStatus}
			out = append(out,
				increment{s.requests, l, float64(r.Count)},
				increment{s.bytes, l, float64(r.Sum.EdgeResponseBytes)})
		}
	}
	return out, truncated, nil
}

type firewallEvents struct {
	cf     *cloudflare.Client
	zones  zoneSet
	events *prometheus.CounterVec
}

func (firewallEvents) name() string { return "firewall_events" }

func (s firewallEvents) fetch(ctx context.Context, w window) ([]increment, bool, error) {
	zones, truncated, err := s.cf.FirewallEvents(ctx, s.zones.tags, w.from, w.to)
	if err != nil {
		return nil, false, err
	}
	var out []increment
	for _, z := range zones {
		for _, e := range z.Rows {
			out = append(out, increment{s.events, []string{s.zones.names[z.ZoneTag], e.Action, e.Source}, float64(max(e.SampleInterval, 1))})
		}
	}
	return out, truncated, nil
}

type workers struct {
	cf                                 *cloudflare.Client
	account                            string
	requests, errors, subrequests, cpu *prometheus.CounterVec
}

func (workers) name() string { return "workers" }

func (s workers) fetch(ctx context.Context, w window) ([]increment, bool, error) {
	rows, truncated, err := s.cf.WorkersInvocations(ctx, s.account, w.from, w.to)
	if err != nil {
		return nil, false, err
	}
	var out []increment
	for _, r := range rows {
		l := []string{r.Dimensions.Script, r.Dimensions.Status}
		out = append(out,
			increment{s.requests, l, float64(r.Sum.Requests)},
			increment{s.errors, l, float64(r.Sum.Errors)},
			increment{s.subrequests, l, float64(r.Sum.Subrequests)},
			increment{s.cpu, l, float64(r.Sum.CPUTimeUs) / 1e6})
	}
	return out, truncated, nil
}

type r2Operations struct {
	cf         *cloudflare.Client
	account    string
	operations *prometheus.CounterVec
}

func (r2Operations) name() string { return "r2_operations" }

func (s r2Operations) fetch(ctx context.Context, w window) ([]increment, bool, error) {
	rows, truncated, err := s.cf.R2Operations(ctx, s.account, w.from, w.to)
	if err != nil {
		return nil, false, err
	}
	out := make([]increment, 0, len(rows))
	for _, r := range rows {
		l := []string{r.Dimensions.Bucket, r.Dimensions.Action, strconv.Itoa(r.Dimensions.StatusCode)}
		out = append(out, increment{s.operations, l, float64(r.Sum.Requests)})
	}
	return out, truncated, nil
}

var (
	r2BytesDesc   = prometheus.NewDesc("cloudflare_r2_stored_bytes", "Bytes stored per R2 bucket and storage class.", []string{"bucket", "storage_class"}, nil)
	r2ObjectsDesc = prometheus.NewDesc("cloudflare_r2_objects", "Objects stored per R2 bucket and storage class.", []string{"bucket", "storage_class"}, nil)
)

const (
	r2StorageLookback = 6 * time.Hour
	r2StorageMaxAge   = 15 * time.Minute
)

type storageKey struct{ bucket, class string }

type storageSample struct {
	at             time.Time
	bytes, objects float64
}

// r2Storage is a Collector over the latest snapshot, so buckets absent from a new
// snapshot disappear from the output without resetting a GaugeVec mid-scrape.
type r2Storage struct {
	cf      *cloudflare.Client
	account string
	now     func() time.Time

	mu        sync.Mutex
	snapshot  map[storageKey]storageSample
	refreshed time.Time
}

func (*r2Storage) name() string { return "r2_storage" }

// A truncated response is still used: rows are newest first, so only buckets whose
// newest sample is older than the cut-off go missing.
func (s *r2Storage) refresh(ctx context.Context, now time.Time) (bool, error) {
	rows, truncated, err := s.cf.R2Storage(ctx, s.account, now.Add(-r2StorageLookback))
	if err != nil {
		return false, err
	}
	snap := map[storageKey]storageSample{}
	for _, r := range rows {
		k := storageKey{r.Dimensions.Bucket, r.Dimensions.StorageClass}
		if cur, ok := snap[k]; ok && !r.Dimensions.Datetime.After(cur.at) {
			continue
		}
		snap[k] = storageSample{r.Dimensions.Datetime, float64(r.Max.PayloadSize), float64(r.Max.ObjectCount)}
	}
	s.mu.Lock()
	s.snapshot, s.refreshed = snap, now
	s.mu.Unlock()
	return truncated, nil
}

func (*r2Storage) Describe(ch chan<- *prometheus.Desc) {
	ch <- r2BytesDesc
	ch <- r2ObjectsDesc
}

func (s *r2Storage) Collect(ch chan<- prometheus.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.now().Sub(s.refreshed) > r2StorageMaxAge {
		return
	}
	for k, v := range s.snapshot {
		ch <- prometheus.MustNewConstMetric(r2BytesDesc, prometheus.GaugeValue, v.bytes, k.bucket, k.class)
		ch <- prometheus.MustNewConstMetric(r2ObjectsDesc, prometheus.GaugeValue, v.objects, k.bucket, k.class)
	}
}
