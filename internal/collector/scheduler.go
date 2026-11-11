package collector

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cosyndev/cloudflare-exporter/internal/cloudflare"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	maxCatchUp = time.Hour
	chunk      = 10 * time.Minute
)

type Config struct {
	AccountID string
	Zones     []Zone
	Interval  time.Duration
	Delay     time.Duration
	Now       func() time.Time
	Logger    *slog.Logger
}

type seriesKey struct {
	counter *prometheus.CounterVec
	labels  string
}

// tracked is owned by one goroutine per tick, so its fields need no lock.
type tracked struct {
	source    windowedSource
	watermark time.Time
	seen      map[seriesKey]struct{}
	pending   []increment
}

// apply defers deltas for series that did not exist before this call: the series is
// exported at 0 first, so rate() and increase() see its first increment.
func (t *tracked) apply(deltas []increment) {
	fresh := map[seriesKey]struct{}{}
	for _, d := range deltas {
		k := seriesKey{d.counter, strings.Join(d.labels, "\xff")}
		if _, ok := t.seen[k]; !ok {
			d.counter.WithLabelValues(d.labels...)
			t.seen[k] = struct{}{}
			fresh[k] = struct{}{}
		}
		if _, ok := fresh[k]; ok {
			t.pending = append(t.pending, d)
			continue
		}
		d.counter.WithLabelValues(d.labels...).Add(d.value)
	}
}

func (t *tracked) flush() {
	for _, d := range t.pending {
		d.counter.WithLabelValues(d.labels...).Add(d.value)
	}
	t.pending = nil
}

// Scheduler owns every watermark and is the only writer of the data counters, so a
// window is applied fully once or not at all and retried on the next tick.
type Scheduler struct {
	windowed  []*tracked
	snapshots []snapshotSource
	self      selfMetrics
	interval  time.Duration
	delay     time.Duration
	now       func() time.Time
	log       *slog.Logger
}

func New(reg prometheus.Registerer, cf *cloudflare.Client, cfg Config) *Scheduler {
	zones := newZoneSet(cfg.Zones)
	zoneLabels := []string{"zone", "host", "status", "cache_status"}
	workerLabels := []string{"script", "status"}
	storage := &r2Storage{cf: cf, account: cfg.AccountID, now: cfg.Now}
	reg.MustRegister(storage)
	windowed := []windowedSource{
		httpRequests{
			cf: cf, zones: zones,
			requests: counter(reg, "cloudflare_zone_requests_total", "Eyeball HTTP requests.", zoneLabels...),
			bytes:    counter(reg, "cloudflare_zone_response_bytes_total", "Edge response bytes for eyeball requests.", zoneLabels...),
		},
		firewallEvents{
			cf: cf, zones: zones,
			events: counter(reg, "cloudflare_zone_firewall_events_total", "Firewall events.", "zone", "action", "source"),
		},
		workers{
			cf: cf, account: cfg.AccountID,
			requests:    counter(reg, "cloudflare_worker_requests_total", "Worker invocations.", workerLabels...),
			errors:      counter(reg, "cloudflare_worker_errors_total", "Worker invocation errors.", workerLabels...),
			subrequests: counter(reg, "cloudflare_worker_subrequests_total", "Worker subrequests.", workerLabels...),
			cpu:         counter(reg, "cloudflare_worker_cpu_seconds_total", "Worker CPU time.", workerLabels...),
		},
		r2Operations{
			cf: cf, account: cfg.AccountID,
			operations: counter(reg, "cloudflare_r2_operations_total", "R2 operations.", "bucket", "action", "status_code"),
		},
	}
	return newScheduler(newSelfMetrics(reg), windowed, []snapshotSource{storage}, cfg)
}

func newScheduler(self selfMetrics, windowed []windowedSource, snapshots []snapshotSource, cfg Config) *Scheduler {
	s := &Scheduler{snapshots: snapshots, self: self, interval: cfg.Interval, delay: cfg.Delay, now: cfg.Now, log: cfg.Logger}
	start := s.end().Add(-cfg.Interval).Truncate(time.Minute)
	for _, src := range windowed {
		s.windowed = append(s.windowed, &tracked{source: src, watermark: start, seen: map[seriesKey]struct{}{}})
		s.self.watermark.WithLabelValues(src.name()).Set(float64(start.Unix()))
		s.self.preinit(src.name(), s.now())
	}
	for _, src := range snapshots {
		s.self.preinit(src.name(), s.now())
	}
	return s
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		s.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) end() time.Time {
	return s.now().Add(-s.delay).Truncate(time.Minute)
}

func (s *Scheduler) tick(ctx context.Context) {
	var wg sync.WaitGroup
	for _, t := range s.windowed {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(ctx, s.interval)
			defer cancel()
			s.advance(ctx, t)
		})
	}
	for _, src := range s.snapshots {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(ctx, s.interval)
			defer cancel()
			s.refresh(ctx, src)
		})
	}
	wg.Wait()
}

func (s *Scheduler) refresh(ctx context.Context, src snapshotSource) {
	name := src.name()
	truncated, err := src.refresh(ctx, s.now())
	if err != nil {
		s.fail(name, err)
		return
	}
	if truncated {
		s.self.truncated.WithLabelValues(name).Inc()
		s.log.Warn("snapshot hit the row limit", "source", name)
	}
	s.self.lastSuccess.WithLabelValues(name).Set(float64(s.now().Unix()))
}

func (s *Scheduler) advance(ctx context.Context, t *tracked) {
	t.flush()
	name := t.source.name()
	end := s.end()
	if gap := end.Sub(t.watermark); gap > maxCatchUp {
		t.watermark = end.Add(-maxCatchUp)
		s.self.skipped.WithLabelValues(name).Add((gap - maxCatchUp).Seconds())
	}
	for t.watermark.Before(end) {
		if err := ctx.Err(); err != nil {
			s.fail(name, err)
			return
		}
		to := t.watermark.Add(chunk)
		if to.After(end) {
			to = end
		}
		deltas, truncated, err := gather(ctx, t.source, window{t.watermark, to})
		if err != nil {
			s.fail(name, err)
			return
		}
		t.apply(deltas)
		s.self.truncated.WithLabelValues(name).Add(float64(truncated))
		t.watermark = to
		s.self.watermark.WithLabelValues(name).Set(float64(to.Unix()))
		s.self.lastSuccess.WithLabelValues(name).Set(float64(s.now().Unix()))
	}
}

func (s *Scheduler) fail(name string, err error) {
	s.self.errors.WithLabelValues(name).Inc()
	s.log.Warn("collection failed", "source", name, "err", err)
}

func gather(ctx context.Context, src windowedSource, w window) ([]increment, int, error) {
	deltas, truncated, err := src.fetch(ctx, w)
	if err != nil {
		return nil, 0, err
	}
	if !truncated {
		return deltas, 0, nil
	}
	span := w.to.Sub(w.from)
	if span <= time.Minute {
		return deltas, 1, nil
	}
	mid := w.from.Add(span / 2).Truncate(time.Minute)
	left, lt, err := gather(ctx, src, window{w.from, mid})
	if err != nil {
		return nil, 0, err
	}
	right, rt, err := gather(ctx, src, window{mid, w.to})
	if err != nil {
		return nil, 0, err
	}
	return append(left, right...), lt + rt, nil
}
