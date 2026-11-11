package collector

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func at(h, m int) time.Time    { return time.Date(2026, 11, 11, h, m, 0, 0, time.UTC) }
func minutes(w window) float64 { return w.to.Sub(w.from).Minutes() }

func value(v *prometheus.CounterVec, label string) float64 {
	return testutil.ToFloat64(v.WithLabelValues(label))
}

type fakeSource struct {
	id        string
	counter   *prometheus.CounterVec
	err       error
	truncated func(window) bool
	failOn    func(window) bool
	onFetch   func(context.Context) error
	label     string
	calls     []window
}

func (f *fakeSource) name() string     { return f.id }
func (f *fakeSource) total() float64   { return value(f.counter, f.label) }
func (f *fakeSource) lastCall() window { return f.calls[len(f.calls)-1] }

func (f *fakeSource) fetch(ctx context.Context, w window) ([]increment, bool, error) {
	f.calls = append(f.calls, w)
	if f.onFetch != nil {
		if err := f.onFetch(ctx); err != nil {
			return nil, false, err
		}
	}
	if f.err != nil {
		return nil, false, f.err
	}
	if f.failOn != nil && f.failOn(w) {
		return nil, false, errors.New("boom")
	}
	truncated := f.truncated != nil && f.truncated(w)
	return []increment{{f.counter, []string{f.label}, minutes(w)}}, truncated, nil
}

type fakeSnapshot struct {
	truncated bool
	err       error
}

func (fakeSnapshot) name() string { return "snap" }
func (f fakeSnapshot) refresh(context.Context, time.Time) (bool, error) {
	return f.truncated, f.err
}

func setup(t *testing.T, sources ...*fakeSource) (*Scheduler, *clock, selfMetrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	clk := &clock{at(12, 0).Add(30 * time.Second)}
	var windowed []windowedSource
	for _, s := range sources {
		s.counter = counter(reg, "fake_"+s.id+"_total", "fake", "l")
		windowed = append(windowed, s)
	}
	self := newSelfMetrics(reg)
	cfg := Config{Interval: time.Minute, Delay: 5 * time.Minute, Now: clk.now, Logger: slog.New(slog.DiscardHandler)}
	s := newScheduler(self, windowed, []snapshotSource{fakeSnapshot{}}, cfg)
	for i, src := range sources {
		if src.label == "" {
			src.label = "x"
			s.windowed[i].seen[seriesKey{src.counter, "x"}] = struct{}{}
		}
	}
	return s, clk, self
}

func TestSuccessAppliesEachWindowOnce(t *testing.T) {
	src := &fakeSource{id: "a"}
	s, clk, self := setup(t, src)

	s.tick(context.Background())
	if got := src.lastCall(); got != (window{at(11, 54), at(11, 55)}) {
		t.Fatalf("first window = %v", got)
	}
	s.tick(context.Background())
	if len(src.calls) != 1 || src.total() != 1 {
		t.Fatalf("same end must not refetch: calls=%d total=%v", len(src.calls), src.total())
	}
	clk.add(time.Minute)
	s.tick(context.Background())
	if src.total() != 2 || src.lastCall() != (window{at(11, 55), at(11, 56)}) {
		t.Fatalf("total=%v last=%v", src.total(), src.lastCall())
	}
	if got := testutil.ToFloat64(self.watermark.WithLabelValues("a")); got != float64(at(11, 56).Unix()) {
		t.Fatalf("watermark gauge = %v", got)
	}
	if got := testutil.ToFloat64(self.lastSuccess.WithLabelValues("a")); got != float64(clk.t.Unix()) {
		t.Fatalf("last success = %v", got)
	}
}

func TestErrorAppliesNothingAndRetriesSameWindow(t *testing.T) {
	src := &fakeSource{id: "a"}
	s, clk, self := setup(t, src)

	src.err = errors.New("boom")
	s.tick(context.Background())
	if src.total() != 0 || value(self.errors, "a") != 1 {
		t.Fatalf("after error: total=%v errors=%v", src.total(), value(self.errors, "a"))
	}
	clk.add(2 * time.Minute)
	src.err = nil
	s.tick(context.Background())
	if got := src.lastCall(); got != (window{at(11, 54), at(11, 57)}) {
		t.Fatalf("retry window = %v", got)
	}
	if src.total() != 3 {
		t.Fatalf("total after recovery = %v, want 3", src.total())
	}
}

func TestCatchUpCap(t *testing.T) {
	src := &fakeSource{id: "a"}
	s, clk, self := setup(t, src)

	clk.add(3 * time.Hour)
	s.tick(context.Background())
	if got := src.lastCall(); len(src.calls) != 6 || got != (window{at(14, 45), at(14, 55)}) {
		t.Fatalf("calls = %v", src.calls)
	}
	if src.total() != 60 {
		t.Fatalf("total = %v", src.total())
	}
	if got := value(self.skipped, "a"); got != (2*time.Hour + time.Minute).Seconds() {
		t.Fatalf("skipped = %v", got)
	}
}

func TestTruncationSplits(t *testing.T) {
	tests := []struct {
		name          string
		gap           time.Duration
		truncated     func(window) bool
		wantTotal     float64
		wantTruncated float64
		maxLeaf       float64
	}{
		{"split until under limit", 7 * time.Minute, func(w window) bool { return minutes(w) > 2 }, 8, 0, 2},
		{"one-minute windows accepted", 2 * time.Minute, func(window) bool { return true }, 3, 3, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &fakeSource{id: "a", truncated: tt.truncated}
			s, clk, self := setup(t, src)
			clk.add(tt.gap)
			s.tick(context.Background())
			if src.total() != tt.wantTotal {
				t.Fatalf("total = %v, want %v (windows %v)", src.total(), tt.wantTotal, src.calls)
			}
			if got := value(self.truncated, "a"); got != tt.wantTruncated {
				t.Fatalf("truncated windows = %v, want %v", got, tt.wantTruncated)
			}
			for _, w := range src.calls {
				if w.from.Second() != 0 || w.to.Second() != 0 {
					t.Fatalf("unaligned window %v", w)
				}
			}
			if last := src.lastCall(); minutes(last) > tt.maxLeaf {
				t.Fatalf("last leaf %v longer than %v", last, tt.maxLeaf)
			}
		})
	}
}

func TestTruncationWithFailedHalfAppliesNothing(t *testing.T) {
	src := &fakeSource{
		id:        "a",
		truncated: func(w window) bool { return minutes(w) > 1 },
		failOn:    func(w window) bool { return w.from.Equal(at(11, 57)) },
	}
	s, clk, self := setup(t, src)
	clk.add(3 * time.Minute)
	s.tick(context.Background())
	if src.total() != 0 || value(self.errors, "a") != 1 {
		t.Fatalf("total=%v errors=%v", src.total(), value(self.errors, "a"))
	}
	src.failOn = nil
	s.tick(context.Background())
	if src.total() != 4 {
		t.Fatalf("total after retry = %v, want 4", src.total())
	}
}

func TestFailingSourceDoesNotBlockOthers(t *testing.T) {
	bad := &fakeSource{id: "bad"}
	good := &fakeSource{id: "good"}
	s, clk, self := setup(t, bad, good)
	s.snapshots = []snapshotSource{fakeSnapshot{err: errors.New("down")}}

	bad.err = errors.New("boom")
	clk.add(time.Minute)
	s.tick(context.Background())
	if good.total() != 2 || bad.total() != 0 {
		t.Fatalf("good=%v bad=%v", good.total(), bad.total())
	}
	if value(self.errors, "bad") != 1 || value(self.errors, "snap") != 1 || value(self.errors, "good") != 0 {
		t.Fatal("errors_total not attributed per source")
	}
}

func TestCatchUpAdvancesInChunks(t *testing.T) {
	src := &fakeSource{id: "a", failOn: func(w window) bool { return w.from.Equal(at(12, 4)) }}
	s, clk, self := setup(t, src)
	clk.add(24 * time.Minute)

	s.tick(context.Background())
	if len(src.calls) != 2 || src.calls[0] != (window{at(11, 54), at(12, 4)}) {
		t.Fatalf("calls = %v", src.calls)
	}
	if src.total() != 10 || value(self.errors, "a") != 1 {
		t.Fatalf("total=%v errors=%v", src.total(), value(self.errors, "a"))
	}
	if got := testutil.ToFloat64(self.watermark.WithLabelValues("a")); got != float64(at(12, 4).Unix()) {
		t.Fatalf("watermark = %v", got)
	}

	src.failOn = nil
	s.tick(context.Background())
	if src.total() != 25 || src.lastCall() != (window{at(12, 14), at(12, 19)}) {
		t.Fatalf("total=%v last=%v", src.total(), src.lastCall())
	}
}

func TestSourceStopsAtItsDeadline(t *testing.T) {
	src := &fakeSource{id: "a", onFetch: func(context.Context) error { time.Sleep(120 * time.Millisecond); return nil }}
	s, clk, self := setup(t, src)
	s.interval = 200 * time.Millisecond
	clk.add(30 * time.Minute)

	s.tick(context.Background())
	if n := len(src.calls); n < 1 || n > 2 || src.total() != float64(10*n) {
		t.Fatalf("calls=%d total=%v", n, src.total())
	}
	if value(self.errors, "a") != 1 {
		t.Fatalf("deadline not reported: errors=%v", value(self.errors, "a"))
	}
}

func TestSourcesRunConcurrently(t *testing.T) {
	aStarted, bStarted := make(chan struct{}), make(chan struct{})
	await := func(mine, other chan struct{}) func(context.Context) error {
		return func(context.Context) error {
			close(mine)
			select {
			case <-other:
				return nil
			case <-time.After(2 * time.Second):
				return errors.New("sources ran sequentially")
			}
		}
	}
	a := &fakeSource{id: "a", onFetch: await(aStarted, bStarted)}
	b := &fakeSource{id: "b", onFetch: await(bStarted, aStarted)}
	s, _, self := setup(t, a, b)

	s.tick(context.Background())
	if a.total() != 1 || b.total() != 1 || value(self.errors, "a")+value(self.errors, "b") != 0 {
		t.Fatalf("a=%v b=%v", a.total(), b.total())
	}
}

func TestSelfMetricsPreinitialized(t *testing.T) {
	s, clk, self := setup(t, &fakeSource{id: "a"})
	for _, name := range []string{"a", "snap"} {
		for _, v := range []*prometheus.CounterVec{self.errors, self.truncated, self.skipped} {
			if testutil.CollectAndCount(v) != 2 || value(v, name) != 0 {
				t.Fatalf("%s not pre-initialized at 0", name)
			}
		}
		if got := testutil.ToFloat64(self.lastSuccess.WithLabelValues(name)); got != float64(clk.t.Unix()) {
			t.Fatalf("last success %s = %v, want process start", name, got)
		}
	}
	if got := testutil.ToFloat64(self.watermark.WithLabelValues("a")); got != float64(s.windowed[0].watermark.Unix()) || testutil.CollectAndCount(self.watermark) != 1 {
		t.Fatalf("watermark = %v", got)
	}
}

func TestNewSeriesExportedAtZeroFirst(t *testing.T) {
	src := &fakeSource{id: "a", label: "new"}
	s, clk, _ := setup(t, src)

	s.tick(context.Background())
	if testutil.CollectAndCount(src.counter) != 1 || src.total() != 0 {
		t.Fatalf("after first apply: series=%d value=%v", testutil.CollectAndCount(src.counter), src.total())
	}
	s.tick(context.Background())
	if src.total() != 1 {
		t.Fatalf("deferred delta not applied on next tick: %v", src.total())
	}
	clk.add(time.Minute)
	s.tick(context.Background())
	if src.total() != 2 {
		t.Fatalf("known series must apply immediately: %v", src.total())
	}
}

func TestSnapshotTruncationCounted(t *testing.T) {
	s, clk, self := setup(t)
	s.snapshots = []snapshotSource{fakeSnapshot{truncated: true}}
	clk.add(time.Minute)

	s.tick(context.Background())
	if value(self.truncated, "snap") != 1 || value(self.errors, "snap") != 0 {
		t.Fatalf("truncated=%v errors=%v", value(self.truncated, "snap"), value(self.errors, "snap"))
	}
	if got := testutil.ToFloat64(self.lastSuccess.WithLabelValues("snap")); got != float64(clk.t.Unix()) {
		t.Fatalf("last success = %v", got)
	}
}
