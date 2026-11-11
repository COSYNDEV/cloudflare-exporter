package collector

import (
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type selfMetrics struct {
	errors      *prometheus.CounterVec
	lastSuccess *prometheus.GaugeVec
	watermark   *prometheus.GaugeVec
	truncated   *prometheus.CounterVec
	skipped     *prometheus.CounterVec
}

func newSelfMetrics(reg prometheus.Registerer) selfMetrics {
	return selfMetrics{
		errors:      counter(reg, "cloudflare_exporter_errors_total", "Failed collections per source.", "source"),
		lastSuccess: gauge(reg, "cloudflare_exporter_last_success_timestamp_seconds", "Unix time of the last successful collection per source.", "source"),
		watermark:   gauge(reg, "cloudflare_exporter_watermark_timestamp_seconds", "End of the last window applied per source.", "source"),
		truncated:   counter(reg, "cloudflare_exporter_truncated_windows_total", "One-minute windows still over the row limit, applied incomplete.", "source"),
		skipped:     counter(reg, "cloudflare_exporter_skipped_seconds_total", "Seconds of data dropped because collection fell behind the catch-up cap.", "source"),
	}
}

// preinit starts last success at process start so a source that never succeeds goes visibly stale.
func (m selfMetrics) preinit(source string, start time.Time) {
	m.errors.WithLabelValues(source)
	m.truncated.WithLabelValues(source)
	m.skipped.WithLabelValues(source)
	m.lastSuccess.WithLabelValues(source).Set(float64(start.Unix()))
}

func counter(reg prometheus.Registerer, name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	reg.MustRegister(c)
	return c
}

func gauge(reg prometheus.Registerer, name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	reg.MustRegister(g)
	return g
}

// hostMatcher bounds the host label to a zone's proxied DNS names, because the Host
// header is client-controlled and scanners send arbitrary values.
type hostMatcher struct {
	exact     map[string]struct{}
	wildcards []string
}

func newHostMatcher(names []string) hostMatcher {
	m := hostMatcher{exact: map[string]struct{}{}}
	for _, n := range names {
		n = normalizeHost(n)
		if suffix, ok := strings.CutPrefix(n, "*."); ok {
			m.wildcards = append(m.wildcards, suffix)
		} else {
			m.exact[n] = struct{}{}
		}
	}
	slices.SortFunc(m.wildcards, func(a, b string) int { return len(b) - len(a) })
	return m
}

func (m hostMatcher) label(raw string) string {
	h := normalizeHost(raw)
	if _, ok := m.exact[h]; ok {
		return h
	}
	for _, suffix := range m.wildcards {
		if strings.HasSuffix(h, "."+suffix) {
			return "*." + suffix
		}
	}
	return "other"
}

func normalizeHost(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	if i := strings.LastIndexByte(h, ':'); i >= 0 && !strings.Contains(h[i:], "]") {
		h = h[:i]
	}
	return strings.TrimSuffix(h, ".")
}
