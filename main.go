package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/cosyndev/cloudflare-exporter/internal/cloudflare"
	"github.com/cosyndev/cloudflare-exporter/internal/collector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	version = "dev"
	commit  = "dev"
)

type config struct {
	token    string
	account  string
	zones    []string
	listen   string
	interval time.Duration
	delay    time.Duration
}

func parseConfig(getenv func(string) string) (config, error) {
	c := config{
		token:   getenv("CF_API_TOKEN"),
		account: getenv("CF_ACCOUNT_ID"),
		zones:   splitList(getenv("CF_ZONES")),
		listen:  orDefault(getenv("LISTEN_ADDRESS"), ":9184"),
	}
	var errs []error
	if c.token == "" {
		errs = append(errs, errors.New("CF_API_TOKEN is required"))
	}
	if c.account == "" {
		errs = append(errs, errors.New("CF_ACCOUNT_ID is required"))
	}
	var err error
	if c.interval, err = parseDuration(getenv, "COLLECT_INTERVAL", "60s"); err != nil {
		errs = append(errs, err)
	} else if c.interval <= 0 {
		errs = append(errs, errors.New("COLLECT_INTERVAL must be positive"))
	}
	if c.delay, err = parseDuration(getenv, "COLLECT_DELAY", "5m"); err != nil {
		errs = append(errs, err)
	} else if c.delay < 0 {
		errs = append(errs, errors.New("COLLECT_DELAY must not be negative"))
	}
	return c, errors.Join(errs...)
}

func parseDuration(getenv func(string) string, key, def string) (time.Duration, error) {
	d, err := time.ParseDuration(orDefault(getenv(key), def))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func splitList(v string) []string {
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.ToLower(strings.TrimSpace(s)); s != "" && !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}

func selectZones(all []cloudflare.Zone, names []string) ([]cloudflare.Zone, error) {
	if len(names) == 0 {
		return all, nil
	}
	byName := map[string]cloudflare.Zone{}
	for _, z := range all {
		byName[strings.ToLower(z.Name)] = z
	}
	var picked []cloudflare.Zone
	var unknown []string
	for _, n := range names {
		if z, ok := byName[n]; ok {
			picked = append(picked, z)
		} else {
			unknown = append(unknown, n)
		}
	}
	if len(unknown) > 0 {
		return nil, fmt.Errorf("CF_ZONES: zones not in account: %s", strings.Join(unknown, ", "))
	}
	return picked, nil
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log, os.Getenv, cloudflare.DefaultBaseURL); err != nil {
		log.Error("exiting", "err", err)
		stop()
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger, getenv func(string) string, apiURL string) error {
	cfg, err := parseConfig(getenv)
	if err != nil {
		return err
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()

	cf := cloudflare.New(apiURL, cfg.token, "cloudflare-exporter/"+version)
	all, err := cf.Zones(ctx, cfg.account)
	if err != nil {
		return err
	}
	selected, err := selectZones(all, cfg.zones)
	if err != nil {
		return err
	}
	zones := make([]collector.Zone, len(selected))
	hostCounts := map[string]int{}
	for i, z := range selected {
		hosts, err := cf.ProxiedHostnames(ctx, z.ID)
		if err != nil {
			return fmt.Errorf("zone %s: list DNS records (the token needs Zone > DNS > Read): %w", z.Name, err)
		}
		zones[i] = collector.Zone{ID: z.ID, Name: z.Name, ProxiedHosts: hosts}
		hostCounts[z.Name] = len(hosts)
	}
	log.Info("starting", "version", version, "proxied_hosts_per_zone", hostCounts, "interval", cfg.interval.String(), "delay", cfg.delay.String())

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cloudflare_exporter_build_info", Help: "Build metadata."}, []string{"version", "revision", "goversion"})
	buildInfo.WithLabelValues(version, commit, runtime.Version()).Set(1)
	reg.MustRegister(buildInfo)
	sched := collector.New(reg, cf, collector.Config{
		AccountID: cfg.account, Zones: zones,
		Interval: cfg.interval, Delay: cfg.delay, Now: time.Now, Logger: log,
	})

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("cloudflare-exporter " + version + "\n/metrics\n"))
	})
	srv := &http.Server{Addr: cfg.listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}

	done := make(chan struct{})
	go func() {
		defer close(done)
		sched.Run(ctx)
	}()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	select {
	case err = <-serveErr:
		stop()
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = srv.Shutdown(shutdownCtx)
	}
	<-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
