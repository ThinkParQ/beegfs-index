// Command beegfs-index-sync feeds BeeGFS Watch events to GUFI's incremental update. The collector
// receives events from Watch and spools them to disk; markwalk turns the spool into suspect files
// for beegfs index update. -mode runs both in one process or either one alone, see MERGE-DESIGN.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-index/beegfs_watch/internal/collector"
	"github.com/thinkparq/beegfs-index/beegfs_watch/internal/markwalk"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func main() {
	var (
		c collector.Config
		m markwalk.Config
	)
	mode := flag.String("mode", "both", "both, collect (Watch to spool) or process (spool to suspect files)")
	spool := flag.String("spool", "/var/lib/index-sync/spool", "segment directory shared by the collector and markwalk")
	flag.StringVar(&c.Listen, "collector.listen", "0.0.0.0:50052", "address Watch connects to")
	flag.StringVar(&c.Checkpoint, "collector.checkpoint", "/var/lib/index-sync/checkpoint.json", "file recording acknowledged sequence IDs")
	flag.StringVar(&c.Counter, "collector.counter", "/var/lib/index-sync/counter.json", "file recording the highest sealed segment number")
	flag.DurationVar(&c.AckFrequency, "collector.ack-frequency", time.Second, "how often acknowledgements reach Watch and the checkpoint")
	flag.DurationVar(&c.RollEvery, "collector.roll-every", 30*time.Second, "seal the open segment at least this often")
	flag.IntVar(&c.RollEvents, "collector.roll-events", 100_000, "seal the open segment once it holds this many events; (seal-queue + 2) times this many can be unacknowledged, keep that under Watch's event-buffer-size")
	flag.IntVar(&c.Queue, "collector.queue", 65536, "events buffered between the gRPC streams and the writer")
	flag.IntVar(&c.SealQueue, "collector.seal-queue", 4, "written segments that may wait for fsync before receiving blocks")
	flag.BoolVar(&c.TLSDisable, "collector.tls-disable", true, "disable TLS for the subscriber gRPC server, not for production")
	flag.StringVar(&m.State, "markwalk.state", "/var/lib/index-sync/markwalk.json", "file recording the last processed segment")
	flag.StringVar(&m.Work, "markwalk.work", "/var/lib/index-sync/markwalk", "directory for suspect files, one batch-<N>/suspects per batch")
	flag.StringVar(&m.Mount, "markwalk.mount", "/mnt/beegfs", "BeeGFS mount point")
	flag.StringVar(&m.Index, "markwalk.index", "/var/lib/gufi", "GUFI index root")
	flag.StringVar(&m.Query, "markwalk.query", "gufi_query", "gufi_query binary, used to find the other names of hard-linked files; empty disables this")
	flag.DurationVar(&m.Every, "markwalk.every", time.Minute, "least time between batches; beegfs index update runs once per batch")
	flag.DurationVar(&m.Poll, "markwalk.poll", 30*time.Second, "how often to look for new segments in -mode process")
	flag.IntVar(&m.MaxSegments, "markwalk.max-segments", 100, "most segments in one batch")
	flag.IntVar(&m.StatCache, "markwalk.stat-cache", 200_000, "most stat results kept per batch")
	flag.BoolVar(&m.TrackOpens, "markwalk.track-opens", false, "include open events so atime changes are indexed")
	flag.BoolVar(&m.Retain, "markwalk.retain", false, "delete segments once their suspect file is written, keeping the newest")
	flag.BoolVar(&m.Once, "markwalk.once", false, "process the segments present now and exit, -mode process only")
	flag.Parse()
	c.Spool, m.Spool = *spool, *spool

	log := zap.Must(zap.NewProduction())
	defer log.Sync()
	if err := validate(*mode, c, m); err != nil {
		log.Fatal("invalid configuration", zap.Error(err))
	}
	runCollector := *mode == "both" || *mode == "collect"
	runMarkwalk := *mode == "both" || *mode == "process"

	// In one process a seal nudges markwalk through this channel. Alone, markwalk polls instead.
	var sealed chan struct{}
	if runCollector && runMarkwalk {
		sealed = make(chan struct{}, 1)
	}

	// SIGINT or SIGTERM stops both: the collector seals and acknowledges what it holds, markwalk
	// starts nothing new. A collector error stops the process too; markwalk retries its own.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g, gctx := errgroup.WithContext(ctx)
	if runCollector {
		g.Go(func() error { return collector.Run(gctx, c, log, sealed) })
	}
	if runMarkwalk {
		g.Go(func() error { return markwalk.Run(gctx, m, log, sealed) })
	}
	if err := g.Wait(); err != nil {
		log.Fatal("stopped on error", zap.Error(err))
	}
	log.Info("stopped")
}

// validate reports every bad value at once.
func validate(mode string, c collector.Config, m markwalk.Config) error {
	var errs []error
	switch mode {
	case "both", "collect", "process":
	default:
		errs = append(errs, fmt.Errorf("-mode must be both, collect or process, not %q", mode))
	}
	if c.RollEvents < 1 || c.Queue < 1 || c.SealQueue < 1 || c.RollEvery <= 0 {
		errs = append(errs, errors.New("-collector.roll-events, -collector.queue and -collector.seal-queue must be at least 1, -collector.roll-every above 0"))
	}
	if m.MaxSegments < 1 || m.StatCache < 1 || m.Every <= 0 || m.Poll <= 0 {
		errs = append(errs, errors.New("-markwalk.max-segments and -markwalk.stat-cache must be at least 1, -markwalk.every and -markwalk.poll above 0"))
	}
	if m.Once && mode != "process" {
		errs = append(errs, errors.New("-markwalk.once needs -mode process"))
	}
	return errors.Join(errs...)
}
