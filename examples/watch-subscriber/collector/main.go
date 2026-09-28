// Command collector receives events from one or more BeeGFS Watch instances and
// writes them to spool files for the processor to pick up.
//
// Events are appended, exactly as Watch sent them, to a file ending
// .binpb.partial: protobuf binary, each record prefixed with its length
// (protodelim). The spool package reads them, and spoolcat prints them as JSON. When it holds enough
// events, or has been open long enough, it is handed to a sealer, which fsyncs
// it, gives it its final .binpb name, fsyncs the directory, and only then acks
// its events back to Watch. A plain .binpb file is therefore complete and on
// disk, and is the processor's to take. Files are sealed in the order they were
// written, so reading them in name order gives the order events arrived.
//
// Nothing here can slow down the file system: the metadata service and Watch
// never wait for us. What falling behind costs is Watch's buffer, which drops
// the oldest events once it is full. So the receive path never waits on disk,
// and the sealer only gets in its way when fsync is several files behind.
//
// Watch replays anything we never ack, so a crash costs a replay and nothing
// else. Any .partial left behind is incomplete and is deleted at startup. A
// disk error stops the collector instead of guessing: nothing unsealed is acked,
// and the replay after a restart fills the hole.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	"github.com/thinkparq/beegfs-index/examples/watch-subscriber/spool"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

// partialSuffix marks a file that is still being written.
const partialSuffix = spool.PartialSuffix

// file is one spool file and the acks owed for what is in it.
type file struct {
	f    *os.File
	name string
	acks []subscriber.Ack
}

type collector struct {
	log       *zap.Logger
	dir       string
	maxEvents int

	events chan *bw.Event
	acks   chan subscriber.Ack
	sealq  chan *file // written, waiting for fsync; the sealer is its only reader

	// Owned by the receive loop.
	cur *file
	w   *bufio.Writer // reset onto each new file
}

// prepare creates the spool directory and removes any file left mid-write.
func (c *collector) prepare() error {
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return err
	}
	old, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	for _, e := range old {
		if !strings.HasSuffix(e.Name(), partialSuffix) {
			continue
		}
		if err := os.Remove(filepath.Join(c.dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (c *collector) add(ev *bw.Event) error {
	if c.cur == nil {
		// Microseconds, so two files never share a name within a run. The
		// sealer's link refuses to replace a file, which covers the rest (a
		// clock stepped back across a restart, say).
		name := time.Now().UTC().Format("20060102T150405.000000Z") + spool.Ext
		f, err := os.OpenFile(filepath.Join(c.dir, name+partialSuffix), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return err
		}
		c.cur = &file{f: f, name: name, acks: make([]subscriber.Ack, 0, min(c.maxEvents, 1<<16))}
		c.w.Reset(f)
	}
	// Every event has to be acked exactly once or its meta's watermark stops
	// moving, so the ack waits in the file's list until the file is safe.
	c.cur.acks = append(c.cur.acks, subscriber.Ack{MetaId: ev.GetMetaId(), SeqId: ev.GetSeqId()})
	// The whole event, v1 or v2, as Watch sent it. Reading it is downstream's job.
	if _, err := protodelim.MarshalTo(c.w, ev); err != nil {
		return err
	}
	if len(c.cur.acks) >= c.maxEvents {
		return c.rotate()
	}
	return nil
}

// rotate pushes the buffered tail into the file and hands the file to the
// sealer. It blocks only when the sealer is a whole queue of files behind.
func (c *collector) rotate() error {
	if c.cur == nil {
		return nil
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	c.sealq <- c.cur
	c.cur = nil
	return nil
}

// seal makes one file durable and then acks it: fsync the data, link it to its
// final name, fsync the directory so the name survives a power cut, and only
// then ack. Any other order can ack an event that is not on disk. It returns
// the per-meta highest sequence ID it acked.
func (c *collector) seal(fl *file, dir *os.File, high map[uint32]uint64) error {
	if err := fl.f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", fl.name, err)
	}
	if err := fl.f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", fl.name, err)
	}
	partial := filepath.Join(c.dir, fl.name+partialSuffix)
	// Link, not rename: rename silently replaces an existing file, and every
	// event in a sealed file has already been acked.
	if err := os.Link(partial, filepath.Join(c.dir, fl.name)); err != nil {
		return fmt.Errorf("seal %s: %w", fl.name, err)
	}
	if err := os.Remove(partial); err != nil {
		return fmt.Errorf("seal %s: %w", fl.name, err)
	}
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("fsync spool directory: %w", err)
	}
	for _, a := range fl.acks {
		c.acks <- a
		high[a.MetaId] = max(high[a.MetaId], a.SeqId)
	}
	c.log.Info("sealed", zap.String("file", fl.name), zap.Int("events", len(fl.acks)))
	return nil
}

// sealer seals files in the order they were written until sealq is closed.
// A failure is returned at once and nothing after it is acked.
func (c *collector) sealer(high map[uint32]uint64) error {
	dir, err := os.Open(c.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	for fl := range c.sealq {
		if err := c.seal(fl, dir, high); err != nil {
			return err
		}
	}
	return nil
}

// run is the only reader of events, so the open file needs no lock. Events
// from every Watch arrive already tagged with a meta ID and are written in the
// order they arrive.
func (c *collector) run(ctx context.Context, every time.Duration) error {
	defer close(c.sealq)
	tick := time.NewTicker(every)
	defer tick.Stop()

	for {
		select {
		case ev := <-c.events:
			if err := c.add(ev); err != nil {
				return err
			}
		case <-tick.C:
			if err := c.rotate(); err != nil {
				return err
			}
		case <-ctx.Done():
			for {
				select {
				case ev := <-c.events:
					if err := c.add(ev); err != nil {
						return err
					}
				default:
					return c.rotate()
				}
			}
		}
	}
}

func main() {
	listen := flag.String("listen", "0.0.0.0:50052", "address Watch dials into")
	dir := flag.String("spool", "/var/lib/index-sync/spool", "spool directory for "+spool.Ext+" files")
	ckpt := flag.String("checkpoint", "/var/lib/index-sync/checkpoint.json", "acked sequence IDs")
	ackEvery := flag.Duration("checkpoint-every", time.Second, "how often acks reach Watch and disk; Watch frees buffer space only then")
	rollEvery := flag.Duration("roll-every", 30*time.Second, "seal the open file at least this often")
	rollEvents := flag.Int("roll-events", 1_000_000, "seal the open file once it holds this many events")
	queue := flag.Int("queue", 65536, "events held between the gRPC streams and the writer")
	sealQueue := flag.Int("seal-queue", 4, "written files that may wait for fsync before receiving waits")
	noTLS := flag.Bool("tls-disable", true, "disable TLS, not for production")
	flag.Parse()

	log, _ := zap.NewProduction()
	defer log.Sync()

	// Watch frees buffer space only for what we have acked, and we ack only at
	// seal, so these bound how far behind Watch's buffer can get.
	if *rollEvents < 1 || *queue < 1 || *sealQueue < 1 || *rollEvery <= 0 {
		log.Fatal("roll-events, queue and seal-queue must be at least 1, roll-every above 0")
	}

	c := &collector{
		log:       log,
		dir:       *dir,
		maxEvents: *rollEvents,
		events:    make(chan *bw.Event, *queue),
		acks:      make(chan subscriber.Ack, 1<<16),
		sealq:     make(chan *file, *sealQueue),
		w:         bufio.NewWriterSize(nil, 1<<20),
	}
	if err := c.prepare(); err != nil {
		log.Fatal("could not prepare spool", zap.Error(err))
	}

	store, err := subscriber.NewDiskStore(*ckpt)
	if err != nil {
		log.Fatal("could not open checkpoint", zap.Error(err))
	}
	server, err := subscriber.NewServer(log, subscriber.Config{
		Address:      *listen,
		TlsDisable:   *noTLS,
		AckFrequency: *ackEvery,
	}, store)
	if err != nil {
		log.Fatal("could not create server", zap.Error(err))
	}

	// A disk error ends the process. Carrying on would mean acking around a
	// file that never reached disk; exiting leaves those events unacked, so
	// they come back when the collector does.
	fatal := func(what string, err error) {
		if errors.Is(err, syscall.ENOSPC) {
			log.Fatal(what+": spool disk is full; events stay in Watch until it has room", zap.Error(err))
		}
		if errors.Is(err, fs.ErrExist) {
			log.Fatal(what+": a sealed file already has this name; refusing to replace it", zap.Error(err))
		}
		log.Fatal(what, zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	high := map[uint32]uint64{}
	received := make(chan struct{})
	sealed := make(chan struct{})
	go func() {
		if err := c.run(ctx, *rollEvery); err != nil {
			fatal("write failed", err)
		}
		close(received)
	}()
	go func() {
		if err := c.sealer(high); err != nil {
			fatal("seal failed", err)
		}
		close(sealed)
	}()

	errs := make(chan error, 1)
	go server.ListenAndServe(c.events, c.acks, errs)
	log.Info("collector started", zap.String("listen", *listen), zap.String("spool", *dir),
		zap.Int("roll-events", *rollEvents), zap.Duration("roll-every", *rollEvery))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errs:
		log.Error("subscriber server failed", zap.Error(err))
	case <-sig:
	}

	// Stop the source first so nothing new arrives, let the loop drain, then
	// let the sealer finish every file before acks close for the final flush.
	server.Stop()
	cancel()
	<-received
	<-sealed
	// The SDK records watermarks in the checkpoint from its per-stream
	// goroutines, which Stop has already ended, so the last files' acks would
	// never reach it and would replay after a restart. Everything received has
	// now been sealed, so each meta's highest acked ID is safe to record. Only
	// raise an entry, or replace the SDK's "seek to end" placeholder (MaxUint64)
	// for a meta it had not checkpointed yet.
	if stored, err := store.Retrieve(); err == nil {
		for meta, seq := range high {
			if old, ok := stored[meta]; !ok || old == math.MaxUint64 || seq > old {
				store.Store(meta, seq)
			}
		}
	}
	close(c.acks)
	server.WaitFlushed()
	log.Info("collector stopped")
}
