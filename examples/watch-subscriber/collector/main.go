// Command collector receives events from one or more BeeGFS Watch instances and
// writes them to spool files for the processor to pick up.
//
// Events are appended to a file ending .jsonl.partial. Every so often that file
// is fsynced, renamed to drop the suffix, and only then acked back to Watch. A
// plain .jsonl file is therefore complete, and is the processor's to take.
//
// Watch replays anything we never ack, so a crash costs a replay and nothing
// else. Any .partial left behind is incomplete and is deleted at startup.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
)

// line is one entry in a spool file. JSON so the files can be read with grep
// while something is going wrong.
type line struct {
	Meta       uint32 `json:"meta"`
	Seq        uint64 `json:"seq"`
	Type       string `json:"type"`
	Path       string `json:"path"`
	TargetPath string `json:"target_path,omitempty"`
	NumLinks   uint64 `json:"num_links,omitempty"`
	EntryID    string `json:"entry_id,omitempty"`
	ParentID   string `json:"parent_entry_id,omitempty"`
	Timestamp  int64  `json:"ts"`
}

// partialSuffix marks a file that is still being written.
const partialSuffix = ".partial"

type collector struct {
	log *zap.Logger
	dir string

	events chan *bw.Event
	acks   chan subscriber.Ack

	// The file currently being written, and the acks owed for what is in it.
	f       *os.File
	enc     *json.Encoder
	name    string
	pending []subscriber.Ack
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
	v2 := ev.GetV2()
	if v2 == nil {
		return nil // a v1 event from BeeGFS 7, nothing here understands it
	}
	if c.f == nil {
		c.name = time.Now().UTC().Format("20060102T150405Z") + ".jsonl"
		f, err := os.Create(filepath.Join(c.dir, c.name+partialSuffix))
		if err != nil {
			return err
		}
		c.f, c.enc = f, json.NewEncoder(f)
	}
	if err := c.enc.Encode(line{
		Meta: ev.GetMetaId(), Seq: ev.GetSeqId(),
		Type: v2.GetType().String(), Path: v2.GetPath(),
		TargetPath: v2.GetTargetPath(), NumLinks: v2.GetNumLinks(),
		EntryID: v2.GetEntryId(), ParentID: v2.GetParentEntryId(),
		Timestamp: v2.GetTimestamp(),
	}); err != nil {
		return err
	}
	// Every event has to be acked exactly once or this meta's watermark stops
	// moving, so hold it until the file it is in is safe.
	c.pending = append(c.pending, subscriber.Ack{MetaId: ev.GetMetaId(), SeqId: ev.GetSeqId()})
	return nil
}

// seal fsyncs the current file, drops the .partial suffix and acks what was in it.
// fsync, then rename, then ack. Any other order can ack an event that is not on disk.
func (c *collector) seal() error {
	if c.f == nil || len(c.pending) == 0 {
		return nil
	}
	if err := c.f.Sync(); err != nil {
		return err
	}
	if err := c.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(c.dir, c.name+partialSuffix), filepath.Join(c.dir, c.name)); err != nil {
		return err
	}
	for _, a := range c.pending {
		c.acks <- a
	}
	c.log.Info("sealed", zap.String("file", c.name), zap.Int("events", len(c.pending)))
	c.f, c.enc, c.pending = nil, nil, nil
	return nil
}

// run is the only reader of events and the only writer of acks, so the file
// needs no lock. Events from every Watch arrive here already tagged with a meta
// ID, and they can interleave freely: the processor turns them into a set of
// directories, and a set has no order.
func (c *collector) run(ctx context.Context, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()

	for {
		select {
		case ev := <-c.events:
			if err := c.add(ev); err != nil {
				// Not acking is the right outcome here: Watch redelivers.
				c.log.Error("write failed, event will be redelivered", zap.Error(err))
			}
		case <-tick.C:
			if err := c.seal(); err != nil {
				c.log.Error("seal failed, events will be redelivered", zap.Error(err))
			}
		case <-ctx.Done():
			for {
				select {
				case ev := <-c.events:
					if err := c.add(ev); err != nil {
						c.log.Error("write failed, event will be redelivered", zap.Error(err))
					}
				default:
					if err := c.seal(); err != nil {
						c.log.Error("seal failed, events will be redelivered", zap.Error(err))
					}
					return
				}
			}
		}
	}
}

func main() {
	listen := flag.String("listen", "0.0.0.0:50052", "address Watch dials into")
	dir := flag.String("spool", "/var/lib/index-sync/spool", "spool directory for .jsonl files")
	ckpt := flag.String("checkpoint", "/var/lib/index-sync/checkpoint.json", "acked sequence IDs")
	ackEvery := flag.Duration("checkpoint-every", 5*time.Second, "how often acks reach Watch and disk")
	rollEvery := flag.Duration("roll-every", 30*time.Second, "how often to seal the current file")
	noTLS := flag.Bool("tls-disable", true, "disable TLS, not for production")
	flag.Parse()

	log, _ := zap.NewProduction()
	defer log.Sync()

	c := &collector{
		log:    log,
		dir:    *dir,
		events: make(chan *bw.Event, 1024),
		acks:   make(chan subscriber.Ack, 1024),
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.run(ctx, *rollEvery); close(done) }()

	errs := make(chan error, 1)
	go server.ListenAndServe(c.events, c.acks, errs)
	log.Info("collector started", zap.String("listen", *listen), zap.String("spool", *dir))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errs:
		log.Error("subscriber server failed", zap.Error(err))
	case <-sig:
	}

	// Stop the source first so nothing new arrives, let the loop drain and seal,
	// then close acks so the checkpoint gets its final flush.
	server.Stop()
	cancel()
	<-done
	close(c.acks)
	server.WaitFlushed()
	log.Info("collector stopped")
}
