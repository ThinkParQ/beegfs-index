package main

// Part 4: the reader. The collector seals numbered segment files; markwalk takes the new ones
// in number order, turns each batch of them into one suspect file, and records how far it got.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/thinkparq/beegfs-index/examples/watch-subscriber/spool"
	bw "github.com/thinkparq/protobuf/go/beewatch"
)

// state is how far markwalk got, kept in one small file.
type state struct {
	// Segment is the number of the last segment whose batch is done. The next run starts after
	// it. Until the incremental update runs here, "done" means its suspect file is on disk.
	Segment uint64 `json:"segment"`
	// Seqs is each meta's highest sequence ID seen, so a hole across batches and restarts is
	// still found.
	Seqs map[uint32]uint64 `json:"seqs"`
}

func readState(path string) (state, error) {
	st := state{Seqs: map[uint32]uint64{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, fmt.Errorf("%s: %w", path, err)
	}
	if st.Seqs == nil {
		st.Seqs = map[uint32]uint64{}
	}
	return st, nil
}

func writeState(path string, st state) error {
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, append(data, '\n'))
}

// writeFileAtomic replaces path with data so that a crash leaves either the old file or the
// new one, never part of either: temp file, fsync, rename, fsync the directory.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name()) // a no-op once renamed
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// reader turns segments into suspect files.
type reader struct {
	log         *slog.Logger
	spool       string // the collector's segment directory
	statePath   string
	work        string // one directory per batch: batch-<last segment>/suspects
	mount       string
	index       string
	query       string // gufi_query, for the sibling lookup
	trackOpens  bool
	maxSegments int
}

// run processes new segments until ctx is done. With once it processes what is there now and
// returns. Between batches it waits every, so a batch collects that long; with nothing new it
// looks again after poll.
func (r *reader) run(ctx context.Context, every, poll time.Duration, once bool) error {
	st, err := readState(r.statePath)
	if err != nil {
		return err
	}
	r.log.Info("markwalk started", "spool", r.spool, "after_segment", st.Segment, "state", r.statePath)
	for {
		segs, err := spool.Sealed(r.spool, st.Segment)
		if err != nil {
			return err
		}
		if len(segs) > r.maxSegments {
			segs = segs[:r.maxSegments]
		}
		wait := poll
		if len(segs) > 0 {
			if err := r.process(&st, segs); err != nil {
				return err
			}
			wait = every
		} else if once {
			return nil
		}
		if once {
			continue // take the next batch now
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// process makes one batch of segs, writes its suspect file, and only then moves st past it. A
// crash before the state write repeats the batch; it writes the same file again, which is
// harmless.
func (r *reader) process(st *state, segs []spool.Segment) error {
	first, last := segs[0].Number, segs[len(segs)-1].Number
	seqs := maps.Clone(st.Seqs)
	dirs := map[string]struct{}{}
	// Entry IDs of files with more than one name; the index supplies where the others are.
	links := map[string]struct{}{}
	// Renames, source to destination: new directories below a moved one need their own marks.
	moves := map[[2]string]struct{}{}
	var events, v1, missing uint64

	for _, s := range segs {
		err := readSegment(s.Path, func(ev *bw.Event) {
			events++
			if n := seeSeq(seqs, ev.GetMetaId(), ev.GetSeqId()); n > 0 {
				missing += n
				r.log.Warn("missing events: the spool has a hole, those directories may stay stale",
					"meta", ev.GetMetaId(), "from", ev.GetSeqId()-n, "to", ev.GetSeqId()-1,
					"missing", n, "segment", s.Number)
			}
			v2 := ev.GetV2()
			if v2 == nil {
				v1++ // a BeeGFS 7 event; not used
				return
			}
			ds, id := dirsFor(v2, r.mount, r.trackOpens)
			for _, d := range ds {
				dirs[d] = struct{}{}
			}
			if id != "" {
				links[id] = struct{}{}
			}
			if v2.GetType() == bw.V2Event_RENAME {
				moves[[2]string{filepath.Join(r.mount, v2.GetPath()),
					filepath.Join(r.mount, v2.GetTargetPath())}] = struct{}{}
			}
		})
		if err != nil {
			return err // a damaged segment stops markwalk rather than skip what it held
		}
	}

	// One query for the whole batch, not one per event.
	marked := append(slices.Collect(maps.Keys(dirs)),
		siblingDirs(slices.Collect(maps.Keys(links)), r.mount, r.index, r.query)...)
	for m := range moves {
		marked = append(marked, newDirsUnder(m[1], m[0], r.mount, r.index)...)
	}
	out := filepath.Join(r.work, fmt.Sprintf("batch-%010d", last), "suspects")
	marks, err := writeSuspects(marked, r.mount, r.index, out, false)
	if err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}

	st.Segment, st.Seqs = last, seqs
	if err := writeState(r.statePath, *st); err != nil {
		return fmt.Errorf("write %s: %w", r.statePath, err)
	}

	if marks == 0 {
		out = "none: nothing left to mark"
	}
	r.log.Info("batch done", "first_segment", first, "last_segment", last, "segments", len(segs),
		"events", events, "directories", len(dirs), "multi_name_inodes", len(links), "marks", marks,
		"v1_events_skipped", v1, "missing_events", missing, "suspects", out)
	return nil
}

// readSegment calls fn for every event in one sealed segment. The event is reused between
// calls, so fn must not keep it.
func readSegment(path string, fn func(*bw.Event)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sr := spool.NewReader(f)
	var ev bw.Event
	for {
		err := sr.Next(&ev)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		fn(&ev)
	}
}

// seeSeq records seq for meta in seqs and returns how many events were skipped just before
// it. A meta seen for the first time starts there, and a replay (at or below the highest seen)
// skips nothing: the collector can write an event twice after it restarts.
func seeSeq(seqs map[uint32]uint64, meta uint32, seq uint64) uint64 {
	last, ok := seqs[meta]
	if !ok || seq > last {
		seqs[meta] = seq
	}
	if !ok || seq <= last+1 {
		return 0
	}
	return seq - last - 1
}
