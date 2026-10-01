// Command spoolcat prints collector spool files as JSON lines, one event per
// line, with the same fields the collector's old .jsonl files had. It is the
// way to grep a spool, and a working example of reading one.
//
//	spoolcat spool/*.binpb | grep /mytest/        # every sealed file
//	spoolcat -count spool/*.binpb                 # number of events
//	spoolcat -follow spool                        # new events as they are written
//	spoolcat -follow -from-start spool            # the whole spool, then follow
//	spoolcat -last 12 spool                       # newest 12 events, then exit
//	spoolcat -type MKDIR,RENAME -meta 2 -grep '^/soak' spool/*.binpb
//
// A .partial file may end part way through a record; that tail is skipped, and
// in -follow mode read once the rest of it is written. In a sealed file the
// same thing means damage, and is reported.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/thinkparq/beegfs-index/examples/watch-subscriber/spool"
	bw "github.com/thinkparq/protobuf/go/beewatch"
)

type filter struct {
	grep  *regexp.Regexp
	types map[string]bool
	metas map[uint32]bool
}

func (f filter) match(l spool.Line) bool {
	if f.metas != nil && !f.metas[l.Meta] {
		return false
	}
	if f.types != nil && !f.types[l.Type] {
		return false
	}
	if f.grep != nil && !f.grep.MatchString(l.Path) && !f.grep.MatchString(l.TargetPath) {
		return false
	}
	return true
}

type printer struct {
	f     filter
	count bool
	n     int64
	w     *bufio.Writer
	enc   *json.Encoder
}

func (p *printer) event(ev *bw.Event) error {
	l := spool.ToLine(ev)
	if !p.f.match(l) {
		return nil
	}
	p.n++
	if p.count {
		return nil
	}
	return p.enc.Encode(l)
}

// cat prints whole files in the order given.
func (p *printer) cat(paths []string) error {
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, tail, err := spool.Decode(data, p.event)
		if err != nil {
			return err
		}
		if tail && !strings.HasSuffix(path, spool.PartialSuffix) {
			return fmt.Errorf("%s: damaged record after the last complete one", path)
		}
	}
	return nil
}

// spoolFiles lists sealed and partial files in dir in name order, which is the
// order they were written. Each is keyed by its sealed name, so a file keeps
// its key when the collector seals it.
func spoolFiles(dir string) ([][2]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out [][2]string
	for _, e := range ents {
		n := e.Name()
		key := strings.TrimSuffix(n, spool.PartialSuffix)
		if !strings.HasSuffix(key, spool.Ext) {
			continue
		}
		out = append(out, [2]string{key, filepath.Join(dir, n)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out, nil
}

// readFrom decodes the complete records in path from offset off on, and
// returns the offset to carry on from.
func readFrom(path string, off int64, fn func(*bw.Event) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return off, err
	}
	defer f.Close()
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return off, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return off, err
	}
	n, _, err := spool.Decode(data, fn)
	return off + int64(n), err
}

// follow prints events as they reach the spool, across seals, until killed.
func (p *printer) follow(dir string, fromStart bool, every time.Duration) error {
	offsets := map[string]int64{}
	first := true
	for {
		files, err := spoolFiles(dir)
		if err != nil {
			return err
		}
		live := map[string]bool{}
		for _, kf := range files {
			key, path := kf[0], kf[1]
			live[key] = true
			fn := p.event
			if first && !fromStart {
				fn = func(*bw.Event) error { return nil } // history: find where it ends
			}
			off, err := readFrom(path, offsets[key], fn)
			if errors.Is(err, os.ErrNotExist) {
				continue // sealed between the listing and the open; next poll has it
			}
			if err != nil {
				return err
			}
			offsets[key] = off
		}
		for key := range offsets {
			if !live[key] {
				delete(offsets, key) // removed by whoever consumes the spool
			}
		}
		first = false
		if err := p.w.Flush(); err != nil {
			return err
		}
		time.Sleep(every)
	}
}

// last prints the newest n matching events of the newest file, preferring the
// one being written.
func (p *printer) last(dir string, n int) error {
	files, err := spoolFiles(dir)
	if err != nil || len(files) == 0 {
		return err
	}
	path := files[len(files)-1][1]
	for _, kf := range files {
		if strings.HasSuffix(kf[1], spool.PartialSuffix) {
			path = kf[1]
		}
	}
	var ring []spool.Line
	_, err = readFrom(path, 0, func(ev *bw.Event) error {
		if l := spool.ToLine(ev); p.f.match(l) {
			ring = append(ring, l)
			if len(ring) > n {
				ring = ring[1:]
			}
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return p.last(dir, n) // sealed while we looked; look again
	}
	if err != nil {
		return err
	}
	for _, l := range ring {
		if err := p.enc.Encode(l); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	follow := flag.Bool("follow", false, "follow a spool directory, printing new events as they are written")
	fromStart := flag.Bool("from-start", false, "with -follow, print what is already there first")
	last := flag.Int("last", 0, "print the newest N events of a spool directory and exit")
	count := flag.Bool("count", false, "print how many events match instead of the events")
	grep := flag.String("grep", "", "only events whose path or target path matches this regexp")
	types := flag.String("type", "", "only these event types, comma separated (MKDIR,RENAME)")
	metas := flag.String("meta", "", "only these meta IDs, comma separated (1,3)")
	interval := flag.Duration("interval", 200*time.Millisecond, "with -follow, how often to look for new events")
	flag.Parse()

	var f filter
	if *grep != "" {
		f.grep = regexp.MustCompile(*grep)
	}
	if *types != "" {
		f.types = map[string]bool{}
		for _, t := range strings.Split(*types, ",") {
			f.types[strings.ToUpper(strings.TrimSpace(t))] = true
		}
	}
	if *metas != "" {
		f.metas = map[uint32]bool{}
		for _, m := range strings.Split(*metas, ",") {
			id, err := strconv.ParseUint(strings.TrimSpace(m), 10, 32)
			if err != nil {
				fmt.Fprintln(os.Stderr, "spoolcat: bad -meta:", err)
				os.Exit(2)
			}
			f.metas[uint32(id)] = true
		}
	}

	w := bufio.NewWriterSize(os.Stdout, 1<<16)
	p := &printer{f: f, count: *count, w: w, enc: json.NewEncoder(w)}
	var err error
	switch {
	case *follow && flag.NArg() == 1:
		err = p.follow(flag.Arg(0), *fromStart, *interval)
	case *last > 0 && flag.NArg() == 1:
		err = p.last(flag.Arg(0), *last)
	case !*follow && *last == 0 && flag.NArg() > 0:
		err = p.cat(flag.Args())
	default:
		fmt.Fprintln(os.Stderr, "usage: spoolcat [filters] FILE... | -follow DIR | -last N DIR")
		os.Exit(2)
	}
	if err == nil && *count {
		fmt.Fprintln(w, p.n)
	}
	if ferr := w.Flush(); err == nil && !errors.Is(ferr, os.ErrClosed) {
		err = ferr
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "spoolcat:", err)
		os.Exit(1)
	}
}
