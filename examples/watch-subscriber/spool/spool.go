// Package spool reads the collector's spool files.
//
// A spool file is a stream of beewatch.Event messages, each written with
// protodelim.MarshalTo: the message length as a varint, then the message in
// protobuf binary form. Events keep the meta and sequence IDs Watch gave them,
// so (MetaId, SeqId) identifies an event across replays.
//
// A sealed file (Ext) is complete and never changes. A file still being written
// (Ext + PartialSuffix) can end part way through a record; Decode stops before
// such a tail so it can be read again once the rest arrives.
package spool

import (
	"bufio"
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	bw "github.com/thinkparq/protobuf/go/beewatch"
	"google.golang.org/protobuf/encoding/protodelim"
)

const (
	// Ext ends the name of a sealed spool file.
	Ext = ".binpb"
	// PartialSuffix follows Ext on a file still being written.
	PartialSuffix = ".partial"
	// SegPrefix starts the name of a spool file: SegPrefix, a zero-padded
	// number, Ext. The collector numbers files from 1 in the order it writes
	// them, and carries on from the highest after a restart.
	SegPrefix = "seg-"
)

// Segment is a sealed spool file.
type Segment struct {
	Number uint64
	Path   string
}

// SegmentNumber returns n for a sealed file named SegPrefix + n + Ext. Files
// still being written, and names from before numbering, return false.
func SegmentNumber(name string) (uint64, bool) {
	digits, ok := strings.CutPrefix(name, SegPrefix)
	if !ok {
		return 0, false
	}
	digits, ok = strings.CutSuffix(digits, Ext)
	if !ok || digits == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	return n, err == nil
}

// Sealed lists the sealed segments in dir numbered above after, lowest first.
// It sorts by number, not by name: the padding is a width, not a limit.
func Sealed(dir string, after uint64) ([]Segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []Segment
	for _, e := range entries {
		if n, ok := SegmentNumber(e.Name()); ok && n > after && e.Type().IsRegular() {
			segs = append(segs, Segment{Number: n, Path: filepath.Join(dir, e.Name())})
		}
	}
	slices.SortFunc(segs, func(a, b Segment) int { return cmp.Compare(a.Number, b.Number) })
	return segs, nil
}

// Reader reads events one at a time from a complete spool file.
type Reader struct {
	r *bufio.Reader
}

// NewReader returns a Reader for r. It buffers r itself; protodelim avoids an
// allocation per record when it is handed a *bufio.Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 1<<20)}
}

// Next decodes the next event into ev. It returns io.EOF at the end of the
// file. Any other error means the file is truncated or corrupt from this point.
func (r *Reader) Next(ev *bw.Event) error {
	err := protodelim.UnmarshalFrom(r.r, ev)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("spool record: %w", err)
	}
	return err
}

// Decode calls fn for each complete record at the start of data and returns
// how many bytes those records took. A record cut off by the end of data is
// not an error: consumed stops before it, and tail is true. That is how a file
// still being written ends. An error from fn stops decoding and is returned.
func Decode(data []byte, fn func(*bw.Event) error) (consumed int, tail bool, err error) {
	br := bytes.NewReader(data)
	for br.Len() > 0 {
		ev := &bw.Event{}
		if err := protodelim.UnmarshalFrom(br, ev); err != nil {
			// Whatever follows the last good record is either a record still
			// being written or damage; the caller knows which kind of file it has.
			return consumed, true, nil
		}
		if err := fn(ev); err != nil {
			return consumed, false, err
		}
		consumed = len(data) - br.Len()
	}
	return consumed, false, nil
}

// Line is an event flattened for people and scripts: spoolcat prints one per
// line as JSON, with the field names the collector's old JSON files used.
type Line struct {
	Meta       uint32 `json:"meta"`
	Seq        uint64 `json:"seq"`
	Type       string `json:"type"`
	Path       string `json:"path"`
	TargetPath string `json:"target_path,omitempty"`
	NumLinks   uint64 `json:"num_links,omitempty"`
	EntryID    string `json:"entry_id,omitempty"`
	ParentID   string `json:"parent_entry_id,omitempty"`
	Timestamp  int64  `json:"ts,omitempty"`
	Version    int    `json:"v,omitempty"` // 1 for a BeeGFS 7 event; omitted for v2
}

// ToLine flattens ev.
func ToLine(ev *bw.Event) Line {
	l := Line{Meta: ev.GetMetaId(), Seq: ev.GetSeqId()}
	if v2 := ev.GetV2(); v2 != nil {
		l.Type, l.Path, l.TargetPath = v2.GetType().String(), v2.GetPath(), v2.GetTargetPath()
		l.NumLinks, l.EntryID, l.ParentID = v2.GetNumLinks(), v2.GetEntryId(), v2.GetParentEntryId()
		l.Timestamp = v2.GetTimestamp()
	} else if v1 := ev.GetV1(); v1 != nil {
		l.Type, l.Path, l.TargetPath = v1.GetType().String(), v1.GetPath(), v1.GetTargetPath()
		l.EntryID, l.ParentID, l.Version = v1.GetEntryId(), v1.GetParentEntryId(), 1
	}
	return l
}
