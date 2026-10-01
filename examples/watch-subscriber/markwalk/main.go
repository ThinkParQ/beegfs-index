// From the collector's segment files to a GUFI suspect file.
//
// The collector receives Watch's events and seals them into numbered segment files. markwalk
// reads the new ones in number order and turns each batch into the input GUFI's incremental
// update wants: one "<inode> d" line per directory to rescan.
//
// There are three steps between an event and that file, and each one can lose data quietly:
//
//  1. an event names a path        -> which directories does it dirty?
//  2. a directory                  -> is it still there, and does the index know its parents?
//  3. a set of directories         -> "<inode> d" lines
//
// Each batch goes to <work>/batch-<last segment>/suspects, and only then does the state file
// move past it, so a crash repeats a batch and never skips one. Running the incremental update
// on that file, and deleting segments once it succeeded, come next (DESIGN.md §4, §6).
//
//	go run ./markwalk -spool /local/bt/collector/spool -mount /mnt/beegfs -index /var/lib/gufi
//
// Pass -once to process what is there and exit, or -demo to see the walk handle five awkward
// situations on a temp tree.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	bw "github.com/thinkparq/protobuf/go/beewatch"
)

// ---------------------------------------------------------------------------
// Part 1: an event names a path. Which directories does it dirty?
//
// GUFI rescans directories, so every event has to become one or more of them. Usually that is
// just the parent, because only the listing changed. The exceptions are where the bugs live.
// ---------------------------------------------------------------------------

// dirsFor returns the paths this event dirties, and — when the inode has more than one name —
// its entry ID, so the other names can be looked up later. See siblingDirs.
//
// A listing change names the parent. An inode change names the path itself: markChain marks it
// if it is a directory, whose own mode, owner and times live in its own db.db, and otherwise
// rises to the parent, where a file's are recorded. An event on the root has path "/", which
// makes the mount itself, never the directory the mount sits in.
func dirsFor(ev *bw.V2Event, mount string, trackOpens bool) (dirs []string, entryID string) {
	// Event paths are mount-relative, so they need the mount put back on the front.
	abs := func(p string) string { return filepath.Join(mount, p) }
	path := abs(ev.GetPath())
	parent := filepath.Dir(path)

	switch ev.GetType() {
	case bw.V2Event_INVALID:
		return nil, "" // carries no path; joining "" would name the mount's parent

	case bw.V2Event_MKDIR:
		// The new directory needs its own line. Marking only the parent creates it in the
		// index with no db.db, and its contents never show up.
		return []string{parent, path}, ""

	case bw.V2Event_RENAME:
		// RENAME fires once, on the source, with the destination in target_path. Mark the
		// common ancestor as well: two parents alone make the diff see a delete plus a
		// create, which throws the moved databases away instead of moving them.
		//
		// The destination itself too. GUFI's incremental update rebuilds a moved or new
		// directory it has no database for on its own, so this is cheap insurance, not a
		// requirement: a directory made and moved in one batch was marked by its MKDIR
		// under a path that is gone by now. A file there rises to dst, marked anyway.
		target := abs(ev.GetTargetPath())
		dst := filepath.Dir(target)
		return []string{commonAncestor(parent, dst), parent, dst, target}, ""

	case bw.V2Event_HARDLINK:
		// nlink is stored per name, in whichever directory lists it, so the directory
		// holding the other name is now stale too.
		// Both names are in the event, so nothing needs looking up. If the inode already
		// had other names, those are found the same way a write finds them, below.
		return []string{parent, filepath.Dir(abs(ev.GetTargetPath()))}, linkedEntryID(ev)

	case bw.V2Event_OPEN_READ, bw.V2Event_OPEN_WRITE, bw.V2Event_OPEN_READ_WRITE:
		// Opens are the only events that can keep atime current, and there are a great many
		// of them. Off unless you ask.
		if !trackOpens {
			return nil, ""
		}
		return []string{parent}, linkedEntryID(ev) // only files emit opens; see FLUSH below

	case bw.V2Event_OPEN_BLOCKED:
		return nil, "" // the open was refused, so nothing on the inode changed

	case bw.V2Event_INODE_LOCKED:
		return nil, "" // its path is a bare filename, not a path

	case bw.V2Event_SETATTR, bw.V2Event_STRIPE_PATTERN_CHANGED:
		// The inode changed, and it can be a directory's (chmod, setpattern on a directory):
		// the path itself if it is a directory, else its parent.
		return []string{path}, linkedEntryID(ev)

	case bw.V2Event_FLUSH, bw.V2Event_TRUNCATE, bw.V2Event_CLOSE_WRITE, bw.V2Event_LAST_WRITER_CLOSED:
		// Only a regular file is written, truncated or closed after writing, so these name
		// the parent straight away. Naming the file makes every file a separate entry that
		// markChain must stat before it rises to the same parent: a tree written once and
		// removed is millions of stats of names that are gone (fslab, 15M events: >15 min).
		return []string{parent}, linkedEntryID(ev)

	default:
		// CREATE MKNOD SYMLINK RMDIR UNLINK: only the listing changed. An UNLINK that leaves
		// other names also changed their link count; linkedEntryID finds them.
		// A type added after this was written lands here too, which marks the parent: a
		// rescan too many, never one too few.
		return []string{parent}, linkedEntryID(ev)
	}
}

// linkedEntryID returns the event's entry ID when the inode has more than one name and
// this event changed the inode, or took away one of its names.
//
// A write reaches the inode, so every name reports a new size and mtime. The event names only
// one of them. num_links says the situation exists; only the index knows where the others are.
//
// The entry ID is used rather than the path because hard links share it, the index records it,
// and it needs no stat — so a name unlinked before the batch runs still refreshes the rest.
func linkedEntryID(ev *bw.V2Event) string {
	// UNLINK reports the names left after it (measured: 3 names, rm -> 2, rm -> 1, the last rm
	// reports none), so any left means their link count changed. Every other type counts the
	// event's own name too.
	if ev.GetType() == bw.V2Event_UNLINK {
		if ev.GetNumLinks() >= 1 && validEntryID(ev.GetEntryId()) {
			return ev.GetEntryId()
		}
		return ""
	}
	if ev.GetNumLinks() <= 1 {
		return ""
	}
	switch ev.GetType() {
	case bw.V2Event_FLUSH, bw.V2Event_TRUNCATE, bw.V2Event_SETATTR,
		bw.V2Event_CLOSE_WRITE, bw.V2Event_LAST_WRITER_CLOSED,
		bw.V2Event_STRIPE_PATTERN_CHANGED, bw.V2Event_HARDLINK,
		// atime is on the inode too, so a read through one name moves it for all of them
		bw.V2Event_OPEN_READ, bw.V2Event_OPEN_WRITE, bw.V2Event_OPEN_READ_WRITE:
		if id := ev.GetEntryId(); validEntryID(id) {
			return id
		}
	}
	return ""
}

// validEntryID reports whether id is safe to put in a SQL string literal.
//
// BeeGFS builds an entry ID as three hex fields joined by dashes, and the reserved ones are
// plain words. Nothing legitimate carries a quote. It is checked rather than escaped because
// the value reaches gufi_query as text on a command line, with nothing to bind it to.
func validEntryID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '-':
		default:
			return false
		}
	}
	return true
}

// siblingDirs asks the index where else these inodes are listed.
//
// beegfs_entries is the table the BeeGFS index plugin fills in. It carries the entry ID the
// event already gave us, so no stat is needed and nothing depends on how the client computes
// inode numbers.
//
// This is the only part of the program that reads from the index rather than the filesystem,
// and the only part that cannot be answered per event: it needs every entry ID in the batch
// so it can ask once.
func siblingDirs(entryIDs []string, mount, indexRoot, queryBin string) []string {
	if len(entryIDs) == 0 || queryBin == "" {
		return nil
	}

	quoted := make([]string, 0, len(entryIDs))
	for _, id := range entryIDs {
		quoted = append(quoted, "'"+id+"'") // filtered through validEntryID already
	}
	sql := fmt.Sprintf("SELECT path() FROM beegfs_entries WHERE entry_id IN (%s);",
		strings.Join(quoted, ","))

	out, err := exec.Command(queryBin, "-d", "\n", "-E", sql, indexRoot).Output()
	if err != nil {
		slog.Warn("sibling lookup failed; other names of multi-name files keep a stale size", "error", err)
		return nil
	}

	var dirs []string
	seen := map[string]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		indexDir := strings.TrimSpace(line)
		if indexDir == "" {
			continue
		}
		rel, err := filepath.Rel(indexRoot, indexDir)
		if err != nil || strings.HasPrefix(rel, "..") {
			continue
		}
		src := filepath.Join(mount, rel)
		if !seen[src] {
			seen[src] = true
			dirs = append(dirs, src)
		}
	}
	return dirs
}

// ---------------------------------------------------------------------------
// Part 2: a directory. Is it still there, and does the index know its parents?
//
// Two things can be wrong by the time you act. The path may be gone, because events describe
// the filesystem as it was. Or the index may never have heard of the directories above it,
// which is what a client mounted without an event mask leaves behind.
//
// Both are fixed by walking up. The rest is knowing when to stop.
// ---------------------------------------------------------------------------

// statCache remembers statDir answers for one batch, so a path many marks share (an
// ancestor, the inode lookup after markChain) costs one round trip to a metadata server, not
// one per mark.
//
// It holds at most limit answers and drops the least recently used one to make room, so the
// paths a batch keeps asking about stay and memory stays bounded (about 150 bytes plus the
// path per answer). Forgetting is always safe: the path is only asked again.
//
// A batch is a moment: what changes while it runs is in the next batch's events, so a cache
// lives for one batch only.
type statCache struct {
	limit  int
	byPath map[string]*statEntry
	// A ring through a sentinel: head.next is the most recently used, head.prev the least.
	head  statEntry
	stats int // statDir calls, the round trips this batch made
}

type statEntry struct {
	path       string
	inode      uint64
	isDir      bool
	prev, next *statEntry
}

func newStatCache(limit int) *statCache {
	c := &statCache{limit: max(limit, 1), byPath: map[string]*statEntry{}}
	c.head.prev, c.head.next = &c.head, &c.head
	return c
}

func (c *statCache) stat(path string) (inode uint64, isDir bool) {
	if e, ok := c.byPath[path]; ok {
		c.unlink(e)
		c.pushFront(e)
		return e.inode, e.isDir
	}
	e := &statEntry{path: path}
	e.inode, e.isDir = statDir(path)
	c.stats++
	if len(c.byPath) >= c.limit {
		old := c.head.prev
		c.unlink(old)
		delete(c.byPath, old.path)
	}
	c.pushFront(e)
	c.byPath[path] = e
	return e.inode, e.isDir
}

func (c *statCache) unlink(e *statEntry) {
	e.prev.next, e.next.prev = e.next, e.prev
}

func (c *statCache) pushFront(e *statEntry) {
	e.prev, e.next = &c.head, c.head.next
	c.head.next.prev = e
	c.head.next = e
}

// statDir answers "is this a directory" and "what is its inode" from a single stat.
//
// The type check matters: a directory can be replaced by a file of the same name before you
// get here, and writing that file's inode into the suspect file tells GUFI the inode is a
// directory when it is not.
func statDir(path string) (inode uint64, isDir bool) {
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return 0, false
	}
	return info.Sys().(*syscall.Stat_t).Ino, true
}

// indexCovers reports whether the index already holds dir.
//
// It looks for the db.db, not the directory. GUFI makes the directory in one pass and the
// database in another, so a run that died in between leaves a directory with no database.
// Counting that as covered strands everything below it: every later event stops walking at
// the same broken level and never gets past it.
func indexCovers(dir, mount, indexRoot string) bool {
	rel, err := filepath.Rel(mount, dir)
	if err != nil {
		return false
	}
	info, err := os.Stat(filepath.Join(indexRoot, rel, "db.db"))
	return err == nil && info.Mode().IsRegular()
}

// markChain returns the directories to name for dir, shallowest first.
//
// One walk, two stops. First rise to something that exists and is still a directory: that is
// the deepest thing you can honestly mark. Then keep rising while the *parent* has no db.db,
// collecting each level on the way.
//
// Why the parent rather than the directory itself? GUFI creates index directories with plain
// mkdir, not mkdir -p, so it fails when the parent is absent. The only question that matters
// is whether this directory can be placed. Whether it has a database of its own is beside the
// point: if it has none, marking it is how it gets one.
func markChain(dir, mount, indexRoot string, trace bool, stat func(string) (uint64, bool)) []string {
	cur := dir

	for {
		if _, ok := stat(cur); ok {
			break
		}
		if trace {
			fmt.Printf("      %-26s %s, go up\n", rel(cur, mount), why(cur))
		}
		parent := filepath.Dir(cur)
		if !within(parent, mount) {
			return nil
		}
		cur = parent
	}

	chain := []string{cur}
	for cur != mount {
		parent := filepath.Dir(cur)
		if !within(parent, mount) {
			break
		}
		if indexCovers(parent, mount, indexRoot) {
			if trace {
				fmt.Printf("      %-26s parent is in the index, stop\n", rel(cur, mount))
			}
			break
		}
		if trace {
			fmt.Printf("      %-26s parent has no db.db, mark it too\n", rel(cur, mount))
		}
		chain = append([]string{parent}, chain...)
		cur = parent
	}
	return chain
}

// ---------------------------------------------------------------------------
// Part 3: a set of directories becomes the file.
//
// One "<inode> d" line each. Order does not matter, because GUFI sorts creations by depth
// itself. Duplicates do, so dedupe by inode.
// ---------------------------------------------------------------------------

// writeSuspects writes the suspect file for dirs and returns how many directories it marks
// and how many stats that took. cacheSize bounds the stat cache (see statCache).
func writeSuspects(dirs []string, mount, indexRoot, out string, trace bool, cacheSize int) (marks, stats int, err error) {
	seen := map[uint64]string{}
	stat := newStatCache(cacheSize)
	for _, d := range dirs {
		for _, c := range markChain(d, mount, indexRoot, trace, stat.stat) {
			if ino, ok := stat.stat(c); ok {
				seen[ino] = rel(c, mount)
			}
		}
	}
	if len(seen) == 0 {
		return 0, stat.stats, nil
	}

	var b strings.Builder
	for ino, name := range seen {
		fmt.Fprintf(&b, "%d d\n", ino)
		if trace {
			fmt.Printf("    %d d        # %s\n", ino, name)
		}
	}
	return len(seen), stat.stats, writeFileAtomic(out, []byte(b.String()))
}

func main() {
	spoolDir := flag.String("spool", "/var/lib/index-sync/spool", "the collector's segment directory")
	statePath := flag.String("state", "/var/lib/index-sync/markwalk.json", "how far markwalk got")
	work := flag.String("work", "/var/lib/index-sync/markwalk", "where each batch's suspect file goes")
	mount := flag.String("mount", "/mnt/beegfs", "the BeeGFS mount, so paths can be made absolute")
	index := flag.String("index", "/var/lib/gufi", "GUFI index root")
	query := flag.String("query", "gufi_query", "gufi_query binary, used to find the other names of a multi-link inode")
	every := flag.Duration("every", time.Minute, "how long a batch collects before it is made")
	poll := flag.Duration("poll", 5*time.Second, "how often to look for new segments when there are none")
	maxSegs := flag.Int("max-segments", 100, "most segments in one batch")
	cacheSize := flag.Int("stat-cache", 200_000,
		"most stat answers a batch keeps, least recently used dropped first (~150 bytes + path each)")
	opens := flag.Bool("track-opens", false, "include open events, for atime")
	once := flag.Bool("once", false, "process the segments there now and exit")
	demo := flag.Bool("demo", false, "run the walk against a temp tree and exit")
	update := flag.String("update", "",
		"gufi_incremental_update binary; run after every suspect file, and only its exit 0 finishes a batch (default: suspect files only)")
	plugin := flag.String("plugin", "", "the update's --plugin, e.g. beegfs_index_ops:/opt/beegfs/lib/libbeegfs_indexing.so")
	tree := flag.String("tree", "", "the directory the index covers, the update's source tree (default: -mount)")
	threads := flag.Int("threads", min(runtime.NumCPU(), 16), "the update's -n")
	updateTimeout := flag.Duration("update-timeout", time.Hour,
		"longest one update may run before it is killed and the batch tried again; it walks the whole tree, so size it to that")
	keepFailed := flag.Int("keep-failed", 5, "failed batch directories kept for diagnosis")
	flag.Parse()

	if *demo {
		runDemo()
		return
	}
	if *maxSegs < 1 || *cacheSize < 1 || *every <= 0 || *poll <= 0 || *threads < 1 || *updateTimeout <= 0 {
		slog.Error("max-segments, stat-cache and threads must be at least 1; every, poll and update-timeout above 0")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r := &reader{
		log: slog.Default(), spool: *spoolDir, statePath: *statePath, work: *work,
		mount: filepath.Clean(*mount), index: filepath.Clean(*index), query: *query,
		trackOpens: *opens, maxSegments: *maxSegs, statCacheSize: *cacheSize,
	}
	if *update != "" {
		u, err := newUpdater(*update, *plugin, *tree, r.mount, r.index, *threads, *updateTimeout, *keepFailed)
		if err != nil {
			slog.Error("bad -update settings", "error", err)
			os.Exit(2)
		}
		r.update = u
		slog.Info("incremental update after every batch", "bin", u.bin, "index", u.index, "tree", u.tree,
			"plugin", u.plugin, "threads", u.threads, "timeout", u.timeout)
	}
	if err := r.run(ctx, *every, *poll, *once); err != nil {
		slog.Error("markwalk stopped", "error", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func commonAncestor(a, b string) string {
	for !within(b, a) {
		parent := filepath.Dir(a)
		if parent == a {
			return a
		}
		a = parent
	}
	return a
}

func within(path, root string) bool {
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}

func why(path string) string {
	if _, err := os.Stat(path); err != nil {
		return "gone"
	}
	return "not a directory"
}

func rel(path, root string) string {
	if path == root {
		return "<root>"
	}
	if r, err := filepath.Rel(root, path); err == nil {
		return r
	}
	return path
}
