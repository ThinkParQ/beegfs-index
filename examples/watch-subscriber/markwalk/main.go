// From Watch events to a GUFI suspect file.
//
// The subscriber in the parent directory prints events. This one does the next job: it turns
// them into the input GUFI's incremental update actually wants.
//
// There are three steps between an event and that file, and each one can lose data quietly:
//
//  1. an event names a path        -> which directories does it dirty?
//  2. a directory                  -> is it still there, and does the index know its parents?
//  3. a set of directories         -> "<inode> d" lines
//
// Watch dials OUT to this process, so we are the gRPC server. Run it, make some noise on the
// mount, and watch the suspect file fill up:
//
//	go run ./markwalk -mount /mnt/beegfs -index /var/lib/gufi -out /tmp/suspects
//
// Pass -demo to skip the network and see the walk handle five awkward situations instead.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	bw "github.com/thinkparq/protobuf/go/beewatch"
	"google.golang.org/grpc"
)

// ---------------------------------------------------------------------------
// Part 1: an event names a path. Which directories does it dirty?
//
// GUFI rescans directories, so every event has to become one or more of them. Usually that is
// just the parent, because only the listing changed. The exceptions are where the bugs live.
// ---------------------------------------------------------------------------

// dirsFor returns the directories this event dirties, and — when the inode has more than one
// name — its entry ID, so the other names can be looked up later. See siblingDirs.
func dirsFor(ev *bw.V2Event, mount string, trackOpens bool) (dirs []string, entryID string) {
	// Event paths are mount-relative, so they need the mount put back on the front.
	abs := func(p string) string { return filepath.Join(mount, p) }
	path := abs(ev.GetPath())
	parent := filepath.Dir(path)

	switch ev.GetType() {
	case bw.V2Event_MKDIR:
		// The new directory needs its own line. Marking only the parent creates it in the
		// index with no db.db, and its contents never show up.
		return []string{parent, path}, ""

	case bw.V2Event_RENAME:
		// RENAME fires once, on the source, with the destination in target_path. Mark the
		// common ancestor as well: two parents alone make the diff see a delete plus a
		// create, which throws the moved databases away instead of moving them.
		dst := filepath.Dir(abs(ev.GetTargetPath()))
		return []string{commonAncestor(parent, dst), parent, dst}, ""

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
		return []string{parent}, linkedEntryID(ev)

	case bw.V2Event_OPEN_BLOCKED:
		return nil, "" // the open was refused, so nothing on the inode changed

	case bw.V2Event_INODE_LOCKED:
		return nil, "" // its path is a bare filename, not a path

	default:
		// CREATE MKNOD SYMLINK RMDIR UNLINK: only the listing changed.
		// FLUSH TRUNCATE SETATTR CLOSE_WRITE LAST_WRITER_CLOSED STRIPE_PATTERN_CHANGED:
		// the inode changed, and the parent is where its size and mtime are recorded.
		//
		return []string{parent}, linkedEntryID(ev)
	}
}

// linkedEntryID returns the event's entry ID when the inode has more than one name and
// this event changed the inode rather than a listing.
//
// A write reaches the inode, so every name reports a new size and mtime. The event names only
// one of them. num_links says the situation exists; only the index knows where the others are.
//
// The entry ID is used rather than the path because hard links share it, the index records it,
// and it needs no stat — so a name unlinked before the batch runs still refreshes the rest.
func linkedEntryID(ev *bw.V2Event) string {
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
		log.Printf("sibling lookup failed, other names keep a stale size: %v", err)
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
func markChain(dir, mount, indexRoot string, trace bool) []string {
	cur := dir

	for {
		if _, ok := statDir(cur); ok {
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

func writeSuspects(dirs []string, mount, indexRoot, out string, trace bool) (int, error) {
	seen := map[uint64]string{}
	for _, d := range dirs {
		for _, c := range markChain(d, mount, indexRoot, trace) {
			if ino, ok := statDir(c); ok {
				seen[ino] = rel(c, mount)
			}
		}
	}
	if len(seen) == 0 {
		return 0, nil
	}

	var b strings.Builder
	for ino, name := range seen {
		fmt.Fprintf(&b, "%d d\n", ino)
		if trace {
			fmt.Printf("    %d d        # %s\n", ino, name)
		}
	}
	return len(seen), os.WriteFile(out, []byte(b.String()), 0o644)
}

// ---------------------------------------------------------------------------
// Part 4: the subscriber. Watch dials in, we collect, and every interval we flush.
// ---------------------------------------------------------------------------

type collector struct {
	bw.UnimplementedSubscriberServer

	mount, indexRoot, out, queryBin string
	trackOpens                      bool

	mu      sync.Mutex
	pending map[string]struct{}
	// links holds the entry IDs of files with more than one name. Their other names sit in
	// directories no event will ever mention, so they are resolved once per batch.
	links  map[string]struct{}
	events int
}

func (c *collector) add(dirs []string, entryID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range dirs {
		c.pending[d] = struct{}{}
	}
	if entryID != "" {
		c.links[entryID] = struct{}{}
	}
	c.events++
}

// flush turns everything collected so far into the suspect file and empties the set.
//
// Note what this does NOT do: it does not wait for GUFI to finish before clearing. A real
// service must not acknowledge events until the rescan they caused has actually run, or a
// crash in between loses them silently.
func (c *collector) flush() {
	c.mu.Lock()
	dirs := make([]string, 0, len(c.pending))
	for d := range c.pending {
		dirs = append(dirs, d)
	}
	links := make([]string, 0, len(c.links))
	for f := range c.links {
		links = append(links, f)
	}
	n := c.events
	c.pending = map[string]struct{}{}
	c.links = map[string]struct{}{}
	c.events = 0
	c.mu.Unlock()

	if len(dirs) == 0 && len(links) == 0 {
		return
	}
	fmt.Printf("\n%s  %d events -> %d directories\n", time.Now().Format("15:04:05"), n, len(dirs))

	// One query for the whole batch, not one per event.
	if extra := siblingDirs(links, c.mount, c.indexRoot, c.queryBin); len(extra) > 0 {
		fmt.Printf("  %d multi-link inodes also live in: %v\n", len(links), extra)
		dirs = append(dirs, extra...)
	}
	marks, err := writeSuspects(dirs, c.mount, c.indexRoot, c.out, true)
	if err != nil {
		log.Printf("write %s: %v", c.out, err)
		return
	}
	fmt.Printf("  wrote %d marks to %s\n", marks, c.out)
	fmt.Printf("  gufi_incremental_update --suspect-method 1 --suspect-file %s %s %s <parking-lot>\n",
		c.out, c.indexRoot, c.mount)
}

// ReceiveEvents is the only RPC. Watch streams Events in, we stream Responses back to
// acknowledge them. An empty EventFilter on the first Response means "send me everything".
func (c *collector) ReceiveEvents(stream bw.Subscriber_ReceiveEventsServer) error {
	log.Println("Watch connected")
	if err := stream.Send(&bw.Response{CompletedSeq: 0}); err != nil {
		return err
	}

	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			log.Println("Watch closed the stream")
			return nil
		}
		if err != nil {
			return err
		}

		if v2 := ev.GetV2(); v2 != nil {
			c.add(dirsFor(v2, c.mount, c.trackOpens))
		}

		// Acknowledging here is too early, see flush. Fine for a demo.
		if err := stream.Send(&bw.Response{CompletedSeq: ev.GetSeqId()}); err != nil {
			return err
		}
	}
}

func main() {
	addr := flag.String("listen", "0.0.0.0:50052", "address Watch should dial")
	mount := flag.String("mount", "/mnt/beegfs", "the BeeGFS mount, so paths can be made absolute")
	index := flag.String("index", "/var/lib/gufi", "GUFI index root")
	out := flag.String("out", "/tmp/suspects", "suspect file to write")
	query := flag.String("query", "gufi_query", "gufi_query binary, used to find the other names of a multi-link inode")
	every := flag.Duration("every", 5*time.Second, "how often to flush")
	opens := flag.Bool("track-opens", false, "include open events, for atime")
	demo := flag.Bool("demo", false, "skip the network, run the walk against a temp tree")
	flag.Parse()

	if *demo {
		runDemo()
		return
	}

	c := &collector{
		mount: *mount, indexRoot: *index, out: *out, queryBin: *query, trackOpens: *opens,
		pending: map[string]struct{}{},
		links:   map[string]struct{}{},
	}

	go func() {
		for range time.Tick(*every) {
			c.flush()
		}
	}()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	g := grpc.NewServer()
	bw.RegisterSubscriberServer(g, c)

	log.Printf("listening on %s; mount=%s index=%s out=%s", *addr, *mount, *index, *out)
	log.Fatal(g.Serve(lis))
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
