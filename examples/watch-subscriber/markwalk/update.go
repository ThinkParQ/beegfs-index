package main

// Part 5: the incremental update. After a batch's suspect file is written, gufi_incremental_update
// brings the index up to date with it, and only a run that exits 0 moves markwalk past the batch.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// errUpdate marks a batch whose incremental update failed. The batch is not done: markwalk keeps
// its state where it was and makes the batch again on the next round.
var errUpdate = errors.New("incremental update failed")

// updater runs gufi_incremental_update for one batch at a time.
type updater struct {
	bin     string // gufi_incremental_update
	plugin  string // its --plugin value, "" for none
	threads int
	tree    string // the directory the index covers, under the mount
	index   string // the index of tree: <index root>/<tree relative to the mount>
	timeout time.Duration
	// keepFailed is how many failed batch directories stay for diagnosis; older ones go.
	keepFailed int
}

// newUpdater checks the -update settings. tree defaults to the mount and must be under it; the
// index of tree is the index root's directory at the same place below it.
func newUpdater(bin, plugin, tree, mount, indexRoot string, threads int, timeout time.Duration,
	keepFailed int) (*updater, error) {
	if tree == "" {
		tree = mount
	}
	tree = filepath.Clean(tree)
	if !within(tree, mount) {
		return nil, fmt.Errorf("-tree %s is not under -mount %s", tree, mount)
	}
	index := filepath.Join(indexRoot, strings.TrimPrefix(strings.TrimPrefix(tree, mount), "/"))
	if _, err := os.Stat(filepath.Join(index, "db.db")); err != nil {
		return nil, fmt.Errorf("no index of %s to update: %w", tree, err)
	}
	path, err := exec.LookPath(bin)
	if err != nil {
		return nil, err
	}
	return &updater{bin: path, plugin: plugin, threads: threads, tree: tree, index: index,
		timeout: timeout, keepFailed: keepFailed}, nil
}

// run updates the index from suspects. dir is the batch's own directory: the run's working
// directory (the tool makes a temporary database there), its parking lot and its log.
func (u *updater) run(ctx context.Context, dir, suspects string) error {
	ctx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()
	logPath := filepath.Join(dir, "update.log")
	logf, err := os.Create(logPath)
	if err != nil {
		return err
	}
	defer logf.Close()

	argv := []string{"-n", strconv.Itoa(u.threads), "--suspect-method", "1", "--suspect-file", suspects}
	if u.plugin != "" {
		argv = append(argv, "--plugin", u.plugin)
	}
	argv = append(argv, u.index, u.tree, filepath.Join(dir, "parking"))
	cmd := exec.CommandContext(ctx, u.bin, argv...)
	cmd.Dir, cmd.Stdout, cmd.Stderr = dir, logf, logf
	cmd.WaitDelay = 10 * time.Second // SIGKILL went out on timeout; don't hang on its pipes

	err = cmd.Run()
	switch {
	case err == nil:
		return nil
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return fmt.Errorf("%w: timed out after %s (update.log has its output)", errUpdate, u.timeout)
	case ctx.Err() != nil:
		return ctx.Err() // markwalk is stopping; the batch is simply not done
	default:
		return fmt.Errorf("%w: %v: %s", errUpdate, err, tail(logPath, 400))
	}
}

// uncovered returns the marked directories, under tree and still there, that have no db.db in
// the index after a run. Exit 0 is necessary, not sufficient: the signed-inode bug (fc9e33e8)
// skipped directories and still exited 0. A directory made after the run walked past it shows up
// here too, and its own MKDIR marks it again in a later batch, so this only warns.
func (u *updater) uncovered(marked []string, mount, indexRoot string) []string {
	var out []string
	for _, d := range marked {
		if !within(d, u.tree) {
			continue
		}
		if fi, err := os.Stat(d); err != nil || !fi.IsDir() {
			continue
		}
		if !indexCovers(d, mount, indexRoot) {
			out = append(out, d)
		}
	}
	slices.Sort(out)
	return out
}

// keepFailedBatch renames a failed batch's directory out of the way, so the retry starts clean,
// and removes all but the newest keepFailed of them.
func (u *updater) keepFailedBatch(work, dir string) (string, error) {
	kept := fmt.Sprintf("%s.failed-%d", dir, time.Now().UnixNano())
	if err := os.Rename(dir, kept); err != nil {
		return "", err
	}
	old, err := filepath.Glob(filepath.Join(work, "batch-*.failed-*"))
	if err != nil {
		return kept, err
	}
	// The suffix is a fixed-width nanosecond time for years to come, so name order is age order
	// within one batch; across batches the batch number sorts first, which is also oldest first.
	slices.Sort(old)
	for len(old) > max(u.keepFailed, 0) {
		if err := os.RemoveAll(old[0]); err != nil {
			return kept, err
		}
		old = old[1:]
	}
	return kept, nil
}

// tail returns the last n bytes of a file, on one line, for an error message.
func tail(path string, n int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return ""
	}
	off := max(fi.Size()-n, 0)
	buf := make([]byte, fi.Size()-off)
	if _, err := f.ReadAt(buf, off); err != nil {
		return ""
	}
	return strings.Join(strings.Fields(string(buf)), " ")
}
