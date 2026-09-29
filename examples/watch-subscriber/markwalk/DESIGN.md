# markwalk redesign — consume the collector's segment files

Related: `../DESIGN.md` (the collector that produces the segment files), `./FLOW.md` (the
event→suspect-file logic, still valid), ThinkParQ/beegfs-index issue #5.

---

## 1. What changes, in one sentence

Today `markwalk` **is a gRPC subscriber**: Watch dials into it and it processes events live
(`ReceiveEvents`, `main.go:377`). In the new architecture the **collector** owns the gRPC endpoint
and persists events to **binary segment files**; `markwalk` becomes a **reader of those segment
files**. The event→directory logic (Parts 1–3) is unchanged; only the *input source* changes, and
markwalk gains `metaId`/`seqId` for dedup, gap detection, and restartable progress.

```
BEFORE:  Watch ──gRPC──► markwalk (subscriber) ──► suspect file ──► gufi_incremental_update

AFTER:   Watch ──gRPC──► collector ──► seg-*.bin (durable)
                                           │
                                           ▼
                          markwalk (segment reader) ──► suspect file ──► gufi_incremental_update
```

Why this is better: the collector acks Watch as soon as a segment is durable (see `../DESIGN.md`
§7), so Watch's ring buffer is protected even if markwalk is slow or down. markwalk can crash and
restart, re-reading unprocessed segments, without any interaction with Watch. The two are decoupled
by the durable segment queue on disk.

---

## 2. Input: the segment format (recap)

Produced by the collector, defined in `../DESIGN.md` §6. Read that as the source of truth; summarised
here:

```
spool dir:  seg-0000001, seg-0000002, …          # sealed, immutable, consume in number order
record:     [uvarint payloadLen][uint32 metaId LE][uint64 seqId LE][payload]
payload:    protobuf wire bytes of a beewatch.Event  (proto.Unmarshal → *bw.Event)
```

- `metaId` / `seqId` are in the record header, so markwalk can dedup, detect Watch drops, and
  checkpoint progress **without unmarshaling the payload**.
- The `*bw.Event` gives the same fields markwalk already uses: `GetV2()`, and on the V2 message
  `GetType()`, `GetPath()`, `GetTargetPath()`, `GetNumLinks()`, `GetEntryId()`,
  `GetParentEntryId()`, plus `GetTargetParentId()`, `GetTimestamp()`, `GetMsgUserId()`. The outer
  `Event` also has `GetMetaId()`, `GetSeqId()`, `GetMetaMirror()`, `GetEventFlags()`.

A tiny reference decoder should live alongside the collector; markwalk's reader is the real consumer
and doubles as the format's second implementation (keep the two in sync).

---

## 3. New markwalk structure

Delete the gRPC server; add a spool reader. Keep Parts 1–3 verbatim.

| Concern | Current (`main.go`) | Change |
|---|---|---|
| Transport | `ReceiveEvents` gRPC server, `net.Listen`, `grpc.NewServer` (`377`, `432–440`) | **Remove.** Replace with a segment-file reader loop. |
| Event → dirs | `dirsFor` (`47`), `linkedEntryID` (`104`) | **Keep as-is** (operates on `*bw.V2Event`). Add `INVALID`/`INODE_LOCKED` guards (see §5). |
| Batch set | `collector.add` (`322`), `pending`/`links` maps | **Keep**, feed from the reader instead of the RPC. |
| Walk | `markChain` (`236`), `statDir`, `indexCovers` | **Keep unchanged** (filesystem + index, transport-independent). |
| Sibling lookup | `siblingDirs` (`150`) | **Keep unchanged.** |
| Suspect file | `writeSuspects` (`281`) | **Keep** (`<inode> d` lines; see §6). |
| Progress | acks to Watch (`398`) | **Replace** with a processed-segment checkpoint (see §4). |
| Flags | `-listen` (`405`) | Remove `-listen`; add `-spool`, `-state`; keep `-mount -index -out -query -every -track-opens -demo`. Add optional `-run`. |

Reader loop (shape):

```
lastProcessed := readState(statePath)          // highest fully-processed segment number
for {
    segs := listSealedSegments(spoolDir)       // seg-*, numeric sort, > lastProcessed
    if len(segs) == 0 { sleep(every); continue }

    batch := newBatch()
    for _, seg := range segs {                 // bounded: e.g. up to N segments or M bytes per batch
        forEachRecord(seg, func(metaId uint32, seqId uint64, payload []byte) {
            if seen(metaId, seqId) { return }   // dedup (see §4)
            ev := unmarshal(payload)
            checkGap(metaId, seqId)             // Watch-drop detection (see §7)
            if v2 := ev.GetV2(); v2 != nil {
                dirs, entryID := dirsFor(v2, mount, trackOpens)
                batch.add(dirs, entryID)
            }
        })
        batch.segments = append(batch.segments, seg)
    }

    // flush == today's collector.flush(): siblingDirs + writeSuspects + (optionally) run gufi
    if flushBatch(batch) {                       // returns true only if the rescan is done/handed off
        for _, seg := range batch.segments { markProcessed(seg) }   // advance checkpoint, then delete/move seg
        writeState(statePath, batch.segments.max())
    }
}
```

`forEachRecord`: read `uvarint` len, then `metaId`,`seqId`, then `payloadLen` bytes; use a buffered
reader; reuse a scratch `[]byte` for payloads to keep allocations flat.

---

## 4. Segment consumption, progress, and idempotency

- **Order:** iterate segments by ascending number; records within a segment in file order. This is
  the subscriber arrival order (`../DESIGN.md` §8). For suspect-file correctness order does **not**
  matter (Part 3 / FLOW.md: "a set has no order"), but processing in order keeps the checkpoint
  simple and makes gap detection meaningful.
- **Only sealed segments.** Never open a `*.partial` (still being written by the collector).
- **Progress checkpoint** (`-state` file): store the highest segment number that has been fully
  processed *and* whose rescan completed. On restart, resume after it. This is markwalk's
  durability boundary — independent of the collector's ack-to-Watch boundary.
- **Reprocess-safe.** If markwalk crashes after writing a suspect file but before recording progress,
  it reprocesses those segments. That is safe because:
  - the suspect file is a **set of directories** — reprocessing marks the same dirs again (idempotent
    by inode in `writeSuspects`, `main.go:282`);
  - `gufi_incremental_update` "accepts the updated filesystem as is, however it got there" (issue #5).
- **Duplicate events** (collector at-least-once + Watch replay) are possible. Dedup by
  `(metaId, seqId)` in the reader — a small per-batch set is enough since duplicates arrive close
  together; a bounded LRU/lastSeq-per-meta covers cross-batch replay. Even without dedup the output
  is still correct (same dir marked twice = one mark); dedup is purely to save walk work.
- **Segment retention:** after a batch's rescan completes, delete or move its segments to a `done/`
  dir. Deleting is simplest and keeps the spool bounded; the collector and markwalk must agree the
  spool dir is markwalk-owned for cleanup.

---

## 5. Event-type handling (the core of the change request)

The mapping already exists in `dirsFor`/`linkedEntryID`; this section makes it complete and explicit
against the full `beewatch.V2Event_Type` enum (protobuf `v0.8.5-…`, values 0–19) so the reviewer can
confirm every type is handled. **Guiding rule:** GUFI rescans *directories*, so every event resolves
to zero or more directories to re-read; the suspect file only carries `d` lines (§6).

| # | Type | Dirties (directories to mark) | Fields used | Notes |
|---|------|-------------------------------|-------------|-------|
| 0 | `INVALID` | **nothing** | — | **New guard needed** — currently falls through `default` and wrongly marks a parent. Skip it. |
| 1 | `FLUSH` | parent (+ sibling dirs if `num_links>1`) | Path, NumLinks, EntryId | inode changed; size/mtime recorded in parent. |
| 2 | `TRUNCATE` | parent (+ siblings) | Path, NumLinks, EntryId | as FLUSH. |
| 3 | `SETATTR` | parent (+ siblings) | Path, NumLinks, EntryId | as FLUSH. |
| 4 | `CLOSE_WRITE` | parent (+ siblings) | Path, NumLinks, EntryId | as FLUSH. |
| 5 | `CREATE` | parent | Path | listing changed only. Multi-link CREATE does **not** qualify for siblings. |
| 6 | `MKDIR` | **parent AND the new dir** | Path | marking only the parent leaves the new dir with no db.db. |
| 7 | `MKNOD` | parent | Path | listing changed. |
| 8 | `SYMLINK` | parent | Path | listing changed. |
| 9 | `RMDIR` | parent | Path | listing changed (dir gone; walk handles "now missing"). |
| 10 | `UNLINK` | parent | Path | listing changed. Multi-link UNLINK does **not** qualify for siblings. |
| 11 | `HARDLINK` | **both parents** (src + target-parent) | Path, TargetPath, NumLinks, EntryId | nlink is per-name; the other name's dir is stale. Sibling lookup via EntryId. Consider `TargetParentId` if TargetPath is unavailable. |
| 12 | `RENAME` | **common ancestor + src parent + dst parent** | Path, TargetPath | fires once on src, dst in TargetPath. Marking both parents *without* the ancestor makes GUFI see delete+create and discard moved DBs. |
| 13 | `OPEN_READ` | parent, **only if `-track-opens`** | Path, NumLinks, EntryId | atime only; very high volume. Siblings apply (atime is on the inode). |
| 14 | `OPEN_WRITE` | parent, only if `-track-opens` | Path, NumLinks, EntryId | as OPEN_READ. |
| 15 | `OPEN_READ_WRITE` | parent, only if `-track-opens` | Path, NumLinks, EntryId | as OPEN_READ. |
| 16 | `LAST_WRITER_CLOSED` | parent (+ siblings) | Path, NumLinks, EntryId | inode changed. |
| 17 | `OPEN_BLOCKED` | **nothing** | — | open refused; nothing changed. |
| 18 | `STRIPE_PATTERN_CHANGED` | parent (+ siblings) | Path, NumLinks, EntryId | inode changed. |
| 19 | `INODE_LOCKED` | **nothing** | — | Path is a bare filename, not a path — must not be joined to the mount. |

Deltas from current code:
1. **Add an explicit `INVALID` (0) case → return nothing.** Today `default` (`main.go:87`) would
   mark `filepath.Dir(mount + "")` — wrong.
2. Everything else already matches `dirsFor` (`main.go:53`) and `linkedEntryID` (`main.go:108`);
   re-verify the switch against this table during the change.
3. **Multi-link "inode changed" set** (siblings apply): FLUSH, TRUNCATE, SETATTR, CLOSE_WRITE,
   LAST_WRITER_CLOSED, STRIPE_PATTERN_CHANGED, HARDLINK, and (if enabled) the three OPENs. This is
   exactly the `linkedEntryID` allowlist (`main.go:109`) — keep it.

Path handling reminders (unchanged, but now fed from decoded events):
- Event paths are **mount-relative**; prepend `mount` (`dirsFor`'s `abs`, `main.go:49`). The
  `INODE_LOCKED` bare-filename case is the one exception and is skipped.
- `validEntryID` (`main.go:126`) must still gate every entry ID before it reaches `gufi_query`
  (it is interpolated into SQL text, not bound).

---

## 6. Suspect file and the GUFI command (from issue #5)

- markwalk uses **`--suspect-method 1` + `--suspect-file`**. Per issue #5, mode 1 today **only honors
  `d` (directory) lines** — `f`/`l` are parsed then ignored. So `writeSuspects` writing only
  `<inode> d` lines (`main.go:296`) is correct and should stay; do **not** add file/link lines.
- The `<inode>` is the directory's inode from `statDir` (`main.go:203`), deduped by inode.
- Positional args are now **3**: `<index> <source-tree> <parking-lot>` (issue #5: 4→3 in commit
  59e2337f). Keep the printed/executed command in that form (`main.go:371`).
- Alternative mode 3 (`--suspect-method 3 --suspect-time <t>`) uses **no** suspect file and relies on
  mtime/ctime; it misses pure file-content changes that don't touch the dir mtime (issue #5). markwalk
  stays on **mode 1** because Watch tells us precisely which dirs changed — that's the whole point.
- Keep the "do not advance progress until the rescan actually ran" rule (FLOW.md gap #1): only
  `markProcessed`/checkpoint after `gufi_incremental_update` returns success (with `-run`) or after
  the operator-driven run completes. Until then the segments stay unprocessed and are safe to replay.

---

## 7. Watch-drop detection and gaps

- The collector detects ring-buffer drops via `seqId` jumps (`../DESIGN.md` §10). markwalk can do the
  same from the record headers: track `lastSeq[metaId]`; a gap >1 means events were lost upstream.
- Effect on the index (FLOW.md gap #2): a dropped event for a subtree means that subtree stays stale
  until a later event touches it, at which point the walk backfills the gap. Log/metric the gap so
  it's visible; no special handling is required for correctness.
- `metaId` does **not** change path interpretation (paths are global, mount-relative). It is used
  only for dedup, gap detection, and checkpoint bookkeeping. If metadata mirroring can double-report
  an event (outer `Event.GetMetaMirror()`), dedup by `(metaId, seqId)` — or by `EntryId`/path — absorbs
  it; confirm whether Watch already suppresses mirror duplicates before relying on this.

---

## 8. Batching, ordering, and why order-independence still holds

FLOW.md's central design choice is unchanged and is what makes the segment-reader trivial:

- markwalk emits **which directories to re-read**, not a replay of operations.
- Therefore: order does not matter, repeats are free, a late mark still works, a missed mark leaves a
  dir stale (not wrong) until the next event repairs it.
- The three cases that look order-sensitive are handled **structurally**, not by ordering:
  - RENAME → mark both parents + common ancestor (so GUFI moves, not delete+create);
  - MKDIR → mark the new dir too (so it gets a db.db);
  - dir-replaced-by-file → `statDir`'s type check (`main.go:203`) refuses to write a non-dir inode.

Consequence: markwalk may batch **freely across segments and metas** (a whole batch of segments →
one suspect file → one rescan), exactly as the current per-interval `flush` batches across events.
Bound each batch by segment count, byte size, or the `-every` interval, whichever fires first.

---

## 9. Flags / CLI

| flag | change | purpose |
|---|---|---|
| `-listen` | **remove** | no longer a gRPC server |
| `-spool` | **add** | collector's segment directory to read |
| `-state` | **add** | checkpoint file: last fully-processed segment number |
| `-run` | **add (optional)** | actually execute `gufi_incremental_update` (default: print the command, as today) |
| `-mount`,`-index`,`-out`,`-query`,`-every`,`-track-opens`,`-demo` | keep | unchanged semantics |

`-demo` (`demo.go`) stays and needs no change — it never touched the network; it exercises
`writeSuspects` directly.

---

## 10. Performance / resource notes

- markwalk's per-event cost is `proto.Unmarshal` + a type switch + map inserts — CPU-light and fully
  decoupled from Watch (segments are durable, so lag is harmless). It does **not** need to match peak
  event rate instantaneously; it just needs to drain the spool over time.
- Use a buffered reader and a reused payload scratch buffer; `proto.Unmarshal` into a reused
  `*bw.Event` (reset per record) to keep allocations flat.
- The expensive operations remain the **walk** (stat per level) and the **sibling query** — both are
  already amortized per batch (`siblingDirs` runs once per flush, `main.go:361`). Keep them per-batch.
- The dedup/`lastSeq` structures are O(active metas) plus a bounded per-batch set — negligible.

---

## 11. Migration steps

1. Add the segment reader (`forEachRecord`, numeric segment listing, `-spool`/`-state`), feeding the
   existing `collector.add`. Rename the `collector` type (it is now a *reader/batcher*, not a
   subscriber).
2. Delete `ReceiveEvents`, `net.Listen`, `grpc.NewServer`, and the `-listen` flag; drop the
   `google.golang.org/grpc` and gRPC-related imports.
3. Add the `INVALID` (0) guard to `dirsFor`; re-verify the switch against §5.
4. Wire progress: `markProcessed` + `-state` checkpoint; only advance after the rescan (with `-run`)
   succeeds. Add segment cleanup (delete or `done/`).
5. Add `seqId`-gap detection and `(metaId,seqId)` dedup.
6. Keep `-demo` working; update `README.md` and `FLOW.md` (the picture becomes
   `collector → seg files → markwalk`, and gap #1 is now "advance the segment checkpoint only after
   the rescan").
7. Tests: a golden segment file (a handful of records covering every enum value in §5) decoded into
   an expected suspect file; plus the existing demo trees for the walk.

---

## 12. Open questions for the team

- **Who runs GUFI?** markwalk with `-run`, or an external scheduler consuming the suspect file? This
  decides where the "advance checkpoint only after rescan" gate lives.
- **Mirror duplicates:** does Watch de-duplicate events from mirrored metadata buddies, or must
  markwalk dedup by `EntryId`/path across `metaId`s?
- **Suspect-time:** mode 1 ignores `--suspect-time` unless `--suspect-stat` is set; confirm whether we
  want stat-based confirmation of suspects or trust Watch entirely (issue #5 shows mode-1 behavior
  changed across GUFI commits — pin a GUFI version).
- **Spool ownership:** confirm markwalk owns segment deletion so the collector never removes a segment
  markwalk hasn't processed.
```
