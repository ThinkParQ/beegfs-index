# Merging collector + markwalk into one service — design
## 0. TL;DR

Merge into **one binary with two independent goroutine groups**, but **keep the disk spool as the
data handoff between them — do not pass event data over an in-memory channel.** A channel is used
only for a zero-payload "a segment was sealed" wakeup. Add a `--mode both|collect|process` flag so
the single binary can still run the two roles on separate hosts.

The merge is about process/lifecycle unification and operational simplicity, **not** about changing
the data path. The file handoff is load-bearing and an in-memory handoff would cost gigabytes of
GC-heavy RAM at this event rate (or reintroduce backpressure that makes Watch drop events).

Non-negotiable: **a markwalk failure must never stop the collector.**

---

## 1. Current state (main)

Two separate `package main` programs under `beegfs_watch/`, communicating through the on-disk spool:

```
Watch (gRPC) ──► collector ──► seg-<N>.binpb (sealed, durable) ──► markwalk ──► suspect files
                  (continuous)      spool dir                      (periodic)     + markwalk.json state
```

- **collector** (`collector/main.go`): continuous gRPC subscriber. Receives `*bw.Event` from the
  SDK, writes length-prefixed protobuf to `seg-<N>.binpb.partial`, seals (fsync → link → dir fsync),
  and **only then acks Watch**. Numbering continues from the highest sealed file after restart.
  Constraints: receive path never blocks on disk; `(seal-queue + 2) × roll-events` unacked events
  must stay well under Watch's `event-buffer-size` (Watch drops the oldest unacked on overflow);
  disk error → fatal → replay.
- **markwalk** (`markwalk/main.go`, `markwalk/reader.go`): periodic batch job. Reads sealed segments
  in number order, turns a batch into one `<inode> d` suspect line per directory, writes it
  durably, then advances `markwalk.json`. Crash repeats a batch, never skips one. Dedups by inode;
  resolves multi-name files via `gufi_query`.
- **spool** (`spool/spool.go`): shared reader + segment naming/ordering. The format is the contract.

Cadence/failure models differ sharply:

| | collector | markwalk |
|---|---|---|
| cadence | real-time, 100k–1M events/s | periodic (default `--markwalk.every 1m`, up to `--markwalk.max-segments 100` = up to ~10M events/batch) |
| work | receive → encode → fsync → **ack Watch** | stat-walks + `gufi_query` subprocesses + build suspect file |
| crash recovery | replay from Watch | repeat batch from durable spool + state file |
| failure cost | Watch ring buffer **drops events** if acks stall | just redo the batch |

---

## 2. Decision: keep the disk spool as the handoff (do not use an in-memory channel)

Three properties the collector cannot give up all depend on the file handoff:

1. **Durability gates the ack.** The collector may ack Watch only after a segment is fsync+sealed
   (`seal()` acks post-link + dir-fsync). That disk write is mandatory. A channel handoff would
   either (a) still require the disk write for durability — making the channel redundant — or
   (b) ack Watch on a non-durable in-memory handoff, breaking at-least-once: a crash loses everything
   acked-but-not-yet-committed to a suspect file. **The spool is the durability boundary.**

2. **Backpressure isolation.** markwalk is slow and bursty (minutes/batch, subprocesses). A *bounded*
   channel between the two would sit full whenever markwalk is mid-batch → backpressure into the
   collector's receive loop (`s.events <- event`) → gRPC `Recv` stalls → **Watch drops events**. The
   disk is an effectively unbounded buffer that absorbs markwalk being slow or down with zero effect
   on the collector's ack rate. This decoupling is the whole point of the two-stage split.

3. **Independent recovery + split-host deployment.** markwalk restarts and re-reads the durable spool
   via its state file, never touching Watch. Because the handoff is files, the collector can run on
   the Watch/metadata host while markwalk runs on the GUFI-index host (shared/synced spool) — the
   "remote index" case (issue #5). An in-memory channel forbids that split.

### 2.1 Memory overhead — file vs channel

`*bw.Event` is a fully-unmarshaled, pointer-heavy protobuf (nested `V2Event` + strings: path,
target_path, entry_id, parent_entry_id) ≈ **~300 B live each, several pointers each**.

- **Channel / in-memory handoff** must hold a batch's worth, because markwalk batches over minutes:
  - 1 min @ 100k/s ≈ 6M events ≈ **~1.8 GB**, pointer-heavy → GC scans all of it every cycle.
  - A full `max-segments=100 × roll-events=100k` batch ≈ 10M events ≈ **~3 GB**.
  - A *bounded* channel avoids the RAM blowup but reintroduces backpressure (point 2) → Watch drops.
    Either way the channel loses.
- **Disk spool (current)** stores the same events as flat, length-prefixed protobuf (~150 B each,
  **pointer-free**) on **disk**, read back **streaming with a single reused `bw.Event`**
  (`readSegment` reuses one `ev`). Steady-state handoff RAM is **O(1)** — a few hundred bytes — not
  O(events).

At this event rate the file handoff costs ~0 RAM; the channel costs ~GBs (or forces backpressure).
Decisive.

---

## 3. What "one service" means here

A single binary, two **independent** goroutine groups, sharing config/logging/metrics/lifecycle and
the spool directory — not an in-memory pipeline.

Operational wins:
- one binary / systemd unit / config / log stream / metrics endpoint to deploy and monitor;
- a natural owner for segment retention;
- an in-process wakeup so markwalk reacts to new segments without poll latency.

The single real risk is **loss of the OS process boundary** that today gives free fault isolation.
In one process a markwalk panic (stat-walk, a `gufi_query` subprocess) must **never** stop the
collector — that would make Watch drop events. This is engineered, not free (see §5).

---

## 4. Target architecture

```
                         one process (beegfs-index-sync)
   Watch (gRPC) ──► [collector goroutines] ──► seg-<N>.binpb ──► [markwalk goroutine] ──► suspect files
                         receive/seal/ack        spool dir            read/mark/retire
                                │                                          ▲
                                └──── sealed chan struct{} (cap 1, nudge) ─┘   (data stays on disk)
```

- Data path between stages is **always the spool files**.
- `sealed chan struct{}` carries only "something new is sealed", never event data.
- `--mode`:
  - `both` (default): collector + markwalk in one process, shared spool, nudge enabled.
  - `collect`: collector only (e.g., Watch/metadata host).
  - `process`: markwalk only (e.g., index host, reading a shared/synced spool); nudge absent, polls.

The binary is named `beegfs-index-sync`, matching the existing `/var/lib/index-sync/` defaults.
`beegfs-watch` is taken: it is the BeeGFS Watch daemon in beegfs-go (`watch/cmd/beegfs-watch`).

Split mode is kept because it is cheap, not because the remote-index case needs it. markwalk needs
the BeeGFS mount and the index on the same host, and Watch dials into the collector, so running
`both` on the index host is the simpler deployment. When the spool is shared instead, the seal
(`link` → `unlink` → directory `fsync`) relies on the shared filesystem: on NFS, directory attribute
caching can delay when the reader sees a sealed name, and each `fsync` is a round trip. A linked
file is complete when it appears, so correctness holds, but latency and ENOSPC behaviour change.
Test the split on the real shared filesystem before relying on it.

---

## 5. Fault isolation rules (the main risk of merging)

- markwalk's goroutine runs under **panic recovery**; a panic or batch error is logged (a panic
  with its stack), counted in a metric, and the batch is **retried with exponential backoff**, never
  fatal to the process and never silently stopped. A stopped markwalk starts the ENOSPC chain in §9,
  and the same input panics the same way, so recovery must back off rather than loop. `recover`
  covers only its own goroutine: every worker goroutine markwalk starts (§9.3) needs its own, or an
  errgroup that turns panics into errors.
- The collector keeps **fatal-on-disk-error** (that *should* stop everything; unsealed events replay).
- markwalk shares no mutable state with the collector; its only input is sealed files on disk.
- A collector fatal propagates to process exit; a markwalk error does not.
- Note shared-disk contention (collector sequential writes + markwalk sequential reads); markwalk is
  mostly IO-bound (stat/subprocess) so it blocks rather than CPU-spins, but watch it under load.

---

## 6. Lifecycle & shutdown

- Start both goroutine groups under a shared `context`.
- On SIGTERM: run the collector's existing graceful stop (stop Watch server → drain → seal →
  checkpoint, including storing `high` watermarks before closing acks) **and** cancel markwalk's
  context. markwalk mid-batch is safe to abandon (state advances only after a batch's suspect file is
  durable → the batch just repeats). markwalk shutdown must **not** block the collector's durable
  shutdown; don't wait indefinitely on an in-flight `gufi_query` subprocess — bound it or let the
  batch be redone.

---

## 7. Low-latency wakeup (the only channel; zero payload)

- Add `sealed chan struct{}` (cap 1). After a successful `seal()`, the collector does a non-blocking
  send: `select { case sealed <- struct{}{}: default: }` (coalesces; never blocks the sealer).
- The nudge means only "work is pending". It must not set the batch cadence: the collector seals
  every `--collector.roll-every` (30s) or `--collector.roll-events`, so a markwalk that batches on every nudge would make
  one batch per segment and multiply the `batch-<N>` directories `beegfs index update` has to apply.
  So markwalk, when caught up, blocks on `select { <-ctx.Done() ; <-sealed }`; once nudged it waits
  until `--markwalk.every` has passed since its last batch, then reads **event data from disk**. Idle costs
  nothing, steady load gives one batch per `--markwalk.every`, and a backlog drains per §9.1.
- In `collect`/`process` split mode the channel is absent and markwalk polls (§10.2).

---

## 8. Segment retention (gap to close during the merge)

Today markwalk never deletes segments (`process()` only advances `st.Segment`), so the spool **grows
unbounded**. The merged service should own retention: delete sealed segments with
`number ≤ markwalk.committedSegment`, with a small safety margin, **never deleting the current
highest**.

**Hazard — numbering reset:** the collector derives its counter in `prepare()` as
`max(existing sealed numbers)`. If retention deletes segments and the collector restarts with
few/none left, the counter could **reset below markwalk's watermark**, and markwalk's
`Sealed(dir, after=high)` would then **skip** new low-numbered segments.
**Required fix before enabling retention:** persist the collector's segment counter and resume from
`max(persisted, max-on-disk)`, so numbering is monotonic independent of deletion. This also covers an
operator wiping the spool by hand. The SDK owns `checkpoint.json`, so the counter gets its own file
next to `markwalk.json`; the service then has three state files, each with one owner.

Rules:
- Retention runs on the markwalk side only. It knows the committed segment and can read the
  directory for the highest one; `collect` mode knows neither and never deletes.
- Never delete `.partial` files, and always keep at least one sealed segment.
- Retention keys on markwalk's commit, not on the index. Once a batch's suspect file is durable, its
  segments are no longer needed.

The service does **not** run `gufi_incremental_update`. Applying suspect files is the job of
`beegfs index update` in the CTL; a second consumer here would mean two checkpoints and a race on the
parking lot.

---

## 9. Keeping the two in sync (backlog control)

They are **not** meant to run in lockstep — the two-stage design deliberately lets markwalk lag, and
batch over minutes. "In sync" here means **bounded backlog, bounded disk, bounded staleness**, not
equal frequency. Two facts frame this:

- markwalk lagging **never loses events** (durable in sealed segments) and **never skips** (reads in
  segment-number order via its state file). It only increases index staleness.
- The **only hard coupling is spool disk capacity.** The failure chain to prevent is:

```
markwalk can't keep up → segments pile up → spool disk fills → collector hits ENOSPC
                       → collector fatal (stops acking) → Watch ring buffer overflows → event loss
```

So the goal is to keep the backlog bounded so the disk never fills.

### 9.1 Greedy drain — make `--markwalk.every` a max-idle time, not a throttle (primary fix)

Today markwalk waits `--markwalk.every` (default 1 min) **between every batch unconditionally**
(`reader.go` `run()`: process, then `time.After(every)`), and caps a batch at `--markwalk.max-segments` (100).
If the collector produces more than `max-segments` per `every`, markwalk falls behind **forever** —
exactly the "collector generates lots of files, markwalk runs infrequently" case.

Change the loop so it self-throttles:
- If the last pass hit the `max-segments` cap (more segments were waiting), **loop again
  immediately**, no `--markwalk.every` wait, until the backlog is drained.
- Only wait `--markwalk.every` (or the seal-nudge, §7) when **caught up** (fewer than a batch waiting).

```
behind    → process next batch immediately, repeat until drained
caught up → block on the seal-nudge (split mode: poll)
nudged    → wait until --markwalk.every has passed since the last batch, then process
```

`--markwalk.every` becomes the minimum spacing between batches under steady load, not a throttle on a
backlog. This is the single most important change for backlog control. Pick its default with the
consumer in mind: `beegfs index update` runs one `gufi_incremental_update` per batch directory, so a
1-minute `--markwalk.every` means up to 1,440 runs a day.

### 9.2 Bigger catch-up batches are cheaper, not more expensive

Suspect files are a **set of directories** (dedup by inode), so a large batch of segments collapses
to a small set of dirty directories: 10 vs 1000 segments over the same tree yield nearly the same
suspect file and one `gufi_incremental_update`. When behind, process **large** batches — per-batch
overhead (sibling query, suspect write, rescan) is amortized over far more events, and dedup
coalescing does the catching up. Raise `--markwalk.max-segments` (or auto-size it up when the backlog is
large); do not keep it small. Larger batches do cost memory in markwalk: the per-batch directory set
and the `links` set of multi-name entry IDs (`reader.go`) grow with the batch, and sibling lookups
run one `gufi_query` per ~3,500 IDs. Auto-sizing therefore needs a cap, by events or bytes, not just
"larger when behind".

### 9.3 Scale markwalk throughput to match production

markwalk is mostly IO-bound (stat-walks + `gufi_query` subprocesses), so it can be sped up:
parallelize the stat walks and sibling queries within a batch, and keep batches large (§9.2). The
stat cache is `simplelru.LRU` (`markwalk/main.go`), the lock-free variant; before `markChain` runs in
parallel, switch to the locking `lru.Cache` from the same module or give each worker its own cache.
Each worker also needs its own panic recovery (§5). If,
after greedy drain + large batches + parallelism, markwalk still cannot match the sustained event
rate, that is a **provisioning signal** (markwalk/index host too slow for the change rate) — not
something to fix by throttling the collector.

### 9.4 Retention bounds disk only if the backlog is bounded

Retention (§8) caps spool disk at roughly `backlog × segment size`. It is **necessary but not
sufficient**: if markwalk is permanently slower than production, the backlog grows regardless and the
disk still fills. §9.1–§9.3 keep the backlog bounded; retention then bounds the disk.

### 9.5 Never back-pressure the collector onto markwalk

Making the collector slow down when the backlog is large would propagate into the receive path →
gRPC `Recv` stalls → **Watch drops events**, defeating the decoupling. The collector must keep
sealing and acking regardless of markwalk. Backlog is managed by making markwalk faster
(§9.1–§9.3) and by capacity/observability (§9.6) — never by stalling the collector. If the disk
truly fills, the collector failing loudly (ENOSPC → fatal) is the correct, visible outcome, not
silent stalling.

### 9.6 Observe the lag and alert before the disk fills

In the merged process both watermarks are in memory, so these are exact and cheap:

- `spool_segments_pending` = `collector.last − markwalk.committed` (backlog depth).
- `spool_bytes_pending` and **spool filesystem free space** — the early warning for the ENOSPC chain.
- markwalk batch duration, segments/batch, events/batch; collector segments/sec.
- Trend of `pending`: flat/oscillating is healthy; monotonically rising means markwalk cannot keep
  up — act (scale per §9.2–§9.3) before the disk fills.

Alert when pending segments or spool usage crosses a threshold, and when free space drops below a few
batches' worth. The seal-nudge (§7) improves freshness when idle but does **not** help throughput
when behind — that is the stat/`gufi_query` work (§9.3).

Summary:

| Mechanism | Purpose |
|---|---|
| Greedy drain (§9.1) | **primary** — markwalk runs continuously until caught up |
| Large / auto-sized batches (§9.2) | catch up cheaply via dedup coalescing |
| Parallel stat / `gufi_query` (§9.3) | raise markwalk throughput to match the collector |
| Retention + persisted counter (§8) | bound spool disk, given a bounded backlog |
| Lag + disk-free metrics/alerts (§9.6) | detect "can't keep up" before ENOSPC |
| Never block the collector (§9.5) | preserve decoupling; avoid Watch drops |

---

## 10. Idle behavior (no busy-spin when the filesystem is quiet)

The mirror of §9: when few or no events are generated, the service must sit **blocked**, not poll or
spin. Event-driven waits, not short fixed-interval loops.

### 10.1 Collector when idle — already fine

The receive loop (`collector/main.go` `run()`) is a blocking `select` on `c.events`, the roll
ticker, and `ctx.Done()`. With no events it blocks on the network/channel; the only wakeups are:
- the `--collector.roll-every` ticker (30s): fires `rotate()`, which is a **no-op when no file is open**
  (`c.cur == nil`) — so idle costs one cheap timer wakeup per 30s;
- the SDK ack flusher (`--collector.ack-frequency`, 1s): `Flush()` returns early when nothing changed
  (`needsFlush == false`), so idle costs a cheap 1s timer wakeup with no disk write.

These are bounded timer wakeups, not a spin — no change required. (Optional micro-tidy: only arm the
roll ticker while a file is open; negligible, not worth it.)

### 10.2 markwalk when idle — switch from polling to the seal-nudge

Today `markwalk` `run()` re-scans the spool directory every `--markwalk.poll` (5s) forever, even when the
filesystem is idle for hours: `spool.Sealed()` (an `os.ReadDir` + sort) runs each iteration, then
`time.After(poll)`. That is a constant periodic wakeup + directory scan with no work to do, and the
scan is O(files) — worse if the spool is large.

In `both` mode, make markwalk **block on the seal-nudge** instead of polling:
- When caught up, wait on `select { <-ctx.Done() ; <-sealed }`. After a nudge, wait out the rest of
  `--markwalk.every` since the last batch (§7), then scan. No timer runs while idle.
- Idle ⇒ the collector seals nothing ⇒ no nudge ⇒ markwalk stays blocked. **Zero directory scans
  while idle.**
- The nudge is cap-1 and coalesced; a seal that happens in the gap between "Sealed() returned empty"
  and "block on sealed" is buffered, so the wakeup is never missed, and markwalk always re-scans on
  wake (so one nudge covers any number of new segments).
- In split-host `process` mode there is no nudge, so markwalk polls — but with a **longer** default
  `--markwalk.poll` and reliance on retention (§8) to keep the scanned directory small. Consider exponential
  backoff up to a cap when consecutive scans find nothing.

### 10.3 Greedy drain must not become a busy-loop

§9.1's "loop immediately when behind" applies **only when the last pass made progress and hit the
`max-segments` cap**. When caught up (the idle case) markwalk blocks per §10.2. Guard the greedy loop
so it re-loops without waiting *only* if `st.Segment` advanced; otherwise fall through to the
blocking wait. This prevents a hot loop if `process()` ever returns without advancing (e.g., a batch
that marks nothing still advances `st.Segment`, so it terminates; a repeating error backs off per §5
rather than spinning or stopping).

### 10.4 `time.After` is fine here

The module is on Go 1.26; since Go 1.23 an unreferenced `time.After` timer is garbage-collected, and
this loop runs once per batch. No timer reuse is needed.

Net: idle ⇒ both halves are **blocked on an event or a long timer**, with no periodic directory
scans (in `both` mode) and no busy-loop. Busy only when there is real work.

---

## 11. Code-change plan

1. **Package refactor (no behavior change).** Both logics are `package main` today; make them
   importable:
   - `beegfs_watch/internal/collector/` — move the `collector` type + `run`/`sealer`/`seal`/`add`/
     `writeRecord`/`prepare`/`seqTracker`; expose `Run(ctx, cfg, log) error`. Keep ack-after-seal,
     roll bounds, seq tracking, and the SDK shutdown/checkpoint sequence verbatim.
   - `beegfs_watch/internal/markwalk/` — move the `reader` type + `run`/`process`/`dirsFor`/
     `markChain`/`writeSuspects`/`siblingDirs`/`statCache`/`readState`; expose `Run(ctx, cfg, log)
     error`.
   - `beegfs_watch/spool/` — unchanged (the on-disk contract).
2. **Unified entrypoint** `beegfs_watch/cmd/beegfs-index-sync/main.go`: one flag set grouped by role,
   `--mode both|collect|process`, shared logger/metrics, one signal handler, shutdown coordination
   (§6), and the `sealed` nudge wired in `both` mode (§7).
3. **markwalk wait loop (§7, §9.1, §10.2–§10.3):** rework `markwalk` `run()` so that, when
   **behind**, it loops immediately (no `--markwalk.every` wait) while the last pass advanced state and hit the
   `max-segments` cap; when **caught up / idle**, it blocks on the seal-nudge rather than polling the
   directory, and after a nudge waits out `--markwalk.every` since the last batch. Errors and panics back off
   per §5. In `process` split mode (no nudge), poll with a longer `--markwalk.poll` and back off. Optionally
   auto-size `max-segments` up under a large backlog, with a cap (§9.2).
4. **Retention + counter persistence** (§8), gated on a flag initially.
5. **Metrics (follow-up PR):** neither program has an endpoint today, so this is new behaviour,
   not part of the merge. One endpoint exposing both collector (events/s, seal latency, unacked
   depth, seq gaps) and markwalk (batch size, marks, stats, sibling-query time,
   `spool_segments_pending`, `spool_bytes_pending`, spool free space, retention, consecutive
   failures) — see §9.6.
6. Keep the standalone `collector`/`markwalk` `main` packages only as long as needed for transition,
   then remove in favor of `--mode`.
7. **Docs:** `markwalk/DESIGN.md` and `markwalk/FLOW.md` still describe markwalk running
   `gufi_incremental_update` and deleting segments, and list flags that do not exist. Update them
   with the merge.

---

## 12. Constraints to preserve verbatim (no regressions)

- Collector: receive path never blocks on disk; `(seal-queue + 2) × roll-events` unacked bound kept
  well under Watch's `event-buffer-size`; ack only after seal; disk error → fatal → replay; store
  `high` watermarks before closing acks on shutdown.
- markwalk: write suspect file durably **then** advance state (crash repeats, never skips); dedup by
  inode; stop on a damaged segment rather than silently skip; `(MetaId, SeqId)` gap/dedup logic.
- spool format and `spool` package unchanged → on-disk compatibility with any running split
  deployment.

---

## 13. Testing & rollout

1. Refactor to packages with **no behavior change** (both still runnable via `--mode`).
2. Unit tests: wakeup nudge (non-blocking, coalesced; under steady seals the batch count stays at
   one per `--markwalk.every`); retention + counter-persistence across a simulated restart (the skip hazard);
   markwalk error and panic back off and never stop the process; shutdown ordering (markwalk abandon
   mid-batch is safe).
3. Load test `both` at target event rate with markwalk deliberately slowed (large stat trees):
   confirm the collector's ack rate and Watch-drop count are **unaffected** by markwalk lag (proves
   the disk decoupling survived the merge).
4. Backlog/catch-up test (§9): drive a burst so segments outpace markwalk, then confirm greedy drain
   catches up without the `--markwalk.every` throttle, retention bounds spool disk, and `spool_segments_pending`
   returns to baseline. Verify the collector is never stalled by the backlog.
5. Idle test (§10): with no events for an extended period, confirm near-zero CPU — no periodic
   directory scans in `both` mode (markwalk blocked on the nudge/long timer), only the bounded
   collector/SDK timer wakeups. Then seal one segment and confirm the nudge wakes markwalk promptly.
6. Fault injection: panic in markwalk → collector keeps sealing/acking; disk-full → collector fatal
   as today.
7. Rollout: ship one binary; run `--mode both` co-located, or `collect`/`process` split — same spool
   contract.

---

## 14. Bottom line

- **Merge: yes** — one binary, two isolated goroutine groups, unified flags/logging/metrics/
  lifecycle, plus a wakeup nudge and segment retention.
- **Handoff: stay on the disk spool.** An in-memory/channel handoff would cost ~GBs of GC-heavy RAM
  (or reintroduce backpressure that makes Watch drop events) and would forbid split-host deployment.
  The channel earns its place only as a zero-payload "segment sealed" signal.
- **Non-negotiable:** markwalk failures must never stop the collector.
