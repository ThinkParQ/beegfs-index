# Watch event collector — design

Status: proposed
Audience: developer implementing the collector
Scope: the subscriber-side program that receives BeeGFS Watch events and persists them to disk.
It does **not** cover the downstream utility that consumes the persisted files.

---

## 1. Problem

BeeGFS is a parallel filesystem. Its metadata servers can emit **hundreds of thousands to
millions of metadata events per second** in aggregate. The Watch service proxies those events and
streams them to a subscriber. We need a subscriber that:

1. Receives that firehose without falling behind (falling behind = data loss; see §3).
2. Persists every event durably, exactly once acknowledged.
3. Preserves the order in which events are received at the subscriber.
4. Is cheap on CPU and memory at peak rate.

The final on-disk format does **not** need to be human-readable. A separate downstream utility
reads the files and knows how to decode them. This removes any need for parsing/transformation in
the collector and is the single most important simplifying constraint (see §4, D1).

The existing example (`collector/main.go`) is a correctness-first sketch: it JSON-encodes one event
at a time straight to an unbuffered file and only acks every 30 s. It is not built for this rate.
This document describes the design that is.

---

## 2. How Watch delivers events (protocol facts the design relies on)

These were established by reading the Watch sender and the subscriber SDK in
`github.com/thinkparq/beegfs-go` (`watch/…`). They are load-bearing; re-verify if the SDK changes.

- **Watch is the gRPC client; the subscriber is the gRPC server.** Watch dials *out* and opens one
  bidirectional stream **per metadata server**. Multiple streams run concurrently.
  (`watch/pkg/subscriber/service.go`, `ReceiveEvents`.)
- **One event per gRPC message.** Watch does `stream.Send(event)` per event
  (`watch/internal/subscriber/grpc.go:116`, `watch/internal/subscribermgr/handler.go:501`).
  **There is no wire batching to enable from our side.** Any batching happens after `Recv`.
- **The subscriber SDK delivers events on a single Go channel of `*bw.Event`**, already unmarshaled
  from the wire, tagged with `MetaId` and `SeqId`. All metas share that one channel; origin is
  distinguished by `MetaId`.
- **Acks gate Watch's buffer.** Watch holds events in a fixed-size ring buffer
  (`MultiCursorRingBuffer`) and, on overflow, **drops the oldest un-acknowledged events**. The
  subscriber acks a cumulative low-watermark per meta (`Response{CompletedSeq}`); Watch will not
  advance past, nor free, un-acked events. **A slow subscriber does not just lag — it loses data.**
- **The SDK ack contract is per-event.** Every event delivered on the channel must be acked exactly
  once (`Ack{MetaId, SeqId}` on the acks channel). The SDK tracks one `pending` entry per forwarded
  event and advances the per-meta watermark only across the contiguous acked prefix
  (`watch/pkg/subscriber/service.go`, `trackForwarded` / `setAckedSeqID`). Consequences: acks may be
  out of order, but an event that is never acked permanently halts that meta's watermark (unbounded
  redelivery), and the SDK's in-memory `pending` grows with the number of un-acked in-flight events.

### Ordering, precisely

- **Within a meta:** events arrive strictly in `SeqId` order (one SDK goroutine per stream forwards
  them sequentially). This is the authoritative order.
- **Across metas:** events interleave nondeterministically on the shared channel. This interleave
  approximates wall-clock order but is **not** a reliable global timeline — it is perturbed by
  per-stream backpressure, network/scheduler jitter, and (most sharply) **replay after a reconnect**,
  where old events arrive now.
- Each event also carries a nanosecond `Timestamp`, but cross-meta timestamps are subject to
  clock skew (NTP) between metadata nodes.

The product requirement is: **preserve the order in which events are received at the subscriber**,
in a single interleaved stream. That order is well-defined and cheap to preserve (§5). We do not
attempt a global temporal sort.

---

## 3. Non-negotiable constraints

1. **Never block the gRPC receive path on disk.** If the goroutine draining the SDK channel stalls
   on `write`/`fsync`, the channel fills, `Recv` stops, Watch back-pressures, and its ring buffer
   drops events. Disk latency must be absorbed by RAM, not pushed back to Watch.
2. **Ack only after the event is durable**, and ack **promptly**. Durable = the segment file
   containing the event has been `fsync`ed and atomically renamed into place. Ack latency must stay
   comfortably below `EventBufferSize / peakRate` (Watch's ring-buffer time window).
3. **Preserve subscriber arrival order** in the persisted stream.
4. **Bounded, low memory and low GC pressure** at peak rate.

---

## 4. Design decisions (with rationale)

### D1 — Binary passthrough; no parse, no transform, no second stage
The final format is the event's serialized bytes, framed with a small header. Because the
downstream utility decodes the events, the collector performs **no** protobuf→JSON conversion and
**no** field-by-field transformation.

*Why:* reflection-based JSON encoding was the dominant CPU cost and the reason an earlier design
proposed a separate parser stage. With a binary product there is nothing to parse, so the parser
stage disappears entirely. The file the collector writes **is** the final artifact. This also
removes disk IO amplification: we write once; downstream reads once (no rewrite).

### D2 — Buffer serialized *bytes*, not `*bw.Event` structs
Events are serialized on arrival and appended to a flat `[]byte` buffer; the `*bw.Event` is dropped
immediately.

*Why:* buffering ~1M live `*bw.Event` (each with a nested V2 message and several strings, ~300–500 B)
is ~400 MB of **pointer-heavy** memory that the GC must scan every cycle — a large, recurring CPU
tax. A flat byte buffer is a single **pointer-free** allocation the GC effectively ignores, and each
event becomes short-lived garbage. Memory scales as `avg_record_bytes × count` (≈150 B × count),
roughly a quarter of the struct approach, with negligible GC scanning.

### D3 — Single receiver + single writer (two goroutines), FIFO hand-off
One goroutine drains the SDK channel and fills the active buffer (the **receiver**). When the buffer
is full or a timer fires, it hands the buffer to a single **writer** goroutine over a small channel
and swaps in a fresh buffer. The writer does `write → fsync → rename → ack`.

*Why:*
- The receiver defines arrival order by the order it reads the channel; appending sequentially
  preserves it. (Satisfies constraint 3 with zero machinery.)
- Moving `write`/`fsync` to a separate goroutine keeps the receive path off the disk (constraint 1).
- **Exactly one writer** consuming the hand-off channel FIFO guarantees segments are written in fill
  order. Any parallelism on the write side would reorder the single stream, so it is disallowed.

### D4 — Rotate on size **or** time; size dominates at high rate
Seal the active buffer when it reaches a byte threshold (default 32–64 MB) **or** after a time bound
(default ~1 s), whichever comes first.

*Why:* the ack (and thus Watch's ability to free ring-buffer space) happens at seal. The time bound
caps ack latency when idle; the size bound caps it when busy. At high rate the size bound fires well
under a second, so acks stay frequent automatically and the system self-regulates: aggressive when
busy, lazy when idle. A large time bound (e.g. the initially-proposed 10 s) would risk Watch drops
at peak — see the timing constraint in §7.

### D5 — Length-prefixed binary framing with a `meta`/`seq` header
```
record: [uvarint payloadLen][uint32 metaId][uint64 seqId][payload bytes]
```
*Why:*
- Payloads are binary protobuf and may contain `\n`; newline-delimited framing is unusable.
- Putting `metaId`/`seqId` in a fixed header lets the collector ack and detect drops, and lets the
  downstream utility route/regroup by meta, **without unmarshaling the payload**.
- `payload` is the protobuf wire form of the event, so downstream decodes with a single
  `proto.Unmarshal`.

### D6 — Segments form one ordered stream; ack after seal
Sealed files are numbered monotonically (`seg-0000001`, `seg-0000002`, …). Concatenating them in
number order reproduces exact subscriber arrival order. The durability boundary is the sealed
segment; acks are sent immediately after the atomic rename.

*Why:* a single infinite file complicates crash-safety and hand-off to the consumer. Immutable,
numbered segments are each complete and safe to consume the instant they appear, and their number
sequence *is* the single ordered stream the requirement asks for.

### D7 — Serialize as cheaply as the SDK allows
Preferred: an SDK change that hands the receiver the **raw pre-marshaled bytes** plus the already-
extracted `metaId`/`seqId`, making the receiver a `copy`. Fallback with today's SDK: `proto.Marshal`
into a reused buffer (generated code, no reflection — cheap) and read `GetMetaId()/GetSeqId()`.

*Why:* the SDK unmarshals the wire bytes to `*bw.Event` and discards the original bytes; re-marshaling
them is redundant work on the hot path. Exposing the raw bytes removes it. The `proto.Marshal`
fallback is acceptable (marshal ≠ parse, and it is codegen, not reflection). Parallelizing the
marshal is rejected because it would reorder the stream and require a reorder buffer.

### D8 — Cumulative acking (SDK enhancement) to collapse ack traffic
Today the SDK requires one ack per event. Recommend adding `AckUpTo(metaId, seqId)` that marks all
pending ≤ `seqId` done.

*Why:* at 1M events/s, per-event acking means millions of channel sends and a per-event `pending`
slice in the SDK. Because within a segment a meta's `SeqId`s are increasing and segments are sealed
in order, acking the **per-meta max `SeqId` in the segment** is a correct cumulative watermark. This
turns the ack path from O(events) into O(metas × segments) and lets the collector keep only a tiny
`map[metaId]maxSeq` per buffer instead of a full per-event list. Until this lands, the writer replays
the segment's per-event `(meta, seq)` list (bounded to one segment's worth).

---

## 5. Architecture

```
Watch (N streams, 1 event/msg)
        │
        ▼
  SDK ReceiveEvents ──► events chan (*bw.Event, buffered 64k–256k)
        │
        ▼
  RECEIVER goroutine                          WRITER goroutine (single)
  ┌───────────────────────────┐   handoff    ┌──────────────────────────────┐
  │ serialize event → bytes    │   chan       │ write whole buffer (1–few     │
  │ append [len][meta][seq][p] │ (cap 2–4) ──►│   write() syscalls)          │
  │ update ack info            │              │ fsync                         │
  │ track lastSeq per meta     │              │ rename .partial → seg-N       │
  │ rotate on size OR time     │              │ ack Watch (per-event or       │
  │ swap buffer from sync.Pool │◄─────────────│   AckUpTo per meta)           │
  └───────────────────────────┘  return buf  └──────────────────────────────┘
        │                                              │
        ▼                                              ▼
   detect seqId gaps (Watch drops)             segment files = final product
```

Two goroutines. Disk (the numbered segment sequence) is the single ordered, durable output. There is
no third "parser" stage (D1).

---

## 6. On-disk layout and format

```
<spool>/
  seg-0000001.partial   # being written, deleted on startup if left behind
  seg-0000001           # sealed: fsync'd, complete, immutable, safe to consume
  seg-0000002
  ...
<checkpoint path>       # SDK DiskStore: acked seqId per meta (atomic temp+rename)
```

Segment file = a sequence of records, each:

| field       | type        | notes                                        |
|-------------|-------------|----------------------------------------------|
| payloadLen  | uvarint     | length of `payload` in bytes                 |
| metaId      | uint32 (LE) | source metadata server                       |
| seqId       | uint64 (LE) | per-meta sequence id                         |
| payload     | bytes       | protobuf wire form of the `bw.Event`         |

- Segment number is zero-padded and strictly increasing; **consume in number order** to get arrival
  order.
- Records within a segment are in arrival order.
- A reference reader/decoder should ship with the collector so the format is executable
  documentation for the downstream utility team.

---

## 7. Ack strategy and the drop-safety timing

- **When:** the writer acks a segment's events immediately after `rename` succeeds. Not before
  (would ack data not on disk); not deferred to any later stage (there is none).
- **How (today):** the receiver records, per active buffer, the list of `(metaId, seqId)`; the writer
  replays them to the SDK acks channel after seal.
- **How (with `AckUpTo`):** the receiver records `map[metaId]maxSeq` per buffer; the writer calls
  `AckUpTo(metaId, maxSeq)` once per meta per segment.

**Timing constraint (must hold):**
```
timeBound + writeLatency  <  EventBufferSize / peakEventsPerSec        (with margin)
```
`EventBufferSize` is Watch's ring-buffer capacity in events. Example: a 1,000,000-event Watch buffer
at 100,000 events/s is a 10 s window; a ~1 s time bound leaves comfortable margin, a 10 s bound does
not. Document the operator guidance: **size Watch's `EventBufferSize` and the collector's flush
bounds together.**

---

## 8. Ordering guarantee (statement and proof sketch)

**Guarantee:** reading segments in ascending segment number, and records within each segment in
file order, yields exactly the order events were received on the SDK channel.

**Why it holds:**
1. A single receiver reads the SDK channel; the read order defines arrival order.
2. The receiver appends records to the active buffer in that order.
3. Rotation hands buffers to the writer over a FIFO channel; the writer is the sole consumer and
   assigns strictly increasing segment numbers as it seals.
4. Therefore segment number order == buffer fill order == arrival order, and in-file order ==
   append order == arrival order. ∎

**Caveat (inherent to at-least-once):** after a crash, un-acked events are *re-received* and appended
at their new position in the stream. This is consistent with "order received at the subscriber" (they
are received again), but it means **duplicates are possible**. The idempotency/dedup key is
`(metaId, seqId)`; deduplication is the downstream utility's responsibility.

---

## 9. Crash recovery and idempotency

- Write protocol: create `seg-N.partial` → append records → `fsync` → `rename` to `seg-N` → ack.
- Startup: delete any `*.partial` (incomplete, their events were never acked and will be replayed).
- The SDK checkpoint (`DiskStore`) persists the per-meta acked watermark atomically (temp file +
  `fsync` + `rename`); on restart Watch resumes after it, minimizing replay.
- Duplicates after replay are expected; downstream dedups on `(metaId, seqId)`.

---

## 10. Backpressure and failure modes

- Normal: receiver fills buffer B while the writer flushes buffer A. RAM (pool buffers + hand-off
  channel + SDK events channel) absorbs transient `fsync` spikes.
- Disk sustained-slow: hand-off channel fills → receiver blocks → SDK events channel fills → `Recv`
  stalls → Watch back-pressures → **ring-buffer drops** if it overflows. This is the unavoidable end
  of the line; the design's job is to make RAM deep enough that it is rarely reached, and to make it
  **observable** when it is.
- **Drop detection:** the receiver tracks `lastSeq[metaId]`; a jump greater than 1 means Watch
  dropped events (real loss). Emit a metric and a log line — this is the only signal of loss.

---

## 11. Observability

Export at minimum:

- events/s and bytes/s ingested
- active buffer fill (bytes, count), rotate rate
- write latency, fsync latency (histograms)
- hand-off channel depth, SDK events channel depth (proximity-to-stall indicators)
- per-meta acked watermark and its lag behind the newest received seqId
- **seqId gap count per meta** (Watch drops)
- segments sealed, bytes written, `.partial` cleaned on startup

---

## 12. Configuration

| knob                     | default        | rationale                                                     |
|--------------------------|----------------|---------------------------------------------------------------|
| `spool-dir`              | —              | where segments are written                                    |
| `checkpoint`             | —              | SDK `DiskStore` path                                          |
| `segment-max-bytes`      | 32–64 MB       | one large sequential write; dominates at high rate (D4)       |
| `segment-max-time`       | 1 s            | caps ack latency when idle; keep << ring-buffer window (§7)   |
| `handoff-buffers`        | 3–4            | pooled buffers absorbing fsync spikes                         |
| `events-chan-size`       | 64k–256k       | cover worst-case writer stall before Watch feels it           |
| `ack-frequency` (SDK)    | ≤ 1 s          | how often watermarks reach Watch and the checkpoint           |
| `listen`                 | 0.0.0.0:50052  | address Watch dials                                           |
| TLS options              | per deployment | production must not disable TLS                                |

---

## 13. Resource budget (per event, at peak)

| component | CPU | memory / GC |
|-----------|-----|-------------|
| receiver  | channel recv + (copy or `proto.Marshal`) + append + header | none retained; event freed immediately |
| writer    | ~0 per event; 1 `write` + 1 `fsync` per **segment** | — |
| buffers   | — | pool of `handoff-buffers` × `segment-max-bytes`, flat/pointer-free (low GC) |
| ack track | tiny | `map[metaId]maxSeq` per buffer (with `AckUpTo`) or `(meta,seq)` list (fallback) |

The intended steady-state bottleneck is **sequential disk bandwidth**, which is the correct place for
it to be.

---

## 14. Implementation milestones

1. **Hot path + durability.** Receiver, pooled double-buffer, single writer, `[len][meta][seq][raw]`
   framing, seal → ack, `.partial` startup sweep. Serialize via `proto.Marshal` (current SDK). Ship a
   reference segment decoder.
2. **Observability.** seqId-gap detection and the metrics in §11.
3. **Hardening.** Load test to peak rate; tune `segment-max-bytes` / `segment-max-time`; confirm zero
   seqId gaps (no Watch drops) at target throughput; verify crash/replay + dedup behavior.
4. **SDK enhancements (beegfs-go).** Raw-bytes receive path (zero-copy serialize) and
   `AckUpTo(metaId, seqId)`; switch the collector to zero-copy append and per-meta-max acking.

---

## 15. Rejected / deferred alternatives

- **JSONL or any human-readable final format** — rejected (D1): reflection JSON is the main CPU cost
  and the product is machine-consumed.
- **A separate parser/transform stage** — removed (D1): no transform means no stage.
- **Per-meta sharded output files** — deferred: the requirement is a single arrival-ordered stream.
  Per-meta shards would give natural parallelism and per-meta ordering but change the output model;
  revisit only if a single writer cannot keep up with sequential disk bandwidth.
- **Parallel serialization / multiple writers** — rejected: reorders the single stream; would require
  a reorder buffer keyed by an ingress sequence number. Only reconsider if single-core serialize
  becomes the proven bottleneck.
- **Buffering `*bw.Event` structs** — rejected (D2): pointer-heavy, high GC cost.
- **Deferring acks to a downstream stage** — rejected: violates the drop-safety timing (§7); the
  sealed segment is the correct durability boundary.
```
