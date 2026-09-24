# Watch subscriber examples

Two small Go programs that read the BeeGFS Watch event stream. They are meant to be read in
order: the first shows the protocol, the second shows what an index has to do with it.

Neither is part of the CMake build. They are a separate Go module so they can be built and run
on their own.

```bash
cd examples/watch-subscriber
go mod tidy
```

## subscriber/

About eighty lines. It prints every event it receives and nothing else.

Watch dials **out** to you, so this program is the gRPC server and Watch is the client. The
whole contract is one bidirectional stream: Watch sends `Event`, you send `Response`. A
`Response` acknowledges a sequence id, and the first one after connecting may carry an
`EventFilter`. An empty filter means every type, which is what you want while learning.

```bash
go run ./subscriber --listen 0.0.0.0:50052
```

Then make something happen on the mount. Events appear a moment later.

## markwalk/

The next job: turning those events into the input GUFI's incremental update wants.

GUFI does not take paths. It takes a suspect file of `<inode> d` lines and rescans exactly
those directories. Three stages sit in between, and each one can lose data quietly.

```
an event names a path   ->  which directories does it dirty?
a directory             ->  is it still there, and does the index know its parents?
a set of directories    ->  "<inode> d" lines
```

```bash
go run ./markwalk -mount /mnt/beegfs -index /var/lib/gufi -out /tmp/suspects
```

Every few seconds it prints what it marked and the `gufi_incremental_update` command to run
with the file.

No cluster to hand? This builds small trees in a temp directory and runs the same walk over
five awkward cases, printing each step:

```bash
go run ./markwalk -demo
```

`markwalk/FLOW.md` is the design in plain text: the flow, the per-event rules, the walk, a
worked example, and the known gaps.

## What to read for what

| question | look at |
|---|---|
| how do I receive events at all | `subscriber/main.go` |
| what does each event type mean for an index | `markwalk/main.go`, part 1 |
| why walk up the tree | `markwalk/main.go`, part 2, and FLOW.md |
| what could go wrong | FLOW.md, "Known gaps" |
