# Watch subscriber examples

A small Go program that reads the BeeGFS Watch event stream and prints it, to show the
protocol. The collector, markwalk and the spool reader built on it live in `beegfs_watch/`.

It is not part of the CMake build. It is a separate Go module so it can be built and run on
its own.

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

## Next steps

`beegfs_watch/` turns the stream into input for GUFI's incremental update. One binary,
`cmd/beegfs-index-sync`, runs both halves (`--mode both`, the default) or either one alone:

| package | what it does | design |
|---|---|---|
| `internal/collector/` | receives Watch events and seals them into numbered segment files | `DESIGN.md` |
| `internal/markwalk/` | turns each batch of segments into a `<inode> d` suspect file | `internal/markwalk/DESIGN.md`, `FLOW.md` |

How the two share one process: `MERGE-DESIGN.md`.
