# pulse-sre

A small, durable event queue in Go, built to practice the reliability details that matter in ingestion pipelines: what "written" actually means, what survives a crash, and how to test concurrent code honestly.

**Status:** the append-only `Writer` is implemented and tested. The `Reader` (`internal/queue/reader.go`) is not written yet.

## What the writer guarantees

`Writer.Write` appends one event as a line of JSON to `queue.log` and returns `nil` only once the event is durable. A caller, such as an HTTP ingest handler, can safely answer `200` only after `Write` returns.

- **Append-only JSON lines**, one event per line, in write order.
- **Explicit durability tradeoff.** `OpenWriter(dir, syncEvery)`: with `syncEvery=true` every write is `fsync`ed and survives power loss; with `false` it survives a process crash but not power loss, in exchange for lower latency.
- **Safe for concurrent use.** A mutex makes marshal-then-write a single step; verified with `go test -race`.
- **Validation before write.** Events with an empty type or ID, a zero timestamp or a non-JSON payload are rejected, and nothing is written.
- **Scoped file access** with `os.OpenRoot` (Go 1.24+).

```go
w, err := queue.OpenWriter("data", true) // true = fsync every write
if err != nil { /* ... */ }
defer w.Close()

err = w.Write(queue.Event{
    EventType: "page_view",
    Payload:   json.RawMessage(`{"url":"/pricing"}`),
    Timestamp: queue.Now(),
    ID:        "evt_123",
})
```

## Run it

Requires Go 1.24+.

```bash
go test -race -count=1 ./...
go run ./cmd/writer-sanity-check
```

## Design notes

[`docs/writer.md`](docs/writer.md) is a build log of the design decisions and the 12 bugs hit along the way, with root cause and fix for each.

## Roadmap

- [ ] `Reader` with offset tracking
- [ ] Log rotation
- [ ] Crash-recovery test (truncated final line)
