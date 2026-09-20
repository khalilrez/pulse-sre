# The queue Writer: design, bugs, and tests

This document covers the `Writer` half of `internal/queue` — the append-only
log writer used by the ingest API. It's written as a build log rather than
just a spec, because the bugs found along the way are as informative as the
final code, and worth being able to talk through in an interview.

## What it is

`Writer` appends events to `queue.log` as newline-delimited JSON — one JSON
object per line, in the order they were written. It's the durability boundary
for the whole system: the HTTP handler that uses it must not respond `200`
to a client until `Write` returns `nil`.

```go
type Writer struct {
	mu        sync.Mutex
	file      *os.File
	syncEvery bool
}

type Event struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
	ID        string          `json:"id"`
}
```

### Design decisions

- **Append-only, JSON lines.** Simple format, trivially appendable, trivially
  readable back line-by-line by a `bufio.Scanner` on the reader side.
- **`syncEvery` is an explicit durability/latency tradeoff, not a hidden
  default.** Without it, a successful `Write` survives the Go process
  crashing (the bytes are in the OS page cache) but not a full power loss
  (the OS hasn't necessarily flushed the cache to disk yet). With `fsync`
  called on every write, it survives power loss too, at a latency cost.
  Neither is "correct" — it's a tradeoff to state, not hide.
- **A mutex around the whole marshal-then-write sequence**, even though
  `O_APPEND` already gives atomic single-write guarantees at the OS level.
  The mutex isn't protecting the OS — it's making the *two-step* sequence
  (marshal, then write) look atomic to the rest of the program, so nothing
  else has to reason about interleaved partial writes from this package.
- **A trailing newline after every event**, since the reader depends on it as
  the record delimiter. Miss this and two events concatenate into one
  unparseable line.
- **`os.OpenRoot`** is used to scope file access to the queue directory
  (Go 1.24+), rather than opening an arbitrary path directly. Filenames
  passed to `root.OpenFile` are resolved *relative to that root* — not
  relative to the working directory, and not as an absolute/joined path.
- **If `f.Sync()` fails after a successful `f.Write()`,** `Write` still
  returns the error. The bytes are sitting in the page cache either way, but
  `Write`'s contract is "returns `nil` only if durable," and a failed sync
  means that promise can't be honestly kept — even though the write itself
  succeeded.

## Bugs hit while building it (and why they happened)

| # | Bug | Root cause | Fix |
|---|-----|-----------|-----|
| 1 | `os.OpenFile(dir, ...)` failed | Passed the *directory* path where a *file* path was expected — tried to open the directory itself as the log file | Use `os.OpenRoot(dir)` then `root.OpenFile("queue.log", ...)`, keeping the filename separate from the root |
| 2 | Same bug, second form | First fix to `OpenRoot` still passed `dir` again as the filename argument to `root.OpenFile`, doubling the path | Pass just `"queue.log"` — the root is already scoped to `dir` |
| 3 | No newline between events | `Write` appended raw marshaled JSON with nothing between records | Append `'\n'` after marshaling, before writing |
| 4 | Double-wrapped errors | Both `open()` and `OpenWriter()` wrapped the same underlying error with similar context strings, producing a redundant message | Decide once where context gets added; don't wrap twice for the same failure |
| 5 | `isJSON` check inverted | `case isJSON(e.Payload): return false` rejects events *whose payload is already valid JSON* — backwards from the intent (reject *invalid* JSON) | Flip the condition to `case !isJSON(e.Payload)` |
| 6 | Test asserted wrong shape | `TestWriterHappyPath` compared the file's contents against `e.Payload` alone, but `Write` marshals the *whole* `Event`, not just the payload field | Compare against the full marshaled `Event` (or unmarshal the line back and compare fields) |
| 7 | Unreadable test log output | `t.Logf("%s", r)` was called with the default verb, printing a `[]byte` as a long list of decimal numbers instead of text | Explicitly use `%s` to print the byte slice as a string |
| 8 | Test result looked wrong across "separate" runs | `go test` caches results per package when nothing has changed and replays old output instead of re-executing — made it look like the same temp dir and 6 lines were reappearing | Run with `-count=1` while iterating to force a real re-execution every time |
| 9 | Off-by-one on line count | `bytes.Split` on content ending in `\n` produces a trailing empty element after the final delimiter | Trim the trailing empty element (`events[:len(events)-1]`), or switch to a split method that doesn't produce one |
| 10 | `t.Fatal` inside a goroutine | `t.Fatal`/`t.Fatalf` call `runtime.Goexit()`, which only halts *the calling goroutine* — using them inside a spawned goroutine doesn't fail the test safely and can strand a `WaitGroup` | Use `t.Errorf` (safe to call from any goroutine) instead of `t.Fatal`/`t.Fatalf` inside goroutines |
| 11 | Race condition in the concurrency test itself | A single `Event` variable declared outside the loop was mutated (`e.ID = ...`) on each iteration and then read inside a goroutine with no synchronization — the goroutine could read `e.ID` after a later iteration had already overwritten it | Construct a fresh, independent `Event` per iteration/goroutine rather than mutating one shared variable |
| 12 | `no Go files in ...` running `-race` | `go test` was run from the repo root, not from (or pointed at) the package directory containing the test files | `cd` into the package, or pass the package path explicitly: `go test -race ./internal/queue` |

## Test suite

All tests live in `internal/queue/writer_test.go`, package `queue`, run with
`go test ./internal/queue/...` (add `-race` when touching anything
concurrent, and `-count=1` while iterating to bypass the test cache).

- **`TestWriterHappyPath`** — writes one valid event, closes the writer,
  reads the file back, and confirms it round-trips (full marshaled event,
  newline-terminated).
- **`TestTwoConsecutiveWrites`** — writes two events sequentially, confirms
  the file has exactly two well-formed lines in order, proving `O_APPEND`
  behaves as expected across multiple writes.
- **`TestConcurrentWrites`** — writes 10 events concurrently from separate
  goroutines, each with a distinct `ID`, and confirms the file ends up with
  exactly 10 well-formed lines representing all 10 distinct IDs — the test
  that actually exercises the mutex. Run with `-race` to catch synchronization
  bugs the line-count assertion alone can't see.
- **`TestInvalidEventRejected`**  — writes an
  event with a malformed payload, asserts `Write` returns a non-nil error,
  and asserts the file is unchanged (no partial/garbage line written).