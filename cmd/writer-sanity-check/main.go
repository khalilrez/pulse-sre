// sanity check for the writer module
package main

import (
	"log/slog"
	"pulse-sre/internal/queue"
)

func main() {
	slog.Info("starting writer sanity check")
	slog.Info("opening writer")
	w, err := queue.OpenWriter("test", true)
	if err != nil {
		slog.Error("failed to open writer", "err", err)
		return
	}
	slog.Info("writer opened", "writer", w)
	slog.Info("writing event")
	e := queue.Event{
		EventType: "test",
		Payload:   []byte("test"),
		Timestamp: queue.Now(),
		ID:        "test",
	}
	err = w.Write(e)
	if err != nil {
		slog.Error("failed to write event", "err", err)
		return
	}
	slog.Info("event written successfully")
	slog.Info("closing writer")
	err = w.Close()
	if err != nil {
		slog.Error("failed to close writer", "err", err)
		return
	}
	slog.Info("writer closed successfully")
	slog.Info("done")
}
