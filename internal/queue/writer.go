// simple append-only log writer for events
package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

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

func (w *Writer) open(dir string) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("failed to open root directory: %w", err)
	}
	f, err := root.OpenFile("queue.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open queue file: %w", err)
	}
	w.file = f
	return nil
}

func OpenWriter(dir string, syncEvery bool) (*Writer, error) {
	w := &Writer{
		syncEvery: syncEvery,
	}
	// make sure the directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create writer directory: %w", err)
	}
	// open the file
	if err := w.open(dir); err != nil {
		return nil, err
	}

	return w, nil
}
func isJSON(b []byte) bool {
	var js json.RawMessage
	return json.Unmarshal(b, &js) == nil
}

func validateEvent(e Event) bool {
	switch {
	case e.EventType == "":
		return false
	case !isJSON(e.Payload):
		return false
	case e.Timestamp.IsZero():
		return false
	case e.ID == "":
		return false
	default:
		return true
	}
}
func (w *Writer) Write(e Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// validate the event
	if valid := validateEvent(e); !valid {
		return fmt.Errorf("invalid event: %v", e)
	}

	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	// add a newline to separate events
	b = append(b, '\n')
	// write the event
	_, err = w.file.Write(b)
	if err != nil {
		return fmt.Errorf("failed to write event: %w", err)
	}
	if w.syncEvery {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	}
	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
