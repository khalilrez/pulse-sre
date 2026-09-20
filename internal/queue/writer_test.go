package queue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestWriterHappyPath(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	w, err := OpenWriter(tempDir, true)
	if err != nil {
		t.Fatal(err)
	}
	e := Event{
		EventType: "test",
		Payload:   []byte("{\"test\":\"test\"}"),
		Timestamp: Now(),
		ID:        "test",
	}
	err = w.Write(e)
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	r, err := os.ReadFile(tempDir + "/queue.log")
	if err != nil {
		t.Fatal(err)
	}
	// check that the file contains the exact event
	expected, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	expected = append(expected, '\n')
	if string(r) != string(expected) {
		t.Fatalf("expected %s, got %s", expected, string(r))
	}
}

func TestTwoConsecutiveWrites(t *testing.T) {
	tempDir := t.TempDir()
	t.Logf("temp dir: %s", tempDir)
	w, err := OpenWriter(tempDir, true)
	if err != nil {
		t.Fatal(err)
	}
	eOne := Event{
		EventType: "test",
		Payload:   []byte("{\"test\":\"test\"}"),
		Timestamp: Now(),
		ID:        "test",
	}
	err = w.Write(eOne)
	if err != nil {
		t.Fatal(err)
	}
	eTwo := Event{
		EventType: "test2",
		Payload:   []byte("{\"test\":\"test2\"}"),
		Timestamp: Now(),
		ID:        "test2",
	}
	err = w.Write(eTwo)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := os.ReadFile(tempDir + "/queue.log")
	if err != nil {
		t.Fatal(err)
	}
	// read two events using the newline as a separator
	events := bytes.Split(r, []byte("\n"))
	events = events[:len(events)-1]
	if len(events) != 2 {
		t.Logf("events: %s", events)
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	var eOneFromFile Event
	err = json.Unmarshal(events[0], &eOneFromFile)
	if err != nil {
		t.Fatal(err)
	}
	var eTwoFromFile Event
	err = json.Unmarshal(events[1], &eTwoFromFile)
	if err != nil {
		t.Fatal(err)
	}
	if eOneFromFile.EventType != "test" {
		t.Fatalf("expected event type test, got %s", eOneFromFile.EventType)
	}
	if eTwoFromFile.EventType != "test2" {
		t.Fatalf("expected event type test2, got %s", eTwoFromFile.EventType)
	}
	if eOneFromFile.ID != "test" {
		t.Fatalf("expected id test, got %s", eOneFromFile.ID)
	}
	if eTwoFromFile.ID != "test2" {
		t.Fatalf("expected id test2, got %s", eTwoFromFile.ID)
	}
	if eOneFromFile.Payload == nil {
		t.Fatalf("expected payload, got nil")
	}
	if eTwoFromFile.Payload == nil {
		t.Fatalf("expected payload, got nil")
	}
	if string(eOneFromFile.Payload) != "{\"test\":\"test\"}" {
		t.Fatalf("expected payload {test:test}, got %s", string(eOneFromFile.Payload))
	}
	if string(eTwoFromFile.Payload) != "{\"test\":\"test2\"}" {
		t.Fatalf("expected payload {test:test2}, got %s", string(eTwoFromFile.Payload))
	}
}

func TestWriterWithInvalidJSON(t *testing.T) {
	tempDir := t.TempDir()
	t.Logf("temp dir: %s", tempDir)

	w, err := OpenWriter(tempDir, true)
	if err != nil {
		t.Fatal(err)
	}
	e := Event{
		EventType: "test",
		Payload:   []byte("\"test\":\"test\"}"),
		Timestamp: Now(),
		ID:        "test",
	}
	// add a newline to the end of the payload
	e.Payload = append(e.Payload, '\n')
	err = w.Write(e)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()
	t.Logf("temp dir: %s", tempDir)
	w, err := OpenWriter(tempDir, true)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		e := Event{
			EventType: "test",
			Payload:   []byte("{\"test\":\"test\"}"),
			Timestamp: Now(),
			ID:        fmt.Sprintf("test%d", i),
		}
		go func() {
			defer wg.Done()
			err := w.Write(e)
			if err != nil {
				t.Errorf("failed to write event: %v", err)
			}
		}()
	}
	wg.Wait()
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	r, err := os.ReadFile(tempDir + "/queue.log")
	if err != nil {
		t.Fatal(err)
	}
	events := bytes.Split(r, []byte("\n"))
	events = events[:len(events)-1]
	if len(events) != 10 {
		t.Fatalf("expected 10 events, got %d", len(events))
	}
}
