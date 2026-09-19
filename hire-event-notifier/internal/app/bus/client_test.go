package bus_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	bus "github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/bus"
)

func newTestClient(t *testing.T, startAtOldest bool) (*bus.Client, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)

	client, err := bus.New(&bus.Config{
		RedisURL:      "redis://" + server.Addr(),
		DB:            0,
		GroupID:       "test-group",
		ConsumerID:    "test-consumer",
		MaxLen:        100,
		BatchCount:    2,
		Block:         50 * time.Millisecond,
		DialTimeout:   time.Second,
		StartAtOldest: startAtOldest,
	})
	if err != nil {
		t.Fatalf("failed to build client: %v", err)
	}

	return client, server
}

// consume runs the consumer until it has seen wantCount messages or the
// deadline expires, returning whatever it collected.
func consume(
	t *testing.T,
	client *bus.Client,
	topic string,
	wantCount int,
	handler func(bus.Message) error,
) []bus.Message {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var (
		mutex    sync.Mutex
		received []bus.Message
		waiter   sync.WaitGroup
	)

	waiter.Add(1)

	go func() {
		defer waiter.Done()

		_ = client.Consume(ctx, topic, func(_ context.Context, message bus.Message) error {
			mutex.Lock()
			received = append(received, message)
			done := len(received) >= wantCount
			mutex.Unlock()

			if done {
				cancel()
			}

			return handler(message)
		})
	}()

	waiter.Wait()

	mutex.Lock()
	defer mutex.Unlock()

	return append([]bus.Message(nil), received...)
}

func TestPublishAndConsume(t *testing.T) {
	tests := []struct {
		name       string
		published  [][2]string
		wantValues []string
	}{
		{
			name:       "single message",
			published:  [][2]string{{"k1", "v1"}},
			wantValues: []string{"v1"},
		},
		{
			name:       "batch larger than BatchCount",
			published:  [][2]string{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}},
			wantValues: []string{"v1", "v2", "v3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := newTestClient(t, true)
			topic := "test-stream"

			for _, pair := range tt.published {
				err := client.Publish(context.Background(), topic, []byte(pair[0]), []byte(pair[1]))
				if err != nil {
					t.Fatalf("publish failed: %v", err)
				}
			}

			received := consume(t, client, topic, len(tt.wantValues), func(bus.Message) error {
				return nil
			})

			if len(received) != len(tt.wantValues) {
				t.Fatalf("got %d messages, want %d", len(received), len(tt.wantValues))
			}

			for i, want := range tt.wantValues {
				if string(received[i].Value) != want {
					t.Errorf("message %d: got value %q, want %q", i, received[i].Value, want)
				}
				if received[i].Topic != topic {
					t.Errorf("message %d: got topic %q, want %q", i, received[i].Topic, topic)
				}
			}

			if string(received[0].Key) != tt.published[0][0] {
				t.Errorf("got key %q, want %q", received[0].Key, tt.published[0][0])
			}
		})
	}
}

func TestFailedHandlerLeavesEntryPendingAndDoesNotSpin(t *testing.T) {
	client, _ := newTestClient(t, true)
	topic := "test-stream"

	err := client.Publish(context.Background(), topic, []byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	var deliveries int

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	var mutex sync.Mutex

	_ = client.Consume(ctx, topic, func(_ context.Context, _ bus.Message) error {
		mutex.Lock()
		deliveries++
		mutex.Unlock()

		return errors.New("handler failed")
	})

	mutex.Lock()
	got := deliveries
	mutex.Unlock()

	// The entry must be delivered once and then skipped, not re-read in a
	// tight loop for the rest of the run.
	if got != 1 {
		t.Fatalf("handler ran %d times, want exactly 1 (hot loop on the pending list)", got)
	}
}

func TestPendingEntriesAreReplayedOnRestart(t *testing.T) {
	client, server := newTestClient(t, true)
	topic := "test-stream"

	err := client.Publish(context.Background(), topic, []byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	failing := consume(t, client, topic, 1, func(bus.Message) error {
		return errors.New("handler failed")
	})
	if len(failing) != 1 {
		t.Fatalf("first run got %d messages, want 1", len(failing))
	}

	restarted, err := bus.New(&bus.Config{
		RedisURL:      "redis://" + server.Addr(),
		GroupID:       "test-group",
		ConsumerID:    "test-consumer",
		MaxLen:        100,
		BatchCount:    2,
		Block:         50 * time.Millisecond,
		DialTimeout:   time.Second,
		StartAtOldest: true,
	})
	if err != nil {
		t.Fatalf("failed to rebuild client: %v", err)
	}

	replayed := consume(t, restarted, topic, 1, func(bus.Message) error {
		return nil
	})

	if len(replayed) != 1 {
		t.Fatalf("restart replayed %d messages, want 1", len(replayed))
	}

	if string(replayed[0].Value) != "v1" {
		t.Errorf("got replayed value %q, want %q", replayed[0].Value, "v1")
	}
}

func TestStartAtOldestControlsBacklogVisibility(t *testing.T) {
	tests := []struct {
		name          string
		startAtOldest bool
		wantBacklog   bool
	}{
		{name: "oldest reads the backlog", startAtOldest: true, wantBacklog: true},
		{name: "newest skips the backlog", startAtOldest: false, wantBacklog: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := newTestClient(t, tt.startAtOldest)
			topic := "test-stream"

			err := client.Publish(context.Background(), topic, []byte("k1"), []byte("v1"))
			if err != nil {
				t.Fatalf("publish failed: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()

			var (
				mutex sync.Mutex
				count int
			)

			_ = client.Consume(ctx, topic, func(_ context.Context, _ bus.Message) error {
				mutex.Lock()
				count++
				mutex.Unlock()

				return nil
			})

			mutex.Lock()
			got := count > 0
			mutex.Unlock()

			if got != tt.wantBacklog {
				t.Errorf("backlog visible = %v, want %v", got, tt.wantBacklog)
			}
		})
	}
}
