package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// SplitBrokers parses a comma-separated broker list.
func SplitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// EnsureTopics creates the topics with TopicPartitions partitions each if
// they do not exist yet. It retries while the broker is still starting.
func EnsureTopics(ctx context.Context, brokers []string, topics ...string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		lastErr = nil
		resp, err := admin.CreateTopics(ctx, TopicPartitions, 1, nil, topics...)
		if err != nil {
			lastErr = err
		} else {
			for _, r := range resp {
				if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
					lastErr = fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
				}
			}
		}
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return lastErr
}
