package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"audit-service/internal/domain"

	"github.com/IBM/sarama"
)

type DeadLetterProducer interface {
	SendDeadLetter(context.Context, *sarama.ConsumerMessage, error) error
}

type Replayer struct {
	client   sarama.Client
	consumer sarama.Consumer
	topic    string
	dlq      DeadLetterProducer
	log      *slog.Logger
}

type partitionRange struct {
	start int64
	end   int64
}

func New(
	brokers []string,
	topic string,
	dlq DeadLetterProducer,
	log *slog.Logger,
) (*Replayer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_0_0
	cfg.Consumer.Return.Errors = true

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create replay Kafka client: %w", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("create replay Kafka consumer: %w", err)
	}

	return &Replayer{
		client:   client,
		consumer: consumer,
		topic:    topic,
		dlq:      dlq,
		log:      log,
	}, nil
}

func (r *Replayer) Close() error {
	if err := r.consumer.Close(); err != nil {
		_ = r.client.Close()
		return err
	}
	return r.client.Close()
}

func (r *Replayer) Rebuild(
	ctx context.Context,
	from time.Time,
	to time.Time,
) (domain.ReplayResult, error) {
	partitions, err := r.client.Partitions(r.topic)
	if err != nil {
		return domain.ReplayResult{}, fmt.Errorf("list replay partitions: %w", err)
	}

	counts := make(map[string]int64)
	var processed int64

	for _, partition := range partitions {
		partitionProcessed, err := r.replayPartition(ctx, partition, from, to, counts)
		if err != nil {
			return domain.ReplayResult{}, err
		}
		processed += partitionProcessed
	}

	// Replay returns an independent historical result. It deliberately does
	// not replace the rolling one-hour stats_cache maintained by the group.
	return domain.ReplayResult{
		EventsProcessed: processed,
		Stats:           statsFromCounts(counts),
	}, nil
}

func (r *Replayer) replayPartition(
	ctx context.Context,
	partition int32,
	from time.Time,
	to time.Time,
	counts map[string]int64,
) (int64, error) {
	offsets, ok, err := r.offsetRange(partition, from, to)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}

	partitionConsumer, err := r.consumer.ConsumePartition(r.topic, partition, offsets.start)
	if err != nil {
		return 0, fmt.Errorf("consume replay partition %d: %w", partition, err)
	}

	processed, readErr := r.readPartition(
		ctx,
		partitionConsumer,
		offsets.end,
		from,
		to,
		counts,
	)
	if closeErr := partitionConsumer.Close(); readErr == nil && closeErr != nil {
		readErr = closeErr
	}
	if readErr != nil {
		return 0, fmt.Errorf("replay partition %d: %w", partition, readErr)
	}
	return processed, nil
}

func (r *Replayer) offsetRange(
	partition int32,
	from time.Time,
	to time.Time,
) (partitionRange, bool, error) {
	startOffset, err := r.client.GetOffset(r.topic, partition, from.UnixMilli())
	if err != nil {
		return partitionRange{}, false, fmt.Errorf(
			"get start offset for partition %d: %w",
			partition,
			err,
		)
	}
	if startOffset == sarama.OffsetNewest {
		return partitionRange{}, false, nil
	}

	endOffset, err := r.client.GetOffset(r.topic, partition, to.UnixMilli())
	if err != nil {
		return partitionRange{}, false, fmt.Errorf(
			"get end offset for partition %d: %w",
			partition,
			err,
		)
	}
	if endOffset == sarama.OffsetNewest {
		endOffset, err = r.client.GetOffset(r.topic, partition, sarama.OffsetNewest)
		if err != nil {
			return partitionRange{}, false, fmt.Errorf(
				"get newest offset for partition %d: %w",
				partition,
				err,
			)
		}
	}
	if startOffset >= endOffset {
		return partitionRange{}, false, nil
	}

	return partitionRange{start: startOffset, end: endOffset}, true, nil
}

func statsFromCounts(counts map[string]int64) []domain.Stat {
	actions := make([]string, 0, len(counts))
	for action := range counts {
		actions = append(actions, action)
	}
	sort.Strings(actions)

	stats := make([]domain.Stat, 0, len(actions))
	for _, action := range actions {
		stats = append(stats, domain.Stat{Group: action, Count: counts[action]})
	}
	return stats
}

func (r *Replayer) readPartition(
	ctx context.Context,
	partitionConsumer sarama.PartitionConsumer,
	endOffset int64,
	from time.Time,
	to time.Time,
	counts map[string]int64,
) (int64, error) {
	var processed int64

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()

		case consumerError, ok := <-partitionConsumer.Errors():
			if ok && consumerError != nil {
				return 0, consumerError
			}

		case message, ok := <-partitionConsumer.Messages():
			if !ok {
				return processed, nil
			}
			if message.Offset >= endOffset {
				return processed, nil
			}

			var event domain.Event
			if err := json.Unmarshal(message.Value, &event); err != nil {
				if err := r.sendToDLQ(ctx, message, fmt.Errorf("decode JSON: %w", err)); err != nil {
					return 0, err
				}
			} else if _, ok := domain.ValidActions[event.Action]; !ok {
				if err := r.sendToDLQ(
					ctx,
					message,
					fmt.Errorf("unknown action %q", event.Action),
				); err != nil {
					return 0, err
				}
			} else if !event.Timestamp.Before(from) && event.Timestamp.Before(to) {
				counts[event.Action]++
				processed++
			}

			// endOffset points to the next position after the requested range.
			if message.Offset+1 >= endOffset {
				return processed, nil
			}
		}
	}
}

func (r *Replayer) sendToDLQ(
	ctx context.Context,
	message *sarama.ConsumerMessage,
	cause error,
) error {
	if err := r.dlq.SendDeadLetter(ctx, message, cause); err != nil {
		return fmt.Errorf(
			"send replay message at partition %d offset %d to DLQ: %w",
			message.Partition,
			message.Offset,
			err,
		)
	}
	r.log.Warn(
		"invalid replay message sent to DLQ",
		"error", cause,
		"key", string(message.Key),
		"partition", message.Partition,
		"offset", message.Offset,
	)
	return nil
}
