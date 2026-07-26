package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"audit-service/internal/domain"

	"github.com/IBM/sarama"
)

type StatsRepository interface {
	ReplaceStatsCache(context.Context, map[string]int64, time.Time, time.Time) error
}

type Replayer struct {
	client     sarama.Client
	consumer   sarama.Consumer
	topic      string
	repository StatsRepository
}

type partitionRange struct {
	start int64
	end   int64
}

func New(
	brokers []string,
	topic string,
	repository StatsRepository,
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
		client:     client,
		consumer:   consumer,
		topic:      topic,
		repository: repository,
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

	if err := r.repository.ReplaceStatsCache(ctx, counts, from, to); err != nil {
		return domain.ReplayResult{}, err
	}

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

	processed, readErr := readPartition(ctx, partitionConsumer, offsets.end, from, to, counts)
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

func readPartition(
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
				return 0, fmt.Errorf("decode offset %d: %w", message.Offset, err)
			}
			if !event.Timestamp.Before(from) && event.Timestamp.Before(to) {
				if _, ok := domain.ValidActions[event.Action]; !ok {
					return 0, fmt.Errorf("unknown action %q at offset %d", event.Action, message.Offset)
				}
				counts[event.Action]++
				processed++
			}

			// endOffset указывает на следующую позицию после последнего нужного
			// сообщения, поэтому ожидать сообщение с самим endOffset не надо.
			if message.Offset+1 >= endOffset {
				return processed, nil
			}
		}
	}
}
