package clientservice

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"sync"

	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ClientProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewClientProducer(topic string, cfg types.ProducerConfig) *ClientProducer {
	cfgMap := kafka.ConfigMap{
		"bootstrap.servers":                   cfg.BootstrapServers,
		"security.protocol":                   cfg.SecurityProtocol,
		"ssl.ca.location":                     cfg.CACertLocation,
		"sasl.mechanisms":                     cfg.SASLMechanism,
		"sasl.username":                       cfg.SASLUsername,
		"sasl.password":                       cfg.SASLPassword,
		"ssl.certificate.location":            cfg.CertLocation,
		"ssl.key.location":                    cfg.CertKeyLocation,
		"acks":                                cfg.Acks,
		"client.id":                           cfg.ClientID,
		"enable.ssl.certificate.verification": cfg.EnableCertVerify,
	}

	p, err := kafka.NewProducer(&cfgMap)
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}
	return &ClientProducer{
		producer: p,
		topic:    topic,
	}
}

func (p *ClientProducer) Run(ctx context.Context, wg *sync.WaitGroup, reqCh <-chan types.UserRequest) {

	deliveryChan := make(chan kafka.Event)

	defer func() {
		p.producer.Close()
		close(deliveryChan)
	}()

	slog.Info("Producer running", slog.Any("producer", p))

	for {
		select {
		case <-ctx.Done():
			slog.Info("Получен сигнал выхода, остановка продьюсера...")
			wg.Done()
		case value := <-reqCh:
			payload, err := json.Marshal(value)
			if err != nil {
				slog.Warn("Failed to serialize payload", slog.Any("error", err))
				continue
			}
			err = p.producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
				Value:          payload,
			}, deliveryChan)

			if err != nil {
				slog.Warn("Produce failed", slog.Any("error", err))
			}

			e := <-deliveryChan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				slog.Info("Delivery failed", slog.Any("error", m.TopicPartition.Error))
			} else {
				slog.Info("Delivered message ",
					slog.String("topic", *m.TopicPartition.Topic),
					slog.Int("partition", int(m.TopicPartition.Partition)),
					slog.Any("offset", m.TopicPartition.Offset))
			}
		}

	}

}
