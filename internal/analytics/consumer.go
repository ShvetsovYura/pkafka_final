package analytics

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type AnalyticsConsumer struct {
	hadoopClient *HadoopClient
	consumer     *kafka.Consumer
	topic        string
}

func NewAnalyticsConsumer(topic string, config types.ConsumerConfig, hadoopCLient *HadoopClient) (*AnalyticsConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                   config.BootstrapServers,
		"group.id":                            config.GroupID,
		"auto.offset.reset":                   config.AutoOffsetReset,
		"enable.auto.commit":                  config.EnableAutoCommit,
		"session.timeout.ms":                  config.SessionTimeoutMs,
		"security.protocol":                   config.SecurityProtocol,
		"ssl.ca.location":                     config.CACertLocation,
		"ssl.certificate.location":            config.CertLocation,
		"ssl.key.location":                    config.CertKeyLocation,
		"sasl.mechanism":                      config.SASLMechanism,
		"sasl.username":                       config.SASLUsername,
		"sasl.password":                       config.SASLPassword,
		"enable.ssl.certificate.verification": config.EnableCertVerify,
		"client.id":                           config.ClientID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %s", err)
	}
	return &AnalyticsConsumer{
		topic:        topic,
		hadoopClient: hadoopCLient,
		consumer:     consumer,
	}, nil
}

func (c *AnalyticsConsumer) Run(ctx context.Context, wg *sync.WaitGroup) error {

	err := c.consumer.Subscribe(c.topic, nil)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	defer c.consumer.Close()

	//make dir

	// Poll for messages
	for {
		msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
		if err == nil {
			value := string(msg.Value)
			fmt.Printf("Received message: value=%s, partition=%v\n", value, msg.TopicPartition)
			err = c.hadoopClient.Write(string(msg.Key), value)
			if err != nil {
				slog.Error("error on handle msg", slog.Any("error", err))
				continue
			}
		} else {
			// Handle errors
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.IsFatal() {
				log.Fatalf("Fatal error: %s", kafkaErr)
			}
		}
	}
}
