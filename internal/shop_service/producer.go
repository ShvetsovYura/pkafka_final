package shopservice

import (
	"fmt"
	"log"

	"github.com/ShvetsovYura/pkafka_final/internal/models"
	registryclient "github.com/ShvetsovYura/pkafka_final/internal/registry_client"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func StartProducer(topic string, messagesCh <-chan models.Product, registryClient *registryclient.SchemaRegistryClient) {
	var cfgMap = kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"security.protocol": "plaintext",
	}
	deliveryChan := make(chan kafka.Event)

	p, err := kafka.NewProducer(&cfgMap)
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	defer func() {
		p.Close()
		close(deliveryChan)
	}()

	for {

		msg := <-messagesCh
		payload, err := registryClient.Serializer.Serialize(topic, &msg)
		if err != nil {
			fmt.Println("Failed ser")
			// logger.Error("Failed to serialize payload", slog.Any("error", err))
		}

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(msg.ProductID),
			Value:          payload,
		}, deliveryChan)
		if err != nil {
			fmt.Println("producer failed")
			// logger.Error("Produce failed", slog.Any("error", err))
		}

		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Println("delivery failed")
			// logger.Error("Delivery failed", slog.Any("error", m.TopicPartition.Error))
		} else {
			fmt.Println("delivery msg")
			// logger.Info("Delivered message ",
			// 	slog.String("topic", *m.TopicPartition.Topic),
			// 	slog.Int("partition", int(m.TopicPartition.Partition)),
			// 	slog.Any("offset", m.TopicPartition.Offset))
		}
	}
}
