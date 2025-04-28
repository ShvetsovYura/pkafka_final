package shopservice

import (
	"context"
	"log"
	"log/slog"
	"sync"

	"github.com/ShvetsovYura/pkafka_final/internal/models"
	registryclient "github.com/ShvetsovYura/pkafka_final/internal/registry_client"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ShopProducer struct {
	sr       *registryclient.SchemaRegistryClient
	producer *kafka.Producer
	topic    string
}

func NewShopProducer(topic string, cfg types.ProducerConfig, schemaRegistryClient *registryclient.SchemaRegistryClient) *ShopProducer {
	cfgMap := kafka.ConfigMap{
		"bootstrap.servers":                   cfg.BootstrapServers, //"kafka.local:9994",
		"security.protocol":                   cfg.SecurityProtocol, //"SASL_SSL",
		"ssl.ca.location":                     cfg.CACertLocation,   //"ca.crt",
		"sasl.mechanism":                      cfg.SASLMechanism,    //"PLAIN",
		"sasl.username":                       cfg.SASLUsername,     //"admin",
		"sasl.password":                       cfg.SASLPassword,     //"admin-secret",
		"ssl.certificate.location":            cfg.CertLocation,     //"/etc/kafka/service.cert",
		"ssl.key.location":                    cfg.CertKeyLocation,  //"/etc/kafka/service.key",
		"acks":                                cfg.Acks,             //"all",
		"client.id":                           cfg.ClientID,         //"client-producer",
		"enable.ssl.certificate.verification": cfg.EnableCertVerify,
	}

	p, err := kafka.NewProducer(&cfgMap)
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}
	return &ShopProducer{
		sr:       schemaRegistryClient,
		producer: p,
		topic:    topic,
	}
}

func (p *ShopProducer) Run(ctx context.Context, wg *sync.WaitGroup, productCh <-chan models.Product) {

	deliveryChan := make(chan kafka.Event)

	defer func() {
		p.producer.Close()
		close(deliveryChan)
	}()

	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		case msg := <-productCh:
			payload, err := p.sr.Serializer.Serialize(p.topic, &msg)
			if err != nil {
				slog.Error("Failed to serialize payload", slog.Any("error", err))
				continue
			}
			err = p.producer.Produce(
				&kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &p.topic,
						Partition: kafka.PartitionAny,
					},
					Key:   []byte(msg.ProductID),
					Value: payload,
				}, deliveryChan)

			if err != nil {
				slog.Error("Produce failed", slog.Any("error", err))
			}

			e := <-deliveryChan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				slog.Error("Delivery failed", slog.Any("error", m.TopicPartition.Error))
			} else {
				slog.Info("Delivered message ",
					slog.String("topic", *m.TopicPartition.Topic),
					slog.Int("partition", int(m.TopicPartition.Partition)),
					slog.Any("offset", m.TopicPartition.Offset))
			}
		}
	}
}
