package blockservice

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"log/slog"
	"os"
	"sync"

	"github.com/IBM/sarama"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

type Blocker struct {
	brokers            []string
	blockerStream      goka.Stream
	blockerGroup       goka.Group
	productInStream    goka.Stream
	productOutStream   goka.Stream
	productFilterGroup goka.Group
	connectionConfig   *sarama.Config
	// productCodec       productCodec
}

// type productCodec struct {
// 	srClient  *registryclient.SchemaRegistryClient
// 	topicName string
// }

// func (p *productCodec) Encode(value any) ([]byte, error) {
// 	return p.srClient.Serializer.Serialize(p.topicName, value)
// }
// func (p *productCodec) Decode(data []byte) (any, error) {
// 	// КОСТЫЛЬ!!! Я ХЗ КАК СЮДА ПРИКРУТИТЬ SCHEMA REGISTRY - ВЕСЬ мозг изломал
// 	js := data[5:] // срезаем 5 байт, которые идентифицируют схему
// 	var prod models.Product
// 	err := json.Unmarshal(js, &prod)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// res, err := p.srClient.Deserializer.Deserialize(p.topicName, data)) <- не работает чертова бабина
// 	return prod, err
// }

func NewBlocker(brokers []string, topics types.BlockerTopics, cert types.Certs, user types.Cred) *Blocker {

	caCert, err := os.ReadFile(cert.CaCertLocation)
	if err != nil {
		panic(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsCert, err := tls.LoadX509KeyPair(cert.CertLocation, cert.CertKeyLocation)
	if err != nil {
		panic(err)
	}

	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	config := goka.DefaultConfig()
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig

	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	config.Net.SASL.User = user.Username
	config.Net.SASL.Password = user.Password

	return &Blocker{
		brokers:            brokers,
		blockerStream:      goka.Stream(topics.BlockerTopic),
		blockerGroup:       goka.Group(topics.BlockerTopic),
		productInStream:    goka.Stream(topics.InTopic),
		productOutStream:   goka.Stream(topics.OutTopic),
		productFilterGroup: "product_filter",
		connectionConfig:   config,
	}
}

func (b *Blocker) RunEmmiter(ctx context.Context, wg *sync.WaitGroup, blockCh chan *types.BlockItem) {

	e, err := goka.NewEmitter(b.brokers, b.blockerStream, new(codec.String),
		goka.WithEmitterProducerBuilder(goka.ProducerBuilderWithConfig(b.connectionConfig)),
	)
	if err != nil {
		log.Fatal(err)
	}
	slog.Info("blocker emmiter started")
	defer e.Finish()
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		case item := <-blockCh:
			slog.Info("incoming blockCh to block/unblock", slog.Any("item", item))
			e.EmitSync(item.ProductId, item.Status)
		}
	}
}

func (b *Blocker) RunProcessor(ctx context.Context) {
	group := goka.DefineGroup(b.blockerGroup,
		goka.Input(b.blockerStream, new(codec.String), func(ctx goka.Context, msg any) {
			// k := ctx.Key()
			ctx.SetValue(msg)
		}),
		goka.Persist(new(codec.String)))

	p, err := goka.NewProcessor(b.brokers, group,
		goka.WithConsumerSaramaBuilder(goka.SaramaConsumerBuilderWithConfig(b.connectionConfig)),
		goka.WithConsumerGroupBuilder(goka.ConsumerGroupBuilderWithConfig(b.connectionConfig)),
		goka.WithProducerBuilder(goka.ProducerBuilderWithConfig(b.connectionConfig)),
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithConfig(b.connectionConfig, goka.NewTopicManagerConfig())),
	)
	if err != nil {
		log.Fatal(err)
	}
	err = p.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func (b *Blocker) RunFilter(ctx context.Context) {
	group := goka.DefineGroup(b.productFilterGroup,
		goka.Input(b.productInStream, new(codec.String), func(ctx goka.Context, msg any) {
			v := ctx.Join(goka.Table(b.blockerGroup))

			if v != nil && v.(string) == "blocked" {
				slog.Info("product blocked", slog.String("productID", ctx.Key()))
				return
			}
			ctx.Emit(b.productOutStream, ctx.Key(), msg)
		}),
		goka.Output(b.productOutStream, new(codec.String)),
		goka.Join(goka.Table(b.blockerGroup), new(codec.String)))

	p, err := goka.NewProcessor(b.brokers, group,
		goka.WithConsumerSaramaBuilder(goka.SaramaConsumerBuilderWithConfig(b.connectionConfig)),
		goka.WithConsumerGroupBuilder(goka.ConsumerGroupBuilderWithConfig(b.connectionConfig)),
		goka.WithProducerBuilder(goka.ProducerBuilderWithConfig(b.connectionConfig)),
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithConfig(b.connectionConfig, goka.NewTopicManagerConfig())),
	)
	if err != nil {
		log.Fatal(err)
	}

	e := p.Run(ctx)
	if e != nil {
		log.Fatal(e)
	}
}
