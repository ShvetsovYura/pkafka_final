package blockservice

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

type Blocker struct {
	brokers            []string
	blockerStream      goka.Stream
	blockerGroup       goka.Group
	blockCh            chan *BlockItem
	productInStream    goka.Stream
	productOutStream   goka.Stream
	productFilterGroup goka.Group
}

func NewBlocker(topic string, brokers []string, blockCh chan *BlockItem, productInTopic string, productOutTopic string) *Blocker {
	return &Blocker{
		brokers:            brokers,
		blockerStream:      goka.Stream(topic),
		blockerGroup:       goka.Group(topic),
		productInStream:    goka.Stream(productInTopic),
		productOutStream:   goka.Stream(productOutTopic),
		productFilterGroup: "product_filter",
	}
}

func (b *Blocker) Run(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go b.startEmmiter()
	go b.startProcessor()
	go b.startFilter()

}

func (b *Blocker) startEmmiter() {
	caCert, err := os.ReadFile("ca-cert.pem")
	if err != nil {
		panic(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// 2. Загрузка клиентского сертификата и ключа (если нужно)
	cert, err := tls.LoadX509KeyPair("client-cert-signed.pem", "client-key.pem")
	if err != nil {
		panic(err)
	}
	// 3. Настройка TLS
	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,              // CA для проверки сервера
		Certificates: []tls.Certificate{cert}, // Клиентский сертификат (для mTLS)
		MinVersion:   tls.VersionTLS11,        // Минимальная версия TLS
	}
	c := goka.DefaultConfig()
	c.Net.TLS.Enable = true
	c.Net.TLS.Config = tlsConfig

	c.Net.SASL.Enable = true
	c.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	c.Net.SASL.User = "admin"
	c.Net.SASL.Password = "admin-secret"
	// c.Net.TLS.Config.Certificates =
	e, err := goka.NewEmitter(b.brokers, b.blockerStream, new(codec.String), goka.WithEmitterProducerBuilder(goka.ProducerBuilderWithConfig(c)))
	if err != nil {
		log.Fatal(err)
	}
	defer e.Finish()
	item := <-b.blockCh
	e.EmitSync(item.productId, item.status)
}

func (b *Blocker) startProcessor() {
	group := goka.DefineGroup(b.blockerGroup,
		goka.Input(b.blockerStream, new(codec.String), func(ctx goka.Context, msg any) {
			// k := ctx.Key()
			ctx.SetValue(msg)
		}),
		goka.Persist(new(codec.String)))
	p, err := goka.NewProcessor(b.brokers, group)
	if err != nil {
		log.Fatal(err)
	}
	p.Run(context.TODO())
}

func (b *Blocker) startFilter() {
	group := goka.DefineGroup(b.productFilterGroup,
		goka.Input(b.productInStream, new(codec.String), func(ctx goka.Context, msg any) {
			v := ctx.Join(goka.Table(b.blockerGroup))
			if v != nil && v.(string) == "lock" {
				println("locked")
				return

			}
			ctx.Emit(b.productOutStream, ctx.Key(), msg)
		}),
		goka.Output(b.productOutStream, new(codec.String)),
		goka.Join(goka.Table(b.blockerGroup), new(codec.String)))

	p, err := goka.NewProcessor(b.brokers, group)
	if err != nil {
		log.Fatal(err)
	}

	e := p.Run(context.TODO())
	if e != nil {
		log.Fatal(e)
	}
}
