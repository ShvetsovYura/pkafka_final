package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	clientservice "github.com/ShvetsovYura/pkafka_final/internal/client_service"
	dbclient "github.com/ShvetsovYura/pkafka_final/internal/db_client"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"gopkg.in/yaml.v2"
)

func main() {
	config_path := flag.String("config", "", "path to config file")
	flag.Parse()
	data, err := os.ReadFile(*config_path)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var cfg types.ClientAppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	reqCh := make(chan types.UserRequest, cfg.Common.QueueSize)
	var wg sync.WaitGroup

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer func() {
		stop()
		close(reqCh)
	}()
	es, err := dbclient.NewElasticClient(cfg.ElasticClientConfig.Addr, cfg.ElasticClientConfig.IndexName)
	if err != nil {
		log.Fatal(err)
	}

	wg.Add(2)
	recGr := goka.Group("recommendations")

	caCert, err := os.ReadFile("/home/yura/Documents/pkafka_final/cmd/blocker/ca.crt")
	if err != nil {
		panic(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsCert, err := tls.LoadX509KeyPair("/home/yura/Documents/pkafka_final/cmd/blocker/client.pem", "/home/yura/Documents/pkafka_final/cmd/blocker/client.key")
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
	config.Net.SASL.User = "kafka-internal"
	config.Net.SASL.Password = "H6tFPQDI9Wnu"

	recomendView, err := goka.NewView([]string{"br3.kafka.local:9794"},
		goka.GroupTable(recGr),
		&codec.String{},
		goka.WithViewConsumerSaramaBuilder(goka.SaramaConsumerBuilderWithConfig(config)),
		goka.WithViewTopicManagerBuilder(goka.TopicManagerBuilderWithConfig(config, goka.NewTopicManagerConfig())),
	)
	if err != nil {
		panic(err)
	}

	err = recomendView.Run(context.TODO())
	if err != nil {
		panic(err)
	}
	val, err := recomendView.Get("12345678")
	if err != nil {
		log.Fatal(err)
	}
	api := clientservice.NewClientApi(cfg.WebAPI.Listen, es)
	producer := clientservice.NewClientProducer(cfg.Topic, cfg.Producer)

	go api.Run(ctx, &wg, reqCh)
	go producer.Run(ctx, &wg, reqCh)
	wg.Wait()

}
