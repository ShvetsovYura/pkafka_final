package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	clientservice "github.com/ShvetsovYura/pkafka_final/internal/client_service"
	dbclient "github.com/ShvetsovYura/pkafka_final/internal/db_client"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"gopkg.in/yaml.v2"
)

func main() {
	data, err := os.ReadFile("config.yaml")
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
	api := clientservice.NewClientApi(cfg.WebAPI.Listen, es)
	producer := clientservice.NewClientProducer(cfg.Topic, cfg.Producer)

	go api.Run(ctx, &wg, reqCh)
	go producer.Run(ctx, &wg, reqCh)
	wg.Wait()

}
