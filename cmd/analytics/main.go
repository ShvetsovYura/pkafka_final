package main

import (
	"context"
	"log"
	"os"
	"sync"

	analytics "github.com/ShvetsovYura/pkafka_final/internal/analytics"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"gopkg.in/yaml.v2"
)

const QUEUE_SIZE = 100

func main() {
	data, err := os.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var cfg types.AndlyticsAppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
	productReqCh := make(chan types.UserRequest, QUEUE_SIZE)
	analyticCh := make(chan types.UserRequest)

	hdfsClient, err := analytics.NewHDFSClient(cfg.HDFS, productReqCh)
	if err != nil {
		log.Fatal("not create hadoop client, %s", err)
	}
	consumer, err := analytics.NewAnalyticsConsumer("hadoop-topic", cfg.Consumer, hdfsClient)
	if err != nil {
		log.Fatal("not create analytics consumer, %s", err)
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(1)
	go consumer.Run(ctx, &wg)
	go webapi.Run(ctx, &wg)
	go analytics.Run(ctx, &wg)
	go analytics.RunRecomendationProducer(ctx)
	wg.Wait()
}
