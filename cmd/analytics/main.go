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

func main() {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var cfg types.AndlyticsAppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
	hdfsClient, err := analytics.NewHDFSClient(cfg.HDFS)
	if err != nil {
		log.Fatal("not create hadoop client, %s", err)
	}
	consumer, err := analytics.NewAnalyticsConsumer("hadoop-topic", cfg.Consumer, hdfsClient)
	if err != nil {
		log.Fatal("not create analytics consumer, %s", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go consumer.Run(context.TODO(), &wg)
	// go analytics.RunCalc()
	go analytics.RunRecomendationProducer()
	wg.Wait()
}
