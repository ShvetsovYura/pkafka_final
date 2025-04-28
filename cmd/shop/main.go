package main

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/ShvetsovYura/pkafka_final/internal/models"
	registryclient "github.com/ShvetsovYura/pkafka_final/internal/registry_client"
	shopservice "github.com/ShvetsovYura/pkafka_final/internal/shop_service"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"gopkg.in/yaml.v2"
)

func main() {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var cfg types.ShopConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
	productCh := make(chan models.Product, cfg.Common.QueueSize)
	var wg sync.WaitGroup
	wg.Add(1)
	sr, err := registryclient.NewSchemaRegistryClient(cfg.SchemaRegistry.URL)
	if err != nil {
		log.Fatal(err)
	}
	// cwd, _ := os.Getwd()
	shopProducer := shopservice.NewShopProducer(cfg.Producer.Topic, cfg.Producer, sr)
	go shopProducer.Run(context.TODO(), &wg, productCh)
	go shopservice.StartApi(cfg.WebAPI.Listen, productCh)
	wg.Wait()
}
