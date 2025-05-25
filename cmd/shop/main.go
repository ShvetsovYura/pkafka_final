package main

import (
	"context"
	"flag"
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
	name := flag.String("config", "", "путь до конфигурационного файла")
	flag.Parse()
	data, err := os.ReadFile(*name)
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

	shopProducer := shopservice.NewShopProducer(cfg.Producer.Topic, cfg.Producer, sr)
	go shopProducer.Run(context.TODO(), &wg, productCh)
	go shopservice.StartApi(cfg.WebAPI.Listen, productCh)
	wg.Wait()
}
