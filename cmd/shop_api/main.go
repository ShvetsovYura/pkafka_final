package main

import (
	"sync"

	"github.com/ShvetsovYura/pkafka_final/internal/models"
	registryclient "github.com/ShvetsovYura/pkafka_final/internal/registry_client"
	shopservice "github.com/ShvetsovYura/pkafka_final/internal/shop_service"
)

func main() {
	messagesCh := make(chan models.Product, 100)
	var wg = sync.WaitGroup{}
	wg.Add(1)
	sr := registryclient.NewSchemaRegistryClient()
	go shopservice.StartProducer("products_in", messagesCh, sr)
	go shopservice.StartApi(messagesCh)
	wg.Wait()
}
