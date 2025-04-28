package main

import (
	"context"
	"log"
	"os/signal"
	"sync"
	"syscall"

	clientservice "github.com/ShvetsovYura/pkafka_final/internal/client_service"
	dbclient "github.com/ShvetsovYura/pkafka_final/internal/db_client"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
)

func main() {
	reqCh := make(chan types.UserRequest, 100)
	var wg sync.WaitGroup
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer func() {
		stop()
		close(reqCh)
	}()
	es, err := dbclient.NewElasticClient("http://localhost:9200", "products")
	if err != nil {
		log.Fatal(err)
	}
	wg.Add(2)
	api := clientservice.NewClientApi(":9092", es)
	// producer := clientservice.NewClientProducer("client_requests", types.ProducerConfig{})

	go api.Run(ctx, &wg, reqCh)
	// go producer.Run(ctx, &wg, reqCh)
	wg.Wait()

}
