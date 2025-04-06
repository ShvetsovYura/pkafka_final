package main

import (
	"sync"

	analytics "github.com/ShvetsovYura/pkafka_final/internal/analytics"
)

func main() {
	var wg = sync.WaitGroup{}
	wg.Add(1)
	go analytics.RunSyncConsumer()
	go analytics.RunCalc()
	go analytics.RunRecomendationProducer()
	wg.Wait()
}
