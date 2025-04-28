package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"sync"

	blockservice "github.com/ShvetsovYura/pkafka_final/internal/block_service"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
)

func RunBlocker() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	blockCh := make(chan *types.BlockItem, 10)
	defer func() {
		close(blockCh)
		stop()

	}()

	cwd, _ := os.Getwd()

	blocker := blockservice.NewBlocker(

		[]string{"b1.kafka.local:9194", "b2.kafka.local:9294", "b3.kafka.local:9394"},
		types.BlockerTopics{
			BlockerTopic: "block",
			InTopic:      "products_in",
			OutTopic:     "products",
		}, types.ClientCert{
			CaCertPath:        filepath.Join(cwd, "secrets", "blocker", "ca.crt"),
			ClientCertPath:    filepath.Join(cwd, "secrets", "blocker", "client.pem"),
			ClientCertKeyPath: filepath.Join(cwd, "secrets", "blocker", "client.key"),
		}, types.Cred{
			Username: "kafka-internal",
			Password: "H6tFPQDI9Wnu",
		})
	blockerApi := blockservice.NewBlockerWebapi(":9091", blockCh)
	wg.Add(1)
	go blocker.Run(ctx, wg, blockCh)
	go blockerApi.Start(ctx, wg)
	wg.Wait()
}
