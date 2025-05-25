package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"sync"

	blockservice "github.com/ShvetsovYura/pkafka_final/internal/block_service"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"gopkg.in/yaml.v2"
)

func main() {
	name := flag.String("config", "", "path to config file")
	flag.Parse()

	data, err := os.ReadFile(*name)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var cfg types.BlockerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	blockCh := make(chan *types.BlockItem, 10)
	defer func() {
		close(blockCh)
		stop()
	}()

	blocker := blockservice.NewBlocker(cfg.BootstrapServers, cfg.Topics, cfg.Certs, cfg.Credentials)

	blockerApi := blockservice.NewBlockerWebapi(cfg.WebAPI.Listen, blockCh)
	wg.Add(2)
	go blocker.RunEmmiter(ctx, wg, blockCh)
	go blocker.RunProcessor(ctx)
	go blockerApi.Start(ctx, wg)
	wg.Wait()
}
