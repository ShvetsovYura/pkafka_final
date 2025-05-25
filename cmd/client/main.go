package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/redis/go-redis/v9"

	"syscall"

	clientservice "github.com/ShvetsovYura/pkafka_final/internal/client_service"
	dbclient "github.com/ShvetsovYura/pkafka_final/internal/db_client"
	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"gopkg.in/yaml.v2"
)

func main() {
	config_path := flag.String("config", "", "path to config file")
	flag.Parse()
	data, err := os.ReadFile(*config_path)
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

func NewRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "redis:6379", // Имя сервиса из docker-compose
		Password: "",           // Пароль, если установлен
		DB:       0,            // Номер базы данных
	})
}
