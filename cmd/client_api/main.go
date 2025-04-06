package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	dbclient "github.com/ShvetsovYura/pkafka_final/internal/db_client"
	"github.com/go-chi/chi/v5"
)

func main() {
	r := make(chan string, 100)
	es, _ := dbclient.NewElasticClient("http://localhost:9200")
	c := NewClientApi(r, es)
	c.Start()

}

type DbClient interface {
	SearchByName(name string) ([]any, error)
}

type ClientApi struct {
	requests chan string
	dbClient DbClient
}

func NewClientApi(req chan string, dbClient DbClient) *ClientApi {
	return &ClientApi{
		requests: req,
		dbClient: dbClient,
	}
}

func (s *ClientApi) Start() {
	r := chi.NewRouter()
	r.Get("/search", func(w http.ResponseWriter, r *http.Request) {
		u, err := url.Parse(r.RequestURI)
		if err != nil {
			fmt.Println("ugi")
		}
		qp, _ := url.ParseQuery(u.RawQuery)
		val := qp.Get("name")
		// c.SearchByName("умные часы")
		searchResult, _ := s.dbClient.SearchByName(val)
		prettySource, _ := json.Marshal(searchResult)
		// fmt.Println(string(prettySource))
		w.Write(prettySource)
	})
	r.Get("/recomendations", func(w http.ResponseWriter, r *http.Request) {
		r.Header.Get("client_id")
	})
	http.ListenAndServe(":9081", r)
}

// func StartProducer(ctx context.Context, wg *sync.WaitGroup, topic string, cfg map[string]any, schema_config *schemaregistry.Config, messagesCh <-chan Message) {
// 	var cfgMap kafka.ConfigMap = kafka.ConfigMap{}
// 	deliveryChan := make(chan kafka.Event)

// 	for k, v := range cfg {
// 		cfgMap.SetKey(k, v)
// 	}
// 	p, err := kafka.NewProducer(&cfgMap)
// 	if err != nil {
// 		log.Fatalf("Failed to create producer: %s\n", err)
// 	}

// 	defer func() {
// 		p.Close()
// 		close(deliveryChan)
// 		wg.Done()
// 	}()

// 	logger.Info("Created Producer", slog.Any("producer", p))
// 	client, err := schemaregistry.NewClient(schema_config)
// 	if err != nil {
// 		log.Fatalf("Failed to create schema registry client: %s\n", err)
// 	}

// 	serializer_config := jsonschema.NewSerializerConfig()
// 	ser, err := jsonschema.NewSerializer(client, serde.ValueSerde, serializer_config)
// 	if err != nil {
// 		log.Fatalf("Failed to create serializer: %s\n", err)
// 	}
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			logger.Info("Получен сигнал выхода, остановка продьюсера...")
// 			return
// 		case msg := <-messagesCh:
// 			payload, err := ser.Serialize(topic, &msg)
// 			if err != nil {
// 				logger.Error("Failed to serialize payload", slog.Any("error", err))
// 			}

// 			err = p.Produce(&kafka.Message{
// 				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
// 				Key:            []byte(msg.Uuid),
// 				Value:          payload,
// 			}, deliveryChan)
// 			if err != nil {
// 				logger.Error("Produce failed", slog.Any("error", err))
// 			}

// 			e := <-deliveryChan
// 			m := e.(*kafka.Message)

// 			if m.TopicPartition.Error != nil {
// 				logger.Error("Delivery failed", slog.Any("error", m.TopicPartition.Error))
// 			} else {
// 				logger.Info("Delivered message ",
// 					slog.String("topic", *m.TopicPartition.Topic),
// 					slog.Int("partition", int(m.TopicPartition.Partition)),
// 					slog.Any("offset", m.TopicPartition.Offset))
// 			}
// 		}

// 	}

// }
