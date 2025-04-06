package registryclient

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

type SchemaRegistryClient struct {
	Serializer   *jsonschema.Serializer
	Deserializer *jsonschema.Deserializer
}

func NewSchemaRegistryClient() *SchemaRegistryClient {
	url := "http://0.0.0.0:8081"
	cfg := schemaregistry.NewConfig(url)
	client, err := schemaregistry.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create schema registry client: %s\n", err)
	}
	deser_config := jsonschema.NewDeserializerConfig()
	deser, err := jsonschema.NewDeserializer(client, serde.ValueSerde, deser_config)
	if err != nil {
		log.Fatalf("Failed to create deserializer: %s\n", err)
	}
	serializer_config := jsonschema.NewSerializerConfig()
	ser, err := jsonschema.NewSerializer(client, serde.ValueSerde, serializer_config)
	if err != nil {
		log.Fatalf("Failed to create serializer: %s\n", err)
	}
	return &SchemaRegistryClient{
		Serializer:   ser,
		Deserializer: deser,
	}
}
