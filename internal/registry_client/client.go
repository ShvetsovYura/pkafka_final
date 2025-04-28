package registryclient

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

type SchemaRegistryClient struct {
	Serializer   *jsonschema.Serializer
	Deserializer *jsonschema.Deserializer
}

func NewSchemaRegistryClient(url string) (*SchemaRegistryClient, error) {
	cfg := schemaregistry.NewConfig(url)

	client, err := schemaregistry.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry client: %w", err)
	}

	jsonDeserConf := jsonschema.NewDeserializerConfig()
	jsonDeserConf.EnableValidation = true
	deser, err := jsonschema.NewDeserializer(client, serde.ValueSerde, jsonDeserConf)
	if err != nil {
		return nil, fmt.Errorf("failed to create deserializer: %w", err)
	}
	jsonSerConf := jsonschema.NewSerializerConfig()
	jsonSerConf.AutoRegisterSchemas = false
	jsonSerConf.EnableValidation = true

	ser, err := jsonschema.NewSerializer(client, serde.ValueSerde, jsonSerConf)
	if err != nil {
		return nil, fmt.Errorf("failed to create serializer: %w", err)
	}
	return &SchemaRegistryClient{
		Serializer:   ser,
		Deserializer: deser,
	}, nil
}
