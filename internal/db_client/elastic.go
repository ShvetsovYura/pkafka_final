package ElasticClient

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticClient struct {
	indexName string
	es        *elasticsearch.Client
}

func NewElasticClient(conStr string, indexName string) (*ElasticClient, error) {

	cfg := elasticsearch.Config{
		Addresses: []string{conStr},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Elasticsearch: %w", err)
	}

	res, err := es.Info()
	if err != nil {
		return nil, fmt.Errorf("error getting Elasticsearch info: %w", err)
	}
	defer res.Body.Close()

	return &ElasticClient{
		es:        es,
		indexName: indexName,
	}, nil
}

func (c *ElasticClient) SearchByName(name string) ([]any, error) {
	var buf bytes.Buffer
	query := map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				"name": name,
			},
		},
	}

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %w", err)
	}

	res, err := c.es.Search(
		c.es.Search.WithIndex(c.indexName),
		c.es.Search.WithBody(&buf),
		c.es.Search.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("error searching: %w", err)
	}
	defer res.Body.Close()

	// Чтение и парсинг результата
	var result map[string]any
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error parsing response: %w", err)
	}

	searchRes := make([]any, 0)
	for _, hit := range result["hits"].(map[string]any)["hits"].([]any) {
		source := hit.(map[string]any)["_source"]
		searchRes = append(searchRes, source)

	}
	return searchRes, nil
}
