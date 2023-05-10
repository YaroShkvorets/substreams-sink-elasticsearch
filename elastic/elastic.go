package elastic

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v8"
)

type Tables map[string]Fields
type Fields map[string]DatabaseType
type DatabaseType string

const (
	INTEGER   DatabaseType = "integer"
	DOUBLE    DatabaseType = "double"
	BOOLEAN   DatabaseType = "boolean"
	TIMESTAMP DatabaseType = "timestamp"
	NULL      DatabaseType = "null"
	DATE      DatabaseType = "date"
)

type Loader struct {
	client *elasticsearch.Client
	index  string
	tables Tables

	cursorCollectionName string
	entityCollectionName string

	logger *zap.Logger
}

func NewElasticSearch(address string, indexName string, logger *zap.Logger) (*Loader, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			address,
		},
	}
	es, _ := elasticsearch.NewClient(cfg)
	info, err := es.Info()
	if err != nil {
		return nil, err
	}
	logger.Info("elasticSearch client",
		zap.String("version1", elasticsearch.Version),
		zap.String("status", info.Status()))

	return &Loader{client: es, index: indexName, logger: logger}, nil
}

func (l *Loader) Ping(ctx context.Context) error {
	return nil
}

func (l *Loader) Save(ctx context.Context, collectionName string, id string, entity map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	entityBytes, _ := json.Marshal(entity)

	req := esapi.IndexRequest{
		Index:      l.index,
		DocumentID: id,
		Body:       strings.NewReader(string(entityBytes)),
		Refresh:    "true",
	}

	res, err := req.Do(ctx, l.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}

func (l *Loader) Update(ctx context.Context, collectionName string, id string, changes map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	changesBytes, _ := json.Marshal(changes)

	req := esapi.IndexRequest{
		Index:      l.index,
		DocumentID: id,
		Body:       strings.NewReader(string(changesBytes)),
		Refresh:    "true",
	}

	res, err := req.Do(ctx, l.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}

func (l *Loader) Delete(ctx context.Context, collectionName string, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req := esapi.DeleteRequest{
		Index:      l.index,
		DocumentID: id,
	}

	res, err := req.Do(ctx, l.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}
