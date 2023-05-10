package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/esapi"
	sink "github.com/streamingfast/substreams-sink"
)

var ErrCursorNotFound = errors.New("cursor not found")
var CursorPrefix = "_cursor-"

type esData struct {
	Data string `json:"data"`
}

type cursorDocument struct {
	Id       string `json:"id"`
	Cursor   string `json:"cursor"`
	BlockNum uint64 `json:"block_num"`
	BlockID  string `json:"block_id"`
}

func (l *Loader) GetCursor(ctx context.Context, outputModuleHash string) (*sink.Cursor, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req := esapi.GetRequest{
		Index:      l.index,
		DocumentID: CursorPrefix + outputModuleHash,
	}

	res, err := req.Do(ctx, l.client)
	if err != nil {
		return nil, fmt.Errorf("getting cursor %q:  %w", outputModuleHash, err)
	}
	defer res.Body.Close()
	if res.StatusCode == 404 {
		return nil, ErrCursorNotFound
	}

	var doc map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decoding cursor for module %q:  %w", outputModuleHash, err)
	}

	c := doc["_source"].(map[string]interface{})["cursor"].(string)
	return sink.NewCursor(c)
}

func (l *Loader) WriteCursor(ctx context.Context, moduleHash string, c *sink.Cursor) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cursor := cursorDocument{Id: moduleHash, Cursor: c.String(), BlockNum: c.Block().Num(), BlockID: c.Block().ID()}

	cursorBytes, _ := json.Marshal(cursor)

	req := esapi.IndexRequest{
		Index:      l.index,
		DocumentID: CursorPrefix + moduleHash,
		Body:       strings.NewReader(string(cursorBytes)),
		Refresh:    "true",
	}

	res, err := req.Do(ctx, l.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}
