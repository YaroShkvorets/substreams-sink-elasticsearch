package main

import (
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var zlog, tracer = logging.RootLogger("sink-elasticsearch", "github.com/yaroshkvorets/substreams-sink-elasticsearch/cmd/substreams-sink-elasticsearch")

func init() {
	cli.SetLogger(zlog, tracer)

	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))
}
