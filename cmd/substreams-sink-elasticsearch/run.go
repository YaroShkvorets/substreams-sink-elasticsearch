package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/yaroshkvorets/substreams-sink-elasticsearch/elastic"
	"github.com/yaroshkvorets/substreams-sink-elasticsearch/sinker"
	"go.uber.org/zap"
)

var sinkRunCmd = Command(sinkRunE,
	"run <dsn> <database_name> <endpoint> <manifest> <module> [<start>:<stop>]",
	"Runs ElasticSearch sink process",
	RangeArgs(5, 6),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
	}),
	OnCommandErrorLogAndExit(zlog),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	elasticDSN := args[0]
	databaseName := args[1]
	endpoint := args[2]
	manifestPath := args[3]
	outputModuleName := args[4]
	blockRange := ""
	if len(args) > 5 {
		blockRange = args[5]
	}

	elasticLoader, err := elastic.NewElasticSearch(elasticDSN, databaseName, zlog)
	if err != nil {
		return fmt.Errorf("unable to create elastic loader: %w", err)
	}

	sink, err := sink.NewFromViper(
		cmd,
		"sf.substreams.sink.database.v1.DatabaseChanges",
		endpoint, manifestPath, outputModuleName, blockRange,
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}

	elasticSinker, err := sinker.New(sink, elasticLoader, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup elastic sinker: %w", err)
	}

	elasticSinker.OnTerminating(app.Shutdown)
	app.OnTerminating(func(err error) {
		elasticSinker.Shutdown(err)
	})

	go func() {
		elasticSinker.Run(ctx)
	}()

	zlog.Info("ready, waiting for signal to quit")

	signalHandler, isSignaled, _ := cli.SetupSignalHandler(0*time.Second, zlog)
	select {
	case <-signalHandler:
		go app.Shutdown(nil)
		break
	case <-app.Terminating():
		zlog.Info("run terminating", zap.Bool("from_signal", isSignaled.Load()), zap.Bool("with_error", app.Err() != nil))
		break
	}

	zlog.Info("waiting for run termination")
	select {
	case <-app.Terminated():
	case <-time.After(30 * time.Second):
		zlog.Warn("application did not terminate within 30s")
	}

	if err := app.Err(); err != nil {
		return err
	}

	zlog.Info("run terminated gracefully")
	return nil
}
