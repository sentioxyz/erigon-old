package mev

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"

	"github.com/ledgerwatch/erigon/eth"
	"github.com/ledgerwatch/erigon/mev-lib/api"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var grpcServer *grpc.Server
var shutdownCh = make(chan struct{})

func newExporter(w io.Writer) (trace.SpanExporter, error) {
	return stdouttrace.New(
		stdouttrace.WithWriter(w),
		// Use human-readable output.
		stdouttrace.WithPrettyPrint(),
		// Do not print timestamps for the demo.
		stdouttrace.WithoutTimestamps(),
	)
}

func MainInit() {
	if os.Getenv("OTEL_TRACE_OUTPUT") != "" {
		// Write telemetry data to a file.
		f, err := os.Create(os.Getenv("OTEL_TRACE_OUTPUT"))
		if err != nil {
			panic(err)
		}

		exp, err := newExporter(f)
		if err != nil {
			panic(err)
		}

		tp := trace.NewTracerProvider(trace.WithBatcher(exp))
		runtime.SetFinalizer(tp, func(tp *trace.TracerProvider) {
			if err := tp.Shutdown(context.Background()); err != nil {
				panic(err)
			}
			f.Close()
		})
		otel.SetTracerProvider(tp)
		log.Info("Tracing enabled", "output", os.Getenv("OTEL_TRACE_OUTPUT"))
	}
}

func LaunchInfraServer(ctx context.Context,
	configPath string,
	eth *eth.Ethereum,
	backend []rpc.API) error {
	config, err := loadMEVconfig(configPath)
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	maxRecvMsgSize := 128 * 1024 * 1024
	if config.Server.GrpcMaxRecvMsgSz > 0 {
		maxRecvMsgSize = config.Server.GrpcMaxRecvMsgSz
	}
	opts = append(opts, grpc.MaxRecvMsgSize(maxRecvMsgSize))
	grpcServer = grpc.NewServer(opts...)
	infra := NewInfraServer(eth, backend)
	api.RegisterMEVInfraServer(grpcServer, infra)

	wg, ctx := errgroup.WithContext(ctx)
	if config.Server.TcpPort > 0 {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Server.TcpPort))
		if err != nil {
			return fmt.Errorf("failed to listen: %w", err)
		}
		wg.Go(func() error {
			return grpcServer.Serve(lis)
		})
	}
	if config.Server.DomainSocketPath != "" {
		os.Remove(config.Server.DomainSocketPath)
		lis, err := net.Listen("unix", config.Server.DomainSocketPath)
		if err != nil {
			return fmt.Errorf("failed to listen: %w", err)
		}
		wg.Go(func() error {
			return grpcServer.Serve(lis)
		})
	}
	log.Info("MEV infra server started",
		"tcp", config.Server.TcpPort,
		"socket", config.Server.DomainSocketPath)
	go func() {
		if err := wg.Wait(); err != nil {
			log.Error("MEV infra server failed", "err", err)
		}
		close(shutdownCh)
	}()
	return nil
}

func WaitForShutdown() {
	if grpcServer != nil {
		grpcServer.Stop()
	}
	<-shutdownCh
}
