package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/distributed-ingest/internal/api"
	"github.com/example/distributed-ingest/internal/coordinator"
	"github.com/example/distributed-ingest/internal/iceberg"
	"google.golang.org/grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	addr := getEnv("COORDINATOR_ADDRESS", ":50051")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	client, err := newTableClient(ctx)
	if err != nil {
		log.Fatalf("failed to initialize iceberg client: %v", err)
	}
	jobManager := coordinator.NewJobManager(client)
	jobManager.StartTimeoutWatcher(ctx, 30*time.Second)
	grpcServer := grpc.NewServer()
	api.RegisterCoordinatorServer(grpcServer, coordinator.NewService(jobManager))

	go func() {
		<-ctx.Done()
		log.Println("shutting down coordinator server")
		grpcServer.GracefulStop()
	}()

	log.Printf("coordinator listening on %s", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("coordinator server stopped: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func newTableClient(ctx context.Context) (iceberg.TableClient, error) {
	name := getEnv("ICEBERG_CATALOG_NAME", "default")
	typeName := getEnv("ICEBERG_CATALOG_TYPE", "rest")
	props := map[string]string{"type": typeName}
	if uri := os.Getenv("ICEBERG_CATALOG_URI"); uri != "" {
		props["uri"] = uri
	}
	if warehouse := os.Getenv("ICEBERG_WAREHOUSE"); warehouse != "" {
		props["warehouse"] = warehouse
	}

	return iceberg.NewClient(ctx, iceberg.Config{
		DefaultCatalog: name,
		Catalogs: []iceberg.CatalogConfig{{
			Name:       name,
			Properties: props,
		}},
	})
}
