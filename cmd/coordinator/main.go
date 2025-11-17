package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/distributed-ingest/internal/api"
	"github.com/example/distributed-ingest/internal/coordinator"
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

	grpcServer := grpc.NewServer()
	api.RegisterCoordinatorServer(grpcServer, coordinator.NewService())

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
