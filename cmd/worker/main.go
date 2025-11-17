package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/distributed-ingest/internal/api"
	"github.com/example/distributed-ingest/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	addr := getEnv("WORKER_ADDRESS", ":50052")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	coordAddr := getEnv("COORDINATOR_ADDRESS", "localhost:50051")
	coordConn, err := grpc.DialContext(ctx, coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect to coordinator %s: %v", coordAddr, err)
	}
	defer coordConn.Close()

	workerID := getEnv("WORKER_ID", "worker-1")
	coordClient := worker.NewCoordinatorClient(api.NewCoordinatorClient(coordConn))
	dataSource := worker.NewStaticDataSource(os.Getenv("WORKER_DATA_BASE"))
	logic := worker.NewWorker(workerID, coordClient, nil, dataSource)

	grpcServer := grpc.NewServer()
	api.RegisterWorkerServer(grpcServer, worker.NewService(logic))

	go func() {
		<-ctx.Done()
		log.Println("shutting down worker server")
		grpcServer.GracefulStop()
	}()

	log.Printf("worker listening on %s", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("worker server stopped: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
