package worker

import (
	"context"

	"github.com/example/distributed-ingest/internal/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the Worker gRPC server interface.
type Service struct {
	api.UnimplementedWorkerServer
}

// NewService creates a new worker gRPC service handler.
func NewService() *Service {
	return &Service{}
}

// AssignTasks delivers task assignments to the worker node.
func (s *Service) AssignTasks(ctx context.Context, req *api.AssignTasksRequest) (*api.AssignTasksResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "AssignTasks is not implemented yet")
}

// Heartbeat records the health status of a worker.
func (s *Service) Heartbeat(ctx context.Context, req *api.HeartbeatRequest) (*api.HeartbeatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Heartbeat is not implemented yet")
}
