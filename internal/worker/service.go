package worker

import (
	"context"
	"fmt"

	"github.com/example/distributed-ingest/internal/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the Worker gRPC server interface.
type Service struct {
	api.UnimplementedWorkerServer
	worker *Worker
}

// NewService creates a new worker gRPC service handler.
func NewService(worker *Worker) *Service {
	return &Service{worker: worker}
}

// AssignTasks delivers task assignments to the worker node.
func (s *Service) AssignTasks(ctx context.Context, req *api.AssignTasksRequest) (*api.AssignTasksResponse, error) {
	if s.worker == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker logic is not configured")
	}
	resp, err := s.worker.ProcessAssignments(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "process assignments: %v", err)
	}
	return resp, nil
}

// Heartbeat records the health status of a worker.
func (s *Service) Heartbeat(ctx context.Context, req *api.HeartbeatRequest) (*api.HeartbeatResponse, error) {
	if s.worker == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker logic is not configured")
	}
	message := "worker healthy"
	if req != nil && req.Status != "" {
		message = fmt.Sprintf("worker healthy (%s)", req.Status)
	}
	return &api.HeartbeatResponse{Healthy: true, Instructions: message}, nil
}
