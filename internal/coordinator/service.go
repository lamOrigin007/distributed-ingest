package coordinator

import (
	"context"

	"github.com/example/distributed-ingest/internal/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the Coordinator gRPC server interface.
type Service struct {
	api.UnimplementedCoordinatorServer
}

// NewService builds a new coordinator service instance.
func NewService() *Service {
	return &Service{}
}

// StartJob initializes a distributed ingest job.
func (s *Service) StartJob(ctx context.Context, req *api.StartJobRequest) (*api.StartJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "StartJob is not implemented yet")
}

// CommitJob finalizes a distributed snapshot commit.
func (s *Service) CommitJob(ctx context.Context, req *api.CommitJobRequest) (*api.CommitJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "CommitJob is not implemented yet")
}

// ReportManifest registers finished manifest files from workers.
func (s *Service) ReportManifest(ctx context.Context, req *api.ReportManifestRequest) (*api.ReportManifestResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "ReportManifest is not implemented yet")
}
