package coordinator

import (
	"context"
	"errors"

	"github.com/example/distributed-ingest/internal/api"
	"github.com/example/distributed-ingest/internal/iceberg"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the Coordinator gRPC server interface.
type Service struct {
	api.UnimplementedCoordinatorServer
	jobs *JobManager
}

// NewService builds a new coordinator service instance.
func NewService(jobManager *JobManager) *Service {
	return &Service{jobs: jobManager}
}

// StartJob initializes a distributed ingest job.
func (s *Service) StartJob(ctx context.Context, req *api.StartJobRequest) (*api.StartJobResponse, error) {
	if s.jobs == nil {
		return nil, status.Error(codes.FailedPrecondition, "job manager is not configured")
	}
	if req == nil || req.Table == nil {
		return nil, status.Error(codes.InvalidArgument, "table identifier is required")
	}

	props := make(map[string]string)
	if req.Requester != "" {
		props["requested_by"] = req.Requester
	}

	job, err := s.jobs.CreateJob(ctx, fromProtoTable(req.Table), props)
	if err != nil {
		if errors.Is(err, errTableClientMissing) {
			return nil, status.Error(codes.FailedPrecondition, "iceberg table client is not initialized")
		}
		return nil, status.Errorf(codes.Internal, "create job: %v", err)
	}

	return &api.StartJobResponse{
		JobId:    job.ID,
		Table:    req.Table,
		Snapshot: toProtoSnapshot(job.DistributedSnapshot),
		Tasks:    []*api.Task{},
	}, nil
}

// CommitJob finalizes a distributed snapshot commit.
func (s *Service) CommitJob(ctx context.Context, req *api.CommitJobRequest) (*api.CommitJobResponse, error) {
	if s.jobs == nil {
		return nil, status.Error(codes.FailedPrecondition, "job manager is not configured")
	}
	if req == nil || req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	job, ok := s.jobs.GetJob(req.JobId)
	if !ok {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	if len(job.Manifests) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "no manifests have been reported")
	}

	if err := s.jobs.MarkCommitting(req.JobId); err != nil {
		return nil, status.Errorf(codes.Internal, "mark committing: %v", err)
	}
	if err := s.jobs.MarkCompleted(req.JobId); err != nil {
		return nil, status.Errorf(codes.Internal, "mark completed: %v", err)
	}

	return &api.CommitJobResponse{
		JobId:     req.JobId,
		Committed: true,
		Message:   "commit deferred to future implementation",
	}, nil
}

// ReportManifest registers finished manifest files from workers.
func (s *Service) ReportManifest(ctx context.Context, req *api.ReportManifestRequest) (*api.ReportManifestResponse, error) {
	if s.jobs == nil {
		return nil, status.Error(codes.FailedPrecondition, "job manager is not configured")
	}
	if req == nil || req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if len(req.ManifestPaths) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one manifest path is required")
	}

	for _, path := range req.ManifestPaths {
		if path == "" {
			continue
		}
		if err := s.jobs.AddManifest(req.JobId, iceberg.ManifestInfo{Path: path}); err != nil {
			if errors.Is(err, ErrJobNotFound) {
				return nil, status.Error(codes.NotFound, "job not found")
			}
			return nil, status.Errorf(codes.Internal, "add manifest: %v", err)
		}
	}

	return &api.ReportManifestResponse{Accepted: true}, nil
}

func fromProtoTable(tbl *api.TableIdentifier) iceberg.TableIdentifier {
	if tbl == nil {
		return iceberg.TableIdentifier{}
	}
	return iceberg.TableIdentifier{
		Catalog:   tbl.Catalog,
		Namespace: tbl.Namespace,
		Table:     tbl.Table,
	}
}

func toProtoSnapshot(ds *iceberg.DistributedSnapshot) *api.DistributedSnapshot {
	if ds == nil {
		return nil
	}
	result := &api.DistributedSnapshot{
		SnapshotId: ds.SnapshotID,
		CommitUuid: ds.CommitUUID,
		Properties: mapsClone(ds.Properties),
	}
	if ds.ParentSnapshotID != nil {
		result.ParentSnapshotId = *ds.ParentSnapshotID
	}
	return result
}

func mapsClone(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}
