package worker

import (
	"context"

	"github.com/example/distributed-ingest/internal/api"
)

// CoordinatorReporter exposes the subset of coordinator RPCs the worker relies on.
type CoordinatorReporter interface {
	ReportManifest(ctx context.Context, jobID string, table *api.TableIdentifier, snapshotID int64, workerID string, manifestPaths []string) error
}

// coordinatorClient wraps the generated gRPC client to provide helper methods that
// accept domain-specific parameters instead of full protobuf requests.
type coordinatorClient struct {
	client api.CoordinatorClient
}

// NewCoordinatorClient builds a helper around the provided CoordinatorClient implementation.
func NewCoordinatorClient(client api.CoordinatorClient) CoordinatorReporter {
	return &coordinatorClient{client: client}
}

// ReportManifest notifies the coordinator that the worker has finished producing the
// supplied manifest files for the given job and snapshot.
func (c *coordinatorClient) ReportManifest(ctx context.Context, jobID string, table *api.TableIdentifier, snapshotID int64, workerID string, manifestPaths []string) error {
	if c == nil || c.client == nil {
		return nil
	}

	_, err := c.client.ReportManifest(ctx, &api.ReportManifestRequest{
		JobId:         jobID,
		Table:         table,
		SnapshotId:    snapshotID,
		WorkerId:      workerID,
		ManifestPaths: manifestPaths,
	})
	return err
}
