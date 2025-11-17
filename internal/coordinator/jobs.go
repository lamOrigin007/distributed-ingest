package coordinator

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"

	"github.com/example/distributed-ingest/internal/iceberg"
)

// JobStatus represents a coarse grained lifecycle state for ingest jobs.
type JobStatus string

const (
	JobStatusPending    JobStatus = "PENDING"
	JobStatusRunning    JobStatus = "RUNNING"
	JobStatusCommitting JobStatus = "COMMITTING"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
)

// IngestJob contains the state tracked by the coordinator for each ingest run.
type IngestJob struct {
	ID                  string
	TableID             iceberg.TableIdentifier
	DistributedSnapshot *iceberg.DistributedSnapshot
	Manifests           []iceberg.ManifestInfo
	Status              JobStatus
}

var (
	errTableClientMissing = errors.New("coordinator: table client is not configured")
	// ErrJobNotFound is returned when a job lookup fails.
	ErrJobNotFound = errors.New("coordinator: job not found")
)

// JobManager keeps ingest job metadata in memory.
type JobManager struct {
	mu sync.RWMutex

	jobs map[string]*IngestJob

	tableClient iceberg.TableClient
	idFn        func() string
}

// NewJobManager constructs a new in-memory job manager.
func NewJobManager(tableClient iceberg.TableClient) *JobManager {
	return &JobManager{
		jobs:        make(map[string]*IngestJob),
		tableClient: tableClient,
		idFn:        func() string { return uuid.NewString() },
	}
}

// CreateJob opens the target table, reserves a distributed snapshot and stores
// an in-memory job record.
func (jm *JobManager) CreateJob(ctx context.Context, tableID iceberg.TableIdentifier, props map[string]string) (*IngestJob, error) {
	if jm.tableClient == nil {
		return nil, errTableClientMissing
	}

	tbl, err := jm.tableClient.OpenTable(ctx, tableID)
	if err != nil {
		return nil, err
	}

	snapshot, err := tbl.BeginDistributedSnapshot(ctx, props)
	if err != nil {
		return nil, err
	}

	job := &IngestJob{
		ID:                  jm.idFn(),
		TableID:             tableID,
		DistributedSnapshot: snapshot,
		Status:              JobStatusRunning,
	}

	jm.mu.Lock()
	defer jm.mu.Unlock()
	jm.jobs[job.ID] = job

	return job, nil
}

// GetJob returns a copy of the stored job. The boolean return value is false
// when the job identifier does not exist.
func (jm *JobManager) GetJob(id string) (*IngestJob, bool) {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	job, ok := jm.jobs[id]
	if !ok {
		return nil, false
	}

	clone := *job
	if len(job.Manifests) > 0 {
		clone.Manifests = append([]iceberg.ManifestInfo(nil), job.Manifests...)
	}

	return &clone, true
}

// AddManifest registers a manifest file for the provided job identifier.
func (jm *JobManager) AddManifest(jobID string, manifest iceberg.ManifestInfo) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	job, ok := jm.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}

	job.Manifests = append(job.Manifests, manifest)
	if job.Status == JobStatusPending {
		job.Status = JobStatusRunning
	}

	return nil
}

// MarkCommitting transitions the job into the committing state.
func (jm *JobManager) MarkCommitting(jobID string) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	job, ok := jm.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}

	job.Status = JobStatusCommitting
	return nil
}

// MarkCompleted marks the job as completed.
func (jm *JobManager) MarkCompleted(jobID string) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	job, ok := jm.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}

	job.Status = JobStatusCompleted
	return nil
}

// MarkFailed sets the job status to failed.
func (jm *JobManager) MarkFailed(jobID string) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	job, ok := jm.jobs[jobID]
	if !ok {
		return ErrJobNotFound
	}

	job.Status = JobStatusFailed
	return nil
}
