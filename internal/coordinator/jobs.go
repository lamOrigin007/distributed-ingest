package coordinator

import (
	"context"
	"errors"
	"log"
	"maps"
	"sync"
	"time"

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
	JobStatusConflict   JobStatus = "CONFLICT"
)

// IngestJob contains the state tracked by the coordinator for each ingest run.
type IngestJob struct {
	ID                  string
	TableID             iceberg.TableIdentifier
	DistributedSnapshot *iceberg.DistributedSnapshot
	Manifests           []iceberg.ManifestInfo
	manifestPaths       map[string]struct{}
	Status              JobStatus
	ExpectedManifests   int
	ReadyToCommit       bool
	Summary             map[string]string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	Timeout             time.Duration
	Deadline            time.Time
}

var (
	errTableClientMissing = errors.New("coordinator: table client is not configured")
	// ErrJobNotFound is returned when a job lookup fails.
	ErrJobNotFound = errors.New("coordinator: job not found")
	// ErrJobNotReady is returned when a commit is attempted before a job reaches the
	// ready threshold.
	ErrJobNotReady = errors.New("coordinator: job is not ready to commit")
	// ErrJobInvalidState is returned when a commit is attempted from a terminal
	// status.
	ErrJobInvalidState = errors.New("coordinator: job is in an invalid state for commit")
)

// JobManager keeps ingest job metadata in memory.
type JobManager struct {
	mu sync.RWMutex

	jobs map[string]*IngestJob

	tableClient iceberg.TableClient
	idFn        func() string
	nowFn       func() time.Time
	defaultTTL  time.Duration
}

// NewJobManager constructs a new in-memory job manager.
func NewJobManager(tableClient iceberg.TableClient) *JobManager {
	return &JobManager{
		jobs:        make(map[string]*IngestJob),
		tableClient: tableClient,
		idFn:        func() string { return uuid.NewString() },
		nowFn:       time.Now,
		defaultTTL:  10 * time.Minute,
	}
}

// CreateJob opens the target table, reserves a distributed snapshot and stores
// an in-memory job record.
func (jm *JobManager) CreateJob(ctx context.Context, tableID iceberg.TableIdentifier, props map[string]string, timeout time.Duration) (*IngestJob, error) {
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

	now := jm.nowFn()
	if timeout <= 0 {
		timeout = jm.defaultTTL
	}
	job := &IngestJob{
		ID:                  jm.idFn(),
		TableID:             tableID,
		DistributedSnapshot: snapshot,
		Status:              JobStatusRunning,
		ExpectedManifests:   1,
		Summary:             map[string]string{"operation": "append"},
		manifestPaths:       make(map[string]struct{}),
		CreatedAt:           now,
		UpdatedAt:           now,
		Timeout:             timeout,
		Deadline:            now.Add(timeout),
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
	if len(job.Summary) > 0 {
		clone.Summary = maps.Clone(job.Summary)
	}

	return &clone, true
}

// AddManifest registers a manifest file for the provided job identifier.
func (jm *JobManager) AddManifest(jobID string, manifest iceberg.ManifestInfo) (bool, error) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	job, ok := jm.jobs[jobID]
	if !ok {
		return false, ErrJobNotFound
	}
	if job.manifestPaths == nil {
		job.manifestPaths = make(map[string]struct{})
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed || job.Status == JobStatusConflict {
		return false, nil
	}

	path := manifest.FilePath()
	if path != "" {
		if _, exists := job.manifestPaths[path]; exists {
			return false, nil
		}
		job.manifestPaths[path] = struct{}{}
	}
	job.Manifests = append(job.Manifests, manifest)
	jm.touchLocked(job, true)
	if job.Status == JobStatusPending {
		job.Status = JobStatusRunning
	}

	readyNow := false
	if !job.ReadyToCommit {
		threshold := job.ExpectedManifests
		if threshold <= 0 {
			threshold = 1
		}
		if len(job.Manifests) >= threshold {
			job.ReadyToCommit = true
			readyNow = true
		}
	}

	return readyNow, nil
}

// CommitJob publishes the distributed snapshot manifests to the target table.
func (jm *JobManager) CommitJob(ctx context.Context, jobID string) error {
	if jm.tableClient == nil {
		return errTableClientMissing
	}

	jm.mu.Lock()
	job, ok := jm.jobs[jobID]
	if !ok {
		jm.mu.Unlock()
		return ErrJobNotFound
	}
	if job.Status == JobStatusCompleted {
		jm.mu.Unlock()
		return nil
	}
	if job.Status == JobStatusFailed || job.Status == JobStatusConflict {
		jm.mu.Unlock()
		return ErrJobInvalidState
	}
	if len(job.Manifests) == 0 || !job.ReadyToCommit {
		jm.mu.Unlock()
		return ErrJobNotReady
	}
	job.Status = JobStatusCommitting
	jm.touchLocked(job, false)
	manifests := append([]iceberg.ManifestInfo(nil), job.Manifests...)
	ds := job.DistributedSnapshot
	tableID := job.TableID
	var summary map[string]string
	if len(job.Summary) > 0 {
		summary = maps.Clone(job.Summary)
	}
	jm.mu.Unlock()

	tbl, err := jm.tableClient.OpenTable(ctx, tableID)
	if err != nil {
		jm.setStatus(jobID, JobStatusFailed)
		return err
	}
	if ds == nil {
		jm.setStatus(jobID, JobStatusFailed)
		return errors.New("coordinator: distributed snapshot metadata missing")
	}

	commitErr := tbl.CommitDistributedSnapshot(ctx, ds, manifests, summary)
	finalStatus := JobStatusCompleted
	if commitErr != nil {
		if errors.Is(commitErr, iceberg.ErrCommitConflict) {
			finalStatus = JobStatusConflict
		} else {
			finalStatus = JobStatusFailed
		}
	}
	jm.setStatus(jobID, finalStatus)
	return commitErr
}

func (jm *JobManager) setStatus(jobID string, status JobStatus) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	if job, ok := jm.jobs[jobID]; ok {
		job.Status = status
		jm.touchLocked(job, false)
	}
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
	jm.touchLocked(job, false)
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
	jm.touchLocked(job, false)
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
	jm.touchLocked(job, false)
	return nil
}

// StartTimeoutWatcher launches a background goroutine that periodically scans for
// jobs that exceeded their deadline and marks them as failed.
func (jm *JobManager) StartTimeoutWatcher(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				jm.expireStaleJobs()
			}
		}
	}()
}

func (jm *JobManager) expireStaleJobs() {
	now := jm.nowFn()
	jm.mu.Lock()
	defer jm.mu.Unlock()
	for _, job := range jm.jobs {
		if job == nil {
			continue
		}
		if job.Status == JobStatusCompleted || job.Status == JobStatusFailed || job.Status == JobStatusConflict {
			continue
		}
		deadline := job.Deadline
		if deadline.IsZero() && job.Timeout > 0 {
			deadline = job.UpdatedAt.Add(job.Timeout)
		}
		if deadline.IsZero() {
			continue
		}
		if now.After(deadline) {
			job.Status = JobStatusFailed
			job.ReadyToCommit = false
			jm.touchLocked(job, false)
			log.Printf("coordinator: job %s timed out after %s", job.ID, job.Timeout)
		}
	}
}

func (jm *JobManager) touchLocked(job *IngestJob, progress bool) {
	if job == nil {
		return
	}
	job.UpdatedAt = jm.nowFn()
	if progress && job.Timeout > 0 {
		job.Deadline = job.UpdatedAt.Add(job.Timeout)
	}
}
