package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/example/distributed-ingest/internal/iceberg"
)

func TestJobManagerCreateJob(t *testing.T) {
	snapshot := &iceberg.DistributedSnapshot{SnapshotID: 42}
	tbl := &fakeTable{snapshot: snapshot}
	client := &fakeTableClient{table: tbl}

	jm := NewJobManager(client)
	job, err := jm.CreateJob(context.Background(), iceberg.TableIdentifier{Catalog: "test", Namespace: "ns", Table: "tbl"}, map[string]string{"attempt": "1"}, time.Minute)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	if job.ID == "" {
		t.Fatalf("expected job ID to be set")
	}
	if job.Status != JobStatusRunning {
		t.Fatalf("expected status %s, got %s", JobStatusRunning, job.Status)
	}
	if job.DistributedSnapshot == nil {
		t.Fatalf("distributed snapshot is nil")
	}
	if job.DistributedSnapshot.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("snapshot mismatch: expected %d got %d", snapshot.SnapshotID, job.DistributedSnapshot.SnapshotID)
	}

	if _, ok := jm.GetJob(job.ID); !ok {
		t.Fatalf("GetJob should succeed")
	}
}

func TestJobManagerAddManifest(t *testing.T) {
	snapshot := &iceberg.DistributedSnapshot{SnapshotID: 101}
	jm := NewJobManager(&fakeTableClient{table: &fakeTable{snapshot: snapshot}})

	job, err := jm.CreateJob(context.Background(), iceberg.TableIdentifier{Table: "tbl"}, nil, time.Minute)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	manifest := iceberg.ManifestInfo{Path: "manifest.avro"}
	ready, err := jm.AddManifest(job.ID, manifest)
	if err != nil {
		t.Fatalf("AddManifest failed: %v", err)
	}
	if !ready {
		t.Fatalf("job should be ready after the first manifest by default")
	}

	stored, ok := jm.GetJob(job.ID)
	if !ok {
		t.Fatalf("job lookup failed")
	}
	if len(stored.Manifests) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(stored.Manifests))
	}
	if stored.Manifests[0].FilePath() != manifest.FilePath() {
		t.Fatalf("manifest path mismatch: expected %s got %s", manifest.FilePath(), stored.Manifests[0].FilePath())
	}
}

func TestJobManagerAddManifestIdempotent(t *testing.T) {
	snapshot := &iceberg.DistributedSnapshot{SnapshotID: 55}
	jm := NewJobManager(&fakeTableClient{table: &fakeTable{snapshot: snapshot}})
	job, err := jm.CreateJob(context.Background(), iceberg.TableIdentifier{Table: "tbl"}, nil, time.Minute)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	manifest := iceberg.ManifestInfo{Path: "manifest.avro"}
	ready, err := jm.AddManifest(job.ID, manifest)
	if err != nil {
		t.Fatalf("AddManifest failed: %v", err)
	}
	if !ready {
		t.Fatalf("job should be ready after first manifest")
	}
	ready, err = jm.AddManifest(job.ID, manifest)
	if err != nil {
		t.Fatalf("AddManifest failed on duplicate: %v", err)
	}
	if ready {
		t.Fatalf("duplicate manifest should not retrigger readiness")
	}
	stored, ok := jm.GetJob(job.ID)
	if !ok {
		t.Fatalf("job lookup failed")
	}
	if len(stored.Manifests) != 1 {
		t.Fatalf("expected 1 manifest after duplicate, got %d", len(stored.Manifests))
	}
}

func TestJobManagerDistributedSnapshotPreserved(t *testing.T) {
	snapshot := &iceberg.DistributedSnapshot{SnapshotID: 7}
	jm := NewJobManager(&fakeTableClient{table: &fakeTable{snapshot: snapshot}})

	job, err := jm.CreateJob(context.Background(), iceberg.TableIdentifier{Table: "tbl"}, nil, time.Minute)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	stored, ok := jm.GetJob(job.ID)
	if !ok {
		t.Fatalf("job lookup failed")
	}

	if stored.DistributedSnapshot == nil {
		t.Fatalf("stored snapshot is nil")
	}
	if stored.DistributedSnapshot.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("snapshot mismatch")
	}
}

func TestJobManagerCommitJobSuccess(t *testing.T) {
	ctx := context.Background()
	snapshot := &iceberg.DistributedSnapshot{SnapshotID: 99}
	tbl := &fakeTable{snapshot: snapshot}
	jm := NewJobManager(&fakeTableClient{table: tbl})

	job, err := jm.CreateJob(ctx, iceberg.TableIdentifier{Table: "tbl"}, nil, time.Minute)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	job.ExpectedManifests = 2

	ready, err := jm.AddManifest(job.ID, iceberg.ManifestInfo{Path: "one.avro"})
	if err != nil {
		t.Fatalf("AddManifest failed: %v", err)
	}
	if ready {
		t.Fatalf("job should not be ready after first manifest")
	}
	ready, err = jm.AddManifest(job.ID, iceberg.ManifestInfo{Path: "two.avro"})
	if err != nil {
		t.Fatalf("AddManifest failed: %v", err)
	}
	if !ready {
		t.Fatalf("job should be ready after reaching threshold")
	}

	if err := jm.CommitJob(ctx, job.ID); err != nil {
		t.Fatalf("CommitJob failed: %v", err)
	}
	stored, ok := jm.GetJob(job.ID)
	if !ok {
		t.Fatalf("job lookup failed")
	}
	if stored.Status != JobStatusCompleted {
		t.Fatalf("expected status %s, got %s", JobStatusCompleted, stored.Status)
	}
	if tbl.commitCount != 1 {
		t.Fatalf("expected 1 commit, got %d", tbl.commitCount)
	}
}

func TestJobManagerCommitJobConflict(t *testing.T) {
	ctx := context.Background()
	snapshot := &iceberg.DistributedSnapshot{SnapshotID: 100}
	tbl := &fakeTable{snapshot: snapshot, commitErr: iceberg.ErrCommitConflict}
	jm := NewJobManager(&fakeTableClient{table: tbl})

	job, err := jm.CreateJob(ctx, iceberg.TableIdentifier{Table: "tbl"}, nil, time.Minute)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	job.ExpectedManifests = 1

	ready, err := jm.AddManifest(job.ID, iceberg.ManifestInfo{Path: "conflict.avro"})
	if err != nil {
		t.Fatalf("AddManifest failed: %v", err)
	}
	if !ready {
		t.Fatalf("job should be ready after single manifest when threshold is 1")
	}

	err = jm.CommitJob(ctx, job.ID)
	if !errors.Is(err, iceberg.ErrCommitConflict) {
		t.Fatalf("expected commit conflict error, got %v", err)
	}
	stored, ok := jm.GetJob(job.ID)
	if !ok {
		t.Fatalf("job lookup failed")
	}
	if stored.Status != JobStatusConflict {
		t.Fatalf("expected status %s, got %s", JobStatusConflict, stored.Status)
	}
}

func TestJobManagerTimeout(t *testing.T) {
	ctx := context.Background()
	snapshot := &iceberg.DistributedSnapshot{SnapshotID: 200}
	jm := NewJobManager(&fakeTableClient{table: &fakeTable{snapshot: snapshot}})
	now := time.Now()
	jm.nowFn = func() time.Time { return now }
	job, err := jm.CreateJob(ctx, iceberg.TableIdentifier{Table: "tbl"}, nil, time.Second)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	now = now.Add(2 * time.Second)
	jm.expireStaleJobs()
	stored, ok := jm.GetJob(job.ID)
	if !ok {
		t.Fatalf("job lookup failed")
	}
	if stored.Status != JobStatusFailed {
		t.Fatalf("expected status %s, got %s", JobStatusFailed, stored.Status)
	}
}

type fakeTableClient struct {
	table iceberg.Table
	err   error
}

func (f *fakeTableClient) OpenTable(ctx context.Context, identifier iceberg.TableIdentifier) (iceberg.Table, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.table, nil
}

type fakeTable struct {
	snapshot    *iceberg.DistributedSnapshot
	commitErr   error
	commitCount int
}

func (f *fakeTable) BeginDistributedSnapshot(ctx context.Context, props map[string]string) (*iceberg.DistributedSnapshot, error) {
	return f.snapshot, nil
}

func (f *fakeTable) CommitDistributedSnapshot(ctx context.Context, ds *iceberg.DistributedSnapshot, manifests []iceberg.ManifestInfo, summary map[string]string) error {
	if f.commitErr != nil {
		return f.commitErr
	}
	f.commitCount++
	return nil
}

func (f *fakeTable) CurrentSchema() *icebergpkg.Schema { return icebergpkg.NewSchema(0) }

func (f *fakeTable) PartitionSpec() icebergpkg.PartitionSpec { return *icebergpkg.UnpartitionedSpec }

func (f *fakeTable) FormatVersion() int { return 2 }

func (f *fakeTable) Location() string { return "file:///tmp/test" }

func (f *fakeTable) IO(context.Context) (icebergio.WriteFileIO, error) {
	return icebergio.LocalFS{}, nil
}
