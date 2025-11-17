package coordinator

import (
	"context"
	"testing"

	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/example/distributed-ingest/internal/iceberg"
)

func TestJobManagerCreateJob(t *testing.T) {
	snapshot := &iceberg.DistributedSnapshot{SnapshotID: 42}
	tbl := &fakeTable{snapshot: snapshot}
	client := &fakeTableClient{table: tbl}

	jm := NewJobManager(client)
	job, err := jm.CreateJob(context.Background(), iceberg.TableIdentifier{Catalog: "test", Namespace: "ns", Table: "tbl"}, map[string]string{"attempt": "1"})
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

	job, err := jm.CreateJob(context.Background(), iceberg.TableIdentifier{Table: "tbl"}, nil)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	manifest := iceberg.ManifestInfo{Path: "manifest.avro"}
	if err := jm.AddManifest(job.ID, manifest); err != nil {
		t.Fatalf("AddManifest failed: %v", err)
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

func TestJobManagerDistributedSnapshotPreserved(t *testing.T) {
	snapshot := &iceberg.DistributedSnapshot{SnapshotID: 7}
	jm := NewJobManager(&fakeTableClient{table: &fakeTable{snapshot: snapshot}})

	job, err := jm.CreateJob(context.Background(), iceberg.TableIdentifier{Table: "tbl"}, nil)
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
	snapshot *iceberg.DistributedSnapshot
}

func (f *fakeTable) BeginDistributedSnapshot(ctx context.Context, props map[string]string) (*iceberg.DistributedSnapshot, error) {
	return f.snapshot, nil
}

func (f *fakeTable) CommitDistributedSnapshot(ctx context.Context, ds *iceberg.DistributedSnapshot, manifests []iceberg.ManifestInfo, summary map[string]string) error {
	return nil
}

func (f *fakeTable) CurrentSchema() *icebergpkg.Schema { return icebergpkg.NewSchema(0) }

func (f *fakeTable) PartitionSpec() icebergpkg.PartitionSpec { return *icebergpkg.UnpartitionedSpec }

func (f *fakeTable) FormatVersion() int { return 2 }

func (f *fakeTable) Location() string { return "file:///tmp/test" }

func (f *fakeTable) IO(context.Context) (icebergio.WriteFileIO, error) {
	return icebergio.LocalFS{}, nil
}
