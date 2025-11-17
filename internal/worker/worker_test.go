package worker

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/example/distributed-ingest/internal/api"
	"github.com/example/distributed-ingest/internal/coordinator"
	"github.com/example/distributed-ingest/internal/iceberg"
)

func TestWorkerProcessesAssignmentsAndReportsManifest(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		_ = os.RemoveAll("file:")
	})

	tableDir := t.TempDir()
	fakeTbl := newFakeTable(fmt.Sprintf("file://%s/table", tableDir))
	fakeClient := &fakeTableClient{table: fakeTbl}

	jobManager := coordinator.NewJobManager(fakeClient)
	coordServer, coordConn := startCoordinatorServer(t, coordinator.NewService(jobManager))
	defer coordServer.Stop()
	defer coordConn.Close()

	coordClient := NewCoordinatorClient(api.NewCoordinatorClient(coordConn))
	dataSource := NewStaticDataSource(fmt.Sprintf("file://%s/data", tableDir))
	logic := NewWorker("worker-1", coordClient, fakeClient, dataSource)

	workerServer, workerConn := startWorkerServer(t, logic)
	defer workerServer.Stop()
	defer workerConn.Close()

	tableID := iceberg.TableIdentifier{Namespace: "default", Table: "events"}
	job, err := jobManager.CreateJob(ctx, tableID, map[string]string{"requested_by": "test"}, time.Minute)
	require.NoError(t, err)

	workerClient := api.NewWorkerClient(workerConn)
	assignReq := &api.AssignTasksRequest{
		WorkerId: "worker-1",
		JobId:    job.ID,
		Table: &api.TableIdentifier{
			Namespace: tableID.Namespace,
			Table:     tableID.Table,
		},
		Snapshot: &api.DistributedSnapshot{SnapshotId: job.DistributedSnapshot.SnapshotID, CommitUuid: job.DistributedSnapshot.CommitUUID},
		Tasks:    []*api.Task{{TaskId: "task-1", Partitions: []string{"region=us"}}},
	}

	resp, err := workerClient.AssignTasks(ctx, assignReq)
	require.NoError(t, err)
	require.Equal(t, uint32(1), resp.ManifestsCreated)
	require.Equal(t, uint32(1), resp.TasksCompleted)

	updatedJob, ok := jobManager.GetJob(job.ID)
	require.True(t, ok)
	require.Len(t, updatedJob.Manifests, 1)
	manifestPath := updatedJob.Manifests[0].FilePath()
	require.NotEmpty(t, manifestPath)

	fsPath := strings.TrimPrefix(manifestPath, "file://")
	_, err = os.Stat(fsPath)
	require.NoError(t, err, "manifest file should exist on disk")

	entries, err := os.ReadDir(filepath.Dir(fsPath))
	require.NoError(t, err)
	require.NotEmpty(t, entries)
}

func TestWorkerAssignmentsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		_ = os.RemoveAll("file:")
	})
	tableDir := t.TempDir()
	fakeTbl := newFakeTable(fmt.Sprintf("file://%s/table", tableDir))
	fakeClient := &fakeTableClient{table: fakeTbl}
	jobManager := coordinator.NewJobManager(fakeClient)
	coordServer, coordConn := startCoordinatorServer(t, coordinator.NewService(jobManager))
	defer coordServer.Stop()
	defer coordConn.Close()
	coordClient := NewCoordinatorClient(api.NewCoordinatorClient(coordConn))
	dataSource := NewStaticDataSource(fmt.Sprintf("file://%s/data", tableDir))
	logic := NewWorker("worker-1", coordClient, fakeClient, dataSource)
	workerServer, workerConn := startWorkerServer(t, logic)
	defer workerServer.Stop()
	defer workerConn.Close()
	tableID := iceberg.TableIdentifier{Namespace: "default", Table: "events"}
	job, err := jobManager.CreateJob(ctx, tableID, nil, time.Minute)
	require.NoError(t, err)
	workerClient := api.NewWorkerClient(workerConn)
	assignReq := &api.AssignTasksRequest{
		WorkerId: "worker-1",
		JobId:    job.ID,
		Table:    &api.TableIdentifier{Namespace: tableID.Namespace, Table: tableID.Table},
		Snapshot: &api.DistributedSnapshot{SnapshotId: job.DistributedSnapshot.SnapshotID, CommitUuid: job.DistributedSnapshot.CommitUUID},
		Tasks:    []*api.Task{{TaskId: "task-1"}},
	}
	resp1, err := workerClient.AssignTasks(ctx, assignReq)
	require.NoError(t, err)
	require.Equal(t, uint32(1), resp1.ManifestsCreated)
	resp2, err := workerClient.AssignTasks(ctx, assignReq)
	require.NoError(t, err)
	require.Equal(t, uint32(0), resp2.ManifestsCreated)
	stored, ok := jobManager.GetJob(job.ID)
	require.True(t, ok)
	require.Len(t, stored.Manifests, 1)
}

func startCoordinatorServer(t *testing.T, svc *coordinator.Service) (*grpc.Server, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	api.RegisterCoordinatorServer(server, svc)
	go func() {
		_ = server.Serve(lis)
	}()

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	return server, conn
}

func startWorkerServer(t *testing.T, logic *Worker) (*grpc.Server, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	api.RegisterWorkerServer(server, NewService(logic))
	go func() {
		_ = server.Serve(lis)
	}()

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	return server, conn
}

type fakeTableClient struct {
	table iceberg.Table
}

func (c *fakeTableClient) OpenTable(context.Context, iceberg.TableIdentifier) (iceberg.Table, error) {
	return c.table, nil
}

type fakeTable struct {
	spec         icebergpkg.PartitionSpec
	schema       *icebergpkg.Schema
	format       int
	location     string
	fs           icebergio.WriteFileIO
	nextSnapshot int64
}

func newFakeTable(location string) *fakeTable {
	return &fakeTable{
		spec:     *icebergpkg.UnpartitionedSpec,
		schema:   icebergpkg.NewSchema(0, icebergpkg.NestedField{ID: 1, Name: "id", Required: true, Type: icebergpkg.PrimitiveTypes.Int64}),
		format:   2,
		location: location,
		fs:       icebergio.LocalFS{},
	}
}

func (t *fakeTable) BeginDistributedSnapshot(context.Context, map[string]string) (*iceberg.DistributedSnapshot, error) {
	id := atomic.AddInt64(&t.nextSnapshot, 1)
	return &iceberg.DistributedSnapshot{SnapshotID: id, CommitUUID: uuid.NewString()}, nil
}

func (t *fakeTable) CommitDistributedSnapshot(context.Context, *iceberg.DistributedSnapshot, []iceberg.ManifestInfo, map[string]string) error {
	return nil
}

func (t *fakeTable) CurrentSchema() *icebergpkg.Schema { return t.schema }

func (t *fakeTable) PartitionSpec() icebergpkg.PartitionSpec { return t.spec }

func (t *fakeTable) FormatVersion() int { return t.format }

func (t *fakeTable) Location() string { return t.location }

func (t *fakeTable) IO(context.Context) (icebergio.WriteFileIO, error) { return t.fs, nil }
