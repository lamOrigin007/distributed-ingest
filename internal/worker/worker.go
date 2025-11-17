package worker

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/example/distributed-ingest/internal/api"
	"github.com/example/distributed-ingest/internal/iceberg"
)

// DataSource exposes the interface workers use to retrieve data for a task.
type DataSource interface {
	LoadDataFiles(ctx context.Context, task *api.Task, spec icebergpkg.PartitionSpec) ([]icebergpkg.DataFile, error)
}

// Worker coordinates manifest generation for a set of assigned tasks.
type Worker struct {
	id         string
	coord      CoordinatorReporter
	tables     iceberg.TableClient
	dataSource DataSource
}

// NewWorker constructs a worker instance.
func NewWorker(id string, coord CoordinatorReporter, tables iceberg.TableClient, dataSource DataSource) *Worker {
	return &Worker{
		id:         id,
		coord:      coord,
		tables:     tables,
		dataSource: dataSource,
	}
}

// ProcessAssignments handles an AssignTasks gRPC request by materializing the manifests
// for every supplied task.
func (w *Worker) ProcessAssignments(ctx context.Context, req *api.AssignTasksRequest) (*api.AssignTasksResponse, error) {
	if w == nil {
		return nil, errors.New("worker is not configured")
	}
	if req == nil {
		return nil, errors.New("assign request is required")
	}
	if req.JobId == "" {
		return nil, errors.New("job_id is required")
	}
	if req.Table == nil {
		return nil, errors.New("table identifier is required")
	}
	if req.Snapshot == nil {
		return nil, errors.New("distributed snapshot is required")
	}
	if len(req.Tasks) == 0 {
		return &api.AssignTasksResponse{
			WorkerId: w.id,
			JobId:    req.JobId,
			Message:  "no tasks provided",
		}, nil
	}
	if w.tables == nil {
		return nil, errors.New("table client is not configured")
	}
	if w.dataSource == nil {
		return nil, errors.New("data source is not configured")
	}

	tbl, err := w.tables.OpenTable(ctx, toInternalTable(req.Table))
	if err != nil {
		return nil, fmt.Errorf("open table: %w", err)
	}

	spec := tbl.PartitionSpec()
	schema := tbl.CurrentSchema()
	formatVersion := tbl.FormatVersion()
	location := tbl.Location()
	fs, err := tbl.IO(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve table IO: %w", err)
	}

	var manifestsCreated uint32
	manifestPaths := make([]string, 0, len(req.Tasks))
	snapshotID := req.Snapshot.SnapshotId

	for _, task := range req.Tasks {
		info, err := w.processTask(ctx, task, spec, schema, formatVersion, snapshotID, fs, location, req.JobId)
		if err != nil {
			return nil, fmt.Errorf("task %q: %w", task.GetTaskId(), err)
		}
		if info.FilePath() != "" {
			manifestPaths = append(manifestPaths, info.FilePath())
			manifestsCreated++
		}
	}

	if len(manifestPaths) > 0 && w.coord != nil {
		if err := w.coord.ReportManifest(ctx, req.JobId, req.Table, snapshotID, w.id, manifestPaths); err != nil {
			return nil, fmt.Errorf("report manifest: %w", err)
		}
	}

	return &api.AssignTasksResponse{
		WorkerId:         w.id,
		JobId:            req.JobId,
		TasksCompleted:   uint32(len(req.Tasks)),
		ManifestsCreated: manifestsCreated,
		Message:          "ok",
	}, nil
}

func (w *Worker) processTask(
	ctx context.Context,
	task *api.Task,
	spec icebergpkg.PartitionSpec,
	schema *icebergpkg.Schema,
	formatVersion int,
	snapshotID int64,
	fs icebergio.WriteFileIO,
	location string,
	jobID string,
) (iceberg.ManifestInfo, error) {
	if task == nil {
		return iceberg.ManifestInfo{}, errors.New("task description is required")
	}
	files, err := w.dataSource.LoadDataFiles(ctx, task, spec)
	if err != nil {
		return iceberg.ManifestInfo{}, fmt.Errorf("load data files: %w", err)
	}
	if len(files) == 0 {
		return iceberg.ManifestInfo{}, errors.New("no data files produced for task")
	}

	path := manifestPath(location, jobID, task.GetTaskId(), w.id)
	writer, err := icebergpkg.NewManifestWriterForSnapshot(fs, formatVersion, spec, schema, snapshotID, path)
	if err != nil {
		return iceberg.ManifestInfo{}, fmt.Errorf("create manifest writer: %w", err)
	}
	defer writer.Close()

	for _, file := range files {
		entry := icebergpkg.NewManifestEntry(icebergpkg.EntryStatusADDED, &snapshotID, nil, nil, file)
		if err := writer.Add(entry); err != nil {
			return iceberg.ManifestInfo{}, fmt.Errorf("append manifest entry: %w", err)
		}
	}

	manifestFile, err := writer.ToManifestFile()
	if err != nil {
		return iceberg.ManifestInfo{}, fmt.Errorf("finalize manifest: %w", err)
	}

	return iceberg.ManifestFromFile(manifestFile), nil
}

func manifestPath(tableLocation, jobID, taskID, workerID string) string {
	base := strings.TrimSuffix(tableLocation, "/")
	if base == "" {
		base = "file:///tmp/distributed-ingest"
	}
	filename := fmt.Sprintf("manifest-%s-%s-%s.avro", workerID, jobID, taskID)
	return path.Join(base, "manifests", workerID, filename)
}

func toInternalTable(tbl *api.TableIdentifier) iceberg.TableIdentifier {
	if tbl == nil {
		return iceberg.TableIdentifier{}
	}
	return iceberg.TableIdentifier{
		Catalog:   tbl.Catalog,
		Namespace: tbl.Namespace,
		Table:     tbl.Table,
	}
}
