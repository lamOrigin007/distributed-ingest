package worker

import (
	"context"
	"fmt"
	"path"
	"strings"

	icebergpkg "github.com/apache/iceberg-go"
	"github.com/example/distributed-ingest/internal/api"
)

// StaticDataSource is a simple implementation that generates placeholder data files
// for every requested task. It is intended for integration tests and the current
// prototype worker which does not yet read from a real upstream system.
type StaticDataSource struct {
	baseURI string
}

// NewStaticDataSource builds a StaticDataSource that stores files underneath the
// supplied base URI. When the base URI is empty a temporary path in /tmp is used.
func NewStaticDataSource(baseURI string) *StaticDataSource {
	if strings.TrimSpace(baseURI) == "" {
		baseURI = "file:///tmp/distributed-ingest/data"
	}
	return &StaticDataSource{baseURI: strings.TrimSuffix(baseURI, "/")}
}

// LoadDataFiles implements the DataSource interface.
func (s *StaticDataSource) LoadDataFiles(_ context.Context, task *api.Task, spec icebergpkg.PartitionSpec) ([]icebergpkg.DataFile, error) {
	if task == nil {
		return nil, fmt.Errorf("task description is required")
	}

	partitions := task.GetPartitions()
	if len(partitions) == 0 {
		partitions = []string{"default"}
	}
	files := make([]icebergpkg.DataFile, 0, len(partitions))

	for idx := range partitions {
		filePath := path.Join(s.baseURI, fmt.Sprintf("%s-%d.parquet", task.GetTaskId(), idx))
		builder, err := icebergpkg.NewDataFileBuilder(spec, icebergpkg.EntryContentData, filePath, icebergpkg.ParquetFile, nil, nil, nil, 1024, 128*1024)
		if err != nil {
			return nil, fmt.Errorf("build data file: %w", err)
		}
		files = append(files, builder.Build())
	}

	return files, nil
}
