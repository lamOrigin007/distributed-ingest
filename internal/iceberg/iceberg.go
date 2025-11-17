package iceberg

import "context"

// TableIdentifier uniquely addresses an Iceberg table in the catalog.
type TableIdentifier struct {
	Catalog   string
	Namespace string
	Table     string
}

// Snapshot captures metadata from the distributed snapshot primitives.
type Snapshot struct {
	SnapshotID       int64
	ParentSnapshotID int64
	ManifestListPath string
	Summary          map[string]string
}

// SnapshotOptions control the creation of a distributed snapshot.
type SnapshotOptions struct {
	Identifier TableIdentifier
	Properties map[string]string
}

// Table represents the subset of the iceberg-go Table API the system relies on.
type Table interface {
	BeginDistributedSnapshot(ctx context.Context, opts SnapshotOptions) (*Snapshot, error)
	CommitDistributedSnapshot(ctx context.Context, snapshot *Snapshot, manifestPaths []string) error
}

// Client produces typed table handles from the configured catalog.
type Client interface {
	LoadTable(ctx context.Context, identifier TableIdentifier) (Table, error)
}
