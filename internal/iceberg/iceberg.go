package iceberg

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	icebergpkg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/glue"
	_ "github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// TableIdentifier uniquely addresses an Iceberg table in a configured catalog.
type TableIdentifier struct {
	Catalog   string
	Namespace string
	Table     string
}

// DistributedSnapshot exposes the metadata that workers need to write manifests
// for a distributed commit.
type DistributedSnapshot struct {
	SnapshotID       int64
	ParentSnapshotID *int64
	CommitUUID       string
	Properties       map[string]string

	descriptor *table.DistributedSnapshot
}

// ManifestInfo carries manifest metadata collected from workers. When the
// underlying iceberg.ManifestFile metadata is unavailable we keep the path so
// operators can inspect it while the job is still running.
type ManifestInfo struct {
	Path string

	file icebergpkg.ManifestFile
}

// ManifestFromFile converts an iceberg ManifestFile into a ManifestInfo value
// that can be tracked by the coordinator state machine.
func ManifestFromFile(file icebergpkg.ManifestFile) ManifestInfo {
	if file == nil {
		return ManifestInfo{}
	}

	return ManifestInfo{
		Path: file.FilePath(),
		file: file,
	}
}

// FilePath returns the manifest path associated with the manifest info if one
// has been recorded.
func (m ManifestInfo) FilePath() string {
	if m.file != nil {
		return m.file.FilePath()
	}
	return m.Path
}

// Table provides the minimal subset of Table functionality the coordinator
// requires to orchestrate distributed snapshots.
type Table interface {
	BeginDistributedSnapshot(ctx context.Context, props map[string]string) (*DistributedSnapshot, error)
	CommitDistributedSnapshot(ctx context.Context, ds *DistributedSnapshot, manifests []ManifestInfo, summary map[string]string) error
	CurrentSchema() *icebergpkg.Schema
	PartitionSpec() icebergpkg.PartitionSpec
	FormatVersion() int
	Location() string
	IO(ctx context.Context) (icebergio.WriteFileIO, error)
}

// TableClient opens table handles from configured catalogs.
type TableClient interface {
	OpenTable(ctx context.Context, identifier TableIdentifier) (Table, error)
}

// CatalogConfig contains the properties required to instantiate a catalog.
type CatalogConfig struct {
	Name       string
	Properties map[string]string
}

// Config enumerates every catalog the coordinator should be able to access.
type Config struct {
	DefaultCatalog string
	Catalogs       []CatalogConfig
}

// NewClient constructs a TableClient using the provided catalog configuration.
func NewClient(ctx context.Context, cfg Config) (TableClient, error) {
	if len(cfg.Catalogs) == 0 {
		return nil, errors.New("iceberg: at least one catalog configuration is required")
	}

	catalogs := make(map[string]catalog.Catalog, len(cfg.Catalogs))
	for _, catCfg := range cfg.Catalogs {
		if catCfg.Name == "" {
			return nil, errors.New("iceberg: catalog name cannot be empty")
		}
		props := make(icebergpkg.Properties, len(catCfg.Properties))
		maps.Copy(props, catCfg.Properties)

		cat, err := catalog.Load(ctx, catCfg.Name, props)
		if err != nil {
			return nil, fmt.Errorf("iceberg: load catalog %q: %w", catCfg.Name, err)
		}

		catalogs[catCfg.Name] = cat
	}

	defaultName := cfg.DefaultCatalog
	if defaultName == "" {
		defaultName = cfg.Catalogs[0].Name
	}

	if _, ok := catalogs[defaultName]; !ok {
		return nil, fmt.Errorf("iceberg: default catalog %q is not configured", defaultName)
	}

	return &client{catalogs: catalogs, defaultCatalog: defaultName}, nil
}

type client struct {
	catalogs       map[string]catalog.Catalog
	defaultCatalog string
}

func (c *client) OpenTable(ctx context.Context, identifier TableIdentifier) (Table, error) {
	catalogName := identifier.Catalog
	if catalogName == "" {
		catalogName = c.defaultCatalog
	}

	cat, ok := c.catalogs[catalogName]
	if !ok {
		return nil, fmt.Errorf("iceberg: catalog %q is not configured", catalogName)
	}

	tblIdent, err := toTableIdentifier(identifier)
	if err != nil {
		return nil, err
	}

	tbl, err := cat.LoadTable(ctx, tblIdent)
	if err != nil {
		return nil, fmt.Errorf("iceberg: load table %v: %w", tblIdent, err)
	}

	return &tableHandle{tbl: tbl}, nil
}

type tableHandle struct {
	tbl *table.Table
}

func (h *tableHandle) BeginDistributedSnapshot(ctx context.Context, props map[string]string) (*DistributedSnapshot, error) {
	var icebergProps icebergpkg.Properties
	if len(props) > 0 {
		icebergProps = make(icebergpkg.Properties, len(props))
		maps.Copy(icebergProps, props)
	}

	ds, err := h.tbl.BeginDistributedSnapshot(ctx, icebergProps)
	if err != nil {
		return nil, err
	}

	return newDistributedSnapshot(ds), nil
}

func (h *tableHandle) CommitDistributedSnapshot(ctx context.Context, ds *DistributedSnapshot, manifests []ManifestInfo, summary map[string]string) error {
	if ds == nil {
		return errors.New("iceberg: distributed snapshot descriptor is required")
	}

	manifestFiles := make([]icebergpkg.ManifestFile, len(manifests))
	for i, m := range manifests {
		file, err := m.toManifestFile()
		if err != nil {
			return err
		}
		manifestFiles[i] = file
	}

	_, err := h.tbl.CommitDistributedSnapshot(ctx, ds.descriptor, manifestFiles, summary)
	return err
}

func (h *tableHandle) CurrentSchema() *icebergpkg.Schema {
	return h.tbl.Schema()
}

func (h *tableHandle) PartitionSpec() icebergpkg.PartitionSpec {
	return h.tbl.Spec()
}

func (h *tableHandle) FormatVersion() int {
	return h.tbl.Metadata().Version()
}

func (h *tableHandle) Location() string {
	return h.tbl.Location()
}

func (h *tableHandle) IO(ctx context.Context) (icebergio.WriteFileIO, error) {
	fs, err := h.tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	writer, ok := fs.(icebergio.WriteFileIO)
	if !ok {
		return nil, errors.New("iceberg: table filesystem does not support writes")
	}
	return writer, nil
}

func newDistributedSnapshot(ds *table.DistributedSnapshot) *DistributedSnapshot {
	var parent *int64
	if ds.ParentSnapshotID != nil {
		parent = new(int64)
		*parent = *ds.ParentSnapshotID
	}

	props := make(map[string]string, len(ds.SnapshotProps))
	maps.Copy(props, ds.SnapshotProps)

	return &DistributedSnapshot{
		SnapshotID:       ds.SnapshotID,
		ParentSnapshotID: parent,
		CommitUUID:       ds.CommitUUID.String(),
		Properties:       props,
		descriptor:       ds,
	}
}

func (m ManifestInfo) toManifestFile() (icebergpkg.ManifestFile, error) {
	if m.file == nil {
		return nil, fmt.Errorf("iceberg: manifest metadata missing for %q", m.Path)
	}

	return m.file, nil
}

func toTableIdentifier(id TableIdentifier) (table.Identifier, error) {
	if id.Table == "" {
		return nil, errors.New("iceberg: table name cannot be empty")
	}

	var ident table.Identifier
	if trimmed := strings.TrimSpace(id.Namespace); trimmed != "" {
		parts := strings.Split(trimmed, ".")
		for _, p := range parts {
			if component := strings.TrimSpace(p); component != "" {
				ident = append(ident, component)
			}
		}
	}

	ident = append(ident, id.Table)
	return ident, nil
}
