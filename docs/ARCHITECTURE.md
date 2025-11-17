# Distributed Iceberg Ingest Architecture

## Overview
The system orchestrates distributed writes into Apache Iceberg tables using the
extended distributed snapshot capabilities from our `iceberg-go` fork. Each
cluster runs a single coordinator service and one worker service per host. The
coordinator drives the distributed snapshot lifecycle while the workers execute
physical ingest tasks and report newly created manifests.

## Coordinator role
- Accept ingest requests through the gRPC `Coordinator` service.
- Materialize an Iceberg `Table` handle via the `internal/iceberg` wrapper and
  call `BeginDistributedSnapshot` to reserve a snapshot for the job.
- Partition the incoming workload into tasks that can be handed off to workers.
- Track manifests reported by workers and determine readiness for commit.
- Finalize the ingest by calling `CommitDistributedSnapshot` once the job
  reaches a consistent state.
- Optionally drive task assignment and liveness checks by dialing the `Worker`
  service on each registered host.

## Worker role
- Maintain a lightweight gRPC server implementing the `Worker` service.
- Accept task assignments from the coordinator and process local data into
  Iceberg data files.
- Produce manifest files referencing the generated data files, tagging them with
  the distributed snapshot identifier received from the coordinator.
- Call `ReportManifest` to push manifest metadata back to the coordinator.
- Periodically emit `Heartbeat` RPCs (or respond to coordinator initiated ones)
  to advertise health and progress metrics.

## Interaction with iceberg-go distributed snapshots
1. The coordinator loads the target table through `internal/iceberg.Client` and
   invokes `BeginDistributedSnapshot`. The resulting snapshot metadata and
   manifest list path are distributed to the workers through the gRPC API.
2. Workers use the provided snapshot identifier when writing manifests via the
   extended functionality of the forked `iceberg-go` library. Each manifest is
   persisted locally or in shared storage and then reported back to the
   coordinator.
3. After collecting manifests from all workers, the coordinator performs the
   final `CommitDistributedSnapshot`, passing the snapshot metadata plus the
   ordered manifest paths, to atomically publish the ingest.
