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
1. The coordinator loads the target table through the `internal/iceberg`
   wrapper. The wrapper hides catalog initialization (REST/Glue/etc.), exposes a
   `TableClient` interface, and translates the forked `iceberg-go` API into data
   structures that are convenient for the coordinator and workers.
2. `BeginDistributedSnapshot` is invoked once per job. The coordinator stores
   the returned descriptor (snapshot id, optional parent id, commit UUID, and
   snapshot properties) in an in-memory `JobManager`. The gRPC `StartJob`
   response returns the snapshot id and commit UUID so workers can derive
   manifest names deterministically.
3. Workers use the provided snapshot identifier when writing manifests via the
   extended functionality of the forked `iceberg-go` library. Each manifest is
   persisted locally or in shared storage and then reported back to the
   coordinator.
4. After collecting manifests from all workers, the coordinator performs the
   final `CommitDistributedSnapshot`, passing the snapshot metadata plus the
   ordered manifest paths, to atomically publish the ingest.

## Commit readiness and conflict handling
The coordinator tracks ingest progress inside the in-memory `JobManager`.
Each job stores the manifests that have been reported as well as a configurable
threshold of how many manifests are required before it can be committed. The
default threshold is one manifest, but tests or orchestration layers can adjust
the value to match the number of worker tasks. Every call to `ReportManifest`
updates the job state and, once the threshold is reached, the coordinator
automatically invokes `CommitDistributedSnapshot`. External orchestrators can
still call the `CommitJob` RPC to force a commit; the RPC is idempotent when the
job is already in the `COMPLETED` state.

During the commit phase the coordinator relies on Iceberg's optimistic
concurrency controls. If another writer updates the same table before the
distributed snapshot is published, the Iceberg client returns an error that the
coordinator maps to the `CONFLICT` job status and surfaces as an `ABORTED`
gRPC error. Operators must rebuild the distributed snapshot (or restart the job)
after resolving the underlying contention. All other commit failures transition
the job to the `FAILED` status so the orchestrator can diagnose or retry.
