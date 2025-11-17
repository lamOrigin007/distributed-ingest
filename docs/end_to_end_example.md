# End-to-end ingest example

This walkthrough wires a single coordinator with one worker and exercises the
complete distributed ingest flow entirely on `localhost`. It uses the provided
binaries but the same gRPC calls can be scripted from any client.

## 1. Start the coordinator

```bash
COORDINATOR_ADDRESS=:50051 \
ICEBERG_CATALOG_NAME=example \
ICEBERG_CATALOG_TYPE=rest \
ICEBERG_CATALOG_URI=http://localhost:8181 \
ICEBERG_WAREHOUSE=file:///tmp/warehouse \
./bin/coordinator
```

The coordinator process allocates job identifiers, tracks manifests reported by
workers and commits the final distributed snapshot once enough manifests arrive.
It also runs a watchdog that fails jobs whose workers stop reporting progress
before their `job_timeout_seconds` window elapses.

## 2. Start a worker

```bash
WORKER_ID=worker-1 \
WORKER_ADDRESS=:50052 \
COORDINATOR_ADDRESS=localhost:50051 \
WORKER_DATA_BASE=/tmp/worker-data \
./bin/worker
```

Each worker hosts a gRPC service that accepts `AssignTasks` requests. In this
example the static data source emits synthetic files from `WORKER_DATA_BASE`.
The worker caches completed `(job_id, task_id)` pairs so that retries of the
same assignment simply re-report the existing manifest path.

## 3. Kick off a job

A control-plane component (or a quick `grpcurl` call) starts a job. The timeout
controls how long the coordinator waits for fresh manifests before failing the
run.

```bash
grpcurl -d '{
  "table": {"catalog":"example","namespace":"analytics","table":"events"},
  "requester": "demo-user",
  "job_timeout_seconds": 600
}' localhost:50051 api.Coordinator/StartJob
```

The response contains `job_id` and the reserved distributed snapshot metadata.

## 4. Assign tasks to workers

In a production deployment the coordinator would push work automatically. For
this example we emulate the scheduler by calling the worker directly:

```bash
grpcurl -d '{
  "worker_id": "worker-1",
  "job_id": "<job_id from StartJob>",
  "table": {"catalog":"example","namespace":"analytics","table":"events"},
  "snapshot": {"snapshot_id": 1234, "commit_uuid": "<uuid from StartJob>"},
  "tasks": [{"task_id":"task-1"},{"task_id":"task-2"}]
}' localhost:50052 api.Worker/AssignTasks
```

The worker builds manifest files for each task, stores the completed manifest
paths locally, and invokes `ReportManifest` on the coordinator. Duplicate
`AssignTasks` calls are harmless: the cached manifest paths are reported again
without reprocessing the data.

## 5. Monitor job status

Clients can poll the new `GetJobStatus` RPC to observe progress and timeouts:

```bash
grpcurl -d '{"job_id":"<job_id>"}' localhost:50051 api.Coordinator/GetJobStatus
```

The response includes the job state (Pending/Running/Committing/Completed), the
number of manifests accepted so far, and timestamps for creation, last progress
and the current deadline.

## 6. Commit the distributed snapshot

Once `ready_to_commit` becomes `true`, the coordinator can finalize the job. If
`ExpectedManifestCount` was configured correctly the service may commit
automatically after the final `ReportManifest`, but it is also possible to call
`CommitJob` explicitly:

```bash
grpcurl -d '{"job_id":"<job_id>"}' localhost:50051 api.Coordinator/CommitJob
```

The final status transitions to `COMPLETED`. Any late manifest retries are
acknowledged and ignored because both the worker cache and the coordinator's
manifest set enforce idempotency.

With these steps you now have a reproducible blueprint for bootstrapping the
coordinator/worker topology, exercising retries, and verifying that the
coordinator publishes a single atomic Iceberg snapshot at the end of the run.
