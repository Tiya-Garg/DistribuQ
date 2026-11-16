# DistribuQ

A fault-tolerant distributed task scheduler written in Go. Multiple scheduler replicas elect a leader via a Raft-inspired protocol; the leader replicates every task event (submit, assign, complete, fail) to followers before acknowledging it; workers heartbeat all replicas so whichever one wins the next election can dispatch immediately. Inter-node consensus, worker dispatch, and CLI communications are fully implemented with gRPC and Protocol Buffers.

## Requirements

- **Go 1.26+** (uses Go standard library, `google.golang.org/grpc`, and `google.golang.org/protobuf`)

## Architecture

```
                 ┌────────────┐   RequestVote / AppendEntries   ┌────────────┐
   submit ──────▶│ scheduler  │ ◀─────────────────────────────▶│ scheduler  │
   (client)      │  (leader)  │         replicated log          │ (follower) │
                 └─────┬──────┘             (gRPC)              └────────────┘
          dispatch     │  ▲ register / heartbeat /
          (gRPC)       │  │ complete / fail (gRPC)
                 ┌─────▼──┴───┐   ┌────────────┐
                 │  worker 1  │   │  worker 2  │ ...
                 └────────────┘   └────────────┘
```

| Package | Responsibility |
| --- | --- |
| `proto` | Protocol Buffer definitions and generated gRPC clients/servers for Raft, Scheduler, and Worker services |
| `internal/raft` | Raft consensus engine: leader election, log replication, snapshot compaction, and dynamic membership |
| `internal/scheduler` | Task queue, least-loaded dispatch, priority scheduling, DAG dependencies, DLQ, and deduplication |
| `internal/worker` | Worker gRPC service, heartbeat loop, concurrency limits, and shell executor |
| `internal/cron` | Distributed cron engine for scheduled recurring task execution |
| `internal/wal` | High-performance write-ahead log with CRC32 checksums for durable persistence |
| `internal/autoscaler` | Dynamic worker autoscaler with queue-depth driven scaling and scale-to-zero |
| `internal/webhook` | Webhook event notification dispatcher with HMAC-SHA256 signing and backoff retries |
| `internal/security` | Mutual TLS (mTLS 1.3) certificate loading and transport credentials |
| `internal/metrics` | Prometheus metrics registry and HTTP export handler (`/metrics`) |
| `internal/dashboard` | Live cluster dashboard and real-time Server-Sent Events (SSE) monitor |
| `cmd/scheduler` | Runs one scheduler replica; `-data-dir` persists Raft and scheduler state across restarts |
| `cmd/worker` | Runs one worker; `-scheduler` accepts a comma-separated list of replica addresses |
| `cmd/submit` | CLI to submit, delay, cancel, or deduplicate tasks; routes to the active leader |
| `cmd/status` | CLI to view a replica's tasks and worker count |
| `cmd/chaos` | End-to-end failure-injection test (random worker + leader kills under load) |
| `cmd/bench` | Cluster benchmarking tool measuring throughput (req/sec) and p50/p90/p95/p99 latency percentiles |

The `raft.Transport` interface and the scheduler's `dispatchFn` are injected, decoupled from the underlying transport. Node-to-node consensus, worker orchestration, and client interactions are powered by **gRPC and Protocol Buffers**.

## Task lifecycle

```
queued ──▶ assigned ──▶ completed
   ▲           │
   └───────────┤  worker failure / dispatch error / dead worker
               ▼  (requeued while attempts < MaxRetries)
             failed
```

Every state transition is first committed as a log entry to a majority of scheduler replicas. On leader re-election, in-flight (assigned) tasks are requeued so surviving workers pick them up again.

## Running locally

```bash
# terminal 1–3: three-node scheduler cluster (with durable state and optional web dashboard)
mkdir -p /tmp/distribuq-state
go run ./cmd/scheduler -id s1 -addr :8000 -peers s2=:8001,s3=:8002 -data-dir /tmp/distribuq-state -http :8080
go run ./cmd/scheduler -id s2 -addr :8001 -peers s1=:8000,s3=:8002 -data-dir /tmp/distribuq-state
go run ./cmd/scheduler -id s3 -addr :8002 -peers s1=:8000,s2=:8001 -data-dir /tmp/distribuq-state

# open web dashboard in browser: http://localhost:8080

# terminals 4 and 5: workers register with every replica so failover is seamless (supports tags and concurrency limits)
go run ./cmd/worker -id w1 -addr :9001 -scheduler localhost:8000,localhost:8001,localhost:8002 -tags gpu,highmem -max-concurrent 4
go run ./cmd/worker -id w2 -addr :9002 -scheduler localhost:8000,localhost:8001,localhost:8002 -tags general

# terminal 6: submit a task (supports priority, capability tags, and idempotency deduplication)
go run ./cmd/submit -id task1 -payload "echo hello && sleep 1" -schedulers localhost:8000,localhost:8001,localhost:8002 -priority high -tag gpu -idempotency-key order-101

# duplicate submission with same key is safely deduplicated without re-executing
go run ./cmd/submit -id task1-retry -payload "echo hello && sleep 1" -schedulers localhost:8000,localhost:8001,localhost:8002 -idempotency-key order-101

# check cluster state (ask any replica)
go run ./cmd/status -scheduler localhost:8000
```

A single-node setup still works: omit `-peers` and the single scheduler is immediately its own leader.

Pass `-data-dir DIR` to persist each replica's Raft term, vote, and replicated log. A restarted replica recovers its state and rejoins without violating safety invariants (it can't vote twice in the same term, and a stale node can't win an election against nodes with a more up-to-date log).

## Tests

```bash
go test -race ./... -v
```

Covers:
- Single-leader election; no two nodes claim leadership simultaneously
- Followers converge on the current leader's ID
- Re-election after the leader stops
- Follower log repair on conflict (prevLog consistency check)
- Minority partition cannot commit (log entry stays uncommitted in a 1-of-3 partition)
- Stale node cannot win election (election restriction / log up-to-date check)
- Committed task survives leader kill and persists on new leader
- Restarted node recovers full log from disk
- No double vote after restart (and a vote refused when it can't be saved)
- Election abandoned when term/vote can't be persisted
- Worker registers with all schedulers; re-registration on heartbeat rejection
- Task completion/failure report reaches whichever scheduler owns the task
- Least-loaded dispatch; task reassignment when a worker stops heartbeating
- Shell executor: success, non-zero exit, timeout, empty payload

## Chaos test

Builds all binaries, then runs *rounds* of:

1. Submit a batch of tasks.
2. Kill a random **worker** mid-execution, restart it.
3. Wait for all tasks in the batch to run.
4. Kill the current **leader**, restart it from persisted state; wait for a new leader.

```bash
make build
go run ./cmd/chaos -rounds 3 -tasks 10 -workers 3
```

The harness exits non-zero if any task never ran. Tasks running more than once are reported but are not failures — reassignment after a worker death is at-least-once by design.

## Benchmarks & Performance Harness

DistribuQ includes a full benchmarking suite spanning microbenchmarks and an end-to-end cluster load runner (`cmd/bench`):

```bash
# Run all microbenchmarks (WAL, Raft, scheduler queues, worker execution, Protobuf, metrics)
make test-bench

# Run cluster benchmark tool (submits tasks concurrently, measuring throughput and latency percentiles)
go run ./cmd/bench -schedulers localhost:8000,localhost:8001,localhost:8002 -tasks 1000 -concurrency 16
```

### Measured Subsystem Latencies
- **WAL Append (with fsync)**: ~1.2ms per synchronous disk sync with CRC32 integrity checks.
- **Raft Consensus Commit (Single-Node)**: ~7.2µs per committed proposal.
- **Scheduler Task Ingestion**: ~1.0µs per queued task in-memory.
- **Protobuf Marshal / Unmarshal**: ~0.6µs per `SubmitTaskRequest`.
- **Metrics Ingestion**: ~19ns per atomic counter update.

## Current limitations

This is a portfolio/learning prototype. Known gaps:

- **At-least-once execution.** A slow (not declared dead) worker can cause a task to run twice: it finishes after being declared dead and reassigned. This is expected and reported by the chaos harness.
- **Single-term sequential dispatch.** Tasks are dispatched by the leader only; followers serve status reads but don't dispatch. This is correct but means throughput scales with a single leader's capacity.

## Security & Mutual TLS (mTLS)

DistribuQ supports optional end-to-end mutual TLS (TLS 1.3) with client & server certificate verification against a shared root CA across all cluster transport connections:
- Raft peer consensus transport
- Worker heartbeat and task execution registration
- Task submission (`submit`) and inspection (`status`) CLI commands

### Flags
All binaries accept the following TLS flags:
- `-ca-cert`: Path to PEM-encoded Root CA certificate verifying peers.
- `-tls-cert`: Path to PEM-encoded certificate for this node or client.
- `-tls-key`: Path to PEM-encoded private key for this node or client.
- `-server-name`: (Optional) Expected TLS Server Name (SAN) when dialing.

When `-ca-cert` and `-tls-cert` are omitted, communication defaults to insecure plaintext for local evaluation.

## Task Output & Log Streaming

DistribuQ provides real-time task output and log streaming from execution workers to the cluster state:
- **Worker Execution Streaming**: Real-time stdout and stderr output chunk capture from executing commands.
- **Durable Snapshot & Raft Replication**: Persisted log storage bounded in scheduler memory and replicated across Raft cluster logs.
- **REST API Endpoint**: Query execution output at `GET /api/tasks/{id}/logs`.
  Returns JSON: `{"task_id": "...", "logs": [{"worker_id": "w1", "content": "...", "is_stderr": false, "timestamp": 123456789}]}`.
- **CLI Commands**: View real-time or historical execution logs using `submit -logs <task-id>` and `status -logs <task-id>`.
  ```bash
  # Submit a task and fetch logs
  ./bin/submit -logs <task-id>
  ./bin/status -logs <task-id>
  ```
- **Dashboard Modal**: Interactive log viewer modal embedded directly in the live web UI table.

## Dynamic Worker Autoscaling & Scale-to-Zero

DistribuQ features an integrated worker autoscaler that monitors queue backlog depth and dynamically scales workers:
- **Queue-Driven Scaling**: Dynamically evaluates queue backlog against configurable targets (`-autoscale-target-queue`).
- **Scale-to-Zero**: Automatically drains all worker nodes when the cluster is idle past `-autoscale-zero-idle`.
- **Cooldown & Smoothing**: Protects against thrashing using separate scale-up and scale-down cooldown timers.
- **CLI & Web Telemetry**: View live scaling decisions and target capacities via `status -autoscaler` or the web dashboard.

## Webhooks & Event Notifications

DistribuQ includes an asynchronous event notification system:
- **Event Categories**: Real-time events emitted for `task.completed`, `task.failed`, `task.dlq`, and `worker.offline`.
- **HMAC-SHA256 Signing**: Every delivery includes an `X-DistribuQ-Signature: sha256=...` header using a shared secret.
- **Reliable Delivery**: Automatic exponential backoff retries on transmission failure.
- **CLI & REST Subscriptions**: Register webhook targets via `submit -subscribe-webhook <url>`, `status -webhooks`, or `POST /api/webhooks`.

## Roadmap

- [x] Real task execution backend (shell executor with timeout)
- [x] Status CLI
- [x] Ignore stale completion/failure reports from non-assigned workers
- [x] Persist Raft term/vote and replicated log (`-data-dir`)
- [x] Workers register and heartbeat with all scheduler replicas
- [x] Replicate task state to followers (AppendEntries, majority commit, log repair)
- [x] Chaos test harness (random worker/leader kills under load)
- [x] GitHub Actions CI with race detector and chaos test
- [x] Log compaction / snapshotting (Raft §7 log truncation, InstallSnapshot RPC, and durable snapshots)
- [x] Swap `net/rpc` for gRPC (Protobuf schemas, Raft/Scheduler/Worker gRPC transports)
- [x] Multi-level priority task scheduling (CRITICAL, HIGH, NORMAL, LOW with preemption)
- [x] Real-time live web dashboard & cluster monitor (embedded dark-mode UI with Server-Sent Events)
- [x] Dynamic worker registration, capability tags, telemetry heartbeats & graceful drain
- [x] Mutual TLS (mTLS) Authentication & Encryption for cluster transport and CLI commands
- [x] Task Retries, Exponential Backoff & Dead Letter Queue (DLQ) with Replay/Purge APIs
- [x] End-to-end Idempotent Task Submissions with wire-level deduplication
- [x] Persistent Task Storage & High-Performance Write-Ahead Log (WAL) with CRC32 integrity verification
- [x] Precision Scheduled & Delayed Tasks (`-delay`, `-run-at`)
- [x] Dynamic Raft Cluster Membership Changes (§6 `AddPeer`, `RemovePeer`) without cluster restart
- [x] Task Cancellation & Abort Propagation (`submit -cancel`) to terminate in-flight executions
- [x] Distributed Cron & Recurring Task Engine (standard 5-field cron parsing, Raft log replication, persistent snapshot state)
- [x] Prometheus Metrics Endpoint (`/metrics`) with task counters and live cluster gauges
- [x] Task Dependencies & Directed Acyclic Graph (DAG) Workflows (`submit -depends-on`, topological unblocking, cycle detection)
- [x] Task Deduplication & Idempotency Keys (`submit -idempotency-key`, duplicate detection, conflict rejection)
- [x] Task Output & Log Streaming (`submit -logs <task-id>`, gRPC streaming, snapshot persistence, live UI viewer)
- [x] Dynamic Worker Autoscaling & Scale-to-Zero (`status -autoscaler`, queue-driven scaling, process provisioner)
- [x] Webhooks & Event Notification System (`submit -subscribe-webhook`, `status -webhooks`, HMAC signing, Slack payload formatting)
- [x] Benchmarking & Performance Harness (`cmd/bench`, microbenchmarks across all core subsystems, CI validation)

## License

MIT
