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
| `internal/raft` | Raft-style leader election **and log replication** (terms, randomised timeouts, RequestVote, AppendEntries / heartbeats, election restriction, majority commit) behind a `Transport` interface |
| `internal/scheduler` | Task queue, least-loaded dispatch, retries (max 3 attempts), dead-worker detection and task reassignment; applies committed Raft log entries |
| `internal/worker` | Worker gRPC service, heartbeat loop, shell-command executor with timeout |
| `cmd/scheduler` | Runs one scheduler replica; `-data-dir` persists Raft state across restarts |
| `cmd/worker` | Runs one worker; `-scheduler` accepts a comma-separated list of replica addresses |
| `cmd/submit` | CLI to submit a task; tries each replica until it finds the leader |
| `cmd/status` | CLI to view a replica's tasks and worker count |
| `cmd/chaos` | End-to-end failure-injection test (random worker + leader kills under load) |

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

# terminal 6: submit a task (supports priority and capability tags)
go run ./cmd/submit -id task1 -payload "echo hello && sleep 1" -schedulers localhost:8000,localhost:8001,localhost:8002 -priority high -tag gpu

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
- [x] Persistent Task Storage & High-Performance Write-Ahead Log (WAL) with CRC32 integrity verification
- [x] Precision Scheduled & Delayed Tasks (`-delay`, `-run-at`)
- [x] Dynamic Raft Cluster Membership Changes (§6 `AddPeer`, `RemovePeer`) without cluster restart
- [x] Task Cancellation & Abort Propagation (`submit -cancel`) to terminate in-flight executions
- [x] Distributed Cron & Recurring Task Engine (standard 5-field cron parsing, Raft log replication, persistent snapshot state)

## License

MIT
