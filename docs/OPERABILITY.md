# Manual operability exercise: overload, storage exhaustion, and rolling restarts

`scripts/docker-cluster-resilience.sh` is a manual resilience exercise. It stands up the Docker Compose
topology exactly as `scripts/docker-cluster-e2e.sh` does — production mTLS profile, hardened
containers, the same four images — and then drives the deployment through seven faults that
production actually produces: request overload, exhausted admission capacity at two tiers, a slow
downstream, an ungracefully lost downstream, an index volume with no write headroom, read-only
index storage, a coordinator restart, and a rolling replacement of every query and index
container.

The functional end-to-end exercise (`scripts/docker-cluster-e2e.sh`) proves the deployment is correct
at steady state and can be recovered from a snapshot. This exercise proves it stays *bounded and
honest* while it is broken, and that capacity comes back on its own afterwards. Both share
`scripts/lib/docker-cluster.sh`.

## The four invariants

Every scenario asserts the same four things. A scenario that cannot assert all four is not part of
this exercise.

1. **Bounded.** Every request returns a definite HTTP status inside its budget. The budget is
   `requestLimits.requestTimeoutMillis` from `app-config.docker.yaml` plus a fixed allowance for
   TLS setup and JVM scheduling. A client-side timeout (`http_code` 000) is a failure, not a slow
   pass.
2. **Explicit.** A `200` must carry real fan-out metadata (`SUCCESS` or `PARTIAL_FAILURE` with node
   counts) or an acknowledged mutation. A non-`200` must carry a structured error document with a
   message. An empty result set that merely looks successful fails the exercise.
3. **Intact.** Once the fault is removed, all acknowledged writes are searchable *exactly once*.
   The exercise re-runs a marker query and compares both the total hit count and the full sorted set of
   document ids, so loss and duplicate application both fail.
4. **Self-healing.** Capacity returns without operator action and without resetting the
   coordinator. The coordinator state volume is never deleted, the epoch must not change, and the
   topology version must never regress.

## Scenarios and how each fault is injected

| Scenario | Injection | What it proves |
| --- | --- | --- |
| `request-overload` | `docker compose pause` on both index nodes, then a concurrent search burst of `maxConcurrentHttpRequests + 64` | Gateway HTTP admission and query-node fan-out admission both shed load with `429` + `Retry-After`; nothing hangs; nothing reports an empty success |
| `slow-downstream` | `docker compose pause` on `index-node-1` | The frozen leg ends at the deadline and the response is an explicit `PARTIAL_FAILURE` with node counts, not a silently smaller result |
| `unavailable-downstream` | `docker kill --signal KILL` on `index-node-1` (restart policy disabled first) | An ungracefully lost node stays in the topology until the coordinator lease expires, reads continue at explicitly reduced capacity, writes owned by the lost node are refused rather than rerouted, and the node rejoins on restart |
| `index-disk-full` | Recreate `index-node-1` with `INDEX_NODE_MINIMUM_FREE_DISK_BYTES` above any achievable free space | The node reports readiness reason `disk_space_below_threshold`, the gateway reports `DEGRADED` and names `in1`, and requests stay bounded and explicit |
| `index-read-only-storage` | Revoke the write bit on `index-node-1`'s `/data/index`, then restart it | The running node reports readiness reason `lucene_directory_not_writable`; after a restart Lucene cannot open at all, so the node never claims readiness, the topology shrinks, and restoring the write bit recovers the shard |
| `coordinator-restart` | `docker compose stop coordinator`, then `start` | Nodes keep serving from their last observed topology, the gateway reports the missing coordinator, and the persisted epoch survives the restart |
| `rolling-restart` | `docker compose up --force-recreate --no-deps`, one container at a time | Replacing every query and index container preserves the dataset and never needs a coordinator reset |

`docker pause` is used for "slow": a `SIGSTOP`ed container still completes the TCP handshake, so the
fan-out leg can only end at the deadline. `docker kill` is used for "lost": a graceful stop makes a
node deregister itself, which would never exercise the membership lease. The read-only fault revokes
the write bit from inside the container — the Lucene volume is owned by the container identity, so
that identity can revoke its own access — rather than remounting it, which keeps the fault inside
the deployed topology instead of depending on how two Compose files merge a volume list.

The exercise reads `requestTimeoutMillis`, `maxConcurrentHttpRequests`, `maxConcurrentFanoutCalls`,
`nodeExpirySeconds`, and `refreshIntervalSeconds` from
`dk.common/src/main/resources/app-config.docker.yaml` instead of hard-coding them, so tightening a
limit tightens the exercise rather than silently invalidating it.

The burst is retried up to three times. On a small runner the shell can take long enough to fork
the burst that too few requests overlap; the retry keeps the gate deterministic in outcome without
weakening the assertion that both admission tiers must engage.

## Evidence

Everything lands in `$DSEARCH_RESILIENCE_DIAGNOSTICS` (default
`target/docker-resilience-diagnostics`). Preserve the directory when retaining evidence from a
manual exercise.

| Artifact | Contents |
| --- | --- |
| `fault-timeline.jsonl` | One JSON object per event with a UTC timestamp and elapsed seconds: `scenario_started`, `fault_injected`, `assertion_passed`, `recovery_complete`, `fault_removed`, `scenario_passed` |
| `resilience-report.json` | Machine-readable per-scenario record: fault injection and removal timestamps, recovery duration, scenario duration, and every assertion |
| `resilience-report.md` | The same record as a summary table plus the assertion list |
| `metrics/<scenario>-<phase>.prom` | Gateway `/actuator/prometheus` scrape with the admin bearer token before, during, and after each fault |
| `<scenario>-after-services.log` | Timestamped per-scenario Compose log slice |
| `compose.log`, `compose-ps.txt`, `container-inspect.json`, `compose-config.yaml` | Full container and service diagnostics at teardown |
| `bursts/request-overload-<attempt>/` | Per-request status, wall time, response body, and response headers for every request in the overload burst |

Recovery duration is measured from the moment the fault is removed until the cluster serves a
full-fan-out search again, and is recorded per scenario as `recoverySeconds`.

## Running it

```bash
# Requires docker, docker compose, openssl, curl, jq, and xargs.
scripts/docker-cluster-resilience.sh

# Or through the Makefile, which also names the diagnostics directory.
make resilience
```

The script generates a run-local `DSEARCH_ADMIN_TOKEN` if none is supplied, passes it to the
Compose gateway, and uses it for metrics snapshots. Gateway health and readiness probes do not
require this token. Production scrapers must provide the token as a bearer credential.

The script builds the four images locally, owns its own Compose project name, and tears the project
down — including volumes — on exit. It publishes the otherwise-unpublished node health endpoints on
`19070` (coordinator), `19081` (query-node-0), `19090` (index-node-0), and `19091` (index-node-1) so
it can read each service's exact readiness reason rather than only the aggregated gateway view. The
gateway stays on `19080`.

Useful environment variables:

- `DSEARCH_RESILIENCE_DIAGNOSTICS` — evidence directory.
- `DSEARCH_COMPOSE_PROJECT` — override the generated Compose project name.
- `DSEARCH_GATEWAY_URL` — gateway base URL (default `http://localhost:19080`).

Budget on a two-core runner: roughly 25 minutes of scenarios after image build and cluster startup.

## Availability

The repository does not run this Docker exercise in GitHub Actions. Run it manually when its
operability evidence is needed. Runtime is roughly 25 minutes on a two-core machine after image
build and cluster startup.

## Coordinator availability decision

This is an engineering evaluation target for the Docker Compose control plane, not an externally
promised SLA. It deliberately distinguishes serving a last accepted topology from accepting a new
authoritative topology.

| Failure scope | Allowed control-plane/data-plane effect | Recovery objective | Recovery point | Operator intervention | Drill evidence |
| --- | --- | --- | --- | --- | --- |
| Coordinator process or container loss with its `/data` volume intact | New membership and topology changes pause; reads continue only from a previously accepted topology and remain bounded and explicit | Coordinator and full fan-out capacity return within 240 seconds | No loss of the durable coordinator epoch or topology | None | `coordinator-restart` stops and starts the coordinator, checks a bounded search during loss, then checks epoch, non-regressing version, full fan-out, and the 240-second target. |
| Coordinator network partition from data-plane nodes | The same bounded-staleness reads may continue only for `serviceDiscovery.maxStalenessSeconds`; after that, discovery fails closed. No replacement coordinator is elected. | Connectivity restoration and coordinator rejoin within 240 seconds after the partition is removed | No new authoritative topology is accepted while isolated | None to heal a transient partition | The restart drill exercises the same unavailable-coordinator client path. A partition injector is not yet present, so a partition is explicitly not evidence for automatic failover. |
| Coordinator machine or durable-disk loss | Control-plane mutation is unavailable; data-plane requests remain bounded but are not an availability guarantee after their accepted topology becomes stale | One trained operator restores a verified backup into an empty deployment and completes public verification within 900 seconds | At most 300 seconds from the last durably acknowledged write to the recovery boundary | One operator performs the documented restore; no hand editing of state | `docker-cluster-e2e.sh` produces `recovery-report.json` and fails when its measured RPO or RTO exceeds the stated target. The empty-project restore is the machine/disk-loss recovery exercise. |

The target includes process, machine, durable-disk, and network-partition failures. It does not
claim transparent control-plane failover for the latter three: the target accepts bounded stale
data-plane service and a documented restore where necessary. A run must retain the generated
`resilience-report.json`, `recovery-report.json`, and `recovery-drill-record.md`; prose alone is
not evidence that the target was met.

### Current decision: retain one coordinator

The hardened single-coordinator design meets this target when both scripts pass with their default
thresholds (or stricter operator-supplied thresholds). Therefore **no coordinator-HA implementation
is required for this target**. This is not a claim of automatic machine-loss or partition failover.
If a drill exceeds a threshold, or an owner requires uninterrupted authoritative control-plane
writes during coordinator host/disk loss or partition, file one bounded coordinator-HA task before
selecting a consensus system. Its acceptance criteria must require a fault-injected active-leader
loss and partition test, a durable failover RPO/RTO report, fenced single-writer proof, and an
upgrade/rollback rehearsal.

### State and future HA boundary

The coordinator's minimal authoritative state is the durable topology epoch and monotonically
increasing version, member identities/endpoints/roles/health and lease timestamps, plus replica
placement and repair-control state. It is atomically written with a backup copy. Lucene shard data,
model caches, and the recovery manifest are data-plane/backup state rather than leader-election
state. A future HA design must replicate this state with linearizable single-writer semantics before
acknowledging membership, placement, repair-control, or topology changes.

Existing fencing is version and epoch based: clients reject a changed epoch or regressing topology
version, data-plane discovery fails closed once bounded staleness expires, and lost shard owners do
not have writes silently rerouted. This prevents a stale coordinator view from becoming an
unacknowledged writer, but it is not leader fencing across two coordinators. Any HA implementation
must add a durable fencing token checked by every mutating participant, reject stale leaders before
they serve writes, and preserve the current epoch/version compatibility contract during a rolling
upgrade. The deployment owner must operate quorum membership, backup/restore of consensus state,
certificate rotation, alerting, and a version-skew/rollback procedure; that permanent operational
cost is not justified by the target above.

When the exercise fails, start with `resilience-report.md` to find the scenario, then
`fault-timeline.jsonl` for the exact injection time, then the matching
`<scenario>-after-services.log` and `metrics/<scenario>-during.prom`.

## Related runbooks

### Replica repair controls

For a replicated layout, use the coordinator gRPC API `GetReplicaRepairs` to inspect the latest
bounded repair records. `ControlReplicaRepairs(action=pause|resume)` stops or restarts admission of
new transfers; `ControlReplicaRepairs(action=retry, repair_id=...)` clears a failed record for the
next reconciliation pass. A target remains outside index-node discovery until every expected shard
has the source's placement generation, committed position, and content checksum.

Repair work is bounded by `replicaRepair.chunkBytes`, `maxSnapshotBytes`,
`bandwidthBytesPerSecond`, `maxConcurrentRepairs`, and the per-RPC deadline. Target staging lives
under the index volume's `.replica-repair` directory, so an interrupted transfer resumes after a
target or coordinator restart. Do not delete that directory to retry; use the control RPC.

- [Snapshot, restore, and recovery drills](./RECOVERY.md) — the supported data recovery path.
- [Document ownership](./DOCUMENT_OWNERSHIP.md) — why mutations to a lost owner are refused instead
  of rerouted.
