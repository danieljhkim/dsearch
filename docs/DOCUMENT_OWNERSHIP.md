# Document Ownership and Replication

Every document has one authoritative primary range and a configurable set of replica
copies. This page defines placement, acknowledgement, failover, migration, and what the
gateway's document counters do and do not mean.

## Placement rule

Lucene upserts are local, so uncoordinated copies would make updates ambiguous and
inflate hits, document totals, and facets. Replication therefore creates explicit copies
of a logical primary range; it is never inferred from duplicate requests.

```
primary(partitionId, documentId) = argmax over configured index nodes of
                                   hash64(nodeId, partitionId, documentId)
```

`DocumentOwnership` implements this rendezvous-hash rule. `ReplicaPlacement` chooses
`replicationFactor - 1` distinct followers deterministically from the same configured
node set. Every range is named `index/<primaryNodeId>`, carries the coordinator topology
generation, and lists the primary first. Ties use the lexicographically smallest node id,
so every process computes the same result independent of input iteration order.

The candidate set is `indexNodes.nodes[].id` from configuration, captured when a client
manager is built. Health is volatile; placement is not. A runtime-discovered node outside
that configured set cannot accept replicated traffic until an explicit rebalance changes
the placement contract.

## Writes, idempotency, and fencing

Every replicated upsert and delete carries an `operation_id`, monotonic
`operation_generation`, placement generation, primary id, target id, and explicit
primary/replica role. The gateway commits the primary first and then its followers. Generation
ownership is explicit:

- when an HTTP index request supplies `generation`, the caller owns ordering and must provide a
  positive, monotonically increasing value for that document; the primary and every replica use
  that exact value;
- when `generation` is omitted, the gateway sends protobuf `operation_generation = 0` only to the
  declared primary. Under the same
  per-shard commit lock used for the mutation, the primary allocates one plus that document's last
  committed generation, commits the mutation and fence together, and returns the positive value in
  `committed_generation`;
- the gateway forwards that returned positive value to followers and returns it in the HTTP index
  response. Followers reject the zero allocation sentinel. Gateway clocks, process age, and restart
  history therefore never participate in generated ordering.

Concurrent gateways share the same primary allocation point for a document. Retrying an omitted-
generation request with the same `operation_id` while it is still the latest document operation
returns the already committed generation, so an uncertain primary response can be retried without
allocating another operation. Reusing a committed generation with another identity or mutation type
remains a conflict, and explicitly supplied values below the committed generation remain stale.

HTTP upserts and deletes both accept `operationId` and `generation`. Bulk delete accepts
identity-bearing `items` as well as the legacy `ids` form, and returns the effective identity and
generation for every replicated success or retryable item whose primary generation is known. A retry
means resending those same values. Omitting them creates a new delete operation; doing that after a
timeout can legitimately delete a newer document version. Clients that coordinate concurrent
mutations must assign positive monotonic generations before the first attempt. An old explicit
generation is rejected after a newer mutation, preserving the newer document; it is not silently
promoted into a new delete.

Index nodes durably retain the latest identity and generation per document:

- duplicate delivery of the same identity is an idempotent success;
- reordered operations and stale placement generations are rejected;
- a target mismatch is rejected, and a follower cannot accept a primary-role write;
- a primary outage rejects writes instead of allowing a client-side promotion;
- retrying the same operation after an uncertain response either completes missing copies or is
  rejected as stale once a newer operation has already made those copies converge; it never becomes a
  newer mutation.

`indexNodes.durabilityPolicy` selects when the gateway may acknowledge:

- `one`: the primary commit. Followers may lag, so only `readConsistency: available` is
  valid.
- `quorum`: a majority of the configured replica set. A minority may lag, so only
  `readConsistency: available` is valid.
- `all`: every configured copy. This is required by `readConsistency: acknowledged`; any
  eligible failover copy then contains every acknowledged write.

The runtime rejects `readConsistency: acknowledged` with `one` or `quorum`, and rejects a
replication factor larger than the configured eligible-node set. A failure after the
primary commit but before the selected threshold produces no successful client
acknowledgement. A network partition therefore cannot create a writable second primary.

The mutation fence is stored per physical shard in Lucene commit user data. Lucene publishes
the document changes and the complete per-document identity/generation map through the same
checksummed `segments_N` commit point and durably syncs that commit before returning. A restart
therefore observes both the mutation and its fence or neither; there is no separately replaced
process-wide ledger that can lose another shard's update. Metadata format, entry count, encoded
identities, positive generations, and mutation types are validated at startup. Missing,
incomplete, conflicting, or unknown replication metadata prevents the index manager from
starting rather than serving without a fence. The former `replication-mutations.properties`
ledger is accepted only as a strictly validated upgrade source, committed into each shard, and
then removed.

If a replicated commit returns an uncertain failure, that shard is write-fenced for the lifetime
of the manager and its uncommitted writer state is rolled back on close. Restart resolves the
outcome from the latest valid Lucene commit. The coordinator repair loop compares each physical
copy's logical partition, primary identity, placement generation, maximum per-document operation
generation, document count, and canonical content checksum. The maximum is diagnostic, not a shard
commit position that can order two different histories. A missing copy can receive a full checksummed
snapshot from matching observed copies. Existing copies with conflicting manifests remain ineligible
and are preserved for manual resolution; automatic repair cannot establish which one retained every
acknowledged write.

## Reads and failover

Query fanout selects the first eligible copy of each logical range, preferring its primary
and then deterministic followers. Exactly one copy of every range is queried, even when
several ranges fail over to the same physical node. Hits, total counts, and facets are
therefore aggregated once per logical shard rather than once per copy.

If neither a primary nor any follower is eligible, the range remains explicitly unavailable
in the read plan. It is never silently omitted: the response reports a non-success fanout
status and a separate unavailable-logical-range count (rather than classifying it as a failed
downstream call). This applies to both read modes. With `acknowledged` and `all` durability,
any eligible copy contains acknowledged writes; with `available`, the chosen eligible copy may
lag a write acknowledged under `one` or `quorum`. Neither mode permits a response to claim
success when any logical range has no eligible copy.

With `readConsistency: acknowledged` and `durabilityPolicy: all`, losing a primary does
not lose acknowledged writes. With `available`, a selected replica may omit writes that
were acknowledged under `one` or `quorum`; this is the declared consistency tradeoff.
Stale-generation replicas remain fenced from writes in either mode.

Exact `GET /api/v1/index/{id}?partitionId=...` follows this same read plan for the one logical
range that owns `(partitionId, id)`. The id is sent to Lucene as a `Term`, not through the query
parser. A selected copy's `NOT_FOUND` becomes HTTP `404`; an unavailable or ineligible logical
range becomes HTTP `503` and is never inferred from document counters. The request carries the
same partition identity used by mutations, so aliases resolve through the existing index-manager
path on the chosen physical copy.

## Topology and observability

`GetShardMap` exposes placement generation, primary/replica role, eligibility, current repair
records, acknowledgement policy, read consistency, and under-replicated count. `GetReplicaRepairs`
and `ControlReplicaRepairs` let an authenticated operator inspect, pause/resume, or retry repair
without editing topology files. `GET /cluster/health`
includes the bounded replication summary and failover count. Prometheus metrics cover
under-replication, failover, apply outcomes, missing acknowledgements, and
acknowledgement/apply latency without document- or shard-id labels.

Coordinator topology mutations increment the durable topology version. Restart accepts
the previous version-1 state format and rewrites it in version 2, retaining the existing
epoch/version monotonicity and node-lease fencing.

## Migration and cluster changes

`replicationFactor: 1` preserves the historical on-disk partition names and single-copy
behavior. For a factor above one, each logical primary range uses a distinct deterministic
physical partition on every replica. That isolation is what lets query fanout select one
copy without mixing ranges.

Existing single-copy partitions are not silently relabelled. Before increasing the
factor, reindex or restore every partition into the replicated layout, verify the admin
topology reports zero under-replicated ranges, and then switch clients. Rolling back to
factor one likewise requires reindexing into the historical layout.

Adding or removing an index node changes some primary ranges and follower sets. Use the
online handoff/rebalance workflow; editing configuration alone never moves Lucene data.
An unavailable follower remains explicitly under-replicated until repair completes. Transfer
staging is durable across target restarts and is resumed at the exact accepted byte offset. The
target rejects foreground mutations while its shard is staged; the source takes its snapshot under
the normal per-shard commit lock, and writes that race after that point are detected by the next
manifest comparison rather than silently admitted as converged.

## Restarts, deletes, ids, and counters

Placement is recomputed from configuration and the document key, so it is identical across
gateway restarts. A delete for a missing id is an idempotent success. The gateway mints a
document id before placement when the client omits one, ensuring later updates use the same
key.

`GET /api/v1/index/count?partitionId=...` is the product cardinality endpoint. It queries the
current committed Lucene count from exactly one eligible replica per logical shard (primary when
eligible, otherwise the deterministic failover copy) and sums those observations once. It has no
gateway mutation history or restart-persisted delta. The response names unavailable logical shards
and selected shards whose count RPC failed; its numeric count is therefore only complete when both
lists are empty, and a missing observation is never substituted with zero. The count is a
point-in-time read rather than a cluster snapshot: writes committed while fanout runs may be
reflected by some shards and not others.
