# dsearch (Distributed Search Engine)

<p align="center">
  <img src="https://img.shields.io/badge/Java-21-007396?style=for-the-badge&logo=openjdk&logoColor=white">
  <img src="https://img.shields.io/badge/Apache_Lucene-9.x-orange?style=for-the-badge&logo=apache&logoColor=white">
  <img src="https://img.shields.io/badge/gRPC-1.x-34A7C1?style=for-the-badge&logo=grpc&logoColor=white">
  <img src="https://img.shields.io/badge/Spring_Boot-3.x-6DB33F?style=for-the-badge&logo=springboot&logoColor=white">
  <img src="https://img.shields.io/badge/Vector_Search-Embeddings-4B0082?style=for-the-badge&logo=target&logoColor=white">
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge">
  </a>
</p>

`dsearch` is a horizontally scalable, Lucene-based distributed search engine in Java 21. It gives
small to medium-sized applications **BM25**, **vector kNN** and **hybrid** search without the
operational weight of an Elasticsearch / OpenSearch cluster.

## Architecture

```text
             Client
               │ HTTP
     ┌─────────▼─────────┐        ┌───────────────────┐
     │      Gateway      │◄──────►│    Coordinator    │
     │ (Spring Boot API) │        │ membership, leases│
     └─────────┬─────────┘        │ shard map, repair │
               │ gRPC             └───────────────────┘
     ┌─────────▼─────────┐
     │    Query nodes    │  fan out, merge, fuse
     └─────────┬─────────┘
               │ gRPC
   ┌───────────▼─────────────┐
   │       Index nodes       │
   │ shard-0 shard-1 shard-2 │  Lucene
   └─────────────────────────┘
```

| Component | Role |
| --- | --- |
| **Gateway** | HTTP API, routing, load balancing, cluster health. |
| **Query nodes** | Fan a query out to every index node holding the partition, merge results, apply hybrid fusion. |
| **Index nodes** | Host Lucene shards. A node can hold many partitions (e.g. `movies`, `shows`), each a Lucene index for one domain slice. |
| **Coordinator** *(optional)* | Durable membership and topology authority: persists epoch, version, membership, health and leases to `serviceDiscovery.coordinatorStateFile`; serves health-aware discovery; expires leases; repairs replicas. |

### Ownership and replication

- Every document has **exactly one primary** index node, chosen by rendezvous hashing over
  `(nodeId, partitionId, documentId)`. The mapping depends only on the key and `app-config.yaml`,
  so every gateway agrees across restarts and it never moves when a node goes unhealthy.
- If the primary is down, the write fails with `503`; replicas are never promoted for writes.
- The gateway writes the primary, then its deterministic followers, under a `one`, `quorum` or `all`
  acknowledgement policy. Acknowledged-consistency failover requires `all`.
- Reads pick exactly one eligible copy per logical range, so replicas never duplicate hits, totals
  or facets.
- Nodes join with `RegisterNode` and renew leases with `Heartbeat`. The coordinator's `GetShardMap` returns a generation-fenced primary and replicas per range, with
  a monotonic version so clients reject stale topology. It restores a restarted or divergent
  replica from a verified snapshot before returning it to discovery.

Details: [Document Ownership](./docs/DOCUMENT_OWNERSHIP.md), [Recovery](./docs/RECOVERY.md).

## Search modes

Each document is stored once with both a **BM25 text field** and a **vector field**, embedded at
index time (default `all-MiniLM-L6-v2` via DJL; configurable).

| `searchType` | How it ranks | Best for |
| --- | --- | --- |
| `BM25` | Lucene keyword relevance | Exact terms, short queries |
| `SEMANTIC` | HNSW kNN over the query embedding | Natural-language, conceptual queries |
| `HYBRID` | Top-K of both, merged by id, then fused | Mixed intent |

Hybrid `fusionStrategy`: `RRF` (reciprocal rank fusion, default), `SCORE_SUM`, or `WEIGHTED`
(α·bm25 + β·semantic).

## Quick start

Requires Java 21 and Maven 3.9+ (optionally `k6` / `ghz` for load tests).

```bash
make build
make run-multi   # 2 index (5000-5001), 2 query (6000-6001), coordinator (7000), gateway (:8080)
```

Index a document, then search:

```bash
curl -H 'Content-Type: application/json' http://localhost:8080/api/v1/index -d '{
  "partitionId": "movies", "id": "movie_001",
  "fields": {"title": "Interstellar", "content": "Explorers travel through a wormhole...", "year": "2014"}
}'

curl -H 'Content-Type: application/json' http://localhost:8080/api/v1/search -d '{
  "query": "space travel", "partitionId": "movies", "pageSize": 10,
  "searchType": "HYBRID", "fusionStrategy": "RRF",
  "filters": [{"field": "year", "operator": "GTE", "values": ["2000"]}],
  "facets":  [{"field": "genre", "size": 10}]
}'
```

The API also supports field sorting with signed cursor pagination, exact document retrieval,
counts, bulk operations, and token-protected admin endpoints for schema inspection, reindexing and
atomic alias swaps. See the [API reference](./docs/API.md) and [Quick Start Guide](./docs/QUICKSTART.md).

## Configuration

Everything lives in `app-config.yaml`, loaded by the gateway and every node:

```yaml
serviceDiscovery:
  enabled: true
  refreshIntervalSeconds: 30
  maxStalenessSeconds: 30           # how long to trust the last topology if the coordinator is down

indexNodes:
  routingStrategy: "LEAST_LOADED"     # read-client selection
  componentLabel: "dsearch-index-node"
  replicationFactor: 2
  durabilityPolicy: "all"             # one | quorum | all
  readConsistency: "acknowledged"     # available | acknowledged
  nodes:
    - { id: "0", host: "localhost", port: 5000, healthPort: 5100 }

queryNodes:
  routingStrategy: "ROUND_ROBIN"
  componentLabel: "dsearch-query-node"
  nodes:
    - { id: "0", host: "localhost", port: 6000, healthPort: 6100 }

coordinatorNodes:
  routingStrategy: "ROUND_ROBIN"
  componentLabel: "dsearch-coordinator-node"
  nodes:
    - { id: "0", host: "localhost", port: 7000, healthPort: 7100 }

ml:
  models:
    textEmbedding:
      url: "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2"
      engine: "PyTorch"

pagination:
  cursorSigningKey: ""   # must match on every query node; blank = process-local key + warning
  maxSortFields: 8

replicaRepair:
  enabled: true
  intervalSeconds: 15
  chunkBytes: 262144
  maxSnapshotBytes: 1073741824
  bandwidthBytesPerSecond: 10485760
  maxConcurrentRepairs: 1
```

With discovery enabled, clients route only from a versioned coordinator response. During a
coordinator outage they reuse the last accepted topology for up to `maxStalenessSeconds`, never
the static node list.

## Operations

- **Metrics**: gateway exposes Actuator/Prometheus (bearer-token protected); query and index nodes
  expose `/metrics` with JVM and gRPC client/server latency. See [Observability](./docs/OBSERVABILITY.md).
- **Health**: `/health` on every node, `/cluster/health` on the gateway.
- **Resilience**: `make resilience` exercises overload, disk-full, slow downstreams, coordinator
  restart and rolling replacement. See [Operability](./docs/OPERABILITY.md).
- **Benchmarks**: `k6` (HTTP) and `ghz` (gRPC) scripts and results in [Benchmarks](./docs/BENCHMARKS.md).

## Limitations

- **No automatic rebalancing.** Changing the `indexNodes` list reassigns part of the ownership ring
  and nothing moves the affected documents, so it requires a reindex. Node removal and shard
  relocation are manual. Replica repair fixes copies within an existing replica set only.
- **Pre-sorting indexes need a reindex.** Segments written before field sorting lack the id
  DocValues the sort tie-breaker needs; reindex through the alias workflow.

## Releases

`main` is the integration and release branch. Development uses the root Maven `revision` with a
`-SNAPSHOT` suffix; a release sets it to the final version, tags `v<version>`, and publishes images
with the same tag.

## License

An educational and portfolio project, licensed under [MIT](LICENSE).
