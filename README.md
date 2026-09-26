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

`dsearch` is a horizontally scalable, Lucene‑based distributed search engine written in Java 21.

`main` is the authoritative integration and release branch. Development commits use the root Maven
`revision` with a `-SNAPSHOT` suffix; a release sets that one value to its final version, tags the
`main` commit as `v<version>`, and publishes images tagged with the same version.

It targets **small to medium‑sized applications** that need:
- **Lexical search (BM25)**
- **Semantic search (vector kNN)**
- **Hybrid ranking (BM25 + embeddings)**

…without the operational and conceptual overhead of a full Elasticsearch / OpenSearch cluster.

---

## Overview

The system is composed of three primary components:

- **Gateway Node**  
  Spring Boot HTTP API entrypoint, load balancing and routing, and system health.

- **Query Nodes**  
  gRPC services that **fan‑out queries to all index nodes for a given partition**, merge partial results, and apply hybrid fusion strategies.

- **Index Nodes**  
  gRPC services hosting **Lucene shards**. Each index node can host multiple partitions (e.g. `shard-movies`, `shard-shows`, …), and sharded across multiple nodes. 
  Each partition is a Lucene index responsible for a categorical or domain‑specific slice of your data.

- **Coordinator Node**  
  Optional, durable membership and topology authority. It atomically persists the topology epoch,
  version, membership, health, and leases to `serviceDiscovery.coordinatorStateFile` (plus a
  backup), exposes health-aware discovery, removes expired non-coordinator leases, and runs the
  bounded replica-repair loop described below.

### Sharding & Load Balancing

- Shards represent **logical partitions** of your data (e.g., by domain or category).
- Each logical primary range has a configurable, explicit replica set.
- Every document has **exactly one primary index node**, derived from the document key:
  - For a given `(partitionId, documentId)` write/delete, the Gateway routes to
    `owner = argmax hash(nodeId, partitionId, documentId)` over the configured index nodes
    (rendezvous hashing). A Lucene upsert is local to one node, so sending an update anywhere
    else would leave the cluster holding two copies of the same document.
  - The mapping is a pure function of the key and `app-config.yaml`, so it is identical in
    every Gateway process and across restarts, and it does not move when nodes go unhealthy.
  - If the primary is unavailable the mutation fails with `503`; clients never promote a replica for writes.
  - Hashing also spreads documents evenly across nodes without a coordinator.
  - `GET /api/v1/index/count?partitionId=...` reads committed Lucene counts from one eligible
    replica of every logical shard. It reports unavailable or failed logical shards explicitly;
    a partial observation is never presented as a zero count.
  - See [Document Ownership](./docs/DOCUMENT_OWNERSHIP.md) for restart and topology‑change behavior.

The Gateway writes the primary first and then its deterministic followers. `one`, `quorum`,
and `all` acknowledgement policies are supported; acknowledged-consistency failover requires
`all`. Query fanout selects exactly one eligible copy of each logical range, so replicas do
not multiply hits, totals, or facets.

### Exact document retrieval

Retrieve a document without constructing a search query:

```bash
curl 'http://localhost:8080/api/v1/index/doc-123?partitionId=movies'
```

`GET /api/v1/index/{id}` treats `{id}` as an exact Lucene term, so characters such as `:` and
`*` have no query-parser meaning. It returns the logical `partitionId`, document `id`, and
stored `fields`. The gateway selects the same eligible primary-or-replica logical range as other
reads; `404` therefore means the selected authoritative copy confirmed absence. If that range has
no eligible copy, the endpoint returns `503` rather than claiming the document is absent. With
`readConsistency: acknowledged` and `durabilityPolicy: all`, a successful upsert is immediately
retrievable and a successful delete is immediately absent, including after replica failover.

The coordinator exposes a versioned, health-aware node registry. `RegisterNode` creates or updates
membership, `Heartbeat` renews a previously registered node's lease without creating membership,
and `GetShardMap` returns the generation-fenced primary and replicas for each logical `index/<nodeId>` range. Those
    RPCs return the durable topology epoch and monotonic version so clients can reject stale state.
    For replicated layouts, the coordinator also compares committed manifests and transfers a
    verified snapshot before a restarted or divergent node is returned to discovery.

This design intentionally keeps the system:
- **Simple to operate** (few moving parts)
- **Easy to reason about**
- **Horizontally scalable** by just adding more index/query nodes and updating config.

---

## Architecture Overview

```text
          +--------------+
          |   Client     |
          +------+-------+
                 |
           HTTP /search
                 |
        +--------v---------+
        |    Gateway Node  |           
        | (Spring Boot API)|             
        +--------+---------+           
                 |                            +--------v----------+
                 |                            | Coordinator Node  |
                 |----------------------------| - registry        |
                 |                            | - node membership |
                 | gRPC QueryService          | - health checks   |
                 |                            +--------+----------+
        +--------v---------+             
        |    Query Nodes   |             
        |  - Fan-out RPCs  |          
        |  - Merge Results |
        +--------+---------+
                 |
                 | gRPC IndexService
                 |
   +-----------------------------+
   |        Index Nodes          |
   |-----------------------------|
   | shard-0 | shard-1 | shard-2 |
   |  Lucene |  Lucene |  Lucene |
   +-----------------------------+
```

---

## Search Flow (BM25 + Semantic Search with Lucene kNN)

The engine supports **two complementary retrieval modes** that can be used independently or combined:

1. **Lexical Search (BM25)** – classic keyword relevance
2. **Semantic Search (Embeddings + Lucene kNN)** – retrieves results by *meaning*, not by exact words
3. **Hybrid Search** – fuses BM25 and semantic scores into a single ranked list

### 1. Document Indexing

For each document:

- All textual fields are concatenated into a single representation.
- A dense embedding is generated using a default transformer model:
  - `all-MiniLM-L6-v2` via DJL (`textEmbedding` model in `app-config.yaml`).
  - or any other compatible model you configure.
- The document is stored in Lucene as:
  - A **BM25 text field** for lexical search.
  - A **vector field** (`KnnVectorField`) for semantic similarity.

This enables BM25, semantic, and hybrid retrieval over the **same underlying data**.

### 2. Query Execution Modes

#### A. BM25 Search (Keyword)

- Lucene processes the query using BM25.
- Best for **exact keywords**, short queries, and when you care about precise term matches.
- Typically low‑latency.

#### B. Semantic Search (Embedding kNN)

- Query text is embedded using the same transformer model as indexing.
- Query Node fans out to all Index Nodes that host the requested shard/category.
- Each Index Node runs Lucene HNSW‑based kNN over its vector field.
- Results are merged and sorted by semantic similarity.
- Great for **natural‑language queries** and conceptual similarity (e.g., “space opera about time dilation”).

#### C. Hybrid Search (BM25 + Semantic Fusion)

To combine lexical and semantic signals:

1. Run BM25 search → top K
2. Run semantic kNN search → top K
3. Merge hits by document ID
4. Apply a fusion strategy:
   - **RRF**: Reciprocal Rank Fusion - rank‑based blending (default)
   - **score_sum:** bm25Score + semanticScore
   - **weighted:** α·bm25 + β·semantic
5. Paginate the fused list and return to the client.

---

## Quick Start

For full details, see [Quick Start Guide](./docs/QUICKSTART.md).

### Prerequisites

- Java 21
- Maven 3.9+
- (Optional) `k6` / `ghz` for load/latency testing

### Build & Run (multi‑node demo cluster)

```bash
# from repo root
make build

# Start a local cluster with 2 index nodes, 2 query nodes, 1 coordinator, and 1 gateway
make run-multi

# Cluster layout (by default):
#  - Index Nodes : 5000, 5001
#  - Query Nodes : 6000, 6001
#  - Coordinator  : 7000
#  - Gateway     : http://localhost:8080
```

### Example HTTP API Requests

#### Search Request

```json
{
  "query": "time travel romance",
  "page": 0,
  "pageSize": 10,
  "partitionId": "movies",
  "searchType": "HYBRID",  // BM25 | SEMANTIC | HYBRID
  "fusionStrategy": "RRF" // SCORE_SUM | WEIGHTED | RRF
  "filters": [
    {
      "field": "year",
      "operator": "GTE",
      "values": ["2000"]
    }
  ],
  "facets": [
    {
      "field": "genre",
      "size": 10
    },
    {
      "field": "year",
      "size": 5
    }
  ]
}
```

#### Sorting and cursor pagination

`sort` orders results by any field marked `sortable` in `fieldConfigs`, plus the pseudo-fields
`_score` (relevance) and `_id` (document id). Components apply most-significant first, and `_id ASC`
is always appended, so the ordering is **total**: no two documents ever tie, and the same query
always returns the same order. Documents with no value for a sort field order last, in both
directions.

```json
{
  "query": "time travel romance",
  "pageSize": 10,
  "partitionId": "movies",
  "searchType": "BM25",
  "sort": [
    { "field": "year", "order": "desc" },
    { "field": "rating", "order": "asc" }
  ]
}
```

A sorted response carries `nextCursor`. Send it back as `cursor` to get the following page:

```json
{
  "query": "time travel romance",
  "pageSize": 10,
  "partitionId": "movies",
  "searchType": "BM25",
  "sort": [{ "field": "year", "order": "desc" }],
  "cursor": "v1.CgQIARAB.9tYw…"
}
```

`cursor` and `page` are mutually exclusive. `nextCursor` is absent once a page comes back shorter
than `pageSize`, which is how a traversal ends.

**Why a cursor rather than a deeper page.** Offset paging has to ask *every* index node for
`page × pageSize + pageSize` hits, because any one node could own the whole page; that is why
`requestLimits.maxResultWindow` caps it. A cursor pins an exact position in a total order, so each
node only has to return the `pageSize` hits after it. Page 500 costs exactly what page 1 costs, and
`maxResultWindow` does not apply.

**When a cursor is refused.** The cursor is opaque, versioned, and signed. It is rejected with an
explicit error — never a plausible-looking wrong page — when it was tampered with, when the query,
filters, sort, page size, or index schema changed since it was issued (`400 Bad Request`), or when
an alias swap moved the partition to a different index generation (`412 Precondition Failed`).

Cursor pagination requires `BM25` with a leading field sort. Two shapes cannot support it and say
so explicitly:

- **Ordering by `_score`.** Lucene computes BM25 from each node's *local* term statistics, so a
  score boundary means something different on every node. Score ordering still works; it just
  cannot be resumed.
- **`SEMANTIC` and `HYBRID` search.** Both rank within a bounded per-node nearest-neighbour
  candidate pool rather than a total order over the partition, so resuming past the pool would stop
  returning documents that exist. Use offset paging for these.

**Consistency under concurrent writes.** A traversal is not a snapshot. Each page reads whatever
the shards have committed at the moment it runs, so a document indexed after the traversal started
appears only if it sorts after the current cursor position, and a document deleted mid-traversal
simply stops appearing. Because the ordering is total and the cursor names an exact position,
concurrent writes never cause a *duplicate* or a skipped document among the rows that existed and
kept their sort values for the whole traversal — only the newly written and deleted rows differ.
Updating a document's sort field moves it, so it may be seen twice or not at all, exactly as it
would with any non-snapshot cursor. `totalHits` is captured when the traversal starts and reported
unchanged on every resumed page, so the denominator does not drift while a client pages through.

**Partial failures.** If a node fails or times out mid-traversal, the page is still returned and
`fanout` reports the shortfall; the hits that node would have contributed are missing from that page
and are not recovered by continuing. Treat a `PARTIAL_FAILURE` fanout status during a traversal as a
signal to restart it.

**Cursor signing key.** `pagination.cursorSigningKey` must be identical on every query node: a
gateway load-balances the pages of one traversal across nodes, so a per-node key makes page two fail
signature verification. Leaving it blank generates a process-local key and logs a warning, which is
fine only for a single-node cluster.

#### Admin index schema and aliases

Administrative create-index, inspect-schema, analyze, reindex, and atomic
alias-swap operations require `Authorization: Bearer $DSEARCH_ADMIN_TOKEN`.
Each call returns an auditable result (`auditId`, actor, operation, from/to
index, status) and appends a JSON line to `dsearch.admin.audit-log`.

```bash
# Create a named index and alias
curl -H "Authorization: Bearer $DSEARCH_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"indexName":"movies_1","alias":"movies"}' \
  http://localhost:8080/api/v1/admin/indexes

# Inspect the persisted schema (fields, analyzer, embedding identity/digest/dimension)
curl -H "Authorization: Bearer $DSEARCH_ADMIN_TOKEN" \
  http://localhost:8080/api/v1/admin/indexes/movies/schema

# Preview how the index's actual analyzer tokenizes sample text (read-only;
# bounded by requestLimits.maxAnalyzeTextBytes/maxAnalyzeTokens; sample text is
# never logged or persisted to the audit log)
curl -H "Authorization: Bearer $DSEARCH_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"text":"Interstellar (2014)"}' \
  http://localhost:8080/api/v1/admin/indexes/movies/analyze

# Rebuild into a distinct target, verify counts and representative queries.
# The source alias stays live until swap succeeds.
curl -H "Authorization: Bearer $DSEARCH_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"targetIndex":"movies_2","verificationQueries":[{"query":"interstellar","searchType":"BM25","size":10}]}' \
  http://localhost:8080/api/v1/admin/indexes/movies/reindex

# Atomically point the alias at the verified target
curl -H "Authorization: Bearer $DSEARCH_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"alias":"movies","targetIndex":"movies_2"}' \
  http://localhost:8080/api/v1/admin/aliases/swap

# Restore the previous alias target
curl -H "Authorization: Bearer $DSEARCH_ADMIN_TOKEN" \
  -X POST http://localhost:8080/api/v1/admin/aliases/movies/rollback
```

Index and query nodes persist `dsearch-schema.json` beside each Lucene generation
and refuse to open or serve a generation whose field types, analyzer, embedding
model identity/digest, or vector dimension do not match the running process.
The diagnostic names the mismatched property.

#### Index Request

```json
{
  "partitionId": "movies",
  "id": "movie_001",
  "fields": {
    "title": "Interstellar",
    "content": "A team of explorers travel through a wormhole...",
    "year": "1999",
    "genre": "sci-fi",
    "rating": "8.7",
    "createdAt": "915148800000"
  }
}
```

---

## Observability

`dsearch` is instrumented end‑to‑end so you can see what your cluster is doing under load.

### Gateway (HTTP, Spring Boot + Micrometer)

The Gateway uses **Spring Boot Actuator + Micrometer + Prometheus registry**:

- **Metrics endpoints**
  - `GET http://localhost:8080/actuator/metrics` – metric catalog
  - `GET http://localhost:8080/actuator/metrics/dsearch.search.http` – HTTP search handler metric
  - `GET http://localhost:8080/actuator/metrics/dsearch.index.http` – HTTP index handler metric
  - `GET http://localhost:8080/actuator/prometheus` – Prometheus scrape endpoint

  These endpoints and `/actuator/info` require `Authorization: Bearer $DSEARCH_ADMIN_TOKEN`.
  Configure `DSEARCH_ADMIN_TOKEN` for the gateway in Compose before scraping. A missing token
  disables non-health Actuator access. Only `/actuator/health`, `/actuator/health/liveness`,
  and `/actuator/health/readiness` remain unauthenticated for probes; the existing `/health`
  and `/cluster/health` routes remain available. Keep the bearer token out of logs and client code.

- **Key metrics** (examples):
  - `dsearch.search.http` – high‑level timing for the `/api/v1/search` handler.
  - `dsearch.gateway.search.latency{searchType,shardId}` – fine‑grained latency per search type and shard.

### gRPC Nodes (Query / Index)

Both Query Nodes and Index Nodes expose **Prometheus‑compatible `/metrics` endpoints** (via Prometheus Java client):

- JVM metrics (GC, memory, threads) via `DefaultExports.initialize()`
- gRPC server metrics via `PrometheusGrpcServerInterceptor`:
  - `dsearch_grpc_server_latency_seconds{service,method,status}`
  - `dsearch_grpc_server_requests_total{service,method,status}`

On the Gateway side, gRPC clients are instrumented with a **Prometheus gRPC client interceptor**:

- `dsearch_grpc_client_latency_seconds{component,service,method,status}`
- `dsearch_grpc_client_requests_total{component,service,method,status}`

This lets you compare **client‑side** vs **server‑side** latency per RPC method and component (e.g. `gateway->query-node`).

### Health Endpoints

- **Gateway** – aggregated health across itself and downstream nodes
  - `GET /health` – Gateway health
  - `GET /cluster/health` – Overall Cluster health

- **Query Nodes / Index Nodes** – each exposes a lightweight HTTP health check endpoint
  - `GET /health` – Node health

### Manual resilience exercise

Behaviour under overload, exhausted admission capacity, slow or lost downstreams, index disk-full
and read-only storage, coordinator restart, and rolling node replacement is exercised by
`make resilience`. See [Operability](./docs/OPERABILITY.md) for the scenarios, the evidence it
records, and its manual run instructions.

---

## Benchmarks

Detailed methodology and raw results are documented in [Benchmarks](./docs/BENCHMARKS.md).

See the benchmarks document for:
- `k6` HTTP load‑test scripts for Gateway
- `ghz` gRPC benchmarks for Query Node / Index Node
- How to reproduce and extend these benchmarks on your own hardware

---

## Configuration

Cluster configuration is defined in `app-config.yaml` and loaded into the Gateway and nodes at startup:

```yaml
serviceDiscovery:
  enabled: true
  refreshIntervalSeconds: 30

indexNodes:
  routingStrategy: "LEAST_LOADED"
  componentLabel: "dsearch-index-node"
  replicationFactor: 2
  durabilityPolicy: "all"          # one | quorum | all
  readConsistency: "acknowledged" # available | acknowledged
  nodes:
    - id: "0"
      host: "localhost"
      port: 5000
      healthPort: 5100

queryNodes:
  routingStrategy: "ROUND_ROBIN"
  componentLabel: "dsearch-query-node"
  nodes:
    - id: "0"
      host: "localhost"
      port: 6000
      healthPort: 6100

coordinatorNodes:
  routingStrategy: "ROUND_ROBIN"
  componentLabel: "dsearch-coordinator-node"
  nodes:
    - id: "0"
      host: "localhost"
      port: 7000
      healthPort: 7100

ml:
  models:
    textEmbedding:
      url: "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2"
      engine: "PyTorch"

pagination:
  # Must be identical on every query node, or a traversal breaks when the gateway
  # routes page two elsewhere. Blank generates a process-local key and warns.
  cursorSigningKey: ""
  maxSortFields: 8

replicaRepair:
  enabled: true
  intervalSeconds: 15
  chunkBytes: 262144
  maxSnapshotBytes: 1073741824
  bandwidthBytesPerSecond: 10485760
  maxConcurrentRepairs: 1
```

- `pagination.cursorSigningKey` signs opaque search cursors. Set one shared value in any cluster
  with more than one query node; see *Sorting and cursor pagination* above.
- `pagination.maxSortFields` bounds sort components per request, before the id tie-breaker.
- `indexNodes.routingStrategy` currently supports **`LEAST_LOADED`** for read-client selection;
  document mutations use deterministic ownership and cardinality comes from Lucene, not gateway deltas.
- `queryNodes.routingStrategy` currently supports **`ROUND_ROBIN`** for fan‑out queries across multiple query node instances.
- With discovery enabled, index and query clients require a versioned coordinator response before
  routing. During a coordinator outage they may use only a previously accepted topology for
  `maxStalenessSeconds`; they do not silently return to the configured static node list.
- The coordinator persists its membership/topology state at `coordinatorStateFile` (or the
  `COORDINATOR_STATE_FILE` override), supports registration, lease heartbeats, health checks,
  expiry, discovery, the replicated logical shard-map RPC, and bounded replica repair. Range
  rebalancing after changing the configured ownership ring remains a separate operator workflow.

---

## Limitations & Roadmap

This project is intentionally minimal and educational. Some trade‑offs and potential future work:

- **Replica repair does not rebalance the ownership ring**
  - The coordinator repairs missing, lagging, wrong-generation, and checksum-divergent copies of
    an existing replica set. Adding/removing ownership nodes still requires explicit range handoff.
- **Coordinator membership remains the placement authority**
  - Heartbeat and shard-map RPCs provide durable membership and eligibility; a repaired target is
    published only after its committed position and canonical content checksum match the source.
  - Expired node leases are removed from coordinator discovery. Node removal, shard relocation,
    and data rebalancing remain manual operational work.
- **Indexes created before field sorting need a reindex**
  - The universal sort tie-breaker reads DocValues on the document id, which are written at index
    time. Segments written before that existed have no such values, so documents in them cannot be
    ordered or traversed reliably. Reindex through the alias workflow to pick them up.
- **No rebalancing when the index-node list changes**
  - Adding or removing an entry under `indexNodes` reassigns part of the document ownership
    ring, and nothing moves the affected documents, so the change requires a reindex.
  - Future direction: coordinator-driven handoff of the reassigned key range.

---

## License

This repository is intended as an educational and portfolio project.
This project is licensed under the MIT License. 

See the [LICENSE](LICENSE) file for details.
