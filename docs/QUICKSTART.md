
# Quickstart

This quickstart guide will help you set up a multi-node cluster of the Vector Search Engine on your local machine.

--- 

### Prerequisites
- Java 21+

---

### Start a local multi-node cluster

Package the project:
```bash
make build
```

Spin up a full multi-node cluster:

```bash
make run-multi
```

This starts:
- 2 Index Nodes (ports 5000, 5001)
- 2 Query Nodes (ports 6000, 6001)
- Gateway (HTTP API at http://localhost:8080)

### Coordinator state in Docker Compose

`docker-compose.yml` mounts the named `coordinator-state` volume at `/data` in
the coordinator container. The authoritative state file is
`/data/coordinator-topology.properties`; its same-version backup is
`/data/coordinator-topology.properties.bak`. Recreating the coordinator
container retains both files because the named volume is not removed by
`docker compose rm` or `docker compose up --force-recreate`.

The coordinator writes a fully synchronized temporary file and atomically
replaces the authoritative state file. It preserves the topology epoch and
monotonic topology version across ordinary restarts. It never starts with a
fresh epoch when an existing state file is corrupt, truncated, or from an
unsupported format version: startup fails and requires deliberate recovery.

Coordinator state alone is not a recoverable search backup: it must be from
the same consistency boundary as every Lucene shard and the schema/model
metadata used to build and query those shards. Use the supported manifest,
snapshot, empty-deployment restore, and public-query drill in
[RECOVERY.md](RECOVERY.md). Do not copy a live volume or replace coordinator
files independently to recover a cluster.

The coordinator persistence tests exercise the same backup-and-restore flow
and verify that corrupt and incompatible state fails explicitly rather than
silently resetting topology.

The default test suite covers the persistence contract. To exercise a real
container replacement against a fresh named volume, run:

```bash
mvn test -pl dk.coordinator -am \
  -Dtest=CoordinatorContainerRestartIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddsearch.docker.it=true
```

### Index sample data

```bash
cd benchmark
make ingest
```

### Run a search

```bash
curl -X POST http://localhost:8080/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "time travel romance",
    "page": 0,
    "pageSize": 10,
    "shardId": "0",
    "searchType": "HYBRID"
  }'
```

This returns BM25 + Semantic fused results.

### Select stored fields in each hit

Use `storedFields` as an explicit allowlist when result cards do not need the complete stored
document. `title` and `content` select the legacy top-level hit properties with those names;
highlights are returned only for selected fields. Other selected values appear under `fields`.
Unknown or missing stored fields are simply absent.

```bash
curl -X POST http://localhost:8080/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "time travel romance",
    "partitionId": "0",
    "pageSize": 10,
    "storedFields": ["title", "year"]
  }'
```

The three request shapes are deliberately distinct:

- omit `storedFields` to preserve the legacy response containing every stored user field;
- send `"storedFields": []` to return only each hit's `docId`, `score`, and internal traversal
  metadata, while response totals, facets, fanout status, and cursors remain available;
- send names to return only those stored fields.

The same contract is available to gRPC callers through
`QueryRequest.stored_field_selection` and `IndexSearchRequest.stored_field_selection`. The wrapper
message is presence-aware: omit it for legacy behavior or send an empty wrapper for no user
fields. A selection is bounded by `requestLimits.maxFieldsPerDocument` (100 by default); names must
be non-blank, unique, and no larger than `requestLimits.maxFieldValueBytes` in UTF-8. Invalid
selections return HTTP 400 or gRPC `INVALID_ARGUMENT` with the applicable bound.

Projection is a response-shaping option, not part of ranking. Sorting, filtering, facets, hit
order, `totalHits`, and cursor traversal therefore do not change when fields are excluded. A
caller may change `storedFields` between cursor pages; the query, filters, sort, search type,
fusion strategy, page size, schema, and index generation remain cursor-bound.

### Delete one document with a retry identity

Replicated deletes accept the same stable identity and generation contract as upserts:

```bash
curl -X DELETE \
  'http://localhost:8080/api/v1/index/doc-1?partitionId=0&operationId=delete-doc-1-v4&generation=4'
```

Reuse both values when retrying an uncertain response. Omitting them creates a new operation, which is
appropriate for an ordinary one-off delete but not for replaying a timed-out operation after another
client may have written a newer version. The successful response returns the effective `operationId`
and `generation`. A caller that permits concurrent writes should allocate its positive monotonic
generation before the first attempt.

### Delete documents in bulk

`POST /api/v1/index/bulk-delete` accepts identity-bearing delete items and returns one ordered outcome
per item, including duplicate document ids:

```bash
curl -X POST http://localhost:8080/api/v1/index/bulk-delete \
  -H "Content-Type: application/json" \
  -d '{
    "partitionId": "0",
    "items": [
      {"id": "doc-1", "operationId": "delete-1", "generation": 4},
      {"id": "doc-2", "operationId": "delete-2", "generation": 9},
      {"id": "doc-1", "operationId": "delete-3", "generation": 5}
    ]
  }'
```

```json
{
  "success": true,
  "items": [
    { "requestIndex": 0, "id": "doc-1", "status": "success", "error": null,
      "operationId": "delete-1", "generation": 4 },
    { "requestIndex": 1, "id": "doc-2", "status": "success", "error": null,
      "operationId": "delete-2", "generation": 9 },
    { "requestIndex": 2, "id": "doc-1", "status": "success", "error": null,
      "operationId": "delete-3", "generation": 5 }
  ]
}
```

The legacy `"ids": ["doc-1", ...]` request remains accepted. Under replication, the gateway generates
an operation identity and primary generation for each such item and includes known values in its item
response. Prefer `items` with caller-assigned values when a whole HTTP response could be lost.

A missing document deletes as a `success`, a duplicate id is never rejected as a conflict, and each
occurrence is reported independently. A durably deleted document is immediately absent from subsequent
search. These document-level semantics do not make a newly generated delete equivalent to retrying an
older operation after an intervening upsert.

The list is bounded by `requestLimits.maxBulkItems` (100 by default, shared with
`/api/v1/index/bulk`); an over-budget request returns HTTP 413 before any document is deleted. A
blank or oversized id is reported per item as `validation_failure` without blocking the rest of the
batch. An item whose owning node is unavailable, or whose delete outcome is unknown because of a
timeout, is reported `retryable_failure` — resend its `operationId` and `generation`, when present,
instead of creating a new delete. Reaching the shared
request deadline (`requestLimits.requestTimeoutMillis`) marks every unattempted item
`retryable_failure` too, while items already durably committed keep their `success` outcome.

### Sort and page with a cursor

Order by any field marked `sortable` in `fieldConfigs`. A document-id tie-breaker is appended
automatically, so the order is total and stable across requests.

```bash
curl -X POST http://localhost:8080/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "time travel romance",
    "pageSize": 10,
    "partitionId": "0",
    "searchType": "BM25",
    "sort": [{ "field": "year", "order": "desc" }]
  }'
```

The response carries `nextCursor`. Send it back as `cursor` — with the same query, filters, sort,
and `pageSize` — to walk forward without duplicates or gaps:

```bash
curl -X POST http://localhost:8080/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "time travel romance",
    "pageSize": 10,
    "partitionId": "0",
    "searchType": "BM25",
    "sort": [{ "field": "year", "order": "desc" }],
    "cursor": "PASTE_NEXT_CURSOR_HERE"
  }'
```

Stop when a response has no `nextCursor`. `cursor` cannot be combined with `page`, and changing the
query, filters, sort, or `pageSize` mid-traversal invalidates the cursor rather than silently
returning results from a different result set. See [API reference](./API.md#sorting-and-cursor-pagination) for the full rules, including why
`SEMANTIC` and `HYBRID` search use offset paging instead.

### Stop the cluster

```bash
make stop
```
