# HTTP API reference

All endpoints are served by the Gateway (default `http://localhost:8080`). For a guided tour, see
[Quick Start](./QUICKSTART.md).

## Index a document

`POST /api/v1/index`

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

Writes route to the document's single primary index node; see
[Document Ownership](./DOCUMENT_OWNERSHIP.md).

## Search

`POST /api/v1/search`

```json
{
  "query": "time travel romance",
  "page": 0,
  "pageSize": 10,
  "partitionId": "movies",
  "searchType": "HYBRID",  // BM25 | SEMANTIC | HYBRID
  "fusionStrategy": "RRF", // SCORE_SUM | WEIGHTED | RRF
  "filters": [
    { "field": "year", "operator": "GTE", "values": ["2000"] }
  ],
  "facets": [
    { "field": "genre", "size": 10 },
    { "field": "year", "size": 5 }
  ]
}
```

## Sorting and cursor pagination

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

**Indexes created before field sorting need a reindex.** The universal sort tie-breaker reads
DocValues on the document id, which are written at index time. Segments written before that existed
have no such values, so documents in them cannot be ordered or traversed reliably. Reindex through
the [alias workflow](#admin-index-schema-and-aliases) to pick them up.

## Exact document retrieval

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

## Document count

`GET /api/v1/index/count?partitionId=...` reads committed Lucene counts from one eligible replica of
every logical shard. It reports unavailable or failed logical shards explicitly; a partial
observation is never presented as a zero count.

## Admin: index schema and aliases

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
