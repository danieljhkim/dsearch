# Snapshot, restore, and recovery drills

This runbook is the supported recovery path for the Docker Compose deployment. A snapshot is an
offline, point-in-time copy of the coordinator epoch and topology, every Lucene shard, the exact
schema and model configuration, the Maven and Lucene versions, and each query/index node model
cache. `scripts/dsearch-recovery.sh` publishes an artifact only after every file has been
checksummed and the versioned manifest is complete.

The deployment has one copy of each shard. A snapshot therefore requires a short write outage. Do
not copy a live Lucene directory or a Docker volume by hand.

## Prerequisites and recovery contract

- Run from the exact source commit and configuration used by the deployment. Restore deliberately
  rejects even a byte-level configuration or root-POM difference.
- Install Docker with Compose, `curl`, `jq`, Git, and either `sha256sum` or `shasum`.
- Quiesce external writers before snapshot. The command verifies the public dataset, stops the
  gateway first so no new requests enter, stops the coordinator to freeze its epoch, and then
  gracefully stops query and index nodes. Closing each index node commits its Lucene writers.
- Store artifacts outside the checkout on durable storage. The examples use `/srv/dsearch-backups`.
- Give each drill or deployment a verification file whose marker query covers every expected
  document and whose three probes exercise BM25, vector, and hybrid search.

Example `recovery-verification.json`:

```json
{
  "schemaVersion": 1,
  "datasetId": "catalog-2026-08-30",
  "gatewayUrl": "http://localhost:19080",
  "lastAcknowledgedWriteAt": "2026-08-30T23:58:00Z",
  "documentCount": {
    "partitionId": "catalog",
    "query": "recovery-marker-2026-08-30",
    "expected": 125000
  },
  "queries": [
    {
      "name": "bm25",
      "partitionId": "catalog",
      "query": "lucene",
      "searchType": "BM25",
      "expectedDocIds": ["catalog-17"]
    },
    {
      "name": "vector",
      "partitionId": "catalog",
      "query": "semantic retrieval",
      "searchType": "SEMANTIC",
      "expectedDocIds": ["catalog-29"]
    },
    {
      "name": "hybrid",
      "partitionId": "catalog",
      "query": "lucene vectors",
      "searchType": "HYBRID",
      "fusionStrategy": "RRF",
      "expectedDocIds": ["catalog-41"]
    }
  ]
}
```

The marker is part of the recovery contract, not a generic match-all query: it makes the exact
expected document count observable through the public gateway. Update the dataset identifier,
timestamp, count, and probe IDs only after the corresponding writes are durably acknowledged.

## Take and validate a snapshot

Use a unique final path. Existing paths are never overwritten.

```bash
scripts/dsearch-recovery.sh snapshot \
  --project dsearch \
  --output /srv/dsearch-backups/catalog-2026-08-30 \
  --verification /etc/dsearch/recovery-verification.json

scripts/dsearch-recovery.sh validate \
  --snapshot /srv/dsearch-backups/catalog-2026-08-30
```

By default, the source deployment restarts after the snapshot. Pass `--leave-stopped` when the next
step is a restore drill. If the command is interrupted, it leaves only a sibling
`<output>.partial.<random>` staging directory, never the requested final path. Validation refuses
partial paths, a missing manifest, unlisted or missing files, symlinks, size changes, checksum
changes, unsupported formats, incompatible configuration, or incompatible Maven/Lucene metadata.

The manifest records:

- artifact ID, source commit and dirty-state evidence, creation time, and recovery point;
- coordinator state format, topology epoch/version, and index/query service names;
- exact runtime configuration, rendered Compose configuration, Maven revision, and Lucene version;
- all coordinator, Lucene, and model-cache files with byte size and SHA-256 checksum;
- the dataset ID, verification contract, and pre-snapshot gateway evidence.

Copy the entire final directory to durable storage. Do not copy a `.partial.*` directory and do not
edit `manifest.json`.

## Restore into an empty deployment

The repository currently assigns fixed container names, so remove the stopped source containers
while retaining their volumes before creating the isolated restore project:

```bash
DSEARCH_TLS_DIR=/etc/dsearch/tls \
  docker compose --project-name dsearch --file docker-compose.yml \
  down --remove-orphans

DSEARCH_TLS_DIR=/etc/dsearch/tls \
  scripts/dsearch-recovery.sh restore \
  --project dsearch-restore-20260830 \
  --snapshot /srv/dsearch-backups/catalog-2026-08-30 \
  --report /srv/dsearch-backups/restore-report-20260830.json
```

Restore refuses a destination project that already owns a container or volume. It validates the
complete artifact before creating anything, creates new project-scoped volumes, restores data into
those empty volumes, fixes the hardened service ownership, and starts the deployment. It then
requires the original coordinator epoch, a non-regressing topology version, the expected index-node
count, the exact public document count, and successful BM25, semantic-vector, and hybrid probes.
Only then does it write a passed report.

The source volumes are never mounted by the restore project. On interruption or failure, retain the
destination for diagnosis or remove only that project:

```bash
DSEARCH_TLS_DIR=/etc/dsearch/tls \
  docker compose --project-name dsearch-restore-20260830 --file docker-compose.yml \
  down --volumes --remove-orphans
```

Do not add `--volumes` when removing the source project unless the snapshot has been restored and
verified elsewhere.

## Recovery drill and failure cases

`scripts/docker-cluster-e2e.sh` is a manual recovery drill. It indexes a five-document dataset and
then performs all of these checks against real Compose volumes and the public gateway:

1. An interruption immediately after quiescence cannot publish a valid snapshot and the source
   cluster restarts with all five documents.
2. A missing artifact, a changed checksummed file, and incompatible schema/model configuration are
   each refused before restore writes.
3. An interruption after the first Lucene volume is copied affects only a new destination project.
   The drill deletes that destination and reopens the untouched source volumes successfully.
4. A full restore into another empty project retains the coordinator epoch and exact document count,
   then returns the representative BM25, vector, and hybrid documents through the gateway.

Run the same gate locally with:

```bash
scripts/docker-cluster-e2e.sh
```

On failure, preserve `DSEARCH_E2E_DIAGNOSTICS`; it contains service state and logs. The recovery
artifact and report contain the full index and model caches, so place both on the approved backup
target for an operational drill.

## Measured test-environment drill record

Every manual drill produces the measured record rather than relying on a stale number in this runbook.
Retain `recovery-drill-record.md`, `recovery-manifest.json`, and `recovery-report.json` from
`DSEARCH_E2E_DIAGNOSTICS`. The record states the exact checked-out commit,
the fixed `docker-e2e-recovery-v1` five-document dataset, the generated artifact ID, the measured
recovery point in seconds, and the measured recovery time in seconds. The script also prints the three
most operationally important values in one line:

```text
Recovery drill passed: artifact=<artifactId> RPO=<recoveryPointSeconds>s RTO=<recoveryTimeSeconds>s
```

Recovery point is measured from the last durably acknowledged dataset write to the frozen snapshot
boundary. Recovery time is measured from empty-project creation through epoch/topology restoration,
exact document-count validation, and public BM25/vector/hybrid verification. Retain all three evidence
files with the backup under test; together they record the commit, dataset, artifact, checksums, and
measurements needed to reproduce or audit the drill.

## Coordinator loss evaluation boundary

For the current single-coordinator engineering target, this restore is the recovery path for a lost
coordinator machine or durable disk. The target allows one trained operator to restore into an empty
deployment with RPO at most 300 seconds and RTO at most 900 seconds. `docker-cluster-e2e.sh` enforces
those limits against the measured report (override them only to make the test stricter with
`DSEARCH_RECOVERY_RPO_TARGET_SECONDS` and `DSEARCH_RECOVERY_RTO_TARGET_SECONDS`). A process-only
coordinator failure takes the faster durable-volume restart path in
[`OPERABILITY.md`](./OPERABILITY.md), which has a 240-second no-operator target.

Do not infer a second leader from a backup restore. Restore creates one fresh, fenced deployment and
verifies its preserved epoch and non-regressing topology; it does not permit two coordinators to
write the same topology. A future HA design must retain this single-writer property with a durable
fencing token and a tested upgrade/rollback path before it can replace this runbook.
