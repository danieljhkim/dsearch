#!/usr/bin/env bash

set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
compose_file="$repo_root/docker-compose.yml"
run_suffix=${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$
project_name=${DSEARCH_COMPOSE_PROJECT:-dsearch-e2e-$run_suffix}
project_name=$(printf '%s' "$project_name" | tr '[:upper:]_' '[:lower:]-' | tr -cd 'a-z0-9-')
diagnostics_dir=${DSEARCH_E2E_DIAGNOSTICS:-"$repo_root/target/docker-e2e-diagnostics"}
tls_root=$(mktemp -d "${TMPDIR:-/tmp}/dsearch-e2e-tls.XXXXXX")
compose=(docker compose --project-name "$project_name" --file "$compose_file")
compose_started=false
prebuilt_images=false
recovery_restore_project=
recovery_interrupted_project=
recovery_rpo_target_seconds=${DSEARCH_RECOVERY_RPO_TARGET_SECONDS:-300}
recovery_rto_target_seconds=${DSEARCH_RECOVERY_RTO_TARGET_SECONDS:-900}

[[ "$recovery_rpo_target_seconds" =~ ^[0-9]+$ && "$recovery_rto_target_seconds" =~ ^[0-9]+$ ]] \
  || { echo 'DSEARCH_RECOVERY_RPO_TARGET_SECONDS and DSEARCH_RECOVERY_RTO_TARGET_SECONDS must be non-negative integers' >&2; exit 2; }

DSEARCH_LOG_TAG=docker-e2e
source "$repo_root/scripts/lib/docker-cluster.sh"

cleanup() {
  local exit_code=$?
  trap - EXIT
  set +e
  if ((exit_code != 0)); then
    capture_diagnostics
    printf '[docker-e2e] Diagnostics retained at %s\n' "$diagnostics_dir" >&2
  fi
  if [[ -n "$recovery_restore_project" ]]; then
    docker compose --project-name "$recovery_restore_project" --file "$compose_file" \
      down --volumes --remove-orphans --timeout 10
  fi
  if [[ -n "$recovery_interrupted_project" ]]; then
    docker compose --project-name "$recovery_interrupted_project" --file "$compose_file" \
      down --volumes --remove-orphans --timeout 10
  fi
  if [[ "$compose_started" == "true" ]]; then
    "${compose[@]}" down --volumes --remove-orphans --timeout 10
  fi
  rm -rf "$tls_root"
  exit "$exit_code"
}
trap cleanup EXIT

require_commands docker openssl curl jq
docker info >/dev/null
docker compose version >/dev/null

image_variables=(
  DSEARCH_COORDINATOR_IMAGE
  DSEARCH_GATEWAY_IMAGE
  DSEARCH_QUERY_NODE_IMAGE
  DSEARCH_INDEX_NODE_IMAGE
)
configured_image_count=0
for variable in "${image_variables[@]}"; do
  if [[ -n "${!variable:-}" ]]; then
    configured_image_count=$((configured_image_count + 1))
    [[ "${!variable}" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]] \
      || fail "$variable must be an exact image digest reference, got: ${!variable}"
  fi
done
if ((configured_image_count != 0 && configured_image_count != ${#image_variables[@]})); then
  fail "set all four DSEARCH_*_IMAGE variables or none of them"
fi
if ((configured_image_count == ${#image_variables[@]})); then
  prebuilt_images=true
fi

assert_hardened_container() {
  local service=$1
  local expected_image=${2:-}
  local container_id
  container_id=$("${compose[@]}" ps --quiet "$service")
  [[ -n "$container_id" ]] || fail "$service container id is unavailable"

  [[ "$(docker inspect --format '{{.Config.User}}' "$container_id")" == "10001:10001" ]] \
    || fail "$service is not running as UID/GID 10001:10001"
  [[ "$(docker inspect --format '{{.HostConfig.ReadonlyRootfs}}' "$container_id")" == "true" ]] \
    || fail "$service root filesystem is not read-only"
  docker inspect --format '{{json .HostConfig.CapDrop}}' "$container_id" | jq -e \
    'map(ascii_upcase) | index("ALL") != null' >/dev/null \
    || fail "$service does not drop all Linux capabilities"
  docker inspect --format '{{json .HostConfig.SecurityOpt}}' "$container_id" | jq -e \
    'map(ascii_downcase) | any(startswith("no-new-privileges"))' >/dev/null \
    || fail "$service does not enforce no-new-privileges"
  docker inspect --format '{{json .HostConfig.Tmpfs}}' "$container_id" | jq -e \
    'has("/tmp")' >/dev/null \
    || fail "$service does not use an explicit writable /tmp tmpfs"

  if [[ -n "$expected_image" ]]; then
    [[ "$(docker inspect --format '{{.Config.Image}}' "$container_id")" == "$expected_image" ]] \
      || fail "$service did not start from the requested exact digest $expected_image"
  fi
}

assert_writable_path() {
  local service=$1
  local path=$2
  "${compose[@]}" exec --no-TTY "$service" sh -eu -c \
    'probe="$1/.dsearch-write-probe"; : > "$probe"; rm "$probe"' sh "$path" \
    || fail "$service cannot write its documented mount $path as the hardened user"
}

index_document_after_owner_rejoin() {
  local id=$1
  local payload=$2
  local timeout_seconds=${3:-30}
  local deadline=$((SECONDS + timeout_seconds))

  while ((SECONDS < deadline)); do
    http_request POST /api/v1/index "$payload"
    if [[ "$HTTP_STATUS" == "200" ]]; then
      assert_json "$HTTP_BODY" ".id == \"$id\" and .success == true" \
        'rejoined owner confirms the previously rejected key'
      return 0
    fi
    if [[ "$HTTP_STATUS" != "503" ]] || ! jq -e --arg id "$id" \
      '.status == 503
        and .path == "/api/v1/index"
        and (.message | type == "string"
          and contains("Owner node ")
          and contains("document " + $id + " in partition")
          and contains(" is not available; the mutation is not rerouted"))' \
      <<<"$HTTP_BODY" >/dev/null 2>&1; then
      fail "rejoined owner returned unexpected HTTP $HTTP_STATUS: $HTTP_BODY"
    fi
    sleep 1
  done

  fail "expired owner did not accept its key within ${timeout_seconds}s after automatic rejoin: $HTTP_STATUS $HTTP_BODY"
}

log "Generating production-profile mTLS identities"
generate_cluster_identities

log "Rendering Compose configuration"
"${compose[@]}" config --quiet
if [[ "$prebuilt_images" == "true" ]]; then
  log "Pulling the four prebuilt release digests"
  "${compose[@]}" pull
else
  log "Building every service image locally"
  "${compose[@]}" build
fi

log "Starting the Docker topology"
compose_started=true
"${compose[@]}" up --detach --no-build
await_gateway_ready 420
await_cluster_index_count 2 30

log "Verifying hardened identity, filesystems, capabilities, and exact release images"
assert_hardened_container coordinator "${DSEARCH_COORDINATOR_IMAGE:-}"
assert_hardened_container gateway "${DSEARCH_GATEWAY_IMAGE:-}"
assert_hardened_container query-node-0 "${DSEARCH_QUERY_NODE_IMAGE:-}"
assert_hardened_container index-node-0 "${DSEARCH_INDEX_NODE_IMAGE:-}"
assert_hardened_container index-node-1 "${DSEARCH_INDEX_NODE_IMAGE:-}"
assert_writable_path coordinator /data
assert_writable_path query-node-0 /var/cache/dsearch
assert_writable_path index-node-0 /data/index
assert_writable_path index-node-0 /var/cache/dsearch
assert_writable_path index-node-1 /data/index
assert_writable_path index-node-1 /var/cache/dsearch

log "Verifying graceful shutdown and restart under the hardened runtime"
query_container_id=$("${compose[@]}" ps --quiet query-node-0)
"${compose[@]}" stop --timeout 20 query-node-0
query_exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$query_container_id")
[[ "$query_exit_code" == "0" || "$query_exit_code" == "143" ]] \
  || fail "query-node-0 returned unexpected exit code $query_exit_code after SIGTERM"
[[ "$(docker inspect --format '{{.State.OOMKilled}}' "$query_container_id")" == "false" ]] \
  || fail "query-node-0 was OOM-killed during graceful shutdown"
"${compose[@]}" logs --no-color query-node-0 | grep -F 'Shutting down QueryNode gRPC server' >/dev/null \
  || fail "query-node-0 did not run its graceful shutdown hook"
"${compose[@]}" start query-node-0
await_gateway_ready 180

log "Indexing the bounded end-to-end dataset"
index_document_success doc-lucene 'Lucene cluster guide' 'recoveryset distributed lucene search gateway ownership' docs 2025
index_document_success doc-vector 'Vector retrieval handbook' 'recoveryset semantic neural vector embeddings retrieval' docs 2024
index_document_success doc-hybrid 'Hybrid search news' 'recoveryset hybrid lucene semantic search ranking' news 2026
index_document_success doc-update 'Update lifecycle' 'recoveryset retiredtoken search lifecycle' docs 2023
index_document_success doc-delete 'Delete lifecycle' 'recoveryset deletable sentinel search' temp 2022

bm25_payload=$(jq -nc '{query:"lucene",partitionId:"tenant-a",page:0,pageSize:10,searchType:"BM25",highlight:true,facets:[{field:"category",size:10}]}')
search_until "$bm25_payload" \
  '(.hits | map(.docId) | index("doc-lucene")) != null and (.hits | map(.docId) | index("doc-hybrid")) != null' \
  'BM25 documents are visible'
assert_json "$HTTP_BODY" \
  '.fanout.status == "SUCCESS" and .fanout.attemptedNodes == 2 and .fanout.succeededNodes == 2' \
  'BM25 fan-out succeeds across both index nodes'
assert_json "$HTTP_BODY" \
  '[.hits[]?.highlightedFields? // {} | .[]] | any(contains("<em>"))' \
  'BM25 response contains a real highlight fragment'
assert_json "$HTTP_BODY" \
  'any(.facets[]?; .field == "category" and any(.buckets[]?; .value == "docs" and .count >= 1) and any(.buckets[]?; .value == "news" and .count >= 1))' \
  'facet response aggregates category buckets'

filter_payload=$(jq -nc '{query:"search",partitionId:"tenant-a",page:0,pageSize:10,searchType:"BM25",highlight:false,filters:[{field:"category",operator:"EQ",values:["docs"]}]}')
search_until "$filter_payload" \
  '.totalHits >= 1 and all(.hits[]; .fields.category == "docs")' \
  'filtered BM25 search returns only docs'

semantic_payload=$(jq -nc '{query:"neural vector retrieval",partitionId:"tenant-a",page:0,pageSize:10,searchType:"SEMANTIC",highlight:false}')
search_until "$semantic_payload" \
  '.totalHits >= 1 and ((.hits | map(.docId) | index("doc-vector")) != null) and .fanout.status == "SUCCESS"' \
  'semantic vector search returns the vector document' 60

hybrid_payload=$(jq -nc '{query:"lucene vector",partitionId:"tenant-a",page:0,pageSize:10,searchType:"HYBRID",fusionStrategy:"RRF",highlight:false}')
search_until "$hybrid_payload" \
  '.totalHits >= 1 and (.hits | length >= 1)' \
  'hybrid search returns ranked results' 60
assert_json "$HTTP_BODY" '.fanout.status == "SUCCESS" and .fanout.succeededNodes == 4' \
  'hybrid response reports both BM25 and semantic fan-outs'

log "Running manifest, interruption, and empty-deployment recovery drills"
recovery_root="$tls_root/recovery"
mkdir -p "$recovery_root"
recovery_spec="$recovery_root/verification.json"
jq -n \
  --arg acknowledgedAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{schemaVersion:1,datasetId:"docker-e2e-recovery-v1",gatewayUrl:"http://localhost:19080",
    lastAcknowledgedWriteAt:$acknowledgedAt,
    documentCount:{partitionId:"tenant-a",query:"recoveryset",expected:5},
    queries:[
      {name:"bm25",partitionId:"tenant-a",query:"lucene",searchType:"BM25",
        expectedDocIds:["doc-lucene","doc-hybrid"]},
      {name:"vector",partitionId:"tenant-a",query:"neural vector retrieval",searchType:"SEMANTIC",
        expectedDocIds:["doc-vector"]},
      {name:"hybrid",partitionId:"tenant-a",query:"lucene vector",searchType:"HYBRID",
        fusionStrategy:"RRF",expectedDocIds:["doc-hybrid"]}
    ]}' >"$recovery_spec"

interrupted_snapshot="$recovery_root/interrupted-snapshot"
if DSEARCH_RECOVERY_FAILPOINT=snapshot-after-quiesce \
  "$repo_root/scripts/dsearch-recovery.sh" snapshot \
    --project "$project_name" --output "$interrupted_snapshot" --verification "$recovery_spec"; then
  fail 'interrupted snapshot failpoint unexpectedly published an artifact'
fi
[[ ! -e "$interrupted_snapshot" ]] \
  || fail 'interrupted snapshot exposed its requested final path as valid'
partial_snapshot=$(find "$recovery_root" -maxdepth 1 -type d \
  -name 'interrupted-snapshot.partial.*' -print -quit)
[[ -n "$partial_snapshot" ]] || fail 'interrupted snapshot did not retain diagnostic staging state'
if "$repo_root/scripts/dsearch-recovery.sh" validate --snapshot "$partial_snapshot"; then
  fail 'validator accepted an interrupted partial snapshot'
fi
rm -rf "$interrupted_snapshot".partial.*
"$repo_root/scripts/dsearch-recovery.sh" verify --verification "$recovery_spec" >/dev/null

snapshot_dir="$recovery_root/snapshot"
snapshot_result=$("$repo_root/scripts/dsearch-recovery.sh" snapshot \
  --project "$project_name" --output "$snapshot_dir" --verification "$recovery_spec" --leave-stopped)
artifact_id=$(jq -r '.artifactId' <<<"$snapshot_result")
[[ -n "$artifact_id" && "$artifact_id" != null ]] || fail 'snapshot did not return an artifact identifier'
"$repo_root/scripts/dsearch-recovery.sh" validate --snapshot "$snapshot_dir" >/dev/null

if "$repo_root/scripts/dsearch-recovery.sh" validate --snapshot "$recovery_root/missing-artifact"; then
  fail 'validator accepted a missing artifact'
fi

checksum_target="$snapshot_dir/metadata/source-commit.txt"
checksum_backup="$recovery_root/source-commit.backup"
cp "$checksum_target" "$checksum_backup"
printf 'corrupt\n' >>"$checksum_target"
if "$repo_root/scripts/dsearch-recovery.sh" validate --snapshot "$snapshot_dir"; then
  mv "$checksum_backup" "$checksum_target"
  fail 'validator accepted a checksum mismatch'
fi
mv "$checksum_backup" "$checksum_target"
"$repo_root/scripts/dsearch-recovery.sh" validate --snapshot "$snapshot_dir" >/dev/null

incompatible_config="$recovery_root/incompatible-app-config.yaml"
cp "$repo_root/dk.common/src/main/resources/app-config.docker.yaml" "$incompatible_config"
printf '\n# incompatible recovery drill\n' >>"$incompatible_config"
if "$repo_root/scripts/dsearch-recovery.sh" validate \
  --snapshot "$snapshot_dir" --config "$incompatible_config"; then
  fail 'validator accepted incompatible schema/model metadata'
fi

"${compose[@]}" down --remove-orphans --timeout 10
recovery_interrupted_project="${project_name}-restore-interrupted"
if DSEARCH_RECOVERY_FAILPOINT=restore-after-first-index \
  "$repo_root/scripts/dsearch-recovery.sh" restore \
    --project "$recovery_interrupted_project" --snapshot "$snapshot_dir"; then
  fail 'interrupted restore failpoint unexpectedly completed'
fi
docker compose --project-name "$recovery_interrupted_project" --file "$compose_file" \
  down --volumes --remove-orphans --timeout 10
recovery_interrupted_project=

log 'Reopening the source volumes after interrupted restore to prove they were not mutated'
"${compose[@]}" up --detach --no-build
await_gateway_ready 180
"$repo_root/scripts/dsearch-recovery.sh" verify --verification "$recovery_spec" >/dev/null
"${compose[@]}" down --remove-orphans --timeout 10

recovery_restore_project="${project_name}-restore"
recovery_report="$recovery_root/restore-report.json"
"$repo_root/scripts/dsearch-recovery.sh" restore \
  --project "$recovery_restore_project" --snapshot "$snapshot_dir" --report "$recovery_report" >/dev/null
jq -e --arg artifact "$artifact_id" \
  '.status == "passed" and .artifactId == $artifact
    and .datasetId == "docker-e2e-recovery-v1"
    and .recoveryPointSeconds >= 0 and .recoveryTimeSeconds >= 0' \
  "$recovery_report" >/dev/null || fail 'restore report is incomplete'
recovery_point_seconds=$(jq -r '.recoveryPointSeconds' "$recovery_report")
recovery_time_seconds=$(jq -r '.recoveryTimeSeconds' "$recovery_report")
((recovery_point_seconds <= recovery_rpo_target_seconds)) \
  || fail "recovery drill RPO ${recovery_point_seconds}s exceeds the ${recovery_rpo_target_seconds}s target"
((recovery_time_seconds <= recovery_rto_target_seconds)) \
  || fail "recovery drill RTO ${recovery_time_seconds}s exceeds the ${recovery_rto_target_seconds}s target"
log "Recovery drill passed: artifact=$artifact_id RPO=$(jq -r '.recoveryPointSeconds' "$recovery_report")s RTO=$(jq -r '.recoveryTimeSeconds' "$recovery_report")s"
mkdir -p "$diagnostics_dir"
cp "$snapshot_dir/manifest.json" "$diagnostics_dir/recovery-manifest.json"
cp "$recovery_report" "$diagnostics_dir/recovery-report.json"
jq -r '
  "# Docker recovery drill record\n\n"
  + "- Source commit: `" + .sourceCommit + "`\n"
  + "- Dataset: `" + .datasetId + "`\n"
  + "- Artifact: `" + .artifactId + "`\n"
  + "- Recovery point: " + (.recoveryPointSeconds | tostring) + " seconds after the last acknowledged write\n"
  + "- Recovery time: " + (.recoveryTimeSeconds | tostring) + " seconds from empty project creation through public verification\n"
  + "- Evaluated RPO target: " + $rpoTarget + " seconds\n"
  + "- Evaluated RTO target: " + $rtoTarget + " seconds\n"
  + "- Completed: `" + .completedAt + "`\n"' \
  --arg rpoTarget "$recovery_rpo_target_seconds" \
  --arg rtoTarget "$recovery_rto_target_seconds" \
  "$recovery_report" >"$diagnostics_dir/recovery-drill-record.md"

docker compose --project-name "$recovery_restore_project" --file "$compose_file" \
  down --volumes --remove-orphans --timeout 10
recovery_restore_project=
"${compose[@]}" up --detach --no-build
await_gateway_ready 180
"$repo_root/scripts/dsearch-recovery.sh" verify --verification "$recovery_spec" >/dev/null

log "Verifying update and delete visibility"
index_document_success doc-update 'Update lifecycle' 'currenttoken search lifecycle' docs 2023
updated_payload=$(jq -nc '{query:"currenttoken",partitionId:"tenant-a",page:0,pageSize:10,searchType:"BM25",highlight:false}')
search_until "$updated_payload" '(.hits | map(.docId) | index("doc-update")) != null' 'updated document is searchable'
old_payload=$(jq -nc '{query:"retiredtoken",partitionId:"tenant-a",page:0,pageSize:10,searchType:"BM25",highlight:false}')
search_until "$old_payload" '.totalHits == 0' 'old document content is replaced'

http_request DELETE '/api/v1/index/doc-delete?partitionId=tenant-a'
[[ "$HTTP_STATUS" == "200" ]] || fail "delete request returned HTTP $HTTP_STATUS: $HTTP_BODY"
assert_json "$HTTP_BODY" '.id == "doc-delete" and .success == true' 'delete response confirms success'
deleted_payload=$(jq -nc '{query:"deletable",partitionId:"tenant-a",page:0,pageSize:10,searchType:"BM25",highlight:false}')
search_until "$deleted_payload" '.totalHits == 0' 'deleted document is no longer searchable'

log "Verifying persisted coordinator topology across container recreation"
state_before=$("${compose[@]}" exec --no-TTY coordinator sh -c 'cat /data/coordinator-topology.properties')
epoch_before=$(state_property 'topology\.epoch' "$state_before")
version_before=$(state_property 'topology\.version' "$state_before")
[[ -n "$epoch_before" && "$version_before" =~ ^[0-9]+$ ]] || fail "coordinator state before recreation is malformed"
"${compose[@]}" up --detach --force-recreate --no-deps coordinator
await_gateway_ready 90
state_after=$("${compose[@]}" exec --no-TTY coordinator sh -c 'cat /data/coordinator-topology.properties')
epoch_after=$(state_property 'topology\.epoch' "$state_after")
version_after=$(state_property 'topology\.version' "$state_after")
[[ "$epoch_after" == "$epoch_before" ]] || fail "coordinator epoch changed across recreation"
[[ "$version_after" =~ ^[0-9]+$ && "$version_after" -ge "$version_before" ]] \
  || fail "coordinator topology version regressed across recreation"

log "Forcing one ungraceful node loss to assert partial failure, expiry, ownership, and rejoin"
"${compose[@]}" restart query-node-0
await_gateway_ready 120
index_one_id=$("${compose[@]}" ps --quiet index-node-1)
[[ -n "$index_one_id" ]] || fail "index-node-1 container id is unavailable"
docker update --restart=no "$index_one_id" >/dev/null
docker kill --signal KILL "$index_one_id" >/dev/null

partial_deadline=$((SECONDS + 8))
partial_observed=false
while ((SECONDS < partial_deadline)); do
  http_request POST /api/v1/search "$bm25_payload"
  if [[ "$HTTP_STATUS" == "200" ]] \
    && jq -e '.fanout.status == "PARTIAL_FAILURE" and .fanout.attemptedNodes == 2 and .fanout.succeededNodes == 1 and (.fanout.failedNodes + .fanout.timedOutNodes) == 1' \
      <<<"$HTTP_BODY" >/dev/null 2>&1; then
    partial_observed=true
    break
  fi
done
[[ "$partial_observed" == "true" ]] \
  || fail "partial fan-out failure metadata was not observed; last status=$HTTP_STATUS body=$HTTP_BODY"

await_cluster_index_count 1 75

live_owner_doc=
expired_owner_doc=
ownership_probe_deadline=$((SECONDS + 45))
candidate_number=1
while ((candidate_number <= 64 && SECONDS < ownership_probe_deadline)); do
  candidate="ownership-probe-$candidate_number"
  payload=$(jq -nc --arg id "$candidate" \
    '{id:$id,partitionId:"tenant-a",fields:{title:"Ownership probe",content:"ownership probe search",category:"probe",year:"2026"}}')
  http_request POST /api/v1/index "$payload"
  if [[ "$HTTP_STATUS" == "200" ]]; then
    live_owner_doc=$candidate
  elif [[ "$HTTP_STATUS" == "503" ]] && jq -e --arg id "$candidate" \
    '.status == 503
      and .path == "/api/v1/index"
      and (.message | type == "string"
        and contains("Owner node ")
        and contains("document " + $id + " in partition")
        and contains(" is not available; the mutation is not rerouted"))' \
    <<<"$HTTP_BODY" >/dev/null 2>&1; then
    expired_owner_doc=$candidate
  elif jq -e \
    '(.status == 504 and .path == "/api/v1/index" and (.message | contains("deadline exceeded")))
      or (.status == 503 and .path == "/api/v1/index" and (.message | contains("Unable to resolve host dsearch-index-1")))' \
    <<<"$HTTP_BODY" >/dev/null 2>&1; then
    sleep 1
    continue
  else
    fail "ownership probe returned unexpected HTTP $HTTP_STATUS: $HTTP_BODY"
  fi
  if [[ -n "$live_owner_doc" && -n "$expired_owner_doc" ]]; then
    break
  fi
  candidate_number=$((candidate_number + 1))
done
[[ -n "$live_owner_doc" && -n "$expired_owner_doc" ]] \
  || fail "ownership probes did not find keys for both configured owners within 45s; last status=$HTTP_STATUS body=$HTTP_BODY"

docker update --restart=unless-stopped "$index_one_id" >/dev/null
"${compose[@]}" start index-node-1
await_gateway_ready 180
await_cluster_index_count 2 30
rejoin_payload=$(jq -nc --arg id "$expired_owner_doc" \
  '{id:$id,partitionId:"tenant-a",fields:{title:"Rejoined owner",content:"rejoined ownership search",category:"probe",year:"2026"}}')
index_document_after_owner_rejoin "$expired_owner_doc" "$rejoin_payload"

log "Asserting total fan-out failure is explicit rather than an empty success"
"${compose[@]}" restart query-node-0
await_gateway_ready 120
index_zero_id=$("${compose[@]}" ps --quiet index-node-0)
index_one_id=$("${compose[@]}" ps --quiet index-node-1)
[[ -n "$index_zero_id" && -n "$index_one_id" ]] || fail "index container ids are unavailable"
docker update --restart=no "$index_zero_id" "$index_one_id" >/dev/null
docker kill --signal KILL "$index_zero_id" "$index_one_id" >/dev/null
http_request POST /api/v1/search "$bm25_payload"
[[ "$HTTP_STATUS" == "503" || "$HTTP_STATUS" == "504" ]] \
  || fail "total fan-out failure returned HTTP $HTTP_STATUS instead of an explicit error: $HTTP_BODY"
assert_json "$HTTP_BODY" \
  '(.status == 503 or .status == 504) and (.message | contains("Search fanout failed"))' \
  'total fan-out failure includes explicit failure metadata'

log "Docker cluster end-to-end gate passed"
