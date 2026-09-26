#!/usr/bin/env bash
#
# Operability gate: overload, exhausted admission capacity, slow and unavailable
# downstreams, index disk-full and read-only storage, coordinator restart, and
# rolling query/index node replacement.
#
# Every scenario injects a deterministic fault against the Docker Compose
# topology, asserts that in-flight requests terminate inside their configured
# budget with an explicit outcome, asserts that the acknowledged dataset is
# still exactly intact once the fault is removed, and measures how long capacity
# took to return on its own. The coordinator state volume is never reset.
#
# See docs/OPERABILITY.md for the manual runbook.

set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
compose_file="$repo_root/docker-compose.yml"
docker_config_file="$repo_root/dk.common/src/main/resources/app-config.docker.yaml"
run_suffix=${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$
project_name=${DSEARCH_COMPOSE_PROJECT:-dsearch-resilience-$run_suffix}
project_name=$(printf '%s' "$project_name" | tr '[:upper:]_' '[:lower:]-' | tr -cd 'a-z0-9-')
diagnostics_dir=${DSEARCH_RESILIENCE_DIAGNOSTICS:-"$repo_root/target/docker-resilience-diagnostics"}
tls_root=$(mktemp -d "${TMPDIR:-/tmp}/dsearch-resilience.XXXXXX")
overlay_dir="$tls_root/compose"
compose_started=false
paused_services=()

DSEARCH_LOG_TAG=docker-resilience

# Health endpoints the base Compose file deliberately keeps unpublished. The
# gate needs the machine-readable readiness reason of each node, not just the
# aggregated gateway view, so it publishes them for the duration of the run.
coordinator_health_url=http://localhost:19070
query0_health_url=http://localhost:19081
index0_health_url=http://localhost:19090
index1_health_url=http://localhost:19091

mkdir -p "$overlay_dir" "$diagnostics_dir" "$diagnostics_dir/metrics" "$diagnostics_dir/bursts"
timeline_file="$diagnostics_dir/fault-timeline.jsonl"
report_file="$diagnostics_dir/resilience-report.json"
report_markdown="$diagnostics_dir/resilience-report.md"
scenario_records="$tls_root/scenario-records.jsonl"
: >"$timeline_file"
: >"$scenario_records"

cat >"$overlay_dir/observability.yml" <<'YAML'
# Publishes the per-node health endpoints so the resilience gate can read the
# exact readiness reason string each service reports while a fault is injected.
services:
  coordinator:
    ports:
      - "19070:8080"
  query-node-0:
    ports:
      - "19081:8081"
  index-node-0:
    ports:
      - "19090:8090"
  index-node-1:
    ports:
      - "19091:8090"
YAML

# Injects the documented free-space admission threshold above any achievable
# usable space, which is the deterministic stand-in for a full index volume.
cat >"$overlay_dir/index1-disk-full.yml" <<'YAML'
services:
  index-node-1:
    environment:
      INDEX_NODE_MINIMUM_FREE_DISK_BYTES: "9223372036854775807"
YAML

compose=(docker compose --project-name "$project_name"
  --file "$compose_file" --file "$overlay_dir/observability.yml")

source "$repo_root/scripts/lib/docker-cluster.sh"

cleanup() {
  local exit_code=$?
  trap - EXIT
  set +e
  local service
  for service in "${paused_services[@]:-}"; do
    if [[ -n "$service" ]]; then
      "${compose[@]}" unpause "$service" >/dev/null 2>&1
    fi
  done
  capture_diagnostics
  if declare -F write_reports >/dev/null; then
    write_reports
  fi
  if ((exit_code != 0)); then
    printf '[docker-resilience] Diagnostics retained at %s\n' "$diagnostics_dir" >&2
  fi
  if [[ "$compose_started" == "true" ]]; then
    "${compose[@]}" down --volumes --remove-orphans --timeout 10
  fi
  rm -rf "$tls_root"
  exit "$exit_code"
}
trap cleanup EXIT

require_commands docker openssl curl jq xargs
DSEARCH_ADMIN_TOKEN=${DSEARCH_ADMIN_TOKEN:-$(openssl rand -hex 32)}
export DSEARCH_ADMIN_TOKEN
docker info >/dev/null
docker compose version >/dev/null

# ---------------------------------------------------------------------------
# Runtime configuration, read from the deployed config so the gate cannot drift
# ---------------------------------------------------------------------------

config_value() {
  local key=$1
  local value
  value=$(sed -n "s/^[[:space:]]*${key}:[[:space:]]*\([0-9][0-9]*\).*/\1/p" "$docker_config_file" | head -n 1)
  [[ -n "$value" ]] || fail "could not read $key from $docker_config_file"
  printf '%s' "$value"
}

request_timeout_millis=$(config_value requestTimeoutMillis)
max_concurrent_http=$(config_value maxConcurrentHttpRequests)
max_concurrent_fanout=$(config_value maxConcurrentFanoutCalls)
node_expiry_seconds=$(config_value nodeExpirySeconds)
health_refresh_seconds=$(config_value refreshIntervalSeconds)
control_plane_restart_target_seconds=${DSEARCH_CONTROL_PLANE_RESTART_TARGET_SECONDS:-240}
[[ "$control_plane_restart_target_seconds" =~ ^[0-9]+$ && "$control_plane_restart_target_seconds" -gt 0 ]] \
  || fail "DSEARCH_CONTROL_PLANE_RESTART_TARGET_SECONDS must be a positive integer"

# One request may sit in the gateway for its whole budget plus TLS setup and
# JVM scheduling noise. Anything past this bound is treated as a hang.
single_request_bound_seconds=$(((request_timeout_millis + 999) / 1000 + 7))
burst_request_bound_seconds=$(((request_timeout_millis + 999) / 1000 + 17))
# Every admitted request holds one fan-out permit per active index node, so the
# burst must exceed the HTTP semaphore and, through it, the fan-out semaphore.
burst_concurrency=$((max_concurrent_http + 64))
((burst_concurrency <= 256)) \
  || fail "maxConcurrentHttpRequests=$max_concurrent_http is too large for a $burst_concurrency-way burst"

marker_token=resilienceset
document_count=12
expected_doc_ids=$(seq -f 'res-doc-%02g' 1 "$document_count" | jq -R . | jq -sc 'sort')
marker_payload=$(jq -nc --arg query "$marker_token" --arg partitionId "$partition_id" \
  '{query:$query, partitionId:$partitionId, page:0, pageSize:50, searchType:"BM25", highlight:false}')
search_payload=$(jq -nc --arg partitionId "$partition_id" \
  '{query:"lucene", partitionId:$partitionId, page:0, pageSize:10, searchType:"BM25", highlight:false}')
printf '%s' "$search_payload" >"$tls_root/burst-search.json"

baseline_epoch=
last_topology_version=
current_scenario=bootstrap
scenario_started_at_seconds=0
scenario_fault_injected_at=
scenario_fault_removed_at=
scenario_recovery_seconds=null
scenario_assertions=()

# ---------------------------------------------------------------------------
# Evidence recording
# ---------------------------------------------------------------------------

timestamp_utc() {
  local stamp
  stamp=$(date -u '+%Y-%m-%dT%H:%M:%S.%3NZ' 2>/dev/null || true)
  if [[ -z "$stamp" || "$stamp" == *3N* ]]; then
    stamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  fi
  printf '%s' "$stamp"
}

record_event() {
  local event=$1
  local detail=${2:-}
  jq -nc \
    --arg at "$(timestamp_utc)" \
    --argjson elapsedSeconds "$SECONDS" \
    --arg scenario "$current_scenario" \
    --arg event "$event" \
    --arg detail "$detail" \
    '{at:$at, elapsedSeconds:$elapsedSeconds, scenario:$scenario, event:$event, detail:$detail}' \
    >>"$timeline_file"
  log "[$current_scenario] $event${detail:+ - $detail}"
}

snapshot_metrics() {
  local phase=$1
  curl --silent --show-error --fail --max-time 10 \
    --header "Authorization: Bearer $DSEARCH_ADMIN_TOKEN" \
    "$gateway_base_url/actuator/prometheus" \
    >"$diagnostics_dir/metrics/${current_scenario}-${phase}.prom" 2>/dev/null || true
}

# Appends per-service logs so an injected fault keeps its own log slice even
# though the whole compose log is captured again at teardown.
snapshot_service_logs() {
  local phase=$1
  "${compose[@]}" logs --no-color --timestamps --tail 400 \
    >"$diagnostics_dir/${current_scenario}-${phase}-services.log" 2>&1 || true
}

pass() {
  local assertion=$1
  scenario_assertions+=("$assertion")
  record_event assertion_passed "$assertion"
}

begin_scenario() {
  current_scenario=$1
  scenario_started_at_seconds=$SECONDS
  scenario_fault_injected_at=
  scenario_fault_removed_at=
  scenario_recovery_seconds=null
  scenario_assertions=()
  record_event scenario_started
  snapshot_metrics before
}

fault_injected() {
  scenario_fault_injected_at=$(timestamp_utc)
  record_event fault_injected "$1"
  snapshot_metrics during
}

fault_removed() {
  scenario_fault_removed_at=$(timestamp_utc)
  record_event fault_removed "$1"
}

end_scenario() {
  snapshot_metrics after
  snapshot_service_logs after
  local assertions_json='[]'
  if ((${#scenario_assertions[@]} > 0)); then
    assertions_json=$(printf '%s\n' "${scenario_assertions[@]}" | jq -R . | jq -sc .)
  fi
  jq -nc \
    --arg scenario "$current_scenario" \
    --arg faultInjectedAt "$scenario_fault_injected_at" \
    --arg faultRemovedAt "$scenario_fault_removed_at" \
    --argjson durationSeconds "$((SECONDS - scenario_started_at_seconds))" \
    --argjson recoverySeconds "$scenario_recovery_seconds" \
    --argjson assertions "$assertions_json" \
    '{scenario:$scenario, faultInjectedAt:$faultInjectedAt, faultRemovedAt:$faultRemovedAt,
      durationSeconds:$durationSeconds, recoverySeconds:$recoverySeconds, assertions:$assertions}' \
    >>"$scenario_records"
  record_event scenario_passed
}

write_reports() {
  [[ -s "$scenario_records" ]] || return 0
  jq -sc \
    --arg project "$project_name" \
    --arg completedAt "$(timestamp_utc)" \
    --argjson requestTimeoutMillis "$request_timeout_millis" \
    --argjson maxConcurrentHttpRequests "$max_concurrent_http" \
    --argjson maxConcurrentFanoutCalls "$max_concurrent_fanout" \
    --argjson nodeExpirySeconds "$node_expiry_seconds" \
    --argjson controlPlaneRestartTargetSeconds "$control_plane_restart_target_seconds" \
    '{schemaVersion:1, project:$project, completedAt:$completedAt,
      budget:{requestTimeoutMillis:$requestTimeoutMillis,
              maxConcurrentHttpRequests:$maxConcurrentHttpRequests,
              maxConcurrentFanoutCalls:$maxConcurrentFanoutCalls,
              nodeExpirySeconds:$nodeExpirySeconds},
      objective:{controlPlaneRestartTargetSeconds:$controlPlaneRestartTargetSeconds},
      scenarios:.}' \
    "$scenario_records" >"$report_file" 2>/dev/null || return 0

  {
    printf '# Docker cluster resilience gate\n\n'
    printf -- '- Compose project: `%s`\n' "$project_name"
    printf -- '- Request budget: %s ms; HTTP admission: %s; fan-out admission: %s; membership lease: %ss; coordinator recovery target: %ss\n\n' \
      "$request_timeout_millis" "$max_concurrent_http" "$max_concurrent_fanout" "$node_expiry_seconds" "$control_plane_restart_target_seconds"
    printf '| Scenario | Fault injected | Fault removed | Recovery (s) | Duration (s) | Assertions |\n'
    printf '| --- | --- | --- | --- | --- | --- |\n'
    jq -r '.scenarios[]
      | "| " + .scenario
        + " | " + (if .faultInjectedAt == "" then "-" else .faultInjectedAt end)
        + " | " + (if .faultRemovedAt == "" then "-" else .faultRemovedAt end)
        + " | " + (if .recoverySeconds == null then "-" else (.recoverySeconds | tostring) end)
        + " | " + (.durationSeconds | tostring)
        + " | " + (.assertions | length | tostring) + " |"' "$report_file"
    printf '\n## Assertions\n\n'
    jq -r '.scenarios[] | "### " + .scenario + "\n\n"
      + (.assertions | map("- " + .) | join("\n")) + "\n"' "$report_file"
  } >"$report_markdown" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Probing helpers
# ---------------------------------------------------------------------------

PROBE_STATUS=
PROBE_BODY=

probe() {
  local url=$1
  local response_file="$tls_root/probe-response.json"
  : >"$response_file"
  # No --show-error: probe drives polling loops against services that are
  # deliberately down, and the connection noise would bury the real log.
  if PROBE_STATUS=$(curl --silent --connect-timeout 3 --max-time 10 \
    --output "$response_file" --write-out '%{http_code}' "$url" 2>/dev/null); then
    PROBE_BODY=$(<"$response_file")
  else
    PROBE_STATUS=000
    PROBE_BODY=$(<"$response_file")
  fi
}

HTTP_TIME_TOTAL=

# Like http_request, but also publishes the client-observed wall time so the
# caller can prove the request terminated inside its budget.
timed_request() {
  local method=$1
  local path=$2
  local payload=${3:-}
  local response_file="$tls_root/timed-response.json"
  local curl_args=(
    --silent --show-error --connect-timeout 5 --max-time 30
    --output "$response_file" --write-out '%{http_code} %{time_total}' --request "$method")
  if [[ -n "$payload" ]]; then
    curl_args+=(--header 'Content-Type: application/json' --data-binary "$payload")
  fi

  : >"$response_file"
  local write_out
  if write_out=$(curl "${curl_args[@]}" "$gateway_base_url$path"); then
    HTTP_STATUS=${write_out%% *}
    HTTP_TIME_TOTAL=${write_out##* }
  else
    HTTP_STATUS=000
    HTTP_TIME_TOTAL=$(printf '%s' "$write_out" | awk '{print $NF}')
    [[ -n "$HTTP_TIME_TOTAL" ]] || HTTP_TIME_TOTAL=30
  fi
  HTTP_BODY=$(<"$response_file")
}

assert_within_bound() {
  local observed=$1
  local bound=$2
  local description=$3
  awk -v observed="$observed" -v bound="$bound" 'BEGIN { exit !(observed + 0 <= bound + 0) }' \
    || fail "$description took ${observed}s, beyond the ${bound}s bound"
}

# The core invariant of this gate: a request under fault must come back with a
# definite HTTP status inside its budget, and a 200 must carry explicit fan-out
# metadata rather than an empty body that merely looks successful.
assert_bounded_and_explicit() {
  local description=$1
  local method=$2
  local path=$3
  local payload=${4:-}

  timed_request "$method" "$path" "$payload"
  [[ "$HTTP_STATUS" != "000" ]] \
    || fail "$description never received an HTTP status (client timeout or connection loss)"
  assert_within_bound "$HTTP_TIME_TOTAL" "$single_request_bound_seconds" "$description"

  if [[ "$HTTP_STATUS" == "200" ]]; then
    if [[ "$path" == "/api/v1/search" ]]; then
      assert_json "$HTTP_BODY" \
        '.fanout != null
          and (.fanout.status | IN("SUCCESS", "PARTIAL_FAILURE"))
          and .fanout.attemptedNodes >= 1
          and .fanout.succeededNodes >= 1' \
        "$description returned an explicit non-empty fan-out outcome"
    else
      assert_json "$HTTP_BODY" '.success == true' \
        "$description returned an acknowledged mutation rather than a bare 200"
    fi
  else
    assert_json "$HTTP_BODY" \
      '.status != null and (.message | type == "string" and length > 0)' \
      "$description returned an explicit error document"
  fi
  pass "$description terminated in ${HTTP_TIME_TOTAL}s with HTTP $HTTP_STATUS"
}

await_node_ready() {
  local url=$1
  local timeout_seconds=$2
  local deadline=$((SECONDS + timeout_seconds))
  while ((SECONDS < deadline)); do
    probe "$url/readyz"
    if [[ "$PROBE_STATUS" == "200" ]]; then
      return 0
    fi
    sleep 2
  done
  fail "$url did not become ready within ${timeout_seconds}s; last status=$PROBE_STATUS body=$PROBE_BODY"
}

await_node_not_ready_reason() {
  local url=$1
  local expected_reason=$2
  local timeout_seconds=$3
  local deadline=$((SECONDS + timeout_seconds))
  while ((SECONDS < deadline)); do
    probe "$url/readyz"
    if [[ "$PROBE_STATUS" == "503" ]] && jq -e --arg reason "$expected_reason" \
      '.status == "DOWN" and .reason == $reason' <<<"$PROBE_BODY" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  fail "$url did not report readiness reason '$expected_reason' within ${timeout_seconds}s;" \
    "last status=$PROBE_STATUS body=$PROBE_BODY"
}

await_gateway_degraded() {
  local timeout_seconds=$1
  local deadline=$((SECONDS + timeout_seconds))
  while ((SECONDS < deadline)); do
    http_request GET /readyz
    if [[ "$HTTP_STATUS" == "503" ]] && jq -e '.status == "DOWN" and (.reason | length > 0)' \
      <<<"$HTTP_BODY" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  fail "gateway never surfaced the injected fault as a readiness failure within ${timeout_seconds}s;" \
    "last status=$HTTP_STATUS body=$HTTP_BODY"
}

# ---------------------------------------------------------------------------
# Data, topology, and capacity verification
# ---------------------------------------------------------------------------

verify_dataset() {
  local context=$1
  local timeout_seconds=${2:-60}
  local deadline=$((SECONDS + timeout_seconds))
  while ((SECONDS < deadline)); do
    http_request POST /api/v1/search "$marker_payload"
    if [[ "$HTTP_STATUS" == "200" ]] && jq -e \
      --argjson count "$document_count" --argjson ids "$expected_doc_ids" \
      '.fanout.status == "SUCCESS"
        and .totalHits == $count
        and (.hits | length) == $count
        and (.hits | map(.docId) | sort) == $ids' \
      <<<"$HTTP_BODY" >/dev/null 2>&1; then
      pass "$context: all $document_count acknowledged writes are searchable exactly once"
      return 0
    fi
    sleep 2
  done
  fail "$context: dataset verification failed (loss, duplication, or missing capacity);" \
    "last status=$HTTP_STATUS body=$HTTP_BODY"
}

coordinator_state() {
  "${compose[@]}" exec --no-TTY coordinator sh -c 'cat /data/coordinator-topology.properties'
}

# Rejects an epoch change and any version regression. A new epoch means the
# coordinator lost its durable state and re-bootstrapped, which is exactly the
# "reset" this gate must never need.
assert_topology_continuity() {
  local context=$1
  local state epoch version
  state=$(coordinator_state)
  epoch=$(state_property 'topology\.epoch' "$state")
  version=$(state_property 'topology\.version' "$state")
  [[ -n "$epoch" && "$version" =~ ^[0-9]+$ ]] || fail "$context: coordinator state is malformed"
  if [[ -z "$baseline_epoch" ]]; then
    baseline_epoch=$epoch
    last_topology_version=$version
    return 0
  fi
  [[ "$epoch" == "$baseline_epoch" ]] \
    || fail "$context: coordinator epoch regressed from $baseline_epoch to $epoch"
  ((version >= last_topology_version)) \
    || fail "$context: topology version regressed from $last_topology_version to $version"
  last_topology_version=$version
  pass "$context: coordinator epoch $epoch preserved and topology version advanced to $version"
}

# Waits for the cluster to serve at full fan-out again and returns the elapsed
# seconds, which is the recovery duration recorded for the scenario.
await_full_capacity() {
  local timeout_seconds=$1
  local started=$SECONDS
  await_gateway_ready "$timeout_seconds"
  await_cluster_index_count 2 "$timeout_seconds"
  local deadline=$((SECONDS + timeout_seconds))
  while ((SECONDS < deadline)); do
    http_request POST /api/v1/search "$search_payload"
    if [[ "$HTTP_STATUS" == "200" ]] && jq -e \
      '.fanout.status == "SUCCESS" and .fanout.attemptedNodes == 2 and .fanout.succeededNodes == 2' \
      <<<"$HTTP_BODY" >/dev/null 2>&1; then
      scenario_recovery_seconds=$((SECONDS - started))
      record_event recovery_complete "full fan-out capacity returned after ${scenario_recovery_seconds}s"
      pass "capacity returned automatically in ${scenario_recovery_seconds}s without a coordinator reset"
      return 0
    fi
    sleep 2
  done
  fail "full fan-out capacity did not return within ${timeout_seconds}s;" \
    "last status=$HTTP_STATUS body=$HTTP_BODY"
}

pause_service() {
  local service=$1
  "${compose[@]}" pause "$service" >/dev/null
  paused_services+=("$service")
}

unpause_service() {
  local service=$1
  "${compose[@]}" unpause "$service" >/dev/null
  local remaining=()
  local entry
  for entry in "${paused_services[@]:-}"; do
    if [[ -n "$entry" && "$entry" != "$service" ]]; then
      remaining+=("$entry")
    fi
  done
  paused_services=("${remaining[@]:-}")
}

# ---------------------------------------------------------------------------
# Concurrent burst driver
# ---------------------------------------------------------------------------

run_burst() {
  local concurrency=$1
  local path=$2
  local payload_file=$3
  local out_dir=$4

  rm -rf "$out_dir"
  mkdir -p "$out_dir"
  seq 1 "$concurrency" | xargs -P "$concurrency" -I{} sh -c '
    index=$1; out=$2; url=$3; payload=$4
    if ! curl --silent --show-error --connect-timeout 5 --max-time 30 \
      --header "Content-Type: application/json" --data-binary "@$payload" \
      --output "$out/body-$index.json" --dump-header "$out/head-$index.txt" \
      --write-out "%{http_code} %{time_total}\n" \
      "$url" >"$out/meta-$index.txt" 2>"$out/err-$index.txt"; then
      [ -s "$out/meta-$index.txt" ] || printf "000 30\n" >"$out/meta-$index.txt"
    fi
  ' _ {} "$out_dir" "$gateway_base_url$path" "$payload_file" || true
}

burst_statuses() {
  cat "$1"/meta-*.txt | awk '{print $1}'
}

burst_max_seconds() {
  cat "$1"/meta-*.txt | awk 'BEGIN { max = 0 } { if ($2 + 0 > max) max = $2 + 0 } END { print max }'
}

# Counts responses whose body message contains the given admission-control text.
burst_bodies_matching() {
  local out_dir=$1
  local needle=$2
  local matches=0
  local body
  for body in "$out_dir"/body-*.json; do
    if jq -e --arg needle "$needle" '.message | type == "string" and contains($needle)' \
      "$body" >/dev/null 2>&1; then
      matches=$((matches + 1))
    fi
  done
  printf '%s' "$matches"
}

burst_retry_after_headers() {
  { grep -ril '^retry-after:' "$1"/head-*.txt 2>/dev/null || true; } | wc -l | tr -d ' '
}

# ---------------------------------------------------------------------------
# Bring the cluster up and seed the acknowledged dataset
# ---------------------------------------------------------------------------

log "Generating production-profile mTLS identities"
generate_cluster_identities

log "Rendering Compose configuration with published node health endpoints"
"${compose[@]}" config --quiet
log "Building every service image locally"
"${compose[@]}" build

log "Starting the Docker topology"
compose_started=true
"${compose[@]}" up --detach --no-build
await_gateway_ready 480
await_cluster_index_count 2 60
await_node_ready "$index0_health_url" 60
await_node_ready "$index1_health_url" 60
await_node_ready "$query0_health_url" 60
await_node_ready "$coordinator_health_url" 60
record_event cluster_ready "gateway, coordinator, both index nodes, and the query node are ready"

log "Indexing the acknowledged dataset"
for index in $(seq -f '%02g' 1 "$document_count"); do
  index_document_success "res-doc-$index" \
    "Resilience document $index" \
    "$marker_token lucene distributed search document $index" \
    docs "202$((10#$index % 6))"
done
verify_dataset "baseline"
assert_topology_continuity "baseline"
snapshot_metrics baseline

# ---------------------------------------------------------------------------
# Scenario 1 - request overload and exhausted admission capacity at both tiers
# ---------------------------------------------------------------------------

begin_scenario request-overload
fault_injected "index-node-0 and index-node-1 frozen (SIGSTOP) under a ${burst_concurrency}-way concurrent search burst"

# The freeze is held only for the burst window. Every admitted request then
# occupies the gateway for its whole budget, which is what drives both the HTTP
# and the fan-out semaphore past capacity, while the membership lease
# (${node_expiry_seconds}s) is never at risk of expiring inside this scenario.
burst_dir=
burst_engaged=false
for burst_attempt in 1 2 3; do
  burst_dir="$diagnostics_dir/bursts/request-overload-$burst_attempt"
  record_event burst_attempt_started "attempt $burst_attempt at ${burst_concurrency}-way concurrency"
  pause_service index-node-0
  pause_service index-node-1
  run_burst "$burst_concurrency" /api/v1/search "$tls_root/burst-search.json" "$burst_dir"
  unpause_service index-node-0
  unpause_service index-node-1
  record_event burst_attempt_completed "attempt $burst_attempt"

  gateway_shed=$(burst_bodies_matching "$burst_dir" 'HTTP request capacity exhausted')
  fanout_shed=$(burst_bodies_matching "$burst_dir" 'search fan-out capacity exhausted')
  if ((gateway_shed > 0 && fanout_shed > 0)); then
    burst_engaged=true
    break
  fi
  record_event burst_attempt_retried \
    "attempt $burst_attempt reached gateway_shed=$gateway_shed fanout_shed=$fanout_shed"
  await_gateway_ready 180
done

burst_total=$(burst_statuses "$burst_dir" | wc -l | tr -d ' ')
[[ "$burst_total" == "$burst_concurrency" ]] \
  || fail "burst produced $burst_total results, expected $burst_concurrency"
burst_hung=$(burst_statuses "$burst_dir" | grep -c '^000$' || true)
[[ "$burst_hung" == "0" ]] \
  || fail "$burst_hung of $burst_concurrency overload requests never received an HTTP status"
pass "all $burst_concurrency overloaded requests returned a definite HTTP status"

burst_slowest=$(burst_max_seconds "$burst_dir")
assert_within_bound "$burst_slowest" "$burst_request_bound_seconds" "slowest overloaded request"
pass "slowest overloaded request finished in ${burst_slowest}s, inside the ${burst_request_bound_seconds}s bound"

burst_ok=$(burst_statuses "$burst_dir" | grep -c '^200$' || true)
[[ "$burst_ok" == "0" ]] \
  || fail "$burst_ok requests reported success while every index node was frozen"
pass "no frozen-downstream request masqueraded as a successful search"

burst_unexpected=$(burst_statuses "$burst_dir" | grep -cvE '^(429|503|504)$' || true)
[[ "$burst_unexpected" == "0" ]] \
  || fail "$burst_unexpected overloaded requests returned a status outside {429, 503, 504}"
burst_overloaded=$(burst_statuses "$burst_dir" | grep -c '^429$' || true)
((burst_overloaded > 0)) || fail "the burst never exhausted admission capacity"
pass "$burst_overloaded of $burst_concurrency requests were shed with HTTP 429; the rest were explicit 503/504"

[[ "$burst_engaged" == "true" ]] \
  || fail "three ${burst_concurrency}-way bursts never engaged both admission tiers;" \
    "last gateway_shed=$gateway_shed fanout_shed=$fanout_shed"
pass "$gateway_shed responses came from gateway HTTP admission control (maxConcurrentHttpRequests=$max_concurrent_http)"
pass "$fanout_shed responses came from query-node fan-out admission control (maxConcurrentFanoutCalls=$max_concurrent_fanout)"

retry_after_count=$(burst_retry_after_headers "$burst_dir")
((retry_after_count >= burst_overloaded)) \
  || fail "only $retry_after_count of $burst_overloaded shed responses carried a Retry-After header"
pass "every shed response carried a Retry-After header"

snapshot_service_logs during
fault_removed "index nodes resumed"
await_full_capacity 180
verify_dataset "after request overload"
assert_topology_continuity "after request overload"
end_scenario

# ---------------------------------------------------------------------------
# Scenario 2 - slow downstream: one frozen index node must degrade, not hang
# ---------------------------------------------------------------------------

begin_scenario slow-downstream
pause_service index-node-1
fault_injected "index-node-1 paused (SIGSTOP) so its fan-out leg can only end at the deadline"

partial_observed=false
partial_deadline=$((SECONDS + 15))
while ((SECONDS < partial_deadline)); do
  timed_request POST /api/v1/search "$search_payload"
  assert_within_bound "$HTTP_TIME_TOTAL" "$single_request_bound_seconds" "search against a frozen index node"
  if [[ "$HTTP_STATUS" == "200" ]] && jq -e \
    '.fanout.status == "PARTIAL_FAILURE"
      and .fanout.attemptedNodes == 2
      and .fanout.succeededNodes == 1
      and (.fanout.failedNodes + .fanout.timedOutNodes) == 1' \
    <<<"$HTTP_BODY" >/dev/null 2>&1; then
    partial_observed=true
    break
  fi
  sleep 1
done
[[ "$partial_observed" == "true" ]] \
  || fail "a frozen index node never produced explicit PARTIAL_FAILURE metadata; last body=$HTTP_BODY"
pass "a slow index node produced PARTIAL_FAILURE in ${HTTP_TIME_TOTAL}s instead of hanging or hiding the loss"

assert_bounded_and_explicit "indexing while one index node is frozen" POST /api/v1/index \
  "$(jq -nc --arg partitionId "$partition_id" \
    '{id:"res-probe-slow", partitionId:$partitionId,
      fields:{title:"Slow downstream probe", content:"slow downstream probe", category:"probe", year:"2026"}}')"

unpause_service index-node-1
fault_removed "index-node-1 resumed"
await_full_capacity 180
http_request DELETE "/api/v1/index/res-probe-slow?partitionId=$partition_id"
[[ "$HTTP_STATUS" =~ ^(200|404|503)$ ]] \
  || fail "probe cleanup returned unexpected HTTP $HTTP_STATUS: $HTTP_BODY"
verify_dataset "after slow downstream"
assert_topology_continuity "after slow downstream"
end_scenario

# ---------------------------------------------------------------------------
# Scenario 3 - unavailable downstream, lease expiry, and automatic rejoin
# ---------------------------------------------------------------------------

begin_scenario unavailable-downstream
index_one_id=$("${compose[@]}" ps --quiet index-node-1)
[[ -n "$index_one_id" ]] || fail "index-node-1 container id is unavailable"
# SIGKILL, not a graceful stop: a stopped node deregisters itself on SIGTERM,
# which would never exercise the coordinator lease at all.
docker update --restart=no "$index_one_id" >/dev/null
docker kill --signal KILL "$index_one_id" >/dev/null
fault_injected "index-node-1 SIGKILLed; only the ${node_expiry_seconds}s membership lease can shed it"

http_request GET /cluster/health
assert_json "$HTTP_BODY" '.indexNodes | length == 2' \
  'an ungracefully lost node is still in the topology until its lease expires'
pass "the lost node was not deregistered, so the lease is the only path out of the topology"

assert_bounded_and_explicit "search against an ungracefully lost index node" \
  POST /api/v1/search "$search_payload"
await_gateway_degraded 60
pass "the gateway surfaced the lost node through /readyz instead of silently shrinking"

await_cluster_index_count 1 $((node_expiry_seconds + health_refresh_seconds + 60))
pass "the coordinator expired the index-node-1 membership lease on its own"

# The query node learns the shrunken topology on its own refresh interval, so
# poll rather than assume the gateway and the query node agree on the same tick.
reduced_capacity_observed=false
reduced_deadline=$((SECONDS + health_refresh_seconds + 60))
while ((SECONDS < reduced_deadline)); do
  timed_request POST /api/v1/search "$search_payload"
  [[ "$HTTP_STATUS" == "200" ]] \
    || fail "search after lease expiry returned HTTP $HTTP_STATUS: $HTTP_BODY"
  assert_within_bound "$HTTP_TIME_TOTAL" "$single_request_bound_seconds" "search after lease expiry"
  if jq -e '.fanout.status == "SUCCESS" and .fanout.attemptedNodes == 1 and .fanout.succeededNodes == 1' \
    <<<"$HTTP_BODY" >/dev/null 2>&1; then
    reduced_capacity_observed=true
    break
  fi
  assert_json "$HTTP_BODY" '.fanout.status == "PARTIAL_FAILURE"' \
    'a search that still targets the lost node reports partial failure while the topology settles'
  sleep 2
done
[[ "$reduced_capacity_observed" == "true" ]] \
  || fail "reduced capacity was never reported explicitly; last body=$HTTP_BODY"
pass "reads continued on the surviving node with explicit single-node fan-out metadata"

# Ownership is deliberately not rerouted, so at least one key must be refused
# rather than written to a node that would hold a competing copy.
owner_refusal_observed=false
for candidate in $(seq 1 32); do
  probe_payload=$(jq -nc --arg id "res-owner-probe-$candidate" --arg partitionId "$partition_id" \
    '{id:$id, partitionId:$partitionId,
      fields:{title:"Owner probe", content:"owner probe", category:"probe", year:"2026"}}')
  timed_request POST /api/v1/index "$probe_payload"
  assert_within_bound "$HTTP_TIME_TOTAL" "$single_request_bound_seconds" "ownership probe"
  [[ "$HTTP_STATUS" != "000" ]] || fail "ownership probe never received an HTTP status"
  if [[ "$HTTP_STATUS" == "503" ]] && jq -e \
    '.message | type == "string" and contains("is not available; the mutation is not rerouted")' \
    <<<"$HTTP_BODY" >/dev/null 2>&1; then
    owner_refusal_observed=true
    break
  fi
  [[ "$HTTP_STATUS" =~ ^(200|503|504)$ ]] \
    || fail "ownership probe returned unexpected HTTP $HTTP_STATUS: $HTTP_BODY"
done
[[ "$owner_refusal_observed" == "true" ]] \
  || fail "no write to the departed owner was explicitly refused; stale ownership may be accepting writes"
pass "writes owned by the departed node were explicitly refused instead of being silently rerouted"

docker update --restart=unless-stopped "$index_one_id" >/dev/null
"${compose[@]}" start index-node-1
fault_removed "index-node-1 restarted"
await_node_ready "$index1_health_url" 240
await_full_capacity 240
verify_dataset "after node rejoin"
assert_topology_continuity "after node rejoin"
for candidate in $(seq 1 32); do
  http_request DELETE "/api/v1/index/res-owner-probe-$candidate?partitionId=$partition_id"
done
verify_dataset "after ownership probe cleanup"
end_scenario

# ---------------------------------------------------------------------------
# Scenario 4 - index volume below the free-space admission threshold
# ---------------------------------------------------------------------------

begin_scenario index-disk-full
"${compose[@]}" --file "$overlay_dir/index1-disk-full.yml" \
  up --detach --no-build --no-deps --force-recreate index-node-1
fault_injected "index-node-1 recreated with INDEX_NODE_MINIMUM_FREE_DISK_BYTES above any achievable free space"

await_node_not_ready_reason "$index1_health_url" disk_space_below_threshold 180
pass "index-node-1 reported the exact readiness reason disk_space_below_threshold"

await_gateway_degraded 60
http_request GET /cluster/health
assert_json "$HTTP_BODY" \
  '.status == "DEGRADED" and any(.indexNodes[]?; .nodeId == "in1" and .status == "DOWN")' \
  'cluster health names the index node that lost write headroom'
pass "the disk-full node is reported DOWN in /cluster/health rather than silently serving"

assert_bounded_and_explicit "search while an index node is below its disk threshold" \
  POST /api/v1/search "$search_payload"
assert_bounded_and_explicit "indexing while an index node is below its disk threshold" \
  POST /api/v1/index \
  "$(jq -nc --arg partitionId "$partition_id" \
    '{id:"res-probe-disk", partitionId:$partitionId,
      fields:{title:"Disk probe", content:"disk probe", category:"probe", year:"2026"}}')"

"${compose[@]}" up --detach --no-build --no-deps --force-recreate index-node-1
fault_removed "index-node-1 recreated with the deployed free-space threshold"
await_node_ready "$index1_health_url" 240
await_full_capacity 240
http_request DELETE "/api/v1/index/res-probe-disk?partitionId=$partition_id"
[[ "$HTTP_STATUS" =~ ^(200|404|503)$ ]] \
  || fail "probe cleanup returned unexpected HTTP $HTTP_STATUS: $HTTP_BODY"
verify_dataset "after disk-full recovery"
assert_topology_continuity "after disk-full recovery"
end_scenario

# ---------------------------------------------------------------------------
# Scenario 5 - read-only index storage
# ---------------------------------------------------------------------------

# The Lucene volume is owned by the container identity, so that identity can
# revoke its own write bit. This keeps the fault inside the deployed topology
# instead of depending on how two Compose files merge a volume list.
index_volume_mode() {
  local mode=$1
  "${compose[@]}" exec --no-TTY index-node-1 sh -eu -c 'chmod "$1" /data/index' sh "$mode"
}

begin_scenario index-read-only-storage
index_volume_mode a-w
fault_injected "index-node-1 /data/index write permission revoked"

await_node_not_ready_reason "$index1_health_url" lucene_directory_not_writable 120
pass "index-node-1 reported the exact readiness reason lucene_directory_not_writable"

await_gateway_degraded 60
http_request GET /cluster/health
assert_json "$HTTP_BODY" \
  '.status == "DEGRADED" and any(.indexNodes[]?; .nodeId == "in1" and .status == "DOWN")' \
  'cluster health names the index node that lost its writable Lucene directory'
pass "the read-only node is reported DOWN in /cluster/health rather than silently serving"

assert_bounded_and_explicit "search while an index node has read-only storage" \
  POST /api/v1/search "$search_payload"

# Restarting on a directory it cannot lock means Lucene never opens, so the
# node stays live-but-not-ready and must never re-register.
"${compose[@]}" restart --timeout 20 index-node-1
record_event fault_escalated "index-node-1 restarted onto read-only storage; Lucene cannot open"
# One wait proves both halves: the node must never claim readiness, and the
# topology must shrink to the one node that can actually serve.
read_only_deadline=$((SECONDS + node_expiry_seconds + health_refresh_seconds + 120))
topology_shrank=false
while ((SECONDS < read_only_deadline)); do
  probe "$index1_health_url/readyz"
  [[ "$PROBE_STATUS" != "200" ]] \
    || fail "index-node-1 reported itself ready with a Lucene directory it cannot write: $PROBE_BODY"
  http_request GET /cluster/health
  if jq -e '.indexNodes | length == 1' <<<"$HTTP_BODY" >/dev/null 2>&1; then
    topology_shrank=true
    break
  fi
  sleep 2
done
[[ "$topology_shrank" == "true" ]] \
  || fail "the read-only node never left the topology; last cluster health=$HTTP_BODY"
pass "index-node-1 never claimed readiness on read-only storage and the topology shrank to one index node"

index_volume_mode u+w
"${compose[@]}" restart --timeout 20 index-node-1
fault_removed "index-node-1 /data/index write permission restored"
await_node_ready "$index1_health_url" 240
await_full_capacity 240
verify_dataset "after read-only storage recovery"
assert_topology_continuity "after read-only storage recovery"
end_scenario

# ---------------------------------------------------------------------------
# Scenario 6 - coordinator restart
# ---------------------------------------------------------------------------

begin_scenario coordinator-restart
"${compose[@]}" stop --timeout 20 coordinator
fault_injected "coordinator stopped; nodes keep their last observed topology"

assert_bounded_and_explicit "search while the coordinator is down" POST /api/v1/search "$search_payload"
await_gateway_degraded 60
pass "the gateway reported the missing coordinator instead of failing open"

"${compose[@]}" start coordinator
fault_removed "coordinator restarted from its persisted state volume"
coordinator_recovery_started=$SECONDS
await_node_ready "$coordinator_health_url" 180
await_full_capacity 240
coordinator_recovery_seconds=$((SECONDS - coordinator_recovery_started))
((coordinator_recovery_seconds <= control_plane_restart_target_seconds)) \
  || fail "coordinator restart took ${coordinator_recovery_seconds}s, exceeding the ${control_plane_restart_target_seconds}s control-plane target"
pass "coordinator recovered from durable state in ${coordinator_recovery_seconds}s, within the ${control_plane_restart_target_seconds}s control-plane target"
assert_topology_continuity "after coordinator restart"
verify_dataset "after coordinator restart"
end_scenario

# ---------------------------------------------------------------------------
# Scenario 7 - rolling query and index node replacement
# ---------------------------------------------------------------------------

begin_scenario rolling-restart
fault_injected "rolling replacement of query-node-0, index-node-0, and index-node-1, one container at a time"
for service in query-node-0 index-node-0 index-node-1; do
  record_event rolling_replace "$service"
  "${compose[@]}" up --detach --no-build --no-deps --force-recreate "$service"
  case "$service" in
    query-node-0) await_node_ready "$query0_health_url" 300 ;;
    index-node-0) await_node_ready "$index0_health_url" 300 ;;
    index-node-1) await_node_ready "$index1_health_url" 300 ;;
  esac
  await_full_capacity 300
  verify_dataset "after replacing $service"
  assert_topology_continuity "after replacing $service"
  pass "$service was replaced without losing the acknowledged dataset or resetting the coordinator"
done
fault_removed "rolling replacement complete"
end_scenario

write_reports
log "Resilience gate passed; evidence written to $diagnostics_dir"
