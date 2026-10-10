#!/bin/bash

set -e

############################################
# CONFIG
############################################

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."

COORDINATOR_JAR="$BASE_DIR/dk.coordinator/target/dk-coordinator.jar"
INDEX_NODE_JAR="$BASE_DIR/dk.index-node/target/dk-index-node.jar"
QUERY_NODE_JAR="$BASE_DIR/dk.query-node/target/dk-query-node.jar"
GATEWAY_JAR="$BASE_DIR/dk.gateway/target/dk-gateway.jar"

LOG_DIR="$BASE_DIR/logs"
DATA_DIR="$BASE_DIR/data"
LOCAL_CONFIG_FILE="$BASE_DIR/dk.common/src/main/resources/app-config.yaml"

JAVA_OPTS="--add-modules jdk.incubator.vector"

# Local launchers opt in to plaintext explicitly. Packaged/default configuration remains mTLS.
export DSEARCH_GRPC_PROFILE=local

# Number of nodes
N_INDEX_NODES=${N_INDEX_NODES:-2}
N_QUERY_NODES=${N_QUERY_NODES:-2}
export N_INDEX_NODES N_QUERY_NODES

# Base ports
COORDINATOR_PORT=${COORDINATOR_PORT:-7000}
INDEX_BASE_PORT=${INDEX_BASE_PORT:-5000}
QUERY_BASE_PORT=${QUERY_BASE_PORT:-6000}
GATEWAY_PORT=${GATEWAY_PORT:-8080}

############################################
# FUNCTIONS
############################################

configured_node_ids() {
  local section="$1"

  awk -v section="$section" '
    $0 ~ "^" section ":" { in_section = 1; next }
    in_section && /^[[:alnum:]_-]+:/ { exit }
    in_section && /^[[:space:]]*-[[:space:]]+id:/ {
      id = $0
      sub(/^[^:]*:[[:space:]]*/, "", id)
      gsub(/"/, "", id)
      sub(/[[:space:]#].*$/, "", id)
      gsub(/[[:space:]]+$/, "", id)
      print id
    }
  ' "$LOCAL_CONFIG_FILE"
}

read_local_topology() {
  local configured_index_node_count
  local configured_query_node_count
  local node_id

  if [[ ! -f "$LOCAL_CONFIG_FILE" ]]; then
    echo "Local topology config not found: $LOCAL_CONFIG_FILE" >&2
    exit 1
  fi

  configured_index_node_count=$(configured_node_ids "indexNodes" | wc -l | tr -d ' ')
  configured_query_node_count=$(configured_node_ids "queryNodes" | wc -l | tr -d ' ')

  if (( N_INDEX_NODES > configured_index_node_count )); then
    echo "Cannot launch $N_INDEX_NODES index nodes: $LOCAL_CONFIG_FILE defines only $configured_index_node_count logical IDs." >&2
    exit 1
  fi

  if (( N_QUERY_NODES > configured_query_node_count )); then
    echo "Cannot launch $N_QUERY_NODES query nodes: $LOCAL_CONFIG_FILE defines only $configured_query_node_count logical IDs." >&2
    exit 1
  fi

  INDEX_NODE_IDS=()
  QUERY_NODE_IDS=()

  while IFS= read -r node_id; do
    INDEX_NODE_IDS+=("$node_id")
  done < <(configured_node_ids "indexNodes" | head -n "$N_INDEX_NODES")

  while IFS= read -r node_id; do
    QUERY_NODE_IDS+=("$node_id")
  done < <(configured_node_ids "queryNodes" | head -n "$N_QUERY_NODES")
}

start_coordinator() {
  echo "Starting Coordinator..."
  export COORDINATOR_PORT
  export COORDINATOR_HEALTH_PORT=$((COORDINATOR_PORT + 100))

  nohup java -jar "$COORDINATOR_JAR" \
    > "$LOG_DIR/coordinator.log" 2>&1 &

  COORDINATOR_PID=$!
  echo "Coordinator started (PID: $COORDINATOR_PID, port: $COORDINATOR_PORT)"
}

start_index_nodes() {
  echo "Starting $N_INDEX_NODES Index Node(s)..."

  for ((i=0; i< N_INDEX_NODES; i++)); do
    local port=$((INDEX_BASE_PORT + i))
    local node_id="${INDEX_NODE_IDS[$i]}"
    local node_data_dir="$DATA_DIR/index-node-$i"
    mkdir -p "$node_data_dir"

    export INDEX_NODE_PORT="$port"
    export INDEX_NODE_HEALTH_PORT=$((port + 100))
    export INDEX_NODE_BASE_DIR="$node_data_dir"
    export NODE_ID="$node_id"
    export NODE_ROLE="INDEX"
    export COORDINATOR_HOST="localhost"
    export COORDINATOR_PORT

    nohup java $JAVA_OPTS -jar "$INDEX_NODE_JAR" \
      > "$LOG_DIR/index-node-$i.log" 2>&1 &

    echo "Index Node #$i started (NODE_ID=$node_id, PORT=$port, DATA=$node_data_dir)"
  done
}

start_query_nodes() {
  echo "Starting $N_QUERY_NODES Query Node(s)..."

  for ((i=0; i< N_QUERY_NODES; i++)); do
    local port=$((QUERY_BASE_PORT + i))
    local node_id="${QUERY_NODE_IDS[$i]}"

    export QUERY_NODE_PORT="$port"
    export NODE_ID="$node_id"
    export NODE_ROLE="QUERY"
    export COORDINATOR_HOST="localhost"
    export QUERY_NODE_HEALTH_PORT=$((port + 100))
    export COORDINATOR_PORT

    nohup java -jar "$QUERY_NODE_JAR" \
      > "$LOG_DIR/query-node-$i.log" 2>&1 &

    echo "Query Node #$i started (NODE_ID=$node_id, PORT=$port)"
  done
}

start_gateway() {
  echo "Starting Gateway..."

  # Gateway can talk to coordinator for health-aware node discovery and shard-map lookup.
  export COORDINATOR_HOST="localhost"
  export COORDINATOR_PORT
  export SERVER_PORT="$GATEWAY_PORT"   # Spring Boot

  # For backward-compat if you still use direct node envs:
  export INDEX_NODE_HOST="localhost"
  export INDEX_NODE_PORT="$INDEX_BASE_PORT"
  export QUERY_NODE_HOST="localhost"
  export QUERY_NODE_PORT="$QUERY_BASE_PORT"

  nohup java -jar "$GATEWAY_JAR" \
    > "$LOG_DIR/gateway.log" 2>&1 &

  echo "Gateway started (PORT: $GATEWAY_PORT)"
}

write_cluster_state() {
  echo "Writing cluster state..."
  local state_file="$BASE_DIR/logs/cluster_state.json"

  cat > "$state_file" <<EOF
{
  "ports": {
    "coordinator_port": $COORDINATOR_PORT,
    "index_base_port": $INDEX_BASE_PORT,
    "query_base_port": $QUERY_BASE_PORT,
    "gateway_port": $GATEWAY_PORT
  },
  "counts": {
    "n_index_nodes": $N_INDEX_NODES,
    "n_query_nodes": $N_QUERY_NODES
  },
  "log_dir": "$LOG_DIR",
  "data_dir": "$DATA_DIR"
}
EOF
  echo "Cluster state written to $state_file"
}

stop_cluster() {
  echo "Stopping all cluster processes..."

    pkill -f "dk-coordinator.jar" || true
    pkill -f "dk-index-node.jar" || true
    pkill -f "dk-query-node.jar" || true
    pkill -f "dk-gateway.jar" || true

  echo "Cluster stopped."
}

############################################
# ENTRYPOINT
############################################

if [[ "$1" == "stop" ]]; then
  stop_cluster
  exit 0
fi

echo "================================================="
echo " Spinning up Distributed Search Engine (multi-node)"
echo "================================================="
echo "Coordinator : localhost:${COORDINATOR_PORT}"
echo "Index Nodes : ${N_INDEX_NODES} (ports ${INDEX_BASE_PORT}..$((INDEX_BASE_PORT + N_INDEX_NODES - 1)))"
echo "Query Nodes : ${N_QUERY_NODES} (ports ${QUERY_BASE_PORT}..$((QUERY_BASE_PORT + N_QUERY_NODES - 1)))"
echo "Gateway     : http://localhost:${GATEWAY_PORT}"
echo "================================================="

read_local_topology

mkdir -p "$LOG_DIR"
mkdir -p "$DATA_DIR"

start_coordinator
sleep 5

start_index_nodes
sleep 1

start_query_nodes
sleep 1

start_gateway
sleep 1

write_cluster_state

echo ""
echo "================================================="
echo "Cluster is running!"
echo "Logs  : $LOG_DIR"
echo "Data  : $DATA_DIR"
echo "Stop  : make stop"
echo "================================================="
