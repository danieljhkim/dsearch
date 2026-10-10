#!/bin/bash

echo "Stopping Distributed Search Engine cluster..."

# Match the exact jar filenames in each Java command line.
PATTERNS=(
  '(^|/)dk-coordinator\.jar([[:space:]]|$)'
  '(^|/)dk-index-node\.jar([[:space:]]|$)'
  '(^|/)dk-query-node\.jar([[:space:]]|$)'
  '(^|/)dk-gateway\.jar([[:space:]]|$)'
)

cluster_pids() {
  local pattern matches status pids=""

  for pattern in "${PATTERNS[@]}"; do
    matches=$(pgrep -f "$pattern")
    status=$?
    if (( status > 1 )); then
      echo "pgrep failed for pattern $pattern (status $status)." >&2
      return "$status"
    fi
    if (( status == 0 )); then
      pids+="${matches}"$'\n'
    fi
  done

  if [[ -n "$pids" ]]; then
    printf '%s' "$pids" | awk 'NF && !seen[$0]++ { printf "%s%s", separator, $0; separator = " " } END { if (separator != "") printf "\n" }'
  fi
}

if ! PIDS=$(cluster_pids); then
  echo "Unable to inspect cluster processes; stop was not confirmed." >&2
  exit 1
fi

if [[ -z "$PIDS" ]]; then
  echo "No running cluster JVMs found."
  echo "Done."
  exit 0
fi

echo "Sending SIGTERM to cluster processes (PIDs: $PIDS)"
for pid in $PIDS; do
  if kill -0 "$pid" 2>/dev/null && ! kill -TERM "$pid" 2>/dev/null; then
    echo "Failed to send SIGTERM to PID $pid." >&2
  fi
done

deadline=$((SECONDS + 15))
while true; do
  if ! REMAINING=$(cluster_pids); then
    echo "Unable to inspect cluster processes while waiting for shutdown." >&2
    exit 1
  fi

  if [[ -z "$REMAINING" ]]; then
    echo "All cluster JVMs exited."
    echo "Done."
    exit 0
  fi

  if (( SECONDS >= deadline )); then
    echo "Timed out after 15 seconds waiting for cluster JVMs to exit; surviving PIDs: $REMAINING" >&2
    exit 1
  fi

  sleep 1
done
