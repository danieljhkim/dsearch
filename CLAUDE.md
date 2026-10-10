# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**dsearch** is a horizontally scalable, Lucene-based distributed search engine built with Java 21. It provides lexical (BM25), semantic (vector kNN), and hybrid search capabilities.

`main` is the authoritative integration and release branch. Development builds use the root
Maven `revision` with a `-SNAPSHOT` suffix. To publish a release, set that one value to the
release version on `main`, tag the resulting commit as `v<version>`, and let the publish workflow
verify the branch, Maven version, and image tag before it pushes images.

**Tech Stack**: Java 21, Maven, Apache Lucene 10.5.0, gRPC (inter-service), Spring Boot (REST API), DJL/PyTorch (embeddings)

## Build & Run Commands

```bash
# Build (skips tests)
make build

# Run single-node cluster
make run

# Run multi-node cluster (configurable)
N_INDEX_NODES=3 N_QUERY_NODES=2 make run-multi

# Stop cluster
make stop

# View logs
make logs

# Full reset (clean + stop + wipe data)
make reset

# Format code
make format

# Check formatting
make lint

# Run tests in a module
mvn test -pl dk.index-node

# Run single test
mvn test -pl dk.index-node -Dtest=ShardIndexTest

# Rebuild protobuf stubs
mvn clean compile -pl dk.proto
```

## Architecture

```
Client (HTTP) → Gateway (8080) → Query Nodes (6000+) → Index Nodes (5000+)
                     ↓                    ↓
              Coordinator (7000)    [Lucene shards]
```

**Modules**:
- `dk.proto/` - gRPC/Protobuf service definitions (compile first after changes)
- `dk.common/` - Shared config, models, utilities, gRPC interceptors
- `dk.gateway/` - Spring Boot HTTP API (entry point)
- `dk.coordinator/` - Service discovery, health aggregation
- `dk.query-node/` - Query fanout, result merging, hybrid fusion
- `dk.index-node/` - Lucene shard management, search execution
- `dk.ml/` - DJL text embedding service
- `dk.raft/` - (in development) Raft consensus

**Key Files**:
- `dk.common/src/main/resources/app-config.yaml` - Primary cluster configuration
- `dk.index-node/.../index/ShardIndex.java` - Lucene index operations
- `dk.query-node/.../search/HybridFusion.java` - BM25/semantic score fusion
- `dk.gateway/.../api/SearchController.java` - HTTP search endpoint

## Development Workflow

### Adding New Features
1. Define protobuf in `dk.proto/src/main/proto/`
2. Run `mvn clean compile -pl dk.proto`
3. Implement in `dk.index-node` (Lucene layer)
4. Add fanout/merge in `dk.query-node`
5. Expose via `dk.gateway` REST API

### Modifying Field Configurations
Edit `app-config.yaml` → `fieldConfigs` section. Fields define: filterable, sortable, facetable, highlightable. Used by `FieldConfigRegistry` in dk.common.

### Debugging
```bash
# Set this before starting the local cluster so the gateway can serve protected Actuator endpoints.
export DSEARCH_ADMIN_TOKEN="$(openssl rand -hex 32)"
make run

curl http://localhost:8080/health              # Gateway health
curl http://localhost:8080/cluster/health      # Cluster health
curl -H "Authorization: Bearer $DSEARCH_ADMIN_TOKEN" \
  http://localhost:8080/actuator/prometheus     # Metrics
tail -f logs/index-node-0.log                  # Node logs
```

## Code Style

- Uses Spotless with Palantir Java format
- Run `make format` before committing
- Use Lombok annotations (@Data, @Builder) for DTOs
- Use Java 21 features (records, pattern matching)
- Logging via SLF4J

## Port Assignments

| Component      | gRPC  | Health |
|----------------|-------|--------|
| Index Node-N   | 500N  | 510N   |
| Query Node-N   | 600N  | 610N   |
| Coordinator-N  | 700N  | 710N   |
| Gateway        | N/A   | 8080   |

## Common Issues

- **Port conflict**: Run `make stop` to kill cluster processes
- **Stale index data**: Run `make wipe-data` then restart
- **Protobuf changes not reflected**: Run `mvn clean compile -pl dk.proto`, then rebuild dependent modules
