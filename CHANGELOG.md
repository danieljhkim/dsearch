

# Changelog — dsearch

All notable changes to the **dsearch** distributed search engine will be documented in this file.

This project follows a **Keep a Changelog–inspired** format and adheres loosely to
**Semantic Versioning**, adapted for an evolving infrastructure project.

---

## [Unreleased]

## [2.0.0] — 2026-10-10

Actuator `info`, `metrics`, and `prometheus` now require a bearer token. Before scrape:

- Set `DSEARCH_ADMIN_TOKEN` on the gateway.
- Send `Authorization: Bearer <token>` to `/actuator/prometheus`, `/actuator/metrics`, and `/actuator/info`.
- Leave probe traffic on `/actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness`, `/health`, and `/cluster/health`.

### Added
- Bounded bulk document ingestion over HTTP (DANI-10083)
- Snapshot, restore, and recovery drills (DANI-10078)
- Versioned index schemas and aliases; an index with no schema file is stamped on open rather than rejected (DANI-10085)
- Nested facets (DANI-10086)
- Field sorting and cursor pagination; offset paging is unchanged when no cursor is sent (DANI-10084)
- Stored-field projection on search. Omitting the selection keeps the full stored-field response (DANI-10192)
- Bounded bulk document deletion with ordered per-item outcomes (DANI-10193)
- Bounded analyzer token previews (DANI-10202)
- Replicated shard placement and acknowledged writes. Default replication factor remains 1 (DANI-10087)
- Divergent-replica detection and repair, with repair controls limited to an admin identity (DANI-10088, DANI-10698)
- Exact document retrieval by partition and document ID (DANI-10241)
- Document count from an authoritative shard observation, including unavailable and failed logical shards (DANI-10242)
- SLOs, bounded metrics, and actionable alerts (DANI-10079)
- Overload, disk-full, and rolling-restart drills (DANI-10080)
- Reproducible capacity benchmarks (DANI-10082)
- Hardened containers and gated signed releases (DANI-10081)
- Gateway admin authentication on `/api/v1/admin/*` (DANI-10085, DANI-10747)

### Changed
- Actuator `info`, `metrics`, and `prometheus` require `Authorization: Bearer $DSEARCH_ADMIN_TOKEN`. Health probes stay unauthenticated. A missing token disables non-health Actuator access (DANI-10432)
- Search hit `content` and `title` are omitted from JSON when the protobuf field is unset, instead of being returned as an empty string. Field numbers are unchanged (DANI-10192)
- ONE and QUORUM acknowledgement policies complete at the configured acknowledgement count. The default policy remains `all` (DANI-10269)
- Docker end-to-end checks are no longer part of push CI or the release gate (DANI-10180, DANI-10243)

### Fixed
- Unreadable pre-Lucene-10 shard data fails with an actionable reason instead of retrying. The Lucene pin was already 10.5.0 in 1.0.0 (DANI-10752)
- Encoded and matrix-parameter paths no longer skip gateway admin auth or request admission (DANI-10747)
- Dependency upgrades for Tomcat, jackson-core, Spring Boot 4.1.1 (from 3.5.16) and Spring Web MVC, and DJL 0.37.0 (from 0.36.0, CVE-2026-85228) (DANI-10189, DANI-10190, DANI-10677, DANI-10734, DANI-10802, DANI-10803, DANI-10810, DANI-10812)
- Mac multi-node coordinator startup selects a supported Unix socket (DANI-10315)
- Replica selection no longer admits a divergent replica, a replica that fails manifest inspection, or a replica whose snapshot transfer is incomplete (DANI-10694, DANI-10695, DANI-10696, DANI-10795)
- Coordinator live-topology race, coverage after replica repair, and future-version assertion (DANI-10258, DANI-10278, DANI-10287)
- Unavailable logical ranges are reported in replicated search, and write-fenced shards are visible to index-node reads (DANI-10268, DANI-10277)
- Delete identity is preserved across uncertain retries; mutation generations stay safe across process generations; Lucene mutation data and the retry ledger stay crash-consistent (DANI-10264, DANI-10266, DANI-10267)
- Shard-local and nested facet truncation (DANI-10164, DANI-10165)
- Null bulk field values no longer abort the whole request (DANI-10166)
- Gateway startup with GatewayAdminIndexService (DANI-10213)

### Removed
- None identified for the public API.

## [1.0.0] — Production Baseline

### Added
- Docker support: Multi-stage Dockerfiles for all runnable modules (coordinator, gateway, index-node, query-node)
- `docker-compose.yml` for full cluster orchestration
- `app-config.docker.yaml` for Docker-specific configuration
- GitHub Actions workflows: CI with Spotless checks and automated Docker image publishing
- `APP_CONFIG_PATH` environment variable support for flexible configuration file selection
- `finalName` configuration in all module POMs for consistent JAR naming (dk-*.jar)
- Per-request limits validation (`maxSize`, `maxQueryLength`) enforced at Gateway and QueryNode layers via `RequestLimitsValidator`
- Production gRPC TLS/mTLS for control and data planes, including certificate-volume configuration
- Authoritative coordinator topology, persisted shard-map recovery, node self-registration and rejoin support
- Dependency-aware node liveness and readiness checks
- Replication-factor and node-group configuration
- Client-visible partial and total shard-search failure status in query responses

### Changed
- Refactored `ConfigLoader` to use `APP_CONFIG_PATH` with default fallback to `app-config.yaml`
- Updated all modules to use simplified `ConfigLoader.load()` API
- Updated cluster scripts to use new JAR names (removed version suffixes)
- `Makefile`: Moved log cleanup from `stop` to `build` target
- Secure deployments require `DSEARCH_TLS_DIR`; Docker Compose no longer publishes service ports by default
- Coordinator registration, heartbeat, and shard ownership now use epoch-aware topology contracts
- Index, update, and delete requests route to the owning index node

### Fixed
- Propagated request deadlines and admission rejections end-to-end
- Made successful indexing acknowledgements durable and cross-node BM25 ranking deterministic
- Prevented partition-path traversal and unbounded metric-tag cardinality

---

## [0.2.0] — Tooling & Build Hardening

### Added
- `COMMANDS.md` documenting Makefile-based cluster operations
- devbox configuration for local agentic development
- git hooks for automatic formatting and compilation checks
  - `.githooks/pre-commit` - runs spotless formatting check before commit
  - `.githooks/pre-push` - runs compilation check before push
- `CHANGELOG.md` documenting changes to the project
- `config/eclipse-formatter.xml` - Eclipse Java formatter configuration for consistent code style
- `maven-compiler-plugin` added to all modules with Lombok annotation processor support
- `spotless-maven-plugin` (v2.43.0) added to all modules for automated code formatting
- Eclipse formatter integration via Spotless for consistent code formatting across the project

### Changed
- `Makefile` now uses `mvn spotless:apply` to apply the formatting
- `Makefile` now uses `mvn spotless:check` to check for formatting errors
- Root `pom.xml` updated with `maven-compiler-plugin` in pluginManagement (v3.11.0)
- Root `pom.xml` updated with `spotless-maven-plugin` in pluginManagement (v2.43.0)
- All module `pom.xml` files updated to include compiler and spotless plugins
- Reformatted all Java source files (75 files) to comply with Eclipse formatter rules
- Updated Java compiler configuration to use `maven.compiler.release` property (Java 21)

### Fixed
- Reformatted code to comply with Spotless formatting rules
- Consistent code formatting across all modules

---

## [0.1.0] — Initial Architecture Baseline

### Added
- Lucene-based indexing and search core
- Distributed architecture with:
  - Coordinator node
  - Gateway
  - Query node(s)
  - Index node(s)
- gRPC-based internal communication
- HTTP/gRPC Gateway entrypoint
- BM25 lexical search support
- Initial shard-based index layout
- Basic cluster lifecycle scripts:
  - `run_cluster.sh`
  - `run_cluster_multi.sh`
  - `kill_cluster.sh`
- Makefile for local development and cluster orchestration

### Design Notes
- Query and Index nodes are intentionally separated to allow independent scaling
- Coordinator maintains cluster metadata but does not sit on the query hot path
- System favors availability and partial results over strict consistency

---

## Changelog Guidelines

When updating this file:
- Add new entries under **[Unreleased]**
- Group changes under **Added / Changed / Fixed / Removed**
- Move entries to a versioned section when cutting a release
- Include architectural changes, not just code churn

---

## Versioning Notes

Version 1.0.0 establishes the stable production baseline. Subsequent releases
follow semantic versioning; breaking API, wire-contract, configuration, or
operational changes are documented in their release notes.
