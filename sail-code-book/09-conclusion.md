# Chapter 9: Where Sail Is Going, and How to Navigate the Codebase

## Current State

As of version 0.6.3, Sail covers the Spark 3.5.x and Spark 4.x API surfaces that are most commonly used in production PySpark workloads:

- Spark SQL: most DDL/DML, window functions, lateral joins, subqueries
- DataFrame API: virtually complete for the Spark Connect protocol surface
- Python UDFs and UDAFs: row-by-row, batch (PyArrow), Pandas scalar, iterator, grouped map, co-grouped map
- File formats: Parquet, CSV, JSON, ORC, Avro (read and write)
- Table formats: Delta Lake (Variant Shredding, append/overwrite write, MERGE, basic DELETE — optimize/vacuum/CDC not yet), Apache Iceberg (read-only)
- Catalogs: Memory, AWS Glue, Hive Metastore, Apache Iceberg REST, Unity, OneLake
- Streaming: structured streaming write (append mode), streaming query management
- UDTFs: scalar table functions
- HLL and theta sketch aggregate functions
- Geographic types (Geometry, Geography) with GeoArrow encoding

There are also deliberate gaps — places where `SparkError::todo(...)` or `Status::unimplemented(...)` is returned. These include ML pipelines, resource profile commands, compressed operation plans, and some advanced streaming modes. The `todo` markers are intentional breadcrumbs rather than forgotten code.

## Where Sail Is Headed

Several areas are actively evolving:

**Distributed execution.** The cluster mode driver/worker architecture described in Chapter 6 is functional but evolving. Future work includes Kubernetes-native worker lifecycle management, better fault tolerance with task retry policies, and remote stream storage for shuffle spill.

**Delta Lake write support.** Variant Shredding is merged. Merge-into (upsert) support via `MergeIntoTableCommand` is implemented. Future work: Z-ordering, optimized compaction, Change Data Feed.

**Iceberg write support.** Currently read-only. Write support is a high-priority item.

**Full streaming coverage.** Continuous mode streaming and stateful aggregations are partially supported. Watermarking and event-time triggers are areas of active development.

**Spark 4.x compatibility.** Sail targets both Spark 3.5.x and 4.x simultaneously. As Spark 4.x features land (new catalog APIs, new type system features, `VARIANT` type), Sail adds them. The `GroupedMap/CoGroupedMap` iterator UDFs for Spark 4.1.1 were merged in recent commits.

## How to Navigate the Codebase

### Entry Points

Start here depending on what you want to understand:

| Goal | Start |
|---|---|
| A PySpark query arrives | `crates/sail-spark-connect/src/server.rs:execute_plan` |
| How a relation is converted to a logical plan | `crates/sail-plan/src/resolver/query/` |
| How a specific Spark function is implemented | `crates/sail-function/src/` |
| How a new catalog is added | `crates/sail-catalog/src/provider/mod.rs` (read the trait), then any `crates/sail-catalog-*/` |
| How Delta Lake tables are scanned | `crates/sail-delta-lake/src/` |
| How a Python UDF runs | `crates/sail-python-udf/src/udf/pyspark_udf.rs` |
| How cluster execution works | `crates/sail-execution/src/driver/` |
| How the Python package embeds the server | `crates/sail-python/src/spark/server.rs` |

### The `spec` IR Is the Lingua Franca

If you are confused about where a concept lives, find it in `sail-common/src/spec/`. The spec module defines the canonical Rust representation of Spark's plan and expression types. Every data path through Sail passes through this IR. If a new Spark feature involves a new plan node or expression type, it starts here.

### Reading `resolve_query_plan`

The largest single function in the resolver is `resolve_query_plan` (or its close neighbors). It is a large `match` over `spec::Relation` variants. When adding support for a new Spark relation type, this is where you add the arm. The pattern is consistent: destructure the spec struct, recursively resolve child plans, build a DataFusion `LogicalPlan` node or a `UserDefinedLogicalNodeCore` extension.

### Tests: Gold Tests and Integration Tests

`sail-gold-test` contains golden-file tests that run SQL queries and compare the output against expected results stored in `.json` files. These are the most valuable tests for Spark compatibility: they capture exact output including column names, types, and values.

Integration tests require a running PySpark client connected to a Sail server. They live in `python/pysail/tests/` and can be run with `pytest` after building and installing the Python package.

Unit tests are embedded in the individual crates using the standard `#[cfg(test)] mod tests` pattern.

## How to Contribute

### Adding a Spark Function

1. Find the function in `crates/sail-function/src/`. Functions are organized by type (scalar, aggregate, window, table) and by module (math, string, array, map, date, etc.).
2. Implement `ScalarUDFImpl` (or `AggregateUDFImpl`, `WindowUDFImpl`) in Rust.
3. Register the function in the session's function registry.
4. Add a gold test that exercises the function with the same inputs as Spark produces.

The function crate has many examples to follow. The `to_csv` and `timestampdiff` functions were added in recent commits and are good recent examples.

### Adding a Catalog Backend

1. Create a new crate `sail-catalog-newbackend`.
2. Implement `CatalogProvider` for your struct.
3. Add a `GlueCatalogConfig`-style configuration struct.
4. Register the provider in the session factory in `sail-spark-connect/src/session_manager.rs` (or wherever catalog selection happens based on config).
5. Add integration tests.

### Adding a Logical Plan Node

1. Define the node struct in `sail-logical-plan/src/new_node.rs`.
2. Implement `UserDefinedLogicalNodeCore`.
3. Add the physical counterpart in `sail-physical-plan/src/new_node.rs` implementing `ExecutionPlan`.
4. Add the logical → physical conversion in `sail-plan`'s physical planner extension.
5. Add the spec IR type if needed.
6. Add the resolver arm in `sail-plan/src/resolver/query/`.

### Understanding a Spark Compatibility Issue

When PySpark produces different output than expected:

1. Enable debug logging to see the `InitialLogicalPlan`, `FinalLogicalPlan`, and `FinalPhysicalPlan` strings.
2. Check the `spec` IR types — is the spec conversion correct?
3. Check `resolve_data_type` — is the type mapping correct?
4. Check the function implementation — does it handle all edge cases (nulls, empty inputs, extreme values)?
5. Compare with `sail-gold-test` for the specific function or operator.

## The Broader Vision

Sail's long-term goal is not just to be a Spark replacement. It is to be a general-purpose compute engine that happens to support Spark as its primary protocol. Arrow Flight SQL is the second front — it makes Sail accessible to the BI and analytics ecosystem without requiring PySpark. Future work includes additional protocols (JDBC via the Arrow JDBC driver, DuckDB's ADBC driver).

The distributed execution engine is designed to be independent of any specific protocol. The `JobRunner` trait means the same execution layer serves Spark Connect, Flight SQL, and potentially future protocols. The catalog layer is similarly protocol-independent.

Sail is Apache-2.0 licensed and lives at `github.com/lakehq/sail`. Contributions are welcome. The GitHub issues and community Slack (linked in the README) are the primary coordination channels.

## Closing Thoughts

Sail is a demonstration that Rust's performance and safety characteristics, combined with the Arrow and DataFusion ecosystems, are sufficient to build a production-quality query engine that is genuinely faster and cheaper to run than the JVM-based incumbent. The codebase is not small — ~35 crates, hundreds of files — but it is consistently organized. The patterns described in Chapter 8 appear throughout, making new areas of the codebase recognizable once you have read a few.

The Spark API is, in many ways, a good one. The tabular DataFrame API with SQL support has proven itself across thousands of production pipelines. What Sail shows is that the API and the runtime are separable — and that separating them, building the runtime in a systems language with a modern async ecosystem, yields significant gains. Arrow Flight SQL shows that the same compute engine can serve multiple protocols. The arc of the project bends toward a world where "Spark-compatible" means something richer than "runs on the JVM."
