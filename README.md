# Learning Rust, Apache Arrow, and DataFusion Through Sail

This book is a guided reading of Sail as a modern distributed query engine. It teaches Rust, Apache Arrow, Apache DataFusion, Spark Connect, PySpark interoperability, and extension design by following the code paths that make Sail work.

The first pass is intentionally chapter-by-chapter so each chapter can stay close to the code and leave room for examples, diagrams, and refinements.

## Proposed Structure

### Part I: The System Shape

1. **Architecture Overview**
   - Sail as a Spark-compatible server backed by DataFusion.
   - Local versus cluster execution.
   - The high-level path from PySpark to Spark Connect to Sail specs, DataFusion logical plans, physical plans, stages, tasks, and Arrow batches.
   - The extension seams that matter for issue #1810.

2. **Rust Foundations in Sail**
   - `Arc`, trait objects, async traits, actors, ownership, error propagation, and typed extension patterns.
   - Why Sail uses Rust for query planning, execution, and transport.
   - Reading examples: `JobRunner`, `ExecutionPlan`, `ServerSessionFactory`, and `SessionExtension`.

### Part II: Front Doors and Compatibility

3. **Spark Connect**
   - Spark Connect as Sail's wire protocol.
   - gRPC service shape in `sail-spark-connect`.
   - Request handling, session lookup, relation versus command execution, and streaming responses.
   - How Spark Connect constrains Sail's data types, errors, and reattachment behavior.

4. **PySpark and pysail**
   - How PySpark talks to Sail with no Python query engine in the middle.
   - The `pysail` Python package and its PyO3 bridge to the Rust server.
   - PySpark UDF registration and execution.
   - Where Python entry-point based extensions would fit.

### Part III: The Columnar Runtime

5. **Apache Arrow**
   - Arrow arrays, schemas, record batches, and IPC streams.
   - How Sail serializes results back to Spark Connect clients.
   - Arrow Flight in the distributed data plane.
   - Arrow extension types for GeoArrow and variants.

6. **Apache DataFusion**
   - Logical plans, optimizer rules, physical plans, execution plans, partitions, and task contexts.
   - Sail's custom query planner and extension physical planner.
   - Function registries and Spark compatibility functions.
   - How DataFusion provides the kernel while Sail supplies Spark semantics.

### Part IV: Distributed Query Processing

7. **From Physical Plan to Job Graph**
   - How Sail splits a DataFusion physical plan into stages.
   - Shuffle boundaries, repartitioning, coalescing, broadcast, merge, and rescale modes.
   - Why distributed planning has to rewrite some joins and limits.

8. **Drivers, Workers, Tasks, and Streams**
   - The actor-based control plane.
   - Worker managers for local cluster and Kubernetes.
   - Task scheduling, attempts, worker registration, and task status.
   - The stream manager and how task output is located.

9. **Shuffle and Data Movement**
   - `ShuffleWriteExec`, `ShuffleReadExec`, output channels, hash distribution, and round-robin distribution.
   - Arrow Flight as the data plane.
   - How shuffle data flows between workers and returns results to the driver.

### Part V: Planning Spark Semantics

10. **The Sail Spec and Plan Resolver**
    - Sail's unresolved plan representation.
    - SQL parser and Spark relation conversion.
    - Name resolution, catalogs, functions, commands, and logical extension nodes.

11. **Functions, UDFs, and Codecs**
    - Built-in scalar, aggregate, window, generator, and table functions.
    - PySpark UDF/UDAF/UDTF representation.
    - Why distributed execution needs physical-plan encoding and UDF re-resolution on workers.

12. **Catalogs, Lakehouse Tables, and File Formats**
    - Catalog manager, table format registry, system tables, Delta/Iceberg/lakehouse planner extensions.
    - How file scans and writes cross the DataFusion/Sail boundary.

### Part VI: Extensions

13. **Extension Architecture: From Proposal to Design**
    - Issue #1810 as the central design problem.
    - A unified `SailExtension` trait.
    - Function registration, session config, logical and physical optimizer rules, physical extension planners, and codec fallback registries.
    - Python entry-point discovery for `pip install pysail pysail-sedona`.
    - Collision policy, ordering policy, per-session enablement, and distributed-worker compatibility.
    - Worked extension examples: scalar UDF, optimizer rule, physical planner node, and Sedona-style spatial join.

## Reading Map

Core files for the first chapter:

- `docs/concepts/architecture/index.md`
- `docs/concepts/query-planning/index.md`
- `crates/sail-spark-connect/src/server.rs`
- `crates/sail-spark-connect/src/service/plan_executor.rs`
- `crates/sail-plan/src/lib.rs`
- `crates/sail-session/src/session_factory/server.rs`
- `crates/sail-session/src/planner.rs`
- `crates/sail-execution/src/job_runner.rs`
- `crates/sail-execution/src/job_graph/mod.rs`
- `crates/sail-execution/src/job_graph/planner.rs`
- `crates/sail-execution/src/plan/shuffle_write.rs`
- `crates/sail-execution/src/plan/shuffle_read.rs`
- `crates/sail-execution/src/codec.rs`

The extension proposal is GitHub issue #1810: "Extension API for third-party DataFusion integrations (UDFs, optimizer rules, planner extensions)."
