# Chapter 19: Roadmap And Codebase Navigation

This final chapter is a field guide. It tells you where to start, what is solid,
what is evolving, and how to keep your mental model synchronized with the code.

Sail moves quickly, so treat exact capability claims as snapshots. The stable part
is the architecture: front doors converge to spec, spec resolves to DataFusion,
DataFusion plans run locally or through the distributed job runner, and Arrow
batches move through every layer.

## Start By Task

| Goal | Start here |
|---|---|
| Understand a PySpark query | `crates/sail-spark-connect/src/service/plan_executor.rs` |
| Understand SQL parsing | `crates/sail-sql-parser/src/parser.rs` |
| Understand SQL to spec | `crates/sail-sql-analyzer/src/statement.rs` |
| Understand spec to logical plan | `crates/sail-plan/src/resolver/` |
| Understand session setup | `crates/sail-session/src/session_factory/server.rs` |
| Understand physical planning | `crates/sail-session/src/planner.rs` |
| Understand local execution | `crates/sail-execution/src/job_runner.rs` |
| Understand distributed execution | `crates/sail-execution/src/driver/` |
| Understand job graph splitting | `crates/sail-execution/src/job_graph/` |
| Understand shuffles | `crates/sail-execution/src/plan/shuffle_*.rs` and `crates/sail-execution/src/stream/` |
| Understand Python UDFs | `crates/sail-python-udf/src/` |
| Understand catalogs | `crates/sail-catalog/` and `crates/sail-session/src/catalog.rs` |
| Understand table formats | `crates/sail-common-datafusion/src/datasource.rs` and `crates/sail-session/src/formats.rs` |
| Understand Delta and Iceberg | `crates/sail-delta-lake/`, `crates/sail-iceberg/`, and `crates/sail-session/src/planner.rs` |
| Understand extensions | `crates/sail-session`, `crates/sail-plan`, `crates/sail-execution/src/codec.rs` |

## Start By Symptom

| Symptom | Likely area |
|---|---|
| PySpark API call fails before planning | Spark Connect service/proto conversion |
| SQL text parses incorrectly | SQL parser or analyzer |
| Column cannot be resolved | resolver state or attribute resolution |
| Function gives Spark-incompatible output | function registry or implementation |
| Works in SQL but not DataFrame API | protocol-to-spec conversion mismatch |
| Works locally but not in cluster | codec, worker session, shuffle, or task execution |
| Table name resolves incorrectly | catalog manager or namespace handling |
| File scan has wrong schema | table format or source option resolution |
| Merge/delete fails | lakehouse optimizer/planner path |
| Streaming query starts but never finishes/stops | streaming query manager or background task |

## What Is Mature

The following areas are central and well established architecturally:

- Spark Connect request handling,
- Sail spec as the internal semantic boundary,
- DataFusion logical and physical planning,
- Arrow `RecordBatch` execution,
- local `JobRunner`,
- catalog/table-format separation,
- custom logical/physical node pattern,
- Python UDF architecture,
- distributed job graph architecture.

Mature does not mean bug-free. It means the architecture is settled enough that new
work should usually fit into the existing pattern.

## What Is Evolving

Several areas are active design surfaces:

- third-party extension architecture,
- DataFusion FFI or other execution-time plugin boundaries,
- worker-side extension registration and codec negotiation,
- full streaming feature coverage,
- lakehouse write breadth,
- Iceberg writes,
- broader Flight SQL session semantics,
- Kubernetes worker lifecycle and fault tolerance,
- remote stream storage and shuffle spill behavior.

When working in these areas, prefer small changes that preserve future extension
options.

## Capability Snapshot

At the time this guide was prepared, the important capability shape is:

| Area | Current shape |
|---|---|
| PySpark/DataFrame | Primary target through Spark Connect |
| SQL | Custom parser/analyzer, Spark syntax focus |
| Flight SQL | Secondary SQL front door |
| Arrow | Core memory and wire model |
| DataFusion | Query kernel |
| Local execution | Direct DataFusion stream execution |
| Cluster execution | Driver/worker/job graph/task stream architecture |
| Python UDFs | Multiple PySpark UDF/UDTF paths |
| Catalogs | Memory, Glue, HMS, Iceberg REST, Unity, OneLake, system |
| Formats | Listing file formats, Delta, Iceberg, Python data sources |
| Delta | Reads, append/overwrite writes, row-level paths for MERGE/DELETE, variant-related work |
| Iceberg | Read path, write support evolving |
| Streaming | Architecture present, feature coverage evolving |

Always verify exact support in the current repository before documenting a public
claim. Capability surfaces move faster than architecture chapters.

## Reading The Crate Graph

The crate graph has a useful directional shape:

```text
protocol crates
  -> spec/resolver/session crates
  -> DataFusion extension crates
  -> execution/storage/support crates
```

The most important rule is separation:

- `sail-spark-connect` should know about Spark protobufs.
- `sail-plan` should know about Sail spec and DataFusion logical plans.
- `sail-session` should assemble DataFusion session state and physical planners.
- `sail-execution` should execute physical plans without caring which protocol
  produced them.

When a change violates that separation, pause. Sometimes it is necessary, but it
usually signals that a boundary type or extension point is missing.

## The Files Worth Bookmarking

Bookmark these first:

- `crates/sail-spark-connect/src/service/plan_executor.rs`
- `crates/sail-common/src/spec/plan.rs`
- `crates/sail-plan/src/lib.rs`
- `crates/sail-plan/src/resolver/plan.rs`
- `crates/sail-plan/src/resolver/query/mod.rs`
- `crates/sail-session/src/session_factory/server.rs`
- `crates/sail-session/src/planner.rs`
- `crates/sail-execution/src/job_runner.rs`
- `crates/sail-execution/src/job_graph/planner.rs`
- `crates/sail-execution/src/codec.rs`
- `crates/sail-common-datafusion/src/session/job.rs`
- `crates/sail-common-datafusion/src/datasource.rs`

These files are not the whole system. They are the quickest route back to the
architecture when you feel lost.

## The Definitive Mental Model

The entire guide can be compressed to one path:

```text
client intent
  -> protocol-specific message or SQL
  -> Sail spec
  -> DataFusion logical plan with Sail extensions
  -> optimized logical plan
  -> physical plan with Sail extensions
  -> local stream or distributed job graph
  -> Arrow RecordBatch stream
  -> protocol-specific response
```

And one warning:

```text
If a feature cannot survive every boundary it crosses, it is not complete.
```

## Closing

Sail is interesting because it is not merely a Rust rewrite of Spark. It is a
careful compatibility layer over a modern Rust query stack. The best way to learn it
is to follow conversions: protobuf to spec, SQL to spec, spec to logical plan,
logical plan to physical plan, physical plan to job graph, job graph to tasks, tasks
to Arrow streams, Arrow streams back to the client.

That is the shape to preserve as Sail grows.

Navigation: [Previous: Chapter 18, Feature Playbooks](18-feature-playbooks.md) | [Reader Guide](00-reader-guide.md)
