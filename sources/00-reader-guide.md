# Reader Guide: How This Book Builds

This book is meant to be read in order, but it is also a code companion. Each chapter
introduces one architectural layer, then later chapters reuse that layer when the
distributed and extension stories become more demanding.

## Chapter Links

| Part | Chapters | What They Establish |
|---|---|---|
| System shape | [1. Architecture Overview](01-architecture-overview.md), [2. Rust Foundations](02-rust-foundations-in-sail.md) | The full query pipeline and the Rust patterns that make it possible. |
| Front doors | [3. Spark Connect](03-spark-connect.md), [4. PySpark and pysail](04-pyspark-and-pysail.md) | How user intent enters Sail through PySpark, gRPC, protobufs, and Python packaging. |
| Columnar runtime | [5. Apache Arrow](05-apache-arrow.md), [6. Apache DataFusion](06-apache-datafusion.md) | The data model and query engine Sail builds on. |
| Distribution | [7. Physical Plan to Job Graph](07-physical-plan-to-job-graph.md), [8. Drivers, Workers, Tasks, and Streams](08-drivers-workers-tasks-and-streams.md), [9. Shuffle and Data Movement](09-shuffle-and-data-movement.md) | How one DataFusion plan becomes distributed task execution and Arrow stream movement. |
| Spark semantics | [10. Sail Spec and Plan Resolver](10-sail-spec-and-plan-resolver.md), [11. Functions, UDFs, and Codecs](11-functions-udfs-and-codecs.md), [12. Catalogs, Lakehouse Tables, and File Formats](12-catalogs-lakehouse-tables-and-file-formats.md) | How Spark-compatible names, expressions, functions, commands, tables, and writes become executable DataFusion objects. |
| Extension design | [13. Extension Architecture](13-extension-architecture-from-proposal-to-design.md) | How the previous patterns become a proposed extension architecture for issue #1810. |

## Concept Progression

The chapters deliberately introduce concepts before relying on them:

| Concept | Introduced | Elaborated | Used For Extensions |
|---|---|---|---|
| Spark Connect unresolved plans | Chapter 3 | Chapters 4 and 10 | Chapter 13, where extensions must preserve Spark-facing behavior. |
| Spark Connect extension messages | Chapter 3 | Chapter 10 | Chapter 13, where `Relation.extension`, `Command.extension`, and `Expression.extension` become the plan-time extension ABI. |
| Rust trait objects and `Arc` | Chapter 2 | Chapters 6, 8, 11, and 12 | Chapter 13, where execution-time extension capabilities are trait-object contributions. |
| Arrow `RecordBatch` streams | Chapter 5 | Chapters 8 and 9 | Chapters 11 and 13, where UDFs and custom operators must execute on Arrow batches. |
| DataFusion logical and physical plans | Chapter 6 | Chapters 7 and 10 | Chapter 13, where extensions add optimizer rules and physical planners. |
| Job graphs and stages | Chapter 7 | Chapters 8 and 9 | Chapter 13, where extension plans must survive distributed execution. |
| Typed session extensions | Chapter 2 | Chapters 6, 11, and 12 | Chapter 13, where the extension registry is proposed as a session service. |
| Function resolution and codecs | Chapter 11 | Chapter 13 | The core reason the execution-time extension boundary needs worker-side registration and serialization. |
| Table format registry | Chapter 12 | Chapter 13 | The strongest existing model for extension registration. |
| Two extension boundaries | Chapter 1 | Chapters 3, 10, 11 | Chapter 13, where plan-time (Spark Connect dispatch) and execution-time (DataFusion-shaped contributions) are designed separately under one `SailExtension` object. |

## Code Reading Strategy

Each chapter has a code map, but these are the highest-leverage excerpts to read
first:

| Question | Best Code To Read |
|---|---|
| How does a Spark Connect request enter Sail? | `crates/sail-spark-connect/src/service/plan_executor.rs` and `crates/sail-spark-connect/src/server.rs` |
| How does Sail create a session? | `crates/sail-session/src/session_factory/server.rs` |
| How does Sail customize DataFusion planning? | `crates/sail-session/src/planner.rs` |
| How does a physical plan become distributed work? | `crates/sail-execution/src/job_graph/planner.rs` and `crates/sail-execution/src/job_runner.rs` |
| How do tasks run on workers? | `crates/sail-execution/src/task_runner/core.rs` |
| How does shuffle move Arrow batches? | `crates/sail-execution/src/plan/shuffle_write.rs`, `crates/sail-execution/src/plan/shuffle_read.rs`, and `crates/sail-execution/src/stream/` |
| How do Spark functions become DataFusion functions? | `crates/sail-plan/src/resolver/expression/function.rs` and `crates/sail-plan/src/function/` |
| How do Python UDFs execute? | `crates/sail-python-udf/src/udf/` and `crates/sail-python-udf/src/stream.rs` |
| How do custom functions and plans reach workers? | `crates/sail-execution/src/codec.rs` |
| How do catalogs and file formats plug in? | `crates/sail-catalog/src/manager/mod.rs`, `crates/sail-common-datafusion/src/datasource.rs`, and `crates/sail-session/src/formats.rs` |
| How do lakehouse row-level operations work? | `crates/sail-plan-lakehouse/src/lib.rs`, `crates/sail-delta-lake/src/table_format.rs`, and `crates/sail-logical-plan/src/merge.rs` |

## What To Look For In Code Excerpts

The best excerpts in this book are not chosen because they are short. They are chosen
because they reveal a boundary:

- a protobuf boundary,
- a Python/Rust boundary,
- a Spark/DataFusion semantic boundary,
- a local/distributed execution boundary,
- a driver/worker serialization boundary,
- a catalog/table-format boundary,
- or an extension registration boundary.

When reading a snippet, ask what it converts from and what it converts to. Sail's
architecture is mostly a sequence of careful conversions.

Navigation: [Start Chapter 1: Architecture Overview](01-architecture-overview.md)
