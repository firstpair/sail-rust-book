# Reader Guide: How This Book Builds

This book is mostly meant to be read in order, but it is also a code companion.
Chapters 1-13 walk the core system from architecture through extension design.
Chapters 14-20 are definitive-guide companion chapters: they fill protocol,
optimizer, streaming, testing, contribution, and navigation coverage that readers
will want after the main pass.

## Chapter Links

The book is organized into seven reading paths:

- System shape: Chapters 1 and 2 establish the full query pipeline and the Rust
  patterns that make it possible.
- Front doors: Chapters 3, 4, and 14 show how user intent enters Sail through
  PySpark, Spark Connect, SQL, Flight SQL, gRPC, protobufs, and Python
  packaging.
- Columnar runtime: Chapters 5 and 6 introduce the Arrow data model and the
  DataFusion query engine Sail builds on.
- Distribution: Chapters 7, 8, 9, and 16 show how one DataFusion plan becomes
  local execution, streaming execution, distributed tasks, and Arrow stream
  movement.
- Spark semantics: Chapters 10, 11, 12, and 15 explain how Spark-compatible
  names, expressions, functions, commands, tables, writes, custom logical nodes,
  and optimizer rules become executable DataFusion objects.
- Extension design: Chapter 13 turns the previous patterns into a proposed
  extension architecture for discussion #2001.
- Practice and navigation: Chapters 17, 18, 19, and 20 cover verification,
  feature work, codebase navigation, and the July 2026 Sail surface.

## Concept Progression

The chapters deliberately introduce concepts before relying on them. This section
uses a list instead of a table so the PDF can break cleanly across pages.

- Spark Connect unresolved plans appear first in Chapter 3, then return in
  Chapters 4 and 10. Chapter 13 uses them to explain why extensions must
  preserve Spark-facing behavior.
- Spark Connect extension messages appear first in Chapter 3 and are elaborated
  in Chapter 10. Chapter 13 treats `Relation.extension`, `Command.extension`,
  and `Expression.extension` as the plan-time extension ABI.
- Flight SQL as a second front door appears first in Chapter 14 and connects
  back to Chapters 6 and 10. Chapter 13 uses it to motivate protocol-independent
  registration.
- Rust trait objects and `Arc` appear first in Chapter 2 and return in Chapters
  6, 8, 11, 12, and 16. Chapter 13 uses them for execution-time extension
  capabilities.
- Arrow `RecordBatch` streams appear first in Chapter 5 and return in Chapters
  8, 9, 14, and 16. Chapters 11 and 13 use them for UDFs and custom operators
  that execute on Arrow batches.
- DataFusion logical and physical plans appear first in Chapter 6 and return in
  Chapters 7, 10, and 15. Chapter 13 uses them for optimizer rules and physical
  planners.
- Job graphs and stages appear first in Chapter 7 and return in Chapters 8 and
  9. Chapters 13 and 18 use them to show how extension plans survive distributed
  execution.
- Streaming flow events appear first in Chapter 16 and connect back to Chapters
  5 and 15. Chapter 18 uses them for streaming sources that must emit the right
  event schema.
- Typed session extensions appear first in Chapter 2 and return in Chapters 6,
  11, 12, 14, and 16. Chapter 13 proposes the extension registry as a session
  service.
- Function resolution and codecs appear in Chapter 11 and return in Chapters 17
  and 18. They are the core reason the execution-time extension boundary needs
  worker-side registration and serialization.
- The table format registry appears in Chapter 12 and returns in Chapter 18. It
  is the strongest existing model for extension registration.
- The two extension boundaries appear in Chapter 1 and return in Chapters 3,
  10, 11, 13, and 18. Chapter 13 separates plan-time and execution-time
  contributions.
- The Obsidian code vault appears in Chapter 20 and in the vault edition. It is
  the navigable companion where book chapters, code files, fragments, crates,
  and subsystems are linked directly.

## Code Reading Strategy

Each chapter has a code map, but these are the highest-leverage excerpts to read
first:

- How does a Spark Connect request enter Sail? Read
  `crates/sail-spark-connect/src/service/plan_executor.rs` and
  `crates/sail-spark-connect/src/server.rs`.
- How does Sail create a session? Read
  `crates/sail-session/src/session_factory/server.rs`.
- How does Sail customize DataFusion planning? Read
  `crates/sail-session/src/planner.rs`.
- How does a physical plan become distributed work? Read
  `crates/sail-execution/src/job_graph/planner.rs` and
  `crates/sail-execution/src/job_runner.rs`.
- How do tasks run on workers? Read `crates/sail-execution/src/task_runner/core.rs`.
- How does shuffle move Arrow batches? Read
  `crates/sail-execution/src/plan/shuffle_write.rs`,
  `crates/sail-execution/src/plan/shuffle_read.rs`, and
  `crates/sail-execution/src/stream/`.
- How do Spark functions become DataFusion functions? Read
  `crates/sail-plan/src/resolver/expression/function.rs` and
  `crates/sail-plan/src/function/`.
- How do Python UDFs execute? Read `crates/sail-python-udf/src/udf/` and
  `crates/sail-python-udf/src/stream.rs`.
- How do custom functions and plans reach workers? Read
  `crates/sail-execution/src/codec.rs`.
- How do catalogs and file formats plug in? Read
  `crates/sail-catalog/src/manager/mod.rs`,
  `crates/sail-common-datafusion/src/datasource.rs`, and
  `crates/sail-session/src/formats.rs`.
- How do lakehouse row-level operations work? Read `crates/sail-delta-lake/`,
  `crates/sail-iceberg/`, `crates/sail-logical-plan/`, and
  `crates/sail-session/src/formats.rs`.
- How does Flight SQL enter the same engine? Read `crates/sail-flight/src/service.rs`.
- Where are custom logical nodes planned physically? Read
  `crates/sail-session/src/planner.rs`.
- Where are optimizer rules registered? Read
  `crates/sail-session/src/session_factory/server.rs` and
  `crates/sail-physical-optimizer/src/lib.rs`.
- How does structured streaming change a plan? Read
  `crates/sail-plan/src/streaming/rewriter.rs` and
  `crates/sail-common-datafusion/src/streaming/event/schema.rs`.
- How should I add a feature safely? Read Chapter 18, Feature Playbooks.
- How do I jump from prose to exact code fragments? Read Chapter 20, Current
  Codebase Edition, and use the generated Obsidian vault.

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
