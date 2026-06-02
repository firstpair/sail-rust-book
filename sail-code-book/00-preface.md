# Preface

## What Is Sail?

Sail is a query engine that speaks Spark's language. It implements the Apache Spark Connect gRPC protocol in 100% Rust, meaning that any code written for PySpark — every `DataFrame.groupBy`, every `spark.sql(...)`, every UDF — can run against Sail without modification. There is no JVM. There is no Scala runtime. There is no garbage collector pausing your workload at 200 GB/s to decide what to clean up.

The pitch is credible because Sail is not just a SQL engine bolted behind a Spark-shaped facade. It leans on Apache Arrow as its in-memory data format and Apache DataFusion as its query planning and execution backbone — two of the most production-hardened Rust data-processing libraries available. What Sail adds on top is the thick layer of Spark-specific semantics: the precise type-coercion rules, the nullable-by-default schema, the `coalesce` / `repartition` / `range` operators, the Spark session model, the catalog abstractions that connect to AWS Glue, Hive Metastore, Apache Iceberg, Delta Lake, Databricks Unity Catalog, and Microsoft OneLake.

The result is a binary that starts in milliseconds, fits in a Docker image smaller than the JVM alone, and has been benchmarked at roughly 4× faster than Apache Spark on TPC-H with 94% lower infrastructure cost.

## Who This Book Is For

This book is for engineers who want to understand how Sail works at the code level — not just how to use it, but why it is built the way it is, where the interesting decisions live, and how to extend or contribute to it.

The assumed reader is comfortable with Rust: ownership, lifetimes, traits, async/await, and the tokio executor. You do not need to be a Spark veteran; Spark concepts are explained where they differ from what a DataFusion or Arrow user might expect. You do not need prior experience with gRPC, Protobuf, or Arrow Flight; those are introduced in context.

The book is *not* a user guide. For installation, configuration, and PySpark compatibility tables, see the [Sail documentation](https://docs.lakesail.com/sail/latest/).

## How to Read This Book

Each chapter is self-contained but builds on the previous one. The path through a query — from PySpark client to Arrow bytes on the wire — is the organizing spine:

- **Chapter 1** gives the 10,000-foot view: how all the pieces connect.
- **Chapters 2–5** trace a query from the moment it enters the gRPC server through logical planning, optimization, physical execution, and result delivery.
- **Chapters 6–7** go deeper into the execution engine and the catalog layer, which are the parts most likely to need extension.
- **Chapter 8** collects Rust patterns that appear throughout the codebase — the actor model, error propagation, code generation, the PyO3 bridge — explained in one place.
- **Chapter 9** closes with how to navigate the codebase and contribute.

Code is quoted directly from the repository. File paths are given relative to the repository root (`crates/sail-spark-connect/src/server.rs`) so you can follow along. Version at time of writing: **0.6.3**.

If you want to skip to a specific subsystem:

| "I want to understand..." | Start at |
|---|---|
| The Spark-to-Sail entry point | Chapter 2 |
| How Arrow flows through the system | Chapter 3 |
| DataFusion extension points | Chapter 4 |
| How results stream back to Python | Chapter 5 |
| The distributed executor | Chapter 6 |
| Adding a new catalog | Chapter 7 |
| Rust architectural patterns | Chapter 8 |
