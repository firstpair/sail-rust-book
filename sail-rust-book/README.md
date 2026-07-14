# Sail Rust Book

This book is a definitive guided reading of Sail as a Spark-compatible distributed
query engine. It teaches Rust, Apache Arrow, Apache DataFusion, Spark Connect,
Flight SQL, PySpark interoperability, distributed execution, storage integration,
testing, and extension design by following the code paths that make Sail work.

The source chapters live in `sources/`. The build script renders Markdown and
Mermaid diagrams to PDF, EPUB, HTML, chapter HTML, and MOBI artifacts in
`book/`. The Obsidian vault generator renders a separate code-navigation vault
under `book/dist-obsidian/`.

## Structure

### Part I: Orientation

1. **Architecture Overview** - the end-to-end path from PySpark and SQL clients to
   Sail spec, DataFusion plans, job runners, Arrow streams, and responses.
2. **Rust Foundations in Sail** - `Arc`, trait objects, async traits, actors,
   session extensions, downcasting, and typed errors.

### Part II: Front Doors

3. **Spark Connect** - gRPC service surface, sessions, relations, commands,
   reattachable operations, config, artifacts, and Spark-compatible errors.
4. **PySpark and pysail** - Python packaging, PyO3 startup, the GIL, UDF payloads,
   PySpark compatibility, and Python data sources.
14. **Arrow Flight SQL** - SQL-over-Flight as a second protocol front door that
   converges on the same parser, spec, planner, and `JobRunner`.

### Part III: Data Model and Planning

5. **Apache Arrow** - schemas, arrays, record batches, IPC, PyArrow, extension
   types, shuffle, and common mistakes.
6. **Apache DataFusion** - logical plans, physical plans, session state, extension
   planners, optimizer rules, and execution contracts.
10. **The Sail Spec and Plan Resolver** - Spark Connect and SQL conversion into
   `spec::Plan`, then resolution into DataFusion logical plans.
15. **Custom Nodes and Optimizers** - Sail logical nodes, physical nodes, logical
   optimizer rules, physical optimizer rules, and contributor checklists.

### Part IV: Execution

7. **From Physical Plan to Job Graph** - stage splitting, inputs, distributions,
   driver stages, job topology, and worked examples.
8. **Drivers, Workers, Tasks, and Streams** - actor runtime, worker lifecycle,
   task regions, task attempts, stream management, and job output.
9. **Shuffle and Data Movement** - shuffle write/read, output channels, Arrow
   Flight data movement, and failure behavior.
16. **Local and Streaming Execution** - `LocalJobRunner`, `ClusterJobRunner`,
   streaming plan rewriting, flow-event schemas, streaming sources, and query
   lifecycle.

### Part V: Semantics and Storage

11. **Functions, UDFs, and Codecs** - built-in functions, Python UDFs/UDAFs/UDTFs,
   stream UDFs, remote execution codecs, and worker re-resolution.
12. **Catalogs, Lakehouse Tables, and File Formats** - catalog providers, table
   formats, file scans/writes, Delta, Iceberg, row-level operations, and Python
   data sources.

### Part VI: Extension, Testing, and Practice

13. **Extension Architecture** - a design path for third-party DataFusion
   integrations, plan-time and execution-time extension boundaries, and discussion
   #2001.
17. **Testing Spark Compatibility** - gold tests, PySpark integration tests,
   parser round trips, Flight tests, local/cluster matrices, and extension tests.
18. **Feature Playbooks** - practical checklists for adding functions, UDF paths,
   catalogs, table formats, logical nodes, optimizer rules, streaming sources, and
   distributed codecs.
19. **Roadmap and Codebase Navigation** - task-oriented file map, symptom map,
   maturity/evolution areas, capability snapshot, and closing mental model.
20. **Current Codebase Edition** - July 2026 codebase update, Sail 0.6.6 and
   post-release changes, crate map, and Obsidian vault navigation model.

## Build

The shared FirstPair toolchain supplies the pinned Pandoc, Typst, Node, and
Calibre versions. The source-owned preparation hook stages the chapters and
renders their diagrams before the shared builder creates every format.

Build everything:

```bash
cd sail-rust-book
./build.sh
```

Build only the PDF:

```bash
./build.sh pdf
```

Build artifacts are written to `sail-rust-book/book/` and verified against the
shared FirstPair book contract.

Build the Obsidian vault:

```bash
python3 sail-rust-book/scripts/build-obsidian-vault.py
python3 sail-rust-book/scripts/check-obsidian-vault.py \
  "sail-rust-book/book/dist-obsidian/Sail Rust Book Vault"
```
