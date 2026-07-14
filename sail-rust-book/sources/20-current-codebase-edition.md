# Chapter 20: Current Codebase Edition

This edition updates the book against the local Sail checkout used for this
build, verified on July 14, 2026. The checkout is `main` at
commit `1500ebdf`, whose subject is `fix: count_min_sketch param types +
optimize sketch aggregates (#2190)`. The newest tagged release described in the
local Sail changelog is `0.6.6`, dated July 7, 2026. The important point for a
reader is that Sail has moved from an already coherent Spark-compatible Rust
engine into a broader compatibility and lakehouse implementation surface.

The earlier chapters still describe the core architecture correctly:

```text
client intent
  -> Spark Connect, SQL, or Flight SQL
  -> Sail spec and analyzer state
  -> DataFusion logical plan with Sail extensions
  -> physical plan
  -> local stream or distributed job graph
  -> Arrow RecordBatch stream
  -> protocol-specific response
```

What has changed is the density of the edges. More Spark SQL functions are real.
More lakehouse paths are real. More catalog backends are real. More distributed
execution corner cases have been worked through. The book now needs to be read
less as a sketch of a promising architecture and more as a map of a fast-moving
production codebase.

## The July 2026 Surface

The 0.6.6 changelog emphasizes nine clusters of work:

- distributed query execution and cluster-plan correctness;
- Delta Lake integration;
- `SHOW FUNCTIONS` and `DESCRIBE FUNCTION`;
- Python data-source option recovery from table properties;
- `PIVOT` improvements;
- JSON, CSV, time, struct, binary, and sketch-function parity;
- Spark-compatible overflow and null semantics;
- error preservation across cluster mode;
- typed, vectorized function implementation improvements.

The commits after the 0.6.6 tag continue the same pattern. They add Hive
Metastore support for Spark data-source tables, migrate catalog OpenAPI clients,
add an OpenAPI client generator, support `MERGE INTO` with path-based targets and
DataFrame source references, reject ambiguous UDTF table arguments unless
explicitly enabled, align aggregate and window ordering semantics, align `to_xml`
serialization with Spark, move the codebase to Rust 2024, and raise the Rust
minimum supported compiler.

This is not cosmetic churn. It says where the system is maturing:

- compatibility work has moved from headline protocol support into exact Spark
  behavior;
- lakehouse support has moved from simple reads toward commit, merge, and write
  semantics;
- catalogs are becoming a family of generated and hand-written providers rather
  than a single manager path;
- distributed execution is being hardened at the points where local assumptions
  leak across driver and worker boundaries;
- the function layer is increasingly data-driven, tested, and optimized.

## The Current Crate Map

The current Sail repository has a larger set of crates than a first read of the
architecture suggests. A practical contributor map is:

| Area | Crates |
|---|---|
| Protocol front doors | `sail-spark-connect`, `sail-flight`, `sail-server`, `sail-cli` |
| SQL and functions | `sail-sql-parser`, `sail-sql-analyzer`, `sail-sql-macro`, `sail-function` |
| Spec and planning | `sail-common`, `sail-plan`, `sail-logical-plan`, `sail-logical-optimizer` |
| Session and DataFusion integration | `sail-session`, `sail-common-datafusion` |
| Physical execution | `sail-physical-plan`, `sail-physical-optimizer`, `sail-execution` |
| Python and Arrow interop | `sail-python`, `sail-python-udf`, `sail-pyarrow` |
| Catalogs | `sail-catalog`, `sail-catalog-memory`, `sail-catalog-system`, `sail-catalog-hms`, `sail-catalog-glue`, `sail-catalog-iceberg`, `sail-catalog-unity`, `sail-catalog-onelake` |
| Lakehouse formats | `sail-delta-lake`, `sail-iceberg` |
| Storage and cache | `sail-data-source`, `sail-object-store`, `sail-cache` |
| Support | `sail-build-scripts`, `sail-gold-test`, `sail-telemetry` |

This map is more useful than a dependency graph when you are trying to make a
change. Start with the area that owns the user's observable behavior, then walk
inward until you find the semantic boundary:

- protocol conversion lives near Spark Connect, Flight SQL, or SQL parser code;
- Spark semantic decisions usually land in analyzer, spec, resolver, function,
  or catalog layers;
- DataFusion integration lands in session, logical/physical extension nodes, or
  physical planners;
- distributed behavior lands in job graphs, codecs, workers, task streams, and
  shuffle paths;
- lakehouse writes land in table-format code and driver-side commit rules.

## What The Book Should Teach More Strongly

The review for this edition found ten improvements that matter more than surface
polish.

First, the book needs release-aware text. A reader should know which claims are
timeless architecture and which are July 2026 capability snapshots.

Second, the short late chapters should be expanded. Flight SQL, custom nodes,
local and streaming execution, testing, feature playbooks, and navigation should
be full working chapters because they are exactly where contributors go after
they understand the core path.

Third, SQL function coverage should be less abstract. The implementation now has
enough function metadata, generated code, vectorization work, ANSI behavior, and
Spark parity fixes to deserve a deeper explanation of how one function becomes
parser support, analyzer support, resolver behavior, DataFusion execution, tests,
and remote execution codecs.

Fourth, lakehouse coverage should move past "Delta and Iceberg exist." The book
should explain Delta and Iceberg as table-format contracts that interact with
catalogs, data sources, row-level operations, path-based targets, DataFrame
source references, and driver-side commits.

Fifth, catalog coverage should make the provider family visible. HMS, Glue,
Unity, OneLake, Iceberg REST, system, and memory catalogs are not only names in a
list. They are examples of how Sail isolates namespace, table status,
authentication, generated OpenAPI clients, and Spark-compatible metadata.

Sixth, distributed execution coverage should name the failure modes: scalar
subqueries in distributed plans, remote function semantics, data-source work
stealing, noop sinks, Flight schema mismatches, worker error preservation, and
lakehouse commits running on the driver.

Seventh, cache and object-store architecture should be first-class. The
`sail-cache` and `sail-object-store` crates show how Sail is growing the storage
substrate beneath DataFusion instead of treating object access as a detail.

Eighth, the Rust foundations chapter should be updated for Rust 2024 and the new
MSRV. Contributors need to know when modern language features are available and
when Sail's style still favors explicit boundary types.

Ninth, examples should become traceable. A code excerpt should not be a dead
quotation copied into prose. It should carry a fragment identity, source path,
line range, and subsystem summary.

Tenth, the book now needs a vault edition.

## The Obsidian Vault Edition

The generated Obsidian vault is an additional format, not a replacement for the
PDF or EPUB. Its job is to make the book and codebase navigable together.

The vault is generated at:

```text
sail-rust-book/book/dist-obsidian/Sail Rust Book Vault/
```

It contains:

- all book chapters as Obsidian notes;
- every included Sail source file as a code-file note;
- crate notes;
- subsystem notes;
- code-fragment notes for extracted Rust, Python, and Markdown definitions;
- machine-readable ledgers under `_data/`;
- a local `sail-code-fragments` plugin.

The plugin is intentionally small. A generated chapter note contains
`sail-fragment` cards. Clicking one opens the collocated code-file note and asks
Obsidian to highlight the selected fragment region. This changes the reading
model. The PDF and EPUB teach the system linearly. The vault lets a reader follow
a paragraph into the codebase, then follow the codebase back into crates,
subsystems, and adjacent fragments.

The vault currently excludes generated local environments and data-output
folders such as `.venvs/`, `target/`, `node_modules/`, and `spark-warehouse/`.
That keeps the vault focused on the authored codebase instead of generated
dependency or test-output material.

## How To Build This Edition

From the source book repository:

```sh
cd "$HOME/src/book-sources/sail-rust-book"
./sail-rust-book/build.sh
python3 sail-rust-book/scripts/build-obsidian-vault.py \
  --sail-root "$HOME/src/sail"
python3 sail-rust-book/scripts/check-obsidian-vault.py \
  "sail-rust-book/book/dist-obsidian/Sail Rust Book Vault"
```

The first command builds the FirstPair PDF, EPUB, HTML, chapter HTML, and MOBI
artifacts. The second command builds the Obsidian vault. The third validates
required notes, data ledgers, fragment targets, plugin files, and internal
wikilinks.

Do not confuse this with public publication. Building refreshes local artifacts.
FirstPair publication is a separate outward-facing action governed by
`FIRSTPAIR.md` and the central FirstPair repository.

## The New Mental Model

The previous mental model was a pipeline. Keep it. It is still correct.

The new mental model adds an index:

```text
book paragraph
  -> code fragment
  -> source file note
  -> crate note
  -> subsystem note
  -> neighboring fragments
  -> back to the book
```

For a codebase book, that loop is the point. The book should not only describe
Sail. It should give a reader a durable way to move through Sail while the
project keeps changing.
