# Chapter 20: The 0.7.2 Preparation Snapshot

This edition separates two source trees. Upstream `lakehq/sail` is pinned at
`85d06ce08825dfba4e066e9a5f54123e40829d43`; its workspace declares 0.7.1 and the
book prepares for the planned 0.7.2 release. The experimental extension tree is
`querygraph/sail` at `dcd44f287b8422a62abd06d2410aaf96e225e136`. An experimental
feature's presence in that tree does not make it part of an upstream release.

The architecture remains a sequence of translations:

```text
client intent -> protocol or SQL -> Sail planning representation
  -> DataFusion logical and physical plans -> local or distributed execution
  -> Arrow results -> client protocol
```

The ownership of those translations has changed since the July edition. The
current source uses `DataSource` and optional `LakeSource` capabilities, separate
task-runner actors and stream managers, and codec code under execution's `proto/`
module. Following an obsolete filename can obscure an API change rather than
merely delay navigation.

## Changes That Matter for Extension Authors

Merged upstream #2630 lets an embedder select the Spark server's session factory.
The default entry point still selects the ordinary Spark factory. This provides a
way to compose session setup; it does not by itself deploy native packages to
workers or serialize their operators.

Upstream #2675 adds mimalloc as the Rust global allocator for the CLI and Python
extension. It does not turn a separately built wheel's allocations into admitted
query memory. The experimental resource-domain and lease contract remains a
separate accounting/ownership mechanism. Existing measured artifacts predate
this allocator change and must keep their original source attribution.

Upstream #2676 adds `SelectSemiJoinBuildSide` after DataFusion's
`JoinSelection`. For a partitioned, non-null-aware left semi-join with
inconclusive size statistics, it can build from the right input when that input
is an aggregate grouped on the join keys. It preserves partitioned execution;
it does not infer that the aggregate is small enough to broadcast. The rule is
in `crates/sail-physical-optimizer/src/select_semi_join_build_side.rs`.
The newer main snapshot also updates npm dependencies and migration guidance.
These changes do not retroactively apply to the experimental branch or its
measured binaries.

The extension branch tests that boundary with Sedona scalar functions and native
graph kernels. Pecan provides the relational control path: graph algorithms can
run as ordinary distributed Sail queries without adding a native graph operator.
Argentea then isolates what retained worker-native graph state actually needs:
package decoding, task scope, stable ownership, admission and lifecycle cleanup.

## The Current Crate Map

The current Sail repository has a larger set of crates than a first read of the
architecture suggests. A practical contributor map is:

| Area | Crates |
|---|---|
| Protocol front doors | `sail-spark-connect`, `sail-flight`, `sail-cli` |
| SQL and functions | `sail-sql-parser`, `sail-sql-analyzer`, `sail-sql-macro`, `sail-function` |
| Spec and planning | `sail-common`, `sail-plan`, `sail-logical-plan`, `sail-logical-optimizer` |
| Session and DataFusion integration | `sail-session`, `sail-common-datafusion` |
| Physical execution | `sail-physical-plan`, `sail-physical-optimizer`, `sail-execution` |
| Python and Arrow interop | `sail-python`, `sail-python-udf` |
| Catalogs | `sail-catalog`, `sail-catalog-memory`, `sail-catalog-system`, `sail-catalog-hms`, `sail-catalog-glue`, `sail-catalog-iceberg`, `sail-catalog-unity`, `sail-catalog-onelake` |
| Lakehouse formats | `sail-delta-lake`, `sail-iceberg` |
| Storage and cache | `sail-data-source`, `sail-object-store`, `sail-cache`, `sail-system-store` |
| Shuffle service integration | `sail-celeborn` |
| Support | `sail-build-scripts`, `sail-gold-test`, `sail-telemetry`, `sail-common-hms`, `sail-mimalloc` |

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

## Reading Source and Evidence Together

For an implementation claim, begin with the pinned tree and the owning module.
For a qualification claim, begin with the receipt's host binary, native wheel,
source revision and command. These are related but different questions. A new
source commit does not retroactively change which executable an older receipt
tested.

The graph qualification records include exact vectors, owner identities, native
phases, Sail stage/task inventories, cleanup and failed attempts. PageRank and the
original BFS bound have physical two-host results. The later WCC/SSSP fault and
memory-reuse checks and expanded BFS bound have ARM process-cluster evidence;
remaining platform gates stay explicit in the extension integration document.
Performance comparisons have their own datasets, semantics, resource envelopes,
profiles and time/memory boundaries.

Source-path validation in this book is intentionally limited. The script
`sail-rust-book/scripts/audit-source-paths.py` resolves explicit inline code paths
against both pinned trees. A successful check proves path existence, not that a
Rust excerpt compiles or that its explanation is correct. Those require source
review and, for runnable examples, execution against the corresponding artifact.

## Build the Ordinary Book Formats

The source repository owns the manuscript, diagrams, metadata and source-specific
preparation hook. `FIRSTPAIR.md` defines the delivery identity and points to the
shared FirstPair builder. From a checkout with the required FirstPair tooling:

```sh
repo_root="$(git rev-parse --show-toplevel)"
"$HOME/src/firstpair/publishing/scripts/build-library-book.sh" \
  --repo-root "$repo_root"
```

`book.build.json` selects the preparation hook and output formats. A local build
produces the ordinary book artifacts; it does not publish the public library.
Validate the PDF visually and check EPUB, HTML and links as well as successful
process exit. A source revision and an existing generated PDF can represent
different editions until the build and its validation have completed.

## The Code-Navigation Vault

The Obsidian vault is a separate generated product linking chapters, code files,
fragments and symbols. A vault generated from an older source pin is not a current
code index merely because the Markdown manuscript has changed. Its source
identity and fragment targets require their own regeneration and validation.

Vault mutation has an additional operational requirement: close it in Obsidian
and confirm closure before generation, because the application can rewrite index
and workspace files concurrently. The source contract and shared FirstPair
workflow define candidate validation and publication. Ordinary manuscript and
PDF/EPUB work can proceed independently of vault replacement.

## Continue from a Concrete Boundary

To add a scalar function, follow resolution, field semantics and codec
reconstruction. To add a source, follow logical reads/writes and any optional lake
capability. To add native state, follow admission, owner lifetime, task placement
and failure behavior. Keep domain computation outside the host when an existing
plan can express it, and add a host hook only for an observed missing contract.

This makes the extension experiment useful beyond graphs: it provides executable
examples of where Sail's existing abstractions suffice and where a focused
lifecycle or serialization contract is required.
