# Chapter 12: Catalogs, Lakehouse Tables, and Data Sources

Storage has several identities: a catalog name, a table location, a source
implementation, a logical query, and the physical files or transaction metadata
that execution reads or changes. Treating these as one object makes it hard to
explain where validation and side effects belong.

This chapter follows upstream `b2470ea4b`, still declaring Sail 0.7.1 while the
book prepares for 0.7.2. Its source interface is `DataSource`, with optional
`LakeSource` capability. The earlier edition's `TableFormat` trait and physical
writer sketch are obsolete; they are not the current embedding API.

## Code Map

| Responsibility | Current source |
|---|---|
| Data source contract and registry | `crates/sail-common-datafusion/src/datasource.rs` |
| Lake metadata, DDL and DML capability | `crates/sail-common-datafusion/src/lakesource.rs` |
| Session source registration | `crates/sail-session/src/formats.rs` |
| Listing source implementation | `crates/sail-data-source/src/listing/source.rs` |
| Listing logical write node | `crates/sail-data-source/src/listing/write.rs` |
| Listing physical planner | `crates/sail-data-source/src/listing/planner.rs` |
| Delta source | `crates/sail-delta-lake/src/lake_source.rs` |
| Iceberg source | `crates/sail-iceberg/src/lake_source.rs` |
| Python adapter | `crates/sail-data-source/src/formats/python/adapter.rs` |
| Generic row-level logical nodes | `crates/sail-logical-plan/src/row_level.rs` |

## Catalogs: Names Before Data

The `CatalogManager` in `crates/sail-catalog/src/manager/mod.rs` is a session
extension. It owns:

- the configured catalog providers,
- the default catalog,
- the default database,
- the global temporary database,
- temporary views,
- registered functions,
- tracked logical plans and function objects.

Its most important job is name resolution. A query like this:

```sql
SELECT * FROM sales.orders
```

does not say whether `sales` is a catalog or a database. Sail follows Spark-style
resolution:

```text
[name]
  -> default catalog + default database + table

[prefix..., table]
  -> if prefix starts with a known catalog, use that catalog
  -> otherwise use default catalog and treat prefix as database
```

The result of resolution is not data. It is metadata: a `TableStatus`. The table can
be a physical table, a view, a temporary view, or a global temporary view.

```rust
pub enum TableKind {
    Table { ... },
    View { ... },
    TemporaryView { ... },
    GlobalTemporaryView { ... },
}
```

That enum is small, but it carries a lot of Spark compatibility:

| Kind | What Sail does with it |
|---|---|
| `Table` | Builds a table scan using the table's format, location, schema, properties, and partitioning. |
| `View` | Parses the stored SQL definition and resolves it again into a logical plan. |
| `TemporaryView` | Reuses a stored logical plan from the session. |
| `GlobalTemporaryView` | Reuses a stored logical plan from the configured global temporary database. |

This is why catalogs come before file formats. Sail cannot decide whether to invoke
Parquet, Delta, Iceberg, or Python data source code until it knows what the name
refers to.

## Catalog Providers

Session startup builds the catalog manager in `crates/sail-session/src/catalog.rs`.
The configured catalog list can include:

- memory catalog,
- Iceberg REST catalog,
- Unity catalog,
- OneLake catalog,
- Glue catalog,
- Hive Metastore catalog,
- the built-in system catalog.

Some catalog providers are wrapped in `RuntimeAwareCatalogProvider`, which lets them
perform blocking or IO-heavy setup on the runtime intended for IO. Some are wrapped
in `CachingCatalogProvider`, depending on whether the configuration asks for global
or session cache behavior.

The key Rust idea here is trait objects:

```rust
HashMap<Arc<str>, Arc<dyn CatalogProvider>>
```

Sail does not need every catalog backend to have the same concrete Rust type. It
needs each backend to implement the `CatalogProvider` contract.

That same shape reappears for named data sources:

```rust
HashMap<String, Arc<dyn DataSource>>
```

This is a pattern worth remembering for extension design: Sail favors small traits
plus session registries over large enums that must know every implementation.

## DataSource Is a Logical Planning Interface

`DataSource` names a source and plans its reads and writes. The essential return
types distinguish it from the old physical-writer model:

```rust
async fn create_source(
    &self,
    ctx: &dyn Session,
    info: SourceInfo,
) -> Result<Arc<dyn TableSource>>;

async fn create_writer(
    &self,
    ctx: &dyn Session,
    info: SinkInfo,
) -> Result<LogicalPlan>;
```

These are method excerpts from the trait in `datasource.rs`. A read returns a
logical table source; a write returns a logical plan. Physical planning happens
later. The default `infer_schema` obtains a source and reads its schema, while an
implementation can specialize schema inference.

A source may expose `as_lake_source(self: Arc<Self>)`. Its default is `None`.
The owned receiver keeps the source alive while a caller uses the capability
across asynchronous operations. A plain file or streaming source need not pretend
to implement lakehouse metadata or row-level transaction semantics.

## Registration and Capabilities

`DataSourceRegistry` stores `Arc<dyn DataSource>` values under lowercase names
behind an `RwLock`. `get_data_source` retrieves a source; `get_lake_source` also
requires the optional lake capability. A registered Parquet source can therefore
be a valid data source while being invalid for a lake operation.

The registry's `register_data_source` uses map insertion. It does not implement
the native extension bootstrap's duplicate-name refusal policy. Reusing a registry
pattern does not mean that two registries have identical collision semantics.

`crates/sail-session/src/formats.rs` registers Arrow, Avro, binary, CSV, JSON,
Parquet, text, socket, rate, console and noop sources, plus `DeltaLakeSource` and
`IcebergLakeSource`. It then discovers Python sources and registers them through
`PythonDataSourceAdapter::register_all`. The accompanying test checks that ordinary
sources are not accepted as lake sources and that Delta and Iceberg are.

## Options, Locations and Time Travel

`OptionLayer` preserves where a setting came from: table properties, operation
options, table location, or a timestamp/integer/string time-travel selector.
Later option layers override earlier ones when consumed. Keeping these layers
separate lets the source interpret a path differently from an ordinary option.

Converting a layer to opaque key/value options does not carry every semantic
field: the implementation turns property and option lists into maps, while
location and time-travel variants return empty maps. A new source must consume
those typed variants deliberately rather than assuming map conversion preserves
the entire read request.

`SourceInfo` and `SinkInfo` are planning inputs. Their source-specific interpretation
belongs with the registered source. A catalog table contributes metadata and
location; it does not remove the source's responsibility to validate its own
read or write semantics.

## Listing Writes

The listing implementation creates `FileWriteNode` in
`crates/sail-data-source/src/listing/write.rs`. Its current options are:

```rust
pub struct FileWriteOptions {
    pub format: Arc<dyn WriteFormat>,
    pub url: Url,
    pub overwrite: bool,
    pub partition_by: Vec<CatalogPartitionField>,
    pub sort_by: Vec<Sort>,
}
```

This excerpt omits derive attributes, not fields. The node has one logical input
and an empty result schema. Its `with_exprs_and_inputs` requires zero replacement
expressions and exactly one replacement input, preserving its write options.

`listing/planner.rs` recognizes this node and checks both logical and physical
input cardinality. This path belongs to listing data sources. It is not a generic
claim that every Delta or Iceberg write first becomes this listing node.

```text
resolved write request
  -> DataSource.create_writer -> source-specific LogicalPlan
  -> physical planner -> writer and any required commit work
  -> Sail job planning and execution
```

Generating a logical write plan is not a commit. Execution and format-specific
transaction rules still determine when data and metadata become durable.

## LakeSource: Metadata and Row-Level Semantics

`LakeSource: DataSource` adds metadata inference, table metadata creation,
row-level planning and alteration operations. Its default row-level planner
returns explicit not-implemented errors for DELETE, UPDATE and MERGE. Supporting
a scan is not evidence that a source supports those mutations.

Table metadata creation is separate from registering the catalog object. The
trait permits a source to create required storage metadata before catalog
registration; its default implementation does no work. This is one reason that
catalog and storage behavior must remain distinct in the planner and tests.

Generic row-level planning uses `RowLevelWriteNode` in
`crates/sail-logical-plan/src/row_level.rs`. Source-specific implementations then
choose the appropriate physical writers and commit behavior. The shared
`RowLevelWriteMode` distinguishes copy-on-write from merge-on-read. When rows
must be materialized, the first rewrites affected data files; the second writes
row-level delete artifacts alongside new data. Metadata-only optimizations may
bypass materialization in either mode.

Internal columns are not user data. `__sail_file_path` identifies source files;
`__sail_file_row_index` identifies file-local rows for deletion vectors;
`__sail_operation_type` carries write intent. Source writers must remove the
operation column before persisting user rows. Its integer tags are internal
physical-plan conventions, not external storage protocol values.

## Delta and Iceberg Have Their Own Write Paths

Read `DeltaLakeSource` and `IcebergLakeSource` before following their lower-level
writers. Delta keeps scan construction under `datasource/`, logical operations
under `logical/`, and write/delete planning under `physical_plan/planner/`.
Its transaction and snapshot modules own different responsibilities from a
DataFusion batch writer.

Iceberg likewise separates its lake-source entry point, data source planning,
logical nodes, physical writers and commit modules. Position/equality delete
writers, write locations and manifest-related logic are not interchangeable with
a plain Parquet sink. A reader following only the file writer will miss the
metadata operation that makes those files part of a table.

For distributed changes, test both where batches are written and where the
transaction commits. Driver placement for a metadata operation does not imply
that every input scan or data writer runs on the driver. Conversely, successful
worker output does not prove that a table commit succeeded.

## Python Data Sources

`PythonDataSourceAdapter` implements `DataSource`. Its module contains a
`PythonWriteNode`; the package separates table-provider, execution, write and
commit code. Discovery is part of session source registration. This is an
existing extension mechanism, distinct from the experimental `pysail.extensions`
native-package bootstrap in Chapter 13.

The distinction prevents two common mistakes: expecting the native relation
manifest to register an arbitrary Python data source, and assuming a Python
source's discovery hook supplies native worker codec compatibility. Each path
has its own deployment and execution contract.

## Storage in the Graph Extensions

Pecan owns input snapshots, intermediate materialization and final output paths.
These are ordinary query storage operations. Native Banda staging additionally
owns native graph state. Argentea's worker adjacency lives within a bounded job;
it is not a durable lakehouse table or a recovery log.

Shared storage can make inputs and results accessible to both hosts without
serializing them through the client. It does not establish stable native owner
placement, replay safety, budget admission, or the lifetime of exported Arrow
buffers. Those are separate contracts in the extension and execution layers.
A storage-sharing feature should therefore be assessed as storage infrastructure,
not taken as proof that a stateful graph algorithm is distributed correctly.

## Contributor Checks

For a new source, verify ordinary reads, schema inference, option precedence,
logical writes and explicit unsupported operations. If it exposes `LakeSource`,
add metadata and row-level cases appropriate to that capability. For distributed
writes, include codec reconstruction, worker output, driver-side metadata work,
failed attempts and cleanup. Use an independent read or metadata inspection to
confirm the final table state.

For graph staging, test owned-path cleanup after success, cancellation and failed
writes. Retain the failure receipt and distinguish deferred session cleanup from
immediate cleanup. A later empty directory is useful evidence, but cannot by
itself prove that a native memory lease was released.

Follow a concrete operation through the current source: registry lookup,
`create_source` or `create_writer`, the returned logical node, physical planning,
job placement, execution and final metadata state. This trace makes the boundary
between reusable Sail infrastructure and source-owned behavior reviewable.
