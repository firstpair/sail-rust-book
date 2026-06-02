# Chapter 3: Apache Arrow — Sail's Data Backbone

## What Is Apache Arrow?

Apache Arrow is a specification for a columnar, language-agnostic, zero-copy in-memory data format, together with a growing set of implementations in C++, Rust, Python, Java, Go, and others. The core idea is simple but powerful: data is stored column-by-column rather than row-by-row, and the in-memory layout is standardized so that two processes that both use Arrow can share data by passing a pointer — no serialization, no copying.

For a query engine, this matters a great deal. Most analytical operations — aggregations, filters, projections — access all the values in a column before moving to the next column. Columnar layout means those values are contiguous in memory: the CPU prefetcher works, SIMD instructions apply cleanly, and zero-copy batch hand-offs between operators are possible.

Sail uses the [`arrow`](https://crates.io/crates/arrow) Rust crate (part of the Arrow project's native Rust implementation, also used by DataFusion). The two foundational types are `Schema` and `RecordBatch`.

## `Schema` and `Field`

An Arrow `Schema` describes the column layout of a batch of data: it is a list of `Field` values, each carrying a name, a `DataType`, and a nullable flag, plus optional key-value metadata.

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

let schema = Schema::new(vec![
    Field::new("id",     DataType::Int64,  false),
    Field::new("amount", DataType::Float64, true),
    Field::new("name",   DataType::Utf8,   true),
]);
let schema_ref: Arc<Schema> = Arc::new(schema);
```

`Arc<Schema>` (aliased as `SchemaRef`) is ubiquitous in Sail and DataFusion. Schemas are immutable and shared — multiple `RecordBatch`es from the same scan share one `Arc<Schema>`.

Arrow's `DataType` is a rich enum:

```rust
pub enum DataType {
    Null, Boolean,
    Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
    Float16, Float32, Float64,
    Timestamp(TimeUnit, Option<Arc<str>>),  // timezone
    Date32, Date64,
    Time32(TimeUnit), Time64(TimeUnit),
    Duration(TimeUnit),
    Interval(IntervalUnit),
    Binary, LargeBinary, FixedSizeBinary(i32),
    Utf8, LargeUtf8,
    List(Arc<Field>), LargeList(Arc<Field>), FixedSizeList(Arc<Field>, i32),
    Struct(Fields),
    Map(Arc<Field>, bool),   // (entries field, keys_sorted)
    Decimal128(u8, i8), Decimal256(u8, i8),
    // ... extension types, dictionaries, unions
}
```

The nested types (`List`, `Struct`, `Map`) contain child `Field` definitions, allowing arbitrary nesting — Spark's nested DataFrames map directly.

## `RecordBatch`

A `RecordBatch` is a table: a schema plus one `Arc<dyn Array>` per column, all with the same number of rows. The `dyn Array` abstraction is how Arrow handles type-heterogeneous columns: at runtime each column is a concrete typed array (e.g. `Int64Array`, `StringArray`, `StructArray`), all sharing the common `Array` interface.

```rust
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;

let ids    = Arc::new(Int64Array::from(vec![1, 2, 3]));
let names  = Arc::new(StringArray::from(vec!["alice", "bob", "carol"]));
let batch  = RecordBatch::try_new(Arc::new(schema), vec![ids, names]).unwrap();

println!("{} rows, {} columns", batch.num_rows(), batch.num_columns());
```

`RecordBatch` is the unit of work throughout Sail. Every `ExecutionPlan::execute` returns a `SendableRecordBatchStream`:

```rust
type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;
```

where `RecordBatchStream` is:

```rust
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}
```

Operators pull batches from their input streams, process them, and emit batches downstream. When a batch reaches the top of the physical plan tree, it is in the `Executor` in `sail-spark-connect`, waiting to be serialized to Arrow IPC.

## Spark Types → Arrow Types: The Mapping

Spark has its own type system. The Spark Connect protobuf defines types like `ByteType`, `ShortType`, `LongType`, `TimestampType`, `TimestampNtzType`, `DayTimeIntervalType`, and so on. Sail converts these through two stages:

1. **Proto → `spec::DataType`**: `sail-spark-connect`'s proto conversion layer maps protobuf `data_type::Kind` variants to `sail-common`'s `spec::DataType` enum.
2. **`spec::DataType` → `adt::DataType`**: `PlanResolver::resolve_data_type` in `crates/sail-plan/src/resolver/data_type.rs` maps the internal IR to Arrow's type system.

The second mapping is the interesting one because it has subtleties the proto-to-spec stage cannot resolve alone:

```rust
// crates/sail-plan/src/resolver/data_type.rs
pub(super) fn resolve_data_type(
    &self,
    data_type: &spec::DataType,
    state: &mut PlanResolverState,
) -> PlanResult<adt::DataType> {
    use spec::DataType;

    match data_type {
        DataType::Null    => Ok(adt::DataType::Null),
        DataType::Boolean => Ok(adt::DataType::Boolean),
        DataType::Int8    => Ok(adt::DataType::Int8),
        DataType::Int16   => Ok(adt::DataType::Int16),
        DataType::Int32   => Ok(adt::DataType::Int32),
        DataType::Int64   => Ok(adt::DataType::Int64),
        DataType::Float32 => Ok(adt::DataType::Float32),
        DataType::Float64 => Ok(adt::DataType::Float64),
        DataType::Timestamp { time_unit, timestamp_type } => Ok(adt::DataType::Timestamp(
            Self::resolve_time_unit(time_unit)?,
            self.resolve_timezone(timestamp_type)?,
        )),
        DataType::List { element_type, element_nullable, metadata } => {
            Ok(adt::DataType::List(Arc::new(
                self.resolve_field_with_metadata(
                    SAIL_LIST_FIELD_NAME,
                    element_type,
                    *element_nullable,
                    metadata.iter().cloned(),
                    state,
                )?,
            )))
        }
        DataType::Struct { fields } => {
            Ok(adt::DataType::Struct(self.resolve_fields(fields, state)?))
        }
        DataType::Decimal128 { precision, scale } => {
            Ok(adt::DataType::Decimal128(*precision, *scale))
        }
        DataType::Map { key_type, value_type, value_nullable, .. } => {
            // Map in Arrow is: List<entries: Struct<key: K, value: V>>
            Ok(adt::DataType::Map(Arc::new(/* ... */), false))
        }
        // ...
    }
}
```

**The timestamp subtlety.** Spark's `TimestampType` is "timestamp with local time zone" — values are stored in UTC but displayed in the session timezone. Spark's `TimestampNtzType` has no timezone. In Arrow, `Timestamp(Microseconds, Some("UTC"))` represents the former and `Timestamp(Microseconds, None)` the latter. This distinction is handled by `resolve_timezone`:

```rust
fn resolve_timezone(
    &self,
    timestamp_type: &spec::TimestampType,
) -> PlanResult<Option<Arc<str>>> {
    match timestamp_type {
        spec::TimestampType::Configured => match self.config.default_timestamp_type {
            DefaultTimestampType::TimestampLtz => Ok(Some(Arc::from("UTC"))),
            DefaultTimestampType::TimestampNtz => Ok(None),
        },
        spec::TimestampType::WithLocalTimeZone  => Ok(Some(Arc::from("UTC"))),
        spec::TimestampType::WithoutTimeZone    => Ok(None),
    }
}
```

The `default_timestamp_type` configuration key (`spark.sql.timestampType`) lets users switch the behavior globally — Sail tracks this per session via the `SparkRuntimeConfig`.

**The string/binary subtlety.** Arrow distinguishes between `Utf8` (32-bit offsets, ≤2 GiB per column) and `LargeUtf8` (64-bit offsets). Most tools produce `Utf8` by default. Sail has a configuration flag `arrow_use_large_var_types` that, when set, uses `LargeUtf8` and `LargeBinary` instead — useful for very wide string columns:

```rust
fn arrow_string_type(&self, state: &mut PlanResolverState) -> adt::DataType {
    if self.config.arrow_use_large_var_types
        && state.config().arrow_allow_large_var_types
    {
        adt::DataType::LargeUtf8
    } else {
        adt::DataType::Utf8
    }
}
```

## Schema → Spark Schema: The Reverse Direction

When Sail needs to send a schema back to PySpark (e.g. in response to `df.schema`), it converts an Arrow `SchemaRef` into a Spark Connect `DataType` (specifically a `DataType::Struct`). This happens in `crates/sail-spark-connect/src/schema.rs`:

```rust
// crates/sail-spark-connect/src/schema.rs
pub(crate) fn to_spark_schema(schema: SchemaRef) -> SparkResult<sc::DataType> {
    DataType::Struct(schema.fields().clone()).try_into()
}
```

The `TryInto` implementation performs the inverse mapping: Arrow `DataType` → protobuf `sc::DataType`. This conversion also needs to handle extension types (like GeoArrow geometry columns) and Arrow metadata (field-level key-value pairs that encode extra Spark semantics).

## Arrow IPC: Streaming Format

The on-wire format between Sail and PySpark is Arrow IPC, specifically the *streaming* format (as opposed to the *file* format which has a footer). The streaming format is a sequence of:

```
[schema message]
[record batch message]*
[end-of-stream marker]
```

Each message is a byte sequence: a 4-byte length prefix, a FlatBuffers-encoded header describing the buffer layout, then the raw buffer data. Sail writes this with `arrow::ipc::writer::StreamWriter`:

```rust
// crates/sail-spark-connect/src/executor.rs
pub(crate) fn to_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
    let mut output = ArrowBatch::default();
    {
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, batch.schema().as_ref())?;
        writer.write(batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}
```

Each `ArrowBatch` protobuf contains:
- `data: bytes` — the complete IPC stream (schema + one batch)
- `row_count: i64` — the number of rows, used by PySpark to allocate buffers

On the Python side, `pyarrow.ipc.open_stream(pa.py_buffer(data)).read_all()` reconstructs the `RecordBatch` from bytes. Because both sides use the same IPC format definition, there is no custom serialization layer — Arrow is the protocol.

## Arrow in the Execution Layer

Arrow's zero-copy architecture influences Sail's execution operators directly. The `RowRoundRobinPartitioner` in `crates/sail-physical-plan/src/repartition.rs` redistributes rows across output partitions without copying column buffers unnecessarily:

```rust
pub fn partition<F>(&mut self, batch: RecordBatch, mut f: F) -> Result<()>
where
    F: FnMut(usize, RecordBatch) -> Result<()>,
{
    let schema = batch.schema();
    let mut indices = vec![Vec::new(); self.num_partitions];
    for row_index in 0..batch.num_rows() {
        let partition = (self.next_idx + row_index) % self.num_partitions;
        indices[partition].push(row_index as u32);
    }
    self.next_idx = (self.next_idx + batch.num_rows()) % self.num_partitions;

    for (partition, partition_indices) in indices.into_iter().enumerate() {
        if partition_indices.is_empty() { continue; }
        let indices_array: PrimitiveArray<UInt32Type> = partition_indices.into();
        let columns = take_arrays(batch.columns(), &indices_array, None)?;
        let options = RecordBatchOptions::new()
            .with_row_count(Some(indices_array.len()));
        let partition_batch =
            RecordBatch::try_new_with_options(schema.clone(), columns, &options)?;
        f(partition, partition_batch)?;
    }
    Ok(())
}
```

`take_arrays` is Arrow's gather operation: given an array and an array of indices, it produces a new array by selecting the rows at those indices. This is a fundamental Arrow operation that the C++ and Rust implementations have highly optimized, including SIMD paths for fixed-width types.

## Summary

Arrow is not just a wire format for Sail — it is the data model at every level:

- **Planning**: schemas and field definitions drive type inference and validation.
- **Execution**: `RecordBatch` is the unit passed between operators; Arrow compute kernels do the actual work.
- **Transport**: Arrow IPC streams encode results for the Spark Connect wire.

The type mapping between Spark's type system and Arrow's type system is handled in `sail-plan/src/resolver/data_type.rs` with careful attention to Spark-specific subtleties — especially around timestamps, nullable semantics, and large variable-length types.
