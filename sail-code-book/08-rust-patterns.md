# Chapter 8: Rust Patterns Throughout Sail

## Overview

Sail is a large, production Rust codebase with strict Clippy settings. Reading through it, certain patterns appear repeatedly: a particular approach to error handling, a specific async model, a code generation strategy, and a way of bridging Rust and Python. This chapter collects those patterns in one place.

## 1. Error Handling with `thiserror`

Sail has one typed error enum per crate. Every crate defines its own `XxxError` and `XxxResult<T>` alias. The workspace-level Clippy configuration bans `unwrap_used` and `expect_used`:

```toml
# Cargo.toml (workspace)
[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
```

This forces every error to be handled explicitly. There are no panics in user-triggered paths.

### The `thiserror` Pattern

Each crate uses `thiserror` for its error enum:

```rust
// crates/sail-spark-connect/src/error.rs
#[derive(Debug, Error)]
pub enum SparkError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("error in Arrow: {0}")]
    ArrowError(#[from] ArrowError),
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("analysis error: {0}")]
    AnalysisError(String),
    // ...
}

pub type SparkResult<T> = Result<T, SparkError>;
```

The `#[from]` attribute generates `impl From<DataFusionError> for SparkError` automatically, so `?` works across error type boundaries.

### Layered Conversion

When a lower-level crate's error propagates up to a higher-level crate, there is an explicit `From` implementation that maps each variant. This makes the conversion semantics visible and prevents silent information loss:

```rust
impl From<PlanError> for SparkError {
    fn from(error: PlanError) -> Self {
        match error {
            PlanError::DataFusionError(e)    => SparkError::DataFusionError(e),
            PlanError::MissingArgument(msg)  => SparkError::MissingArgument(msg),
            PlanError::InvalidArgument(msg)  => SparkError::InvalidArgument(msg),
            PlanError::NotImplemented(msg)   => SparkError::NotImplemented(msg),
            PlanError::AnalysisError(msg)    => SparkError::AnalysisError(msg),
            PlanError::DeltaTableError(e)    => SparkError::InternalError(e.to_string()),
        }
    }
}
```

`DeltaTableError` does not have a corresponding `SparkError` variant, so it is stringified into `InternalError`. This is an explicit choice — it avoids leaking Delta-specific error types into the protocol layer.

### Constructor Methods

Each error type provides named constructors for the common variants:

```rust
impl SparkError {
    pub fn todo(message: impl Into<String>) -> Self { SparkError::NotImplemented(message.into()) }
    pub fn unsupported(message: impl Into<String>) -> Self { SparkError::NotSupported(message.into()) }
    pub fn invalid(message: impl Into<String>) -> Self { SparkError::InvalidArgument(message.into()) }
    pub fn internal(message: impl Into<String>) -> Self { SparkError::InternalError(message.into()) }
}
```

`SparkError::todo(...)` is the honest marker for features not yet implemented — a deliberate choice over `todo!()` which would panic. Sail's Clippy configuration bans `todo` macros, so `SparkError::todo(...)` is the escape hatch.

### gRPC Error Mapping

At the gRPC boundary, `SparkError` must become `tonic::Status`. The `From<SparkError> for Status` implementation maps error variants to HTTP/gRPC status codes:

```rust
impl From<SparkError> for Status {
    fn from(error: SparkError) -> Self {
        match error {
            SparkError::MissingArgument(msg)  => Status::invalid_argument(msg),
            SparkError::InvalidArgument(msg)  => Status::invalid_argument(msg),
            SparkError::NotImplemented(msg)   => Status::unimplemented(msg),
            SparkError::NotSupported(msg)     => Status::unimplemented(msg),
            SparkError::AnalysisError(msg)    => {
                // Spark uses a special error detail type for analysis errors
                let mut details = ErrorDetails::new();
                details.set_error_info(/* ... */);
                Status::with_error_details(Code::InvalidArgument, msg, details)
            }
            SparkError::InternalError(msg) => Status::internal(msg),
            // Python errors get special treatment to include traceback
            SparkError::DataFusionError(e) => {
                let python_cause = /* extract Python traceback if available */;
                // ...
            }
            // ...
        }
    }
}
```

Spark clients expect specific error shapes, including `ErrorInfo` gRPC status details for analysis errors. Sail populates these so PySpark's error messages are recognizable.

## 2. `async`/`await` and Tokio

Sail's entire server is async. The convention is:

- **Protocol layer** (gRPC handlers): `#[tonic::async_trait]` on trait implementations. This is a `proc_macro` that rewrites `async fn` in trait impls into boxed futures, working around Rust's current limitation on `async fn` in traits.
- **Planning**: `async fn` for catalog lookups and UDF resolution (which may require Python calls).
- **Execution**: Tokio tasks spawned via `tokio::spawn`, channels for coordination.

### `async_trait` for Catalog

All catalog operations are async because they may call remote APIs:

```rust
#[async_trait::async_trait]
pub trait CatalogProvider: Send + Sync {
    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus>;
    // ...
}
```

`#[async_trait]` (from the `async-trait` crate) desugars this to:

```rust
fn get_table<'life0, 'life1, 'life2, 'async_trait>(
    &'life0 self,
    database: &'life1 Namespace,
    table: &'life2 str,
) -> Pin<Box<dyn Future<Output = CatalogResult<TableStatus>> + Send + 'async_trait>>
```

The `+ Send` bound is critical — it ensures catalog implementations can be used across Tokio threads without data races.

### `tokio::select!` for Timeouts and Cancellation

The heartbeat mechanism in the executor uses `select!`:

```rust
tokio::select! {
    batch = self.stream.next() => Ok(batch.transpose()?),
    _ = tokio::time::sleep(self.heartbeat_interval) => {
        Ok(Some(RecordBatch::new_empty(self.stream.schema())))
    }
}
```

`select!` races two futures and resolves with the first to complete. If the stream produces a batch before the timer fires, the batch wins. If the timer fires first, an empty batch is emitted to keep the connection alive. The timer branch returns `Ok(Some(empty_batch))` — the empty batch is a valid signal that keeps the gRPC response stream from timing out.

### Tokio Channels for Actor Communication

All actor communication uses `tokio::sync::mpsc` (bounded, async):

```rust
const ACTOR_CHANNEL_SIZE: usize = 8;

pub fn spawn<T: Actor>(&mut self, options: T::Options) -> ActorHandle<T> {
    let (tx, rx) = mpsc::channel(ACTOR_CHANNEL_SIZE);
    let handle = ActorHandle { sender: tx };
    // ...
}
```

The small buffer size (8) is intentional: it provides backpressure. If the actor is processing messages slowly, the sender's `await` on `tx.send(...)` will block, propagating backpressure up to the caller. This prevents unbounded memory growth in message queues.

`oneshot` channels are used for request/response patterns:

```rust
let (result_tx, result_rx) = oneshot::channel();
self.driver.send(DriverEvent::ExecuteJob {
    plan,
    context: ctx.task_ctx(),
    result: result_tx,
}).await?;
let stream = result_rx.await?;
```

This is the async equivalent of a synchronous return value across actor boundaries.

## 3. The `UserDefinedLogicalNodeCore` Pattern

When adding a new Spark-specific logical plan node to DataFusion, the pattern is:

1. Define a struct with the node's fields.
2. Derive `Clone, Debug, PartialEq, Eq, Hash` — all required by `UserDefinedLogicalNodeCore`.
3. Use `#[derive(Educe)]` from the `educe` crate to suppress derived traits on fields that cannot implement them (e.g. `DFSchemaRef` does not implement `PartialOrd`).
4. Implement `UserDefinedLogicalNodeCore`.

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct RangeNode {
    range: Range,
    num_partitions: usize,
    #[educe(PartialOrd(ignore))]  // DFSchemaRef cannot be ordered
    schema: DFSchemaRef,
}
```

`educe` is a proc-macro crate that allows fine-grained control over derived traits — here, it derives `PartialOrd` for the whole struct while ignoring the `schema` field. Without this, the `#[derive(PartialOrd)]` would fail to compile because `DFSchemaRef` does not implement `PartialOrd`.

## 4. Code Generation: Protobuf and Thrift

Sail uses two code generation systems:

### Tonic/prost for Protobuf (Spark Connect)

`crates/sail-spark-connect/src/lib.rs`:
```rust
pub mod connect {
    tonic::include_proto!("spark.connect");
    tonic::include_proto!("spark.connect.serde");
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("spark_connect_descriptor");
}
```

`tonic::include_proto!` expands to `include!(concat!(env!("OUT_DIR"), "/spark_connect.rs"))`. The actual generation is in `build.rs` using `tonic_build`. The build script compiles the `.proto` files during `cargo build`, not at runtime.

### volo-build for Thrift (Hive Metastore)

`crates/sail-catalog-hms/build.rs`:
```rust
volo_build::Builder::thrift()
    .add_service("thrift/hive_metastore.thrift")
    .split_generated_files(true)
    .write()?;
```

`crates/sail-catalog-hms/src/lib.rs`:
```rust
pub mod hms {
    mod internal {
        include!(concat!(env!("OUT_DIR"), "/volo_gen.rs"));
    }
    pub use internal::volo_gen::hive_metastore::*;
}
```

Both use the `include!(concat!(env!("OUT_DIR"), "..."))` idiom to bring generated code into the module tree. The `cargo:rerun-if-changed=` directives in `build.rs` ensure incremental rebuilds only regenerate the code when the schema files change.

## 5. PyO3: The Python Bridge

`sail-python` uses PyO3 to expose Rust types to Python. The key types are `SparkConnectServer` (the embedded server) and `SailFlightSqlServer`:

```rust
// crates/sail-python/src/spark/server.rs
#[pyclass]
pub(super) struct SparkConnectServer {
    #[pyo3(get)]
    ip: String,
    #[pyo3(get)]
    port: u16,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    state: Option<SparkConnectServerState>,
}

#[pymethods]
impl SparkConnectServer {
    #[new]
    #[pyo3(signature = (ip, port, /))]
    fn new(py: Python<'_>, ip: &str, port: u16) -> PyResult<Self> { /* ... */ }

    fn start(&mut self, py: Python<'_>, background: bool) -> PyResult<()> {
        // ...
        if !background {
            let state = self.state()?;
            py.detach(move || state.wait(false))?;
        }
        Ok(())
    }
}
```

The critical line is `py.detach(move || ...)`. When `background=False`, `start` blocks until the server stops. But it must release the Python GIL while blocking, otherwise Python UDFs (which need the GIL to call back into Python) would deadlock. `py.detach` releases the GIL for the duration of the closure, allowing other Python threads to run.

### Python UDF Execution

`sail-python-udf` handles Python UDFs. PySpark serializes UDFs using cloudpickle; the serialized bytes are sent over Spark Connect as a `payload` field in the `RegisterFunction` message. Sail deserializes them on the Rust side using PyO3:

```rust
pub struct PySparkUDF {
    kind: PySparkUdfKind,   // Batch, ArrowBatch, ScalarPandas, ScalarArrow, ...
    payload: Vec<u8>,       // cloudpickle bytes
    input_types: Vec<DataType>,
    output_type: DataType,
    udf: LazyPyObject,      // lazily deserialized Python callable
}
```

`LazyPyObject` holds an `Arc<OnceLock<Py<PyAny>>>`. On the first invocation, it acquires the GIL (`Python::attach`), deserializes the cloudpickle bytes, and stores the resulting `PyAny` in the `OnceLock`. Subsequent invocations reuse the cached object. This amortizes the cloudpickle deserialization cost across batch invocations.

`PySparkUDF` implements DataFusion's `ScalarUDFImpl`:

```rust
impl ScalarUDFImpl for PySparkUDF {
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Convert Arrow arrays to Python objects
        // Call the Python callable
        // Convert results back to Arrow arrays
    }
}
```

The Arrow-to-Python conversion uses `pyo3::types::PyList` and `pyo3::types::PyDict` for Python UDFs, and `pyarrow.RecordBatch` (via `pyo3-arrow`) for Arrow-native UDFs. The return value is an Arrow array, re-entered into the DataFusion compute pipeline.

### The GIL and the Tokio Runtime

Python's GIL is a global lock. DataFusion runs on Tokio, which uses a multi-threaded executor. A Python UDF that holds the GIL blocks all other Tokio tasks on that thread from running. Sail handles this by:

1. Running Python UDF evaluation on a dedicated thread pool separate from the main Tokio executor (via `tokio::task::spawn_blocking`).
2. Releasing the GIL with `py.detach` whenever Rust code blocks on I/O.

This prevents Python UDFs from starving network I/O or other query operations.

## 6. The `SessionExtension` Pattern

The same problem — attaching typed state to DataFusion's `SessionContext` — appears in three places: `SparkSession`, `PlanService`, and `JobService`. Sail solves this with the `SessionExtension` marker trait and a typed accessor:

```rust
pub trait SessionExtension: Any + Send + Sync {
    fn name() -> &'static str;
}

pub trait SessionExtensionAccessor {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>>;
}

impl SessionExtensionAccessor for SessionContext {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.state()
            .config()
            .get_extension::<T>()
            .ok_or_else(|| DataFusionError::Internal(
                format!("{} extension not found", T::name())
            ))
    }
}
```

This is a simple type-indexed map. The `T::name()` method provides the human-readable name for error messages. Usages look like:

```rust
let spark = ctx.extension::<SparkSession>()?;
let job_service = ctx.extension::<JobService>()?;
```

The pattern avoids a singleton or global state; each `SessionContext` carries its own extensions, making sessions truly isolated.

## 7. The `ScalarFunctionBuilder` DSL

The function registry in `sail-plan/src/function/` takes a distinctive approach: functions are not structs implementing a trait — they are closures. The type alias is:

```rust
// crates/sail-plan/src/function/common.rs
pub(crate) type ScalarFunction =
    Arc<dyn Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;
```

A `ScalarFunction` takes arguments (a `Vec<expr::Expr>`) plus context, and returns a DataFusion `Expr`. This means most Spark functions are expressed as *logical expression trees*, not as physical UDFs. When Sail resolves `abs(x)`, it does not create a new `ScalarUDF` call node — it emits a DataFusion `abs(x)` expression, which the optimizer can reason about, fold constants in, and push down into scans.

`ScalarFunctionBuilder` provides the ergonomic factory methods:

```rust
// crates/sail-plan/src/function/common.rs
pub struct ScalarFunctionBuilder;

impl ScalarFunctionBuilder {
    pub fn nullary<F, R>(f: F) -> ScalarFunction      // zero args, e.g. pi()
    pub fn unary<F, R>(f: F) -> ScalarFunction        // one arg
    pub fn binary<F, R>(f: F) -> ScalarFunction       // two args
    pub fn ternary<F, R>(f: F) -> ScalarFunction      // three args
    pub fn quaternary<F, R>(f: F) -> ScalarFunction   // four args
    pub fn var_arg<F, R>(f: F) -> ScalarFunction      // variadic
    pub fn binary_op(op: Operator) -> ScalarFunction  // wraps a BinaryExpr operator
    pub fn cast(data_type: DataType) -> ScalarFunction // wraps a cast expression
    pub fn udf<F: ScalarUDFImpl>(f: F) -> ScalarFunction  // wraps a real ScalarUDFImpl
    pub fn custom<F>(f: F) -> ScalarFunction          // full control: takes ScalarFunctionInput
}
```

All argument-counted variants use the `ItemTaker` utility trait (`.zero()`, `.one()`, `.two()`, `.three()`, `.four()`) which returns typed errors if the argument count doesn't match. This produces consistent error messages like "expected 1 argument, got 3" without boilerplate per function.

The registration table is a `lazy_static!` `HashMap<&'static str, ScalarFunction>`. Here is the math function table, showing the three registration styles:

```rust
// crates/sail-plan/src/function/scalar/math.rs
pub(super) fn list_built_in_math_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;
    vec![
        // Expression-level (zero allocation at invocation time, optimizer-transparent):
        ("acos",    F::unary(double(expr_fn::acos))),
        ("atan2",   F::binary(double2(expr_fn::atan2))),
        ("pi",      F::nullary(expr_fn::pi)),
        ("e",       F::nullary(eulers_constant)),
        ("ceil",    F::custom(|arg| ceil_floor(arg, "ceil"))),
        ("floor",   F::custom(|arg| ceil_floor(arg, "floor"))),
        ("negative", F::unary(|x| Expr::Negative(Box::new(x)))),

        // Binary operator shorthands:
        ("%",  F::custom(spark_modulo)),
        ("+",  F::custom(spark_plus)),
        ("-",  F::custom(spark_minus)),
        ("*",  F::custom(spark_multiply)),
        ("/",  F::custom(spark_divide)),

        // Real ScalarUDF (needed when Spark semantics differ from DataFusion):
        ("bin",      F::udf(SparkBin::new())),
        ("bround",   F::udf(SparkBRound::new())),
        ("try_add",  F::udf(SparkTryAdd::new())),
        ("try_div",  F::udf(SparkTryDiv::new())),
        ("rand",     F::udf(Random::new())),
        // ...
    ]
}
```

The three strategies cover different tradeoffs:
- `F::unary/binary/...` with DataFusion built-in functions — zero overhead, optimizer-transparent, no physical UDF
- `F::custom(closure)` — handles complex argument mapping or conditional expression construction
- `F::udf(impl ScalarUDFImpl)` — when Spark's semantics differ from DataFusion's (null handling, overflow behavior, Spark-specific output format)

The final registry is assembled by collecting all category lists:

```rust
// crates/sail-plan/src/function/scalar/mod.rs
pub(super) fn list_built_in_scalar_functions() -> Vec<(&'static str, ScalarFunction)> {
    let mut output = Vec::new();
    output.extend(array::list_built_in_array_functions());
    output.extend(bitwise::list_built_in_bitwise_functions());
    output.extend(datetime::list_built_in_datetime_functions());
    output.extend(geo::list_built_in_geo_functions());
    output.extend(hash::list_built_in_hash_functions());
    output.extend(math::list_built_in_math_functions());
    output.extend(string::list_built_in_string_functions());
    output.extend(variant::list_built_in_variant_functions());
    // ... 22 categories total, ~420 entries
    output
}
```

The map is initialized once at startup via `lazy_static!`:

```rust
// crates/sail-plan/src/function/mod.rs
lazy_static! {
    pub static ref BUILT_IN_SCALAR_FUNCTIONS: HashMap<&'static str, ScalarFunction> =
        HashMap::from_iter(scalar::list_built_in_scalar_functions());
}
```

The `lazy_static!` means the first query pays the initialization cost; all subsequent queries find an already-populated `HashMap<&'static str, ...>` with O(1) lookup.

**HLL and theta sketch aggregates** (added in #1971) follow the same `AggFunctionBuilder::custom(...)` pattern. The accumulator state is serialized as raw bytes:

```rust
// crates/sail-function/src/aggregate/hll_sketch.rs
impl Accumulator for HllSketchAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        update_hll_sketch_from_array(&mut self.sketch, values[0].as_ref(), /* ... */)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Merge partial sketch bytes from other partitions (for distributed execution)
        union_hll_sketches(states[0].as_ref(), /* ... */)?;
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.serialize()))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(estimate_hll_sketch(&self.sketch.serialize(), "hll_sketch_agg")?)))
    }
}
```

The sketch is serialized to/from `Binary` `ScalarValue`, which DataFusion uses to shuffle partial aggregation state between partitions in distributed mode.

## 8. The Gold Test Infrastructure

`sail-gold-test` is a systematic Spark compatibility verifier, not just a test runner. The workflow:

1. **Data generation**: Run the `spark-gold-data` binary against a real Spark cluster. It reads Spark's function documentation (generated from `SparkSQLFunctionDocSuite`) — each function's examples — and saves expected SQL/output pairs as JSON files.

2. **Test replay**: The test suite replays each SQL example against Sail and diffs the output against the saved golden data.

The test infrastructure handles schema matching, result ordering, and type coercion. Functions are organized into groups matching Spark's own documentation structure (`array_funcs`, `string_funcs`, `date_funcs`, etc.).

This is how Sail makes concrete claims about Spark compatibility: not "we think these functions work" but "we have replayed Spark's own documentation examples and the output matches".

## 9. The `spec` IR: A Clean Internal Boundary

One non-obvious pattern is the existence of `spec::Plan`, `spec::Relation`, `spec::Expr` etc. in `sail-common`. These are Rust enums that mirror the Spark Connect protobuf but are cleanly typed — no `Option<Box<dyn Any>>`, no `oneof` boilerplate.

The motivation is separation of concerns: `sail-spark-connect` knows about protobuf; `sail-plan` knows about DataFusion. Neither should know about the other. The `spec` IR is the language they both speak. `sail-spark-connect` converts protobuf → spec; `sail-plan` converts spec → `LogicalPlan`. The conversion in each direction is contained.

This also makes `sail-flight` natural: it uses `sail-sql-analyzer` to convert SQL text → spec, then hands the same `spec::Plan` to `sail-plan`. Both entry points converge on the same planning code without any shared knowledge of how the spec was produced.

## Summary

Sail's Rust patterns are consistent and intentional:

| Pattern | Where | Why |
|---|---|---|
| `thiserror` error enums + layered `From` | Every crate | Explicit error propagation, no silent conversions |
| `SparkError::todo()` instead of `todo!()` | `sail-spark-connect` | Unimplemented paths return gRPC `UNIMPLEMENTED`, not panics |
| `#[async_trait]` | Catalog, gRPC traits | Async fn in traits until native support stabilizes |
| `tokio::select!` | Executor heartbeat | Timeout + cancellation without blocking |
| `OnceCell<Client>` | Glue, HMS, Unity | Lazy initialization of remote clients |
| `LazyPyObject` / `OnceLock` | Python UDFs | Amortize cloudpickle deserialization |
| `py.detach()` | Python server + UDFs | Release GIL to avoid deadlocks with async Tokio |
| `include!(concat!(env!("OUT_DIR"), ...))` | Protobuf, Thrift | Build-time codegen, zero runtime cost |
| `SessionExtension` type-indexed map | Session state | Typed access to per-session context without globals |
| `ScalarFunctionBuilder` DSL | `sail-plan` function registry | 420+ functions as closures, optimizer-transparent, no per-function struct boilerplate |
| `lazy_static! HashMap` for functions | `sail-plan` | Amortize function registry init; O(1) lookup per query |
| `AggregateUDFImpl` + binary state | HLL/theta sketch aggregates | Serialize sketch state as `Binary` for distributed partial aggregation |
| `TreeParser` / `TreeText` proc-macros | `sail-sql-parser` | Derive parsers and unparsers from annotated AST structs |
| Perfect-hash keyword map (`phf`) | `sail-sql-parser` lexer | O(1) keyword lookup from `build.rs`-generated table of 368 keywords |
| `spec::Plan` internal IR | sail-common | Clean boundary between protocol and planning layers |
