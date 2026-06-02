# Chapter 6: The Execution Layer

## The JobRunner Abstraction

After `resolve_and_execute_plan` produces an `Arc<dyn ExecutionPlan>`, that tree must actually run. Sail abstracts execution behind a `JobRunner` trait in `sail-common-datafusion`:

```rust
#[tonic::async_trait]
pub trait JobRunner: Send + Sync {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream>;

    async fn stop(&self, history: oneshot::Sender<JobRunnerHistory>);
}
```

There are two implementations:

| Implementation | When used |
|---|---|
| `LocalJobRunner` | Single-process mode (development, testing, small data) |
| `ClusterJobRunner` | Distributed mode (driver/worker cluster) |

The session chooses which backend to use based on configuration. Both implement the same trait, so all planning and protocol code is identical — the execution backend is swapped transparently.

## `LocalJobRunner`: Execution in One Process

`LocalJobRunner` is the simplest path. It just calls DataFusion's `execute_stream`:

```rust
// crates/sail-execution/src/job_runner.rs
#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        if self.stopped.load(Ordering::Relaxed) {
            return internal_err!("job runner is stopped");
        }
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let options = TracingExecOptions {
            metrics: global_metrics(),
            job_id: Some(job_id),
            stage: None,
            attempt: None,
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        Ok(execute_stream(plan, ctx.task_ctx())?)
    }
}
```

`trace_execution_plan` wraps the plan tree with OpenTelemetry tracing spans so execution metrics flow to the configured exporter. Then `execute_stream` (from `datafusion`) produces a `SendableRecordBatchStream` that drives execution lazily as the consumer polls the stream.

In local mode, all partitions of all physical plan operators run on the Tokio thread pool of the current process. DataFusion's `execute_stream` handles partition fan-out and merge internally.

## `ClusterJobRunner`: Distributed Execution

`ClusterJobRunner` is the path for multi-node execution. It wraps a `DriverActor` handle:

```rust
pub struct ClusterJobRunner {
    driver: ActorHandle<DriverActor>,
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverEvent::ExecuteJob {
                plan,
                context: ctx.task_ctx(),
                result: tx,
            })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        rx.await
            .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
    }
}
```

`execute` sends a `DriverEvent::ExecuteJob` message to the driver actor and awaits the `oneshot::Receiver` for the resulting stream. The driver does not block the caller — it processes the event asynchronously and sends the stream back through the oneshot channel. From the caller's perspective, `execute` returns as soon as the driver has created the stream; actual execution happens as the stream is polled.

## The Actor Model

Sail's execution layer is built on an actor model implemented in `crates/sail-server/src/actor.rs`. Actors are Tokio tasks that own mutable state and receive messages sequentially through an mpsc channel. No mutexes; no shared mutable state between actors.

### The `Actor` Trait

```rust
#[tonic::async_trait]
pub trait Actor: Sized + Send + 'static {
    type Message: Send + SpanAssociation + 'static;
    type Options;

    fn name() -> &'static str;
    fn new(options: Self::Options) -> Self;
    async fn start(&mut self, ctx: &mut ActorContext<Self>) {}
    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction;
    async fn stop(self, ctx: &mut ActorContext<Self>) {}
}
```

The `receive` method is *synchronous* — it must not block. If the actor needs to perform async work (e.g. send an RPC to a worker), it uses `ctx.spawn(...)` to launch a separate task that sends the result back as another message.

The `ActorRunner` drives the actor:

```rust
impl<T: Actor> ActorRunner<T> {
    async fn run(mut self) {
        self.actor.start(&mut self.ctx).await;
        while let Some(MessageEnvelop { message, context }) = self.receiver.recv().await {
            let action = self.actor.receive(&mut self.ctx, message);
            match action {
                ActorAction::Continue => {}
                ActorAction::Stop => break,
            }
            self.ctx.reap();  // join completed child tasks, log errors
        }
        self.receiver.close();
        self.actor.stop(&mut self.ctx).await;
    }
}
```

The event loop processes messages one at a time. All actor state is `&mut self` in `receive`, so there are no data races. Tracing spans are carried through the `MessageEnvelop` struct:

```rust
struct MessageEnvelop<M> {
    message: M,
    context: Option<SpanContext>,
}
```

When an actor sends a message via `ActorHandle::send`, it creates a span with `Span::enter_with_local_parent` and attaches the `SpanContext` to the envelope. When the receiving actor processes it, it creates a child span, connecting the trace across actor boundaries.

### `DriverActor`

The `DriverActor` is the central coordinator for distributed execution. Its state includes:

```rust
pub struct DriverActor {
    options: DriverOptions,
    server: ServerMonitor,         // the driver's gRPC server for workers to connect to
    worker_pool: WorkerPool,       // registered workers and their health state
    job_scheduler: JobScheduler,   // pending and active jobs
    task_assigner: TaskAssigner,   // assigns tasks to workers
    task_runner: TaskRunner,       // executes driver-side tasks directly
    stream_manager: StreamManager, // manages inter-stage data streams
    task_sequences: HashMap<TaskKey, u64>,
    history: Option<oneshot::Sender<JobRunnerHistory>>,
}
```

The driver handles the events defined in `DriverEvent`:

```rust
pub enum DriverEvent {
    ServerReady { port: u16, signal: oneshot::Sender<()> },
    RegisterWorker { worker_id: WorkerId, host: String, port: u16, result: oneshot::Sender<ExecutionResult<()>> },
    WorkerHeartbeat { worker_id: WorkerId },
    ExecuteJob { plan: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>, result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>> },
    UpdateTask { key: TaskKey, status: TaskStatus, message: Option<String>, cause: Option<CommonErrorCause>, sequence: Option<u64> },
    CreateLocalStream { key: TaskStreamKey, storage: LocalStreamStorage, schema: SchemaRef, result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>> },
    FetchWorkerStream { worker_id: WorkerId, key: TaskStreamKey, schema: SchemaRef, result: oneshot::Sender<ExecutionResult<TaskStreamSource>> },
    Shutdown { history: Option<oneshot::Sender<JobRunnerHistory>> },
    // ... more
}
```

## The Job Graph

When `ExecuteJob` arrives, the driver builds a `JobGraph` by analyzing the `ExecutionPlan` tree. The job graph partitions the plan into *stages*, where stage boundaries are exchange operators (hash-repartition, broadcast, sort-merge join shuffles):

```rust
/// A job graph represents a distributed execution plan for a job.
/// A job consists of multiple *stages*, where each stage has one or more
/// *partitions*. There are *tasks* which each corresponds to the execution of a single partition
/// of a stage and can have multiple *attempts*.
/// Each task produces output split into multiple *channels*.
pub struct JobGraph {
    stages: Vec<Stage>,  // topologically sorted
    schema: SchemaRef,
}

pub struct Stage {
    pub inputs: Vec<StageInput>,
    pub plan: Arc<dyn ExecutionPlan>,
    pub group: String,    // slot sharing group for co-location
    pub mode: OutputMode,
    pub distribution: OutputDistribution,
    pub placement: TaskPlacement,  // Driver or Worker
}
```

Stages are topologically sorted: all inputs of a stage appear before it in the list. `TaskPlacement::Driver` marks stages that must run on the driver node — typically catalog operations, `SHOW` commands, and other non-data-parallel operators.

### Input Modes

The relationship between stages is described by `InputMode`:

```rust
pub enum InputMode {
    /// partition p of the current stage reads from partition p of the input stage
    Forward,
    /// each partition of the current stage reads from all partitions of the input stage
    Merge,
    /// partition p reads from channel p of all partitions in the input stage (hash shuffle read)
    Shuffle,
    /// a single partition reads from all partitions of the input stage (broadcast)
    Broadcast,
    /// each partition reads from a contiguous subset of input partitions (coalesce)
    Rescale,
}
```

And output distribution:

```rust
pub enum OutputDistribution {
    Hash { keys: Vec<Arc<dyn PhysicalExpr>>, channels: usize },
    RoundRobin { channels: usize },
    RoundRobinRow { channels: usize },  // row-level, for explicit df.repartition()
}
```

The planner assigns `InputMode::Shuffle` to the output of a hash-repartition stage and `InputMode::Forward` to the output of a map (projection, filter) stage. This determines how inter-stage data is routed.

## Task Identification

Tasks are identified by a composite key:

```rust
pub struct TaskKey {
    pub job_id: JobId,
    pub stage: usize,
    pub partition: usize,
    pub attempt: usize,
}
```

Inter-stage data streams add a channel dimension:

```rust
pub struct TaskStreamKey {
    pub job_id: JobId,
    pub stage: usize,
    pub partition: usize,
    pub attempt: usize,
    pub channel: usize,  // for hash-distributed outputs
}
```

`TaskDefinition` is the serializable description of what a task should execute — the physical plan encoded as bytes (via `datafusion_proto`), plus input and output routing:

```rust
pub struct TaskDefinition {
    pub plan: Arc<[u8]>,       // protobuf-encoded ExecutionPlan subtree
    pub inputs: Vec<TaskInput>,
    pub output: TaskOutput,
}

pub struct TaskOutput {
    pub distribution: TaskOutputDistribution,  // Hash, RoundRobin, or RoundRobinRow
    pub locator: TaskOutputLocator,            // Local or Remote { uri }
}
```

Tasks are serialized and sent to workers over gRPC. The worker deserializes the `plan` bytes back into an `ExecutionPlan` using DataFusion's protobuf codec.

## Task Scheduling and Regions

The scheduler groups tasks into `TaskRegion`s — collections of tasks that must be scheduled and rescheduled together:

```rust
pub struct TaskRegion {
    pub tasks: Vec<(TaskPlacement, TaskSet)>,
}

pub struct TaskSet {
    pub entries: Vec<TaskSetEntry>,
}

pub struct TaskSetEntry {
    pub key: TaskKey,
    pub output: TaskOutputKind,  // Local or Remote
}
```

A region typically corresponds to a pipeline of pipelined stages: stages whose outputs are pipelined (not blocking) can run concurrently on the same worker because they produce and consume data incrementally. This reduces intermediate materialization.

When a task fails, the entire region is rescheduled, because a failure in one pipelined stage may have caused incomplete output in co-running stages.

## Data Exchange Between Stages

Inter-stage data exchange uses three transport modes:

1. **Local streams**: data stays in the same process (driver mode or same-worker optimization). Stored in a `LocalStreamStorage` — either in memory (`Memory`) or on disk (`Disk`).
2. **Worker streams**: data is fetched from another worker via gRPC. The driver mediates the connection: a downstream worker asks the driver for the address of the upstream worker, then fetches directly.
3. **Remote streams**: data is stored at a URI (S3, GCS, Azure Blob) and fetched by URL. Used for data that must survive worker failure.

The stream manager tracks which streams exist and delivers them to tasks that request them.

## Streaming Execution

Structured streaming is a separate execution path that runs alongside batch query execution. The entry point is the same `JobRunner`, but the logical plan is transformed before physical planning.

### The Streaming Plan Rewriter

When `resolve_and_execute_plan` detects a streaming plan (`is_streaming_plan(&plan)?` returns true), it calls `rewrite_streaming_plan` before physical planning. This rewriter, in `crates/sail-plan/src/streaming/rewriter.rs`, converts the batch logical plan into a "flow event" plan:

```rust
// crates/sail-plan/src/streaming/rewriter.rs
impl StreamingRewriter {
    fn f_up_extension(&mut self, extension: Extension) -> Result<Transformed<LogicalPlan>> {
        let node = extension.node.as_ref();
        if node.as_any().is::<RangeNode>() {
            // Wrap range source in a streaming adapter
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(StreamSourceAdapterNode::try_new(Arc::new(
                    LogicalPlan::Extension(extension),
                ))?),
            })))
        } else if let Some(show) = node.as_any().downcast_ref::<ShowStringNode>() {
            // Wrap show_string's input in a collector (gathers all batches before displaying)
            let input = LogicalPlan::Extension(Extension {
                node: Arc::new(StreamCollectorNode::try_new(Arc::clone(show.input()))?),
            });
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: show.with_exprs_and_inputs(vec![], vec![input])?,
            })))
        } else if node.as_any().is::<FileWriteNode>() {
            Ok(Transformed::no(LogicalPlan::Extension(extension)))  // write nodes pass through
        } else {
            plan_err!("unsupported extension node for streaming: {node:?}")
        }
    }
}
```

Table scans are rewritten to use `StreamSourceWrapperNode`, which wraps a `StreamSource` trait object. The `StreamSource` trait is implemented by each streaming source (Kafka, file-based micro-batch, etc.) and provides a `scan()` method that returns a `SendableRecordBatchStream` for each micro-batch.

### The Flow Event Schema

The fundamental design choice in Sail's streaming is the **flow event schema**. Every record in a streaming plan carries two extra fields prepended to the user's data columns:

```rust
// crates/sail-common-datafusion/src/streaming/event/schema.rs
pub const MARKER_FIELD_NAME: &str = "_marker";   // Binary, nullable
pub const RETRACTED_FIELD_NAME: &str = "_retracted";  // Boolean, non-nullable

pub fn to_flow_event_schema(schema: &Schema) -> Schema {
    let mut fields = vec![
        Field::new(MARKER_FIELD_NAME, DataType::Binary, true),
        Field::new(RETRACTED_FIELD_NAME, DataType::Boolean, false),
    ];
    fields.extend(schema.fields().iter().map(|x| x.as_ref().clone()));
    Schema::new(fields)
}
```

- `_marker`: `NULL` for data rows; non-null for control messages (watermarks, checkpoints).
- `_retracted`: `false` for normal INSERT rows; `true` for DELETE/retraction events (used in stateful aggregations with retractions).

The streaming physical plan operates on flow event `RecordBatch`es throughout. Only the final `StreamCollectorExec` strips these fields before writing to the sink. This architecture supports future stateful streaming operations (like retract-mode aggregations) without changing the physical plan execution model.

### Streaming Query Lifecycle

`StreamingQuery` in `crates/sail-spark-connect/src/streaming.rs` manages the lifecycle of a running streaming query. It spawns a background tokio task that drives the execution:

```rust
// crates/sail-spark-connect/src/streaming.rs
pub struct StreamingQuery {
    name: String,
    info: Vec<StringifiedPlan>,       // plan strings for explain()
    error: watch::Receiver<Option<SparkThrowable>>,   // latest error
    stopped: watch::Receiver<bool>,   // has the query stopped?
    signal: Option<oneshot::Sender<()>>,  // stop signal
    awaitable: bool,
}
```

A `watch::Sender`/`Receiver` pair propagates query state (running, stopped, errored) from the background task to `SparkSession.get_streaming_query_status`. When a Python client calls `query.stop()`, it sends a signal to the `oneshot::Sender`, causing the background loop to exit. The `watch::Receiver` is cloneable, so multiple callers can observe the same state channel.

The `StreamingQueryManager` inside `SparkSession` tracks all active streaming queries by `StreamingQueryId` (a `{query_id, run_id}` pair). It supports `list_active_queries()`, `await_queries()` (blocks until all active queries terminate), and `stop_query()`.

### Streaming vs Batch: The Shared Path

Batch and streaming execution share the same `JobRunner` and physical plan execution path below the `rewrite_streaming_plan` step. A streaming plan's physical execution looks like batch execution — the stream runs continuously, micro-batch by micro-batch, through `execute_stream`. The difference is entirely in how the logical plan is structured (flow event schema nodes wrapping the user's plan) and how the `StreamSource` generates RecordBatches (by polling the underlying source repeatedly rather than exhausting a bounded dataset).

## Summary

Sail's execution layer is designed around four key abstractions:

1. **`JobRunner` trait** — decouples planning from execution; `LocalJobRunner` and `ClusterJobRunner` are drop-in substitutes.
2. **Actor model** — all mutable execution state lives in actors (`DriverActor`, `WorkerActor`) that process messages sequentially, eliminating lock contention.
3. **`JobGraph`** — a stage-based distributed execution plan derived from the physical plan tree, with explicit input modes and output distributions for each stage boundary.
4. **Streaming execution** — the same `JobRunner` handles streaming, after a logical plan rewrite that wraps user nodes in flow-event-schema adapters; `StreamingQuery` manages lifecycle via `watch` channels.

The execution layer has no knowledge of Spark, Spark Connect, or Arrow Flight. It receives an `Arc<dyn ExecutionPlan>` and produces a `SendableRecordBatchStream`. The planning layer's job is to put the right tree in; the execution layer's job is to run it efficiently.
