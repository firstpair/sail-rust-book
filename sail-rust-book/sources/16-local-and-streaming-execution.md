# Chapter 16: Local And Streaming Execution

The distributed chapters explain the cluster path in detail: job graphs, drivers,
workers, task regions, streams, and shuffles. This chapter fills in two execution
views that deserve their own concise treatment:

- local execution, where Sail directly asks DataFusion to execute a physical plan,
- structured streaming, where Sail rewrites a logical plan into a flow-event plan
  before physical execution.

Both use the same `JobRunner` abstraction, which is exactly why the abstraction is
valuable.

## Code Map

| Concern | File |
|---|---|
| Job runner trait | `crates/sail-common-datafusion/src/session/job.rs` |
| Local and cluster runners | `crates/sail-execution/src/job_runner.rs` |
| Streaming rewriter | `crates/sail-plan/src/streaming/rewriter.rs` |
| Streaming source trait | `crates/sail-common-datafusion/src/streaming/source.rs` |
| Flow event schema | `crates/sail-common-datafusion/src/streaming/event/schema.rs` |
| Flow event streams | `crates/sail-common-datafusion/src/streaming/event/stream.rs` |
| Streaming logical nodes | `crates/sail-logical-plan/src/streaming/` |
| Streaming physical nodes | `crates/sail-physical-plan/src/streaming/` |
| Streaming query manager | `crates/sail-spark-connect/src/streaming.rs` |
| Rate source | `crates/sail-data-source/src/formats/rate/reader.rs` |
| Socket source | `crates/sail-data-source/src/formats/socket/reader.rs` |

## One Trait, Two Execution Modes

The `JobRunner` trait hides the execution backend:

```rust
#[tonic::async_trait]
pub trait JobRunner: StateObservable<JobRunnerObserver> + Send + Sync + 'static {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream>;

    async fn stop(&self, history: oneshot::Sender<JobRunnerHistory>);
}
```

The local and cluster runners implement the same method. Protocol code does not
need to know which mode is active. It retrieves `JobService` from the session and
calls:

```rust
service.runner().execute(ctx, plan).await
```

That line is one of Sail's key architecture compression points.

## Local Execution

`LocalJobRunner` is intentionally small:

```rust
let plan = trace_execution_plan(plan, options)?;
Ok(execute_stream(plan, ctx.task_ctx())?)
```

The runner wraps the plan with telemetry tracing and then delegates to DataFusion's
`execute_stream`. DataFusion handles partition execution inside the process, and
Sail receives a `SendableRecordBatchStream`.

Local mode is not a lesser engine. It is the same planning path with a simpler
execution backend. That makes it useful for:

- development,
- compatibility tests,
- small deployments,
- debugging physical plans before cluster concerns enter.

## Cluster Execution

`ClusterJobRunner` sends the physical plan to the driver actor:

```rust
self.driver.send(DriverEvent::ExecuteJob {
    plan,
    context: ctx.task_ctx(),
    result: tx,
}).await?;
```

The driver builds a job graph and eventually returns a stream. From the caller's
perspective, local and cluster modes both produce `SendableRecordBatchStream`.

The difference is below the trait boundary.

## Streaming Starts As A Logical Rewrite

Streaming is not a different protocol. A streaming query still enters through Spark
Connect, resolves to a logical plan, and becomes a physical plan. The key difference
is that Sail rewrites the logical plan before physical planning:

```rust
let plan = if is_streaming_plan(&plan)? {
    rewrite_streaming_plan(plan)?
} else {
    plan
};
```

The rewriter turns ordinary plan nodes into streaming-aware extension nodes where
needed. For example:

- batch table scans over streaming sources become `StreamSourceWrapperNode`,
- `RangeNode` can be wrapped by `StreamSourceAdapterNode`,
- filters and limits become streaming filter/limit nodes,
- sinks and display paths use collectors and barriers.

## Stream Sources

A streaming source implements `StreamSource`:

```rust
#[async_trait::async_trait]
pub trait StreamSource: Send + Sync + fmt::Debug {
    fn data_schema(&self) -> SchemaRef;

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}
```

The source returns an `ExecutionPlan`, not a raw stream. That keeps it inside the
DataFusion physical-planning model. Current concrete examples include Sail's rate
and socket sources.

## Flow Event Schema

Streaming records carry more than user columns. Sail prepends flow-event fields:

```text
_marker
_retracted
<user columns...>
```

`_marker` is for control messages. `_retracted` distinguishes normal insert events
from retraction/delete events.

This design lets streaming physical operators process one Arrow `RecordBatch`
shape while preserving event semantics. It also gives future stateful operators a
place to represent retract-mode updates.

## Streaming Physical Nodes

The streaming logical nodes are planned by `ExtensionPhysicalPlanner`, just like
other custom nodes. Their physical counterparts live under
`crates/sail-physical-plan/src/streaming/`.

The important nodes are:

| Node | Role |
|---|---|
| `StreamSourceWrapperNode` | Scans a real streaming source |
| `StreamSourceAdapterNode` | Adapts a bounded source into flow events |
| `StreamFilterNode` | Filters flow-event batches |
| `StreamLimitNode` | Applies streaming limit/offset behavior |
| `StreamCollectorNode` | Collects or strips flow-event fields near output |

`BarrierNode` and `BarrierExec` provide checkpoint-like coordination points.

## Streaming Query Lifecycle

Spark Connect exposes streaming operations such as start, stop, status, and await.
Sail tracks running queries with `StreamingQuery` and `StreamingQueryManager`.

The lifecycle uses asynchronous coordination primitives:

- `watch` channels for stopped/error state,
- `oneshot` channels for stop signals,
- `JoinHandle` for the background task.

The manager lives in `SparkSessionState`, so streaming query state is scoped to the
Spark session.

## Current Boundaries

Streaming support is real, but not all Spark streaming semantics are complete.
Readers should distinguish:

- the architecture, which is present and coherent,
- the feature surface, which is still evolving.

The flow-event schema, streaming rewriter, and query manager are the architectural
foundation. Full stateful aggregations, event-time triggers, and continuous-mode
coverage are areas to verify against the current code before making claims.

## Takeaways

Local execution and cluster execution share `JobRunner`. Streaming and batch
execution share the planning/execution stack below a logical rewrite. That is the
pattern to preserve: new execution behavior should enter through clear boundaries,
not by bypassing the common plan and stream model.

Navigation: [Previous: Chapter 15, Custom Nodes And Optimizers](15-custom-nodes-and-optimizers.md) | [Next: Chapter 17, Testing Spark Compatibility](17-testing-spark-compatibility.md) | [Reader Guide](00-reader-guide.md)
