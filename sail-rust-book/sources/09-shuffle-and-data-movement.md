# Chapter 9: Shuffle And Data Movement

Shuffle is where a distributed query engine proves that it is actually distributed.

Up to this point, the book has followed Sail from the front door through logical plans,
DataFusion physical plans, stage graphs, drivers, workers, and task execution. This
chapter zooms in on the data plane: how a `RecordBatch` produced by one task becomes
input to another task, possibly on another worker, under a distribution chosen by the
query plan.

In Sail, this movement is expressed in a compact set of ideas:

- Data moves as Arrow `RecordBatch` streams.
- A stage boundary is represented in the physical plan by `StageInputExec`.
- At task runtime, `StageInputExec` is rewritten into `ShuffleReadExec`.
- The task's final physical plan is wrapped in `ShuffleWriteExec`.
- The driver assigns stream keys, channel numbers, and read/write locations.
- The stream subsystem moves batches through local memory today, with Arrow Flight
  as the remote transport shape.

That last phrase is important: Sail already has the architecture of a networked
shuffle service, but some remote and blocking pieces are intentionally not finished.
This makes the codebase unusually good for learning. You can see the shape of a
distributed engine without getting lost in years of accumulated production machinery.

## Current transport and ownership map

In the pinned upstream tree, stream code is consolidated under
`crates/sail-execution/src/stream/`. `TaskStreamFactory` in `accessor.rs` creates
readers and writers using a task context and the task-runner actor handle. The
actor receives `TaskRunnerMessage` values; local stream storage lives under
`stream/local/`, storage-backed streams under `stream/storage/`, and Celeborn
support under `stream/celeborn/`. These are distinct execution choices, not
interchangeable names for an in-memory channel.

For blocking outputs, the factory can select `CelebornTaskStreamWriter` when
Celeborn is configured; otherwise it builds the multi-channel writer. A graph
extension reusing ordinary shuffles inherits the selected stream machinery. It
does not need to implement a separate peer-to-peer graph network.

Internal Flight transport is in `stream/service/transport/mod.rs`. Current
upstream uses Hyper's HTTP/2 client directly so it can control the reset budget
needed when many shuffle streams terminate early, such as with LIMIT. Transport
clones share connection establishment and reconnection while dispatching requests
concurrently. Reconnecting a transport does not establish that a stateful native
algorithm may safely replay a task. Argentea's whole-job fail-fast policy remains
a separate scheduling and state-lifetime contract.

## Code Map

The core shuffle code lives in these files:

| Concern | File |
|---|---|
| Physical shuffle writer | `crates/sail-execution/src/plan/shuffle_write.rs` |
| Physical shuffle reader | `crates/sail-execution/src/plan/shuffle_read.rs` |
| Merging task streams | `crates/sail-execution/src/stream/merge.rs` |
| Round-robin partitioning | `crates/sail-physical-plan/src/repartition.rs` |
| Task input/output definitions | `crates/sail-execution/src/task/definition.rs` |
| Scheduler input/output placement | `crates/sail-execution/src/driver/job_scheduler/core.rs` |
| Runtime shuffle rewrite | `crates/sail-execution/src/task_runner/actor/core.rs` |
| Stream reader and writer traits | `crates/sail-execution/src/stream/reader.rs`, `crates/sail-execution/src/stream/writer.rs` |
| Actor bridge to stream manager | `crates/sail-execution/src/stream/accessor.rs` |
| Local stream manager | `crates/sail-execution/src/stream/local/core.rs` |
| In-memory stream replicas | `crates/sail-execution/src/stream/local/memory.rs` |
| Arrow Flight stream service | `crates/sail-execution/src/stream/service/server.rs`, `crates/sail-execution/src/stream/service/client.rs` |

If Chapter 8 was about who runs the work, this chapter is about how the work's bytes
find the next consumer.

## The Vocabulary Of Movement

Sail's shuffle layer uses a small vocabulary. Once these terms are clear, the rest of
the code becomes much easier to read.

| Term | Meaning |
|---|---|
| Stage | A group of physical operators that can run without needing data from a later exchange. |
| Task | One partition of a stage, executed as one attempt. |
| Partition | A task-level unit of parallelism within a stage. |
| Channel | A logical output lane from a producer stage to a consumer stage. |
| Attempt | A retry number for a task. Attempts are part of stream keys. |
| Task input | A set of stream locations that a task should read. |
| Task output | A distribution and locator that describe where a task should write. |
| Stream key | The stable identity of a task stream: job, stage, partition, attempt, channel. |
| Location | Where the stream lives: driver, worker, or remote URI for reads; local or remote for writes. |

The central identity is `TaskStreamKey`. Conceptually, it looks like this:

```text
TaskStreamKey {
  job_id,
  stage,
  partition,
  attempt,
  channel,
}
```

The `channel` field is the part that turns one task output into many possible inputs.
If a producer stage has four upstream partitions and eight shuffle channels, each
producer task may write eight streams. A downstream task then reads the channel or
channels assigned to it from all relevant producer partitions.

That gives Sail the basic distributed exchange shape:

```mermaid
flowchart LR
    subgraph "Producer stage"
        P0["Task partition 0"]
        P1["Task partition 1"]
        P2["Task partition 2"]
    end

    subgraph "Shuffle streams"
        C0["channel 0 streams"]
        C1["channel 1 streams"]
        C2["channel 2 streams"]
    end

    subgraph "Consumer stage"
        Q0["Task partition 0"]
        Q1["Task partition 1"]
        Q2["Task partition 2"]
    end

    P0 --> C0
    P0 --> C1
    P0 --> C2
    P1 --> C0
    P1 --> C1
    P1 --> C2
    P2 --> C0
    P2 --> C1
    P2 --> C2

    C0 --> Q0
    C1 --> Q1
    C2 --> Q2
```

For a hash shuffle, "channel 1" means "rows whose hash maps to bucket 1." For a
round-robin shuffle, it means "the next row assigned to lane 1." For a broadcast-like
movement, the scheduler may arrange the input keys so multiple consumers can read the
same producer output.

## From Job Graph To Task Definition

The job graph knows that one stage depends on another. It does not directly contain
open streams. Before a worker can execute a task, the driver must turn graph edges
into concrete task inputs and outputs.

That happens in `TaskInputBuilder::build()` and `TaskOutputBuilder::build()`
in `crates/sail-execution/src/driver/job_scheduler/core.rs`.

For task inputs, the scheduler:

1. Finds the producer stage for the input.
2. Determines the producer partition count and channel count.
3. Computes the `TaskInputKey` values that this consumer task should read.
4. Finds the latest successful or assigned attempt for each producer partition.
5. Turns those keys into a `TaskInputLocator`.

The input locator records where the consumer should fetch streams from:

```text
TaskInputLocator::Driver { keys }
TaskInputLocator::Worker { keys }
TaskInputLocator::Storage { keys }
TaskInputLocator::ShuffleService { channels }
```

For task outputs, the scheduler:

1. Reads the output distribution from the job graph.
2. Serializes hash expressions if the output is hash-partitioned.
3. Chooses the output locator.
4. Returns a `TaskOutput`.

Output locators distinguish `Pipelined { replicas }` from `Blocking`.
For pipelined input, producer placement selects a driver or worker locator.
Blocking input uses a storage locator for the Storage and Flight backends, or
a shuffle-service locator for Celeborn. The stream layer implements the selected
backend; the scheduler does not open its sockets or files.

The result is not an open socket or a live stream. It is a serializable task
definition: inputs, outputs, partition numbers, attempts, and encoded expressions.
That definition can be sent to a worker.

## The Runtime Rewrite

The most important shuffle transition happens inside `TaskPreparation::rewrite_shuffle()`
in `crates/sail-execution/src/task_runner/preparation.rs`.

The stage-level physical plan contains placeholders:

```text
StageInputExec<usize>
```

Those placeholders are not executable by themselves. They say, "this is where this
stage reads from an upstream stage." When the task runner receives the concrete
`TaskInput` values from the driver, it rewrites each placeholder into a real reader:

```rust
StageInputExec<usize> -> ShuffleReadExec
```

Then the task runner wraps the whole plan with a writer:

```rust
plan -> ShuffleWriteExec(plan)
```

The shape is:

```mermaid
flowchart TB
    A["Stage physical plan with StageInputExec"] --> B["Task definition"]
    B --> C["rewrite_shuffle"]
    C --> D["Replace StageInputExec with ShuffleReadExec"]
    D --> E["Wrap final plan in ShuffleWriteExec"]
    E --> F["Executable task plan"]
```

That rewrite is the bridge between planning and execution:

- Planning says which stages depend on which other stages.
- Scheduling says where the task's input and output streams live.
- Runtime rewriting turns those locations into physical execution nodes.

This is a powerful Rust pattern in miniature. The stage plan is generic and reusable.
The task plan is concrete and contextual. Sail uses a tree transform to keep those
concerns separated until the last responsible moment.

## Writing Shuffle Output

`ShuffleWriteExec` is a DataFusion `ExecutionPlan` implementation, but it behaves a
little differently from ordinary relational operators. It does not produce meaningful
rows downstream. Its job is to consume its child plan, partition the child batches, and
write the resulting batches into task streams.

Its main fields are:

```rust
pub struct ShuffleWriteExec {
    plan: Arc<dyn ExecutionPlan>,
    partitioning: ShufflePartitioning,
    properties: Arc<PlanProperties>,
    writer: Arc<dyn TaskStreamWriter>,
}
```

The operator runs its child, partitions batches according to `partitioning`,
and passes the resulting channel batches to its writer. It does not retain a
matrix of physical write locations. `TaskStreamFactory` constructs the writer
from the task key, output definition and schema during preparation.

### Partitioning

Sail supports two writer-side partitioning modes here:

```text
Partitioning::Hash(keys, channels)
Partitioning::RoundRobinBatch(channels)
```

There is also `UnknownPartitioning`, which Sail treats like round-robin for write
purposes.

Hash partitioning uses DataFusion's `BatchPartitioner`. This is the natural choice
because DataFusion already knows how to evaluate physical expressions against Arrow
batches and assign rows to hash buckets.

Round-robin partitioning uses Sail's own `RowRoundRobinPartitioner` in
`crates/sail-physical-plan/src/repartition.rs`. It is intentionally Arrow-native:

1. Build row-index arrays for each destination partition.
2. Use Arrow's `take_arrays()` compute kernel to select the rows for each partition.
3. Construct new `RecordBatch` values with the same schema.

That avoids converting rows into Rust structs or ad hoc values. The shuffle layer
stays columnar.

```mermaid
flowchart LR
    B["RecordBatch"] --> I["row indices per channel"]
    I --> T["Arrow take_arrays"]
    T --> C0["RecordBatch for channel 0"]
    T --> C1["RecordBatch for channel 1"]
    T --> C2["RecordBatch for channel 2"]
```

The start index for round-robin is derived from the input partition:

```text
start = (input_partition * num_partitions) / num_input_partitions
```

That small detail helps distribute initial rows across output channels when multiple
input partitions are writing at once.

### Sinks And Side Effects

The `shuffle_write()` helper opens one task sink with
`writer.open(partition)`. It skips empty child batches, partitions each remaining
batch into a `Vec<Option<RecordBatch>>`, and passes the whole vector to
`sink.write`. The sink handles its physical channels.

Completion has three paths:

- Exhausted input commits the sink.
- A `TaskStreamWriteState::Closed` result stops reading and aborts the sink.
- A read, partition or write error triggers best-effort abort and returns the
  original error.

The current code still uses `abort` for successful early termination; it has a
TODO to model that separately. Do not equate every abort with query failure.

This matters because a downstream consumer may stop early. A `LIMIT` query is the
classic example: once the driver has enough rows, some readers may close. Sail treats
closed sinks differently from failed sinks so that early termination does not
necessarily become a query failure.

`ShuffleWriteExec::execute()` returns a stream, because DataFusion expects every
physical operator to return a `SendableRecordBatchStream`. But the useful work happens
as a side effect: writing to task streams. After writing, the operator emits an empty
`RecordBatch`.

That makes `ShuffleWriteExec` a boundary operator. It turns a normal DataFusion stream
into Sail task output.

## Reading Shuffle Input

`ShuffleReadExec` is the mirror image. Its fields are:

```rust
pub struct ShuffleReadExec {
    properties: Arc<PlanProperties>,
    reader: Arc<dyn TaskStreamReader>,
}
```

`execute(partition, context)` opens `reader.open(partition)` and adapts the
returned `TaskStreamSource` to a DataFusion record-batch stream. The reader owns
the routing information and merges the producers for that partition. For
bounded input, the operator then coalesces batches using one
`LimitedBatchCoalescer` shared across producers. Unbounded input bypasses that
coalescer so partial batches can be emitted promptly.

The merge is handled by `MergedRecordBatchStream` in
`crates/sail-execution/src/stream/merge.rs`. Internally, it uses a `SelectAll` over
the task streams. That means batches are yielded as upstream streams become ready,
not by fully draining one producer before reading the next.

```mermaid
flowchart LR
    S0["producer stream A"] --> M["MergedRecordBatchStream"]
    S1["producer stream B"] --> M
    S2["producer stream C"] --> M
    M --> R["ShuffleReadExec output"]
```

This is why a consumer task can start processing a pipelined shuffle before every
producer has finished, as long as its input streams are available.

## Stream Readers, Writers and Sinks

The current traits in `stream/reader.rs` and `stream/writer.rs` open one task
partition. The reader returns a `TaskStreamSource`; the writer returns a sink.
Schema and locator information are supplied when `TaskStreamFactory` constructs
the concrete objects, rather than passed into each `open` call.

```rust
async fn open(&self, partition: usize) -> Result<TaskStreamSource>;
```

The writer's corresponding method returns `Result<Box<dyn TaskStreamSink>>`.
A task sink handles all output channels. Its `write` accepts
`Vec<Option<RecordBatch>>`, with at most one batch per channel per call, and
returns `TaskStreamWriteState`. `commit` and `abort` consume the boxed sink.
A `TaskStreamChannelSink` instead represents exactly one physical channel.

The multi-channel implementation can write independent channels concurrently,
while calls preserve order within each channel. It retains sinks so a failed
write can be followed by abort. Closed channels are distinct from active ones;
early consumer termination is part of the stream contract, not necessarily a
query error.

Concrete readers and writers communicate with the task runner through the
accessor layer. That actor owns local streams and delegates storage or Celeborn
operations to the configured managers. The DataFusion operators depend on the
small stream traits, while task definitions determine the actual data movement.

## Local Memory Streams

The local stream manager has three visible states for local streams:

```text
Pending
Created
Failed
```

This solves a real scheduling race. A consumer may ask for a stream before the producer
has created it. Rather than fail immediately, the manager can register that the stream
is pending and wake the reader when the producer creates it. If creation never happens,
the pending stream eventually times out.

For in-memory streams, Sail uses replicas. The task output locator records
`TaskOutputLocator::Pipelined { replicas }`; the local manager creates the
corresponding memory stream.

The producer writes each batch to the active replica senders. This supports multiple
readers for the same produced stream, which is useful for broadcast-like movement and
for cases where more than one consumer needs the same task output.

The memory stream implementation also handles closed receivers. If a receiver is
closed, the producer can keep writing to remaining active replicas. Once no active
replicas remain, the sink can report `Closed`.

That behavior is one of the quiet but important pieces of distributed query execution:
the data plane must distinguish "nobody needs this anymore" from "the query is broken."

## Arrow Flight As The Remote Shape

Sail's stream service transports task streams between processes using Arrow
Flight. This is an implemented path, including the two-host extension tests.

On the server side, `do_get`:

1. Decodes a `TaskStreamTicket`.
2. Fetches the requested task stream.
3. Encodes `RecordBatch` values as Flight data.
4. Returns a Flight stream.

On the client side, `TaskStreamFlightClient::fetch_task_stream()`:

1. Builds a ticket for the requested task stream.
2. Calls Flight `do_get`.
3. Wraps the returned Flight data as a `RecordBatch` stream.

The important architecture point is that Flight does not replace Arrow batches. It
transports them. Sail's task operators still speak in `RecordBatch` streams on both
sides of the network boundary.

```mermaid
sequenceDiagram
    participant Consumer
    participant Client as Flight client
    participant Server as Flight server
    participant Manager as Stream manager
    participant Producer

    Consumer->>Client: fetch TaskStreamKey
    Client->>Server: do_get(ticket)
    Server->>Manager: fetch local stream
    Producer->>Manager: write RecordBatch
    Manager-->>Server: RecordBatch stream
    Server-->>Client: FlightData
    Client-->>Consumer: RecordBatch stream
```

This is the main reason Arrow Flight fits a system like Sail so well. It lets the
engine preserve its columnar execution model while crossing process and machine
boundaries.

## A Hash Shuffle Walkthrough

Imagine a query like:

```sql
SELECT customer_id, COUNT(*)
FROM orders
GROUP BY customer_id
```

At scale, each worker can scan a subset of `orders`, but final groups must be brought
together by `customer_id`. Rows with the same `customer_id` need to land in the same
downstream partition.

The high-level plan is:

```mermaid
flowchart TB
    Scan["Scan orders"] --> Partial["Partial aggregate by customer_id"]
    Partial --> Exchange["Hash shuffle on customer_id"]
    Exchange --> Final["Final aggregate by customer_id"]
```

In Sail terms:

1. The planner inserts a stage boundary at the exchange.
2. The producer stage output distribution is `Hash`.
3. The scheduler serializes the hash expression into `TaskOutputDistribution::Hash`.
4. The task runner decodes that expression back into DataFusion physical expressions.
5. `ShuffleWriteExec` uses DataFusion's `BatchPartitioner`.
6. Each producer task writes one stream per hash channel.
7. Each consumer task opens the producer streams for its assigned channel.
8. `ShuffleReadExec` merges those streams.
9. The final aggregate sees a normal input stream of Arrow batches.

The row movement looks like this:

```mermaid
flowchart LR
    B["orders RecordBatch"] --> H["hash customer_id"]
    H --> R0["rows for channel 0"]
    H --> R1["rows for channel 1"]
    H --> R2["rows for channel 2"]

    R0 --> S0["TaskStreamKey channel 0"]
    R1 --> S1["TaskStreamKey channel 1"]
    R2 --> S2["TaskStreamKey channel 2"]

    S0 --> Q0["consumer partition 0"]
    S1 --> Q1["consumer partition 1"]
    S2 --> Q2["consumer partition 2"]
```

Notice what does not happen:

- Sail does not serialize rows into a custom row format at the shuffle boundary.
- Sail does not make `ShuffleReadExec` understand hash expressions.
- Sail does not make the scheduler evaluate data.

Each layer keeps a narrow job.

## Other Movement Patterns

Hash shuffle is the easiest to visualize, but Sail's input-key construction can
represent several movement patterns.

### One-To-One

A downstream partition reads the corresponding upstream partition. This is the
cheapest movement pattern and is useful when partitioning is already compatible.

```text
producer partition 0 -> consumer partition 0
producer partition 1 -> consumer partition 1
```

### Hash

Every producer may write every channel. Each consumer reads the channel or channels
assigned to it.

```text
producer partition N, channel C -> consumer partition C
```

### Round Robin

Rows are spread across output channels without using data values as keys. Sail's
round-robin partitioner uses Arrow `take` kernels to construct the destination
batches.

### Broadcast

The same upstream output is made readable by multiple downstream consumers. Local
memory stream replicas are the stream-level mechanism that makes this possible.

### Merge

Many upstream streams are merged into one downstream stream. `MergedRecordBatchStream`
is the simple core abstraction here.

### Rescale

Producer and consumer partition counts may differ. The scheduler's input-key builder
can assign groups of producer streams to consumer partitions.

The exact key-building logic belongs to the scheduler, not the stream operators. This
is another example of Sail's separation between control plane and data plane.

## Failure, Attempts, And Early Termination

Distributed data movement needs identity. Without identity, retries are dangerous:
a consumer might accidentally read data from an old failed attempt.

Sail includes `attempt` in every task stream key:

```text
job_id / stage / partition / attempt / channel
```

The scheduler chooses the latest attempt when building task input keys. That lets the
system distinguish replacement work from old work.

There are also several important runtime behaviors:

- Pending streams allow consumers and producers to start in either order.
- Pending stream timeouts prevent consumers from waiting forever.
- Closed sinks can be normal if downstream no longer needs the data.
- Failed streams are distinct from closed streams.
- Stream errors are mapped back into DataFusion errors at the merge boundary.

These are small details, but they are the difference between a toy exchange and an
engine that can tolerate real distributed timing.

## Extension Use and Qualification Boundaries

Argentea uses existing stage dependencies, partition distributions and stream
transport to exchange graph messages. Its worker-local native state adds an
ownership constraint: a later phase must reach the worker that owns its graph
partition. That constraint belongs in placement and lifecycle handling, not a
second graph transport. Chapter 13 describes the focused host additions.

The code includes blocking storage and Celeborn paths; their presence is not
proof that every extension workload has been qualified on each backend. Record
the selected backend with each test. The two-host graph evidence establishes
the configuration actually run, not interchangeable behavior under every
shuffle service or storage failure.

Attempt identities distinguish streams from different task attempts. They do
not reconstruct lost native graph state. The current native graph job therefore
fails as a whole on worker loss instead of replaying one task against missing
state. Reusing Sail's transport preserves its data movement contracts while
leaving this extension-specific recovery limitation explicit.

## Reading Exercise

Trace one hash-shuffled row through the code:

1. Start in `TaskPreparation::rewrite_shuffle()`.
2. Find where `TaskOutput::partitioning()` converts task output metadata into
   `Partitioning::Hash`.
3. Open `ShuffleWriteExec::execute()` and follow the creation of the partitioner.
4. Follow `shuffle_write()` until it calls `sink.write(batch)`.
5. Open `StreamAccessor` and see how the write location becomes a stream-manager
   message.
6. Then reverse direction through `ShuffleReadExec::execute()`.
7. End in `MergedRecordBatchStream::poll_next()`.

The important question to ask at every step is: "Is this layer deciding where data
should go, or only carrying out a decision made earlier?"

That question is the key to reading distributed query engines.

## Takeaways

Sail's shuffle layer is small enough to study and rich enough to teach the real ideas:

- Shuffle is expressed as Arrow `RecordBatch` streams, not row objects.
- The scheduler chooses keys, attempts, channels, and locations.
- The task runner rewrites stage placeholders into concrete shuffle operators.
- `ShuffleWriteExec` partitions and writes batches as a side effect.
- `ShuffleReadExec` opens task streams and merges them.
- Local memory streams support pending readers and replicas.
- Arrow Flight provides the natural shape for remote batch transport.
- The open areas around remote, blocking, disk, and adaptive shuffle are prime
  extension points.

The next chapter moves from movement to memory and execution behavior: how Arrow batch
size, streaming, boundedness, and operator properties influence distributed execution.
