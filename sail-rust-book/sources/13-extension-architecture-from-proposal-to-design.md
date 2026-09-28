# Chapter 13: Extension Architecture in the Implemented Branch

An extension crosses several boundaries: client intent, session registration,
planning, serialization, worker execution, memory ownership, and teardown.
A scalar function exercises fewer of these than a distributed graph algorithm.
The host contract should expose the boundary each needs without importing its
domain implementation into Sail.

This chapter describes the experimental `querygraph/sail` extension branch pinned
in the reader guide. It is not a description of released Sail 0.7.2. Upstream
already includes the session-factory injection from merged #2630; the protocol,
package bootstrap, native placement, and resource contracts below are additional
branch work. The earlier proposal's broad Rust trait sketches are not the
implemented package interface.

## Four Graph Paths and a Spatial Consumer

| Consumer | Where computation lives | Main host responsibility |
|---|---|---|
| SedonaDB scalar functions | Native DataFusion UDFs on executing workers | Registration, package identity, expression codecs and geometry fields |
| Pecan | Ordinary Sail queries submitted by a Python algorithm client | Existing relational execution and owned materialization |
| Nutmeg Banda | Native graph kernels over explicitly staged graph state | Native admission, placement, cancellation and retained ownership |
| Nutmeg Grenada | Relational graph computation inside Sail/DataFusion | Existing plans, joins, aggregates and execution resources |
| Argentea | Native partitions retained on Sail workers during one bounded job | Worker-native decoding, stable placement, job lifetime and fail-fast cleanup |

Banda and Grenada name execution paths within the Nutmeg experiment. Pecan is a
portable graph client; it does not require a new graph execution engine in Sail.
Argentea extends the native path to worker-owned partitions. It is not a second
scheduler or transport service.

The distinction matters for memory. Relational plans use Sail's DataFusion
runtime and can expose their operators to its optimizer. A native kernel keeps
its own state and must account it explicitly. Running that kernel in the same
process does not automatically add its allocations to a query memory pool.

## Code Map

These paths refer to the pinned extension branch unless marked upstream.

| Boundary | Source to read |
|---|---|
| Upstream embedding entry point | `crates/sail-spark-connect/src/entrypoint.rs` |
| Connect envelope decoding | `crates/sail-spark-connect/src/proto/extension.rs` |
| Relation resolution | `crates/sail-plan/src/resolver/query/extension.rs` |
| Session protocol contract | `crates/sail-common-datafusion/src/connect_extension.rs` |
| Driver-native contract | `crates/sail-common-datafusion/src/driver_extension.rs` |
| Worker-native contract | `crates/sail-common-datafusion/src/worker_extension.rs` |
| Native accounting | `crates/sail-common-datafusion/src/native_resource.rs` |
| Resource lease ABI | `crates/sail-native-resource-ffi/src/lib.rs` |
| Worker session construction | `crates/sail-session/src/session_factory/worker.rs` |
| Job partitioning | `crates/sail-execution/src/job_graph/planner.rs` |
| Native graph package | `examples/extensions/nutmeg/` |
| Distributed algorithm core | `examples/extensions/argentea/src/` |
| Portable graph client | `examples/extensions/graph-algorithms/src/pyspark_pecan/` |

The extension examples have independent Cargo workspaces. Sedona has no Sail
engine dependency. Nutmeg shares the small resource-lease ABI definition, rather
than depending on Sail's planner or scheduler crates.

## The Protocol Actually Implemented

Spark Connect offers relation, expression and command extension messages. The
branch does not implement all three raw dispatch paths.

| Client operation | Implemented representation |
|---|---|
| Extension relation | `Relation.extension` carrying a registered type URL |
| Native scalar expression | Ordinary unresolved function call, resolved by name |
| Mutation with a receipt | A relation whose execution performs the operation |

Raw `Expression.extension` and `Command.extension` dispatch are not implemented.
A command-shaped relation must be executed, for example by collecting its receipt.
Constructing a DataFrame or asking for its schema must not mutate graph state.

A relation with DataFrame inputs uses an outer `google.protobuf.Any` type URL
`type.googleapis.com/sail.extension.v1.SailExtensionRequest`. Its payload is:

```proto
message SailExtensionRequest {
  string payload_type_url = 1;
  bytes payload = 2;
  repeated spark.connect.Plan inputs = 3;
  repeated spark.connect.Expression input_expressions = 4;
  uint32 envelope_version = 5;
}
```

Version 1 is required. Inputs are root plans; input expressions are rejected by
this implementation. Sail resolves the input plans and restores their column
names before dispatch. The inner payload belongs to the extension, such as
Nutmeg's versioned JSON request. Bare payloads are accepted only where the
manifest explicitly permits them.

The pinned client environment uses Python 3.12, PySpark 4.0.1, protobuf runtime
7.36.2, grpcio 1.84.0 and PyArrow 21.0.0. PySpark supplies generated Connect messages;
client code does not need to regenerate them with `protoc`. The protobuf Python
runtime version is separate from Sail's build-time protobuf compiler. The example
clients use internal PySpark plan APIs, so this pin is part of reproduction.

## Package Discovery and Compatibility

With `SAIL_EXPERIMENTAL_EXTENSIONS=1`, Sail loads installed `pysail.extensions`
entry points in name order. A package bootstrap returns a factory with a
manifest and binding methods. Without the opt-in, ordinary Sail remains available.
A package declaration looks like:

```toml
[project.entry-points."pysail.extensions"]
nutmeg = "sail_nutmeg:extension"
```

The manifest includes API, DataFusion and Arrow versions, package identity,
placement, and relation registrations. Each registration names a type URL and
its accepted input cardinality. Colliding names or URLs are refused rather than
silently overriding builtins. The pinned examples use DataFusion 55.1.0 and
Arrow 59.3.0. Matching these declarations is necessary, but does not constitute a
universal stable-ABI guarantee.

The loader checks package content and configuration as well as declared identity
when tasks cross to workers. Changing package bytes under an unchanged version
must not silently change an executing job. Deployment must record the native
wheel for each platform; an ARM wheel and an Intel wheel cannot have identical
bytes. Consult the compatibility matrix for the particular artifact combination.
A same-version manifest alone does not establish cross-platform compatibility.

Installed native packages are trusted code. Manifest validation does not sandbox
Rust, Python bootstrap code, GEOS, filesystem access, or network access.

## Planning Without Eager Execution

A bound relation handler receives the payload and named
`datafusion_execution_plan` capsules for its inputs. A driver relation returns a
`datafusion_table_provider` capsule containing an `FFI_TableProvider`. The
provider describes work; execution consumes inputs and produces Arrow batches.
Native scalar objects expose a `datafusion_scalar_udf` capsule containing an
`FFI_ScalarUDF`.

This separation allows schema inspection and optimizer traversal without executing
an algorithm or changing a staged graph. Native state capture, algorithm work,
and drop operations happen during execution. Reconnects and repeated actions do
not imply exactly-once mutation semantics. Clients must consume receipts and
respect the documented graph/session lifetime.

The small buildable packages are the best implementation examples. A new package
should start from their pinned capsule contracts rather than implementing an
imagined public Rust `SailExtension` trait.

## Sedona: Fields Are Part of Correctness

The example imports actual SedonaDB native/GEOS scalar functions. It exposes
128 functions plus aliases at the recorded revision, with selected semantic
checks. The count is not a claim that every function has exhaustive coverage.
Ordinary Sail joins can evaluate spatial predicates; an indexed spatial join,
raster surface and the full Spark Sedona API are not implemented here.

Geometry is represented by WKB with GeoArrow field metadata. Correctness therefore
requires more than transporting binary bytes. Compatible fields must survive
selected expression composition, arrays and shuffle serialization. Arbitrarily
copying metadata from an input would mislabel incompatible binary, geography or
coordinate-reference data. Collision policy also matters: excluded names retain
Sail's existing implementation or placeholder rather than silently taking on a
plugin's semantics.

The pinned Sedona source required a DataFusion/Arrow compatibility port, and its
wheels bundle GEOS. Dependency closure and platform tags are part of the artifact
qualification. Loading a wheel successfully is weaker evidence than executing
its geometry functions on the actual workers.

## Pecan and Grenada: Use the Existing Relational Engine

A graph table is still a relation. Degree, triplet and bounded-walk operations can
be expressed as joins and aggregates over vertex and edge DataFrames. The Nutmeg
graph-table helper can run these ordinary plans without native extension loading
or CSR construction.

Pecan implements iterative graph algorithms by submitting ordinary relational
work and retaining owned materialized state between iterations. Its control flow
is in the client; each query still executes through Sail. Grenada evaluates the
relational graph path through Sail/DataFusion. These paths expose relational work
to the existing engine instead of exporting it to an external graph service.

These choices incur different costs from a native adjacency representation:
joins, repartitioning, materialization, and repeated actions can dominate. They
also reuse ordinary execution and memory mechanisms. Neither advantage proves a
performance result without the same graph, semantics and resource envelope.

## Banda: Explicit Native State and Memory Ownership

Banda stages graph input and invokes native kernels. The driver-native path can
consume distributed input without distributing the kernel itself. Gathering
input from workers is not distributed PageRank or WCC.

The host reserves a native quota from an explicit resource domain and passes a
lease through `bind_with_resources`. Native state and retained output buffers
must keep that lease alive until their final owner releases it. Accounting must
precede expansion, rather than estimating memory only after allocation succeeds.
A shared CSR cache and Arrow output can outlive a temporary computation object;
releasing the session's reference alone is therefore insufficient.

The quota is non-spillable and is not an operating-system RSS ceiling. A global
allocator such as mimalloc changes allocation behavior, not which query pays for
an allocation. The experimental contract supplies accounting and ownership,
while the allocator supplies storage. Cancellation checks, job close and final
buffer release are distinct events that need distinct tests.

## Argentea: Worker-Local Native Partitions

Argentea retains source-owned native graph partitions in workers during one job.
The client builds a bounded sequence of native regions linked by existing Sail
exchanges. Each owner participates in phase completion, including owners with no
vertices. Requests and messages carry operation, snapshot, generation, owner and
phase information so that stale or misrouted work can be rejected.

The host additions provide registration and decoding on workers, admission from
the worker's resource domain, stable task-group placement, and lifecycle closure.
The extension owns adjacency, frontier or component state, algorithm messages,
certificates and convergence. It reuses Sail's planning, task scheduling and Arrow
streams. It does not introduce a graph transport or a general iteration service.

Phase count is bounded before execution. BFS uses `2K+4` stages; its defaults
remain 14 levels and 32 stages, with explicit support up to 62 levels and 128 stages.
The larger limit passes ARM process-cluster tests for all three BFS methods,
including a chain that consumes all 62 expansions and a graph that converges after
four. Lazy session views keep individual Connect plans shallow under the existing
wire guard. View registration is not an action per graph iteration: one terminal
materialization executes the composed native job.

The algorithm families must remain explicit:

- PageRank has reference and residual/frontier implementations. Termination and
  normalization semantics belong in the request and correctness oracle.
- BFS has reference, frontier and direction-optimizing methods. Choosing the last
  does not prove that a particular run entered pull mode; receipts record modes.
- WCC has reference label propagation and seeded star contraction. Argentea's
  advanced form operates over original edges; it is not identical to the fused
  physical edge contraction used in other graph paths.
- SSSP has reference relaxation and an advanced all-edge delta-star method. The
  latter is not classical light/heavy delta stepping. It avoids scanning inactive
  edges, but current barrier work still includes local label scans and copies.

An iteration cap is a refusal to certify a complete result, not permission to
return partial answers as success. Cap tests check the typed native cause,
completed topology or barriers, failed job and cleanup. Algorithm names alone
cannot establish equivalent work or equivalent stopping criteria across paths.

## Failure Semantics and Observability

The prototype uses whole-query failure before attempting recovery of retained
native state. A worker loss must not cause a silent task replay against missing
or inconsistent partition state. Native identity, stage inventory, task attempts
and final job status are checked together.

One focused host repair marks nonterminal tasks canceled before unassigning them
when a job fails or is canceled. Previously a failed job could leave task records
showing RUNNING. The repair preserves successful-job ordering and existing terminal
states. This is a scheduler bookkeeping responsibility, not graph logic.

The first client error can be a scheduler-wrapped failure or an earlier transport
error. A peer's cancellation can also arrive before the original native memory
refusal. WCC and SSSP therefore preserve a causal `native_memory_budget` receipt
with owner identity and whether state had initialized. The adapter matches its
pinned dependency's exact local error; it does not infer a memory cause from a
small quota or a generic cancellation.

Receipts report accounted live and peak reservations after the failed call
unwinds. Those numbers are neither RSS nor the size of the refused allocation.
A memory test requires initialized owners, the causal record, terminal tasks,
closed state and successful subsequent work on the same live workers.

All four reference/advanced WCC and SSSP variants pass that reuse sequence on two
ARM worker processes. A 32 MiB native quota sits inside a 48 MiB Sail pool: a leaked
full reservation would prevent another such admission. The test retains readable
Parquet results while issuing later jobs, then checks session, staging and process
cleanup. It does not claim universal leak freedom or physical two-host coverage.

## Build, Run and Review

Use the branch's source-distribution tutorial to build Sail and the separately
packaged extensions. The build requires Rust 1.97.1, Python 3.12 with a shared
library, uv, Git, protobuf tooling, a C/C++ toolchain and GEOS development files.
Pin the commit before building; the original `sail-extensions-1` tag is an earlier
snapshot and does not contain every later graph feature.

```sh
git clone --branch work/extensions-traversal-bench https://github.com/querygraph/sail.git
cd sail
git checkout dcd44f287b8422a62abd06d2410aaf96e225e136
bash examples/extensions/scripts/build.sh
```

The script builds a development-profile Sail executable and separate Sedona and
Nutmeg wheels, installs Pecan, and reports their locations. The combined Nutmeg
wheel also registers the Argentea worker factory. This is a functional review
build; performance comparisons require the disclosed release-profile artifacts
and benchmark settings. Read
`examples/extensions/TUTORIAL.md` for local/process-cluster and multi-host startup.
Use `examples/extensions/WRITING-AN-EXTENSION.md` for the minimal scalar, relation
and command-shaped examples. Its original driver-native examples are not the
Argentea worker protocol. Use `examples/extensions/argentea/PYTHON.md`, `BFS.md`,
`WCC_ADAPTER.md`, `SSSP_ADAPTER.md`, `FAULTS.md` and `GRAPH_RESOURCES.md` for that
protocol's independently executable qualification commands.

The evidence is intentionally scoped. PageRank and the original bounded BFS have
physical two-host functional results. WCC, SSSP, graph fault/resource sequences
and expanded BFS currently have ARM process-cluster evidence; remaining Linux and
physical-host qualification is not supplied by a successful unit test. The large
Pecan/Banda/Grenada traversal campaign is separate from these functional checks.

## Review the Host Contract by Responsibility

A maintainable review isolates each host responsibility and its executable
invariant: dispatch and naming; package identity; geometry field preservation;
placement and codec reconstruction; native admission and retained ownership;
job-scoped cleanup and fail-fast behavior. Domain algorithms stay in example
packages. Pecan's relational path is a useful control: it demonstrates which graph
operations need no new host hook at all.

The branch contains the combined experiment. Its design review describes logical
review units; it does not claim that independently extracted upstream patches
have already been validated. Keep that distinction when planning integration.
The smallest defensible change is one whose missing capability has been observed,
whose owner in Sail is clear, and whose invariant has an executable test.
