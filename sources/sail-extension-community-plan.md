---
title: "Sail Extensions: Technical Plan and Community Facilitation Brief"
subtitle: "Review of lakehq/sail#1810 and proposed next steps"
author: "Prepared for Sail community coordination"
date: "May 22, 2026"
geometry: margin=0.85in
fontsize: 10.5pt
linkcolor: "1A5FB4"
urlcolor: "1A5FB4"
---

# Executive Summary

Issue [lakehq/sail#1810](https://github.com/lakehq/sail/issues/1810) should be treated as the start of a community design effort, not as a single implementation task. The opening proposal correctly identifies the extension dimensions Sail needs: function registration, session configuration, optimizer rules, physical planner extensions, and execution-time UDF/UDAF re-resolution. The thread then adds an important correction: the long-term plugin boundary should probably not be a Rust trait object ABI.

This brief recommends framing extensions as **two boundaries rather than one**. A plan-time boundary captures user intent once per query; it wants forward and backward wire compatibility, language neutrality, and a format that survives DataFusion and Arrow upgrades. An execution-time boundary runs operators on Arrow batches; it wants zero-copy access, native dispatch, and is willing to accept version coupling in return. The community should pursue these in parallel. Spark Connect's existing `Relation.extension`, `Command.extension`, and `Expression.extension` messages are the natural plan-time channel. DataFusion FFI is the natural execution-time channel. The same `SailExtension` object can register contributions to both.

Recommended leadership posture: frame the work as a short design-and-proof-of-concept track with named owners, narrow deliverables, and explicit decision points. The goal is to turn agreement in the thread into coordinated experiments and then a small sequence of PRs.

# What the Thread Establishes

The issue author, [james-willis](https://github.com/james-willis), motivates the work with Apache SedonaDB. A real spatial extension needs Sail to resolve `ST_*` functions, carry extension session options, run optimizer rules, plan extension nodes, and re-resolve UDFs on workers. The issue also identifies a prerequisite fix: Sail's extension planner currently errors on unknown logical extension nodes rather than returning `Ok(None)`.

[linhr](https://github.com/linhr)'s response validates the extension dimensions but redirects the API strategy. Key excerpt: "we won't use Rust trait as the API". The reason is release independence: a trait-object API couples extension binaries to Sail, Rust, Arrow, DataFusion, and PyO3 versions. The suggested direction is an extension FFI layered on DataFusion FFI.

[paleolimbot](https://github.com/paleolimbot) adds practical risk analysis from DataFusion/Sedona experience. Key excerpt: "the logical plan is not part of the extension FFI". That matters because Sedona-style spatial joins need logical-plan extension nodes and predicate rewrites, not only physical expressions. A second practical warning is that DataFusion FFI version mismatches can crash unless Sail adds version negotiation or compatibility checks.

[shehabgamin](https://github.com/shehabgamin) cc'd `alexy`, which should be treated as a coordination handoff: this is a good moment to organize the work, clarify open decisions, and invite focused contributions.

# Review of the Earlier Plan

The earlier plan had the right inventory but over-weighted a Rust `SailExtension` trait as the main public API. That trait remains useful as an internal representation or a first custom-binary prototype, but the thread makes clear that the community should not make it the stable plugin ABI without first proving the versioning story.

The earlier plan also treated the extension boundary as one decision. In practice extensions cross two boundaries with different stability requirements (see the executive summary). Naming them separately makes the design tractable: one channel does not need to solve every problem.

Revised recommendation:

1. Make the planner-chain prerequisite fix first. It is small, backwards-compatible, and helps any later extension mechanism.
2. Define the extension dimensions as an inventory and test matrix, not yet as a stable trait. Tag each dimension as plan-time, execution-time, or both.
3. Prototype Spark Connect extension dispatch as the plan-time channel. The protocol already defines `Relation.extension`, `Command.extension`, and `Expression.extension`; Sail needs a `type_url`-indexed dispatcher in its resolver. A pattern-A extension (one that decomposes to existing DataFusion operators) is enough to validate the path.
4. Run an end-to-end FFI proof of concept with one execution-time extension, preferably SedonaDB because it exercises the hard path.
5. Decide the execution-time API boundary only after the PoC answers whether DataFusion FFI can cover logical plan nodes, optimizer rules, planner ordering, and worker-side function re-resolution.
6. Keep Python entry-point discovery as a packaging/discovery layer, not the core ABI.

# Source Areas Affected in Sail

## Session Construction

The closest current hook is `ServerSessionMutator`, but the thread correctly notes this is probably too internal to become the public extension API.

Source: [`crates/sail-session/src/session_factory/server.rs`](file:///Users/alexy/src/sail/crates/sail-session/src/session_factory/server.rs#L42)

```rust
pub trait ServerSessionMutator: Send {
    fn mutate_config(
        &self,
        config: SessionConfig,
        info: &ServerSessionInfo,
    ) -> Result<SessionConfig>;
    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder>;
    fn mutate_runtime_env(
        &self,
        builder: RuntimeEnvBuilder,
        info: &ServerSessionInfo,
    ) -> Result<RuntimeEnvBuilder>;
}
```

Session creation also hardcodes built-in Sail config extensions and the query planner.

Source: [`server.rs`](file:///Users/alexy/src/sail/crates/sail-session/src/session_factory/server.rs#L110)

```rust
let mut config = SessionConfig::new()
    .with_create_default_catalog_and_schema(false)
    .with_information_schema(false)
    .with_extension(create_table_format_registry()?)
    .with_extension(Arc::new(create_catalog_manager(...)?))
    .with_extension(Arc::new(ActivityTracker::new()))
    .with_extension(Arc::new(JobService::new(job_runner)))
    .with_extension(Arc::new(self.create_system_table_service(info)?))
    .with_extension(Arc::new(DeltaTableCache::default()));
```

Extension implication: a real API needs a supported way to contribute session config/state without telling extension authors to depend on `ServerSessionMutator` internals.

## Physical Planner Chain

The query planner assembles extension planners in a hardcoded order.

Source: [`crates/sail-session/src/planner.rs`](file:///Users/alexy/src/sail/crates/sail-session/src/planner.rs#L75)

```rust
let mut extension_planners = new_lakehouse_extension_planners();
extension_planners.push(Arc::new(SystemTablePhysicalPlanner));
extension_planners.push(Arc::new(ExtensionPhysicalPlanner));
let planner = DefaultPhysicalPlanner::with_extension_planners(extension_planners);
```

The final branch currently short-circuits the chain.

Source: [`planner.rs`](file:///Users/alexy/src/sail/crates/sail-session/src/planner.rs#L328)

```rust
} else {
    return internal_err!("unsupported logical extension node: {:?}", node);
};
Ok(Some(plan))
```

First PR candidate:

```rust
} else {
    return Ok(None);
};
Ok(Some(plan))
```

This should be proposed as a narrow issue/PR independent of the larger extension API debate.

## Plan-Time Function Resolution

Sail function resolution currently starts from static maps.

Source: [`crates/sail-plan/src/function/mod.rs`](file:///Users/alexy/src/sail/crates/sail-plan/src/function/mod.rs#L22)

```rust
lazy_static! {
    pub static ref BUILT_IN_SCALAR_FUNCTIONS: HashMap<&'static str, ScalarFunction> =
        HashMap::from_iter(scalar::list_built_in_scalar_functions());
    pub static ref BUILT_IN_GENERATOR_FUNCTIONS: HashMap<&'static str, ScalarFunction> =
        HashMap::from_iter(generator::list_built_in_generator_functions());
    pub static ref BUILT_IN_TABLE_FUNCTIONS: HashMap<&'static str, Arc<TableFunction>> =
        HashMap::from_iter(table::list_built_in_table_functions());
}
```

Extension implication: the community must decide whether extensions register high-level Sail function builders, DataFusion UDFs, or FFI descriptors. This is not just a map merge: Sail's planner has Spark compatibility semantics encoded around function construction.

## Execution Codec and Workers

The execution codec has explicit paths to reconstruct known UDFs/UDAFs.

Source: [`crates/sail-execution/src/codec.rs`](file:///Users/alexy/src/sail/crates/sail-execution/src/codec.rs#L2027)

```rust
fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
    // TODO: Implement custom registry to avoid codec for built-in functions
    let udf = ExtendedScalarUdf::decode(buf)
        .map_err(|e| plan_datafusion_err!("failed to decode udf: {e}"))?;
    ...
}
```

Extension implication: any extension mechanism must work in cluster mode. Workers need the same extension registry or FFI library availability as the driver, and mismatches should fail cleanly rather than crashing.

## Existing Python Discovery Precedent

Sail already uses Python entry points for Python data sources.

Source: [`crates/sail-data-source/src/formats/python/discovery.rs`](file:///Users/alexy/src/sail/crates/sail-data-source/src/formats/python/discovery.rs#L27)

```rust
const ENTRY_POINT_GROUP: &'static str = "pysail.datasources";
```

Source: [`crates/sail-session/src/formats.rs`](file:///Users/alexy/src/sail/crates/sail-session/src/formats.rs#L44)

```rust
// Register Python data sources
{
    discover_data_sources()?;
    PythonTableFormat::register_all(registry)?;
}
```

This is useful prior art for discovery, but it is not enough for native query-engine extensions. Data sources can be represented as Python classes; optimizer rules, logical nodes, and physical planners need a native ABI or a tightly version-matched Rust boundary.

# Packaging and Linking Options

## Option A: Statically Linked Rust Extension

This is the fastest prototype path. A custom Sail binary links the extension crate directly.

```toml
[dependencies]
sail-session = { version = "=0.x.y" }
sail-extension = { version = "=0.x.y" }
sail-sedona = { git = "https://github.com/..." }
```

```rust
ServerSessionFactory::new(config, runtime, system, mutator)
    .with_extension(Arc::new(SedonaExtension::new()));
```

Benefit: simplest development model. Risk: not suitable as the long-term plugin story because it requires rebuilding Sail.

## Option B: Python Wheel Discovery Plus Rust Trait Objects

A wheel declares an entry point.

```toml
[project.entry-points."pysail.extensions"]
sedona = "pysail_sedona:register"
```

```python
def register():
    from pysail_sedona import _native
    return _native.extension_handle()
```

Benefit: good end-user UX. Risk: raw Rust trait objects across independently built PyO3 modules are version-coupled and not a stable ABI. Based on the thread, this should not be the primary long-term design.

## Option C: Python Wheel Discovery Plus Extension FFI

A wheel still uses Python entry points for discovery, but the native object is an FFI descriptor with explicit ABI/version metadata.

```toml
[project.entry-points."pysail.extensions"]
sedona = "pysail_sedona:load_extension"
```

```python
def load_extension():
    from pysail_sedona import _native
    return _native.extension_library_path()
```

The extension exposes a C-compatible symbol:

```c
SailExtensionApi *sail_extension_init(const SailHostApi *host);
```

The returned API should include:

- Extension name and semantic version.
- Minimum/maximum Sail extension ABI versions.
- DataFusion FFI version metadata.
- Function registration callbacks.
- Optimizer/planner registration callbacks.
- Error reporting that never crosses the ABI as Rust panics.

Benefit: aligns with [linhr](https://github.com/linhr)'s release-independence goal. Risk: requires a real PoC because DataFusion FFI may not cover logical plan extension needs yet.

## Option D: Spark Connect Protobuf As The Plan-Time ABI

Options A through C all treat the extension boundary as a single ABI decision. Option D treats it as two: Spark Connect protobuf for the plan-time half, and one of A/B/C for the execution-time half. The two halves can ship independently.

The Spark Connect protocol already defines opaque extension hooks:

```text
Relation.extension      google.protobuf.Any
Command.extension       google.protobuf.Any
Expression.extension    google.protobuf.Any
```

These are inherited from upstream Apache Spark. Sail can add a `type_url`-indexed dispatcher in its resolver and route extension payloads to registered handlers. Each handler returns either:

- a Sail spec node built from existing operators (pattern A, pure plan-time), or
- a Sail logical extension node that the execution-time half of the extension then plans and encodes (pattern B).

Pattern A extensions need no Rust ABI at all. A Python wheel that emits Spark Connect messages can contribute a custom relation or command without linking any Sail crate. The compatibility story reduces to "the extension's proto and Sail's dispatcher agreed on a `type_url`."

Pattern B extensions still need an execution-time ABI for their custom operators - that is what Options A, B, or C provide. But the plan-time channel they use is the same as for pattern A.

Benefits:

- forward and backward compatibility follow protobuf rules, not Rust trait-object rules,
- language-neutral; non-Rust extensions become possible,
- no PyO3 version coupling for the plan-time half,
- the same extension proto can target multiple Spark-Connect-compatible engines,
- enables a federation pattern (Sail as Spark Connect client of a separate extension server).

Risks:

- needs proto governance to avoid `type_url` collisions in the community,
- adds a serialization round trip at the plan-time boundary (small, once per query),
- pattern B extensions still depend on the execution-time ABI decision,
- requires Sail to add a new dispatcher in `sail-plan` / `sail-spark-connect`.

Recommended use: pair Option D with Option C. Option C (Python entry-point discovery + extension FFI) covers the execution-time boundary; Option D covers the plan-time boundary. An extension that fits pattern A skips the execution-time work entirely; an extension that needs pattern B uses both.

# Community Action Plan

## Leadership Goal

As community leader, do not assign yourself all implementation. Use the thread to form a small working group around concrete questions. The most useful next move is to post a coordination comment that turns the proposal into tracks with owners and deliverables.

## Proposed Tracks

1. Planner-chain fix.
   - Deliverable: PR changing unknown extension nodes from error to `Ok(None)`.
   - Owner profile: any Sail contributor.
   - Acceptance: existing tests pass; add or update a test proving planner chain fallthrough.

2. Extension inventory and acceptance tests.
   - Deliverable: a checklist of required extension dimensions using SedonaDB as the reference case. Each dimension should be labeled plan-time, execution-time, or both.
   - Suggested lead: [james-willis](https://github.com/james-willis), because the issue and fork already contain the concrete integration.
   - Acceptance: one minimal SQL query that requires scalar UDFs, optimizer rule, logical extension node, physical planner, and worker re-resolution.

3. Spark Connect extension dispatch (plan-time ABI).
   - Deliverable: `type_url`-indexed dispatcher in the resolver for `Relation.extension`, `Command.extension`, and `Expression.extension`. One trivial pattern-A extension that resolves to existing DataFusion operators.
   - Acceptance: a Python client emits a Spark Connect extension message; Sail dispatches it; a recorded DataFusion plan is executed without any execution-side extension involved.
   - Why first: smallest viable proof that the plan-time half of the boundary can move independently of the execution-time PoC.

4. FFI proof of concept (execution-time ABI).
   - Deliverable: minimal external extension loaded without rebuilding Sail.
   - Suggested leads: [linhr](https://github.com/linhr) for Sail/DataFusion API direction and [paleolimbot](https://github.com/paleolimbot) for Sedona/DataFusion FFI reality checks.
   - Acceptance: clear answer on whether DataFusion FFI covers logical plan nodes and optimizer/planner registration.

5. Versioning and safety contract.
   - Deliverable: one-page ABI/versioning policy covering both the plan-time and the execution-time boundary.
   - Required decisions: version negotiation, error behavior on mismatch, supported DataFusion ABI range, whether Sail refuses to load unknown major versions, and what `type_url` namespace policy applies.

6. Ordering and collisions.
   - Deliverable: RFC section defining function-name collision rules, optimizer/planner ordering, and `type_url` collision rules.
   - Required decisions: duplicate functions, duplicate entry-point names, duplicate `type_url` claims, per-session enablement, and rule ordering constraints.

7. Python packaging/discovery.
   - Deliverable: `pysail.extensions` discovery prototype modeled after `pysail.datasources`.
   - Acceptance: installing a wheel makes Sail discover an extension and register on both boundaries; loading failure reports actionable diagnostics that name which boundary failed.

## Specific Asks for Thread Participants

- Ask [james-willis](https://github.com/james-willis) to extract the Sedona fork into a minimal reproducible extension matrix: exact functions, config objects, optimizer rules, logical nodes, physical nodes, and worker decode requirements. Tag each entry plan-time, execution-time, or both.
- Ask [linhr](https://github.com/linhr) to define the preferred host-side boundary on each side: what plan-time dispatch shape belongs in `sail-plan` / `sail-spark-connect`, what execution-time work belongs in Sail vs. DataFusion FFI, and what should remain an internal implementation detail.
- Ask [paleolimbot](https://github.com/paleolimbot) to document the DataFusion FFI gaps that affect Sedona-style logical planning, plus any known failure modes from version mismatches.
- Ask [shehabgamin](https://github.com/shehabgamin) to help triage the thread into linked issues once the tracks are agreed: planner fix, Spark Connect dispatch, FFI PoC, Python discovery, and ordering/collision policy.
- Invite other contributors to claim the small planner-chain fix first, so the community gets an early merged improvement while the harder ABI design continues. The Spark Connect dispatcher prototype is also a good first-extension contribution because it has no FFI prerequisites.

## Suggested Comment to Post

```markdown
Thanks all. I suggest we treat this as a short community design effort rather than one large implementation PR.

My read of the thread:
- The extension dimensions in the issue are right.
- The planner-chain `Ok(None)` fix is a small prerequisite and can move independently.
- The extension boundary is really two boundaries with different stability requirements: a plan-time boundary that captures user intent (once per query, wants wire-format stability) and an execution-time boundary that runs operators on Arrow batches (per batch, wants native dispatch). Naming them separately makes the design tractable.
- Spark Connect's existing `Relation.extension` / `Command.extension` / `Expression.extension` messages are the natural plan-time channel. They are protobuf-versioned and already cross every Sail query.
- The execution-time boundary should likely be FFI-based, not raw Rust trait objects.
- Python entry points are still useful for discovery/packaging, but they should not be confused with either ABI.

Proposed tracks:
1. Planner-chain fix PR.
2. Sedona minimal extension matrix and acceptance query, with each dimension tagged plan-time/execution-time.
3. Spark Connect extension dispatch prototype (plan-time ABI).
4. End-to-end extension FFI PoC (execution-time ABI).
5. ABI/versioning policy covering both boundaries.
6. Optimizer/planner ordering, function collision, and `type_url` collision policy.
7. `pysail.extensions` discovery prototype that registers on both boundaries.

Could james-willis provide the minimal Sedona matrix from the fork, linhr outline the desired Sail/DataFusion FFI boundary on the execution side and the resolver dispatch shape on the plan side, and paleolimbot list the DataFusion FFI gaps that matter for logical plan extensions? Once we have those, we can split follow-up issues and invite contributors to claim individual pieces.
```

# Decision Points Before Implementation

- Is the **plan-time** boundary Spark Connect protobuf dispatch, an internal Rust mechanism, or both?
- Is the **execution-time** boundary Rust trait, C-compatible FFI, DataFusion FFI, or a layered combination?
- Can DataFusion FFI represent the logical-plan extension nodes needed by Sedona-style optimizer rules?
- What `type_url` namespace policy applies to Spark Connect extension dispatch?
- What exact version metadata must every extension expose for each boundary?
- How are extension libraries distributed to workers in local-cluster and Kubernetes-cluster modes?
- Are extensions globally enabled once installed, enabled per server, or enabled per session?
- Do extension function names override built-ins, error on collision, or require namespacing?
- How are optimizer and planner ordering constraints expressed?
- What diagnostics should users see when an extension fails to load, and how do those diagnostics distinguish plan-time from execution-time failure?

# Recommended Sequence

1. Merge the planner-chain fallthrough fix.
2. Publish the Sedona extension matrix as a comment or linked design note, with each dimension tagged plan-time, execution-time, or both.
3. Land a Spark Connect extension dispatcher with one pattern-A toy extension. This proves the plan-time half can ship without resolving the FFI question.
4. Create a focused FFI PoC issue with a 2-3 week target, covering the execution-time half.
5. Decide the ABI/versioning policy for both boundaries from the combined PoC results.
6. Only then design the stable `pysail.extensions` user-facing packaging flow.

This keeps momentum while avoiding a premature stable API. It also gives thread participants concrete ways to contribute according to their strengths: implementation evidence, Sail architecture, DataFusion/Sedona FFI knowledge, and issue triage. The two-boundary framing also means the plan-time work can land independently of the FFI debate, which is the bottleneck step.
