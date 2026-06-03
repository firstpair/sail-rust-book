# Chapter 15: Custom Nodes And Optimizers

Sail uses DataFusion as its query kernel, but Spark compatibility requires plan
constructs DataFusion does not provide directly. The solution is a set of custom
logical nodes, custom physical plans, logical optimizer rules, physical optimizer
rules, and extension planners that connect the pieces.

This chapter collects that machinery in one place. Earlier chapters introduced it
as part of DataFusion and plan resolution; here we treat it as a contributor's map.

## Code Map

| Concern | File |
|---|---|
| Logical nodes | `crates/sail-logical-plan/src/` |
| Physical nodes | `crates/sail-physical-plan/src/` |
| Logical optimizer rules | `crates/sail-logical-optimizer/src/lib.rs` |
| Lakehouse optimizer rule | `crates/sail-plan-lakehouse/src/optimizer.rs` |
| Physical optimizer rules | `crates/sail-physical-optimizer/src/` |
| Physical optimizer pipeline | `crates/sail-physical-optimizer/src/lib.rs` |
| Extension physical planner | `crates/sail-session/src/planner.rs` |
| Session optimizer registration | `crates/sail-session/src/session_factory/server.rs` |

## The Pattern

A Spark-specific plan feature usually crosses five layers:

```text
spec node or resolver condition
  -> DataFusion LogicalPlan::Extension(UserDefinedLogicalNodeCore)
  -> optional logical optimizer rewrite
  -> ExtensionPhysicalPlanner downcast
  -> DataFusion ExecutionPlan
  -> optional physical optimizer rewrite
```

That is a lot of plumbing, but it buys a clear boundary: Sail can keep using
DataFusion's optimizer and execution APIs while adding Spark-shaped behavior.

## Logical Node Inventory

The current Sail logical extension nodes include:

| Logical node | Main purpose |
|---|---|
| `RangeNode` | Spark `range` |
| `ExplicitRepartitionNode` | `repartition`, `coalesce`, `repartitionByRange` |
| `ShowStringNode` | `df.show()` table-string formatting |
| `MapPartitionsNode` | stream and Python/Pandas map-style UDF execution |
| `FileWriteNode` | writes through table/data-source formats |
| `FileDeleteNode` | DELETE planning |
| `MergeIntoNode` | MERGE planning before row-level expansion |
| `MonotonicIdNode` | `monotonically_increasing_id()` |
| `SparkPartitionIdNode` | `spark_partition_id()` |
| `SortWithinPartitionsNode` | Spark partition-preserving sort |
| `SchemaPivotNode` | schema-producing pivot behavior |
| `CatalogCommandNode` | DDL/catalog commands as physical work |
| `BarrierNode` | streaming barrier/checkpoint coordination |
| streaming source/filter/limit/collector nodes | structured streaming flow-event plans |

The exact list can change, so readers should treat `crates/sail-session/src/planner.rs`
as the authoritative dispatch table. If a logical node cannot be downcast there,
it will not become a physical plan.

## Example: Range

`spark.range(start, end, step, numPartitions)` is a good first node because it is
a leaf plan. The logical node carries the range parameters and schema. The physical
planner downcasts it and constructs `RangeExec`.

The lesson is simple:

```text
Spark has a relation; Sail models it as a DataFusion extension node; physical
planning turns it into an executable operator.
```

`Range` also contains partitioning logic, which makes it distributed-friendly. Each
partition receives a slice of the range rather than every worker scanning the whole
sequence.

## Example: Explicit Repartition

Spark repartitioning semantics are more explicit than DataFusion's default optimizer
choices. Sail models user-requested repartitioning as `ExplicitRepartitionNode`.

That node survives logical planning so the physical optimizer can later decide the
right concrete physical shape:

- hash repartition,
- round-robin repartition,
- coalesce,
- or passthrough.

This is a recurring Sail technique: preserve Spark intent long enough that a later
stage can lower it correctly.

## The Extension Physical Planner

`ExtensionPhysicalPlanner` in `sail-session` is the central bridge. It implements
DataFusion's `ExtensionPlanner` trait and performs a sequence of downcasts:

```rust
if let Some(node) = node.as_any().downcast_ref::<RangeNode>() {
    ...
} else if let Some(node) = node.as_any().downcast_ref::<ShowStringNode>() {
    ...
} else if let Some(node) = node.as_any().downcast_ref::<MapPartitionsNode>() {
    ...
}
```

This is not glamorous code, but it is one of the most important files in Sail.
It tells you which extension nodes are executable and where each one enters the
physical layer.

The planner chain is assembled in `ExtensionQueryPlanner`:

```text
lakehouse extension planners
  -> system table planner
  -> listing table planner
  -> Sail custom extension physical planner
```

Ordering matters. Delta and Iceberg table planners get a chance to handle table-
format-specific nodes before the generic Sail planner handles ordinary logical
extension nodes.

## Logical Optimizers

Sail has a small logical optimizer layer in front of DataFusion's defaults.

`DecorrelateLateralProjection` handles a Spark lateral-subquery case before
DataFusion's broader decorrelation rule runs. The important point is not just the
rule itself, but its placement. It must run before DataFusion's `DecorrelateLateralJoin`
because it handles a simpler projection-only case.

Lakehouse writes add another logical rule: `ExpandRowLevelOp`. It rewrites
lakehouse `MERGE` and `DELETE` nodes into row-level write plans that format-specific
planners can execute.

That rule is the bridge between Spark SQL commands and Delta/Iceberg physical
planning.

## Physical Optimizers

Sail's physical optimizer pipeline is more deliberate than "append some rules to
DataFusion." `sail-physical-optimizer` reconstructs the DataFusion rule order and
adds Sail-specific rules at selected points.

The custom rules include:

| Rule | Purpose |
|---|---|
| `JoinReorder` | Dynamic-programming join reorder with safeguards |
| `RewriteExplicitRepartition` | Lowers Sail's explicit repartition placeholder |
| `RewriteCollectLeftHashJoin` | Ensures collect-left joins have valid partitioning |
| `EnforceBarrierPartitioning` | Enforces streaming barrier partition requirements |

This means contributors must distinguish logical optimizer changes from physical
optimizer changes. A logical rule rewrites `LogicalPlan`. A physical rule rewrites
`ExecutionPlan`.

## Contributor Checklist

When adding a new custom operator, ask:

1. Does the Sail spec need a new representation?
2. Does the resolver need to produce a logical extension node?
3. Does the logical node implement `UserDefinedLogicalNodeCore` correctly?
4. Does projection pushdown need `necessary_children_exprs`?
5. Does the physical node implement `ExecutionPlan` and declare `PlanProperties`?
6. Does `ExtensionPhysicalPlanner` downcast and plan it?
7. Does the physical plan need codec support for distributed execution?
8. Does it need logical or physical optimizer support?
9. Does it need tests in local and cluster execution?

The codec question is easy to miss. A node that works locally can still fail in
cluster mode if workers cannot decode it.

## Extension Implications

Third-party extensions need this same multi-layer path. A physical operator alone
is not enough. A logical optimizer rule alone is not enough. A function registry
entry alone is not enough if the physical plan later runs on a worker that cannot
decode it.

That is why the extension architecture chapter treats extension registration as a
bundle of contributions:

- functions,
- logical rules,
- physical rules,
- extension planners,
- session config,
- table formats,
- codecs.

## Takeaways

Custom nodes are the places where Spark semantics become DataFusion-compatible
plans. Optimizer rules preserve or lower those semantics at the right stage.
`sail-session/src/planner.rs` is the physical dispatch map, and
`sail-physical-optimizer/src/lib.rs` is the physical rewrite map.

Navigation: [Previous: Chapter 14, Arrow Flight SQL](14-arrow-flight-sql.md) | [Next: Chapter 16, Local And Streaming Execution](16-local-and-streaming-execution.md) | [Reader Guide](00-reader-guide.md)
