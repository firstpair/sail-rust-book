# Chapter 18: Feature Playbooks

This chapter is a practical contributor guide. It turns the architecture from the
previous chapters into checklists for common changes.

The point is not to replace code review. The point is to help a contributor avoid
the classic Sail mistake: implementing one layer of a feature and forgetting the
other four.

## Adding A Spark Function

Start by classifying the function.

| Kind | Typical location |
|---|---|
| Scalar expression that maps to DataFusion | `crates/sail-plan/src/function/scalar/` |
| Scalar function requiring Spark-specific runtime behavior | `crates/sail-function/src/scalar/` |
| Aggregate function | `crates/sail-function/src/aggregate/` and function registry |
| Window function | `crates/sail-function/src/window/` and function registry |
| Table/generator function | `crates/sail-plan/src/function/table/` and resolver paths |

Then follow the lifecycle:

1. Confirm Spark syntax and behavior.
2. Decide whether the function can be expressed as a DataFusion `Expr`.
3. If yes, register it through the `ScalarFunctionBuilder` closure model.
4. If no, implement the appropriate DataFusion UDF/UDAF/window function.
5. Add type coercion and return-type logic.
6. Add SQL/gold tests.
7. Add DataFrame/PySpark tests if the API path differs.
8. Verify distributed behavior if aggregate state or custom UDF encoding is involved.

Prefer expression-level registration when possible. It keeps the function visible
to DataFusion optimization.

## Adding A Python UDF Path

Python UDF work crosses a wide boundary:

```text
Spark Connect function payload
  -> Sail spec
  -> resolver UDF object
  -> Python serialization/deserialization
  -> Arrow/Python conversion
  -> local or worker execution
  -> codec support if distributed
```

Before editing, identify which UDF family you are touching:

- scalar row-by-row,
- Arrow scalar,
- Pandas scalar,
- grouped aggregate,
- grouped map,
- co-grouped map,
- iterator UDF,
- UDTF.

Then check:

1. Does Spark Connect carry enough metadata?
2. Does the spec type preserve it?
3. Does resolver logic construct the right UDF object?
4. Does `sail-python-udf` know how to call it?
5. Does the codec preserve it for workers?
6. Are Python exceptions mapped back to Spark-friendly errors?

## Adding A Catalog Backend

Catalogs answer questions about names, databases, tables, and views.

1. Add or update configuration in Sail's config model.
2. Implement `CatalogProvider`.
3. Register the provider in `crates/sail-session/src/catalog.rs`.
4. Decide whether it needs runtime-aware wrapping for I/O runtime use.
5. Decide whether catalog caching applies.
6. Map backend errors to `CatalogError`.
7. Return `TableStatus` with accurate format, schema, location, and properties.
8. Test create/list/get/drop behavior.
9. Test table scans through the DataFusion bridge.

Do not stop at metadata tests. A catalog backend is only useful if its `TableStatus`
leads to the right table provider.

## Adding A Table Format

Table formats translate table metadata into read and write behavior.

1. Implement or extend the `TableFormat` contract.
2. Register it in `crates/sail-session/src/formats.rs`.
3. Define source and sink option resolution.
4. Implement scan creation.
5. Implement write planning if supported.
6. Add object-store handling if needed.
7. Add projection/filter tests.
8. Add write-mode tests.
9. Add catalog integration tests.

For lakehouse formats, also ask:

- Does this format support row-level operations?
- Does it need logical expansion rules?
- Does it need an extension physical planner?
- Does it need metadata columns for merge/delete?

## Adding A Logical Plan Node

Use this path when Spark has a logical concept DataFusion does not represent
natively.

1. Add the spec representation if needed.
2. Add resolver logic that creates a `LogicalPlan::Extension`.
3. Implement `UserDefinedLogicalNodeCore`.
4. Preserve schema and expressions correctly.
5. Implement projection-pushdown hints if relevant.
6. Add the physical `ExecutionPlan`.
7. Register downcast planning in `ExtensionPhysicalPlanner`.
8. Add optimizer rules only if planning alone is insufficient.
9. Add codec support for cluster mode if the physical node can reach workers.

The physical planner registration belongs in `sail-session`, not `sail-plan`.

## Adding A Physical Optimizer Rule

Physical optimizer rules rewrite executable plans. They should be used when:

- DataFusion's physical plan is valid but not Spark-compatible,
- a Sail placeholder exec must be lowered,
- a partitioning/distribution contract must be enforced,
- cluster execution needs a safer physical shape.

Checklist:

1. Write down the exact physical invariant.
2. Add a focused rule implementation.
3. Insert it in `get_physical_optimizers` at the correct point.
4. Add tests for before/after plan shape.
5. Confirm the rule does not fight DataFusion's own rules.
6. Test local and cluster execution when distribution changes.

## Adding A Streaming Source

Streaming sources implement `StreamSource`.

1. Define source options and schema.
2. Implement `data_schema`.
3. Implement `scan` to return an `ExecutionPlan`.
4. Ensure execution emits flow-event batches.
5. Register source discovery/read resolution.
6. Test projection behavior.
7. Test query lifecycle: start, status, stop, await.
8. Test interactions with filters, limits, and sinks.

The source should not return ordinary user-schema batches from physical execution.
It must produce the flow-event schema expected by streaming physical operators.

## Adding Distributed Codec Support

If a feature creates a physical object that workers need to execute, the remote
execution codec has to know about it.

Ask:

1. Does the object appear inside an `ExecutionPlan` sent to workers?
2. Does DataFusion's protobuf codec already support it?
3. If not, does Sail's codec need a custom representation?
4. Does the worker session have all function/table-format registrations required
   to decode or re-resolve it?
5. Are version mismatches possible?

Codec work is often the difference between "works locally" and "works in Sail."

## Debugging A Compatibility Bug

Use this order:

1. Reproduce with a minimal SQL query if possible.
2. Compare with Spark output.
3. Inspect the Spark Connect or SQL entry path.
4. Inspect the Sail spec.
5. Inspect initial and final logical plans.
6. Inspect the physical plan.
7. Run in local mode.
8. Run in cluster mode if the feature is distributed-sensitive.
9. Add the smallest regression test that would have caught it.

## Takeaways

Sail features are pipelines. A complete contribution usually needs protocol/spec,
resolver, DataFusion planning, execution, tests, and sometimes distributed codec
support. The playbook is there to keep those layers visible.

Navigation: [Previous: Chapter 17, Testing Spark Compatibility](17-testing-spark-compatibility.md) | [Next: Chapter 19, Roadmap And Codebase Navigation](19-roadmap-and-codebase-navigation.md) | [Reader Guide](00-reader-guide.md)
