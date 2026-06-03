# Chapter 17: Testing Spark Compatibility

Sail's promise is compatibility with Spark-facing behavior, not merely successful
Rust compilation. Testing therefore has to compare Sail against Spark semantics:
SQL output, DataFrame behavior, functions, errors, type coercion, schema names,
streaming state, and protocol responses.

This chapter gives contributors a testing map.

## Code Map

| Concern | File or directory |
|---|---|
| Gold test crate | `crates/sail-gold-test/` |
| Spark gold data scripts | `scripts/spark-gold-data/` |
| Common gold report scripts | `scripts/common-gold-data/` |
| PySpark tests | `python/pysail/tests/spark/` |
| Flight tests | `python/pysail/tests/flight/` |
| Streaming tests | `python/pysail/tests/spark/streaming/` |
| SQL docs and feature docs | `docs/guide/sql/` |
| Spark test recipes | `docs/development/spark-tests/` |
| Function support utilities | `python/pysail/spark/utils/` |

## The Test Pyramid

Sail has several useful test layers:

| Layer | What it catches |
|---|---|
| Rust unit tests | local invariants, parser behavior, optimizer rewrites, codecs |
| Gold tests | Spark SQL/function output compatibility |
| PySpark integration tests | DataFrame API, Connect behavior, Python UDFs |
| Flight tests | Flight SQL protocol and Arrow transport |
| Feature files | behavior-oriented execution scenarios |
| Manual plan inspection | logical/physical plan regressions |

No single layer is enough. A function can pass unit tests and still fail Spark
compatibility because Spark's null handling, string formatting, overflow behavior,
or timestamp display rules differ from DataFusion defaults.

## Gold Tests

Gold tests are the strongest compatibility signal. The workflow is:

1. Run real Spark examples or documentation-derived queries.
2. Store the expected output.
3. Replay the same query through Sail.
4. Compare schemas, values, ordering behavior, and display semantics.

The point is not only "does this expression run?" The point is "does this expression
behave like Spark?"

This is especially important for:

- string functions,
- timestamp and interval functions,
- decimal precision/scale,
- arrays/maps/structs,
- aggregate edge cases,
- null handling,
- ANSI versus non-ANSI behavior.

## Parser Round Trips

The SQL parser has a second testing dimension: syntax preservation. `TreeText`
lets tests parse SQL and unparse it back to normalized text.

Round-trip tests catch grammar regressions that a semantic query test might miss.
For example, a parser can still produce a plan for a common query while losing
support for a rare Spark syntax form.

Use parser round trips for:

- DDL syntax,
- Hive compatibility clauses,
- interval literals,
- complex expressions,
- identifiers and quoting,
- function-call variants.

## PySpark Integration Tests

The Python tests exercise the surface users actually touch. They are especially
important for:

- Spark Connect DataFrame APIs,
- Python UDF registration and execution,
- Pandas and Arrow UDF paths,
- UDTFs,
- streaming commands,
- config behavior,
- error messages as seen by PySpark.

When a test failure shows up here, debug the path in layers:

```text
PySpark call
  -> Spark Connect protobuf
  -> proto-to-spec conversion
  -> PlanResolver
  -> DataFusion plan
  -> JobRunner
  -> Arrow IPC response
  -> PySpark decoding
```

Do not assume the bug is in the final function implementation. Many compatibility
bugs are conversion or type-resolution bugs.

## Flight Tests

Flight SQL tests should verify:

- `GetFlightInfo` schema,
- ticket creation,
- `DoGet` fetch behavior,
- handle consumption,
- command execution,
- Arrow batch encoding,
- basic session/catalog state expectations.

Flight SQL enters through SQL, so it shares parser/analyzer coverage with Spark SQL.
Its unique risk is protocol handling and Arrow Flight framing.

## Plan Inspection

Sail records plan strings for explain output:

- initial logical plan,
- final logical plan,
- final physical plan.

Plan inspection is useful when the output is wrong but no panic occurs. Ask:

1. Did the proto or SQL path produce the right `spec`?
2. Did the resolver choose the right DataFusion expression or Sail extension node?
3. Did the optimizer rewrite away something Spark required?
4. Did physical planning choose the expected custom operator?
5. Did cluster execution introduce a repartition or shuffle issue?

Plan bugs often look like data bugs until you inspect the tree.

## Local Versus Cluster Testing

Local mode is necessary but not sufficient. Cluster mode adds:

- physical plan encoding and decoding,
- worker session setup,
- function re-resolution,
- stream locations,
- shuffle channels,
- task attempts,
- remote data movement.

Any feature that creates a custom physical operator, UDF, UDAF, table format, or
shuffle-sensitive distribution should be tested in cluster mode before being treated
as complete.

## Testing New Functions

For a new Spark function:

1. Add focused Rust unit tests for implementation details.
2. Add gold examples based on Spark behavior.
3. Test nulls, empty inputs, nested types, and edge values.
4. Verify type coercion and return type.
5. Test both SQL and DataFrame entry paths if they differ.
6. If the function creates a UDF or aggregate state, verify distributed execution.

Spark compatibility failures tend to hide in boring edge cases. That is where the
tests earn their keep.

## Testing New Table Formats Or Catalogs

For storage work, test both metadata and execution:

- name resolution,
- create/list/drop behavior,
- schema conversion,
- path and option handling,
- table properties,
- scan projection and filters,
- write modes,
- row-level operations if supported,
- object-store URI behavior.

Catalog code can be correct while the resulting `TableProvider` is wrong. Table
format code can scan correctly while catalog metadata is wrong. Test both sides of
the boundary.

## Testing Extensions

An extension test matrix should include:

| Extension contribution | Required tests |
|---|---|
| Scalar function | SQL, DataFrame, local, cluster if encoded |
| Aggregate/window function | partial aggregation and cluster merge |
| Logical optimizer rule | before/after logical plan |
| Physical planner | local physical execution and cluster codec |
| Table format | read/write plus catalog integration |
| Python discovery | package import and registration |

An extension that only works in a custom local binary has not met Sail's likely
extension bar.

## Takeaways

Testing Sail means testing conversions. Every query crosses protocol, spec,
planning, execution, Arrow, and client boundaries. Good tests identify which
boundary failed.

Navigation: [Previous: Chapter 16, Local And Streaming Execution](16-local-and-streaming-execution.md) | [Next: Chapter 18, Feature Playbooks](18-feature-playbooks.md) | [Reader Guide](00-reader-guide.md)
