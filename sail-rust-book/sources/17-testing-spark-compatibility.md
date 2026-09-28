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

## Testing the Implemented Extension Boundaries

The experimental branch provides executable examples under
`examples/extensions/`. Its scalar and native graph paths need different tests.
A registration count or successful import does not establish worker execution.
A correct returned graph vector does not establish owner placement or cleanup.

| Boundary | Evidence to retain |
|---|---|
| Scalar registration | SQL and DataFrame results, collision refusal, worker package identity |
| Geometry fields | Values and compatible field metadata through composition and shuffle |
| Relation planning | Schema/explain without native execution; validated inputs and payload |
| Physical reconstruction | Actual worker decoding, package identity and complete stage inventory |
| Graph answer | Independent oracle, unreachable/null semantics and deterministic witnesses |
| Stateful worker ownership | Stable owner/worker/process/adjacency identity through every phase |
| Cancellation or loss | Observed initialization before injection; failed job, no replay, surviving closes |
| Native memory refusal | Original causal receipt, terminal tasks and successful same-worker reuse |
| Owned storage | View removal and staging cleanup, including deferred session cleanup |

Reference algorithms are useful correctness controls, but are not automatically
performance references. Argentea's star WCC is not the same implementation as
physical edge contraction in another path. Delta-star SSSP is not classical
light/heavy delta stepping. Compare outputs against an independent oracle and
state the implementation distinction before comparing timings.

## Reproduce a Worker Qualification

Build the pinned Sail executable and native wheel first. Keep the executable and
wheel build receipts; a command-line source SHA does not prove installed bytes.
For a fresh, unmodified build of both artifacts from the checkout in chapter 13,
with the build script's default paths, run from the repository root:

```sh
SAIL_BINARY="$PWD/target/extensions-poc/host/debug/sail"
RUNTIME_SOURCE_SHA="$(git rev-parse HEAD)"
NATIVE_SOURCE_SHA="$RUNTIME_SOURCE_SHA"
SPARK_CONNECT_MODE_ENABLED=1 .venv/bin/python examples/extensions/argentea/python/qualify_bfs.py \
  --case chain-62 --method direction --worker-task-slots 512 \
  --sail-binary "$SAIL_BINARY" \
  --runtime-source-sha "$RUNTIME_SOURCE_SHA" \
  --native-source-sha "$NATIVE_SOURCE_SHA" \
  --output /tmp/argentea-bfs128-review
```

If reusing an older binary or wheel, substitute its recorded source revision; do
not label it with the current checkout hash. Custom build paths also require the
corresponding executable and Python environment.

The output directory must be new. This case exercises 62 actual expansions and
128 native stages. The ordinary small graph with the same bound checks early
convergence and DONE propagation instead. Both are needed to distinguish active
algorithm work from a merely large reserved plan.

Use the separate WCC, SSSP, fault and graph-resource qualifiers documented in
Chapter 13. Their process-cluster evidence uses distinct worker processes on one
host. A two-host claim additionally requires observed host identity and graph
traffic crossing the ownership boundary. A process count cannot prove it.

## Failure Evidence Must Establish the Cause

A small memory quota plus a failed RPC is insufficient to prove a memory refusal.
The first error can be peer cancellation or transport loss. The graph-resource
auditor requires a native `native_memory_budget` receipt after initialization,
matching operation identity, terminal tasks, and owner closure. Its reuse sequence
then runs three successful queries on the same workers and session.

Likewise, a worker-loss test must demonstrate which process was killed and that
the native state existed before the kill. Argentea's functional controls stop the
supervised workers in an observed post-init window before cancellation or loss.
They do not infer the window from a fixed sleep. Every failed attempt is retained,
including failures of the test harness itself.

The first expanded BFS run exposed a client bug before native execution: the
larger request bound was not forwarded to the view composer. Those six failures
remain in the evidence bundle alongside the corrected runs and a reproducing
public-client regression test. Replacing failed receipts with later successes
would hide the tested boundary and its repair.

## Measurements and Qualification Are Separate

A functional development build can establish an answer or cleanup invariant;
it cannot supply a release-performance claim. Preserve compiler/profile,
host binary, wheel bytes, dataset identity, algorithm options, process placement,
thread count, memory limits, repetitions and the original outcome of every cell.

Report elapsed-time boundaries explicitly: end-to-end input validation and staging
are different from kernel time. Report memory by what was observed: native
reservation peaks, process RSS and container peaks are different measurements.
For two hosts, retain per-host observations and explain how an aggregate peak was
formed; summing two independent peak values need not equal simultaneous usage.

Do not erase timeout, error, unsupported or unavailable cells. A fresh successful
control can investigate a failure but does not replace its original observation.
The retained large PageRank/WCC campaign includes one timeout among 180 trials;
its later successful control remains separate. The traversal campaign is another
frozen workload and does not qualify Argentea performance by association.

## Takeaways

Testing Sail means testing conversions. Every query crosses protocol, spec,
planning, execution, Arrow, and client boundaries. Good tests identify which
boundary failed.

Navigation: [Previous: Chapter 16, Local And Streaming Execution](16-local-and-streaming-execution.md) | [Next: Chapter 18, Feature Playbooks](18-feature-playbooks.md) | [Reader Guide](00-reader-guide.md)
