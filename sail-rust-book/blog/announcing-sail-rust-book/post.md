---
title: "Announcing Sail Rust Book"
subtitle: "A source-linked First Pair edition for Rust, Arrow, DataFusion, and Sail"
date: 2026-07-14
author: "Alexy Khrabrov ∈ LakeSail Team"
slug: "announcing-sail-rust-book"
canonical_url: "https://firstpair.press/announcing-sail-rust-book"
header_image: "assets/sail-rust-book-headboard.png"
header_alt: "An atomic icebreaker cutting through an ice field toward clear water and a lighthouse, with the First Pair Press mask logo."
tags:
  - first-pair
  - sail
  - rust
  - datafusion
  - lakesail
  - obsidian
  - publishing
---

# Announcing Sail Rust Book

![An atomic icebreaker cutting through an ice field toward clear water and a lighthouse, with the First Pair Press mask logo.](assets/sail-rust-book-headboard.png)

First Pair Press is publishing *Sail Rust Book*, a codebase-first guide to Sail,
Rust, Apache Arrow, Apache DataFusion, Spark Connect, distributed execution,
lakehouse storage, testing, and extension design.

I am grateful to Shehab Amin and Heran Lin for creating Sail and for bringing
me in as Head of Ecosystems. I am wildly excited to be back in a Spark
ecosystem that is free of the JVM.

For the first time in 15 years, I ran a Spark shell and the familiar ASCII logo
came up, but there was no JVM anywhere: not on the client, and not on the
server.

![Spark shell running on Rust without a JVM.](assets/spark-rust.png)

That moment says a lot about why Sail matters. It keeps the Spark mental model
and ecosystem surface while moving the implementation into Rust, Arrow, and
DataFusion. The result feels familiar at the shell and radically different
underneath.

Thank you to the whole LakeSail team for making this possible. The LakeSail
platform has just launched, and we will be shipping fast. Watch
[lakesail.com](https://lakesail.com/) for updates daily.

The new edition is built against the current Sail checkout and release window.
It is not just a polished export of a manuscript. It is a reproducible
publishing package: text sources, generated diagrams, version manifests, PDF,
EPUB, HTML readers, and an Obsidian Vault that carries the book and the code
next to each other.

You can find the edition from
[First Pair Press](https://firstpair.press/), through the
[First Pair library](https://firstpair.org/#books), and on the
[Sail Rust Book library README](https://firstpair.org/sail-rust-book/README.md).
The book is available as a
[hosted HTML reader](https://firstpair.org/read/sail-rust-book/),
[chapter reader](https://firstpair.org/read/sail-rust-book/chapters/),
[PDF](https://fl6nu3o2c1oqqnum.public.blob.vercel-storage.com/books/sail-rust-book/pdf/4b0e3c74798d945d-sail-rust-book.pdf),
and
[EPUB](https://fl6nu3o2c1oqqnum.public.blob.vercel-storage.com/books/sail-rust-book/epub/e7e97c4f79c8c8ff-sail-rust-book.epub).
The source-linked study edition is the
[Obsidian Vault](https://fl6nu3o2c1oqqnum.public.blob.vercel-storage.com/books/sail-rust-book/vault/808baed2385167d0-sail-rust-book-full-vault%20%282026.07.14.1-ad4488e3%29.zip),
with a separate
[Vault guide](https://firstpair.org/read/sail-rust-book/guide/).
[First Pair Press](https://firstpair.press/) is the publisher; the library is
the public shelf where the edition, formats, vault, and provenance stay
together.

The cover is the editorial metaphor: an atomic icebreaker moves through an ice
field toward open water, with a lighthouse waiting on rock. That is the job of a
good systems book. It should cut a navigable channel through a large codebase
without pretending the ice was never there.

## A Technical Overview

The book starts from a simple claim: Sail is not "Spark rewritten in Rust" in
the small sense. Sail is a Spark-compatible front door over a Rust query engine
whose spine is Apache Arrow and Apache DataFusion.

That distinction matters because it tells you how to read the code. A PySpark
client can keep using Spark DataFrame and SQL APIs. A SQL client can enter
through Arrow Flight SQL. The server can still converge those requests into one
internal path:

```text
Spark Connect, SQL, or Flight SQL
  -> Sail spec plan
  -> DataFusion logical plan
  -> optimized logical plan
  -> physical execution plan
  -> local or distributed JobRunner
  -> Arrow RecordBatch stream
```

The codebase is organized around that flow. The book uses the current Sail
checkout as a map, not as a pile of files. Front doors live in crates such as
`sail-spark-connect`, `sail-flight`, `sail-python`, and `sail-cli`. Shared
semantic structures live around `sail-common`, `sail-common-datafusion`, and the
Sail spec layer. Planning runs through `sail-plan`, SQL parsing and analysis
through `sail-sql-parser`, `sail-sql-analyzer`, and `sail-sql-macro`, and
session integration through `sail-session`.

Once the query is inside DataFusion's world, the important crates become the
ones that teach how Sail bends a general query kernel toward Spark semantics:
`sail-logical-plan`, `sail-physical-plan`, `sail-logical-optimizer`,
`sail-physical-optimizer`, `sail-function`, `sail-execution`, and the catalog,
lakehouse, object-store, cache, Delta, and Iceberg crates around them. The book
is meant to make those boundaries feel navigable.

## Spark Connect As The Front Door

Spark Connect is the compatibility miracle. The client sends unresolved Spark
relations, commands, expressions, session requests, artifacts, and analysis
requests over gRPC. Sail receives those protobuf trees, converts them into its
own unresolved spec, resolves that spec into a DataFusion logical plan, builds a
physical plan, executes it, and streams Arrow batches back in Spark Connect
responses.

That is why the book spends real time on `sail-spark-connect`. It is not a thin
adapter. It owns the public Spark shape of the system: session identity,
operation IDs, reattachable execution, config behavior, command dispatch,
schema analysis, error compatibility, and result streaming. If you understand
that layer, you understand how a normal PySpark shell can talk to Sail while
the server remains Rust all the way down.

The chapter follows the request path from `SparkConnectServer` through plan
execution and response encoding. It shows why Spark Connect is an external
compatibility protocol, not Sail's internal language. Sail's internal language
is the spec layer, because the engine must also serve SQL, Flight SQL, and
future extension inputs without tying the whole system to one wire protocol.

## Arrow And DataFusion

Arrow is the data contract. It gives Sail schemas, arrays, record batches,
extension types, IPC, PyArrow interop, and a common columnar representation at
the Python/Rust/network boundaries. When the book talks about result batches,
shuffle streams, PyArrow, Spark Connect `ArrowBatch` payloads, and Flight SQL
`FlightData`, it is really talking about the same foundation from several
angles.

DataFusion is the query kernel. Sail uses DataFusion logical plans, physical
plans, expressions, optimizers, `SessionContext`, `SessionState`,
`ExecutionPlan`, extension planners, and record-batch streams. But Sail does
not treat DataFusion as a sealed box. It installs Spark-compatible functions,
custom logical nodes, custom physical nodes, session extensions, catalog
behavior, optimizer ordering, lakehouse planners, and distributed execution
code around DataFusion's abstractions.

That is the central engineering lesson of the book. DataFusion gives Sail the
kernel. Sail adds the Spark semantics, distributed runtime, Python bridge,
catalog/lakehouse surface, and compatibility behavior needed to make that
kernel feel like Spark to users.

## Arrow Flight SQL

Arrow Flight SQL is the second front door. Spark Connect is Spark-shaped; Flight
SQL is SQL-shaped. It matters for ADBC clients, BI tools, JDBC-style workflows,
and systems that already speak Arrow Flight.

The book treats Flight SQL as a proof of architectural convergence. A Flight
SQL statement enters through `sail-flight`, is parsed as SQL, becomes the same
kind of Sail spec plan, runs through the same DataFusion planning and job
service machinery, and returns Arrow Flight data. It does not form a second
engine beside Spark Connect. It proves that Sail has a common planning and
execution spine underneath multiple client protocols.

For extension authors, this is a useful discipline. If a feature only works
when the user arrives through Spark Connect protobufs, it is not really part of
the engine yet. A good extension should usually land below the protocol front
door, where Spark Connect, SQL, and Flight SQL can all reach it.

## Extensions, Logical Plans, And Physical Plans

The extension chapters are where the book becomes a contributor guide. Sail's
architecture is layered, so an extension usually needs more than one hook. A
new function, table format, custom relation, optimizer rule, or physical
operator may need parser support, analyzer support, spec conversion, resolver
behavior, DataFusion expression registration, session configuration, logical
optimizer participation, physical planner support, distributed execution
codecs, worker-side reconstruction, and tests.

The key distinction is logical versus physical planning.

A logical plan says what the query means. It is where Spark semantics,
unresolved client intent, catalog names, custom relation nodes, and optimizer
rules become a DataFusion-compatible representation. A physical plan says how
the query runs. It chooses operators, partitioning, exchanges, custom
`ExecutionPlan` implementations, lakehouse writes, shuffle boundaries, and the
batch streams that workers will execute.

If an extension creates a custom logical node, some physical planner must later
recognize it. If a logical optimizer rewrites a join into a domain-specific
operation, the execution side must know how to run that operation. If a
physical plan runs on remote workers, those workers must be able to decode or
reconstruct the same functions and operators. This is why the book keeps
returning to logical nodes, physical nodes, optimizer order, extension planners,
and plan codecs instead of treating plugins as a single registration callback.

That knowledge is what makes Sail exciting as an ecosystem platform. With the
right extension model, a third-party integration can participate in the same
flow as Sail's built-in Spark compatibility: protocol intent, planning,
optimization, physical execution, distributed runtime, Arrow data movement, and
client-visible results.

## The First Pair Process

First Pair treats a book as source code with reader-facing builds.

The manuscript lives as plain text. Diagrams are generated from source. The
book build runs through the shared First Pair toolchain, with pinned versions of
Pandoc, Typst, Calibre, and supporting scripts. Each delivery writes a
`VERSION.md` manifest with the title, edition, source commit, version stamp, and
artifact names.

Publication is a second step. The library workflow stages exactly one book
package, uploads the heavy artifacts to object storage, refreshes the public
catalog, writes a visible README, copies reader files to the local book shelf,
builds the site, runs smoke checks, deploys production, and verifies that the
live catalog points to the new URLs.

That separation matters. A local build answers "can this book be produced?"
Publishing answers "can readers actually get the right edition from the
library?"

## The Library

The [First Pair library](https://firstpair.org/#books) is the public shelf for
finished editions and previews. For *Sail Rust Book*, the
[library entry](https://firstpair.org/sail-rust-book/README.md) exposes several
formats:

- [PDF](https://fl6nu3o2c1oqqnum.public.blob.vercel-storage.com/books/sail-rust-book/pdf/4b0e3c74798d945d-sail-rust-book.pdf)
  for a stable page layout, cover-first reading, printing, and citation.
- [EPUB](https://fl6nu3o2c1oqqnum.public.blob.vercel-storage.com/books/sail-rust-book/epub/e7e97c4f79c8c8ff-sail-rust-book.epub)
  for e-readers and responsive reading systems.
- [Hosted HTML](https://firstpair.org/read/sail-rust-book/) for quick browser
  reading without downloading a file.
- [Hosted chapter HTML](https://firstpair.org/read/sail-rust-book/chapters/)
  for jumping into one chapter at a time.
- [Obsidian Vault](https://fl6nu3o2c1oqqnum.public.blob.vercel-storage.com/books/sail-rust-book/vault/808baed2385167d0-sail-rust-book-full-vault%20%282026.07.14.1-ad4488e3%29.zip)
  for source-linked study, note navigation, and code exploration, with a
  [Vault guide](https://firstpair.org/read/sail-rust-book/guide/).

The library card is the front door. It links the download formats and reader
routes from one catalog entry, then keeps the underlying storage URLs out of the
reader's way.

## Why This Book Needed A Vault

Sail is not a single file, and it is not a single idea. It is a Rust system
where Spark protocol handling, SQL planning, Arrow data movement, DataFusion
extensions, Python interop, lakehouse catalogs, object storage, workers, tasks,
streams, and tests all meet.

A normal PDF can explain that architecture. An EPUB can carry it well. HTML can
make it easy to browse. But a codebase book also needs a format that can keep
the reader inside the system while the prose is still visible.

That is what the Obsidian Vault does.

The Vault contains:

- every book chapter as Markdown notes;
- generated notes for the current Sail code files;
- crate and subsystem index notes;
- generated code-fragment notes with source paths and line ranges;
- machine-readable ledgers for files, fragments, symbols, links, and units;
- a bundled local Obsidian plugin named `sail-code-fragments`.

The plugin gives the prose a practical gesture. When a chapter shows a
generated code-fragment card, clicking `Open code fragment` opens the matching
code-file note and highlights the selected fragment. The book can point at the
code without asking the reader to search a repository by hand.

## Installing The Obsidian Vault

Download the
[Vault archive](https://fl6nu3o2c1oqqnum.public.blob.vercel-storage.com/books/sail-rust-book/vault/808baed2385167d0-sail-rust-book-full-vault%20%282026.07.14.1-ad4488e3%29.zip)
from the *Sail Rust Book*
[library card](https://firstpair.org/sail-rust-book/README.md), then unzip it
somewhere you keep active notes. The
[Vault guide](https://firstpair.org/read/sail-rust-book/guide/) stays visible
beside the download in the library.

Open Obsidian and choose **Open folder as vault**. Select the unzipped `Sail
Rust Book Vault` folder.

Obsidian may ask whether to trust the vault because it includes a local
community plugin. Trust it only if you downloaded the archive from the First
Pair library. Then enable the bundled `sail-code-fragments` plugin under
**Settings -> Community plugins**.

Start at `Home.md`, then open `Sail Rust Book/Book.md`. From there, use:

- `Sail Rust Book/Chapters/` for the book text;
- `Sail Rust Book/Indices/Code Files.md` for the source-file map;
- `Sail Rust Book/Indices/Fragments.md` for extracted code fragments;
- `Sail Rust Book/Indices/Crates.md` for crate-level navigation;
- `Sail Rust Book/Indices/Subsystems.md` for architecture-level navigation.

The Vault is generated from source. Treat it like a published edition, not like
a private scratch folder. If the book or Sail code changes, rebuild the Vault
from the source repository so the fragments, ledgers, and plugin links stay in
sync.

## A Book As A System

*Sail Rust Book* is a technical guide, but it is also a First Pair publishing
experiment: one source project, multiple reader formats, one catalog, one
visible provenance trail.

The PDF and EPUB are the book as readers expect it. The HTML readers are the
book as a public web object. The Obsidian Vault is the book as a navigable
knowledge system, with the codebase pulled close enough for study.

That is the direction First Pair is pushing: books that are not frozen
snapshots, but reproducible systems readers can inspect, use, and revisit.
