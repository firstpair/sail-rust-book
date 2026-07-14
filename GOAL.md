# Goal: Sail Rust Book Current-Codebase Edition

## Objective

Update *Sail: The Rust, Arrow, and DataFusion Guide* against the current Sail
codebase at `/Users/alexy/src/sail`, build refreshed PDF and EPUB artifacts
through the shared FirstPair book pipeline at `/Users/alexy/src/firstpair`, and
add an Obsidian vault edition as a first-class additional format.

The vault must contain the book text, collocated code excerpts, indexed code
file notes, entity and subsystem notes, and a local plugin that lets a reader
click a code fragment from the book context and open the corresponding code
file note with the fragment highlighted.

## Verified Starting State

- Book source repository: `/Users/alexy/src/book-sources/sail-rust-book`
- Source remote: `git@github.com:firstpair/sail-rust-book.git`
- Book delivery contract: `FIRSTPAIR.md`
- Shared builder: `/Users/alexy/src/firstpair/publishing/scripts/build-library-book.sh`
- Current Sail codebase: `/Users/alexy/src/sail`
- Current Sail checkout: `main` at `1500ebdf`, `fix: count_min_sketch param types + optimize sketch aggregates (#2190)`
- Latest tagged Sail release in local docs: `0.6.6`, dated `July 7, 2026`
- Current book build stamp: `2026.06.03-af0cffae`
- Current generated book outputs: `sail-rust-book/book/`
- Current manuscript sources: `sail-rust-book/sources/`
- Current vault model reference: `/Users/alexy/src/venezia/usavenice`

## Important Constraints

- The source repo owns manuscript, hooks, version metadata, and generated build
  outputs.
- FirstPair owns the shared build implementation, public catalog, Blob-backed
  library delivery, hosted readers, iCloud delivery, and production deployment.
- Building is not publishing. Do not run live FirstPair publication without an
  explicit user request to publish the complete book.
- Use `/Users/alexy/src/sail` as the current technical truth source. Do not
  infer current Sail behavior from the old generated book alone.
- Generated Obsidian vault output should be rebuildable and ignored unless a
  later policy explicitly chooses to commit a packaged vault artifact.

## Review Findings And Proposed Improvements

1. Refresh the book from the current Sail architecture.
   The existing book is strong on the core June 2026 architecture, but the
   current codebase and changelog now emphasize Sail 0.6.5, 0.6.6, and
   post-0.6.6 work. The new edition should add a release-aware update chapter or
   weave updates into the affected chapters.

2. Expand the short late chapters.
   Chapters 14 through 19 are much shorter than the core planning and execution
   chapters. They should become real working chapters, especially Flight SQL,
   custom nodes and optimizers, local/streaming execution, testing, feature
   playbooks, and codebase navigation.

3. Add a current subsystem map.
   The Sail crate list now includes dedicated cache, object-store, catalog,
   Delta, Iceberg, Flight, SQL analyzer, SQL macro/parser, telemetry, physical
   optimizer, logical optimizer, Python UDF, and catalog-provider crates. The
   book should include a current crate ownership map and task-oriented routing
   table.

4. Update SQL and function compatibility coverage.
   Recent changes include `SHOW FUNCTIONS`, `DESCRIBE FUNCTION`, `PIVOT`, named
   windows, higher-order array functions, XML/JSON/time functions, sketch
   aggregate fixes, ordering semantics, and many Spark parity corrections.

5. Update lakehouse and catalog coverage.
   The current book needs more detail on Delta Lake, Iceberg, Hive Metastore,
   Unity Catalog, OneLake, Glue, writable data sources, table properties, and
   OpenAPI client generation.

6. Update distributed execution coverage.
   Current Sail work has several cluster-mode fixes around remote execution,
   data-source work stealing, lakehouse commits on the driver, scalar subqueries,
   worker semantics, Flight schema mismatch workarounds, and error preservation.

7. Add cache and object-store architecture.
   New or newly prominent crates such as `sail-cache` and `sail-object-store`
   deserve code-backed treatment, including listing, metadata, statistics, and
   object-store integration.

8. Reflect Rust platform evolution.
   Recent work migrated the codebase to Rust 2024 and raised the MSRV. The Rust
   foundations chapter should explain the implications for contributors.

9. Make code examples navigable.
   The book currently embeds explanatory code, but it does not maintain an
   explicit code-fragment ledger. The new edition should assign stable fragment
   IDs, source paths, line ranges, and summaries so PDF/EPUB text and vault notes
   all point back to the same evidence.

10. Add the Obsidian vault edition.
    Use the Lighthouse Republics vault approach as the editorial model, but
    adapt it for a codebase book: book chapters, code files, crate notes,
    symbols, fragment notes, call-flow notes, subsystem notes, and a plugin for
    selecting/highlighting fragments.

## Obsidian Vault Requirements

The generated vault should live at:

```text
sail-rust-book/book/dist-obsidian/Sail Rust Book Vault/
```

Proposed vault map:

```text
Sail Rust Book Vault/
  Home.md
  Sail Rust Book/
    Book.md
    Chapters/
    Fragments/
    Code/
      crates/
      python/
      docs/
    Crates/
    Subsystems/
    Symbols/
    Flows/
    Indices/
    Assets/
    _data/
  .obsidian/
    plugins/sail-code-fragments/
```

Required generated data:

- `fragments.json`: fragment ID, book chapter, heading, source path, start line,
  end line, language, symbol names, and short summary.
- `files.json`: every included source file, its crate/subsystem, local note path,
  extracted definitions, and summary.
- `symbols.json`: extracted Rust/Python definitions and their source locations.
- `links.json`: book-to-fragment, fragment-to-file, file-to-symbol, and
  subsystem-to-file edges.

Plugin behavior:

- Render code-fragment links inside Obsidian notes.
- On click, open the generated code-file note.
- Highlight the matching fragment block.
- Support a command palette action to search by fragment ID, path, crate,
  subsystem, or symbol.
- Keep implementation local and simple: plain JavaScript, no build step, modeled
  after the local `lighthouse-triptych` plugin style.

## Build Plan

1. Add source-owned vault scripts and docs:
   - `sail-rust-book/scripts/build-obsidian-vault.py`
   - `sail-rust-book/scripts/check-obsidian-vault.py`
   - `sail-rust-book/obsidian-plugin/sail-code-fragments/`
   - `sail-rust-book/docs/OBSIDIAN-VAULT.md`

2. Add code-index generation:
   - scan `/Users/alexy/src/sail` while excluding `target/`, `.git/`, `.venv/`,
     and generated caches;
   - copy relevant source files into the vault as Markdown notes, not as raw
     source-only blobs;
   - preserve path, language, line numbers, and source commit metadata.

3. Refresh manuscript sources:
   - add current-codebase update material;
   - expand short late chapters;
   - add code-fragment IDs where the book cites code;
   - update roadmap and codebase navigation.

4. Build PDF, EPUB, HTML, chapters, and MOBI through:

```sh
cd /Users/alexy/src/book-sources/sail-rust-book
./sail-rust-book/build.sh
```

5. Build and validate the vault:

```sh
cd /Users/alexy/src/book-sources/sail-rust-book
python3 sail-rust-book/scripts/build-obsidian-vault.py
python3 sail-rust-book/scripts/check-obsidian-vault.py \
  "sail-rust-book/book/dist-obsidian/Sail Rust Book Vault"
```

6. Verify final artifacts:
   - `pdfinfo sail-rust-book/book/sail-rust-book.pdf`
   - `unzip -t sail-rust-book/book/sail-rust-book.epub`
   - inspect `sail-rust-book/book/VERSION.md`
   - validate vault manifests and plugin files
   - scan for accidental local path leaks in reader-facing PDF/EPUB/HTML text

## Initial Definition Of Done

- `GOAL.md` exists and reflects the active scope.
- The manuscript identifies the current Sail commit and release window.
- PDF and EPUB rebuild successfully through FirstPair.
- The Obsidian vault builds from source and contains all book text.
- The vault contains code-file notes for the relevant current Sail codebase.
- Fragment links connect book notes to code notes and highlight the selected
  fragment.
- A validator checks required notes, manifests, fragment targets, and plugin
  presence.
- No live FirstPair publication is run unless separately requested.

## Completion State: 2026-07-14 Local Build

- Sail source reviewed from `/Users/alexy/src/sail` at `1500ebdf`.
- Book version advanced to `2026.07.14`.
- Added Chapter 20, "Current Codebase Edition", covering Sail 0.6.6, recent
  post-release codebase changes, crate routing, improvement proposals, and the
  new Obsidian vault edition.
- Refreshed current-code references in the reader guide, architecture overview,
  lakehouse/catalog chapter, extension chapter, custom-node chapter, roadmap,
  README, metadata, and build wrapper.
- Built FirstPair local artifacts in `sail-rust-book/book/`:
  - `sail-rust-book.pdf`
  - `sail-rust-book.epub`
  - `sail-rust-book.html`
  - `sail-rust-book-chapters/`
  - `sail-rust-book.mobi`
  - versioned relative symlinks using `2026.07.14-ad4488e3`
- Generated the Obsidian vault at
  `sail-rust-book/book/dist-obsidian/Sail Rust Book Vault/`.
- Vault validation passed with 21 chapters, 2,027 code files, and 20,201
  fragment records.
- FirstPair build verification passed: PDF layout, library book contract, and
  version marker.
- EPUB archive integrity passed with no compressed-data errors.
- PDF title page was rendered and visually checked after fixing a date metadata
  collision; it now shows `July 2026` cleanly.
- No live FirstPair publication or production deployment was run.

## Cover, Announcement, And Full Publication Scope: 2026-07-14

- New reader-facing title: *Sail Rust Book: The Rust, Arrow, and DataFusion
  Guide*.
- Cover and headboard theme: an atomic icebreaker sailing through an ice field
  toward clear water and a lighthouse on a rock.
- The First Pair Press mask logo from `/Users/alexy/src/firstpair/logo/` is
  superimposed on the cover and headboard with high-contrast color matching.
- Author line: `Alexy Khrabrov ∈ LakeSail Team`.
- Build metadata must embed the cover in the PDF and EPUB editions.
- Publish the full edition through `/Users/alexy/src/firstpair` with PDF, EPUB,
  HTML readers, cover, Obsidian vault archive, and vault README/guide visible
  in the library.
- Add a First Pair announcement post with the headboard packaged as a textpack,
  covering the First Pair process, library, delivered formats, and Obsidian
  Vault installation/use.

## Completion State: 2026-07-14 Cover Edition And Full Publication

- Book version advanced to `2026.07.14.1`.
- Built and verified PDF, EPUB, HTML, chapter HTML, and MOBI artifacts with
  build stamp `2026.07.14.1-ad4488e3`.
- PDF first page was rendered and visually checked with the new cover.
- EPUB archive integrity passed and its package metadata declares title
  `Sail Rust Book`, creator `Alexy Khrabrov ∈ LakeSail Team`, and a cover image.
- Regenerated and validated the Obsidian vault:
  21 chapters, 2,027 code-file notes, and 20,201 fragments.
- Published the full edition through `/Users/alexy/src/firstpair` to the live
  First Pair library at `https://firstpair.org/`.
- Live catalog now exposes PDF, EPUB, hosted HTML, chapter reader, standalone
  cover, Vault ZIP, and hosted Vault guide for `sail-rust-book`.
- Public README is visible at `https://firstpair.org/sail-rust-book/README.md`
  and links the original Sail repository plus the book source repository.
- iCloud copies were verified byte-for-byte for the PDF, EPUB, Vault ZIP, and
  Vault guide.
- Announcement package initially created at
  `/Users/alexy/src/firstpair/blog/announcing-sail-rust-book/` and delivered as
  `~/icloud/blogs/announcing-sail-rust-book (2026.07.14.1-ad4488e3).textpack`.

## Ownership Correction: 2026-07-14

- The announcement source package was moved out of `/Users/alexy/src/firstpair`
  and into this source repository at
  `sail-rust-book/blog/announcing-sail-rust-book/`.
- FirstPair remains the publisher/library/deployment system, but Sail Rust Book
  owns its project-specific announcement Markdown, textpack, and image assets.
