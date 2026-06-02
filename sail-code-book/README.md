# Sail Book

*Sail: A Spark-Compatible Query Engine in Rust*

A technical deep-dive into how Sail works, written for experienced Rust developers who want to understand the internals, contribute, or build on top of Sail.

## Chapters

| File | Title |
|---|---|
| `00-preface.md` | Preface — what Sail is, who this book is for |
| `01-overview.md` | Architecture Overview — big picture, crate layout, query path, spec IR |
| `02-spark-connect.md` | The Spark Connect Protocol — gRPC, sessions, executor, reattachment |
| `02b-sql-pipeline.md` | The SQL Pipeline — custom chumsky parser, 368 keywords, sql-macro proc-macros, AST→spec analyzer |
| `03-arrow.md` | Apache Arrow — data representation, type mapping, IPC |
| `04-datafusion.md` | Apache DataFusion — plan resolver, 17 custom nodes, full physical pipeline, sail-session |
| `05-flight-sql.md` | Apache Arrow Flight SQL — two-phase protocol, metrics, session model |
| `06-execution.md` | The Execution Layer — job runner, actor model, job graph, streaming architecture |
| `07-catalogs.md` | Catalog Integrations — Glue, HMS, Unity, Iceberg, Memory; Delta write scope |
| `08-rust-patterns.md` | Rust Patterns — errors, async, function DSL, PyO3, codegen, gold tests |
| `09-conclusion.md` | Conclusion — contributing, roadmap, codebase navigation |

## Building

### Prerequisites

- [pandoc](https://pandoc.org/installing.html) >= 3.x
- [typst](https://github.com/typst/typst) (optional, for PDF)

On macOS:

```bash
brew install pandoc
brew install typst   # optional
```

### Build

```bash
cd book/
./build.sh
```

Output goes to `book/out/`:

| File | Format |
|---|---|
| `sail-book.epub` | EPUB 3 (e-readers, Kindle) |
| `sail-book-merged.md` | Single Markdown file |
| `sail-book.pdf` | PDF via typst (if typst is installed) |

### Mermaid Diagrams

The book uses standard fenced ` ```mermaid ``` ` blocks. pandoc does not render Mermaid natively; to render diagrams in the EPUB/PDF you need to pre-process with `mermaid-filter` or `mermaid-js`:

```bash
npm install -g @mermaid-js/mermaid-cli
# Then add --filter mermaid-filter to the pandoc command in build.sh
```

Alternatively, GitHub and many Markdown viewers render Mermaid inline — the `.md` files are readable as-is on GitHub.

### Reading Without Building

The Markdown source files are readable directly in any Markdown viewer. All code blocks are self-contained with file paths relative to the repository root.
