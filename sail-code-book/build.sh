#!/usr/bin/env bash
# Build the Sail book as EPUB and a typst-compatible intermediate Markdown.
# Requirements:
#   - pandoc >= 3.x     (https://pandoc.org/installing.html)
#   - typst             (https://github.com/typst/typst)  -- for PDF via typst path
#
# Usage:
#   cd book/
#   ./build.sh
#
# Output:
#   out/sail-book.epub      -- EPUB for e-readers / Kindle
#   out/sail-book.pdf       -- PDF via typst (requires typst installed)
#   out/sail-book-merged.md -- Single merged Markdown (for other tools)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$SCRIPT_DIR/out"
mkdir -p "$OUT_DIR"

CHAPTERS=(
    "$SCRIPT_DIR/00-title.md"
    "$SCRIPT_DIR/00-preface.md"
    "$SCRIPT_DIR/01-overview.md"
    "$SCRIPT_DIR/02-spark-connect.md"
    "$SCRIPT_DIR/02b-sql-pipeline.md"
    "$SCRIPT_DIR/03-arrow.md"
    "$SCRIPT_DIR/04-datafusion.md"
    "$SCRIPT_DIR/05-flight-sql.md"
    "$SCRIPT_DIR/06-execution.md"
    "$SCRIPT_DIR/07-catalogs.md"
    "$SCRIPT_DIR/08-rust-patterns.md"
    "$SCRIPT_DIR/09-conclusion.md"
)

METADATA=(
    --metadata title="Sail Code Book"
    --metadata author="Alexy Khrabrov"
    --metadata author="Claude Code"
    --metadata lang="en-US"
    --metadata date="$(date +%Y-%m-%d)"
)

echo "==> Building EPUB..."
pandoc \
    "${METADATA[@]}" \
    --toc \
    --toc-depth=2 \
    --number-sections \
    --from markdown+fenced_code_blocks+fenced_divs \
    --to epub3 \
    --output "$OUT_DIR/sail-book.epub" \
    "${CHAPTERS[@]}"
echo "    -> $OUT_DIR/sail-book.epub"

echo "==> Merging Markdown for typst..."
pandoc \
    "${METADATA[@]}" \
    --from markdown+fenced_code_blocks \
    --to markdown \
    --output "$OUT_DIR/sail-book-merged.md" \
    "${CHAPTERS[@]}"
echo "    -> $OUT_DIR/sail-book-merged.md"

# PDF via typst. Requires typst and a typst template.
# If typst is not installed, this step is skipped.
if command -v typst &>/dev/null; then
    echo "==> Building PDF via typst..."

    # Convert merged markdown to typst source using pandoc
    pandoc \
        "${METADATA[@]}" \
        --from markdown+fenced_code_blocks \
        --to typst \
        --output "$OUT_DIR/sail-book.typ" \
        "${CHAPTERS[@]}"

    typst compile "$OUT_DIR/sail-book.typ" "$OUT_DIR/sail-book.pdf"
    echo "    -> $OUT_DIR/sail-book.pdf"
else
    echo "==> typst not found; skipping PDF build."
    echo "    Install typst from https://github.com/typst/typst and re-run."
fi

echo ""
echo "Done. Output files in $OUT_DIR/"
