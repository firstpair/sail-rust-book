#!/usr/bin/env bash
# Build the Sail book as EPUB and PDF.
# Requirements:
#   - pandoc >= 3.x     (https://pandoc.org/installing.html)
#   - mmdc              (npm install -g @mermaid-js/mermaid-cli)
#   - typst             (https://github.com/typst/typst)
#
# Usage:
#   cd sail-code-book/
#   ./build.sh
#
# Output:
#   out/sail-book.epub  -- EPUB for e-readers / Kindle
#   out/sail-book.pdf   -- PDF via typst

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$SCRIPT_DIR/out"
mkdir -p "$OUT_DIR"

# Step 1: render Mermaid diagrams → PNG and write processed markdown to processed/
echo "==> Pre-processing Mermaid diagrams..."
python3 "$SCRIPT_DIR/preprocess.py"

PROC="$SCRIPT_DIR/processed"

# Core chapters using pre-processed files (Mermaid replaced with PNG refs)
CHAPTERS=(
    "$PROC/00-preface.md"
    "$PROC/01-overview.md"
    "$PROC/02-spark-connect.md"
    "$PROC/02b-sql-pipeline.md"
    "$PROC/03-arrow.md"
    "$PROC/04-datafusion.md"
    "$PROC/05-flight-sql.md"
    "$PROC/06-execution.md"
    "$PROC/07-catalogs.md"
    "$PROC/08-rust-patterns.md"
    "$PROC/09-conclusion.md"
)

# EPUB prepends the markdown cover; PDF uses cover.typ (prepended below)
CHAPTERS_EPUB=(
    "$SCRIPT_DIR/00-title.md"
    "${CHAPTERS[@]}"
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
    --epub-title-page=false \
    --from markdown+fenced_code_blocks+fenced_divs \
    --to epub3 \
    --output "$OUT_DIR/sail-book.epub" \
    "${CHAPTERS_EPUB[@]}"
echo "    -> $OUT_DIR/sail-book.epub"

# PDF via typst. Requires typst and a typst template.
if command -v typst &>/dev/null; then
    echo "==> Building PDF via typst..."

    # Convert processed chapters to typst (no --toc; cover.typ provides #outline())
    pandoc \
        "${METADATA[@]}" \
        --from markdown+fenced_code_blocks \
        --to typst \
        --output "$OUT_DIR/sail-book-content.typ" \
        "${CHAPTERS[@]}"

    # Prepend cover page (which also includes #outline() for the TOC)
    cat "$SCRIPT_DIR/cover.typ" "$OUT_DIR/sail-book-content.typ" > "$OUT_DIR/sail-book.typ"

    typst compile --root / "$OUT_DIR/sail-book.typ" "$OUT_DIR/sail-book.pdf"
    echo "    -> $OUT_DIR/sail-book.pdf"
else
    echo "==> typst not found; skipping PDF build."
    echo "    Install typst from https://github.com/typst/typst and re-run."
fi

echo ""
echo "Done. Output files in $OUT_DIR/"
