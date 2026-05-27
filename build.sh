#!/usr/bin/env bash
# Build pipeline for "Learning Rust, SparkConnect, Apache Arrow and DataFusion with Sail".
#
# Sources (sources/) -> rendered build copy (build/) -> deliverables (book/)
#
# Requirements:
#   - pandoc       (markdown -> Typst, markdown -> EPUB)
#   - typst        (typst -> PDF)
#   - node         (render-diagrams.mjs: mermaid -> SVG)
#   - ebook-convert from Calibre at /Applications/calibre.app (EPUB -> MOBI)
#
# Usage:
#   ./build.sh              # full build: PDF + EPUB + MOBI
#   ./build.sh pdf          # PDF only
#   ./build.sh epub         # EPUB only
#   ./build.sh mobi         # EPUB + MOBI
#   ./build.sh clean        # remove build/ and book/ deliverables

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$ROOT/sources"
BOOK="$ROOT/book"
BUILD="$ROOT/build"
ART="$BUILD/.codex-artifacts/sail-rust-arrow-datafusion-book"
BASE="sail-rust-arrow-datafusion-book"

EBOOK_CONVERT="/Applications/calibre.app/Contents/MacOS/ebook-convert"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: '$1' not found in PATH" >&2
    exit 1
  }
}

stage_sources() {
  echo "==> Staging sources into $ART"
  rm -rf "$ART"
  mkdir -p "$ART/diagrams"
  cp "$SRC"/0?-*.md "$SRC"/1?-*.md "$ART"/
  cp "$SRC"/book-metadata.yaml "$SRC"/template.typ "$ART"/
}

render_diagrams() {
  echo "==> Rendering mermaid diagrams to SVG"
  # render-diagrams.mjs expects ".codex-artifacts/sail-rust-arrow-datafusion-book/"
  # to be relative to the cwd. It rewrites the staged markdown destructively,
  # replacing mermaid blocks with SVG references.
  ( cd "$BUILD" && node "$ROOT/render-diagrams.mjs" )
}

combine_markdown() {
  echo "==> Combining chapter markdowns"
  local out="$ART/$BASE-combined.md"
  : > "$out"
  for f in "$ART"/00-*.md "$ART"/0[1-9]-*.md "$ART"/1[0-3]-*.md; do
    cat "$f" >> "$out"
    printf "\n\n" >> "$out"
  done
}

generate_typst() {
  echo "==> Generating Typst (pandoc + custom template)"
  # Relative image paths (diagrams/XX-diagram-NN.svg) must resolve from the
  # build dir, so run pandoc with that as cwd.
  ( cd "$ART" && pandoc \
    --metadata-file=book-metadata.yaml \
    --template=template.typ \
    --standalone \
    --toc \
    --toc-depth=2 \
    --number-sections \
    "$BASE-combined.md" \
    -o "$BASE.typ" )
}

generate_pdf() {
  echo "==> Compiling PDF with Typst"
  ( cd "$ART" && typst compile "$BASE.typ" "$BASE.pdf" )
}

generate_epub() {
  echo "==> Generating EPUB"
  # cd into $ART so pandoc finds diagrams/*.svg and embeds them in the EPUB.
  ( cd "$ART" && pandoc \
    --metadata-file=book-metadata.yaml \
    --standalone \
    --toc \
    --toc-depth=2 \
    --number-sections \
    "$BASE-combined.md" \
    -o "$BASE.epub" )
}

generate_mobi() {
  echo "==> Generating MOBI (calibre)"
  if [[ ! -x "$EBOOK_CONVERT" ]]; then
    echo "warning: $EBOOK_CONVERT not found; skipping MOBI" >&2
    return 0
  fi
  "$EBOOK_CONVERT" "$ART/$BASE.epub" "$ART/$BASE.mobi" >/dev/null
}

publish() {
  echo "==> Publishing artifacts to $BOOK"
  mkdir -p "$BOOK"
  for ext in pdf epub mobi typ; do
    [[ -f "$ART/$BASE.$ext" ]] && cp "$ART/$BASE.$ext" "$BOOK/"
  done
  [[ -f "$ART/$BASE-combined.md" ]] && cp "$ART/$BASE-combined.md" "$BOOK/"
  rsync -a --delete "$ART/diagrams/" "$BOOK/diagrams/"
}

prepare_pdf_stack() {
  stage_sources
  render_diagrams
  combine_markdown
  generate_typst
}

prepare_ebook_stack() {
  # PDF and ebook share the markdown prep; only do it once if both are built.
  if [[ ! -f "$ART/$BASE-combined.md" ]]; then
    stage_sources
    render_diagrams
    combine_markdown
  fi
}

cmd="${1:-all}"

case "$cmd" in
  clean)
    echo "==> Removing build/ and book/ deliverables"
    rm -rf "$BUILD"
    rm -f "$BOOK"/$BASE.{pdf,epub,mobi,typ} "$BOOK/$BASE-combined.md"
    rm -rf "$BOOK/diagrams"
    ;;
  pdf)
    need pandoc; need typst; need node
    prepare_pdf_stack
    generate_pdf
    publish
    ;;
  epub)
    need pandoc; need node
    prepare_ebook_stack
    generate_epub
    publish
    ;;
  mobi)
    need pandoc; need node
    prepare_ebook_stack
    generate_epub
    generate_mobi
    publish
    ;;
  all|"")
    need pandoc; need typst; need node
    stage_sources
    render_diagrams
    combine_markdown
    generate_typst
    generate_pdf
    generate_epub
    generate_mobi
    publish
    echo
    echo "Build complete. Deliverables in $BOOK/:"
    ls -lh "$BOOK"/$BASE.* 2>/dev/null || true
    ;;
  *)
    echo "usage: $0 [pdf|epub|mobi|all|clean]" >&2
    exit 2
    ;;
esac
