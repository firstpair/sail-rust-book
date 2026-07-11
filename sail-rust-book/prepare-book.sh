#!/usr/bin/env bash
set -euo pipefail

book_root="$(cd "$(dirname "$0")" && pwd)"
source_dir="$book_root/sources"
build_root="$book_root/build"
artifact_dir="$build_root/.codex-artifacts/sail-rust-arrow-datafusion-book"
base="sail-rust-arrow-datafusion-book"

rm -rf "$artifact_dir"
mkdir -p "$artifact_dir/diagrams"
cp "$source_dir"/[0-9][0-9]-*.md "$artifact_dir/"
cp "$source_dir/book-metadata.yaml" "$source_dir/template.typ" \
  "$source_dir/epub.css" "$artifact_dir/"

(
  cd "$build_root"
  node "$book_root/render-diagrams.mjs"
)

combined="$artifact_dir/$base-combined.md"
: > "$combined"
for chapter in "$artifact_dir"/[0-9][0-9]-*.md; do
  cat "$chapter" >> "$combined"
  printf '\n\n' >> "$combined"
done
