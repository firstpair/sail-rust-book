#!/usr/bin/env python3
"""
Extract ```mermaid blocks from Markdown files, render them to PNG with mmdc,
and replace the blocks with ![](...) image references.
Outputs processed files to book/processed/.
"""
import os
import re
import subprocess
import sys
import shutil

BOOK_DIR = os.path.dirname(os.path.abspath(__file__))
DIAGRAMS_DIR = os.path.join(BOOK_DIR, "diagrams")
PROCESSED_DIR = os.path.join(BOOK_DIR, "processed")

os.makedirs(DIAGRAMS_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

CHAPTERS = [
    "00-preface.md",
    "01-overview.md",
    "02-spark-connect.md",
    "02b-sql-pipeline.md",
    "03-arrow.md",
    "04-datafusion.md",
    "05-flight-sql.md",
    "06-execution.md",
    "07-catalogs.md",
    "08-rust-patterns.md",
    "09-conclusion.md",
]

MERMAID_RE = re.compile(r"```mermaid\n(.*?)```", re.DOTALL)
diagram_counter = [0]
errors = []

def render_diagram(mmd_source: str, slug: str) -> str:
    """Render a mermaid diagram source to PNG. Returns the PNG path relative to BOOK_DIR."""
    diagram_counter[0] += 1
    name = f"{slug}-{diagram_counter[0]}"
    mmd_path  = os.path.join(DIAGRAMS_DIR, f"{name}.mmd")
    png_path  = os.path.join(DIAGRAMS_DIR, f"{name}.png")

    with open(mmd_path, "w") as f:
        f.write(mmd_source.strip() + "\n")

    result = subprocess.run(
        [
            "mmdc",
            "-i", mmd_path,
            "-o", png_path,
            "-b", "white",
            "--width", "1200",
            "--height", "900",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  WARNING: mmdc failed for {name}: {result.stderr.strip()}", file=sys.stderr)
        errors.append(name)
        # Fall back to raw mermaid (pandoc will leave it as a code block)
        return None
    return os.path.relpath(png_path, PROCESSED_DIR)


def process_file(chapter: str) -> str:
    src = os.path.join(BOOK_DIR, chapter)
    dst = os.path.join(PROCESSED_DIR, chapter)

    with open(src) as f:
        content = f.read()

    slug = chapter.replace(".md", "").replace("/", "-")
    replacements = 0

    def replace_mermaid(m):
        mmd_source = m.group(1)
        png_rel = render_diagram(mmd_source, slug)
        nonlocal replacements
        replacements += 1
        if png_rel is None:
            return m.group(0)   # keep original block on failure
        return f"![]({png_rel})"

    processed = MERMAID_RE.sub(replace_mermaid, content)

    with open(dst, "w") as f:
        f.write(processed)

    print(f"  {chapter}: {replacements} diagram(s) processed")
    return dst


print("==> Pre-processing Mermaid diagrams...")
processed_paths = []
for ch in CHAPTERS:
    src = os.path.join(BOOK_DIR, ch)
    if not os.path.exists(src):
        print(f"  WARNING: {ch} not found, skipping", file=sys.stderr)
        continue
    processed_paths.append(process_file(ch))

print(f"\nDone. {diagram_counter[0]} diagram(s) rendered, {len(errors)} failure(s).")
if errors:
    print(f"Failed diagrams: {errors}")

# Write the list of processed file paths for use by build.sh
list_path = os.path.join(BOOK_DIR, "processed", "chapter_list.txt")
with open(list_path, "w") as f:
    for p in processed_paths:
        f.write(p + "\n")
print(f"Chapter list written to {list_path}")
