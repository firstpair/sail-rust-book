#!/usr/bin/env python3
"""Build an Obsidian vault edition for the Sail Rust book."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Iterable


BOOK_ROOT_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = BOOK_ROOT_DIR.parent
DEFAULT_SAIL_ROOT = Path(os.environ.get("SAIL_CODE_ROOT", str(Path.home() / "src" / "sail")))
DEFAULT_OUTPUT = BOOK_ROOT_DIR / "book" / "dist-obsidian" / "Sail Rust Book Vault"
VAULT_BOOK = "Sail Rust Book"

SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    ".venvs",
    "__pycache__",
    "dist",
    "node_modules",
    "spark-warehouse",
    "target",
}
TEXT_SUFFIXES = {
    ".bash",
    ".css",
    ".dockerignore",
    ".editorconfig",
    ".env",
    ".feature",
    ".gitignore",
    ".html",
    ".js",
    ".json",
    ".lock",
    ".md",
    ".mjs",
    ".proto",
    ".py",
    ".rs",
    ".sh",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".yaml",
    ".yml",
}
TEXT_NAMES = {
    "Dockerfile",
    "LICENSE",
    "Makefile",
}
RUST_SYMBOL_RE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?"
    r"(?:(?:async|unsafe|const|extern)\s+)*"
    r"(?P<kind>fn|struct|enum|trait|impl|mod|type|const|static)\b"
    r"(?:\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*))?"
)
PY_SYMBOL_RE = re.compile(
    r"^\s*(?P<kind>class|def|async\s+def)\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
)
MD_HEADING_RE = re.compile(r"^(?P<marks>#{1,6})\s+(?P<name>.+?)\s*$")


@dataclass(slots=True)
class SourceFile:
    path: str
    absolute: Path
    note_path: str
    language: str
    subsystem: str
    crate: str | None
    lines: list[str]
    summary: str
    fragments: list["Fragment"] = field(default_factory=list)


@dataclass(slots=True)
class Fragment:
    id: str
    source_path: str
    note_path: str
    code_note: str
    heading: str
    symbol: str
    kind: str
    language: str
    subsystem: str
    crate: str | None
    start_line: int
    end_line: int
    summary: str


def run_git(args: list[str], cwd: Path) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()
    except Exception:
        return "unknown"


def clean_name(value: str, limit: int = 100) -> str:
    value = re.sub(r'[\\/:*?"<>|#^[\]]+', " ", value)
    value = re.sub(r"\s+", " ", value).strip(" .")
    return (value or "Untitled")[:limit].rstrip()


def slug(value: str, limit: int = 80) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")
    return (value or "item")[:limit].strip("-")


def yaml_value(value: object) -> str:
    return json.dumps(value, ensure_ascii=True)


def frontmatter(values: dict[str, object]) -> str:
    lines = ["---"]
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            lines.append(f"{key}:")
            for item in value:
                lines.append(f"  - {yaml_value(item)}")
        elif isinstance(value, bool):
            lines.append(f"{key}: {'true' if value else 'false'}")
        else:
            lines.append(f"{key}: {yaml_value(value)}")
    lines.append("---")
    return "\n".join(lines)


def wiki(path: str, label: str | None = None) -> str:
    target = path[:-3] if path.endswith(".md") else path
    return f"[[{target}|{label}]]" if label else f"[[{target}]]"


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def write_note(root: Path, path: str, metadata: dict[str, object], body: str) -> None:
    note = root / (path if path.endswith(".md") else f"{path}.md")
    write_text(note, f"{frontmatter(metadata)}\n\n{body}")


def language_for(path: str) -> str:
    suffix = Path(path).suffix.lower()
    return {
        ".css": "css",
        ".feature": "gherkin",
        ".html": "html",
        ".js": "javascript",
        ".json": "json",
        ".md": "markdown",
        ".mjs": "javascript",
        ".proto": "protobuf",
        ".py": "python",
        ".rs": "rust",
        ".sh": "bash",
        ".toml": "toml",
        ".ts": "typescript",
        ".tsx": "tsx",
        ".yaml": "yaml",
        ".yml": "yaml",
    }.get(suffix, "")


def subsystem_for(path: str) -> tuple[str, str | None]:
    parts = PurePosixPath(path).parts
    if len(parts) >= 2 and parts[0] == "crates":
        crate = parts[1]
        if "catalog" in crate:
            return "Catalogs and lakehouse", crate
        if crate in {"sail-execution", "sail-physical-plan", "sail-physical-optimizer"}:
            return "Distributed execution", crate
        if crate in {"sail-plan", "sail-logical-plan", "sail-logical-optimizer"}:
            return "Planning and logical model", crate
        if crate in {"sail-spark-connect", "sail-flight", "sail-server", "sail-cli"}:
            return "Protocol front doors", crate
        if crate in {"sail-python", "sail-python-udf", "sail-pyarrow"}:
            return "Python and Arrow interop", crate
        if crate in {"sail-sql-parser", "sail-sql-analyzer", "sail-sql-macro", "sail-function"}:
            return "SQL and functions", crate
        if crate in {"sail-cache", "sail-object-store", "sail-data-source"}:
            return "Storage and cache", crate
        if crate in {"sail-delta-lake", "sail-iceberg"}:
            return "Lakehouse formats", crate
        if crate in {"sail-common", "sail-common-datafusion", "sail-session"}:
            return "Session and common runtime", crate
        return "Rust crates", crate
    if parts and parts[0] == "python":
        return "Python package", None
    if parts and parts[0] == "docs":
        return "Documentation", None
    if parts and parts[0] == "scripts":
        return "Developer scripts", None
    if parts and parts[0] == ".github":
        return "Project automation", None
    return "Repository root", None


def summary_for(path: str, subsystem: str, lines: list[str]) -> str:
    if path.endswith("Cargo.toml"):
        return f"Cargo manifest for {subsystem}."
    for line in lines[:80]:
        stripped = line.strip()
        if stripped.startswith("//!") or stripped.startswith("///"):
            return stripped.lstrip("/! ").strip()
        if stripped.startswith('"""') and len(stripped) > 3:
            return stripped.strip('" ')
        if stripped.startswith("# "):
            return stripped.lstrip("# ").strip()
    return f"Source file in the {subsystem} subsystem."


def should_include(path: Path, root: Path) -> bool:
    rel = path.relative_to(root)
    if any(part in SKIP_DIRS for part in rel.parts):
        return False
    if not path.is_file():
        return False
    if path.name in TEXT_NAMES:
        return True
    if path.suffix.lower() in TEXT_SUFFIXES:
        return True
    return False


def read_text(path: Path) -> str | None:
    if path.stat().st_size > 1_500_000:
        return None
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if b"\x00" in data:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return data.decode("utf-8", errors="replace")
        except Exception:
            return None


def code_note_path(path: str) -> str:
    safe = "/".join(clean_name(part, 120) for part in PurePosixPath(path).parts)
    return f"{VAULT_BOOK}/Code/{safe}.source"


def fragment_id(path: str, start: int, end: int, symbol: str) -> str:
    raw = f"{path}:{start}:{end}:{symbol}".encode("utf-8")
    return f"sail-frag-{hashlib.sha1(raw).hexdigest()[:12]}"


def extract_fragments(source: SourceFile) -> list[Fragment]:
    matches: list[tuple[int, str, str]] = []
    regex = RUST_SYMBOL_RE if source.language == "rust" else PY_SYMBOL_RE if source.language == "python" else None
    if regex:
        for index, line in enumerate(source.lines, start=1):
            match = regex.match(line)
            if not match:
                continue
            kind = match.group("kind").replace(" ", "-")
            name = match.groupdict().get("name") or "impl"
            matches.append((index, kind, name))
    elif source.language == "markdown":
        for index, line in enumerate(source.lines, start=1):
            match = MD_HEADING_RE.match(line)
            if match and len(match.group("marks")) <= 2:
                matches.append((index, "heading", clean_name(match.group("name"), 80)))

    if not matches and source.lines:
        matches.append((1, "file", Path(source.path).name))

    fragments: list[Fragment] = []
    for i, (start, kind, name) in enumerate(matches[:80]):
        next_start = matches[i + 1][0] if i + 1 < len(matches) else len(source.lines) + 1
        end = min(next_start - 1, start + 80, len(source.lines))
        fid = fragment_id(source.path, start, end, f"{kind}:{name}")
        heading = f"{fid}: {kind} {name}"
        fragments.append(Fragment(
            id=fid,
            source_path=source.path,
            note_path=f"{VAULT_BOOK}/Fragments/{fid}",
            code_note=source.note_path,
            heading=heading,
            symbol=name,
            kind=kind,
            language=source.language,
            subsystem=source.subsystem,
            crate=source.crate,
            start_line=start,
            end_line=end,
            summary=f"{kind} `{name}` in `{source.path}` lines {start}-{end}.",
        ))
    return fragments


def inventory_codebase(sail_root: Path) -> list[SourceFile]:
    files: list[SourceFile] = []
    for path in sorted(sail_root.rglob("*")):
        if not should_include(path, sail_root):
            continue
        text = read_text(path)
        if text is None:
            continue
        rel = path.relative_to(sail_root).as_posix()
        subsystem, crate = subsystem_for(rel)
        lines = text.splitlines()
        source = SourceFile(
            path=rel,
            absolute=path,
            note_path=code_note_path(rel),
            language=language_for(rel),
            subsystem=subsystem,
            crate=crate,
            lines=lines,
            summary=summary_for(rel, subsystem, lines),
        )
        source.fragments = extract_fragments(source)
        files.append(source)
    return files


def chapter_title(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("# "):
            return line.lstrip("# ").strip()
    return clean_name(path.stem)


def chapter_note_path(path: Path) -> str:
    return f"{VAULT_BOOK}/Chapters/{path.stem}"


def chapter_keywords(path: Path) -> set[str]:
    name = path.stem.lower()
    mapping = {
        "spark": {"Protocol front doors", "Planning and logical model"},
        "pyspark": {"Python and Arrow interop", "Python package"},
        "pyarrow": {"Python and Arrow interop"},
        "arrow": {"Python and Arrow interop", "Session and common runtime"},
        "datafusion": {"Planning and logical model", "Session and common runtime"},
        "physical": {"Distributed execution"},
        "drivers": {"Distributed execution"},
        "shuffle": {"Distributed execution", "Storage and cache"},
        "spec": {"Planning and logical model"},
        "functions": {"SQL and functions"},
        "catalogs": {"Catalogs and lakehouse", "Lakehouse formats"},
        "extension": {"Planning and logical model", "Rust crates"},
        "flight": {"Protocol front doors"},
        "custom": {"Planning and logical model", "Distributed execution"},
        "streaming": {"Distributed execution"},
        "testing": {"Developer scripts", "Project automation"},
        "feature": {"Rust crates", "SQL and functions", "Catalogs and lakehouse"},
        "roadmap": {"Rust crates", "Documentation"},
    }
    result: set[str] = set()
    for token, subsystems in mapping.items():
        if token in name:
            result.update(subsystems)
    return result or {"Rust crates"}


def choose_chapter_fragments(chapter: Path, fragments: list[Fragment], limit: int = 10) -> list[Fragment]:
    subsystems = chapter_keywords(chapter)
    selected = [f for f in fragments if f.subsystem in subsystems]
    selected.sort(key=lambda f: (f.crate or "", f.source_path, f.start_line))
    return selected[:limit]


def render_fragment_block(fragment: Fragment) -> str:
    payload = {
        "id": fragment.id,
        "codeNote": fragment.code_note,
        "heading": fragment.heading,
        "sourcePath": fragment.source_path,
        "startLine": fragment.start_line,
        "endLine": fragment.end_line,
    }
    return "```sail-fragment\n" + json.dumps(payload, ensure_ascii=True) + "\n```"


def render_chapter(path: Path, fragments: list[Fragment]) -> tuple[str, str]:
    title = chapter_title(path)
    text = path.read_text(encoding="utf-8").rstrip()
    related = choose_chapter_fragments(path, fragments)
    body = [text, "", "## Generated Code Fragment Index", ""]
    if related:
        body.append("These generated links open the collocated Sail codebase notes.")
        body.append("")
        for fragment in related:
            body.append(render_fragment_block(fragment))
            body.append("")
    else:
        body.append("No generated fragments were matched for this chapter.")
    return title, "\n".join(body)


def render_source_file(source: SourceFile) -> str:
    lines = [
        f"# {source.path}",
        "",
        f"- Subsystem: [[{VAULT_BOOK}/Subsystems/{clean_name(source.subsystem)}|{source.subsystem}]]",
    ]
    if source.crate:
        lines.append(f"- Crate: [[{VAULT_BOOK}/Crates/{source.crate}|{source.crate}]]")
    lines.extend([
        f"- Source path: `{source.path}`",
        f"- Lines: {len(source.lines)}",
        f"- Summary: {source.summary}",
        "",
        "## Extracted Fragments",
        "",
    ])
    for fragment in source.fragments[:80]:
        lines.append(f"- [[{fragment.note_path}|{fragment.symbol}]]: lines {fragment.start_line}-{fragment.end_line}")
    lines.extend(["", "## Full Source", "", f"```{source.language}"])
    lines.extend(source.lines)
    lines.append("```")
    return "\n".join(lines)


def render_fragment_note(fragment: Fragment, source: SourceFile) -> str:
    excerpt = source.lines[fragment.start_line - 1:fragment.end_line]
    lines = [
        f"# {fragment.symbol}",
        "",
        f"- Fragment ID: `{fragment.id}`",
        f"- Source file: [[{fragment.code_note}|{fragment.source_path}]]",
        f"- Lines: {fragment.start_line}-{fragment.end_line}",
        f"- Subsystem: [[{VAULT_BOOK}/Subsystems/{clean_name(fragment.subsystem)}|{fragment.subsystem}]]",
    ]
    if fragment.crate:
        lines.append(f"- Crate: [[{VAULT_BOOK}/Crates/{fragment.crate}|{fragment.crate}]]")
    lines.extend([
        "",
        render_fragment_block(fragment),
        "",
        "## Excerpt",
        "",
        f'<span id="{fragment.id}" class="sail-fragment-target"></span>',
        f"### {fragment.heading}",
        "",
        f"```{fragment.language}",
        *excerpt,
        "```",
    ])
    return "\n".join(lines)


def render_index(title: str, items: Iterable[tuple[str, str]]) -> str:
    lines = [f"# {title}", ""]
    for label, path in sorted(items):
        lines.append(f"- {wiki(path, label)}")
    return "\n".join(lines)


def render_vault_readme(manifest: dict[str, object]) -> str:
    return "\n".join([
        "# Sail Rust Book Obsidian Vault",
        "",
        "This vault is the Obsidian edition of *Sail Rust Book: The Rust, Arrow, and DataFusion Guide*.",
        "It packages the book text together with generated notes for the current Sail codebase.",
        "",
        "## Contents",
        "",
        f"- Book chapters: {manifest['chapter_count']}",
        f"- Sail code-file notes: {manifest['code_file_count']}",
        f"- Generated code fragments: {manifest['fragment_count']}",
        f"- Sail source commit: `{manifest['sail_commit']}`",
        f"- Book source commit: `{manifest['book_commit']}`",
        "",
        "## Start Here",
        "",
        "- Open `Home.md` in Obsidian.",
        "- Follow `Sail Rust Book/Book.md` for the generated chapter map.",
        "- Use `Sail Rust Book/Indices/Code Files.md` for source-file navigation.",
        "- Use `Sail Rust Book/Indices/Fragments.md` for extracted code fragments.",
        "",
        "## Fragment Navigation",
        "",
        "The vault includes a local Obsidian plugin named `sail-code-fragments`.",
        "When enabled, generated `sail-fragment` cards can open the related code-file note",
        "and highlight the selected fragment. The plugin is bundled inside `.obsidian/plugins/`",
        "and does not require a build step.",
        "",
        "## Data Ledgers",
        "",
        "Machine-readable ledgers live under `Sail Rust Book/_data/`:",
        "",
        "- `manifest.json` records the build inputs and counts.",
        "- `files.json` records included Sail source files.",
        "- `fragments.json` records fragment IDs and line ranges.",
        "- `symbols.json` records extracted Rust, Python, and Markdown symbols.",
        "- `links.json` records generated graph edges.",
        "- `units.jsonl` is the publishing compatibility ledger used by FirstPair.",
        "",
        "The vault is generated from source. Rebuild it from the book repository rather than",
        "hand-editing generated notes.",
    ])


def copy_plugin(output: Path) -> None:
    source = BOOK_ROOT_DIR / "obsidian-plugin" / "sail-code-fragments"
    target = output / ".obsidian" / "plugins" / "sail-code-fragments"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target)
    write_text(output / ".obsidian" / "community-plugins.json", json.dumps(["sail-code-fragments"], indent=2))


def build_vault(sail_root: Path, output: Path) -> dict[str, object]:
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)

    sail_commit = run_git(["rev-parse", "--short=8", "HEAD"], sail_root)
    book_commit = run_git(["rev-parse", "--short=8", "HEAD"], REPO_ROOT)
    sources = inventory_codebase(sail_root)
    fragments = [fragment for source in sources for fragment in source.fragments]

    write_note(output, "Home", {
        "type": "vault",
        "book": "Sail Rust Book",
        "sail_commit": sail_commit,
        "book_commit": book_commit,
    }, "\n".join([
        "# Sail Rust Book Vault",
        "",
        f"Open {wiki(f'{VAULT_BOOK}/Book', 'the book map')} to start.",
        "",
        "This generated Obsidian vault collocates the book text with the current Sail codebase.",
        "Use fragment cards to jump from explanatory text to highlighted code excerpts.",
    ]))

    chapter_paths = sorted((BOOK_ROOT_DIR / "sources").glob("[0-9][0-9]-*.md"))
    chapter_links: list[tuple[str, str]] = []
    for chapter in chapter_paths:
        title, body = render_chapter(chapter, fragments)
        note_path = chapter_note_path(chapter)
        chapter_links.append((title, note_path))
        write_note(output, note_path, {
            "type": "chapter",
            "source_file": chapter.relative_to(REPO_ROOT).as_posix(),
        }, body)

    write_note(output, f"{VAULT_BOOK}/Book", {
        "type": "book",
        "sail_commit": sail_commit,
        "book_commit": book_commit,
        "chapter_count": len(chapter_links),
        "code_file_count": len(sources),
        "fragment_count": len(fragments),
    }, "\n".join([
        "# Sail Rust Book",
        "",
        "## Chapters",
        "",
        *[f"- {wiki(path, title)}" for title, path in chapter_links],
        "",
        "## Codebase",
        "",
        f"- {wiki(f'{VAULT_BOOK}/Indices/Code Files', 'Code files')}",
        f"- {wiki(f'{VAULT_BOOK}/Indices/Fragments', 'Code fragments')}",
        f"- {wiki(f'{VAULT_BOOK}/Indices/Crates', 'Crates')}",
        f"- {wiki(f'{VAULT_BOOK}/Indices/Subsystems', 'Subsystems')}",
    ]))

    crate_items: dict[str, list[SourceFile]] = {}
    subsystem_items: dict[str, list[SourceFile]] = {}
    for source in sources:
        if source.crate:
            crate_items.setdefault(source.crate, []).append(source)
        subsystem_items.setdefault(source.subsystem, []).append(source)
        write_note(output, source.note_path, {
            "type": "code-file",
            "source_path": source.path,
            "language": source.language,
            "subsystem": source.subsystem,
            "crate": source.crate,
            "line_count": len(source.lines),
            "fragment_count": len(source.fragments),
            "sail_commit": sail_commit,
        }, render_source_file(source))

    sources_by_path = {source.path: source for source in sources}
    for fragment in fragments:
        source = sources_by_path[fragment.source_path]
        write_note(output, fragment.note_path, {
            "type": "code-fragment",
            "fragment_id": fragment.id,
            "source_path": fragment.source_path,
            "code_note": fragment.code_note,
            "language": fragment.language,
            "subsystem": fragment.subsystem,
            "crate": fragment.crate,
            "symbol": fragment.symbol,
            "kind": fragment.kind,
            "start_line": fragment.start_line,
            "end_line": fragment.end_line,
        }, render_fragment_note(fragment, source))

    for crate, crate_sources in crate_items.items():
        write_note(output, f"{VAULT_BOOK}/Crates/{crate}", {
            "type": "crate",
            "crate": crate,
            "file_count": len(crate_sources),
        }, render_index(crate, [(source.path, source.note_path) for source in crate_sources]))
    for subsystem, subsystem_sources in subsystem_items.items():
        write_note(output, f"{VAULT_BOOK}/Subsystems/{clean_name(subsystem)}", {
            "type": "subsystem",
            "subsystem": subsystem,
            "file_count": len(subsystem_sources),
        }, render_index(subsystem, [(source.path, source.note_path) for source in subsystem_sources]))

    write_note(output, f"{VAULT_BOOK}/Indices/Code Files", {"type": "index"}, render_index(
        "Code Files", [(source.path, source.note_path) for source in sources]
    ))
    write_note(output, f"{VAULT_BOOK}/Indices/Fragments", {"type": "index"}, render_index(
        "Code Fragments", [(f"{fragment.id}: {fragment.symbol}", fragment.note_path) for fragment in fragments]
    ))
    write_note(output, f"{VAULT_BOOK}/Indices/Crates", {"type": "index"}, render_index(
        "Crates", [(crate, f"{VAULT_BOOK}/Crates/{crate}") for crate in crate_items]
    ))
    write_note(output, f"{VAULT_BOOK}/Indices/Subsystems", {"type": "index"}, render_index(
        "Subsystems", [(name, f"{VAULT_BOOK}/Subsystems/{clean_name(name)}") for name in subsystem_items]
    ))

    data_dir = output / VAULT_BOOK / "_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    files_json = [
        {
            "path": source.path,
            "note_path": source.note_path,
            "language": source.language,
            "subsystem": source.subsystem,
            "crate": source.crate,
            "line_count": len(source.lines),
            "summary": source.summary,
            "fragments": [fragment.id for fragment in source.fragments],
        }
        for source in sources
    ]
    fragments_json = [
        {
            "id": fragment.id,
            "source_path": fragment.source_path,
            "note_path": fragment.note_path,
            "code_note": fragment.code_note,
            "heading": fragment.heading,
            "symbol": fragment.symbol,
            "kind": fragment.kind,
            "language": fragment.language,
            "subsystem": fragment.subsystem,
            "crate": fragment.crate,
            "start_line": fragment.start_line,
            "end_line": fragment.end_line,
            "summary": fragment.summary,
        }
        for fragment in fragments
    ]
    links_json = []
    for source in sources:
        for fragment in source.fragments:
            links_json.append({"from": source.note_path, "to": fragment.note_path, "type": "file-fragment"})
            links_json.append({"from": fragment.note_path, "to": source.note_path, "type": "fragment-file"})
    symbols_json = [
        {
            "name": fragment.symbol,
            "kind": fragment.kind,
            "fragment_id": fragment.id,
            "source_path": fragment.source_path,
            "start_line": fragment.start_line,
            "end_line": fragment.end_line,
        }
        for fragment in fragments
    ]
    units = (
        [
            {
                "id": f"chapter:{path}",
                "kind": "chapter",
                "note_path": path,
                "title": title,
            }
            for title, path in chapter_links
        ]
        + [
            {
                "id": f"file:{source.path}",
                "kind": "code-file",
                "note_path": source.note_path,
                "source_path": source.path,
                "language": source.language,
                "subsystem": source.subsystem,
                "crate": source.crate,
            }
            for source in sources
        ]
        + [
            {
                "id": fragment.id,
                "kind": "code-fragment",
                "note_path": fragment.note_path,
                "source_path": fragment.source_path,
                "code_note": fragment.code_note,
                "symbol": fragment.symbol,
                "start_line": fragment.start_line,
                "end_line": fragment.end_line,
            }
            for fragment in fragments
        ]
    )

    for name, payload in {
        "files.json": files_json,
        "fragments.json": fragments_json,
        "symbols.json": symbols_json,
        "links.json": links_json,
    }.items():
        write_text(data_dir / name, json.dumps(payload, indent=2, ensure_ascii=True))
    write_text(
        data_dir / "units.jsonl",
        "\n".join(json.dumps(unit, ensure_ascii=True) for unit in units),
    )

    copy_plugin(output)

    manifest = {
        "book": "Sail Rust Book",
        "sail_root": str(sail_root),
        "sail_commit": sail_commit,
        "book_commit": book_commit,
        "chapter_count": len(chapter_links),
        "code_file_count": len(sources),
        "fragment_count": len(fragments),
        "output": str(output),
    }
    write_text(data_dir / "manifest.json", json.dumps(manifest, indent=2, ensure_ascii=True))
    write_text(output / "README.md", render_vault_readme(manifest))
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sail-root", type=Path, default=DEFAULT_SAIL_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    manifest = build_vault(args.sail_root.resolve(), args.output.resolve())
    print(json.dumps(manifest, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
