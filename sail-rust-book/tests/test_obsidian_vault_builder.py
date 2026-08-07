from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build-obsidian-vault.py"
SPEC = importlib.util.spec_from_file_location("sail_obsidian_builder", SCRIPT)
assert SPEC and SPEC.loader
BUILDER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = BUILDER
SPEC.loader.exec_module(BUILDER)


def test_source_normalization_preserves_lines_and_removes_tabs() -> None:
    lines = BUILDER.normalized_source_lines("first\n\tsecond\nthird\tvalue\n")

    assert len(lines) == 3
    assert lines[0] == "first"
    assert lines[1] == "    second"
    assert "\t" not in "\n".join(lines)
