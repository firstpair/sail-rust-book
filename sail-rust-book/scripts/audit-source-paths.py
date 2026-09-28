#!/usr/bin/env python3
"""Resolve manuscript code paths against the two pinned source trees.

This checks existence, not excerpt fidelity or the truth of surrounding prose.
Use --strict to fail when an explicit path exists in neither snapshot.
"""
import argparse
import datetime
import json
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sail', type=Path, required=True)
    parser.add_argument('--strict', action='store_true')
    parser.add_argument('--output', type=Path,
                        help='write the report here instead of changing the source tree')
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1] / 'sources'
    pins = json.loads((source / 'source-revisions.json').read_text())
    trees = {}
    for name in ('upstream', 'extension'):
        commit = pins[name]['commit']
        trees[name] = set(subprocess.check_output(
            ['git', 'ls-tree', '-r', '--name-only', commit], cwd=args.sail, text=True).splitlines())
    checks = []
    for chapter in sorted(source.glob('[0-9][0-9]-*.md')):
        paths = set(re.findall(r'`((?:crates|examples|python)/[^`\n]+)`', chapter.read_text()))
        for path in sorted(paths):
            if any(token in path for token in ('*', ' ', '<', '>', '#', '::')):
                continue
            matches = {name: path in files or any(f.startswith(path.rstrip('/') + '/') for f in files)
                       for name, files in trees.items()}
            checks.append(dict(chapter=chapter.name, path=path, **matches))
    missing = [x for x in checks if not (x['upstream'] or x['extension'])]
    result = dict(recorded_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  revisions=pins, checks=checks, missing_count=len(missing),
                  boundary='explicit inline code-path existence only; not excerpt or prose validation')
    output = args.output or source / 'source-path-audit.json'
    output.write_text(json.dumps(result, indent=2) + '\n')
    for item in missing:
        print(item['chapter'], item['path'])
    print(f'{len(checks)} references checked; {len(missing)} unresolved')
    if args.strict and missing:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
