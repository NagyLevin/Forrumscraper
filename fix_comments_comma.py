#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import re
import sys

# Csak ezt az egy hibát javítja:
# "comments": [
# ,
#   {
#
# -> "comments": [
#      {

BROKEN_COMMENTS_COMMA_RE = re.compile(
    r'("comments"\s*:\s*\[\s*)\r?\n\s*,(\s*\{)',
    re.MULTILINE,
)


def fix_extra_comma_in_json_file(path: Path) -> bool:
    try:
        original = path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"[HIBA] Nem sikerült beolvasni: {path} | {e}")
        return False

    fixed = BROKEN_COMMENTS_COMMA_RE.sub(r"\1\n\2", original, count=1)

    if fixed == original:
        print(f"[OK] Nem kellett javítani: {path.name}")
        return False

    try:
        path.write_text(fixed, encoding="utf-8")
        print(f"[JAVÍTVA] {path.name}")
        return True
    except Exception as e:
        print(f"[HIBA] Nem sikerült menteni: {path} | {e}")
        return False


def main() -> None:
    current_dir = Path.cwd()
    json_files = sorted(current_dir.glob("*.json"))

    if not json_files:
        print("[INFO] A jelenlegi mappában nincs .json fájl.")
        sys.exit(0)

    fixed_count = 0

    for json_file in json_files:
        if fix_extra_comma_in_json_file(json_file):
            fixed_count += 1

    print()
    print(f"[INFO] Feldolgozott JSON fájlok száma: {len(json_files)}")
    print(f"[INFO] Javított fájlok száma: {fixed_count}")


if __name__ == "__main__":
    main()

    #python3 fix_comments_comma.py
    #place in folder with json files to make it work