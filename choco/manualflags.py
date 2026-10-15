"""The bffs manual override file: what choco's BFFS page edits and the
job's ``manual`` source reads, parsed by one rule on both sides.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from .jobclient import write_text_atomic

MANUAL_HEADER = (
    "# bffs manual overrides — feeds an operator marked bad.\n"
    "# Edited by choco's BFFS page (the element grid) and by hand; keys other\n"
    "# than bad_inputs are kept across edits, comments are not.\n"
)


def read_manual_flags(path: Path | str | None) -> set[str] | None:
    """The labels a bffs manual override file lists as bad.

    The file is what ``jobs/bffs/sources/manual.py`` reads: YAML (or
    JSON) with a ``bad_inputs`` list, or a bare list.  Returns ``None``
    when there is no file (no overrides), and raises ``ValueError`` on
    a file that cannot be parsed or has another shape — the page shows
    that rather than guessing, and a toggle refuses to overwrite it.
    """
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    try:
        data = yaml.safe_load(p.read_text() or "")
    except (OSError, yaml.YAMLError) as e:
        raise ValueError(f"{p}: {e}") from e
    if data is None:
        return set()
    if isinstance(data, dict):
        data = data.get("bad_inputs") or []
    if not isinstance(data, list):
        raise ValueError(f"{p}: expected a bad_inputs list, found {type(data).__name__}")
    return {str(x) for x in data}


def toggle_manual_flag(path: Path | str, label: str) -> bool:
    """Add *label* to the override file's ``bad_inputs``, or remove it if
    it is there.  Returns ``True`` when the label is now flagged.

    Other top-level keys of a dict-shaped file are preserved; a bare-list
    file is rewritten in the dict shape.  The write is a temp file plus
    rename in the same directory, so the bffs job (which reads the file
    every run) never sees a torn file.  A missing parent directory is
    created.  Raises ``ValueError`` on an unparseable file (nothing is
    written) and ``OSError`` on a failed write.
    """
    p = Path(path)
    existing = read_manual_flags(p)
    doc: dict = {}
    if existing is not None:
        data = yaml.safe_load(p.read_text() or "")
        if isinstance(data, dict):
            doc = dict(data)
    bad = set(existing or ())
    flagged = label not in bad
    if flagged:
        bad.add(label)
    else:
        bad.discard(label)
    doc["bad_inputs"] = sorted(bad)
    # 0644 (atomic_output's default): the bffs job, another user, reads it
    write_text_atomic(p, MANUAL_HEADER + yaml.safe_dump(
        doc, default_flow_style=False, sort_keys=False, allow_unicode=True))
    return flagged
