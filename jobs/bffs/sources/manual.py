"""manual source — feeds an operator listed in a watched override file."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import yaml

from choco.jobclient import MANUAL_OVERRIDES_NAME

from .common import report


def override_path(src: dict) -> Path:
    """The override file: ``src['path']`` if set, else
    ``manual_overrides.yaml`` in the job's state directory — the file
    choco's BFFS page edits when the element grid is clicked."""
    if src.get("path"):
        return Path(src["path"])
    if src.get("state_dir"):
        return Path(src["state_dir"]) / MANUAL_OVERRIDES_NAME
    raise ValueError("manual source: no 'path' and no state directory to default into")


def mask(src: dict, labels: np.ndarray, kotekan_file: str):
    """A feed is bad iff listed in the override file (:func:`override_path`).

    File format (YAML or JSON): ``bad_inputs: ["f0017", ...]`` (or a bare list);
    a missing file means no overrides. ``kotekan_file`` is unused.  The
    report notes listed labels that are not on the element axis (a typo,
    or a label from an older ``dish_inputs`` table): they flag nothing.
    """
    p = override_path(src)
    exists = p.exists()
    data = yaml.safe_load(p.read_text() or "") if exists else None
    if isinstance(data, dict):
        data = data.get("bad_inputs") or []
    bad = {str(x) for x in data} if isinstance(data, list) else set()
    axis = [str(lbl) for lbl in labels]
    good = np.array([lbl not in bad for lbl in axis], dtype=bool)
    detail = {"path": str(p), "exists": exists, "n_listed": len(bad)}
    unknown = sorted(bad.difference(axis))
    if unknown:
        detail["not_on_axis"] = unknown[:10]
    return good, report("ok", n_measured=len(axis), **detail)
