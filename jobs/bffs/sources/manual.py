"""manual source — feeds an operator listed in a watched override file."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import yaml

from .common import report


def mask(src: dict, labels: np.ndarray, kotekan_file: str):
    """A feed is bad iff listed in the override file ``src['path']``.

    File format (YAML or JSON): ``bad_inputs: ["f0017", ...]`` (or a bare list);
    a missing file means no overrides. ``kotekan_file`` is unused.  The
    report notes listed labels that are not on the element axis (a typo,
    or a label from an older ``dish_inputs`` table): they flag nothing.
    """
    p = Path(src["path"])
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
