"""manual source — feeds an operator listed in a watched override file."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import yaml


def mask(src: dict, labels: np.ndarray, kotekan_file: str) -> np.ndarray:
    """A feed is bad iff listed in the override file ``src['path']``.

    File format (YAML or JSON): ``bad_inputs: ["f0017", ...]`` (or a bare list);
    a missing file means no overrides. ``kotekan_file`` is unused.
    """
    p = Path(src["path"])
    data = yaml.safe_load(p.read_text() or "") if p.exists() else None
    if isinstance(data, dict):
        data = data.get("bad_inputs") or []
    bad = {str(x) for x in data} if isinstance(data, list) else set()
    return np.array([lbl not in bad for lbl in labels], dtype=bool)
