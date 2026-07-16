"""Helpers shared by the external-service sources (power, fpga, rfi)."""

from __future__ import annotations

import csv
import json
import re
import ssl
import urllib.request
from pathlib import Path

import numpy as np

_METRIC_LINE = re.compile(r'(\w+)\{([^}]*)\}\s+([\d.eE+-]+)')
_LABEL = re.compile(r'(\w+)="([^"]*)"')


def iter_metrics(text: str):
    """Yield ``(name, labels, value)`` for each labelled Prometheus sample line.

    Unlabelled samples and comment lines are skipped; ``labels`` is a dict of
    the sample's label values.
    """
    for name, labelstr, value in _METRIC_LINE.findall(text):
        yield name, dict(_LABEL.findall(labelstr)), float(value)


def load_map(path: str | Path, key) -> dict:
    """Load a hardware -> correlator-input map from CSV.

    ``key(row)`` builds the hardware-coordinate tuple from a CSV row; the value
    is the row's ``correlator_input``. Returns ``{key(row): correlator_input}``.
    """
    with open(path, newline="") as f:
        return {key(row): row["correlator_input"].strip() for row in csv.DictReader(f)}


def choco_group_nodes(choco_url: str, group: str,
                      timeout: float = 10.0) -> list[dict]:
    """Node entries (``{name, host, port, started}``) of one choco group.

    Read-only GET of choco's ``/api/nodes`` registry endpoint, so choco's
    ``nodes.yaml`` stays the single source of truth for which kotekan
    instances exist. Auth is bypassed for localhost callers and choco's
    self-signed certificate goes unverified — same rules as the flag send
    in ``bffs.py``.
    """
    url = choco_url.rstrip("/") + "/api/nodes"
    ctx = None
    if url.startswith("https:"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(url, timeout=timeout, context=ctx) as resp:
        data = json.loads(resp.read())
    return list((data.get("groups") or {}).get(group) or [])


def project(input_good: dict[str, bool], labels: np.ndarray) -> np.ndarray:
    """Project a source's per-input verdict onto the feed axis (``labels``).

    Returns a good-mask over ``labels``. Feeds the source has no entry for
    default to good — a source only flags the feeds it actually covers.
    """
    return np.array([input_good.get(str(lbl), True) for lbl in labels], dtype=bool)
