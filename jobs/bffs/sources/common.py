"""Helpers shared by the external-service sources (power, fpga, rfi)."""

from __future__ import annotations

import csv
import re
from pathlib import Path

import numpy as np

from choco.jobclient import get_json

_METRIC_LINE = re.compile(r'(\w+)\{([^}]*)\}\s+([\d.eE+-]+)')
_LABEL = re.compile(r'(\w+)="([^"]*)"')


def iter_metrics(text: str):
    """Yield ``(name, labels, value)`` for each labelled Prometheus sample line.

    Unlabelled samples and comment lines are skipped; ``labels`` is a dict of
    the sample's label values.
    """
    for name, labelstr, value in _METRIC_LINE.findall(text):
        yield name, dict(_LABEL.findall(labelstr)), float(value)


# The correlator-input column has gone by a few names; choco's master
# PDB table calls it dish_input (it holds kotekan dish_inputs labels).
_INPUT_COLUMNS = ("correlator_input", "dish_input", "label")


def load_map(path: str | Path, key) -> dict:
    """Load a hardware -> correlator-input map from CSV.

    ``key(row)`` builds the hardware-coordinate tuple from a CSV row; the value
    is the row's correlator input. Returns ``{key(row): correlator_input}``.
    """
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        fields = [(f or "").strip() for f in (reader.fieldnames or [])]
        column = next((c for c in _INPUT_COLUMNS if c in fields), None)
        if column is None:
            raise ValueError(
                f"{path}: no correlator-input column "
                f"(one of {', '.join(_INPUT_COLUMNS)})")
        return {key(row): row[column].strip() for row in reader}


def choco_group_nodes(choco_url: str, group: str,
                      timeout: float = 10.0) -> list[dict]:
    """Node entries (``{name, host, port, started}``) of one choco group.

    Read-only GET of choco's ``/api/nodes`` registry endpoint, so choco's
    ``nodes.yaml`` stays the single source of truth for which kotekan
    instances exist. Auth is bypassed for localhost callers and choco's
    self-signed certificate goes unverified — same rules as the flag send
    in ``bffs.py``.
    """
    data = _choco_get(choco_url, "/api/nodes", timeout)
    return list((data.get("groups") or {}).get(group) or [])


def choco_group_config(choco_url: str, group: str,
                       timeout: float = 10.0) -> dict:
    """A sample node's desired kotekan config for one choco group.

    Read-only GET of choco's ``/api/config/<group>``.  The config's
    ``dish_inputs`` table is what kotekan's bad-input mask is indexed
    against, so it is the naming authority for feed labels.
    """
    return _choco_get(choco_url, f"/api/config/{group}", timeout)


def choco_pdb_map(choco_url: str, timeout: float = 10.0) -> dict:
    """choco's master PDB channel map (``GET /api/pdb/map``).

    The dish-input <-> power-channel wiring lives in one CSV beside
    choco's ``nodes.yaml``; reading it through choco keeps that file the
    single authority instead of every consumer vendoring a copy.  The
    payload also carries choco's ``check`` — the same comparison against
    kotekan's ``dish_inputs`` that the PDB page shows.
    """
    return _choco_get(choco_url, "/api/pdb/map", timeout)


def _choco_get(choco_url: str, path: str, timeout: float) -> dict:
    """GET a choco JSON endpoint (localhost auth bypass, unverified TLS)."""
    return get_json(choco_url, path, timeout=timeout)


def project(input_good: dict[str, bool], labels: np.ndarray) -> np.ndarray:
    """Project a source's per-input verdict onto the feed axis (``labels``).

    Returns a good-mask over ``labels``. Feeds the source has no entry for
    default to good — a source only flags the feeds it actually covers.
    """
    return np.array([input_good.get(str(lbl), True) for lbl in labels], dtype=bool)
