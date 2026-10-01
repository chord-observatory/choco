"""Helpers shared by the sources: metrics parsing, maps, choco reads, reports."""

from __future__ import annotations

import csv
import re
from pathlib import Path

import numpy as np

from choco.jobclient import get_json

_METRIC_LINE = re.compile(r'(\w+)\{([^}]*)\}\s+([\d.eE+-]+)')
_LABEL = re.compile(r'(\w+)="([^"]*)"')
# A full Prometheus text-format sample: name, optional {labels}, value and
# the optional trailing timestamp (ms since the epoch) kotekan emits.
_SAMPLE_LINE = re.compile(
    r'^(\w+)(?:\{([^}]*)\})?[ \t]+(\S+)(?:[ \t]+(-?\d+))?[ \t]*$', re.M)


def iter_metrics(text: str):
    """Yield ``(name, labels, value)`` for each labelled Prometheus sample line.

    Unlabelled samples and comment lines are skipped; ``labels`` is a dict of
    the sample's label values.
    """
    for name, labelstr, value in _METRIC_LINE.findall(text):
        yield name, dict(_LABEL.findall(labelstr)), float(value)


def iter_samples(text: str):
    """Yield ``(name, labels, value, timestamp_ms)`` for every sample line.

    Like :func:`iter_metrics` but keeps unlabelled samples and the
    optional trailing timestamp (``None`` when the exporter wrote none).
    kotekan stamps each gauge with the time it was last set, which is
    how a consumer can tell a live gauge from one whose stage has
    stopped receiving frames.
    """
    for name, labelstr, value, ts in _SAMPLE_LINE.findall(text):
        try:
            v = float(value)
        except ValueError:
            continue
        yield name, dict(_LABEL.findall(labelstr or "")), v, (int(ts) if ts else None)


def report(status: str = "ok", reason: str | None = None,
           n_measured: int | None = None, **detail) -> dict:
    """A source's per-run report, returned alongside its mask.

    ``status`` is ``ok`` (the source measured what it covers) or
    ``degraded`` (it ran but abstained, or covered less than configured:
    the run exits 2 and ``reason`` is shown on choco's BFFS page).  A
    source never reports ``skipped`` — the core sets that when it does
    not run a source at all.  ``n_measured`` counts the feeds the source
    actually judged this run (``None`` when that has no meaning), so a
    source that ran and flagged nothing can be told from one that had
    nothing to measure.  ``detail`` is free-form and lands in the run
    file for the page; keep it small and JSON-serialisable.
    """
    if status not in ("ok", "degraded"):
        raise ValueError(f"source report status must be ok or degraded, not {status!r}")
    return {"status": status, "reason": reason, "n_measured": n_measured,
            "detail": dict(detail)}


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
    """Node entries (``{name, host, port, started, status}``) of one choco group.

    Read-only GET of choco's ``/api/nodes`` registry endpoint, so choco's
    ``nodes.yaml`` stays the single source of truth for which kotekan
    instances exist.  ``status`` is the sync loop's last probe of the node
    (``started`` / ``idle`` / ``down`` / ...; absent from older chocos).
    Auth is bypassed for localhost callers and choco's self-signed
    certificate goes unverified — same rules as the flag send in
    ``bffs.py``.
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
