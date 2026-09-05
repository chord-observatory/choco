"""kotekan's ``dish_inputs`` table and the per-element label layout.

Shared by the web process (the PDB map cross-check) and by every job that
names feeds (bffs, eigencal, waterfall, skymap), so it is **stdlib only**.
The label layout is the one fact all of them must agree on: kotekan's
2026-08 configs name each *dish* once (``A1``) and lay the element axis
out as [P][D] — ``element = dish_idx + pol * num_dishes`` — so per-element
labels are derived as label + ``X``/``Y``.  The pre-2026-08 layout named
every element (``A1X``, ``d0_pA``) with an element ordering that turned
out to be wrong, which is why every consumer refuses it.
"""

from __future__ import annotations

import re

#: kotekan pads unpopulated dish_inputs slots with this label.
PLACEHOLDER_LABEL = "Fake"

#: A polarization marker in the label text (``A1X``, ``d0_pA``) means the
#: pre-2026-08 per-element layout.  A bare dish label (``A1``,
#: ``CHORD-A01``) is the per-dish layout.
PER_ELEMENT_LABEL = re.compile(r"\d[XY]$|_p\w$")

#: Per-element suffix by polarization index: 0 = X, 1 = Y.  Matches the
#: old per-element labels, so label-keyed hardware maps kept working
#: across the layout change.
POL_SUFFIXES = "XY"


def find_key(obj, key):
    """The first value of *key* anywhere in a nested dict/list, else None.

    Depth-first, the current mapping before its children, so a top-level
    value wins.  kotekan configs nest the interesting keys inside blocks
    whose names vary between config generations.
    """
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for value in obj.values():
            found = find_key(value, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = find_key(value, key)
            if found is not None:
                return found
    return None


def find_dish_inputs(config) -> list | None:
    """The first non-empty ``dish_inputs`` list in a rendered config, or None."""
    if isinstance(config, dict):
        value = config.get("dish_inputs")
        if isinstance(value, list) and value:
            return value
        for child in config.values():
            found = find_dish_inputs(child)
            if found is not None:
                return found
    return None


def labels_are_per_element(labels) -> bool:
    """True when *labels* use the pre-2026-08 per-element convention.

    The two layouts are structurally identical — same table shape, same
    label count — so the label text is the only distinguishing mark.
    """
    return any(PER_ELEMENT_LABEL.search(str(label)) for label in labels)


def num_polarizations(n_labels: int, num_elements: int) -> int:
    """Polarizations implied by an N² file's ``num_elements`` attribute.

    The element axis is ``num_elements`` long and the label list one
    entry per dish, so the ratio is the polarization count; anything that
    does not divide evenly (or a missing attribute) means the CHORD
    default of two.
    """
    if n_labels and num_elements >= n_labels and num_elements % n_labels == 0:
        return num_elements // n_labels
    return 2


def expand_dish_labels(dish_labels, num_polarizations: int = 2) -> list[str]:
    """Per-element labels from per-dish labels, in the CHORD [P][D] order.

    Mirrors ``CHORDTelescope::encode_station_id``: all of polarization 0
    (X) first, then polarization 1 (Y) — ``A1`` at dish index *i* expands
    to ``A1X`` at element *i* and ``A1Y`` at element ``i + num_dishes``.
    Placeholder dishes expand like any other (``FakeX`` / ``FakeY``);
    duplicates are the caller's problem.
    """
    labels = [str(label) for label in dish_labels]   # may be a generator
    npol = int(num_polarizations)
    if npol == 1:
        # One polarization: the dish is the element; no suffix to add.
        return labels
    out: list[str] = []
    for pol in range(npol):
        suffix = POL_SUFFIXES[pol] if pol < len(POL_SUFFIXES) else f"P{pol}"
        out.extend(f"{label}{suffix}" for label in labels)
    return out
