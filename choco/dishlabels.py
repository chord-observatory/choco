"""kotekan's ``dish_inputs`` table and the per-element label layout.

Shared by the web process (the PDB map cross-check) and by every job that
names feeds (bffs, eigencal, waterfall, skymap), so it is **stdlib only**.
The label layout is the one fact all of them must agree on: kotekan's
2026-08 configs name each *dish* once (``A1``) and lay the element axis
out as [P][D] — ``element = dish_idx + pol * num_dishes`` — so per-element
labels are derived as label + ``X``/``Y``.  The pre-2026-08 layout named
every element (``A1X``, ``d0_pA``) with an element ordering that turned
out to be wrong, which is why every consumer refuses it.

N² files are labelled per *element* since kotekan chord.2021.10+988
(acquisitions from 2026-09-11 on): ``index_map/label`` has one entry per
element of the file's own axis — a full frame's 128 or a compact
(``n2_layout: DishInputs``) frame's 48 alike — with ``index_map/pol``
alongside.  Two spellings exist on disk: dish label + ``p1``/``p2`` for
polarization 0/1 (+988 up to kotekan PR #1695), and dish label + the
polarization *name* ``X``/``Y`` (PR #1695, 2026-09-22; in the data from
2026-10-01).  :func:`file_element_labels` turns either into choco's names
(``B4p1`` → ``B4X``, ``A01X`` → ``A01X``) and is the only accepted file
layout: per-dish tables and the pre-2026-08 per-element tables are
refused, never expanded or guessed.  The ``X``/``Y`` spelling is the
pre-2026-08 spelling too, so it is accepted only when ``index_map/pol``
— written since +988 — is there to vouch for it.

A compact (``DishInputs``) frame's element axis is the table's *connected*
dishes — every row whose ``type`` is not ``Missing`` — in that same [P][D]
order; :func:`connected_elements` is the one copy of that rule.
"""

from __future__ import annotations

import re

#: kotekan pads unpopulated dish_inputs slots with this label.
PLACEHOLDER_LABEL = "Missing"

#: A polarization marker in the label text (``A1X``, ``d0_pA``) means the
#: pre-2026-08 per-element layout.  A bare dish label (``A1``,
#: ``CHORD-A01``) is the per-dish layout.
PER_ELEMENT_LABEL = re.compile(r"\d[XY]$|_p\w$")

#: Per-element suffix by polarization index: 0 = X, 1 = Y.  Matches the
#: old per-element labels, so label-keyed hardware maps kept working
#: across the layout change.
POL_SUFFIXES = "XY"

#: How kotekan's N² writer spelled an element from chord.2021.10+988 up
#: to PR #1695: the dish label followed by ``p`` and the 1-based
#: polarization number (``B4p1``, ``RFIA1p2``, ``Fakep1``).  Greedy, so
#: the *last* ``p<n>`` is the suffix.
FILE_ELEMENT_LABEL = re.compile(r"^(?P<dish>.+)p(?P<pol>[1-9]\d*)$")

#: How it spells one since PR #1695 (2026-09-22): the dish label followed
#: by the polarization *name* (``A01X``, ``RFIB4Y``) — the same text as
#: choco's own names, and as the pre-2026-08 per-element config labels,
#: which is why :func:`file_element_labels` takes it only on the word of
#: ``index_map/pol``.  kotekan names at most two polarizations.
FILE_ELEMENT_LABEL_XY = re.compile(
    r"^(?P<dish>.+)(?P<pol>[" + POL_SUFFIXES + r"])$")


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
    """True when a config table's *labels* use the pre-2026-08 convention.

    The two table layouts are structurally identical — same shape, same
    label count — so the label text is the only distinguishing mark.
    """
    return any(PER_ELEMENT_LABEL.search(str(label)) for label in labels)


def pol_suffix(pol: int) -> str:
    """choco's per-element suffix for polarization index *pol*: X, Y, P2, …"""
    pol = int(pol)
    return POL_SUFFIXES[pol] if 0 <= pol < len(POL_SUFFIXES) else f"P{pol}"


def expand_dish_labels(dish_labels, num_polarizations: int = 2) -> list[str]:
    """Per-element labels from a config's per-dish labels, in [P][D] order.

    Mirrors ``CHORDTelescope::encode_station_id``: all of polarization 0
    (X) first, then polarization 1 (Y) — ``A1`` at dish index *i* expands
    to ``A1X`` at element *i* and ``A1Y`` at element ``i + num_dishes``.
    Placeholder dishes expand like any other (``MissingX`` / ``MissingY``);
    duplicates are the caller's problem.
    """
    labels = [str(label) for label in dish_labels]   # may be a generator
    npol = int(num_polarizations)
    if npol == 1:
        # One polarization: the dish is the element; no suffix to add.
        return labels
    return [f"{label}{pol_suffix(pol)}" for pol in range(npol) for label in labels]


def file_element_labels(labels, num_elements: int | None = None,
                        pol=None) -> list[str]:
    """choco's per-element labels from an N² file's ``index_map/label``.

    *labels* is the file's table as written, one entry per element of the
    file's own axis in the file's own order, so the result needs no
    expansion and no lookup — position for position, in either spelling
    kotekan has used: ``B4p1`` → ``B4X``, ``B4p2`` → ``B4Y``
    (:func:`pol_suffix`; chord.2021.10+988 to PR #1695) and ``A01X`` →
    ``A01X`` (PR #1695 on, the polarization name as the suffix).

    Raises ``ValueError`` — the caller decides whether that is a degraded
    run or "no labels" — when the table is not that layout: a count that
    disagrees with *num_elements* (a per-dish table, or the whole
    telescope table on a compact axis), a label with neither suffix (a
    per-dish ``A1`` or a pre-2026-08 ``d0_pA``), a suffix that disagrees
    with the file's ``index_map/pol`` when *pol* is given, or an ``X``/``Y``
    suffix with no *pol* at all: by text alone that is the pre-2026-08
    per-element layout, whose element order was wrong, and only
    ``index_map/pol`` (written since +988) tells the two apart.  Nothing
    is guessed: a wrong name on an element is worse than none.
    """
    labels = [str(label) for label in labels]
    if num_elements is not None and len(labels) != int(num_elements):
        raise ValueError(
            f"index_map/label has {len(labels)} entries for {int(num_elements)} "
            "elements — not a per-element label table")
    if pol is not None:
        pol = [int(p) for p in pol]
        if len(pol) != len(labels):
            raise ValueError(
                f"index_map/pol has {len(pol)} entries for {len(labels)} labels")
    out: list[str] = []
    for i, label in enumerate(labels):
        m = FILE_ELEMENT_LABEL.match(label)
        if m is not None:
            dish, p = m["dish"], int(m["pol"]) - 1
        else:
            m = FILE_ELEMENT_LABEL_XY.match(label)
            if m is None:
                raise ValueError(
                    f"label {label!r} (element {i}) carries no polarization "
                    "suffix (p<n> or X/Y) — not kotekan's per-element label "
                    "layout")
            if pol is None:
                raise ValueError(
                    f"label {label!r} (element {i}) ends in a polarization "
                    "name but the file has no index_map/pol to vouch for it — "
                    "the pre-2026-08 per-element layout, whose element order "
                    "was wrong")
            dish, p = m["dish"], POL_SUFFIXES.index(m["pol"])
        if pol is not None and pol[i] != p:
            raise ValueError(
                f"label {label!r} (element {i}) reads as polarization {p} but "
                f"index_map/pol says {pol[i]}")
        out.append(dish + pol_suffix(p))
    return out


def connected_elements(dish_inputs, num_polarizations: int = 2) -> list[int]:
    """Fiducial element indices of a compact ``DishInputs`` axis, in order.

    Mirrors ``CHORDTelescope::get_connected_elements`` (kotekan develop,
    PR #1658): walk the full [P][D] element axis and keep every element
    whose dish ``type`` is not ``Missing``, so compact element *c* is
    fiducial element ``result[c]``.  kotekan's ``DishInputs`` descriptor
    does not carry this list — the selection is implied by the
    ``dish_inputs`` table it was configured with, which is the table
    choco pushed — so a consumer with that table derives it here.  The
    caller must still check the length against the descriptor's
    ``num_elements``: a disagreement means the rule has drifted, and a
    wrong identity on an element is worse than none.
    """
    npol = int(num_polarizations)
    if npol < 1:
        raise ValueError(f"num_polarizations must be positive, got {npol}")
    num_dishes = len(dish_inputs)
    connected = [
        dish for dish, row in enumerate(dish_inputs)
        if not (isinstance(row, dict)
                and str(row.get("type")) == PLACEHOLDER_LABEL)
    ]
    return [dish + pol * num_dishes for pol in range(npol) for dish in connected]
