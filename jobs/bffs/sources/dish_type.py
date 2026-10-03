"""dish-type source — elements whose kotekan dish type is never a feed.

Inventory, not measurement: the kotekan config's ``dish_inputs`` table
types every dish (``ArrayDish``, ``RFIDish``, or ``Missing`` for a slot
with no dish behind it), and an element of a ``Missing`` dish has nothing
to receive with, so it is bad by construction — whatever any file or
service says, and whether or not the N² data happens to cover it (the
``subset/`` files leave those elements out altogether, so power-outlier
never sees them).  kotekan's own baseline mask already excludes them;
flagging them here too keeps bffs's bad list, the grid and the history
honest about what is a feed, at no cost.

The types come from the config choco holds for the group, through the
core (``src["dish_types"]``, ``{label: type}``); with no config (choco
down, a dry run on the file's own axis) the source has nothing to judge
and abstains, ``degraded``.  ``bad_types`` (default ``[Missing]``) names
the types to flag; the RFI antennas are deliberately *not* among them —
they are real receivers, judged by the other sources on their own terms.
"""

from __future__ import annotations

import numpy as np

from .common import report

#: Flag these kotekan ``DishType`` names unless the config says otherwise.
DEFAULT_BAD_TYPES = ("Missing",)


def mask(src: dict, labels: np.ndarray, kotekan_file: str):
    """Good-mask over ``labels``: an element is bad iff its dish type is
    in ``bad_types``.  ``kotekan_file`` is unused.

    Each flagged element carries ``type <name>`` in the report's
    ``feed_reasons``; ``type_counts`` gives the axis's element count per
    type for the page.  A label the config has no type for is left good
    and counted (``n_untyped``, ``degraded``): it is not evidence either
    way.
    """
    bad_types = {str(t) for t in (src.get("bad_types") or DEFAULT_BAD_TYPES)}
    types = src.get("dish_types")
    axis = [str(lbl) for lbl in labels]
    if not types:
        return np.ones(len(axis), dtype=bool), report(
            "degraded", "no dish types: the kotekan config was not available",
            n_measured=0, bad_types=sorted(bad_types))
    good = np.ones(len(axis), dtype=bool)
    reasons: dict[str, str] = {}
    counts: dict[str, int] = {}
    untyped = []
    for i, lbl in enumerate(axis):
        t = types.get(lbl)
        if t is None:
            untyped.append(lbl)
            continue
        counts[t] = counts.get(t, 0) + 1
        if t in bad_types:
            good[i] = False
            reasons[lbl] = f"type {t}"
    detail = {
        "bad_types": sorted(bad_types),
        "type_counts": dict(sorted(counts.items())),
        "n_typed": len(axis) - len(untyped),
        "n_untyped": len(untyped),
        "feed_reasons": reasons,
    }
    if untyped:
        detail["untyped"] = untyped[:10]
        return good, report(
            "degraded", f"{len(untyped)} element(s) have no dish type in the "
            f"kotekan config and were left good",
            n_measured=len(axis) - len(untyped), **detail)
    return good, report("ok", n_measured=len(axis), **detail)
