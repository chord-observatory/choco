"""bffs flagging sources.

Each module here exposes ``mask(src, labels, kotekan_file)`` returning a
length-``len(labels)`` good-mask (``True`` = good) for one source, or a
``(mask, report)`` pair where ``report`` comes from ``common.report`` and
says how the measurement went (``ok`` / ``degraded``, a reason, how many
feeds were judged, free-form detail for choco's BFFS page).  A bare mask
is read as an ``ok`` report.  ``src`` is the source's config dict (its
``kind`` plus parameters); a feed is bad if any source flags it.
``bffs.combine_sources`` dispatches via ``get(kind)``.

A source that cannot measure what it covers — no data in the window, an
endpoint down, too little of the band present — abstains for the feeds it
cannot judge (leaves them good) and reports ``degraded``.  It never turns
"could not measure" into "bad".
"""

import importlib

# config `kind` string -> module name within this package
_KINDS = {
    "manual": "manual",
    "dish-type": "dish_type",
    "power-outlier": "power_outlier",
    "power": "power",
    "fpga": "fpga",
    "rfi": "rfi",
}


def get(kind: str):
    """Return the source module for a config ``kind`` (loaded on demand), or None."""
    name = _KINDS.get(kind)
    return importlib.import_module(f".{name}", __package__) if name else None
