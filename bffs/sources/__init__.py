"""bffs flagging sources.

Each module here exposes ``mask(src, labels, kotekan_file) -> np.ndarray`` — a
length-``len(labels)`` good-mask (``True`` = good) for one source. ``src`` is the
source's config dict (its ``kind`` plus parameters); a feed is bad if any source
flags it. ``bffs.combine_sources`` dispatches via ``get(kind)``.
"""

import importlib

# config `kind` string -> module name within this package
_KINDS = {
    "manual": "manual",
    "power-outlier": "power_outlier",
    "power": "power",
    "fpga": "fpga",
    "rfi": "rfi",
}


def get(kind: str):
    """Return the source module for a config ``kind`` (loaded on demand), or None."""
    name = _KINDS.get(kind)
    return importlib.import_module(f".{name}", __package__) if name else None
