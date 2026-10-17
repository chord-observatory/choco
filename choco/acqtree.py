"""The data trees on the mounts and the names in them, written once for
the jobs that walk them and for choco's read side.

kotekan writes each acquisition to ``<root>/acq_<YYYYMMDD>_<HHMMSS>_<ns>/``
and each completed file into it as ``vis_<abs_file_idx>_<YYYYMMDD>T_
<HHMMSS>_<ns>.h5``, renamed in from its ``.partial/`` staging directory
when complete.  The waterfall job mirrors that tree as ``<waterfalls_dir>/
<root name>/<acq>/``: a shard per element (``e0003``) holding each
product's images (``wf_e0003xe0007.png``, ``th_…``).  Stdlib only.
"""

from __future__ import annotations

import glob
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from .jobclient import Degraded

#: Acquisition directories start with this; the rest is the start time,
#: so a name sort is a time sort.
ACQ_PREFIX = "acq_"
#: An acquisition directory named for its start (``acq_20261012_…``).
ACQ_NAME_RE = re.compile(r"^acq_\d{8}")

#: A completed kotekan file.  The index in the name is what lets a file
#: already folded in be skipped without opening it: the archive is
#: thousands of files on the NFS server kotekan is writing to.
VIS_NAME_RE = re.compile(r"^vis_0*(\d+)_(\d{8})T_(\d{6})_(\d+)\.h5$")
#: The index alone, read from any ``vis_<n>_`` name.
_VIS_INDEX_RE = re.compile(r"^vis_0*(\d+)_")

#: The waterfall tree's names, as the job writes them and choco's read
#: side matches every URL component against them.
SHARD_RE = re.compile(r"^e\d{4}$")
IMAGE_RE = re.compile(r"^(?:wf|th)_e(\d{4})xe(\d{4})\.png$")
#: One element's coherence image (the overview; one scanline per file).
COHERENCE_RE = re.compile(r"^co_e(\d{4})\.png$")


def file_index(name: str) -> int | None:
    """The source index in a file name, or None if it is not named that way."""
    m = _VIS_INDEX_RE.match(os.path.basename(name))
    return int(m.group(1)) if m else None


def file_time(name: str) -> float | None:
    """Unix time of a file's first sample, from its name, or None."""
    m = VIS_NAME_RE.match(os.path.basename(name))
    if not m:
        return None
    dt = datetime.strptime(m.group(2) + m.group(3), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    frac = int(m.group(4)) * 1e-9 if len(m.group(4)) == 9 else 0.0
    return dt.timestamp() + frac


def normalize_roots(roots) -> list[dict]:
    """A job's ``roots:`` as ``[{"path", "name"}]``: an entry is a path or a
    mapping with ``path`` and an optional ``name`` (default: the path's
    last component, the name the waterfall tree is keyed by).  Raises
    ``ValueError`` for none at all or an entry without a path."""
    if not roots:
        raise ValueError("no roots configured")
    out = []
    for r in roots:
        if isinstance(r, str):
            r = {"path": r}
        if not isinstance(r, dict) or not r.get("path"):
            raise ValueError(f"root {r!r} has no path")
        out.append({"path": str(r["path"]),
                    "name": str(r.get("name") or Path(r["path"]).name)})
    return out


def list_acquisitions(root: str, *, newest_first: bool = False) -> list[str]:
    """The acquisition directories under *root*, oldest first (or newest
    first).  An unreadable root is :class:`~choco.jobclient.Degraded`:
    a mount that is down heals by itself."""
    try:
        names = [e for e in os.listdir(root)
                 if e.startswith(ACQ_PREFIX) and os.path.isdir(os.path.join(root, e))]
    except OSError as e:
        raise Degraded(f"data root {root} unavailable: {e}") from e
    return sorted(names, reverse=newest_first)


def source_files(acq_dir: str) -> list[str]:
    """An acquisition's completed files, by name.  ``.partial`` is kotekan's
    staging area: a plain ``*.h5`` glob already excludes it (a dotted
    subdirectory), the same rule choco's /files page counts by."""
    return sorted(glob.glob(os.path.join(acq_dir, "*.h5")))


def shard_name(element: int) -> str:
    """The waterfall shard holding element *element*'s products: ``e0003``."""
    return f"e{int(element):04d}"


def product_name(a: int, b: int) -> str:
    """A waterfall product's name: ``e0003xe0007``.  Zero-padded, so a
    lexical sort is a numeric sort and the scheme survives hundreds of
    elements without a rename."""
    return f"{shard_name(a)}x{shard_name(b)}"
