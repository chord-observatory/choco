"""Read side of the waterfall image tree that ``jobs/waterfall`` writes.

The job owns the tree; choco only reads it, to answer two questions:
what has been rendered for each acquisition (a column on ``/files``) and
what images exist for one acquisition (the triangle page).

Both answers come off NFS, so both go through gevent's threadpool for the
same reason ``datafiles.py`` does: a wedged mount blocks in the kernel and
cannot be interrupted, and doing that *in the hub* would freeze the sync
loop, the monitors and every other request.  The summary map is cached
behind a TTL so a page load costs at most one sweep, and a per-acquisition
index is cached on its mtime — the same "edit the file, no restart" rule
``PdbMapFile`` follows.

Nothing here trusts a name from the URL.  Roots, acquisitions, shards and
image filenames are each matched against a pattern before they are joined
onto a path, which is the never-pass-the-caller's-string rule the
journalctl allowlist and the buffer-name check already apply.
"""

from __future__ import annotations

import ast
import json
import logging
import math
import os
import re
import struct
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from gevent.lock import BoundedSemaphore

from .acqtree import COHERENCE_RE, IMAGE_RE, SHARD_RE, product_name, shard_name  # noqa: F401 (re-exported)
from .datafiles import run_blocking

logger = logging.getLogger(__name__)

#: A root or acquisition directory name.  Deliberately strict: these are
#: kotekan's own ``acq_<stamp>`` names and our own root names.
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

# The per-input shard directory and the images inside it, exactly as the
# job writes them: choco.acqtree, the one copy of the tree's names.

DEFAULT_TTL_S = 30.0

#: A wedged mount cannot be interrupted; the greenlet gives up instead.
READ_TIMEOUT_S = 10.0

# How many parsed indices to keep.  One is ~130 kB of JSON at 32 elements
# and ~700 kB at 100, and this store lives as long as the process, so an
# unbounded dict would grow with every acquisition anyone ever opened.
# Only the pages currently being polled are worth keeping.
INDEX_CACHE_MAX = 8


_BROKEN = {"broken": True, "rendered": 0, "products": 0, "rows": 0,
           "elements": 0, "skipped": 0, "source_path": None, "updated": None}


# --- side-file parsers ----------------------------------------------------
#
# The viewer page draws axes around an image from the store's side files:
# freq.npy (the frequency axis, written once), times.bin (one int64 UT1 ns
# per scanline, appended), and the palette sitting inside the PNG itself.
# All three are parsed with the stdlib — the scientific stack deliberately
# lives in the [jobs] extra and the web process never imports numpy.

_PNG_SIG = b"\x89PNG\r\n\x1a\n"

#: Where the writer puts the 256-entry palette: SIG(8) + IHDR chunk(25) +
#: the PLTE chunk's own length/type header(8).  Fixed by construction in
#: ``jobs/waterfall/wfpng.py``; the header bytes are verified before the
#: offset is trusted.
_PLTE_OFF = 41
_PLTE_LEN = 256 * 3

#: Refuse to slurp a side file bigger than this.  freq.npy is a few KB
#: and times.bin grows ~250 KB per observing day, so anything near the
#: cap is not a file the writer produced.
_SIDE_FILE_MAX = 16 * 1024 * 1024


def _read_capped(path: str) -> bytes:
    if os.path.getsize(path) > _SIDE_FILE_MAX:
        raise ValueError(f"{path}: larger than any side file the writer makes")
    with open(path, "rb") as f:
        return f.read()


def read_npy_1d(path: str) -> list[float]:
    """A 1-D little-endian .npy, parsed without numpy.  Blocking.

    The v1/v2 header is a length-prefixed Python dict literal followed by
    the raw values — a few lines of ``struct``/``ast`` for the one layout
    ``np.save`` gives our float64 frequency axis.
    """
    blob = _read_capped(path)
    if blob[:6] != b"\x93NUMPY":
        raise ValueError(f"{path}: not a .npy file")
    if blob[6] == 1:
        (hlen,), off = struct.unpack("<H", blob[8:10]), 10
    else:
        (hlen,), off = struct.unpack("<I", blob[8:12]), 12
    header = ast.literal_eval(blob[off:off + hlen].decode("latin-1"))
    fmt = {"<f8": "d", "<f4": "f", "<i8": "q"}.get(header.get("descr"))
    shape = header.get("shape")
    if fmt is None or header.get("fortran_order") or len(shape) != 1:
        raise ValueError(f"{path}: unsupported npy layout {header}")
    n = int(shape[0])
    data = blob[off + hlen:off + hlen + n * struct.calcsize(fmt)]
    return list(struct.unpack(f"<{n}{fmt}", data))


def read_times(path: str) -> list[int]:
    """times.bin — one little-endian int64 per scanline.  Blocking.

    The values are kotekan's ``time_center_ut1_ns``: UT1 ns **since
    J2000** (2000-01-01 12:00:00 UTC), with 0 padding a scanline whose
    timestamp never arrived (pre-sync samples) — the same conventions
    ``~/pathfinder_tools`` reads these files with.
    """
    blob = _read_capped(path)
    n = len(blob) // 8
    return list(struct.unpack(f"<{n}q", blob[:n * 8]))


def read_palette(path: str) -> list[tuple[int, int, int]]:
    """palette.bin — the 256 RGB triples the overview is drawn with.  Blocking."""
    blob = _read_capped(path)
    if len(blob) != _PLTE_LEN:
        raise ValueError(f"{path}: a palette is {_PLTE_LEN} bytes, not {len(blob)}")
    return [(blob[i], blob[i + 1], blob[i + 2]) for i in range(0, _PLTE_LEN, 3)]


# --- the overview: every product at chunk resolution ----------------------
#
# The job keeps, per acquisition that has one, three append-only cubes
# with one row per source file (jobs/waterfall/store.py): coarse.dat
# (every product, mean |V| per chunk), matrix.dat (per band, element x
# element coherence) and the per-element coherence images.  The web
# renders a slice of a cube into a PNG on request with the stdlib: a
# row of a cube is a row of pixels already, quantized by the job.

#: Most rows a cube read may ask for (a year of 400 s files is ~79k).
CUBE_ROWS_MAX = 100_000


def overview_of(index: dict) -> dict | None:
    """The acquisition's overview record, checked, or None without one."""
    rec = index.get("overview") if isinstance(index, dict) else None
    if not isinstance(rec, dict):
        return None
    try:
        chunks, n_el = int(rec.get("chunks") or 0), int(rec.get("n_el") or 0)
        edges = [float(e) for e in rec.get("edges") or []]
        rows = int(index.get("coarse_rows") or 0)
        n_prod_file = int(index.get("n_prod_file") or 0)
    except (TypeError, ValueError):
        return None
    if chunks <= 0 or n_el <= 0 or len(edges) < 2 or n_prod_file <= 0:
        return None
    order = rec.get("prod_order")
    pairs = rec.get("prod_pairs") if order == "list" else None
    if order == "list" and not isinstance(pairs, list):
        return None
    return {"chunks": chunks, "n_el": n_el, "edges": edges, "n_band": len(edges) - 1,
            "rows": max(0, min(rows, CUBE_ROWS_MAX)), "n_prod_file": n_prod_file,
            "prod_order": "triangle" if order != "list" else "list", "prod_pairs": pairs}


def product_row(ov: dict, a: int, b: int) -> int | None:
    """Where product ``(a, b)`` sits in the coarse cube's product axis
    (kotekan's upper-triangle order, or the recorded list), or None."""
    a, b = int(min(a, b)), int(max(a, b))
    n = ov["n_el"]
    if a < 0 or b >= n:
        return None
    if ov["prod_order"] == "triangle":
        p = a * n - a * (a - 1) // 2 + (b - a)
    else:
        try:
            p = next(i for i, pair in enumerate(ov["prod_pairs"])
                     if isinstance(pair, list) and len(pair) == 2
                     and int(pair[0]) == a and int(pair[1]) == b)
        except (StopIteration, TypeError, ValueError):
            return None
    return p if 0 <= p < ov["n_prod_file"] else None


def read_cube_rows(path: str, offset: int, row_bytes: int, stride: int,
                   n_rows: int) -> list[bytes]:
    """``n_rows`` slices of ``row_bytes`` from a cube, one per ``stride``
    bytes from ``offset``.  Blocking; runs in a thread.  Stops at the
    file's committed end rather than pad: on a live acquisition the file
    can be one uncommitted append ahead, never behind."""
    out = []
    with open(path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        for r in range(min(int(n_rows), CUBE_ROWS_MAX)):
            at = offset + r * stride
            if at + row_bytes > size:
                break
            f.seek(at)
            row = f.read(row_bytes)
            if len(row) < row_bytes:
                break
            out.append(row)
    return out


def _png_chunk(typ: bytes, data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + typ + data + struct.pack(">I", zlib.crc32(typ + data))


def encode_png(width: int, rows: list[bytes], palette) -> bytes:
    """An 8-bit palette PNG from ``rows`` of ``width`` palette indices —
    the same format the job writes, made with the stdlib."""
    if width <= 0 or not rows or any(len(r) != width for r in rows):
        raise ValueError("every row must be the image's width")
    pal = b"".join(bytes((r, g, b)) for r, g, b in palette)
    if len(pal) != _PLTE_LEN:
        raise ValueError("a palette has 256 entries")
    raw = b"".join(b"\x00" + r for r in rows)
    return (_PNG_SIG
            + _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, len(rows), 8, 3, 0, 0, 0))
            + _png_chunk(b"PLTE", pal)
            + _png_chunk(b"IDAT", zlib.compress(raw, 6))
            + _png_chunk(b"IEND", b""))


def read_png_head(path: str) -> dict:
    """One rendered image's dimensions and 256 RGB triples.  Blocking.

    Read from the writer's fixed offsets — the same constant-time
    contract ``wfpng.set_palette`` relies on — so a hundred-megabyte
    image costs a sub-kilobyte read.  The height matters beyond the
    palette: on a live acquisition the served image can be an append
    ahead of the index, and tick fractions must describe the pixels the
    browser actually shows.
    """
    with open(path, "rb") as f:
        head = f.read(_PLTE_OFF + _PLTE_LEN)
    if head[:8] != _PNG_SIG:
        raise ValueError(f"{path}: not a PNG")
    if head[_PLTE_OFF - 8:_PLTE_OFF] != struct.pack(">I", _PLTE_LEN) + b"PLTE":
        raise ValueError(f"{path}: no 256-entry PLTE where one was expected")
    pal = head[_PLTE_OFF:_PLTE_OFF + _PLTE_LEN]
    if len(pal) < _PLTE_LEN:
        raise ValueError(f"{path}: truncated palette")
    width, height = struct.unpack(">II", head[16:24])
    return {"width": width, "height": height,
            "palette": [(pal[i], pal[i + 1], pal[i + 2])
                        for i in range(0, _PLTE_LEN, 3)]}


# --- axis ticks -----------------------------------------------------------
#
# Ticks are placed at evenly spread *pixels* and labelled with the value
# actually recorded there, rather than at round values interpolated onto
# the axis: both axes may be non-uniform (a subset acquisition's channels
# are not contiguous, and a skipped source file leaves a time gap), and a
# label read straight out of the data cannot lie about either.

def _spread(n: int, target: int) -> list[int]:
    """Up to ``target`` indices over ``range(n)``, endpoints included."""
    if n <= 0:
        return []
    k = max(2, min(int(target), n))
    if n == 1:
        return [0]
    step = (n - 1) / (k - 1)
    return sorted({round(i * step) for i in range(k)})


def freq_ticks(freqs, n_freq: int, target: int = 8) -> dict:
    """Ticks along the frequency (column) axis.

    Falls back to channel indices when freq.npy is missing or does not
    match the image width — an honest axis either way.
    """
    have = isinstance(freqs, list) and n_freq > 0 and len(freqs) == n_freq
    ticks = [{"frac": (i + 0.5) / n_freq,
              "label": f"{freqs[i]:g}" if have else str(i)}
             for i in _spread(n_freq, target)]
    return {"ticks": ticks, "unit": "MHz" if have else "channel"}


#: 2000-01-01 12:00:00 UTC as a unix timestamp — ``time_center_ut1_ns``
#: counts from J2000, not from the unix epoch (off by ~30 years if
#: confused; verified against a live file whose name stamps the UTC).
_J2000_UNIX_S = 946_728_000

#: The axis clock.  The instrument runs on UT1 ns since J2000, but the
#: people reading the page are at the site, so labels are shown in this
#: IANA zone (``waterfall.timezone`` in config.yaml overrides).  DUT1
#: (<0.9 s) is ignored at the one-second resolution the labels carry.
DEFAULT_TZ = "America/Vancouver"


def _tzinfo(name: str):
    try:
        return ZoneInfo(name)
    except Exception:
        logger.warning(f"waterfalls: unknown timezone {name!r}; using UTC")
        return timezone.utc


def _j2000_dt(ns: int, zone) -> datetime:
    return datetime.fromtimestamp(ns // 1_000_000_000 + _J2000_UNIX_S, zone)


def time_ticks(times_ns, rows: int, target: int = 12,
               tz: str | None = None) -> dict:
    """Ticks down the time (scanline) axis, plus the span for the header.

    Labels are local time in ``tz`` (default ``DEFAULT_TZ``), and
    ``unit`` carries the zone abbreviation actually in force for the
    span's start (PDT/PST) — or ``scanline`` for the fallback.

    ``times_ns`` is ns since J2000.  A zero entry pads a scanline whose
    timestamp never arrived (pre-sync samples), so zeros are never turned
    into labels: a tick row without a timestamp gets no tick, and the
    span runs first-to-last *real* timestamp.  ``rows`` is the served
    image's height, and on a live acquisition times.bin can be one
    uncommitted append shorter, so rows past the end are treated like
    padding — a transiently unlabelled bottom edge, not a reason to
    distrust the axis.  A store with no real timestamps at all (or an
    older tree with no times.bin) labels scanline numbers instead.
    """
    zone = _tzinfo(tz or DEFAULT_TZ)
    n = min(rows, len(times_ns)) if isinstance(times_ns, list) else 0
    valid = [t for t in (times_ns[:n] if n else []) if t > 0]
    fmt, start, end, unit = "%H:%M", None, None, "scanline"
    if valid:
        t0, t1 = _j2000_dt(valid[0], zone), _j2000_dt(valid[-1], zone)
        unit = t0.strftime("%Z") or "local"
        if (t1 - t0).total_seconds() < 3 * 3600:
            fmt = "%H:%M:%S"
        start = t0.strftime("%Y-%m-%d %H:%M:%S")
        end = t1.strftime("%H:%M:%S" if t1.date() == t0.date()
                          else "%Y-%m-%d %H:%M:%S")
    ticks = []
    for r in _spread(rows, target):
        if valid:
            if r >= n or times_ns[r] <= 0:
                continue
            label = _j2000_dt(times_ns[r], zone).strftime(fmt)
        else:
            label = str(r)
        ticks.append({"frac": (r + 0.5) / rows, "label": label})
    return {"ticks": ticks, "unit": unit, "start": start, "end": end}


def value_ticks(lo, hi, target: int = 6) -> list[dict]:
    """|V| ticks for the colorbar — decades, 2×/5× filled in on a short span.

    Positions are linear in log|V|, which is exactly how the pixel
    indices were quantized, so a tick's fraction up the bar is its
    fraction along the palette.
    """
    try:
        lo, hi = float(lo), float(hi)
    except (TypeError, ValueError):
        return []
    if not (lo > 0 and hi > lo):
        return []
    llo, span = math.log10(lo), math.log10(hi) - math.log10(lo)
    mantissas = (1.0,) if span > 1.5 else (1.0, 2.0, 5.0)
    ticks = []
    for k in range(math.floor(llo), math.ceil(llo + span) + 1):
        for m in mantissas:
            f = (k + math.log10(m) - llo) / span
            if 0.0 <= f <= 1.0:
                ticks.append({"frac": f, "label": f"{m * 10.0 ** k:g}"})
    while len(ticks) > 2 * target:
        ticks = ticks[::2]
    return ticks


def palette_gradient(pal, stops: int = 33) -> str | None:
    """The palette's data levels (1..255) as a CSS gradient, lo at bottom.

    The equalization warp is baked into the palette, so sampling it in
    index order *is* the value->colour curve the pixels use.
    """
    if not pal or len(pal) < 256:
        return None
    parts = []
    for k in range(stops):
        f = k / (stops - 1)
        r, g, b = pal[1 + round(f * 254)]
        parts.append(f"rgb({r},{g},{b}) {f * 100:.1f}%")
    return "linear-gradient(to top, " + ", ".join(parts) + ")"


def summarize_dir(path: str) -> dict | None:
    """One acquisition's headline numbers.  Blocking; runs in a thread.

    Prefers ``summary.json``, which the job writes beside the index for
    exactly this: the index carries one record per product and is ~700 KB
    at 100 elements, where the summary is a few hundred bytes.  Falls back
    to the index for trees written before it existed.

    Returns ``None`` only when there is no acquisition here.  Anything
    present but unusable comes back flagged ``broken`` rather than raised,
    so one bad file costs one row and not the whole sweep.
    """
    for name in ("summary.json", "index.json"):
        try:
            with open(os.path.join(path, name)) as f:
                doc = json.load(f)
        except OSError:
            continue
        except ValueError as exc:
            logger.warning(f"waterfalls: unreadable {name} in {path}: {exc}")
            return dict(_BROKEN)
        try:
            if name == "summary.json":
                return {"broken": False,
                        **{k: doc.get(k, _BROKEN[k]) for k in _BROKEN if k != "broken"}}
            products, files = doc.get("products") or [], doc.get("files") or []
            if not isinstance(products, list) or not isinstance(files, list):
                # len() of a string succeeds, so this would otherwise
                # report a plausible-looking count for a garbage index
                raise TypeError("'products' and 'files' must be lists")
            return {
                "broken": False,
                "rendered": len(files),
                "products": len(products),
                "rows": int(products[0].get("rows", 0)) if products else 0,
                "elements": int(doc.get("n_elements") or 0),
                "skipped": len(doc.get("skipped") or {}),
                "source_path": doc.get("source_path"),
                "updated": doc.get("updated"),
            }
        except (AttributeError, TypeError, ValueError, KeyError, IndexError) as exc:
            logger.warning(f"waterfalls: malformed {name} in {path}: {exc}")
            return dict(_BROKEN)
    return None


def sweep(images_dir: str) -> dict:
    """``{(root, acq): summary}`` for the whole tree.  Blocking."""
    out = {}
    try:
        roots = [e for e in os.scandir(images_dir) if e.is_dir()]
    except OSError as exc:
        logger.warning(f"waterfalls: cannot list {images_dir}: {exc}")
        return out
    for root in roots:
        try:
            acqs = [e for e in os.scandir(root.path) if e.is_dir()]
        except OSError:
            continue
        for acq in acqs:
            summary = summarize_dir(acq.path)
            if summary is not None:
                out[(root.name, acq.name)] = summary
    return out


def stat_and_read(path: str, stamp) -> tuple:
    """``(stamp, index)`` for one acquisition.  Blocking; runs in a thread.

    Stat and read together in a single hop, because the stat is the part
    that hangs on a wedged mount and doing it in the hub would freeze
    everything else.  ``index`` is ``None`` when the file has not changed
    since ``stamp``, so an unchanged acquisition costs one stat.
    """
    try:
        st = os.stat(os.path.join(path, "index.json"))
    except OSError:
        return None, None
    now = (st.st_mtime, st.st_size)
    if stamp is not None and now == stamp:
        return now, None
    try:
        with open(os.path.join(path, "index.json")) as f:
            index = json.load(f)
    except OSError:
        return None, None
    except ValueError as exc:
        logger.warning(f"waterfalls: unreadable index in {path}: {exc}")
        return None, None
    # A parseable non-mapping is not an index; treating it as one only
    # moves the failure into the page that renders it.
    return (now, index) if isinstance(index, dict) else (None, None)


def _stat_parse(path: str, stamp, parser) -> tuple:
    """``(stamp, parsed)`` for one side file.  Blocking; runs in a thread.

    The same one-hop shape as ``stat_and_read``: ``parsed`` is ``None``
    when the file has not changed since ``stamp``, so an unchanged file
    costs one stat.  Parse failures are logged and read as absent.
    """
    try:
        st = os.stat(path)
    except OSError:
        return None, None
    now = (st.st_mtime, st.st_size)
    if stamp is not None and now == stamp:
        return now, None
    try:
        return now, parser(path)
    except (OSError, ValueError, struct.error) as exc:
        logger.warning(f"waterfalls: unreadable {path}: {exc}")
        return None, None


def _open_and_stat(path: str):
    fh = open(path, "rb")
    return fh, os.fstat(fh.fileno())


def open_stream(path, chunk: int = 256 * 1024):
    """``(size, mtime, chunks)`` for a file, or None — never touching the hub.

    A contact sheet is one request per cell (528 today, 5050 at 100
    elements) and a full-resolution image can be a hundred megabytes, so
    the file is neither stat-ed nor read in the hub, and it is streamed
    rather than buffered.
    """
    try:
        fh, st = run_blocking(_open_and_stat, str(path), timeout=READ_TIMEOUT_S)
    except OSError:
        return None

    def chunks():
        # A read that times out ends the response early (the client sees
        # a truncated body), which beats holding the greenlet forever.
        try:
            while True:
                data = run_blocking(fh.read, chunk, timeout=READ_TIMEOUT_S)
                if not data:
                    return
                yield data
        finally:
            try:
                run_blocking(fh.close, timeout=READ_TIMEOUT_S)
            except OSError:
                pass

    return st.st_size, st.st_mtime, chunks()


class WaterfallStore:
    """Cached, hub-safe access to the rendered images."""

    DEFAULT_TTL_S = DEFAULT_TTL_S

    def __init__(self, images_dir=None, ttl_s: float = DEFAULT_TTL_S, *,
                 state_file=None):
        """*images_dir* fixes the tree.  Without it, *state_file* (a callable
        giving the waterfall job's state.json, looked up per call so a
        state_dir set later takes effect) is read for the ``waterfalls_dir``
        the job records every run: waterfall.yaml is the one place the
        tree's location is set."""
        self._images_dir = Path(images_dir) if images_dir else None
        self._state_file = state_file
        self._located: tuple = (None, None)       # (state file stamp, tree)
        self.ttl_s = float(ttl_s)
        self._summaries: dict = {}
        self._swept_at = 0.0
        self._index_cache: dict = {}
        self._side_cache: dict = {}
        # Serialised for the same reason DataFileScan and GainArchive are:
        # N concurrent page loads must cost one walk of the mount, not N,
        # and the threadpool they would each occupy is shared.
        self._lock = BoundedSemaphore()

    @property
    def images_dir(self) -> Path | None:
        """The image tree, or None before the job has said where it is.

        Read from the job's state file (local disk, so no threadpool) and
        re-read only when its stamp moves.  The path is the job's word,
        not the caller's; every component joined under it is still
        matched against the writer's patterns.
        """
        if self._images_dir is not None or self._state_file is None:
            return self._images_dir
        path = self._state_file()
        try:
            st = os.stat(path)
        except OSError:
            return None
        stamp = (st.st_mtime_ns, st.st_size)
        if stamp != self._located[0]:
            try:
                state = json.loads(Path(path).read_text())
            except (OSError, ValueError):
                state = None
            tree = state.get("waterfalls_dir") if isinstance(state, dict) else None
            tree = Path(tree) if isinstance(tree, str) and os.path.isabs(tree) else None
            self._located = (stamp, tree)
        return self._located[1]

    @property
    def configured(self) -> bool:
        return self.images_dir is not None

    # --- path resolution ------------------------------------------------

    def acquisition_dir(self, root: str, acq: str) -> Path | None:
        """Validated ``<images>/<root>/<acq>``, or None if either name is not
        one we could have written."""
        if not self.configured:
            return None
        if not (NAME_RE.match(root or "") and NAME_RE.match(acq or "")):
            return None
        return self.images_dir / root / acq

    def image_file(self, root: str, acq: str, shard: str, name: str):
        """``(directory, filename)`` for one image, or None.

        Split rather than joined because the caller hands the pair to
        ``send_from_directory``, which does its own containment check on
        top of these patterns.
        """
        base = self.acquisition_dir(root, acq)
        if base is None:
            return None
        if not SHARD_RE.match(shard or ""):
            return None
        if not (IMAGE_RE.match(name or "") or COHERENCE_RE.match(name or "")):
            return None
        return base / shard, name

    # --- summaries ------------------------------------------------------

    def summaries(self, force: bool = False) -> dict:
        """``{(root, acq): summary}``, swept at most once per TTL."""
        if not self.configured:
            return {}
        if not force and not self._stale():
            return self._summaries
        with self._lock:
            if not force and not self._stale():       # filled while we waited
                return self._summaries
            try:
                self._summaries = run_blocking(sweep, str(self.images_dir),
                                               timeout=READ_TIMEOUT_S)
            except Exception as exc:                  # never break the page
                logger.warning(f"waterfalls: sweep of {self.images_dir} failed "
                               f"({exc}); serving the last known map")
            self._swept_at = time.time()
            return self._summaries

    def _stale(self) -> bool:
        return (time.time() - self._swept_at) >= self.ttl_s

    def summary(self, root: str, acq: str) -> dict | None:
        return self.summaries().get((root, acq))

    # --- one acquisition ------------------------------------------------

    def index(self, root: str, acq: str) -> dict | None:
        """The acquisition's index, re-read only when its mtime moves."""
        base = self.acquisition_dir(root, acq)
        if base is None:
            return None
        key = str(base)
        cached = self._index_cache.get(key)
        try:
            stamp, index = run_blocking(stat_and_read, str(base),
                                        cached[0] if cached else None,
                                        timeout=READ_TIMEOUT_S)
        except Exception as exc:
            logger.warning(f"waterfalls: read of {base} failed: {exc}")
            return cached[1] if cached else None
        if stamp is None:
            self._index_cache.pop(key, None)
            return None
        if index is None:                             # unchanged since last read
            if cached is None:
                return None
            self._remember(key, cached)
            return cached[1]
        self._remember(key, (stamp, index))
        return index

    def _remember(self, key: str, entry: tuple, cache: dict | None = None,
                  limit: int = INDEX_CACHE_MAX) -> None:
        """Cache one parsed file, evicting the least recently used."""
        cache = self._index_cache if cache is None else cache
        cache.pop(key, None)                      # pop+insert == move to end
        cache[key] = entry
        while len(cache) > limit:
            cache.pop(next(iter(cache)))

    # --- the viewer page's side files -----------------------------------

    def _side_file(self, root: str, acq: str, filename: str, parser):
        """One parsed side file, cached on its mtime like the index."""
        base = self.acquisition_dir(root, acq)
        if base is None:
            return None
        key = str(base / filename)
        cached = self._side_cache.get(key)
        try:
            stamp, value = run_blocking(_stat_parse, key,
                                        cached[0] if cached else None, parser,
                                        timeout=READ_TIMEOUT_S)
        except Exception as exc:
            logger.warning(f"waterfalls: read of {key} failed: {exc}")
            return cached[1] if cached else None
        if stamp is None:
            self._side_cache.pop(key, None)
            return None
        if value is None:                         # unchanged since last read
            if cached is None:
                return None
            value = cached[1]
        # two entries per open acquisition, so twice the index's bound
        self._remember(key, (stamp, value), self._side_cache,
                       limit=2 * INDEX_CACHE_MAX)
        return value

    def freq_axis(self, root: str, acq: str) -> list[float] | None:
        """Frequency centres in MHz, one per image column."""
        return self._side_file(root, acq, "freq.npy", read_npy_1d)

    def coarse_freq(self, root: str, acq: str) -> list[float] | None:
        """The overview's column centres in MHz, one per chunk."""
        return self._side_file(root, acq, "coarse_freq.npy", read_npy_1d)

    def coarse_times(self, root: str, acq: str) -> list[int] | None:
        """UT1 ns since J2000 per overview row (one per source file)."""
        return self._side_file(root, acq, "coarse_times.bin", read_times)

    def palette(self, root: str, acq: str) -> list | None:
        """The 256 RGB triples the overview is drawn with."""
        return self._side_file(root, acq, "palette.bin", read_palette)

    def cube_rows(self, root: str, acq: str, filename: str, offset: int,
                  row_bytes: int, stride: int, n_rows: int) -> list[bytes] | None:
        """Rows of one cube file (``coarse.dat`` / ``matrix.dat``), off the hub."""
        base = self.acquisition_dir(root, acq)
        if base is None or filename not in ("coarse.dat", "matrix.dat"):
            return None
        try:
            return run_blocking(read_cube_rows, str(base / filename), int(offset),
                                int(row_bytes), int(stride), int(n_rows),
                                timeout=READ_TIMEOUT_S)
        except Exception as exc:
            logger.warning(f"waterfalls: read of {base / filename}: {exc}")
            return None

    def times(self, root: str, acq: str) -> list[int] | None:
        """UT1 ns since J2000 per scanline (0 = no timestamp)."""
        return self._side_file(root, acq, "times.bin", read_times)

    def image_head(self, root: str, acq: str, shard: str, name: str):
        """One rendered image's dimensions and palette.

        Uncached: it is a sub-kilobyte read at a fixed offset, and both
        halves go stale between page loads — the palette is exactly what
        the equalization rewrites, and the height grows with every
        append.
        """
        resolved = self.image_file(root, acq, shard, name)
        if resolved is None:
            return None
        directory, filename = resolved
        try:
            return run_blocking(read_png_head, str(directory / filename),
                                timeout=READ_TIMEOUT_S)
        except Exception as exc:
            logger.warning(f"waterfalls: head of {filename}: {exc}")
            return None


def triangle(index: dict, elements=None) -> dict:
    """Lay one acquisition's products out as an upper-triangle grid.

    Returns ``{"elements", "labels", "cells"}`` where ``cells`` maps
    ``(a, b)`` to the product record.  Products absent from the file
    simply have no cell — the grid is built from what was rendered, not
    from an assumed complete triangle, because a subset acquisition is a
    legitimate shape rather than a gap to paper over.
    """
    products = [p for p in (index.get("products") or [])
                if isinstance(p, dict) and p.get("rows")]
    cells = {}
    for p in products:
        try:
            a, b = int(p["a"]), int(p["b"])
        except (KeyError, TypeError, ValueError):
            continue
        # shard by ``a`` because that is what store.product_dir writes;
        # deriving it as min(a, b) would silently point at a directory
        # that does not exist if a product ever came back with a > b
        cells[(min(a, b), max(a, b))] = {
            "name": p.get("name"),
            "shard": shard_name(a),
            "rows": int(p.get("rows") or 0),
        }
    present = sorted({e for pair in cells for e in pair})
    if elements:
        wanted = [e for e in elements if e in present]
        present = wanted or present
    labels = index.get("labels") or []
    return {
        "elements": present,
        "labels": {e: (labels[e] if e < len(labels) else str(e))
                   for e in present},
        "cells": cells,
    }


def parse_elements(raw: str | None) -> list[int] | None:
    """``?elements=0,1,2`` — a display filter, never a path component."""
    if not raw:
        return None
    out = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out or None


def rendered_index(store, force: bool = False) -> dict:
    """What has been rendered, keyed the way the /files table looks it up.

    The scan knows a root by its path, the image tree by its last
    component (the name the job writes under), so each summary is filed
    under ``<root name>/<acq>`` and, where the job recorded the
    acquisition's source path, ``<source path>/<acq>`` too, so two roots
    sharing a basename cannot collide.
    """
    rendered: dict = {}
    for (root_name, acq), summary in store.summaries(force=force).items():
        summary = {**summary, "root": root_name, "acq": acq}
        rendered.setdefault(f"{root_name}/{acq}", summary)
        if summary.get("source_path"):
            rendered[f"{summary['source_path'].rstrip('/')}/{acq}"] = summary
    return rendered


def view_context(store, root: str, acq: str, name: str, tz=None) -> dict | None:
    """What the viewer page draws around one full-resolution image, or
    None when *name* is not a rendered ``wf_`` product of the acquisition.

    The axes live here, at display time, and never in the PNG: the image
    is append-only data, and pixels spent on labels could not be revised
    when the acquisition grows.  Time comes from times.bin, frequency
    from freq.npy, and the |V| colorbar from the palette inside the PNG
    itself plus the frozen lo/hi in the index — each read hub-safe and
    each degrading to a plainer page rather than a 500 when missing.
    """
    index = store.index(root, acq)
    if index is None:
        return None
    if COHERENCE_RE.match(name or ""):
        return coherence_view_context(store, root, acq, name, index, tz=tz)
    m = IMAGE_RE.match(name or "")
    if not m or not name.startswith("wf_"):
        return None
    a, b = int(m.group(1)), int(m.group(2))
    prod = next((p for p in index.get("products") or []
                 if isinstance(p, dict) and p.get("name") == product_name(a, b)), None)
    if prod is None or not prod.get("rows"):
        return None
    shard = shard_name(a)
    n_freq = int(index.get("n_freq") or 0)
    labels = index.get("labels") or []

    def label(e: int) -> str:
        return labels[e] if e < len(labels) else str(e)

    head = store.image_head(root, acq, shard, name)
    pal = head["palette"] if head else None
    # The tick fractions must describe the pixels the browser shows: on a
    # live acquisition the served PNG can be an append ahead of the
    # index's committed row count, so its own IHDR height wins.
    rows = int(head["height"]) if head and head.get("height") else int(prod["rows"])
    # The image renders at page width, so its on-screen height scales
    # with rows/width — a squat early-acquisition image has room for a
    # handful of time labels where a night-long one fits dozens.
    time_target = max(3, min(40, round(43 * rows / max(n_freq, 1))))
    return {
        "root": root, "acq": acq, "shard": shard, "name": name, "kind": "full",
        "image_src": f"/waterfall/{root}/{acq}/{shard}/{name}",
        "a": a, "b": b, "label_a": label(a), "label_b": label(b),
        "rows": rows, "n_freq": n_freq,
        "freq_axis": freq_ticks(store.freq_axis(root, acq), n_freq),
        "time_axis": time_ticks(store.times(root, acq), rows, target=time_target, tz=tz),
        "value_ticks": value_ticks(prod.get("lo"), prod.get("hi")),
        "gradient": palette_gradient(pal),
        "n_files": len(index.get("files") or []),
    }


def _label_of(index: dict):
    labels = index.get("labels") or []

    def label(e: int) -> str:
        return labels[e] if 0 <= e < len(labels) else str(e)
    return label


#: Coherence is drawn on a log scale from 1e-3 (the noise floor of one
#: chunk) to 1, as the job quantizes it: a decade per third of the bar.
_UNIT_TICKS = [{"frac": f, "label": lbl} for f, lbl in
               ((0.0, "0.001"), (1 / 3, "0.01"), (2 / 3, "0.1"), (1.0, "1"))]


def coherence_view_context(store, root: str, acq: str, name: str, index: dict,
                           tz=None) -> dict | None:
    """The viewer page around one element's coherence image (``co_eNNNN.png``)."""
    m = COHERENCE_RE.match(name or "")
    ov = overview_of(index)
    if not m or ov is None:
        return None
    e = int(m.group(1))
    elements = index.get("elements") or []
    rec = elements[e] if 0 <= e < len(elements) and isinstance(elements[e], dict) else None
    if rec is None or not rec.get("rows"):
        return None
    shard = shard_name(e)
    head = store.image_head(root, acq, shard, name)
    rows = int(head["height"]) if head and head.get("height") else int(rec["rows"])
    label = _label_of(index)
    return {
        "root": root, "acq": acq, "shard": shard, "name": name, "kind": "coherence",
        "image_src": f"/waterfall/{root}/{acq}/{shard}/{name}",
        "a": e, "b": e, "label_a": label(e), "label_b": "array",
        "rows": rows, "n_freq": ov["chunks"],
        "freq_axis": freq_ticks(store.coarse_freq(root, acq), ov["chunks"]),
        "time_axis": time_ticks(store.coarse_times(root, acq), rows, target=12, tz=tz),
        "value_ticks": _UNIT_TICKS, "value_title": "coherence",
        "gradient": palette_gradient(head["palette"] if head else None),
        "n_files": len(index.get("files") or []),
    }


def coarse_view_context(store, root: str, acq: str, name: str, tz=None) -> dict | None:
    """The viewer page around one product's coarse waterfall (any product
    of the file, rendered from ``coarse.dat`` on request)."""
    m = IMAGE_RE.match(name or "")
    if not m or not name.startswith("wf_"):
        return None
    index = store.index(root, acq)
    ov = overview_of(index) if index is not None else None
    if ov is None:
        return None
    a, b = int(m.group(1)), int(m.group(2))
    if product_row(ov, a, b) is None or ov["rows"] == 0:
        return None
    label = _label_of(index)
    return {
        "root": root, "acq": acq, "shard": shard_name(a), "name": name, "kind": "coarse",
        "image_src": f"/waterfall/{root}/{acq}/coarse/{name}",
        "a": a, "b": b, "label_a": label(a), "label_b": label(b),
        "rows": ov["rows"], "n_freq": ov["chunks"],
        "freq_axis": freq_ticks(store.coarse_freq(root, acq), ov["chunks"]),
        "time_axis": time_ticks(store.coarse_times(root, acq), ov["rows"], target=12, tz=tz),
        "value_ticks": [], "value_title": "|V|",
        "gradient": palette_gradient(store.palette(root, acq)),
        "n_files": len(index.get("files") or []),
    }


def coarse_image(store, root: str, acq: str, name: str) -> bytes | None:
    """One product's coarse waterfall as a PNG, or None."""
    m = IMAGE_RE.match(name or "")
    if not m or not name.startswith("wf_"):
        return None
    index = store.index(root, acq)
    ov = overview_of(index) if index is not None else None
    if ov is None:
        return None
    p = product_row(ov, int(m.group(1)), int(m.group(2)))
    pal = store.palette(root, acq)
    if p is None or ov["rows"] == 0 or pal is None:
        return None
    rows = store.cube_rows(root, acq, "coarse.dat", offset=p * ov["chunks"],
                           row_bytes=ov["chunks"], stride=ov["n_prod_file"] * ov["chunks"],
                           n_rows=ov["rows"])
    if not rows:
        return None
    return encode_png(ov["chunks"], rows, pal)


def matrix_image(store, root: str, acq: str, band: int, row: int) -> bytes | None:
    """One frame of the element x element matrix as a PNG, or None."""
    index = store.index(root, acq)
    ov = overview_of(index) if index is not None else None
    if ov is None:
        return None
    try:
        band, row = int(band), int(row)
    except (TypeError, ValueError):
        return None
    if not (0 <= band < ov["n_band"] and 0 <= row < ov["rows"]):
        return None
    n = ov["n_el"]
    pal = store.palette(root, acq)
    if pal is None:
        return None
    frame = store.cube_rows(root, acq, "matrix.dat", offset=(row * ov["n_band"] + band) * n * n,
                            row_bytes=n * n, stride=0, n_rows=1)
    if not frame:
        return None
    blob = frame[0]
    return encode_png(n, [blob[i * n:(i + 1) * n] for i in range(n)], pal)


def matrix_context(store, root: str, acq: str, band, row, tz=None) -> dict | None:
    """The matrix page: which band and frame, the labels along the axes,
    and what the page's script needs to name and open cells."""
    index = store.index(root, acq)
    ov = overview_of(index) if index is not None else None
    if ov is None or ov["rows"] == 0:
        return None
    n_rows, n_band = ov["rows"], ov["n_band"]
    try:
        band = int(band) if band not in (None, "") else n_band // 2
        row = int(row) if row not in (None, "") else n_rows - 1
    except ValueError:
        return None
    if not (0 <= band < n_band and 0 <= row < n_rows):
        return None
    edges = ov["edges"]
    bands = [{"i": i, "label": f"{edges[i]:.0f}–{edges[i + 1]:.0f} MHz"} for i in range(n_band)]
    label = _label_of(index)
    n = ov["n_el"]
    ticks = [{"frac": (e + 0.5) / n, "label": label(e)} for e in _spread(n, 9)]
    times = store.coarse_times(root, acq)
    when = None
    if isinstance(times, list) and row < len(times) and times[row] > 0:
        when = _j2000_dt(times[row], _tzinfo(tz or DEFAULT_TZ)).strftime("%Y-%m-%d %H:%M:%S %Z")
    page = f"/files/{root}/{acq}/matrix"
    return {
        "root": root, "acq": acq, "band": band, "row": row, "n_rows": n_rows, "n_el": n,
        "bands": bands, "ticks": ticks, "when": when,
        "gradient": palette_gradient(store.palette(root, acq)),
        "matrix_data": {"n_el": n, "labels": [label(e) for e in range(n)], "band": band,
                        "page": page, "coarse_view": f"/waterfall/{root}/{acq}/coarse-view"},
    }


def strips(index: dict) -> list[dict] | None:
    """The contact sheet as strips, for an acquisition that renders a
    selection rather than the whole triangle: the autocorrelations, each
    reference element's cross products, any other pairs, and the
    coherence images.  None when the triangle is the better layout
    (every product rendered)."""
    products = [p for p in (index.get("products") or [])
                if isinstance(p, dict) and p.get("rows")]
    n_el = int(index.get("n_elements") or 0)
    ov = overview_of(index)
    if not products or (ov is None and n_el and len(products) >= n_el * (n_el + 1) // 2):
        return None
    label = _label_of(index)
    root, acq = index.get("source_root"), index.get("acquisition")

    def cell(p, a, b):
        return {"src": f"/waterfall/{root}/{acq}/{shard_name(a)}/th_{p['name']}.png",
                "view": f"/waterfall/{root}/{acq}/view/wf_{p['name']}.png",
                "label": label(a) if a == b else f"{label(a)}×{label(b)}",
                "title": f"{label(a)} × {label(b)} (elements {a}, {b}) — {p.get('rows')} scanlines"}

    autos, crosses = [], []
    for p in products:
        try:
            a, b = int(p["a"]), int(p["b"])
        except (KeyError, TypeError, ValueError):
            continue
        (autos if a == b else crosses).append((a, b, p))
    out = []
    if autos:
        out.append({"title": "Autocorrelations", "cells": [cell(p, a, b) for a, b, p in autos]})
    # a reference is an element with several cross products; its strip
    # lists them by the other element
    count: dict = {}
    for a, b, _ in crosses:
        count[a] = count.get(a, 0) + 1
        count[b] = count.get(b, 0) + 1
    taken = set()
    for ref, _ in sorted(count.items(), key=lambda kv: (-kv[1], kv[0])):
        mine = [(a, b, p) for a, b, p in crosses if (a == ref or b == ref) and id(p) not in taken]
        if len(mine) < 2:
            break
        mine.sort(key=lambda t: t[1] if t[0] == ref else t[0])
        out.append({"title": f"{label(ref)} × the array",
                    "cells": [cell(p, a, b) for a, b, p in mine]})
        taken.update(id(p) for _, _, p in mine)
    rest = [(a, b, p) for a, b, p in crosses if id(p) not in taken]
    if rest:
        out.append({"title": "Other pairs", "cells": [cell(p, a, b) for a, b, p in rest]})
    if ov is not None:
        els = [(e, rec) for e, rec in enumerate(index.get("elements") or [])
               if isinstance(rec, dict) and rec.get("rows")]
        if els:
            out.append({"title": "Coherence with the array", "cells": [
                {"src": f"/waterfall/{root}/{acq}/{shard_name(e)}/co_{shard_name(e)}.png",
                 "view": f"/waterfall/{root}/{acq}/view/co_{shard_name(e)}.png",
                 "label": label(e),
                 "title": f"{label(e)} coherence with the array (element {e}) — {rec['rows']} files"}
                for e, rec in els]})
    return out
