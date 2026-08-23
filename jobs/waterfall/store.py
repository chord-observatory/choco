#!/usr/bin/env python3
"""The on-disk waterfall store for one acquisition.

Layout, mirroring the acquisition it describes::

    <waterfalls>/<root>/<acq>/
        index.json          the commit record — see below
        freq.npy            frequency axis, written once
        times.bin           int64 per scanline, appended
        counts.npy          (n_prod, 256) index histogram, for the equalization
        thumbs.dat          (n_files, n_prod, n_bins) uint8, appended
        e0000/wf_e0000xe0000.png
              th_e0000xe0000.png

Every product is one PNG that *grows*: kotekan's files land atomically and
never change, so a new file is appended as scanlines and nothing already
written is rewritten.  One triangle of images per acquisition, not one per
source file.

``index.json`` is the commit point and the only authority on what is
done.  It records, per append-only file, the byte length of the last
completed write, and it is written *after* every other write.  So a run
that died partway through leaves files that are too long, never too
short, and the next run truncates them back and redoes the append.  That
is the whole crash-recovery story; there is no repair pass.

Element indices are zero-padded to four digits and sharded by the first
input, so a lexical sort is a numeric sort and no directory holds more
entries than there are elements — which is what keeps the scheme usable
when the array grows from 32 elements to several hundred.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import numpy as np

import wfpng
from reduce import Axes, thumb_rows

log = logging.getLogger("waterfall.store")

INDEX_VERSION = 1

#: Frequency bins kept per product for the contact-sheet thumbnails.
THUMB_BINS = 128

#: Most scanlines a thumbnail is allowed; longer acquisitions bin time
#: down to fit, so a thumbnail stays small however long the night runs.
THUMB_MAX_ROWS = 256


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _append_at(path: Path, offset: int, data: bytes) -> int:
    """Append ``data`` at a *recorded* offset, returning the new length.

    Writing at the recorded offset rather than at EOF is what makes this
    idempotent: a torn write from a previous run is simply overwritten.
    """
    mode = "r+b" if path.exists() else "wb"
    with open(path, mode) as f:
        f.seek(0, 2)
        if f.tell() < offset:
            raise ValueError(f"{path}: {f.tell()} bytes, index records {offset}")
        f.seek(offset)
        f.truncate()
        f.write(data)
        return offset + len(data)


class AcquisitionStore:
    """Open (or start) the waterfall store for one acquisition."""

    def __init__(self, path, acq_name: str, source_root: str,
                 source_path: str | None = None):
        self.path = Path(path)
        self.acq_name = acq_name
        self.source_root = source_root
        self.source_path = source_path
        self.index: dict = {}
        self._counts: np.ndarray | None = None
        self._load()

    # --- index ----------------------------------------------------------

    @property
    def index_path(self) -> Path:
        return self.path / "index.json"

    def _load(self) -> None:
        if not self.index_path.exists():
            return
        try:
            self.index = json.loads(self.index_path.read_text())
        except (OSError, ValueError) as e:
            # One unreadable index costs one acquisition, not the run —
            # the same containment Registry.reload applies to configs.
            log.error("%s: unreadable index (%s); acquisition skipped",
                      self.index_path, e)
            self.index = {"broken": True}

    @property
    def broken(self) -> bool:
        return bool(self.index.get("broken"))

    @property
    def started(self) -> bool:
        return bool(self.index) and not self.broken

    @property
    def processed(self) -> set:
        """Source-file indices already folded in."""
        return set(self.index.get("files", []))

    @property
    def skipped(self) -> dict:
        """Source files that can never be folded in, and why.

        Kept so a permanently-unusable file is reported once instead of
        reappearing in the backlog — and in the exit code — on every run.
        """
        return self.index.get("skipped") or {}

    def skip(self, entries: dict) -> None:
        """Record files as permanently unusable and commit.

        Takes a mapping so a batch costs one commit, which matters
        because the first run of an acquisition collects its skips while
        scanning — before the store exists to write them to.
        """
        if not self.started or not entries:
            return
        self.index.setdefault("skipped", {}).update(entries)
        self._save_index()

    @property
    def n_rows(self) -> int:
        prods = self.index.get("products") or [{}]
        return int(prods[0].get("rows", 0))

    def _save_index(self) -> None:
        self.index["updated"] = time.time()
        if self.source_path and not self.index.get("source_path"):
            # written by a version that did not record it; fill it in so
            # choco can match this acquisition to its scan root exactly
            self.index["source_path"] = self.source_path
        _atomic_write(self.index_path,
                      json.dumps(self.index, separators=(",", ":")).encode())
        _atomic_write(self.path / "summary.json",
                      json.dumps(self.summary()).encode())

    def summary(self) -> dict:
        """The handful of numbers /files needs.

        Written beside the index because the index carries one record per
        product: at 100 elements it is ~700 KB, and choco's 30 s sweep
        would otherwise pull and parse ~23 MB off NFS to count files.
        """
        products = self.index.get("products") or []
        return {
            "rendered": len(self.index.get("files") or []),
            "products": len(products),
            "rows": int(products[0].get("rows", 0)) if products else 0,
            "elements": int(self.index.get("n_elements") or 0),
            "skipped": len(self.index.get("skipped") or {}),
            "source_path": self.index.get("source_path"),
            "updated": self.index.get("updated"),
        }

    # --- creation -------------------------------------------------------

    def start(self, axes: Axes, lo: np.ndarray, hi: np.ndarray) -> None:
        """Record the acquisition's shape and its frozen value scale.

        The scale is fixed here for the acquisition's whole life: it is
        baked into every pixel and there is no way to revise it without
        rewriting every image.  What stays adjustable is the *display*,
        which lives in the palette.
        """
        self.path.mkdir(parents=True, exist_ok=True)
        np.save(self.path / "freq.npy", axes.freq_mhz)
        names = axes.product_names()
        self.index = {
            "version": INDEX_VERSION,
            "acquisition": self.acq_name,
            "source_root": self.source_root,
            "created": time.time(),
            "n_freq": int(axes.n_freq),
            "n_prod": int(axes.n_prod),
            "n_elements": int(axes.n_elements),
            # the acquisition this was rendered from, so choco can match
            # a scan root to its images without guessing from basenames
            "source_path": self.source_path,
            # the *effective* bin count: thumb_rows clamps to the band
            # width, and the stride recorded here has to match what was
            # actually written or thumbs.dat reshapes wrong.
            "thumb_bins": min(THUMB_BINS, int(axes.n_freq)),
            "labels": list(axes.labels),
            "files": [],
            "skipped": {},
            "thumb_stride": 0,
            "thumb_rows": 0,
            "times_bytes": 0,
            "thumbs_bytes": 0,
            "products": [
                {"name": names[p],
                 "a": int(axes.input_a[p]), "b": int(axes.input_b[p]),
                 "lo": (float(lo[p]) if np.isfinite(lo[p]) else None),
                 "hi": (float(hi[p]) if np.isfinite(hi[p]) else None),
                 "width": 0, "rows": 0, "nbytes": 0, "adler": 0}
                for p in range(axes.n_prod)
            ],
        }
        self._counts = np.zeros((axes.n_prod, 256), np.int64)
        self._save_index()

    def scale(self) -> tuple[np.ndarray, np.ndarray]:
        """The frozen per-product ``(lo, hi)``, nan where unscaled."""
        prods = self.index["products"]
        lo = np.array([np.nan if p["lo"] is None else p["lo"] for p in prods])
        hi = np.array([np.nan if p["hi"] is None else p["hi"] for p in prods])
        return lo, hi

    def matches(self, axes: Axes) -> str | None:
        """Why ``axes`` cannot be appended to this store, or None."""
        if int(axes.n_freq) != self.index["n_freq"]:
            return (f"frequency axis is {axes.n_freq}, acquisition is "
                    f"{self.index['n_freq']} — a PNG cannot be widened")
        if int(axes.n_prod) != self.index["n_prod"]:
            return (f"{axes.n_prod} products, acquisition has "
                    f"{self.index['n_prod']}")
        return None

    # --- paths ----------------------------------------------------------

    def product_dir(self, p: int) -> Path:
        return self.path / f"e{self.index['products'][p]['a']:04d}"

    def image_path(self, p: int) -> Path:
        return self.product_dir(p) / f"wf_{self.index['products'][p]['name']}.png"

    def thumb_path(self, p: int) -> Path:
        return self.product_dir(p) / f"th_{self.index['products'][p]['name']}.png"

    # --- the append -----------------------------------------------------

    def counts(self) -> np.ndarray:
        if self._counts is None:
            f = self.path / "counts.npy"
            n_prod = self.index["n_prod"]
            if f.exists():
                try:
                    c = np.load(f)
                    if c.shape == (n_prod, 256):
                        self._counts = c.astype(np.int64)
                except (OSError, ValueError) as e:
                    log.warning("%s: unreadable (%s); histogram restarted", f, e)
            if self._counts is None:
                self._counts = np.zeros((n_prod, 256), np.int64)
        return self._counts

    def add_file(self, axes: Axes, indices: np.ndarray,
                 level: int = wfpng.DEFAULT_LEVEL) -> None:
        """Fold one source file in.

        Order matters and is the crash-safety contract: every append
        happens first, from its own recorded offset, and ``index.json``
        is written last.  Until that final write nothing has been
        committed, and re-running redoes exactly the same work.
        """
        prods = self.index["products"]
        n_prod = len(prods)
        if indices.shape[0] != n_prod:
            raise ValueError(f"{indices.shape[0]} products, index has {n_prod}")

        pal_default = wfpng.palette()
        new_state = []
        for p in range(n_prod):
            rec = prods[p]
            path = self.image_path(p)
            if rec["rows"] == 0:
                path.parent.mkdir(parents=True, exist_ok=True)
                st = wfpng.create(path, indices[p], pal_default, level=level)
            else:
                st = wfpng.append(path, indices[p], wfpng.WfState.from_dict(rec),
                                  level=level)
            new_state.append(st)

        counts = self.counts()
        wfpng.index_counts(indices, out=counts)

        thumbs = thumb_rows(indices, self.index["thumb_bins"])
        thumbs_bytes = _append_at(self.path / "thumbs.dat",
                                  int(self.index["thumbs_bytes"]), thumbs.tobytes())
        times = (np.asarray(axes.times_ns, np.int64) if axes.times_ns is not None
                 else np.zeros(indices.shape[1], np.int64))
        times_bytes = _append_at(self.path / "times.bin",
                                 int(self.index["times_bytes"]), times.tobytes())

        _atomic_write(self.path / "counts.npy", _npy_bytes(counts))

        for p, st in enumerate(new_state):
            prods[p].update(st.to_dict())
        self.index["files"] = sorted(self.processed | {int(axes.file_idx)})
        self.index["thumbs_bytes"] = thumbs_bytes
        self.index["times_bytes"] = times_bytes
        self._save_index()                      # <- the commit

    # --- display --------------------------------------------------------

    def refresh_palettes(self) -> int:
        """Re-derive the equalization from the accumulated histogram.

        Constant time per image and it never touches a pixel, so the
        display tracks the whole acquisition even though the pixels were
        written days apart.
        """
        counts = self.counts()
        done = 0
        for p in range(self.index["n_prod"]):
            if self.index["products"][p]["rows"] == 0:
                continue
            try:
                wfpng.set_palette(
                    self.image_path(p),
                    wfpng.palette(warp=wfpng.warp_from_counts(counts[p])))
                done += 1
            except (OSError, ValueError) as e:
                log.warning("%s: palette not updated (%s)", self.image_path(p), e)
        return done

    def update_thumbnails(self, level: int = wfpng.DEFAULT_LEVEL) -> int:
        """Bring every thumbnail up to date, appending where possible.

        Costs O(new files) in the normal case: the stride keeps existing
        scanlines valid, so only newly *complete* bins are appended.  A
        full rebuild happens only when the stride doubles, which is
        ~log2(N/256) times across an acquisition's life.

        Returns how many thumbnails were touched.
        """
        n_prod = self.index["n_prod"]
        bins = self.index["thumb_bins"]
        raw = self.path / "thumbs.dat"
        row_bytes = n_prod * bins
        n_src = int(self.index["thumbs_bytes"]) // row_bytes
        if not raw.exists() or n_src == 0:
            return 0

        stride = thumb_stride(n_src)
        complete = n_src // stride
        rebuilding = stride != self.index.get("thumb_stride")
        have = 0 if rebuilding else int(self.index.get("thumb_rows") or 0)
        if complete <= have and not rebuilding:
            return 0                                  # nothing new is complete

        data = np.memmap(raw, np.uint8, mode="r", shape=(n_src, n_prod, bins))
        lo, hi = have * stride, complete * stride     # only the sources we need
        counts = self.counts()
        prods = self.index["products"]
        done = 0
        for p in range(n_prod):
            if prods[p]["rows"] == 0:
                continue
            rows = bin_fixed(np.asarray(data[lo:hi, p, :]), stride)
            if rows.shape[0] == 0:
                continue
            pal = wfpng.palette(warp=wfpng.warp_from_counts(counts[p]))
            try:
                if rebuilding or not prods[p].get("th"):
                    st = wfpng.create(self.thumb_path(p), rows, pal, level=level)
                else:
                    st = wfpng.append(self.thumb_path(p), rows,
                                      wfpng.WfState.from_dict(prods[p]["th"]),
                                      level=level)
            except (OSError, ValueError) as e:
                log.warning("%s: thumbnail not written (%s)", self.thumb_path(p), e)
                continue
            prods[p]["th"] = st.to_dict()
            done += 1
        self.index["thumb_stride"] = stride
        self.index["thumb_rows"] = complete
        self._save_index()
        return done

    def refresh_thumbnail_palettes(self) -> int:
        """Re-derive the thumbnails' equalization, as for the full images."""
        counts = self.counts()
        done = 0
        for p, rec in enumerate(self.index["products"]):
            if not rec.get("th"):
                continue
            try:
                wfpng.set_palette(
                    self.thumb_path(p),
                    wfpng.palette(warp=wfpng.warp_from_counts(counts[p])))
                done += 1
            except (OSError, ValueError) as e:
                log.warning("%s: palette not updated (%s)", self.thumb_path(p), e)
        return done


def _npy_bytes(a: np.ndarray) -> bytes:
    import io
    buf = io.BytesIO()
    np.save(buf, a)
    return buf.getvalue()


def thumb_stride(n_src: int) -> int:
    """Source files per thumbnail scanline — always a power of two.

    The point of a fixed stride is that bin boundaries never move: bin
    *i* is always sources [i*stride, (i+1)*stride), so a scanline once
    written stays correct as the acquisition grows and new files only
    *append*.  Binning by ``linspace`` over the current length instead
    made every boundary shift on every append, which forced a full
    rebuild per run — O(acquisition length) work for one new file, and
    at 100 elements a 1.07 GB re-read every two minutes.

    The stride doubles when the row cap is passed, so a full rebuild
    happens ~log2(N/256) times over an acquisition rather than N times.
    """
    stride = 1
    while -(-n_src // stride) > THUMB_MAX_ROWS:
        stride *= 2
    return stride


def bin_fixed(block: np.ndarray, stride: int) -> np.ndarray:
    """Reduce whole groups of ``stride`` rows to one, keeping the minimum.

    Minimum for the same reason the frequency binning uses it: a
    thumbnail exists to show that something went wrong, and a mean would
    average a dropout back into its neighbours.  ``MISSING`` is excluded,
    and a bin with nothing valid stays ``MISSING``.  Any trailing partial
    group is dropped — it would change when the next file lands.
    """
    n = (block.shape[0] // stride) * stride
    if n == 0:
        return np.zeros((0,) + block.shape[1:], np.uint8)
    grouped = block[:n].reshape(n // stride, stride, *block.shape[1:])
    valid = grouped != wfpng.MISSING
    masked = np.where(valid, grouped, 255)
    return np.where(valid.any(axis=1), masked.min(axis=1),
                    wfpng.MISSING).astype(np.uint8)
