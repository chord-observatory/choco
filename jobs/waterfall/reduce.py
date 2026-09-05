#!/usr/bin/env python3
"""Read one kotekan N² visibility file into waterfall scanlines.

The whole file is streamed **once, in chunk-aligned frequency blocks**,
because of how the data sits on disk: ``vis`` is chunked
``(16 freq, 16 prod, 20 time)``, so pulling a single product's column
touches 385 chunks scattered through 700 MB and runs at ~6 MB/s, while a
sequential sweep runs at ~100 MB/s.  Every product together therefore
costs only ~1.9× what one product costs, and looping products over the
file would cost ~17×.  One pass, all products, always.

Magnitude only — gains are deliberately *not* divided out, so |V| carries
the raw instrumental scale and a mid-acquisition gain change shows up as a
step rather than being normalised away.

Quantization happens inside the streaming loop rather than after it: the
intermediate ``(n_prod, n_time, n_freq)`` array is 260 MB as float32 for a
32-element file but 65 MB as the uint8 indices that actually get written,
and the gap only widens with element count.

Validity comes from ``frames_added``, which is ``(n_freq, n_time)`` and
0.1 MB — measured against live frames, ``frames_added == 0`` is exactly
where |V| is zero, so it is the honest "nothing landed here" mask and
costs nothing to read.  ``vis_weight`` would say more but is 199 MB
stored, a ~45% surcharge on the I/O that dominates this job.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import h5py
import hdf5plugin  # noqa: F401  -- registers the bitshuffle filter
import numpy as np

import wfpng

log = logging.getLogger("waterfall.reduce")

from choco.dishlabels import (  # noqa: E402
    expand_dish_labels, labels_are_per_element, num_polarizations,
)


def _element_labels(labels: list, n_elements: int) -> list:
    """Per-element labels from per-dish labels; old layouts get none.

    A pre-2026-08 per-element label set (a polarisation marker in the
    text) is dropped rather than stored: its element ordering was wrong,
    so the names would sit on the wrong axes.  The viewer already falls
    back to element indices when an acquisition has no labels.
    """
    if not labels:
        return labels
    if labels_are_per_element(labels):
        log.warning("per-element (pre-2026-08) labels in the source file; "
                    "storing element indices instead of labels")
        return []
    return expand_dish_labels(labels, num_polarizations(len(labels), n_elements))

#: Frequency rows per streamed block, a multiple of the 16-row chunk.
#: Peak memory tracks this and nothing else; 64 rows measured no slower
#: than 1024 (the job is waiting on NFS, not on the block loop).
BLOCK_ROWS = 64

#: Read one frequency chunk in this many when sampling for the scale.
#: Chunk-aligned so a sample costs 1/N of the I/O rather than all of it.
SAMPLE_BLOCK_STRIDE = 8


@dataclass
class Axes:
    """What a file says about itself, beyond the visibilities."""

    n_freq: int
    n_prod: int
    n_time: int
    n_elements: int
    file_idx: int
    input_a: np.ndarray
    input_b: np.ndarray
    freq_mhz: np.ndarray
    labels: list = field(default_factory=list)
    times_ns: np.ndarray = None

    @property
    def is_auto(self) -> np.ndarray:
        return self.input_a == self.input_b

    def product_names(self) -> list[str]:
        """Stable 4-digit names — the on-disk identity of each product.

        Zero-padded so a lexical sort is a numeric sort and so the scheme
        survives the jump to hundreds of elements without a rename.
        """
        return [f"e{a:04d}xe{b:04d}" for a, b in zip(self.input_a, self.input_b)]


def read_axes(path) -> Axes:
    """Metadata only — opens the file but reads no visibilities."""
    with h5py.File(path, "r") as f:
        prod = f["index_map/prod"][:]
        vis = f["vis"]
        attrs = f.attrs
        labels = []
        if "index_map/label" in f:
            labels = [b.decode() if isinstance(b, bytes) else str(b)
                      for b in f["index_map/label"][:]]
        times = f["time_center_ut1_ns"][:] if "time_center_ut1_ns" in f else None
        n_elements = int(attrs.get("num_elements", 0))
        return Axes(
            n_freq=vis.shape[0],
            n_prod=vis.shape[1],
            n_time=vis.shape[2],
            n_elements=n_elements,
            file_idx=int(attrs.get("abs_file_idx", -1)),
            input_a=prod["input_a"].astype(int),
            input_b=prod["input_b"].astype(int),
            freq_mhz=f["index_map/freq"]["centre"][:].astype(float),
            labels=_element_labels(labels, n_elements),
            times_ns=times,
        )


def _validity(f, n_freq: int, n_time: int) -> np.ndarray | None:
    """``(n_freq, n_time)`` bool: did anything land in this cell."""
    if "frames_added" not in f:
        return None
    fa = f["frames_added"][:]
    if fa.shape != (n_freq, n_time):
        log.warning("frames_added is %s, expected %s; ignoring",
                    fa.shape, (n_freq, n_time))
        return None
    return fa > 0


def sample_magnitudes(path, block_stride: int = SAMPLE_BLOCK_STRIDE) -> np.ndarray:
    """|V| for every product over a chunk-aligned slice of the band.

    Used once per acquisition to set the value scale.  Reads one
    frequency chunk in ``block_stride``, so it costs a fraction of a full
    pass while still spanning the whole band — a scale set from a
    contiguous corner of the spectrum would not survive the parts of the
    band that are unfed.

    Returns ``(n_prod, n_sample)`` float32, zeros included; the caller
    decides what counts as valid.
    """
    with h5py.File(path, "r") as f:
        d = f["vis"]
        n_freq, n_prod, n_time = d.shape
        step = 16 * max(1, int(block_stride))
        out = []
        for i in range(0, n_freq, step):
            blk = d[i:i + 16, :, :]
            out.append(np.abs(blk).transpose(1, 0, 2).reshape(n_prod, -1))
    if not out:
        return np.zeros((0, 0), np.float32)
    return np.concatenate(out, axis=1).astype(np.float32, copy=False)


def scale_from_sample(sample: np.ndarray, pct=(0.5, 99.5),
                      headroom: float = 0.5) -> tuple[np.ndarray, np.ndarray]:
    """Per-product ``(lo, hi)`` for the whole acquisition, from one file.

    The range is frozen for the acquisition's lifetime — it is baked into
    every pixel and cannot be revised later — so it is padded outward by
    ``headroom`` times the observed log span on each side, leaving room
    for the drift a gain update causes without the values clipping.

    Padding costs display contrast only in principle: the equalization
    that decides what the reader actually sees lives in the palette and
    is recomputed from the real distribution at render time.  What the
    range must protect is *resolution* — too wide and the data collapses
    into a handful of levels — which is why the headroom is proportional
    to the observed span rather than a fixed number of decades.

    Products with no usable values get ``(nan, nan)`` and are skipped by
    the caller.
    """
    n_prod = sample.shape[0]
    lo = np.full(n_prod, np.nan)
    hi = np.full(n_prod, np.nan)
    for p in range(n_prod):
        v = sample[p]
        v = v[np.isfinite(v) & (v > 0)]
        if v.size < 16:
            continue
        a, b = np.percentile(v, pct)
        if not (a > 0 and b > a):
            continue
        la, lb = np.log10(a), np.log10(b)
        pad = headroom * (lb - la)
        lo[p], hi[p] = 10.0 ** (la - pad), 10.0 ** (lb + pad)
    return lo, hi


def quantize_file(path, lo: np.ndarray, hi: np.ndarray,
                  out: np.ndarray | None = None) -> np.ndarray:
    """Stream the file into palette indices.

    Returns ``(n_prod, n_time, n_freq)`` uint8 — time-major per product,
    which is the scanline order the images are appended in.  Products
    whose scale is nan come back all-``MISSING``.
    """
    with h5py.File(path, "r") as f:
        d = f["vis"]
        n_freq, n_prod, n_time = d.shape
        if lo.shape != (n_prod,) or hi.shape != (n_prod,):
            raise ValueError(
                f"scale is {lo.shape}/{hi.shape}, file has {n_prod} products")
        if out is None:
            out = np.empty((n_prod, n_time, n_freq), np.uint8)
        elif out.shape != (n_prod, n_time, n_freq):
            raise ValueError(f"out is {out.shape}, expected "
                             f"{(n_prod, n_time, n_freq)}")

        valid = _validity(f, n_freq, n_time)
        usable = np.isfinite(lo) & np.isfinite(hi) & (hi > lo) & (lo > 0)
        lo_l = np.where(usable, np.log10(np.where(usable, lo, 1.0)), 0.0)
        span = np.where(usable, np.log10(np.where(usable, hi, 10.0)) - lo_l, 1.0)
        # shaped to broadcast over a (block_freq, n_prod, n_time) slab
        lo_l = lo_l[None, :, None]
        span = span[None, :, None]
        keep = usable[None, :, None]

        for i in range(0, n_freq, BLOCK_ROWS):
            blk = d[i:i + BLOCK_ROWS, :, :]
            mag = np.abs(blk)
            ok = (mag > 0) & keep
            if valid is not None:
                ok &= valid[i:i + BLOCK_ROWS, None, :]
            lg = np.zeros_like(mag)
            np.log10(mag, out=lg, where=ok)
            frac = (lg - lo_l) / span
            np.clip(frac, 0.0, 1.0, out=frac)
            q = (1.0 + frac * (wfpng.N_LEVELS - 1)).astype(np.uint8)
            q[~np.broadcast_to(ok, q.shape)] = wfpng.MISSING
            out[:, :, i:i + blk.shape[0]] = q.transpose(1, 2, 0)
    return out


def thumb_rows(indices: np.ndarray, n_bins: int = 128) -> np.ndarray:
    """One low-resolution row per product, for the contact-sheet thumbnails.

    Bins the file's time samples down to a single row and the frequency
    axis to ``n_bins``, using the **minimum** over each bin so a dropout
    survives; a mean would average a dead channel back into its
    neighbours, which is the one thing a thumbnail must not hide.
    ``MISSING`` cells are excluded, and a bin with nothing valid in it
    stays ``MISSING``.

    Returns ``(n_prod, n_bins)`` uint8.
    """
    n_prod, n_time, n_freq = indices.shape
    n_bins = max(1, min(int(n_bins), n_freq))
    edges = np.linspace(0, n_freq, n_bins + 1).astype(int)
    out = np.zeros((n_prod, n_bins), np.uint8)
    for b in range(n_bins):
        sl = indices[:, :, edges[b]:edges[b + 1]].reshape(n_prod, -1)
        masked = np.where(sl == wfpng.MISSING, 255, sl)
        lo = masked.min(axis=1)
        out[:, b] = np.where((sl != wfpng.MISSING).any(axis=1), lo, wfpng.MISSING)
    return out
