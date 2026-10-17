#!/usr/bin/env python3
"""Read one kotekan N² visibility file into waterfall scanlines.

The whole file is streamed **once, in chunk-aligned frequency blocks**,
because of how the data sits on disk: ``vis`` is chunked
``(16 freq, every product, every time)`` — 42 MB a chunk at 128 elements
— and HDF5 decompresses a chunk whole.  A product therefore costs
nothing once its chunk is open, and a chunk is the unit of cost: the
selection (:class:`Selection`) says which products become images and
which chunks are read at all (``freq_stride``), and the pass reads each
chosen chunk once.  One pass, never a product loop.

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

from choco import n2file  # noqa: E402
from choco.acqtree import product_name  # noqa: E402


def _element_labels(f) -> list:
    """One label per element of *this file's* axis; doubtful layouts get none.

    kotekan (chord.2021.10+988, acquisitions from 2026-09-11 on) writes
    ``index_map/label`` per element — a full frame's 128 or a compact
    ``DishInputs`` frame's 48, spelled dish label + ``p1``/``p2`` up to
    PR #1695 and dish label + ``X``/``Y`` from then on (2026-10-01 in the
    data) — and ``n2file.element_labels`` turns either into choco's
    ``B4X``/``B4Y`` after checking the count against ``num_elements`` and
    the suffix against ``index_map/pol``.  Any other layout (a per-dish
    table, a table wider than the axis, pre-2026-08 ``A1X`` labels with
    no ``pol`` to vouch for them) is stored as *no* labels rather than
    expanded or guessed: the viewer already falls back to element
    indices, and a wrong name on an axis is worse than none.  A file
    with no label table at all (older acquisitions) says nothing.
    """
    if "index_map/label" not in f:
        return []
    try:
        return n2file.element_labels(f)
    except ValueError as exc:
        log.warning("%s; storing element indices instead of labels", exc)
        return []


#: Frequency rows per streamed block, a multiple of the 16-row chunk.
#: Peak memory tracks this and nothing else; 64 rows measured no slower
#: than 1024 (the job is waiting on NFS, not on the block loop).
BLOCK_ROWS = 64

#: Read one frequency chunk in this many when sampling for the scale.
#: Chunk-aligned so a sample costs 1/N of the I/O rather than all of it.
SAMPLE_BLOCK_STRIDE = 8

#: Frequency rows per chunk of ``vis`` when the file does not say (the
#: size kotekan writes; a contiguous test file has no chunks).
DEFAULT_CHUNK_ROWS = 16


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

    chunk_rows: int = DEFAULT_CHUNK_ROWS

    @property
    def is_auto(self) -> np.ndarray:
        return self.input_a == self.input_b

    def product_names(self) -> list[str]:
        """The on-disk identity of each product (``acqtree.product_name``)."""
        return [product_name(a, b) for a, b in zip(self.input_a, self.input_b)]


def read_axes(path) -> Axes:
    """Metadata only — opens the file but reads no visibilities."""
    with h5py.File(path, "r") as f:
        prod = f["index_map/prod"][:]
        vis = f["vis"]
        attrs = f.attrs
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
            labels=_element_labels(f),
            times_ns=times,
            chunk_rows=int(vis.chunks[0]) if vis.chunks else DEFAULT_CHUNK_ROWS,
        )


# --- what of a file is rendered ---------------------------------------------

@dataclass
class Selection:
    """Which products and channels of a file become pixels.

    Fixed when an acquisition starts and kept for its life (the images
    are append-only), so a started acquisition derives it from its index
    (:func:`selection_from_index`) and only a new one from the config
    (:func:`select`).  ``prod_idx`` indexes the file's product axis,
    ``freq_idx`` its frequency axis, both sorted.
    """

    prod_idx: np.ndarray
    freq_idx: np.ndarray
    input_a: np.ndarray      # per selected product
    input_b: np.ndarray
    freq_mhz: np.ndarray     # per selected channel

    @property
    def n_prod(self) -> int:
        return int(self.prod_idx.shape[0])

    @property
    def n_freq(self) -> int:
        return int(self.freq_idx.shape[0])

    def product_names(self) -> list[str]:
        return [product_name(a, b) for a, b in zip(self.input_a, self.input_b)]

    def is_all_products(self, axes: Axes) -> bool:
        return self.n_prod == axes.n_prod and bool(np.array_equal(self.prod_idx, np.arange(axes.n_prod)))

    def is_all_channels(self, axes: Axes) -> bool:
        return self.n_freq == axes.n_freq


def _selection(axes: Axes, prod_idx: np.ndarray, freq_idx: np.ndarray) -> Selection:
    prod_idx = np.asarray(prod_idx, dtype=int)
    freq_idx = np.asarray(freq_idx, dtype=int)
    return Selection(prod_idx=prod_idx, freq_idx=freq_idx,
                     input_a=axes.input_a[prod_idx], input_b=axes.input_b[prod_idx],
                     freq_mhz=axes.freq_mhz[freq_idx])


def everything(axes: Axes) -> Selection:
    """Every product and channel: the layout before 2026-10-17."""
    return _selection(axes, np.arange(axes.n_prod), np.arange(axes.n_freq))


def strided_channels(axes: Axes, freq_stride: int) -> np.ndarray:
    """One chunk of channels in ``freq_stride``, chunk-aligned.

    ``vis`` is chunked ``(16 freq, every product, every time)`` and a
    chunk decompresses whole, so a channel costs nothing once its chunk
    is open and a chunk skipped is a chunk never read: one chunk in four
    is a quarter of the file for an image with a quarter of the columns,
    each column a real channel.
    """
    stride = max(1, int(freq_stride))
    if stride == 1:
        return np.arange(axes.n_freq)
    cs = max(1, int(axes.chunk_rows))
    keep = np.zeros(axes.n_freq, dtype=bool)
    for start in range(0, axes.n_freq, cs * stride):
        keep[start:start + cs] = True
    return np.flatnonzero(keep)


def select(axes: Axes, products, freq_stride: int = 1) -> tuple[Selection, list[str]]:
    """The selection the config asks of a new acquisition, and warnings.

    ``products`` is None for every product, else a mapping: ``autos``
    (every autocorrelation), ``reference`` (every same-polarization
    cross product with each listed element label) and ``pairs``
    (``[label, label]`` cross products by name).  Labels resolve through
    the file's element table; one the file does not carry is reported
    and skipped, never guessed by position.  Products are kept in the
    file's own order.
    """
    freq_idx = strided_channels(axes, freq_stride)
    if products is None:
        return _selection(axes, np.arange(axes.n_prod), freq_idx), []
    warnings: list[str] = []
    labels = [str(lbl) for lbl in (axes.labels or [])]
    by_label = {lbl: i for i, lbl in enumerate(labels)}
    pairs = {}                                   # (a, b) -> product index
    for p, (a, b) in enumerate(zip(axes.input_a, axes.input_b)):
        pairs[(int(min(a, b)), int(max(a, b)))] = p
    keep = np.zeros(axes.n_prod, dtype=bool)
    if products.get("autos", True):
        keep |= axes.is_auto

    def element(label: str) -> int | None:
        e = by_label.get(str(label))
        if e is None:
            warnings.append(f"{label}: not an element of this file"
                            + ("" if labels else " (no element labels)"))
        return e

    for ref in products.get("reference") or []:
        e = element(ref)
        if e is None:
            continue
        pol = labels[e][-1:]
        for other, lbl in enumerate(labels):
            if other == e or lbl[-1:] != pol:
                continue
            p = pairs.get((min(e, other), max(e, other)))
            if p is not None:
                keep[p] = True
    for pair in products.get("pairs") or []:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            warnings.append(f"{pair!r}: a pair is [label, label]")
            continue
        ea, eb = element(pair[0]), element(pair[1])
        if ea is None or eb is None:
            continue
        p = pairs.get((min(ea, eb), max(ea, eb)))
        if p is None:
            warnings.append(f"{pair[0]} x {pair[1]}: not a product of this file")
        else:
            keep[p] = True
    return _selection(axes, np.flatnonzero(keep), freq_idx), warnings


def selection_from_index(axes: Axes, index: dict) -> tuple[Selection | None, str | None]:
    """The selection a started acquisition recorded, applied to ``axes``,
    or why the file cannot join it.

    The file must have the shape the acquisition started with (its
    product and channel counts, ``n_prod_file`` / ``n_freq_file``; an
    index from before 2026-10-17 recorded every product and channel, so
    its ``n_prod`` / ``n_freq`` are the file's).  Within it, products are
    matched by their ``(a, b)`` inputs, never by position, and channels
    by the recorded ``freq_idx`` (absent: every channel).
    """
    n_freq_file = int(index.get("n_freq_file", index.get("n_freq") or 0))
    if int(axes.n_freq) != n_freq_file:
        return None, (f"frequency axis is {axes.n_freq}, acquisition is "
                      f"{n_freq_file} — a PNG cannot be widened")
    n_prod_file = int(index.get("n_prod_file", index.get("n_prod") or 0))
    if int(axes.n_prod) != n_prod_file:
        return None, f"{axes.n_prod} products, acquisition has {n_prod_file}"
    by_pair = {(int(a), int(b)): p for p, (a, b) in enumerate(zip(axes.input_a, axes.input_b))}
    prod_idx = []
    for rec in index.get("products") or []:
        p = by_pair.get((int(rec["a"]), int(rec["b"])))
        if p is None:
            return None, (f"product {rec.get('name')} (elements {rec['a']}, {rec['b']}) "
                          f"is not in this file")
        prod_idx.append(p)
    if not prod_idx:
        return None, "the acquisition records no products"
    if "freq_idx" in index:
        freq_idx = np.asarray(index["freq_idx"], dtype=int)
        if freq_idx.size and (freq_idx.min() < 0 or freq_idx.max() >= axes.n_freq):
            return None, (f"frequency axis is {axes.n_freq} channels, acquisition reads "
                          f"channel {int(freq_idx.max())}")
    else:
        freq_idx = np.arange(axes.n_freq)
    sel = _selection(axes, np.array(prod_idx), freq_idx)
    if sel.n_freq != int(index.get("n_freq") or 0):
        return None, (f"frequency axis is {sel.n_freq}, acquisition is "
                      f"{index.get('n_freq')} — a PNG cannot be widened")
    return sel, None


def _runs(idx: np.ndarray) -> list[tuple[int, int]]:
    """``[(start, stop), ...]`` of the contiguous runs in sorted ``idx``."""
    if idx.size == 0:
        return []
    cuts = np.flatnonzero(np.diff(idx) != 1) + 1
    return [(int(part[0]), int(part[-1]) + 1) for part in np.split(idx, cuts)]


def _blocks(d, sel: Selection, axes: Axes, every: int = 1):
    """Yield ``(offset, block)``: the selected products of consecutive
    selected channels, ``block`` shaped ``(n_chan, n_sel_prod, n_time)``
    and ``offset`` where its channels start in the selection.  Runs of
    channels longer than ``BLOCK_ROWS`` are read in ``BLOCK_ROWS`` pieces;
    ``every`` keeps one chunk-sized piece in that many (the scale sample).
    A full chunk is read and the products taken in memory: HDF5 opens the
    chunk whole either way, and a point selection over thousands of
    products is slower than the copy.
    """
    take = None if sel.is_all_products(axes) else sel.prod_idx
    offset = 0
    piece = 0
    for start, stop in _runs(sel.freq_idx):
        step = BLOCK_ROWS if every == 1 else max(1, int(axes.chunk_rows))
        for lo in range(start, stop, step):
            hi = min(lo + step, stop)
            if piece % every == 0:
                blk = d[lo:hi, :, :]
                if take is not None:
                    blk = blk[:, take, :]
                yield offset, blk
            offset += hi - lo
            piece += 1


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


def sample_magnitudes(path, sel: Selection | None = None,
                      block_stride: int = SAMPLE_BLOCK_STRIDE,
                      axes: Axes | None = None) -> np.ndarray:
    """|V| for every selected product over a chunk-aligned slice of the band.

    Used once per acquisition to set the value scale.  Reads one
    frequency chunk in ``block_stride`` of the selected channels, so it
    costs a fraction of a full pass while still spanning the whole band —
    a scale set from a contiguous corner of the spectrum would not
    survive the parts of the band that are unfed.

    Returns ``(n_prod, n_sample)`` float32, zeros included; the caller
    decides what counts as valid.
    """
    if axes is None:
        axes = read_axes(path)
    if sel is None:
        sel = everything(axes)
    with h5py.File(path, "r") as f:
        out = [np.abs(blk).transpose(1, 0, 2).reshape(sel.n_prod, -1)
               for _, blk in _blocks(f["vis"], sel, axes, every=max(1, int(block_stride)))]
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
                  out: np.ndarray | None = None, sel: Selection | None = None,
                  axes: Axes | None = None) -> np.ndarray:
    """Stream the file into palette indices.

    Returns ``(n_prod, n_time, n_freq)`` uint8 over the selection —
    time-major per product, which is the scanline order the images are
    appended in.  Products whose scale is nan come back all-``MISSING``.
    ``axes`` saves re-reading the file's metadata when the caller has it.
    """
    if axes is None:
        axes = read_axes(path)
    if sel is None:
        sel = everything(axes)
    n_prod, n_freq, n_time = sel.n_prod, sel.n_freq, axes.n_time
    if lo.shape != (n_prod,) or hi.shape != (n_prod,):
        raise ValueError(
            f"scale is {lo.shape}/{hi.shape}, selection has {n_prod} products")
    if out is None:
        out = np.empty((n_prod, n_time, n_freq), np.uint8)
    elif out.shape != (n_prod, n_time, n_freq):
        raise ValueError(f"out is {out.shape}, expected "
                         f"{(n_prod, n_time, n_freq)}")
    with h5py.File(path, "r") as f:
        d = f["vis"]
        valid = _validity(f, axes.n_freq, n_time)
        if valid is not None:
            valid = valid[sel.freq_idx]
        usable = np.isfinite(lo) & np.isfinite(hi) & (hi > lo) & (lo > 0)
        lo_l = np.where(usable, np.log10(np.where(usable, lo, 1.0)), 0.0)
        span = np.where(usable, np.log10(np.where(usable, hi, 10.0)) - lo_l, 1.0)
        # shaped to broadcast over a (block_freq, n_prod, n_time) slab
        lo_l = lo_l[None, :, None]
        span = span[None, :, None]
        keep = usable[None, :, None]

        for i, blk in _blocks(d, sel, axes):
            mag = np.abs(blk)
            ok = (mag > 0) & keep
            if valid is not None:
                ok &= valid[i:i + blk.shape[0], None, :]
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
