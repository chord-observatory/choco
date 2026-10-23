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

import json
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


# --- the overview: every product, at chunk resolution ------------------------

def triangle_order(n_el: int) -> tuple[np.ndarray, np.ndarray]:
    """kotekan's product order: ``(a, b)`` for a in 0..n, b in a..n."""
    a, b = np.triu_indices(n_el)
    return a.astype(int), b.astype(int)


def band_edges(freq_mhz: np.ndarray, n_bands: int) -> list[float]:
    """``n_bands`` log-spaced bands over the file's frequency range."""
    lo, hi = float(np.min(freq_mhz)), float(np.max(freq_mhz))
    if n_bands < 1 or hi <= lo:
        return [lo, hi]
    return [float(x) for x in np.geomspace(lo, hi, int(n_bands) + 1)]


@dataclass
class OverviewSpec:
    """What the overview of an acquisition records per source file.

    The pass over a file opens every selected chunk whole, so every
    product is in memory for nothing, and three small cubes are kept
    beside the selected images: per product the mean |V| over each chunk
    (``coarse``: any of the 8256 products as a waterfall, one row per
    file, one column per chunk), per element the mean normalized
    amplitude |V_ab| / sqrt(V_aa V_bb) over its partners per chunk
    (``coherence``), and per band the same coherence as an element x
    element matrix (``matrix``), one frame per file.  Columns are the
    chunks the selection keeps (``chunk_freq``, their centres), bands
    are ``edges`` in MHz and a chunk belongs to the band its centre
    falls in.
    """

    n_el: int
    chunk_rows: int
    chunk_starts: np.ndarray     # (n_chunk,) first selected-axis index of each column
    chunk_freq: np.ndarray       # (n_chunk,) MHz
    edges: list[float]           # (n_band + 1,)
    input_a: np.ndarray          # the file's product axis
    input_b: np.ndarray

    @property
    def n_chunk(self) -> int:
        return int(self.chunk_starts.shape[0])

    @property
    def n_band(self) -> int:
        return max(1, len(self.edges) - 1)

    def band_of(self, freq_mhz: float) -> int | None:
        if freq_mhz < self.edges[0] or freq_mhz > self.edges[-1]:
            return None
        return min(int(np.searchsorted(self.edges, freq_mhz, side="right") - 1), self.n_band - 1)

    @property
    def is_triangle(self) -> bool:
        a, b = triangle_order(self.n_el)
        return (a.shape == self.input_a.shape and bool(np.array_equal(a, self.input_a))
                and bool(np.array_equal(b, self.input_b)))

    def to_index(self) -> dict:
        d = {"chunks": self.n_chunk, "chunk_rows": int(self.chunk_rows),
             "edges": [round(e, 3) for e in self.edges], "n_el": int(self.n_el),
             "prod_order": "triangle" if self.is_triangle else "list"}
        if d["prod_order"] == "list":
            d["prod_pairs"] = [[int(a), int(b)] for a, b in zip(self.input_a, self.input_b)]
        return d


def overview_spec(axes: Axes, sel: Selection, n_bands: int = 8,
                  edges: list[float] | None = None) -> OverviewSpec:
    """The overview columns for this selection: one per chunk-sized group
    of consecutive selected channels, their centre frequencies, and the
    bands over the file's range (or the ``edges`` an acquisition recorded)."""
    cs = max(1, int(axes.chunk_rows))
    starts, centres = [], []
    offset = 0
    for lo, hi in _runs(sel.freq_idx):
        for c0 in range(lo, hi, cs):
            c1 = min(c0 + cs, hi)
            starts.append(offset)
            centres.append(float(axes.freq_mhz[c0:c1].mean()))
            offset += c1 - c0
    return OverviewSpec(n_el=int(axes.n_elements or 0), chunk_rows=cs,
                        chunk_starts=np.array(starts, int), chunk_freq=np.array(centres),
                        edges=[float(e) for e in edges] if edges else band_edges(axes.freq_mhz, n_bands),
                        input_a=axes.input_a, input_b=axes.input_b)


@dataclass
class FileOverview:
    """One file's overview, before quantization (the store scales it)."""

    coarse: np.ndarray       # (n_prod_file, n_chunk) float32 mean |V|, nan where no data
    coherence: np.ndarray    # (n_el, n_chunk) float32 in 0..1, nan where no data
    matrix: np.ndarray       # (n_band, n_el, n_el) float32 in 0..1, nan where no data


def _incidence(n_el: int, input_a: np.ndarray, input_b: np.ndarray) -> np.ndarray:
    """``(n_el, n_prod)`` float32: 1 where the product is a cross product of
    the element."""
    m = np.zeros((n_el, input_a.shape[0]), np.float32)
    cross = input_a != input_b
    p = np.flatnonzero(cross)
    m[input_a[p], p] = 1.0
    m[input_b[p], p] = 1.0
    return m


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
                      headroom: float = 0.5, min_values: int = 16) -> tuple[np.ndarray, np.ndarray]:
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
        if v.size < max(2, int(min_values)):
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
    out, _ = fold_file(path, lo, hi, out=out, sel=sel, axes=axes)
    return out


def _quantizer(lo: np.ndarray, hi: np.ndarray):
    """Per-product log scale, shaped to broadcast over a
    ``(block_freq, n_prod, n_time)`` slab; returns ``(lo_l, span, keep)``."""
    usable = np.isfinite(lo) & np.isfinite(hi) & (hi > lo) & (lo > 0)
    lo_l = np.where(usable, np.log10(np.where(usable, lo, 1.0)), 0.0)
    span = np.where(usable, np.log10(np.where(usable, hi, 10.0)) - lo_l, 1.0)
    return lo_l[None, :, None], span[None, :, None], usable[None, :, None]


def _quantize(mag: np.ndarray, ok: np.ndarray, lo_l, span) -> np.ndarray:
    lg = np.zeros_like(mag)
    np.log10(mag, out=lg, where=ok)
    frac = (lg - lo_l) / span
    np.clip(frac, 0.0, 1.0, out=frac)
    q = (1.0 + frac * (wfpng.N_LEVELS - 1)).astype(np.uint8)
    q[~np.broadcast_to(ok, q.shape)] = wfpng.MISSING
    return q


def fold_file(path, lo: np.ndarray, hi: np.ndarray, out: np.ndarray | None = None,
              sel: Selection | None = None, axes: Axes | None = None,
              overview: OverviewSpec | None = None) -> tuple[np.ndarray, FileOverview | None]:
    """One pass over the file: the selected products' pixels and, with an
    ``overview``, the three cubes of :class:`OverviewSpec`.

    With an overview the pass reads one chunk at a time with every
    product in it (the overview needs them all); without, the selected
    products in ``BLOCK_ROWS`` blocks as :func:`quantize_file` always did.
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
    lo_l, span, keep = _quantizer(lo, hi)

    ov = None
    if overview is not None:
        n_el, n_pf = overview.n_el, axes.n_prod
        if n_el <= 0 or overview.input_a.shape[0] != n_pf:
            raise ValueError("overview does not fit this file's product axis")
        auto_of = np.full(n_el, -1, int)
        autos = np.flatnonzero(axes.input_a == axes.input_b)
        auto_of[axes.input_a[autos]] = autos
        has_auto = (auto_of[axes.input_a] >= 0) & (auto_of[axes.input_b] >= 0)
        a_auto = np.where(has_auto, auto_of[axes.input_a], 0)
        b_auto = np.where(has_auto, auto_of[axes.input_b], 0)
        inc = _incidence(n_el, axes.input_a, axes.input_b)       # (n_el, n_prod)
        partners = inc.sum(axis=1)
        coarse = np.full((n_pf, overview.n_chunk), np.nan, np.float32)
        coh = np.full((n_el, overview.n_chunk), np.nan, np.float32)
        band_sum = np.zeros((overview.n_band, n_pf), np.float64)
        band_cnt = np.zeros((overview.n_band, n_pf), np.float64)
        col_of = {int(c): k for k, c in enumerate(overview.chunk_starts)}
        take = None if sel.is_all_products(axes) else sel.prod_idx

    with h5py.File(path, "r") as f:
        d = f["vis"]
        valid = _validity(f, axes.n_freq, n_time)
        if valid is not None:
            valid = valid[sel.freq_idx]
        if overview is None:
            blocks = _blocks(d, sel, axes)
        else:
            blocks = _chunks(d, sel, axes)
        for i, blk in blocks:
            if overview is None:
                mag = np.abs(blk)
                ok = (mag > 0) & keep
                if valid is not None:
                    ok &= valid[i:i + blk.shape[0], None, :]
                out[:, :, i:i + blk.shape[0]] = _quantize(mag, ok, lo_l, span).transpose(1, 2, 0)
                continue
            # the whole chunk is here: pixels for the selection, then the cubes
            mag = np.abs(blk)                                      # (nc, n_prod_file, nt)
            cell = (valid[i:i + blk.shape[0], None, :] if valid is not None
                    else np.ones((blk.shape[0], 1, n_time), bool))
            live = (mag > 0) & cell
            sel_mag = mag if take is None else mag[:, take, :]
            sel_ok = (live if take is None else live[:, take, :]) & keep
            out[:, :, i:i + blk.shape[0]] = _quantize(sel_mag, sel_ok, lo_l, span).transpose(1, 2, 0)
            k = col_of.get(int(i))
            if k is None:
                continue                                           # not a column start (cannot happen)
            cnt = live.sum(axis=(0, 2))                            # (n_prod_file,)
            tot = np.where(live, mag, 0).sum(axis=(0, 2))
            with np.errstate(invalid="ignore", divide="ignore"):
                coarse[:, k] = np.where(cnt > 0, tot / np.maximum(cnt, 1), np.nan)
            band = overview.band_of(float(overview.chunk_freq[k]))
            if band is not None:
                band_sum[band] += tot
                band_cnt[band] += cnt
            # normalized amplitude per cross product, averaged per element
            denom = np.sqrt(mag[:, a_auto, :] * mag[:, b_auto, :])
            norm_ok = live & has_auto[None, :, None] & (denom > 0)
            norm = np.where(norm_ok, mag / np.where(norm_ok, denom, 1.0), 0.0).astype(np.float32)
            flat = norm.transpose(1, 0, 2).reshape(n_pf, -1)        # (n_prod, nc*nt)
            flat_ok = norm_ok.transpose(1, 0, 2).reshape(n_pf, -1).astype(np.float32)
            esum = inc @ flat                                      # (n_el, nc*nt)
            ecnt = inc @ flat_ok
            with np.errstate(invalid="ignore", divide="ignore"):
                coh[:, k] = np.where(ecnt.sum(axis=1) > 0,
                                     esum.sum(axis=1) / np.maximum(ecnt.sum(axis=1), 1), np.nan)
    if overview is None:
        return out, None
    # the matrix: band means of |V_ab| normalized by the band means of the autos
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(band_cnt > 0, band_sum / np.maximum(band_cnt, 1), np.nan)   # (n_band, n_prod)
    matrix = np.full((overview.n_band, n_el, n_el), np.nan, np.float32)
    for bi in range(overview.n_band):
        m = mean[bi]
        auto_mean = np.full(n_el, np.nan)
        auto_mean[axes.input_a[autos]] = m[autos]
        den = np.sqrt(auto_mean[axes.input_a] * auto_mean[axes.input_b])
        with np.errstate(invalid="ignore", divide="ignore"):
            c = np.where(np.isfinite(m) & np.isfinite(den) & (den > 0), m / den, np.nan)
        matrix[bi, axes.input_a, axes.input_b] = c
        matrix[bi, axes.input_b, axes.input_a] = c
    del partners
    return out, FileOverview(coarse=coarse, coherence=coh, matrix=matrix)


def quantize_log(values: np.ndarray, lo: np.ndarray, hi: np.ndarray) -> np.ndarray:
    """``(n_prod, n_cols)`` palette indices for per-product values on the
    products' log scale — the coarse cube's quantization, the same
    levels the images use."""
    lo_l, span, keep = _quantizer(lo, hi)
    v = np.asarray(values, np.float32)[None]                 # (1, n_prod, n_cols)
    ok = np.isfinite(v) & (v > 0) & keep
    return _quantize(np.where(ok, v, 1.0), ok, lo_l, span)[0]


#: The coherence scale: log10 from this floor to 1.  On the live files the
#: noise floor of |V_ab| / sqrt(V_aa V_bb) in a 16-channel x 40-bin block
#: is ~1e-3 and the sky on a short baseline ~0.1, so a linear scale would
#: spend one level on the floor and a few on the sky.
COHERENCE_FLOOR = 1e-3


def quantize_unit(values: np.ndarray) -> np.ndarray:
    """Palette indices for coherence values: 1..255 linear in log10 between
    :data:`COHERENCE_FLOOR` and 1, 0 where there is no value."""
    v = np.asarray(values, np.float32)
    ok = np.isfinite(v) & (v > 0)
    lg = np.log10(np.where(ok, v, 1.0))
    frac = np.clip((lg - np.log10(COHERENCE_FLOOR)) / -np.log10(COHERENCE_FLOOR), 0.0, 1.0)
    q = np.rint(1.0 + frac * (wfpng.N_LEVELS - 1)).astype(np.uint8)
    q[~ok] = wfpng.MISSING
    return q


#: X-engine streams: file channel i comes from stream i mod this (the comb;
#: checked against ``bad_feed_mask/stream_freq_id`` on the 2026-10-23 files).
N_STREAMS = 16

#: The fractions in a diag record are integers in units of this.
DIAG_UNIT = 1e-4


def _per_stream(values: np.ndarray, live: np.ndarray, n_streams: int) -> list:
    """``(n_freq, n_time)`` -> ``n_streams`` lists of per-bin means over
    each stream's live channels, in :data:`DIAG_UNIT` (-1: none live)."""
    out = []
    for s in range(n_streams):
        v, ok = values[s::n_streams], live[s::n_streams]
        n = ok.sum(axis=0)
        with np.errstate(invalid="ignore", divide="ignore"):
            m = np.where(n > 0, np.where(ok, v, 0).sum(axis=0) / np.maximum(n, 1), np.nan)
        out.append([int(round(x / DIAG_UNIT)) if np.isfinite(x) else -1 for x in m])
    return out


def _bin_runs(counts: np.ndarray) -> list:
    """``[[first, stop, count], ...]`` over the bins where ``counts`` > 0."""
    runs, start = [], None
    for b in range(counts.shape[0] + 1):
        c = int(counts[b]) if b < counts.shape[0] else 0
        if start is not None and c != int(counts[start]):
            runs.append([start, b, int(counts[start])])
            start = None
        if start is None and c > 0:
            start = b
    return runs


def read_diag(path, axes: Axes) -> dict:
    """The file's masks and delivery, one JSON line of ``diag.jsonl``.

    Everything here is small beside ``vis``: one chunk of ``flags`` and
    the ``(n_freq, n_time)`` datasets, a few MB a file.
    ``flags[freq, element, time]`` is the per-feed mask kotekan applied
    (0 = masked: the bad feed mask bffs sets, equal to ``bad_feed_mask``
    on the 2026-10-23 files).  Channel i is stream i mod
    :data:`N_STREAMS`, so its first rows give every stream's view;
    ``masked`` counts, per element and bin, the streams that masked it
    among those that delivered (``flag_live``), so a stream that missed
    an update shows as a partial count.  The RFI and packet-loss masks
    are per sample and averaged over feeds, so the file keeps only their
    fractions per channel: ``rfi``, ``pl`` and ``rfi_on`` (the RFI
    mask's first stage was enabled) are per-stream means, ``missing``
    the share of a stream's channels with no frame.
    """
    n_time, ns = axes.n_time, N_STREAMS
    labels = list(axes.labels) or [str(e) for e in range(axes.n_elements)]
    rec: dict = {"file": int(axes.file_idx), "n_bin": int(n_time), "n_streams": ns,
                 "times": ([int(t) for t in axes.times_ns] if axes.times_ns is not None else [])}
    with h5py.File(path, "r") as f:
        live = _validity(f, axes.n_freq, n_time)
        if live is None:
            live = np.ones((axes.n_freq, n_time), bool)
        rec["missing"] = _per_stream((~live).astype(np.float32),
                                     np.ones_like(live), ns)
        for key, name in (("rfi", "frac_rfi"), ("pl", "frac_pl"),
                          ("rfi_on", "frac_rfi_first_stage_enabled")):
            if name in f and f[name].shape == (axes.n_freq, n_time):
                rec[key] = _per_stream(np.nan_to_num(f[name][()].astype(np.float32)), live, ns)
        fl = f.get("flags")
        if fl is not None and fl.shape[1:] == (len(labels), n_time) and fl.shape[0] >= ns:
            rows = fl[0:ns] == 0                                   # (ns, n_el, n_time)
            row_live = live[0:ns]                                  # (ns, n_time)
            counts = (rows & row_live[:, None, :]).sum(axis=0)     # (n_el, n_time)
            rec["flag_live"] = [int(x) for x in row_live.sum(axis=0)]
            rec["masked"] = {labels[e]: _bin_runs(counts[e])
                             for e in range(len(labels)) if counts[e].any()}
        if "fringestop_enabled" in f:
            rec["fringestop"] = int(np.asarray(f["fringestop_enabled"][()], bool).sum())
        if "digital_gains/update_id" in f:
            ids = [x.decode() if isinstance(x, bytes) else str(x)
                   for x in f["digital_gains/update_id"][()]]
            rec["gains"] = ids[-1] if ids else None
        if "config_json" in f:
            versions = set()
            for blob in f["config_json"][()]:
                try:
                    v = json.loads(blob).get("kotekan_version")
                except (ValueError, AttributeError):
                    continue
                if v:
                    versions.add(str(v))
            rec["versions"] = sorted(versions)
    return rec


def _chunks(d, sel: Selection, axes: Axes):
    """Yield ``(offset, chunk)`` over the selected channels, one chunk-sized
    group of consecutive channels at a time with **every** product in it:
    the overview pass."""
    cs = max(1, int(axes.chunk_rows))
    offset = 0
    for start, stop in _runs(sel.freq_idx):
        for lo in range(start, stop, cs):
            hi = min(lo + cs, stop)
            yield offset, d[lo:hi, :, :]
            offset += hi - lo


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
