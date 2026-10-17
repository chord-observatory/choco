"""kotekan — read the feed labels and N² autocorrelation from a kotekan file.

Read-only. Shared by bffs (to check the file against the flag axis) and the
power-outlier source (which judges the file's own axis and projects onto the
flag axis by label).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import h5py
import numpy as np

from choco import n2file


def element_labels(f: h5py.File) -> np.ndarray:
    """The element-axis labels of *f* in choco's names (``B4X``), read by
    :func:`choco.n2file.element_labels`.  Any other layout raises
    ``OSError``, so the job reports degraded (exit 2) rather than flag
    against a guessed axis, and heals once files from the current writer
    land.
    """
    try:
        return np.array(n2file.element_labels(f))
    except ValueError as e:
        raise OSError(f"N2 file is not in kotekan's per-element label layout "
                      f"(chord.2021.10+988, 2026-09-11 on): {e}") from e


def read_labels(path: str | Path) -> np.ndarray:
    """The element-axis labels from the kotekan file, one per element, in
    choco's names; any other label layout raises — see :func:`element_labels`.
    """
    with h5py.File(path, "r") as f:
        return element_labels(f)


def uniquify_labels(labels) -> np.ndarray:
    """Suffix repeated labels with their element index (Missing -> Missing[7]).

    Placeholder elements share the label ``Missing`` (``MissingX``/``MissingY``
    on a per-dish axis); state diffing and per-source projection key by
    label, so duplicates must be made per-element.  Unique labels pass
    through untouched.  The core applies this to the flag axis and the
    file-based sources to a file's own axis, so the two project onto each
    other by name.
    """
    from collections import Counter
    strs = [str(label) for label in labels]
    counts = Counter(strs)
    return np.array([f"{s}[{i}]" if counts[s] > 1 else s
                     for i, s in enumerate(strs)])


@dataclass(frozen=True)
class Frame:
    """The most recent block of autocorrelation data read from the kotekan file."""

    auto: np.ndarray    # (ntime, nfreq, nfeed) power
    weight: np.ndarray  # (ntime, nfreq, nfeed)
    valid: np.ndarray   # (ntime, nfreq) bool
    freq: np.ndarray    # (nfreq,)
    # (nfeed,) the file's own element axis in choco's names
    # (:func:`element_labels`).  A ``subset/`` file is a compact
    # ``DishInputs`` frame over the wired elements only (48 of the
    # correlator's 128), so this axis is a subset of the flag axis and
    # sources project their verdicts onto it by label, never by position.
    labels: np.ndarray | None = None
    # (nfeed,) bool: which feeds the file's product list carries an
    # autocorrelation for.  None means all of them (test frames).  A
    # subset layout (kotekan's DishInputs)
    # only correlates the wired elements — the rest have no data *by
    # construction*, which is different from a wired feed gone silent,
    # and sources must not read the gap as "dead".
    measured: np.ndarray | None = None
    # Where the window sits in the file: the file's row count, and how
    # many empty rows at its end were skipped to reach the newest row
    # that holds any frame (a stopped acquisition leaves such a tail).
    file_ntime: int | None = None
    tail_skipped: int = 0

    @property
    def ntime(self) -> int:
        return self.auto.shape[0]

    @property
    def nfeed(self) -> int:
        return self.auto.shape[2]


#: Chunk-aligned frequency blocks read per file when the band is sampled.
DEFAULT_BLOCKS = 8


def channel_blocks(freq: np.ndarray, chunk_rows: int, freq_lo: float | None,
                   freq_hi: float | None, blocks: int | None) -> np.ndarray:
    """Which channels to read: ``blocks`` chunk-aligned runs spread over
    the band, or the whole band when ``blocks`` is None or 0.

    ``vis`` is chunked ``(16 freq, every product, every time)`` since
    2026-10 (42 MB a chunk at 128 elements), so a read of the diagonal
    decompresses every chunk it touches whole: the cost of this source is
    the number of chunks it opens, not the number of channels it keeps.
    Eight 16-channel blocks spread over 400-800 MHz are 128 channels for
    the price of 8 chunks, where the whole band was 128 chunks (24 GB a
    run, four minutes on the link — every 30 s).

    Returns the sorted channel indices, each block's channels contiguous.
    """
    nfreq = freq.shape[0]
    band = np.ones(nfreq, dtype=bool)
    if freq_lo is not None:
        band &= freq >= freq_lo
    if freq_hi is not None:
        band &= freq <= freq_hi
    chans = np.flatnonzero(band)
    if not blocks or chans.size == 0:
        return chans
    cs = max(1, int(chunk_rows))
    starts = np.unique(chans // cs) * cs            # the aligned blocks the band touches
    if starts.size > blocks:
        pick = np.unique(np.round(np.linspace(0, starts.size - 1, int(blocks))).astype(int))
        starts = starts[pick]
    keep = np.zeros(nfreq, dtype=bool)
    for s in starts:
        keep[s:s + cs] = True
    return np.flatnonzero(keep & band)


def _runs(idx: np.ndarray) -> list[tuple[int, int]]:
    """``[(start, stop), ...]`` of the contiguous runs in sorted ``idx``."""
    if idx.size == 0:
        return []
    cuts = np.flatnonzero(np.diff(idx) != 1) + 1
    out = []
    for part in np.split(idx, cuts):
        out.append((int(part[0]), int(part[-1]) + 1))
    return out


def read_autocorr(path: str | Path, *, chunk: int = 16, freq_lo: float | None = None,
                  freq_hi: float | None = None, blocks: int | None = None) -> Frame | None:
    """Read the most recent ``chunk`` time rows of kotekan N² output as a Frame.

    The autocorrelation diagonal (``input_a == input_b``) is extracted from
    ``vis[freq, prod, time]``.  The feed axis is the element axis
    (:func:`element_labels`, one label per element); products beyond it
    are dropped.  Only the channels :func:`channel_blocks` picks are read
    (``freq_lo``/``freq_hi``/``blocks``; everything by default), and the
    frame's ``freq`` axis is those channels.

    Validity and weight both come from ``frames_added``: a delivered cell
    weighs 1, an empty one 0.  ``vis_weight`` is not read — it holds no
    delivered cell at zero that ``frames_added`` and the file's own
    ``flags`` do not already explain (checked on the 2026-10-17 files),
    and it is 8 GB a file.  The root-level ``flags`` dataset is kotekan's
    own per-input flag state, downstream of this job's flagging, and
    deliberately unused (see the README appendix on the latch problem).

    The window ends at the newest row with any frame
    (``frames_added[freq, time] > 0``), not at the file's last row: a
    stopped acquisition leaves a tail of empty rows, and judging feeds on
    those would read every feed as dead.  ``Frame.tail_skipped`` says how
    many were passed over.  Returns ``None`` if the file is missing, has
    no time rows, or no row holds a frame.
    """
    if not Path(path).exists():
        return None
    with h5py.File(path, "r") as f:
        labels = element_labels(f)
        freq_all = n2file.freq_axis(f)[0].astype(np.float32)
        a, b = n2file.prod_axis(f)
        nfeed = labels.shape[0]
        diag = np.nonzero((a == b) & (a < nfeed))[0]
        feed_idx = a[diag]

        vis = f["vis"]
        ntime = vis.shape[2]
        if ntime == 0:
            return None
        # frames_added[freq, time] tracks which (f, t) cells have data.
        # It is small (nfreq x ntime counters), so read it whole and end
        # the window at the newest row that holds any frame.
        frames_added = None
        hi = ntime
        if "frames_added" in f:
            frames_added = np.asarray(f["frames_added"][()])
            filled = np.flatnonzero(frames_added.sum(axis=0) > 0)
            if filled.size == 0:
                return None
            hi = int(filled[-1]) + 1
        tail_skipped = ntime - hi
        lo = max(0, hi - int(chunk))
        nrows = hi - lo

        chunk_rows = vis.chunks[0] if vis.chunks else 16
        chans = channel_blocks(freq_all, chunk_rows, freq_lo, freq_hi, blocks)
        freq = freq_all[chans]
        nfreq = chans.shape[0]
        # vis[freq, prod, time] -> (time, freq, prod), one read per block
        power = np.zeros((nrows, nfreq, diag.shape[0]), dtype=np.float32)
        at = 0
        for start, stop in _runs(chans):
            blk = np.real(vis[start:stop, diag, lo:hi]).astype(np.float32)
            power[:, at:at + stop - start, :] = blk.transpose(2, 0, 1)
            at += stop - start
        valid = (frames_added[chans, lo:hi].T > 0 if frames_added is not None
                 else np.ones((nrows, nfreq), dtype=bool))

        auto = np.zeros((nrows, nfreq, nfeed), dtype=np.float32)
        auto[..., feed_idx] = power
        weight = np.zeros_like(auto)
        weight[..., feed_idx] = valid[:, :, None]
        measured = np.zeros(nfeed, dtype=bool)
        measured[feed_idx] = True
    return Frame(auto=auto, weight=weight, valid=valid, freq=freq, labels=labels,
                 measured=measured, file_ntime=ntime, tail_skipped=tail_skipped)
