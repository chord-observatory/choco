#!/usr/bin/env python3
"""Append-only 8-bit palette PNG — the waterfall image format.

One image per correlation product per acquisition.  **Rows are time and
columns are frequency**, which is the whole trick: kotekan's files land
one at a time and never change, so a new file is *appended* as scanlines
and no byte already written is ever rewritten.  Frequency-major would
compress ~10% better and cannot be appended to at all.

Two more choices define the format:

* **Index 0 means missing** and is drawn opaque black, so a dead
  channel, a gap, or a fully-lost integration reads as "nothing here"
  on any page background.  Levels 1..255 are linear in log|V| between a
  lo/hi frozen per (acquisition, product).
* **Histogram equalization lives in the palette, not the pixels.**
  Equalization is a monotonic value->colour remap, so it can be carried
  by the 256-entry ``PLTE`` instead of the indices.  The stored pixels
  therefore keep the compressible linear distribution (equalizing them
  costs ~15%, being maximum-entropy by construction) *and* — the reason
  it matters here — they never go stale: the mapping is recomputed from
  the whole distribution at render time, while the acquisition is still
  growing.  ``set_palette`` rewrites it in place in constant time.

The append itself: truncate the fixed-size terminator, write one new
``IDAT`` holding a fresh raw-deflate segment, patch the 4-byte height in
``IHDR``, and write a new terminator carrying the combined adler32.
Segments are independent (``wbits=-15`` ended with ``Z_SYNC_FLUSH``), so
no encoder state has to survive between processes; the first row of each
segment uses the Sub filter for the same reason, since Up would need the
previous segment's last row.

``append`` seeks to the *recorded* offset rather than to EOF, so a run
that died mid-append is truncated back and redone rather than needing a
separate recovery pass.  That makes it idempotent from any recorded
state, which is what lets the caller write its index JSON afterwards.
"""

from __future__ import annotations

import base64
import struct
import zlib
from dataclasses import dataclass, asdict

import numpy as np

PNG_SIG = b"\x89PNG\r\n\x1a\n"

#: Palette entries reserved for data.  Index 0 is ``MISSING``.
N_LEVELS = 255
MISSING = 0

#: Byte offset of the palette payload.  Fixed because the signature,
#: IHDR and the PLTE header are all fixed size and the palette is always
#: written with a full 256 entries -- which is what makes ``set_palette``
#: a constant-time seek-and-write rather than a rewrite of the file.
_PLTE_OFF = len(PNG_SIG) + 25 + 8
_PLTE_LEN = 256 * 3

# viridis (matplotlib), sampled to 256 RGB triples.  Embedded rather
# than imported: choco's dependency set is deliberately small, and a
# colormap is 768 bytes of data.  Its floor is a dark purple, so the
# opaque-black MISSING entry stays distinguishable from the lowest data
# level.
_VIRIDIS_B64 = (
    "RAFURAJWRQRXRQVZRgdaRghcRgpdRgteRw1gRw5hRxBjRxFkRxNlSBRnSBZoSBdpSBhq"
    "SBpsSBttSBxuSB1vSB9wSCBxSCFzSCN0SCR1SCV2SCZ3SCh4SCl5Ryp6Ryx6Ry17Ry58"
    "Ry99RjB+RjJ+RjN/RjSARTWBRTeBRTiCRDmDRDqDRDuEQz2EQz6FQj+FQkCGQkGGQUKH"
    "QUSHQEWIQEaIP0eIP0iJPkmJPkqJPkyKPU2KPU6KPE+KPFCLO1GLO1KLOlOLOlSMOVWM"
    "OVaMOFiMOFmMN1qMN1uNNlyNNl2NNV6NNV+NNGCNNGGNM2KNM2ONMmSOMmWOMWaOMWeO"
    "MWiOMGmOMGqOL2uOL2yOLm2OLm6OLm+OLXCOLXGOLHGOLHKOLHOOK3SOK3WOKnaOKneO"
    "KniOKXmOKXqOKXuOKHyOKH2OJ36OJ3+OJ4COJoGOJoKOJoKOJYOOJYSOJYWOJIaOJIeO"
    "I4iOI4mOI4qNIouNIoyNIo2NIY6NIY+NIZCNIZGMIJKMIJKMIJOMH5SMH5WLH5aLH5eL"
    "H5iLH5mKH5qKHpuKHpyJHp2JH56JH5+IH6CIH6GIH6GHH6KHIKOGIKSGIaWFIaaFIqeF"
    "IqiEI6mDJKqDJauCJayCJq2BJ62BKK6AKa9/KrB/LLF+LbJ9LrN8L7R8MbV7MrZ6NLZ5"
    "Nbd5N7h4OLl3Orp2O7t1Pbx0P7xzQL1yQr5xRL9wRsBvSMFuSsFtTMJsTsNrUMRqUsVp"
    "VMVoVsZnWMdlWshkXMhjXsliYMpgY8tfZcteZ8xcac1bbM1abs5YcM9Xc9BWddBUd9FT"
    "etFRfNJQf9NOgdNNhNRLhtVJidVIi9ZGjtZFkNdDk9dBldhAmNg+m9k8ndk7oNo5oto3"
    "pds2qNs0qtwyrdwwsN0vst0ttd4ruN4put4ovd8mwN8lwt8jxeAhyOAgyuEfzeEd0OEc"
    "0uIb1eIa2OIZ2uMZ3eMY3+MY4uQY5eQZ5+QZ6uUa7OUb7+Uc8eUd9OYe9uYg+OYh++cj"
    "/ecl"
)

VIRIDIS = np.frombuffer(base64.b64decode(_VIRIDIS_B64), np.uint8).reshape(256, 3)


@dataclass
class WfState:
    """Everything needed to append to an image, or to rebuild its tail.

    Persisted per product in the acquisition's index JSON.  ``nbytes`` is
    the size of the last *completed* write; ``append`` trusts it over the
    file's actual length so a torn write is simply overwritten.
    """

    width: int
    rows: int
    nbytes: int
    adler: int

    to_dict = asdict

    @classmethod
    def from_dict(cls, d: dict) -> "WfState":
        return cls(int(d["width"]), int(d["rows"]),
                   int(d["nbytes"]), int(d["adler"]))


def _chunk(typ: bytes, data: bytes) -> bytes:
    body = typ + data
    return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body))


def _ihdr(width: int, height: int) -> bytes:
    # bit depth 8, colour type 3 (palette), no interlace
    return _chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 3, 0, 0, 0))


def _terminator(adler: int) -> bytes:
    """Final empty deflate block + adler32, then IEND.

    ``\x03\x00`` is a BFINAL fixed-Huffman block containing only
    end-of-block, which is how the zlib stream is closed without the
    compressor object that produced the earlier segments.
    """
    return _chunk(b"IDAT", b"\x03\x00" + struct.pack(">I", adler)) + _chunk(b"IEND", b"")


#: Size of the terminator.  Constant, so the append knows where to cut.
TAIL_LEN = len(_terminator(0))


def _filter_segment(rows: np.ndarray) -> bytes:
    """Filter scanlines for one appended segment.

    Row 0 uses Sub (predict from the previous *frequency* channel) so the
    segment needs nothing from the segment before it; the rest use Up
    (predict from the previous *time* sample), which is the stronger
    predictor for a waterfall.
    """
    if rows.ndim != 2:
        raise ValueError(f"expected 2-D rows, got shape {rows.shape}")
    if rows.shape[0] == 0 or rows.shape[1] == 0:
        raise ValueError(f"empty scanlines: {rows.shape}")
    a = np.ascontiguousarray(rows, dtype=np.uint8)
    n, w = a.shape
    out = np.empty((n, w + 1), np.uint8)
    out[0, 0] = 1                                     # Sub
    out[0, 1] = a[0, 0]
    out[0, 2:] = (a[0, 1:].astype(np.int16) - a[0, :-1]).astype(np.uint8)
    if n > 1:
        out[1:, 0] = 2                                # Up
        out[1:, 1:] = (a[1:].astype(np.int16) - a[:-1]).astype(np.uint8)
    return out.tobytes()


#: Deflate level.  Measured over 132 real products: level 9 runs at
#: 5.9 MB/s and level 6 at 20.3 MB/s for 0.58% more bytes, which turns a
#: ~12 s write per source file into ~3.5 s and costs ~1 GB across the
#: whole archive.  Level 4 is faster again (+2.2% size) and is worth
#: passing explicitly for a long backfill.
DEFAULT_LEVEL = 6


def _deflate_segment(raw: bytes, first: bool, level: int = DEFAULT_LEVEL) -> bytes:
    """One independently decodable deflate segment.

    ``Z_SYNC_FLUSH`` ends it at a byte boundary without setting BFINAL,
    so segments concatenate into a single valid stream.  The zlib header
    is only emitted for the first one.
    """
    co = zlib.compressobj(level, zlib.DEFLATED, -15)
    body = co.compress(raw) + co.flush(zlib.Z_SYNC_FLUSH)
    return (b"\x78\x01" + body) if first else body


def create(path, rows: np.ndarray, palette: np.ndarray,
           level: int = DEFAULT_LEVEL) -> WfState:
    """Write a new image whose first scanlines are ``rows``."""
    pal = np.ascontiguousarray(palette, dtype=np.uint8)
    if pal.shape != (256, 3):
        raise ValueError(f"palette must be (256, 3), got {pal.shape}")
    raw = _filter_segment(rows)
    adler = zlib.adler32(raw)
    height, width = rows.shape
    with open(path, "wb") as f:
        f.write(PNG_SIG + _ihdr(width, height)
                + _chunk(b"PLTE", pal.tobytes())
                + _chunk(b"IDAT", _deflate_segment(raw, first=True, level=level))
                + _terminator(adler))
        nbytes = f.tell()
    return WfState(width=width, rows=height, nbytes=nbytes, adler=adler)


def append(path, rows: np.ndarray, st: WfState,
           level: int = DEFAULT_LEVEL) -> WfState:
    """Append scanlines, returning the new state.

    Cuts at ``st.nbytes - TAIL_LEN`` rather than at EOF, so this is
    idempotent from any recorded state: a previous run that died partway
    through its append is simply overwritten.
    """
    if rows.shape[1] != st.width:
        raise ValueError(
            f"width changed: image is {st.width}, appending {rows.shape[1]}")
    cut = st.nbytes - TAIL_LEN
    raw = _filter_segment(rows)
    adler = zlib.adler32(raw, st.adler)
    height = st.rows + rows.shape[0]
    seg = _chunk(b"IDAT", _deflate_segment(raw, first=False, level=level))
    with open(path, "r+b") as f:
        f.seek(0, 2)
        if f.tell() < cut:
            raise ValueError(
                f"{path}: {f.tell()} bytes, state records {st.nbytes}; "
                "file is shorter than its recorded state and cannot be repaired")
        f.seek(cut)
        f.truncate()
        f.write(seg + _terminator(adler))
        f.seek(len(PNG_SIG))
        f.write(_ihdr(st.width, height))
    return WfState(width=st.width, rows=height,
                   nbytes=cut + len(seg) + TAIL_LEN, adler=adler)


def set_palette(path, palette: np.ndarray) -> None:
    """Replace the palette in place, leaving every pixel byte untouched.

    Constant time: the palette always has 256 entries, so it sits at a
    fixed offset.  This is how a re-derived equalization reaches images
    that were written days earlier.

    Images from before the black-missing convention carry a ``tRNS``
    chunk marking index 0 transparent; it sits right after the palette,
    so the same pass rewrites its alpha opaque and old images pick the
    convention up on their next palette refresh.
    """
    pal = np.ascontiguousarray(palette, dtype=np.uint8)
    if pal.shape != (256, 3):
        raise ValueError(f"palette must be (256, 3), got {pal.shape}")
    data = pal.tobytes()
    with open(path, "r+b") as f:
        f.seek(_PLTE_OFF - 8)
        if f.read(8) != struct.pack(">I", _PLTE_LEN) + b"PLTE":
            raise ValueError(f"{path}: no 256-entry PLTE where one was expected")
        f.seek(_PLTE_OFF)
        f.write(data)
        f.write(struct.pack(">I", zlib.crc32(b"PLTE" + data)))
        if f.read(8) == struct.pack(">I", 1) + b"tRNS":
            f.write(b"\xff" + struct.pack(">I", zlib.crc32(b"tRNS\xff")))


def read(path) -> tuple[np.ndarray, np.ndarray]:
    """Decode to ``(indices, palette)``, validating every CRC and the adler32.

    Deliberately independent of the writer above -- it is the check that
    the appended file is a real PNG and not merely one this module can
    read back.
    """
    blob = open(path, "rb").read()
    if blob[:8] != PNG_SIG:
        raise ValueError(f"{path}: not a PNG")
    pos, idat, hdr, pal = 8, [], None, None
    while pos < len(blob):
        (length,) = struct.unpack(">I", blob[pos:pos + 4])
        typ = blob[pos + 4:pos + 8]
        data = blob[pos + 8:pos + 8 + length]
        (want,) = struct.unpack(">I", blob[pos + 8 + length:pos + 12 + length])
        if zlib.crc32(typ + data) != want:
            raise ValueError(f"{path}: bad CRC on {typ.decode()} at {pos}")
        if typ == b"IHDR":
            hdr = struct.unpack(">IIBBBBB", data)
        elif typ == b"PLTE":
            pal = np.frombuffer(data, np.uint8).reshape(-1, 3)
        elif typ == b"IDAT":
            idat.append(data)
        pos += 12 + length
    if hdr is None or pal is None:
        raise ValueError(f"{path}: missing IHDR or PLTE")
    width, height = hdr[0], hdr[1]
    raw = zlib.decompress(b"".join(idat))              # also verifies adler32
    if len(raw) != height * (width + 1):
        raise ValueError(
            f"{path}: {len(raw)} scanline bytes, expected {height * (width + 1)}")
    lines = np.frombuffer(raw, np.uint8).reshape(height, width + 1)
    out = np.empty((height, width), np.uint8)
    prev = np.zeros(width, np.uint8)
    for r in range(height):
        ftype, line = lines[r, 0], lines[r, 1:]
        if ftype == 0:
            cur = line.copy()
        elif ftype == 1:                               # Sub: mod-256 prefix sum
            cur = (np.cumsum(line.astype(np.uint32)) % 256).astype(np.uint8)
        elif ftype == 2:                               # Up
            cur = (line + prev).astype(np.uint8)
        else:
            raise ValueError(f"{path}: unsupported filter {ftype} on row {r}")
        out[r] = cur
        prev = cur
    return out, pal


# --- value -> index, and index -> colour ---------------------------------

def quantize(mag: np.ndarray, lo: float, hi: float,
             valid: np.ndarray | None = None) -> np.ndarray:
    """|V| -> palette indices, linear in log between ``lo`` and ``hi``.

    Non-finite, non-positive, and anything ``valid`` excludes become
    ``MISSING``.  Values outside [lo, hi] clamp rather than blank: they
    are real measurements, just off the frozen scale.
    """
    out = np.zeros(mag.shape, np.uint8)
    ok = np.isfinite(mag) & (mag > 0)
    if valid is not None:
        ok &= valid
    if not (lo > 0 and hi > lo) or not ok.any():
        return out
    lo_l, hi_l = np.log10(lo), np.log10(hi)
    frac = (np.log10(mag[ok]) - lo_l) / (hi_l - lo_l)
    out[ok] = 1 + (np.clip(frac, 0.0, 1.0) * (N_LEVELS - 1)).astype(np.uint8)
    return out


def palette(warp: np.ndarray | None = None,
            base: np.ndarray = VIRIDIS,
            missing: tuple = (0, 0, 0)) -> np.ndarray:
    """Build the 256-entry palette.

    ``warp`` is an optional length-``N_LEVELS`` array of positions in
    [0, 1] giving where each *index* lands on the colormap -- the
    equalization.  Without it the ramp is linear, which matches the
    indices and shows the raw log stretch.
    """
    lut = np.zeros((256, 3), np.uint8)
    lut[MISSING] = missing
    if warp is None:
        pick = np.linspace(0, len(base) - 1, N_LEVELS)
    else:
        warp = np.asarray(warp, dtype=float)
        if warp.shape != (N_LEVELS,):
            raise ValueError(f"warp must be ({N_LEVELS},), got {warp.shape}")
        pick = np.clip(warp, 0.0, 1.0) * (len(base) - 1)
    lut[1:] = base[np.rint(pick).astype(int)]
    return lut


def index_counts(indices: np.ndarray, out: np.ndarray | None = None) -> np.ndarray:
    """Per-product histogram of palette indices, ``(n_prod, 256)`` int64.

    Accumulated across an acquisition, this is the only thing the
    equalization needs — so nothing has to keep raw visibilities, or
    re-read images already written, to keep the display honest as the
    acquisition grows.
    """
    if indices.ndim != 3:
        raise ValueError(f"expected (n_prod, n_time, n_freq), got {indices.shape}")
    n_prod = indices.shape[0]
    if out is None:
        out = np.zeros((n_prod, 256), np.int64)
    elif out.shape != (n_prod, 256):
        raise ValueError(f"out is {out.shape}, expected {(n_prod, 256)}")
    flat = indices.reshape(n_prod, -1)
    for p in range(n_prod):
        out[p] += np.bincount(flat[p], minlength=256)
    return out


def warp_from_counts(counts: np.ndarray) -> np.ndarray:
    """Equalization warp from an index histogram.

    Places each level at its own quantile, so colour is spent where the
    values actually are.  ``MISSING`` is excluded — it is not a value,
    and counting it would drag every real level toward one end in
    proportion to how much of the band is unfed.

    Levels are placed at the *centre* of their share of the distribution
    rather than its edge, which keeps the two ends of the ramp reachable
    instead of collapsing the first and last levels onto the extremes.
    """
    c = np.asarray(counts, dtype=float)
    if c.shape != (256,):
        raise ValueError(f"counts must be (256,), got {c.shape}")
    data = c[1:]
    total = data.sum()
    if total <= 0:
        return np.linspace(0.0, 1.0, N_LEVELS)
    return np.clip((np.cumsum(data) - 0.5 * data) / total, 0.0, 1.0)
