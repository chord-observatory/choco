"""Tests for the append-only waterfall PNG.

The load-bearing property is that appending is *pixel-exact* and
*idempotent from the recorded state* — everything else in the job design
(one growing image per product, an index JSON written after the appends)
rests on those two.
"""

import struct
import zlib

import numpy as np
import pytest

import wfpng


def _rows(n, w, seed=0):
    """Indices in 1..255, i.e. never the reserved MISSING value."""
    return np.random.default_rng(seed).integers(1, 256, (n, w), dtype=np.uint8)


@pytest.fixture
def pal():
    return wfpng.palette()


# --- round trips ---------------------------------------------------------

def test_create_round_trips(tmp_path, pal):
    a = _rows(7, 40)
    p = tmp_path / "a.png"
    st = wfpng.create(p, a, pal)
    got, got_pal = wfpng.read(p)
    assert np.array_equal(got, a)
    assert np.array_equal(got_pal, pal)
    assert (st.rows, st.width) == (7, 40)
    assert st.nbytes == p.stat().st_size


def test_append_is_pixel_exact(tmp_path, pal):
    """Four appends, one wide image — the shape the job actually writes."""
    segs = [_rows(20, 6145, seed=i) for i in range(5)]
    p = tmp_path / "w.png"
    st = wfpng.create(p, segs[0], pal)
    for s in segs[1:]:
        st = wfpng.append(p, s, st)
    got, _ = wfpng.read(p)
    assert np.array_equal(got, np.vstack(segs))
    assert st.rows == 100
    assert st.nbytes == p.stat().st_size


def test_append_one_row_at_a_time(tmp_path, pal):
    a = _rows(12, 33)
    p = tmp_path / "b.png"
    st = wfpng.create(p, a[:1], pal)
    for i in range(1, len(a)):
        st = wfpng.append(p, a[i:i + 1], st)
    got, _ = wfpng.read(p)
    assert np.array_equal(got, a)


def test_missing_index_survives(tmp_path, pal):
    """0 must round-trip; it is the only value with meaning of its own."""
    a = _rows(4, 20)
    a[1, :] = wfpng.MISSING
    a[:, 3] = wfpng.MISSING
    p = tmp_path / "m.png"
    wfpng.create(p, a, pal)
    got, _ = wfpng.read(p)
    assert np.array_equal(got, a)


def test_header_height_tracks_appends(tmp_path, pal):
    p = tmp_path / "h.png"
    st = wfpng.create(p, _rows(5, 16), pal)
    for _ in range(3):
        st = wfpng.append(p, _rows(5, 16), st)

    def height():
        with open(p, "rb") as f:
            return struct.unpack(">I", f.read(24)[20:24])[0]

    assert height() == 20 == st.rows


def test_terminator_is_a_valid_zlib_stream(tmp_path, pal):
    """zlib.decompress validates the adler32 the terminator carries."""
    p = tmp_path / "z.png"
    st = wfpng.create(p, _rows(6, 50), pal)
    st = wfpng.append(p, _rows(6, 50), st)
    blob = p.read_bytes()
    idat, pos = [], 8
    while pos < len(blob):
        (n,) = struct.unpack(">I", blob[pos:pos + 4])
        if blob[pos + 4:pos + 8] == b"IDAT":
            idat.append(blob[pos + 8:pos + 8 + n])
        pos += 12 + n
    assert len(idat) >= 3            # first segment, appended segment, terminator
    zlib.decompress(b"".join(idat))  # raises on a bad adler32


# --- crash safety --------------------------------------------------------

def test_append_is_idempotent_from_recorded_state(tmp_path, pal):
    """A run that died after appending but before saving state is redone.

    The caller writes its index JSON *after* the appends, so on the next
    run the file is one segment ahead of the state.  Appending from that
    older state must reproduce the same image, not a doubled one.
    """
    base, extra = _rows(10, 64, seed=1), _rows(10, 64, seed=2)
    p = tmp_path / "c.png"
    st0 = wfpng.create(p, base, pal)

    good = wfpng.append(p, extra, st0)     # the write that "succeeded"
    expected = wfpng.read(p)[0].copy()

    again = wfpng.append(p, extra, st0)    # replayed from the stale state
    assert np.array_equal(wfpng.read(p)[0], expected)
    assert (again.rows, again.nbytes, again.adler) == (good.rows, good.nbytes, good.adler)


def test_append_repairs_a_torn_tail(tmp_path, pal):
    """A file left without its terminator is still recoverable."""
    p = tmp_path / "t.png"
    st = wfpng.create(p, _rows(8, 24, seed=3), pal)
    with open(p, "r+b") as f:                       # simulate dying mid-write
        f.truncate(st.nbytes - wfpng.TAIL_LEN + 5)
    with pytest.raises(Exception):
        wfpng.read(p)

    nxt = _rows(8, 24, seed=4)
    st2 = wfpng.append(p, nxt, st)
    got, _ = wfpng.read(p)
    assert got.shape == (16, 24) and st2.rows == 16
    assert np.array_equal(got[8:], nxt)


def test_append_refuses_a_file_shorter_than_its_state(tmp_path, pal):
    p = tmp_path / "s.png"
    st = wfpng.create(p, _rows(5, 20), pal)
    with open(p, "r+b") as f:
        f.truncate(30)
    with pytest.raises(ValueError, match="shorter than its recorded state"):
        wfpng.append(p, _rows(5, 20), st)


def test_append_refuses_a_width_change(tmp_path, pal):
    """A frequency-axis change mid-acquisition cannot be represented."""
    p = tmp_path / "wd.png"
    st = wfpng.create(p, _rows(4, 20), pal)
    with pytest.raises(ValueError, match="width changed"):
        wfpng.append(p, _rows(4, 21), st)


def test_read_rejects_a_corrupt_chunk(tmp_path, pal):
    p = tmp_path / "x.png"
    wfpng.create(p, _rows(4, 20), pal)
    blob = bytearray(p.read_bytes())
    blob[60] ^= 0xFF
    p.write_bytes(blob)
    with pytest.raises(Exception):
        wfpng.read(p)


# --- palette -------------------------------------------------------------

def test_set_palette_leaves_pixels_untouched(tmp_path, pal):
    """The equalization is rewritten long after the pixels were written."""
    a = _rows(9, 100)
    p = tmp_path / "p.png"
    st = wfpng.create(p, a, pal)
    st = wfpng.append(p, _rows(9, 100, seed=9), st)
    before = wfpng.read(p)[0].copy()

    warped = wfpng.palette(warp=np.linspace(0, 1, wfpng.N_LEVELS) ** 2)
    wfpng.set_palette(p, warped)

    got, got_pal = wfpng.read(p)
    assert np.array_equal(got, before)
    assert np.array_equal(got_pal, warped)
    assert not np.array_equal(got_pal, pal)
    assert p.stat().st_size == st.nbytes      # in place, same size


def test_set_palette_is_constant_time_offset(tmp_path, pal):
    """The palette must sit where set_palette expects, whatever the size."""
    for n, w in ((1, 8), (50, 4096)):
        p = tmp_path / f"o{n}.png"
        wfpng.create(p, _rows(n, w), pal)
        wfpng.set_palette(p, wfpng.palette(warp=np.zeros(wfpng.N_LEVELS)))
        assert np.array_equal(wfpng.read(p)[1][1:], np.repeat(
            wfpng.VIRIDIS[:1], wfpng.N_LEVELS, axis=0))


def test_palette_reserves_missing_and_rejects_bad_warp():
    lut = wfpng.palette()
    assert lut.shape == (256, 3)
    assert tuple(lut[wfpng.MISSING]) == (0, 0, 0)     # missing draws black
    assert not np.array_equal(lut[1], lut[255])
    with pytest.raises(ValueError, match="warp must be"):
        wfpng.palette(warp=np.zeros(7))


def test_no_transparency_in_new_images(tmp_path, pal):
    """Missing is opaque black now; new files carry no tRNS chunk."""
    p = tmp_path / "t.png"
    wfpng.create(p, _rows(3, 16), pal)
    assert b"tRNS" not in p.read_bytes()


def test_set_palette_makes_a_legacy_trns_opaque(tmp_path, pal):
    """Images from the transparent-missing era go opaque on refresh."""
    import struct
    import zlib
    p = tmp_path / "old.png"
    st = wfpng.create(p, _rows(5, 32), pal)
    # splice the tRNS chunk the old writer emitted back in after PLTE
    blob = p.read_bytes()
    plte_end = wfpng._PLTE_OFF + wfpng._PLTE_LEN + 4
    trns = wfpng._chunk(b"tRNS", bytes([0]))
    p.write_bytes(blob[:plte_end] + trns + blob[plte_end:])

    wfpng.set_palette(p, wfpng.palette())

    blob = p.read_bytes()
    at = blob.index(b"tRNS") + 4
    assert blob[at] == 0xFF                           # alpha now opaque
    (crc,) = struct.unpack(">I", blob[at + 1:at + 5])
    assert crc == zlib.crc32(b"tRNS\xff")
    got, _ = wfpng.read(p)                            # still a valid PNG
    assert got.shape == (5, 32)
    assert p.stat().st_size == st.nbytes + len(trns)


def test_create_rejects_a_bad_palette(tmp_path):
    with pytest.raises(ValueError, match=r"palette must be"):
        wfpng.create(tmp_path / "q.png", _rows(2, 4), np.zeros((16, 3), np.uint8))


# --- quantization --------------------------------------------------------

def test_quantize_endpoints_and_blanking():
    mag = np.array([[1e-3, 1e0, 1e3, np.nan, 0.0, -1.0]])
    q = wfpng.quantize(mag, lo=1e-3, hi=1e3)
    assert q[0, 0] == 1                      # lo -> first data level
    assert q[0, 2] == wfpng.N_LEVELS         # hi -> last
    assert q[0, 1] == 1 + (wfpng.N_LEVELS - 1) // 2
    assert list(q[0, 3:]) == [0, 0, 0]       # NaN, zero, negative all missing


def test_quantize_clamps_rather_than_blanks():
    """Off-scale values are real measurements; only absent ones are missing."""
    mag = np.array([[1e-9, 1e9]])
    q = wfpng.quantize(mag, lo=1e-3, hi=1e3)
    assert list(q[0]) == [1, wfpng.N_LEVELS]


def test_quantize_honours_the_valid_mask():
    mag = np.full((2, 3), 5.0)
    valid = np.array([[True, False, True], [True, True, False]])
    q = wfpng.quantize(mag, 1.0, 10.0, valid=valid)
    assert (q[~valid] == wfpng.MISSING).all()
    assert (q[valid] > 0).all()


def test_quantize_survives_a_degenerate_range():
    mag = np.full((2, 2), 3.0)
    assert (wfpng.quantize(mag, lo=5.0, hi=5.0) == 0).all()
    assert (wfpng.quantize(mag, lo=0.0, hi=1.0) == 0).all()


# --- equalization -------------------------------------------------------

def test_index_counts_shape_and_totals():
    q = np.array([[[1, 1, 2], [3, 3, 3]]], np.uint8)
    c = wfpng.index_counts(q)
    assert c.shape == (1, 256)
    assert c[0, 1] == 2 and c[0, 2] == 1 and c[0, 3] == 3
    assert c.sum() == q.size


def test_index_counts_accumulate_across_files():
    """The acquisition's histogram is built one source file at a time."""
    a = np.full((2, 3, 4), 7, np.uint8)
    acc = wfpng.index_counts(a)
    wfpng.index_counts(a, out=acc)
    assert acc[0, 7] == 2 * 12
    assert acc.sum() == 2 * a.size


def test_index_counts_rejects_bad_shapes():
    with pytest.raises(ValueError, match="expected"):
        wfpng.index_counts(np.zeros((3, 4), np.uint8))
    with pytest.raises(ValueError, match="out is"):
        wfpng.index_counts(np.zeros((2, 2, 2), np.uint8), out=np.zeros((3, 256), np.int64))


def test_warp_from_counts_is_monotonic_in_unit_range():
    rng = np.random.default_rng(0)
    idx = np.clip(rng.normal(128, 30, 50_000), 1, 255).astype(np.uint8)
    w = wfpng.warp_from_counts(np.bincount(idx, minlength=256))
    assert w.shape == (wfpng.N_LEVELS,)
    assert (np.diff(w) >= 0).all()
    assert 0.0 <= w[0] and w[-1] <= 1.0


def test_warp_spends_colour_where_the_values_are():
    """A peaked histogram should get most of the ramp across its peak."""
    counts = np.zeros(256, np.int64)
    counts[100:110] = 10_000          # everything in ten adjacent levels
    counts[1] = counts[255] = 1
    w = wfpng.warp_from_counts(counts)
    across_peak = w[108] - w[98]
    assert across_peak > 0.9          # the peak owns nearly the whole colormap


def test_warp_ignores_missing():
    """A mostly-unfed band must not drag every real level to one end."""
    counts = np.zeros(256, np.int64)
    counts[wfpng.MISSING] = 10_000_000
    counts[50] = counts[200] = 500
    w = wfpng.warp_from_counts(counts)
    assert w[49] == pytest.approx(0.25, abs=0.01)   # midpoint of the first half
    assert w[199] == pytest.approx(0.75, abs=0.01)


def test_warp_from_empty_counts_is_linear():
    w = wfpng.warp_from_counts(np.zeros(256, np.int64))
    assert np.allclose(w, np.linspace(0.0, 1.0, wfpng.N_LEVELS))


def test_warp_rejects_a_bad_histogram():
    with pytest.raises(ValueError, match="counts must be"):
        wfpng.warp_from_counts(np.zeros(10))


def test_warp_drives_the_palette_end_to_end(tmp_path, pal):
    """Counts -> warp -> palette -> set_palette, pixels untouched."""
    a = _rows(6, 64)
    p = tmp_path / "eq.png"
    st = wfpng.create(p, a, pal)
    before = wfpng.read(p)[0].copy()
    counts = wfpng.index_counts(a[None, :, :])[0]
    wfpng.set_palette(p, wfpng.palette(warp=wfpng.warp_from_counts(counts)))
    got, got_pal = wfpng.read(p)
    assert np.array_equal(got, before)
    assert not np.array_equal(got_pal, pal)
    assert p.stat().st_size == st.nbytes


# --- compression level ---------------------------------------------------

def test_level_changes_size_but_not_pixels(tmp_path, pal):
    a = np.repeat(_rows(30, 300), 3, axis=0)      # compressible
    sizes = {}
    for lvl in (1, 6, 9):
        p = tmp_path / f"l{lvl}.png"
        st = wfpng.create(p, a[:45], pal, level=lvl)
        st = wfpng.append(p, a[45:], st, level=lvl)
        assert np.array_equal(wfpng.read(p)[0], a)
        sizes[lvl] = st.nbytes
    assert sizes[9] <= sizes[6] <= sizes[1]
