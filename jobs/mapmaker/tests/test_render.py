"""Accumulators to pixels."""

import numpy as np
import pytest

import render


def test_band_maps_sum_pols_and_floor_exposure():
    num = np.zeros((2, 3, 4, 10), np.float32)
    exp = np.zeros_like(num)
    num[:, :, 1, 5] = 2.0
    exp[:, :, 1, 5] = 1.0
    exp[:, :, 2, 7] = 0.001                      # below the floor
    m, ok = render.band_maps(num, exp, 0.02)
    assert m.shape == (3, 4, 10)
    assert m[0, 1, 5] == pytest.approx(2.0)      # 4 / 2
    assert np.isnan(m[0, 2, 7]) and not ok[0, 2, 7]


RED, GREEN, BLUE = np.array([[1.0, 0, 0], [0, 1.0, 0], [0, 0, 1.0]])
ARCSINH = {"mode": "arcsinh", "soft": 0.02, "max": 1.0}


def test_spectrum_colours_run_red_to_violet():
    c = render.spectrum_colours([300.0, 670.0, 1499.0], 300.0, 1500.0)
    assert c[0].argmax() == 0 and c[-1].argmax() == 2            # red at the bottom, blue at the top
    assert c[1, 1] > 0.9                                          # green in the middle
    assert np.allclose(c.max(axis=1), 1.0)                        # every hue at full brightness


def test_to_rgb_flips_ra_sums_hues_and_normalises_per_pixel():
    maps = np.full((3, 2, 4), np.nan, np.float32)
    maps[:, 0, 0] = 1.0                           # equal in every bin, first column -> last after the flip
    maps[0, 1, 3] = 5.0                           # above max: saturates, red only
    maps[1, 0, 1] = 1.0                           # one bin alone with data: its hue, full brightness
    rgb = render.to_rgb(maps, np.stack([RED, GREEN, BLUE]), ARCSINH)
    assert rgb.shape == (2, 4, 3) and rgb.dtype == np.uint8
    assert rgb[0, 3].tolist() == [255, 255, 255]                  # flat spectrum is white
    assert rgb[1, 0].tolist() == [255, 0, 0]
    assert rgb[0, 2].tolist() == [0, 255, 0]                      # a missing bin leaves no tint
    assert rgb[1, 3].tolist() == [0, 0, 0]                        # no data anywhere: black


def test_to_rgb_needs_one_colour_per_bin():
    with pytest.raises(ValueError):
        render.to_rgb(np.zeros((2, 2, 2)), np.stack([RED, GREEN, BLUE]), ARCSINH)


def test_stretch_modes_share_one_curve_and_keep_noise_black():
    rng = np.random.default_rng(0)
    maps = rng.normal(0, 0.001, (4, 10, 50)).astype(np.float32)
    maps[:, 5, 10] = 1.0                          # bright
    maps[:, 5, 30] = 0.02                         # faint, 20 sigma
    valid = np.ones_like(maps, bool)
    for cfg in ({"mode": "equalize", "floor_sigma": 3.0}, {"mode": "log", "soft": 0.02, "max": 1.0}, ARCSINH):
        y = render.stretch(maps, valid, cfg)
        assert y[:, 5, 10].min() > 0.99 and (y[:, 5, 10] == y[0, 5, 10]).all(), cfg
        assert 0.05 < y[0, 5, 30] < 0.9, cfg
        assert y[0, 5, 30] == y[3, 5, 30]
    y = render.stretch(maps, valid, {"mode": "equalize", "floor_sigma": 3.0})
    assert (y[:, 0, :] == 0).mean() > 0.95                        # the noise floor stays black
    assert y[0, 5, 30] > 0.3                                      # equalisation lifts the faint source most
    with pytest.raises(ValueError):
        render.stretch(maps, valid, {"mode": "gamma"})


def test_pngs_are_written_atomically(tmp_path):
    rgb = np.zeros((3, 8, 3), np.uint8)
    render.write_png_atomic(tmp_path / "s.png", rgb, row_repeat=4)
    edges = np.geomspace(300, 1500, 5)
    render.write_figure_atomic(tmp_path / "f.png", rgb, np.linspace(30, 50, 3), "t",
                               edges, render.spectrum_colours(edges[:-1], 300, 1500), [("X", 10.0, 40.0)],
                               coverage=np.linspace(0, 1, 8))
    assert (tmp_path / "s.png").stat().st_size > 0 and (tmp_path / "f.png").stat().st_size > 0
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".tmp-")]


def test_context_image_pastes_the_strip_and_leaves_gaps_transparent(tmp_path, tiny_sky):
    sky, extent = render.sky_context(tiny_sky, step_deg=2.0)
    assert extent == [360.0, 0.0, -40.0, 90.0]
    assert sky.shape == (65, 180, 3) and np.isfinite(sky).all()
    rgb = np.full((3, 8, 3), 200, np.uint8)
    valid = np.ones((3, 8), bool)
    valid[:, :2] = False                                  # the lowest RA bins were never observed
    out = tmp_path / "ctx.png"
    render.write_context_atomic(out, tiny_sky, rgb, np.linspace(30, 50, 3), "t", [("X", 10.0, 40.0)],
                                valid=valid)
    assert out.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".tmp-")]


def test_context_image_needs_the_map(tmp_path):
    with pytest.raises(OSError):
        render.write_context_atomic(tmp_path / "ctx.png", tmp_path / "absent.fits",
                                    np.zeros((3, 8, 3), np.uint8), np.linspace(30, 50, 3), "t")
    assert not (tmp_path / "ctx.png").exists()


def test_stamp():
    assert render.stamp(None) == "no data yet"
    assert render.stamp(1791083203.0) == "2026-10-04 03:06 UTC"
