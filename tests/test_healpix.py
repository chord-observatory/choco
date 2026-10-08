"""Tests for choco.healpix: the RING lookup and the FITS reader."""

import numpy as np
import pytest

from choco import healpix


def write_healpix(path, values, nside, ordering="RING", coordsys="GALACTIC"):
    """A minimal HEALPix FITS file, laid out like LAMBDA's."""
    from astropy.io import fits
    col = fits.Column(name="TEMPERATURE", format="E", unit="K",
                      array=np.asarray(values, dtype=np.float32))
    hdu = fits.BinTableHDU.from_columns([col])
    hdu.header.update({"PIXTYPE": "HEALPIX", "ORDERING": ordering, "NSIDE": nside,
                       "COORDSYS": coordsys, "BAD_DATA": -1.6375e30})
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path)
    return path


def _pix2ang_ring(nside, p):
    """Pixel centre of RING pixel *p*: an independent transcription of
    HEALPix's pix2ang_ring, to check ang2pix_ring against."""
    npix, ncap = 12 * nside * nside, 2 * nside * (nside - 1)
    if p < ncap:
        i = int((1 + np.sqrt(1 + 2 * p)) / 2)
        j = p - 2 * i * (i - 1) + 1
        return np.arccos(1 - i * i / (3 * nside * nside)), (j - 0.5) * np.pi / (2 * i)
    if p < npix - ncap:
        q = p - ncap
        i, j = q // (4 * nside) + nside, q % (4 * nside) + 1
        fodd = 1.0 if (i + nside) % 2 else 0.5
        return np.arccos(4 / 3 - 2 * i / (3 * nside)), (j - fodd) * np.pi / (2 * nside)
    q = npix - p
    i = int((1 + np.sqrt(2 * q - 1)) / 2)
    j = 4 * i + 1 - (q - 2 * i * (i - 1))
    return np.arccos(-1 + i * i / (3 * nside * nside)), (j - 0.5) * np.pi / (2 * i)


@pytest.mark.parametrize("nside", [1, 2, 4, 16])
def test_ang2pix_finds_every_pixel_centre(nside):
    theta, phi = np.array([_pix2ang_ring(nside, p) for p in range(12 * nside * nside)]).T
    assert (healpix.ang2pix_ring(nside, theta, phi) == np.arange(12 * nside * nside)).all()


def test_ang2pix_known_pixels():
    # NSIDE=1: near the north pole at small phi is pixel 0; (b=0, l=0) is pixel 4
    assert healpix.ang2pix_ring(1, np.array([0.01]), np.array([0.1]))[0] == 0
    assert healpix.ang2pix_ring(1, np.array([np.pi / 2]), np.array([0.0]))[0] == 4


def test_sample_galactic_is_b_up():
    nside = 8
    m = np.arange(12 * nside * nside, dtype=float)
    assert healpix.sample_galactic(m, nside, 0.0, np.radians(89.0)) < 4        # the north cap
    assert healpix.sample_galactic(m, nside, 0.0, np.radians(-89.0)) >= m.size - 4


def test_bad_pixels_become_nan(tmp_path):
    nside = 4
    vals = np.full(12 * nside * nside, 30.0)
    vals[:3] = -1.6375e30
    m, ns = healpix.load_healpix(write_healpix(tmp_path / "m.fits", vals, nside))
    assert ns == nside and np.isnan(m[:3]).all() and np.isfinite(m[3:]).all()


@pytest.mark.parametrize("kw", [{"ordering": "NESTED"}, {"coordsys": "C"}])
def test_other_layouts_are_refused(tmp_path, kw):
    path = write_healpix(tmp_path / "m.fits", np.ones(12 * 16), 4, **kw)
    with pytest.raises(ValueError, match="RING-ordered Galactic"):
        healpix.load_healpix(path)


def test_wrong_pixel_count_is_refused(tmp_path):
    path = write_healpix(tmp_path / "m.fits", np.ones(100), 4)
    with pytest.raises(ValueError, match="not a full sky"):
        healpix.load_healpix(path)


def test_missing_file_is_oserror(tmp_path):
    with pytest.raises(OSError):
        healpix.load_healpix(tmp_path / "absent.fits")


def test_log_stretch_spans_the_sky_and_handles_a_flat_map():
    m = np.logspace(1, 3, 1000)
    x = healpix.log_stretch(m, m)
    assert x.min() == 0 and x.max() == 1 and np.all(np.diff(x) >= 0)
    flat = np.full(10, 5.0)
    assert np.all(healpix.log_stretch(flat, flat) == 0)
    assert np.isnan(healpix.log_stretch(m, np.array([-1.0]))[0])


def test_default_path_is_under_the_skymap_job(tmp_path):
    assert healpix.default_sky_map(tmp_path) == str(tmp_path / "skymap" / healpix.SKY_MAP_FILE)


@pytest.mark.parametrize("ra, dec, l, b", [
    (266.40499, -28.93617, 0.0, 0.0),          # the Galactic centre
    (192.85948, 27.12825, None, 90.0),         # the north Galactic pole (l undefined)
    (299.86815, 40.73392, 76.19, 5.76),        # Cyg A
    (83.63308, 22.01450, 184.56, -5.78),       # Tau A
])
def test_equatorial_to_galactic(ra, dec, l, b):
    lg, bg = healpix.equatorial_to_galactic(np.radians(ra), np.radians(dec))
    assert np.degrees(bg) == pytest.approx(b, abs=0.02)
    if l is not None:
        assert abs((np.degrees(lg) - l + 180) % 360 - 180) < 0.02
