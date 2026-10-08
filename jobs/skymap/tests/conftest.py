"""Shared fixture: the job's state directory is a scratch directory.

``job_state_dir`` honours systemd's ``$STATE_DIRECTORY``; setting it here
keeps every ``main()`` run in the tests out of /var/lib/choco, where the
host running them may have real job state.
"""

import numpy as np
import pytest


def write_healpix(path, values, nside, ordering="RING", coordsys="GALACTIC"):
    """A minimal HEALPix FITS file, laid out like LAMBDA's: one float
    column in a binary-table extension, the map in the header keywords."""
    from astropy.io import fits
    col = fits.Column(name="TEMPERATURE", format="E", unit="K",
                      array=np.asarray(values, dtype=np.float32))
    hdu = fits.BinTableHDU.from_columns([col])
    hdu.header.update({"PIXTYPE": "HEALPIX", "ORDERING": ordering, "NSIDE": nside,
                       "COORDSYS": coordsys, "BAD_DATA": -1.6375e30})
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path)
    return path


@pytest.fixture
def tiny_sky(tmp_path):
    """An NSIDE=4 map: warm everywhere, so the backdrop has a stretch."""
    nside = 4
    vals = 20.0 + np.arange(12 * nside * nside) % 7
    return write_healpix(tmp_path / "tiny.fits", vals, nside)


@pytest.fixture(autouse=True)
def scratch_state_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("STATE_DIRECTORY", str(tmp_path / "state"))
    return tmp_path / "state"
