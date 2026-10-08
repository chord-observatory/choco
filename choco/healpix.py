"""HEALPix all-sky maps for the jobs: read a FITS map, look pixels up.

The skymap's backdrop and the mapmaker's context image both draw the
408 MHz all-sky map (Haslam et al. 1982, destriped by Remazeilles et al.
2015), a 12.6 MB HEALPix FITS that ``choco.sh install`` fetches into
``jobs/skymap/`` (:data:`SKY_MAP_FILE`).  Jobs only: numpy, plus
astropy's FITS reader on demand; the web process never imports this.
No healpy -- the RING lookup below is all either job needs.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

#: The map's file name; choco.sh install puts it in ``jobs/skymap/``.
SKY_MAP_FILE = "haslam408_ds_Remazeilles2014.fits"


def default_sky_map(jobs_dir) -> str:
    """The map's path under a jobs tree (the repo's ``jobs/`` or the
    installed ``/opt/choco/jobs``): ``<jobs_dir>/skymap/<SKY_MAP_FILE>``."""
    return str(Path(jobs_dir) / "skymap" / SKY_MAP_FILE)


HEALPIX_BAD = -1.6375e30     # HEALPix's UNSEEN sentinel


def load_healpix(path):
    """``(map, nside)`` from a full-sky HEALPix FITS file in RING order
    and Galactic coordinates.  Bad pixels become NaN.  Any other layout
    (NESTED, partial sky, another frame) is a ``ValueError``: the
    lookups below assume exactly this one.  A missing or unreadable
    file is an ``OSError``."""
    from astropy.io import fits
    with fits.open(path, memmap=False) as hdul:
        if len(hdul) < 2:
            raise ValueError(f"{path}: no HEALPix table extension")
        hdr, data = hdul[1].header, hdul[1].data
        pixtype = str(hdr.get("PIXTYPE", "")).strip().upper()
        ordering = str(hdr.get("ORDERING", "")).strip().upper()
        frame = str(hdr.get("COORDSYS", "")).strip().upper()
        nside = int(hdr.get("NSIDE", 0))
        if pixtype != "HEALPIX" or ordering != "RING" or frame not in ("G", "GALACTIC"):
            raise ValueError(f"{path}: need a RING-ordered Galactic HEALPix map "
                             f"(PIXTYPE={pixtype!r} ORDERING={ordering!r} COORDSYS={frame!r})")
        m = np.asarray(data.field(0), dtype=np.float64).ravel()
        bad = float(hdr.get("BAD_DATA", HEALPIX_BAD))
    if nside < 1 or m.size != 12 * nside * nside:
        raise ValueError(f"{path}: {m.size} pixels is not a full sky at NSIDE={nside}")
    m[(m <= 0.99 * bad) | ~np.isfinite(m)] = np.nan
    return m, nside


def ang2pix_ring(nside, theta, phi):
    """HEALPix RING pixel index for colatitude *theta*, longitude *phi*
    (radians; arrays).  Górski et al. 2005, ApJ 622, 759, eqs. 4-8."""
    theta = np.asarray(theta, dtype=float)
    z = np.cos(theta)
    za = np.abs(z)
    tt = np.mod(np.asarray(phi, dtype=float), 2 * np.pi) / (np.pi / 2)    # in [0, 4)
    pix = np.empty(np.shape(z), dtype=np.int64)

    eq = za <= 2.0 / 3.0                                   # equatorial belt
    t1 = nside * (0.5 + tt[eq])
    t2 = nside * z[eq] * 0.75
    jp = (t1 - t2).astype(np.int64)
    jm = (t1 + t2).astype(np.int64)
    ir = nside + 1 + jp - jm                               # ring 1 .. 2 nside + 1
    kshift = 1 - (ir & 1)
    ip = np.mod((jp + jm - nside + kshift + 1) // 2, 4 * nside)
    pix[eq] = 2 * nside * (nside - 1) + (ir - 1) * 4 * nside + ip

    pc = ~eq                                               # polar caps
    tp = tt[pc] - np.floor(tt[pc])
    tmp = nside * np.sqrt(3 * (1 - za[pc]))
    jp = (tp * tmp).astype(np.int64)
    jm = ((1 - tp) * tmp).astype(np.int64)
    ir = jp + jm + 1                                       # ring from the pole
    ip = np.mod((tt[pc] * ir).astype(np.int64), 4 * ir)
    north = z[pc] > 0
    pix[pc] = np.where(north, 2 * ir * (ir - 1) + ip,
                       12 * nside * nside - 2 * ir * (ir + 1) + ip)
    return pix


def sample_galactic(m, nside, l_rad, b_rad):
    """The map's value at Galactic (*l*, *b*), radians, nearest pixel."""
    return m[ang2pix_ring(nside, np.pi / 2 - np.asarray(b_rad, dtype=float), l_rad)]


#: ICRS (J2000) unit vectors to Galactic ones (Hipparcos, ESA 1997, vol. 1 §1.5.3).
ICRS_TO_GALACTIC = np.array([[-0.0548755604, -0.8734370902, -0.4838350155],
                             [+0.4941094279, -0.4448296300, +0.7469822445],
                             [-0.8676661490, -0.1980763734, +0.4559837762]])


def equatorial_to_galactic(ra_rad, dec_rad):
    """ICRS (*ra*, *dec*) to Galactic (*l*, *b*), radians, broadcasting."""
    ra, dec = np.broadcast_arrays(np.asarray(ra_rad, dtype=float), np.asarray(dec_rad, dtype=float))
    v = np.stack([np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)])
    g = np.tensordot(ICRS_TO_GALACTIC, v, axes=1)
    return np.mod(np.arctan2(g[1], g[0]), 2 * np.pi), np.arcsin(np.clip(g[2], -1, 1))


def sample_equatorial(m, nside, ra_rad, dec_rad):
    """The map's value at ICRS (*ra*, *dec*), radians, nearest pixel."""
    l, b = equatorial_to_galactic(ra_rad, dec_rad)
    return sample_galactic(m, nside, l, b)


def log_stretch(m, values, lo_pct=1.0, hi_pct=99.7):
    """*values* (brightness temperatures from map *m*) as positions in
    [0, 1]: log T, linear between two percentiles of the whole sky.  A
    flat map maps everything to 0; non-positive or NaN values stay NaN."""
    with np.errstate(invalid="ignore", divide="ignore"):
        r = np.log10(np.where(m > 0, m, np.nan))
        lo, hi = np.nanpercentile(r, [lo_pct, hi_pct])
        span = hi - lo if hi > lo else 1.0
        v = np.log10(np.where(np.asarray(values) > 0, values, np.nan))
    return np.clip((v - lo) / span, 0, 1)
