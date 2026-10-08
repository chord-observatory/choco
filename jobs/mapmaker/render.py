"""Turn the map accumulators into PNGs: a raw RGB strip per map and a
labelled figure for choco's page.

Each colour bin (a slice of the spectrum, see ``mapmaker.colour_edges``)
is painted in the hue of the visible light it maps onto, the bottom of
the radio band red and the top violet, and a pixel's colour is the sum of
its bins' hues weighted by their brightness.  Because every map is in
units of the calibrator's flux density at every frequency (see
``imaging.CalAccumulator``), the calibrator itself is white and colour
means spectral index relative to it: steeper sources redder, flatter
ones bluer.  The stretch is a shared arcsinh in those units, so the bins
are comparable pixel for pixel; bins without data at a pixel (an RFI
range, or exposure below the floor) drop out of that pixel's sum rather
than tinting it.
"""

from __future__ import annotations

import math
import os
import tempfile
from datetime import datetime, timezone

import numpy as np

from choco.healpix import load_healpix, log_stretch, sample_equatorial

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def wavelength_rgb(nm) -> np.ndarray:
    """Approximate sRGB of a visible wavelength (nm), Bruton's piecewise
    fit, brightest channel scaled to 1.  Vectorised over *nm*."""
    nm = np.asarray(nm, dtype=float)
    r = np.zeros_like(nm); g = np.zeros_like(nm); b = np.zeros_like(nm)
    m = (nm >= 380) & (nm < 440); r[m] = -(nm[m] - 440) / 60; b[m] = 1
    m = (nm >= 440) & (nm < 490); g[m] = (nm[m] - 440) / 50; b[m] = 1
    m = (nm >= 490) & (nm < 510); g[m] = 1; b[m] = -(nm[m] - 510) / 20
    m = (nm >= 510) & (nm < 580); r[m] = (nm[m] - 510) / 70; g[m] = 1
    m = (nm >= 580) & (nm < 645); r[m] = 1; g[m] = -(nm[m] - 645) / 65
    m = (nm >= 645) & (nm <= 780); r[m] = 1
    rgb = np.stack([r, g, b], axis=-1)
    peak = rgb.max(axis=-1, keepdims=True)
    return np.where(peak > 0, rgb / np.maximum(peak, 1e-9), 0)


def spectrum_colours(centres_mhz, lo_mhz: float, hi_mhz: float,
                     nm_lo: float = 440.0, nm_hi: float = 650.0) -> np.ndarray:
    """One RGB per colour bin: frequency mapped logarithmically onto
    visible wavelength, *lo_mhz* to the red end (*nm_hi*) and *hi_mhz* to
    the violet end (*nm_lo*).  The ends are kept inside the saturated part
    of the spectrum so no bin is painted nearly black."""
    f = np.clip(np.asarray(centres_mhz, dtype=float), lo_mhz, hi_mhz)
    x = np.log(f / lo_mhz) / np.log(hi_mhz / lo_mhz)          # 0 at lo, 1 at hi
    return wavelength_rgb(nm_hi - x * (nm_hi - nm_lo))


def band_maps(num: np.ndarray, exp: np.ndarray, exposure_floor: float = 0.02):
    """Exposure-normalised maps ``(n_colour, n_dec, n_bins)`` from
    accumulators ``(n_pol, n_colour, n_dec, n_bins)``; polarizations are
    summed and pixels below *exposure_floor* of the bin's peak exposure
    are NaN."""
    n = num.sum(axis=0)
    e = exp.sum(axis=0)
    peak = e.reshape(e.shape[0], -1).max(axis=1)[:, None, None]
    ok = e > exposure_floor * np.maximum(peak, 1e-30)
    with np.errstate(invalid="ignore", divide="ignore"):
        m = np.where(ok, n / np.maximum(e, 1e-30), np.nan)
    return m.astype(np.float32), ok


def stretch(maps: np.ndarray, valid: np.ndarray, cfg: dict) -> np.ndarray:
    """Map brightness to display intensity in [0, 1], the **same** curve for
    every colour bin so that equal brightness across bins stays white.

    ``mode``:

    * ``equalize`` — histogram equalisation of the valid pixels above a
      noise floor (median + ``floor_sigma`` × 1.4826 MAD of all valid
      pixels): the brightest and the faintest detectable sources both get
      their share of the range, and the floor keeps the noise black.
    * ``log`` — ``log(1 + x/soft) / log(1 + max/soft)``.
    * ``arcsinh`` — ``asinh(x/soft) / asinh(max/soft)``: linear below
      ``soft``, logarithmic above.

    ``soft`` and ``max`` are in the map's units (the calibrator's flux).
    """
    x = np.where(valid, np.nan_to_num(maps, nan=0.0), 0.0)
    mode = cfg.get("mode", "equalize")
    soft = float(cfg.get("soft", 0.02))
    vmax = float(cfg.get("max", 1.0))
    if mode == "log":
        y = np.log1p(np.clip(x, 0, None) / soft) / math.log1p(vmax / soft)
    elif mode == "arcsinh":
        y = np.arcsinh(np.clip(x, 0, None) / soft) / math.asinh(vmax / soft)
    elif mode == "equalize":
        vals = x[valid]
        y = np.zeros_like(x)
        if vals.size:
            med = float(np.median(vals))
            mad = float(np.median(np.abs(vals - med)))
            floor = med + float(cfg.get("floor_sigma", 3.0)) * 1.4826 * mad
            above = vals[vals > floor]
            if above.size:
                # the empirical CDF of what is above the floor, on a fixed
                # quantile grid so the mapping is cheap and monotone
                grid = np.quantile(above, np.linspace(0, 1, 1025))
                ranks = np.linspace(0, 1, 1025)
                y = np.where(x > floor, np.interp(x, grid, ranks, left=0.0, right=1.0), 0.0)
    else:
        raise ValueError(f"unknown stretch mode {mode!r}")
    return np.clip(y, 0, 1)


def to_rgb(maps: np.ndarray, colours: np.ndarray, stretch_cfg: dict) -> np.ndarray:
    """``(n_dec, n_bins, 3)`` uint8 with RA increasing to the left (east
    left, the sky convention).

    *maps* is ``(n_colour, n_dec, n_bins)`` with NaN where a bin has no
    data, *colours* ``(n_colour, 3)``.  Each bin's stretched brightness
    (:func:`stretch`) is painted in its hue and the hues are summed,
    normalised per pixel by the hues of the bins that have data there, so
    a source of unit brightness in every bin is white and a missing bin
    leaves no tint.  Pixels with no data in any bin are black."""
    colours = np.asarray(colours, dtype=float)
    if colours.shape != (maps.shape[0], 3):
        raise ValueError(f"need one RGB per colour bin: {colours.shape} for {maps.shape[0]} bins")
    valid = np.isfinite(maps)
    y = stretch(maps, valid, stretch_cfg)                                     # (c, d, n)
    num = np.einsum("cdn,ck->dnk", y * valid, colours)
    den = np.einsum("cdn,ck->dnk", valid.astype(float), colours)
    with np.errstate(invalid="ignore", divide="ignore"):
        rgb = np.where(den > 0, num / np.maximum(den, 1e-9), 0.0)
    rgb = (np.clip(rgb, 0, 1) * 255 + 0.5).astype(np.uint8)
    return rgb[:, ::-1, :]


def write_png_atomic(path, rgb: np.ndarray, row_repeat: int = 1) -> None:
    """Write an RGB array as PNG via a temp file and rename, rows upscaled
    by *row_repeat* so a thin Dec strip stays visible at native size."""
    img = np.repeat(rgb, max(1, int(row_repeat)), axis=0)
    d = os.path.dirname(str(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=".png", dir=d)
    os.close(fd)
    try:
        plt.imsave(tmp, img[::-1], origin="upper")      # row 0 is the southernmost Dec: put north up
        os.chmod(tmp, 0o644)                            # mkstemp makes it private; choco reads it
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def write_figure_atomic(path, rgb: np.ndarray, dec_deg: np.ndarray, title: str,
                        colour_edges, colours, markers=(), coverage: np.ndarray | None = None) -> None:
    """A labelled strip: RA (east left) against Dec, optional source
    markers ``[(name, ra_deg, dec_deg), ...]``, a coverage bar underneath
    and the frequency-to-hue key (*colour_edges* in MHz, one more than
    *colours*) on the right."""
    n_dec, n_bins = rgb.shape[:2]
    rows = 2 if coverage is not None else 1
    fig_h = 3.6 if coverage is None else 4.2
    fig = plt.figure(figsize=(16, fig_h), dpi=110)
    gs = fig.add_gridspec(rows, 2, width_ratios=[40, 1], wspace=0.04,
                          height_ratios=[6, 1] if rows == 2 else [1])
    axes = np.array([[fig.add_subplot(gs[i, 0])] for i in range(rows)])
    for a in axes[1:, 0]:
        a.sharex(axes[0, 0])
    ax = axes[0, 0]
    extent = [360.0, 0.0, dec_deg[0] - (dec_deg[1] - dec_deg[0]) / 2,
              dec_deg[-1] + (dec_deg[1] - dec_deg[0]) / 2]
    ax.imshow(rgb, origin="lower", aspect="auto", extent=extent, interpolation="nearest")
    for name, ra, dec in markers:
        if extent[2] <= dec <= extent[3]:
            ax.plot(ra, dec, "w+", ms=10, mew=1.0, alpha=0.8)
            ax.annotate(name, (ra, dec), xytext=(4, 4), textcoords="offset points",
                        color="w", fontsize=8, alpha=0.9)
    edges = np.asarray(colour_edges, dtype=float)
    ax.set_title(f"{title}    [{len(colours)} colour bins, {edges[0]:.0f}–{edges[-1]:.0f} MHz]",
                 fontsize=10, loc="left")
    ax.set_ylabel("Dec [deg]")
    ax.set_xticks(np.arange(0, 361, 30))
    # the key: one swatch per colour bin, frequency upwards
    kax = fig.add_subplot(gs[0, 1])
    sw = np.asarray(colours, dtype=float)[:, None, :]                       # (c, 1, 3)
    kax.imshow(sw, origin="lower", aspect="auto", extent=[0, 1, 0, len(colours)], interpolation="nearest")
    kax.set_xticks([])
    kax.yaxis.tick_right()
    ticks = [i for i in range(len(edges)) if i % max(1, len(colours) // 6) == 0 or i == len(edges) - 1]
    kax.set_yticks(ticks)
    kax.set_yticklabels([f"{edges[i]:.0f}" for i in ticks], fontsize=7)
    kax.set_ylabel("MHz", fontsize=8)
    kax.yaxis.set_label_position("right")
    if coverage is not None:
        cax = axes[1, 0]
        cax.imshow(coverage[None, ::-1], aspect="auto", extent=[360, 0, 0, 1], cmap="Greys_r",
                   vmin=0, vmax=max(float(np.max(coverage)), 1e-9), interpolation="nearest")
        cax.set_yticks([])
        cax.set_ylabel("cover", fontsize=8)
    axes[-1, 0].set_xlabel("RA [deg]")
    fig.subplots_adjust(left=0.05, right=0.95, top=0.9, bottom=0.14 if rows == 2 else 0.2, hspace=0.08)
    _save_atomic(fig, path)


#: The context image's Dec range: everything that rises at DRAO (lat 49.3).
CONTEXT_DEC_DEG = (-40.0, 90.0)


def sky_context(map_path, step_deg: float = 0.25, cmap: str = "inferno"):
    """The 408 MHz all-sky map (choco.healpix) on an RA x Dec grid at
    *step_deg*, RA running 360 -> 0 left to right like the strips, Dec
    over :data:`CONTEXT_DEC_DEG`: ``(rgb, extent)`` for ``imshow`` with
    ``origin="lower"``, log brightness through *cmap*, unfaded."""
    m, nside = load_healpix(map_path)
    ra = np.arange(360.0, 0.0, -step_deg) - step_deg / 2                 # left to right
    dec = np.arange(CONTEXT_DEC_DEG[0], CONTEXT_DEC_DEG[1], step_deg) + step_deg / 2
    R, D = np.meshgrid(np.radians(ra), np.radians(dec))
    x = log_stretch(m, sample_equatorial(m, nside, R, D))
    rgb = plt.get_cmap(cmap)(np.nan_to_num(x))[..., :3]
    return rgb, [360.0, 0.0, CONTEXT_DEC_DEG[0], CONTEXT_DEC_DEG[1]]


def write_context_atomic(path, map_path, strip_rgb: np.ndarray, dec_deg: np.ndarray,
                         title: str, markers=(), valid: np.ndarray | None = None) -> None:
    """The map's strip in context: the 408 MHz sky over every declination
    that rises, with *strip_rgb* (the colour composite, rows = *dec_deg*)
    pasted into its band and outlined.  Strip pixels where *valid* is
    False (no colour bin has data there) stay transparent, so the
    backdrop shows through them; *valid* is ``(n_dec, n_bins)`` in
    increasing RA, like the accumulators, while :func:`to_rgb`'s
    composite already runs east-left.  The strip's RA is the sidereal angle of date and the
    backdrop's is ICRS; they differ by precession (~0.36 deg in 2026),
    under two of the backdrop's pixels."""
    sky, extent = sky_context(map_path)
    half = (dec_deg[1] - dec_deg[0]) / 2
    lo, hi = dec_deg[0] - half, dec_deg[-1] + half
    fig = plt.figure(figsize=(16, 6.6), dpi=110)
    ax = fig.add_axes([0.05, 0.1, 0.9, 0.8])
    ax.imshow(sky, origin="lower", aspect="auto", extent=extent, interpolation="bilinear")
    rgb = np.asarray(strip_rgb)[..., :3]
    rgb = rgb / 255.0 if np.issubdtype(rgb.dtype, np.integer) else rgb.astype(float)
    strip = np.concatenate([rgb, np.ones(rgb.shape[:2] + (1,))], axis=-1)
    if valid is not None:
        strip[..., 3] = np.asarray(valid, dtype=bool)[:, ::-1]
    ax.imshow(strip, origin="lower", aspect="auto", extent=[360.0, 0.0, lo, hi],
              interpolation="nearest", zorder=2)
    for d in (lo, hi):
        ax.axhline(d, color="#7fd4ff", lw=1.0, zorder=3)
    for name, ra, dec in markers:
        ax.plot(ra, dec, "+", color="#7fd4ff", ms=9, mew=1.2, zorder=4)
        ax.annotate(name, (ra, dec), xytext=(5, 5), textcoords="offset points",
                    color="#7fd4ff", fontsize=8, zorder=4)
    ax.set_xlim(360, 0)
    ax.set_ylim(*extent[2:])
    ax.set_xticks(np.arange(0, 361, 30))
    ax.set_yticks(np.arange(-30, 91, 15))
    ax.set_xlabel("RA [deg]")
    ax.set_ylabel("Dec [deg]")
    ax.set_title(f"{title}    [408 MHz all-sky map (Haslam; Remazeilles et al. 2015), "
                 f"log scale, with the strip at Dec {lo:.1f}..{hi:.1f}]", fontsize=10, loc="left")
    _save_atomic(fig, path)


def _save_atomic(fig, path) -> None:
    d = os.path.dirname(str(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=".png", dir=d)
    os.close(fd)
    try:
        fig.savefig(tmp)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    finally:
        plt.close(fig)


def stamp(t_unix: float | None) -> str:
    if not t_unix:
        return "no data yet"
    return datetime.fromtimestamp(t_unix, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
