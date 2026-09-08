#!/usr/bin/env python3
"""Current-sky strip plot for CHORD at DRAO Penticton (choco job).

Renders the drift-scan strip on a Mollweide projection in galactic
coordinates — RA/Dec grid, ecliptic, faded radio-sky background, the
CHORD beam centre and HPBW bands, bright sources near the beam — with
the **current** positions of the Sun, the Moon and the beam itself (a
"beam now" marker where the meridian crosses the strip, plus local-time
labels for where the beam will point over the next 24 h).

The pointing declination is read live from choco's ``/api/config``:
``dish_coelev_deg`` in the rendered kotekan config plus DRAO's latitude
is the beam declination (49.32° − 27.3° ≈ +22° = Tau A).  An explicit
``dec`` in skymap.yaml overrides it.

Runs from choco-skymap.timer every 5 minutes and renders the same
instant twice: a day image (white page, for the landing card) and a
night image (dark page, for wall displays in a dim control room),
differing only in palette.  Each PNG is written atomically (temp file
+ rename) so choco's ``/skymap.png`` and ``/skymap-night.png`` routes
never serve a half-written image.  There is no state file: the
image is the record (its title carries the render time) and the SKYMAP
badge reads the unit's result from systemd.  Exit codes follow the jobs
convention: 0 ok, 2 degraded (choco unreachable, no pointing found —
the previous image simply stays up), 1 config error / bug.

Derived from a standalone visualizer written iteratively via
conversation with Claude (Anthropic) with input from a user running
observations at DRAO Penticton.  Astropy is the authoritative source
for all time/coordinate calculations; positions verified against
SIMBAD.
"""

import argparse
import os
import sys
import urllib.error
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import yaml

from choco.dishlabels import find_key
from choco.jobclient import get_json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.colors import to_rgb, to_rgba
from matplotlib.font_manager import FontProperties
from matplotlib.lines import Line2D
from matplotlib.patches import Ellipse

# Never block a 5-minute render on the IERS servers: the bundled tables
# are far beyond plot accuracy, and auto_max_age=None accepts their
# stale predictive tail (dUT1 drift is milliseconds — invisible here).
# (eop is the job whose business is fresh IERS data.)
import astropy.utils.iers
astropy.utils.iers.conf.auto_download = False
astropy.utils.iers.conf.auto_max_age = None
warnings.filterwarnings("ignore", module="astropy")
warnings.filterwarnings("ignore", message=".*dubious year.*")

from astropy.coordinates import SkyCoord, EarthLocation, get_sun, get_body
from astropy.time import Time
import astropy.units as u

SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULTS = {
    "choco_url": "https://localhost:5000",
    "group": None,          # kotekan group whose config carries the pointing;
                            # None = every group choco knows (each distinct
                            # pointing becomes its own beam)
    "beams": ["pointing"],  # beams to draw, in order: "pointing" (the live
                            # pointing(s) from choco), a MAJOR_SOURCES name
                            # ("Cyg A"), or a declination in degrees
    "output": "/var/lib/choco/skymap/skymap.png",
    "output_night": "/var/lib/choco/skymap/skymap-night.png",  # "" = no night render
    "background_image": str(SCRIPT_DIR / "sky_background.png"),
    "background_fade": 0.42,   # 0 = flat page colour; 1 = full sky image
    "dpi": 130,
    "timezone": "America/Vancouver",
}

# DRAO Penticton
DRAO_LAT = 49.3214       # deg N
DRAO_LON = -119.6243     # deg E (west negative)
DRAO_ALT = 545           # m

# CHORD beam characteristics (6 m dishes, 300–1500 MHz)
HPBW_300_MHZ = 11.5      # deg
HPBW_1000_MHZ = 3.4      # deg

BEAM_FILTER_DEG = 12.0   # only show sources within this Dec of beam centre

# ============================================================================
# SOURCE CATALOGS (verified J2000 positions from SIMBAD)
# ============================================================================
MAJOR_SOURCES = {
    'Tau A':  ('05h34m31.94s', '+22d00m52.2s'),  # Crab Nebula SNR (M1)
    'Cas A':  ('23h23m24.0s',  '+58d48m54s'),    # SNR
    'Cyg A':  ('19h59m28.36s', '+40d44m02.1s'),  # 3C 405, radio galaxy
    'Vir A':  ('12h30m49.42s', '+12d23m28.0s'),  # M87
    'Sgr A*': ('17h45m40.04s', '-29d00m28.1s'),  # Galactic center BH
}

BRIGHT_SOURCES = {
    # name:      (RA,             Dec)            ~1.4 GHz flux, notes
    'IC 443':    ('06h16m47s',    '+22d34m'),      # 160 Jy, SNR "Jellyfish"
    '3C 84':     ('03h19m48.16s', '+41d30m42.1s'), # 22 Jy, Perseus A, AGN
    '3C 48':     ('01h37m41.30s', '+33d09m35.1s'), # 16 Jy, quasar, primary flux cal
    '3C 123':    ('04h37m04.4s',  '+29d40m14s'),   # 46 Jy, radio galaxy
    '3C 138':    ('05h21m09.90s', '+16d38m22.1s'), # 8 Jy, flat-spec quasar
    '3C 147':    ('05h42m36.14s', '+49d51m07.2s'), # 22 Jy, PFC (primary flux cal)
    '3C 196':    ('08h13m36.05s', '+48d13m02.6s'), # 14 Jy, PFC
    '3C 219':    ('09h21m08.63s', '+45d38m57.4s'), # 8 Jy, FR-II
    '3C 264':    ('11h45m05.0s',  '+19d36m22s'),   # 7 Jy, head-tail (Abell 1367)
    '3C 277.3':  ('12h54m12.03s', '+27d37m32.1s'), # 2 Jy, Coma A
    '3C 286':    ('13h31m08.29s', '+30d30m32.9s'), # 14.5 Jy, PFC, compact quasar
    '3C 345':    ('16h42m58.81s', '+39d48m37.0s'), # 7 Jy, BL Lac, variable
    '3C 380':    ('18h29m31.78s', '+48d44m46.7s'), # 14 Jy, CSS quasar
    '3C 388':    ('18h44m02.37s', '+45d33m29.8s'), # 7 Jy, FR-II
    'W51':       ('19h23m42s',    '+14d30m'),      # 120 Jy, HII, inner Galaxy
    'W63':       ('20h21m00s',    '+45d35m'),      # 9 Jy, SNR
    'DR 21':     ('20h39m01.6s',  '+42d19m43s'),   # 20 Jy, HII, Cygnus X
    'NGC 7027':  ('21h07m01.59s', '+42d14m10.2s'), # 5.5 Jy, planetary nebula
    '3C 452':    ('22h45m48.79s', '+39d41m15.7s'), # 10 Jy, FR-II
    '3C 454.3':  ('22h53m57.75s', '+16d08m53.6s'), # 12 Jy, BL Lac, variable
}

# ============================================================================
# Coordinate transformation: equatorial (J2000) -> galactic
# ============================================================================
RA_NGP  = np.radians(192.8595)
DEC_NGP = np.radians(27.1284)
L_NCP   = np.radians(122.9320)


def eq_to_gal(ra, dec):
    """RA/Dec in radians -> galactic (l, b) in radians. Vectorized."""
    sin_b = (np.sin(dec) * np.sin(DEC_NGP)
             + np.cos(dec) * np.cos(DEC_NGP) * np.cos(ra - RA_NGP))
    b = np.arcsin(np.clip(sin_b, -1, 1))
    y = np.cos(dec) * np.sin(ra - RA_NGP)
    x = (np.cos(DEC_NGP) * np.sin(dec)
         - np.sin(DEC_NGP) * np.cos(dec) * np.cos(ra - RA_NGP))
    l = L_NCP - np.arctan2(y, x)
    l = (l + np.pi) % (2 * np.pi) - np.pi
    return l, b


def small_circle(ra0, dec0, radius, n=120):
    """Equatorial points of the circle of angular *radius* around
    (ra0, dec0) — all in radians."""
    theta = np.linspace(0, 2 * np.pi, n)
    dec = np.arcsin(np.clip(
        np.sin(dec0) * np.cos(radius)
        + np.cos(dec0) * np.sin(radius) * np.cos(theta), -1, 1))
    ra = ra0 + np.arctan2(np.sin(theta) * np.sin(radius) * np.cos(dec0),
                          np.cos(radius) - np.sin(dec0) * np.sin(dec))
    return ra, dec


def split_wrap(l, b):
    """Insert NaN where longitude jumps across the Mollweide seam."""
    l = np.asarray(l, dtype=float)
    b = np.asarray(b, dtype=float)
    diffs = np.abs(np.diff(l))
    wrap = np.where(diffs > np.pi)[0]
    for idx in reversed(wrap):
        l = np.insert(l, idx + 1, np.nan)
        b = np.insert(b, idx + 1, np.nan)
    return l, b


# ============================================================================
# Pointing (pure helpers — unit-tested without astropy/matplotlib)
# ============================================================================
def dec_from_coelev(coelev_deg):
    """Beam declination from kotekan's dish co-elevation.

    A meridian drift telescope at latitude φ tilted by the co-elevation
    angle points at δ = φ + coelev (DRAO: 49.32° − 27.3° ≈ +22°, Tau A,
    matching the config's own comment).
    """
    return DRAO_LAT + float(coelev_deg)


def nearest_major_source(dec_deg, within_deg=1.5):
    """Name of the major source whose Dec is closest to *dec_deg*, if
    any lies within *within_deg* — purely a plot-title nicety."""
    best, best_off = None, within_deg
    for name, (_, dec_str) in MAJOR_SOURCES.items():
        d = _parse_dec_deg(dec_str)
        off = abs(d - dec_deg)
        if off <= best_off:
            best, best_off = name, off
    return best


def _parse_dec_deg(dec_str):
    """'+22d00m52.2s' -> degrees, without astropy (keeps helpers pure)."""
    s = dec_str.strip()
    sign = -1.0 if s.startswith('-') else 1.0
    s = s.lstrip('+-')
    d, rest = s.split('d')
    m, rest = rest.split('m')
    sec = rest.rstrip('s') or '0'
    return sign * (float(d) + float(m) / 60 + float(sec) / 3600)


def fetch_pointings(choco_url, group=None, timeout=15):
    """[(dec_deg, group), ...] from choco's rendered kotekan configs.

    With no group configured, every group choco knows is read, so
    groups pointed differently each contribute a beam.  Raises OSError
    when choco is unreachable and ValueError when no group's config
    carries ``dish_coelev_deg``.  A group whose config choco cannot
    render (an HTTP error for that group alone) is skipped.
    """
    if group:
        groups = [group]
    else:
        nodes = get_json(choco_url, "/api/nodes", timeout=timeout)
        groups = list((nodes.get("groups") or {}).keys())
    found = []
    for g in groups:
        try:
            config = get_json(choco_url, f"/api/config/{g}", timeout=timeout)
        except urllib.error.HTTPError:
            continue
        coelev = find_key(config, "dish_coelev_deg")
        if coelev is not None:
            found.append((dec_from_coelev(coelev), g))
    if not found:
        raise ValueError(
            f"no dish_coelev_deg in any group config (tried {groups})")
    return found


def beam_title(dec_deg, origin=None):
    """Human label for one beam: nearest source if any, plus origin.

    An origin that *is* the source name (an extra_beams entry like
    "Cyg A") would just repeat the ≈ part, so it is left off.
    """
    near = nearest_major_source(dec_deg)
    label = f"Dec {dec_deg:+.2f}°"
    if near:
        label += f" ≈ {near}"
    if origin and origin != near:
        label += f" ({origin})"
    return label


# The beams: entry meaning "the live pointing(s) read from choco".
POINTING = "pointing"


def parse_beams(entries):
    """Validate the ``beams:`` list into [(dec | POINTING, origin), ...].

    Each entry is the POINTING token, a MAJOR_SOURCES name, or a
    declination in degrees.  Raises ValueError on anything else — a
    config error, not a degraded run.  Order is preserved: the first
    beam gets the primary palette and the strip's clock labels.
    """
    if not isinstance(entries, list) or not entries:
        raise ValueError("beams: must be a non-empty list")
    parsed = []
    for entry in entries:
        if isinstance(entry, (int, float)) and not isinstance(entry, bool):
            parsed.append((float(entry), "configured"))
            continue
        name = str(entry).strip()
        if name == POINTING:
            parsed.append((POINTING, None))
        elif name in MAJOR_SOURCES:
            parsed.append((_parse_dec_deg(MAJOR_SOURCES[name][1]), name))
        else:
            raise ValueError(
                f"beams: unknown entry {entry!r} — use {POINTING!r}, a "
                f"declination in degrees, or one of: "
                f"{', '.join(MAJOR_SOURCES)}")
    return parsed


def dedup_beams(beams):
    """Drop beams within 0.1° of an earlier one (same strip visually)."""
    out = []
    for dec, origin in beams:
        if not any(abs(dec - d) < 0.1 for d, _ in out):
            out.append((dec, origin))
    return out


# ============================================================================
# Main plotting function
# ============================================================================

# Every colour the plot uses, by role, for the two renders: "day" (white
# page, the landing card) and "night" (dark page, wall displays in a dim
# control room).  Geometry, labels and placement are identical; only the
# palette differs, so the two images stay directly comparable.
#
# ``face`` is the page colour and what the sky image fades toward;
# ``box`` fills the label boxes; ``halo`` is the marker outline that
# separates a dot from the sky (so it matches the page).  ``rc`` goes
# through plt.rc_context for what matplotlib styles itself: title, tick
# and axis-label ink, the projection outline, the legend box.
#
# ``beams`` is one palette per beam, in order: (centre line, 300 MHz
# band, 1000 MHz band, label ink).  Green stays the primary beam; further
# beams take hues nothing else on the plot uses (RA grid is thin blue
# lines, Dec grid red, sources purple).
THEMES = {
    "day": {
        "face": "white", "box": "white", "halo": "white",
        "grid": "#cfcfd6",
        "ra": "#2e5d9c", "ra_emph": "#0c2a5e",
        "dec": "#a83232", "dec_emph": "#5e0c0c",
        "pole": "black", "pole_box_edge": "black",
        "major": "#0a0a0a", "major_box_edge": "#888",
        "bright": "#7030a0", "bright_ink": "#5a2080",
        "bright_box_edge": "#9a60c0",
        "sun": "#ffcc44", "sun_edge": "#a05010", "sun_ink": "#a05010",
        "moon": "#dddddd", "moon_edge": "#3a3a3a", "moon_ink": "#3a3a3a",
        "moon_box_edge": "#6a6a6a",
        "ecliptic": "#d4a017", "gc": "#cc6600", "beam_now_edge": "#888",
        "beams": [
            ('#0a4d22', '#1f9c4d', '#0f7a3a', '#053812'),   # green
            ('#0a3d5e', '#2a7ab0', '#155e8a', '#062c42'),   # teal-blue
            ('#5e0a42', '#b02a7a', '#8a1560', '#42062e'),   # magenta
        ],
        "rc": {},
    },
    "night": {
        "face": "#0b0f1a", "box": "#151b2b", "halo": "#0b0f1a",
        "grid": "#2c3244",
        "ra": "#5b8fd6", "ra_emph": "#a9c8f2",
        "dec": "#d06060", "dec_emph": "#f0a8a8",
        "pole": "white", "pole_box_edge": "#c8cdd8",
        "major": "#f0f2f6", "major_box_edge": "#6a7080",
        "bright": "#c08ae8", "bright_ink": "#dcc0f8",
        "bright_box_edge": "#8a5cc0",
        "sun": "#ffcc44", "sun_edge": "#c06018", "sun_ink": "#ffb866",
        "moon": "#e6e6e6", "moon_edge": "#606060", "moon_ink": "#c4c4c4",
        "moon_box_edge": "#7a7a7a",
        "ecliptic": "#e8c04a", "gc": "#ff9a40", "beam_now_edge": "#8a90a0",
        "beams": [
            ('#3fd97f', '#2fb865', '#37c872', '#b0f2cc'),   # green
            ('#4aa8e8', '#2a7ab0', '#3a90cc', '#bfe0fa'),   # teal-blue
            ('#e060b0', '#b02a7a', '#c8408f', '#f8c0e4'),   # magenta
        ],
        "rc": {
            "text.color": "#e6e9f0", "axes.labelcolor": "#e6e9f0",
            "xtick.color": "#c8cdd8", "ytick.color": "#c8cdd8",
            "axes.edgecolor": "#6a7080", "axes.facecolor": "#0b0f1a",
            "legend.facecolor": "#151b2b", "legend.edgecolor": "#3a4152",
        },
    },
}


class LabelPlacer:
    """Greedy label de-overlap in display space.

    Fixed labels ``claim()`` their footprint; movable ones go through
    ``place()``, which tries anchor offsets around the marker
    (right-below, right-above, left-below, left-above, then pushed
    farther out) and takes the first that hits nothing already placed.
    Boxes are rough per-character estimates — enough to keep the
    Cygnus clutter (Cyg A / DR 21 / NGC 7027 / W63) readable, not
    typography.
    """

    CANDIDATES = [
        (8, -8, 'left', 'top'), (8, 8, 'left', 'bottom'),
        (-8, -8, 'right', 'top'), (-8, 8, 'right', 'bottom'),
        (14, -20, 'left', 'top'), (14, 20, 'left', 'bottom'),
        (-14, -20, 'right', 'top'), (-14, 20, 'right', 'bottom'),
    ]

    # Inflation around every measured box, in points: the bbox pad plus
    # a visible gap, enforced from both sides of a would-be collision.
    MARGIN_PT = 3.0

    def __init__(self, ax):
        self.ax = ax
        self.px_per_pt = ax.figure.dpi / 72.0
        self.renderer = ax.figure.canvas.get_renderer()
        self.boxes = []

    def _box(self, x, y, text, fontsize, ha, va, dx=0, dy=0,
             weight='normal'):
        # Real text extents from the Agg renderer, not an estimate —
        # a per-character guess put W63's label on Cyg A's.
        tw, th, _ = self.renderer.get_text_width_height_descent(
            text, FontProperties(size=fontsize, weight=weight), False)
        px, py = self.ax.transData.transform((x, y))
        px += dx * self.px_per_pt
        py += dy * self.px_per_pt
        x0 = px - (tw if ha == 'right' else tw / 2 if ha == 'center' else 0)
        y0 = py - (th if va == 'top' else th / 2 if va == 'center' else 0)
        m = self.MARGIN_PT * self.px_per_pt
        return (x0 - m, y0 - m, x0 + tw + m, y0 + th + m)

    def _free(self, box):
        x0, y0, x1, y1 = box
        return not any(x0 < a1 and a0 < x1 and y0 < b1 and b0 < y1
                       for a0, b0, a1, b1 in self.boxes)

    def claim(self, x, y, text, fontsize, ha='center', va='bottom',
              dx=0, dy=0, weight='normal'):
        self.boxes.append(self._box(x, y, text, fontsize, ha, va, dx, dy,
                                    weight=weight))

    def claim_marker(self, x, y, size_pt):
        """Reserve a marker's own footprint so no label covers it.

        Sized just under the candidate offsets, so a marker never
        blocks its *own* label out of every candidate position.
        """
        px, py = self.ax.transData.transform((x, y))
        half = size_pt / 2 * self.px_per_pt
        self.boxes.append((px - half, py - half, px + half, py + half))

    def place(self, x, y, text, fontsize, prefer=None, **annotate_kw):
        weight = annotate_kw.get('fontweight', 'normal')
        candidates = ([prefer] if prefer else []) + self.CANDIDATES
        for dx, dy, ha, va in candidates:
            box = self._box(x, y, text, fontsize, ha, va, dx, dy,
                            weight=weight)
            if self._free(box):
                break
        else:
            # Nowhere free: keep the preferred spot and accept overlap.
            dx, dy, ha, va = candidates[0]
            box = self._box(x, y, text, fontsize, ha, va, dx, dy,
                            weight=weight)
        self.boxes.append(box)
        return self.ax.annotate(text, xy=(x, y), xytext=(dx, dy),
                                textcoords='offset points',
                                fontsize=fontsize, ha=ha, va=va,
                                **annotate_kw)


def plot_skymap(cfg, beams, now=None, theme="day", output=None):
    """Render the strip plot for *beams* = [(dec_deg, origin), ...].

    *theme* names an entry of THEMES; *output* defaults to cfg["output"].
    The day and night images of one run are drawn from the same *now*,
    so pass it explicitly when rendering both.
    """
    if theme not in THEMES:
        raise ValueError(f"unknown theme {theme!r}; one of {list(THEMES)}")
    T = THEMES[theme]
    with plt.rc_context(T["rc"]):
        return _plot_skymap(cfg, beams, now, T,
                            output if output is not None else cfg["output"])


def _plot_skymap(cfg, beams, now, T, output):
    drao = EarthLocation(lat=DRAO_LAT * u.deg, lon=DRAO_LON * u.deg,
                         height=DRAO_ALT * u.m)
    now = now or Time.now()
    tz = ZoneInfo(cfg["timezone"])
    local_now = datetime.fromtimestamp(now.unix, tz)
    tz_label = local_now.tzname()

    # Current ephemerides: single positions, not trajectories.
    sun = get_sun(now)
    moon = get_body('moon', now, location=drao)
    lst_now_h = float(now.sidereal_time('apparent', longitude=drao.lon).hour)

    # ------------------ Figure setup ------------------
    fig = plt.figure(figsize=(15, 8.5), facecolor=T["face"])
    ax_pos = [0.06, 0.18, 0.88, 0.72]

    # Background image axes (matched to Mollweide's 2:1 aspect)
    bg = cfg["background_image"]
    if bg and os.path.exists(bg):
        sky_img = mpimg.imread(bg)
        rgb = sky_img[..., :3]
        fade = float(cfg["background_fade"])
        page = np.array(to_rgb(T["face"]))
        faded = np.clip(rgb * fade + (1 - fade) * page, 0, 1)
        ax_bg = fig.add_axes(ax_pos)
        ax_bg.imshow(faded, aspect='auto', extent=[-1, 1, -1, 1],
                     interpolation='bilinear')
        ax_bg.set_xticks([]); ax_bg.set_yticks([])
        for s in ax_bg.spines.values():
            s.set_visible(False)
        ax_bg.set_aspect(0.5, adjustable='box', anchor='C')
        clip_ellipse = Ellipse((0, 0), 2.0, 2.0, transform=ax_bg.transData,
                               facecolor='none', edgecolor='none')
        ax_bg.add_patch(clip_ellipse)
        for im in ax_bg.images:
            im.set_clip_path(clip_ellipse)
        ax_bg.set_xlim(-1, 1); ax_bg.set_ylim(-1, 1)

    # Mollweide overlay
    ax = fig.add_axes(ax_pos, projection='mollweide')
    ax.patch.set_alpha(0)
    ax.grid(True, color=T["grid"], linestyle=':', linewidth=0.6)
    labels = LabelPlacer(ax)

    n_pts = 800

    # ------------------ RA/Dec grid ------------------
    ra_col, ra_emph = T["ra"], T["ra_emph"]
    dec_col, dec_emph = T["dec"], T["dec_emph"]
    for ra_h in np.arange(0, 24, 2):
        ra = np.radians(ra_h * 15)
        dec_arr = np.linspace(-np.pi / 2 + 0.001, np.pi / 2 - 0.001, n_pts)
        l, b = eq_to_gal(np.full_like(dec_arr, ra), dec_arr)
        lp, bp = split_wrap(-l, b)
        color, lw = (ra_emph, 2.2) if ra_h == 0 else (ra_col, 1.0)
        ax.plot(lp, bp, color=color, linewidth=lw,
                alpha=0.9 if ra_h == 0 else 0.7, zorder=3 if ra_h == 0 else 2)
        l0, b0 = eq_to_gal(np.array([ra]), np.array([0.0]))
        ax.text(-l0[0], b0[0] + 0.04, f"{ra_h}ʰ", color=ra_emph, fontsize=8.5,
                ha='center', va='bottom', fontweight='bold',
                bbox=dict(facecolor=T["box"], edgecolor='none', alpha=0.7,
                          pad=1))

    for dec_d in np.arange(-75, 76, 15):
        dec = np.radians(dec_d)
        ra_arr = np.linspace(0, 2 * np.pi, n_pts)
        l, b = eq_to_gal(ra_arr, np.full_like(ra_arr, dec))
        lp, bp = split_wrap(-l, b)
        color, lw = (dec_emph, 2.2) if dec_d == 0 else (dec_col, 1.0)
        ax.plot(lp, bp, color=color, linewidth=lw,
                alpha=0.9 if dec_d == 0 else 0.65, zorder=3 if dec_d == 0 else 2)

    for dec_d in [-60, -30, 0, 30, 60]:
        l, b = eq_to_gal(np.array([np.radians(6 * 15)]),
                         np.array([np.radians(dec_d)]))
        sign = '+' if dec_d >= 0 else ''
        ax.text(-l[0] + 0.03, b[0], f"{sign}{dec_d}°", color=dec_emph,
                fontsize=8.5, ha='left', va='center', fontweight='bold',
                bbox=dict(facecolor=T["box"], edgecolor='none', alpha=0.7,
                          pad=1))

    # ------------------ Poles ------------------
    for eq_dec, name, va in [(np.pi / 2 - 1e-5, 'NCP', 'top'),
                             (-np.pi / 2 + 1e-5, 'SCP', 'bottom')]:
        l, b = eq_to_gal(np.array([0.0]), np.array([eq_dec]))
        ax.plot(-l, b, marker='*', color=T["pole"], markersize=18,
                markeredgecolor=T["halo"], markeredgewidth=1.0, zorder=6)
        dy = -0.08 if va == 'top' else 0.08
        ax.text(-l[0], b[0] + dy, name, fontsize=10, fontweight='bold',
                ha='center', va=va,
                bbox=dict(facecolor=T["box"], edgecolor=T["pole_box_edge"],
                          alpha=0.85, pad=2))
        labels.claim(-l[0], b[0] + dy, name, 10, ha='center', va=va,
                     weight='bold')

    # ------------------ CHORD strips, one per beam ------------------
    ra_strip = np.linspace(0, 2 * np.pi, n_pts)
    for i, (beam_dec, origin) in enumerate(beams):
        centre, band300, band1000, _ink = T["beams"][i % len(T["beams"])]
        for freq, hpbw, color, n_band, per_alpha, lw in [
            (300, HPBW_300_MHZ, band300, 50, 0.07, 1.4),
            (1000, HPBW_1000_MHZ, band1000, 25, 0.09, 1.1),
        ]:
            half = hpbw / 2.0
            for frac in np.linspace(-half, half, n_band):
                dec_band = np.full_like(ra_strip, np.radians(beam_dec + frac))
                l_b, b_b = eq_to_gal(ra_strip, dec_band)
                l_bp, b_bp = split_wrap(-l_b, b_b)
                ax.plot(l_bp, b_bp, color=color, linewidth=lw, alpha=per_alpha,
                        zorder=1.5)

        l_c, b_c = eq_to_gal(ra_strip,
                             np.full_like(ra_strip, np.radians(beam_dec)))
        l_cp, b_cp = split_wrap(-l_c, b_c)
        ax.plot(l_cp, b_cp, color=centre, linewidth=2.6, alpha=0.95,
                zorder=4.5)

    # ------------------ Beam now (a beam-sized disk per beam) ------------
    # A circle of the 300 MHz HPBW on the sky — an approximate view of
    # what each beam sees right now — projected honestly rather than
    # drawn as a screen-space marker, so it stretches near the map edge,
    # splits across the seam, and (when the disk swallows the galactic
    # pole, which a dec +22° beam does once a day — the NGP sits at dec
    # +27.1°) closes over the pole as a cap.  Translucent fill and rim
    # (35 %) keep the sky underneath visible; unlabelled and unclaimed,
    # since labels stay readable on top of it.
    ra_now = np.radians(lst_now_h * 15)
    r_beam = np.radians(HPBW_300_MHZ / 2)
    for i, (beam_dec, origin) in enumerate(beams):
        centre = T["beams"][i % len(T["beams"])][0]
        dec0 = np.radians(beam_dec)
        ra_c, dec_c = small_circle(ra_now, dec0, r_beam)
        l_c, b_c = eq_to_gal(ra_c, dec_c)
        x, y = -l_c, b_c
        face = to_rgba(centre, 0.35)
        rim = to_rgba('white', 0.35)

        sep_ngp = np.arccos(np.clip(
            np.sin(dec0) * np.sin(DEC_NGP)
            + np.cos(dec0) * np.cos(DEC_NGP) * np.cos(ra_now - RA_NGP),
            -1, 1))
        pole = (np.pi / 2 if sep_ngp < r_beam
                else -np.pi / 2 if np.pi - sep_ngp < r_beam else None)
        if pole is not None:
            # The outline winds right around in longitude: sort it by l
            # and close the fill over the pole; the rim is the sorted
            # curve alone, without the artificial closing edges.
            order = np.argsort(x)
            xs, ys = x[order], y[order]
            ax.fill(np.concatenate([xs, [np.pi, np.pi, -np.pi, -np.pi]]),
                    np.concatenate([ys, [ys[-1], pole, pole, ys[0]]]),
                    facecolor=face, edgecolor='none', zorder=7.5)
            ax.plot(xs, ys, color=rim, linewidth=1.8, zorder=7.5)
        elif x.max() - x.min() > np.pi:
            # Crosses the seam: draw the unwrapped polygon twice, one
            # copy per side, each clipped by the projection boundary.
            xs = np.where(x < 0, x + 2 * np.pi, x)
            for shift in (0.0, -2 * np.pi):
                patch, = ax.fill(xs + shift, y, facecolor=face,
                                 edgecolor=rim, linewidth=1.8, zorder=7.5)
                patch.set_clip_path(ax.patch)
        else:
            ax.fill(x, y, facecolor=face, edgecolor=rim, linewidth=1.8,
                    zorder=7.5)

    # The Sun and Moon markers are drawn (and labelled) later, but their
    # footprints are claimed up front so no earlier label covers them.
    for body, size in ((sun, 16), (moon, 13)):
        l_b, b_b = eq_to_gal(np.array([body.ra.rad]), np.array([body.dec.rad]))
        labels.claim_marker(-l_b[0], b_b[0], size)

    # ------------------ Local-time labels along the primary strip -----------
    # The beam always points at RA = LST, so where it will point at a
    # given clock time is exact — no solar-transit approximation needed.
    # Labels every 2 h (on even local hours) across the next 24 h; the
    # RA of a label is the same for every beam, so only the primary
    # strip carries them to keep the plot readable.
    primary_dec = beams[0][0]
    primary_centre, _, _, primary_ink = T["beams"][0]
    first = local_now.replace(minute=0, second=0, microsecond=0)
    while first <= local_now or first.hour % 2:
        first += timedelta(hours=1)
    for k in range(12):
        t_local = first + timedelta(hours=2 * k)
        t_k = Time(t_local.timestamp(), format='unix')
        ra_hrs = float(t_k.sidereal_time('apparent', longitude=drao.lon).hour)
        l_pt, b_pt = eq_to_gal(np.array([np.radians(ra_hrs * 15)]),
                               np.array([np.radians(primary_dec)]))
        label_str = (f"{t_local.hour:02d} {tz_label}" if k == 0
                     else f"{t_local.hour:02d}")
        ax.plot(-l_pt[0], b_pt[0], marker='o', color=primary_centre,
                markersize=5, markeredgecolor=T["halo"], markeredgewidth=1.0,
                zorder=5.5)
        labels.place(-l_pt[0], b_pt[0], label_str, 8,
                     prefer=(8, 8, 'left', 'bottom'),
                     color=primary_ink, fontweight='bold',
                     bbox=dict(facecolor=T["box"], edgecolor=primary_centre,
                               alpha=0.88, pad=1.5, linewidth=0.6), zorder=6)

    # ------------------ Sources near any beam ------------------
    # Two passes: every marker is drawn and claimed first, then the
    # labels are placed — so an early label can never sit on a marker
    # whose turn had not come yet.
    majors = []
    for name, (ra_str, dec_str) in MAJOR_SOURCES.items():
        sc = SkyCoord(ra_str, dec_str, frame='icrs')
        l, b = eq_to_gal(np.array([sc.ra.rad]), np.array([sc.dec.rad]))
        ax.plot(-l[0], b[0], marker='o', markerfacecolor='none',
                markeredgecolor=T["major"], markersize=10, markeredgewidth=1.6,
                zorder=5)
        labels.claim_marker(-l[0], b[0], 11)
        majors.append((name, -l[0], b[0]))

    brights = []
    for name, (ra_str, dec_str) in BRIGHT_SOURCES.items():
        sc = SkyCoord(ra_str, dec_str, frame='icrs')
        offset = min(abs(float(sc.dec.deg) - d) for d, _ in beams)
        if offset > BEAM_FILTER_DEG:
            continue
        beam_dec = min((d for d, _ in beams),
                       key=lambda d: abs(float(sc.dec.deg) - d))
        l, b = eq_to_gal(np.array([sc.ra.rad]), np.array([sc.dec.rad]))
        ax.plot(-l[0], b[0], marker='o', markerfacecolor='none',
                markeredgecolor=T["bright"], markersize=7, markeredgewidth=1.3,
                zorder=4.8)
        labels.claim_marker(-l[0], b[0], 8)
        brights.append((name, -l[0], b[0], sc.dec.deg > beam_dec))

    for name, x, y in majors:
        labels.place(x, y, name, 9.5, prefer=(7, -9, 'left', 'top'),
                     fontweight='bold',
                     bbox=dict(facecolor=T["box"], edgecolor=T["major_box_edge"],
                               alpha=0.9, pad=2),
                     zorder=5)

    for name, x, y, above in brights:
        # Prefer the side of the strip the source sits on, but let the
        # placer flip a label that would land on a neighbour's.
        prefer = (7, 6, 'left', 'bottom') if above else (7, -6, 'left', 'top')
        labels.place(x, y, name, 8, prefer=prefer,
                     color=T["bright_ink"],
                     bbox=dict(facecolor=T["box"], edgecolor=T["bright_box_edge"],
                               alpha=0.85, pad=1.5, linewidth=0.5),
                     zorder=5)

    # ------------------ Sun (current position) ------------------
    l, b = eq_to_gal(np.array([sun.ra.rad]), np.array([sun.dec.rad]))
    ax.plot(-l[0], b[0], marker='o', color=T["sun"], markersize=14,
            markeredgecolor=T["sun_edge"], markeredgewidth=1.8, zorder=7)
    labels.place(-l[0], b[0], "Sun", 9.5, prefer=(12, -8, 'left', 'top'),
                 color=T["sun_ink"], fontweight='bold',
                 bbox=dict(facecolor=T["box"], edgecolor=T["sun_ink"],
                           alpha=0.92, pad=2), zorder=8)

    # ------------------ Moon (current position) ------------------
    l, b = eq_to_gal(np.array([moon.ra.rad]), np.array([moon.dec.rad]))
    ax.plot(-l[0], b[0], marker='o', color=T["moon"], markersize=11,
            markeredgecolor=T["moon_edge"], markeredgewidth=1.6, zorder=7)
    labels.place(-l[0], b[0], "Moon", 9.5, prefer=(10, 10, 'left', 'bottom'),
                 color=T["moon_ink"], fontweight='bold',
                 bbox=dict(facecolor=T["box"], edgecolor=T["moon_box_edge"],
                           alpha=0.92, pad=2), zorder=8)

    # ------------------ Ecliptic + galactic center ------------------
    eps = np.radians(23.4393)
    ecl_lon = np.linspace(0, 2 * np.pi, n_pts)
    ra_ecl = np.arctan2(np.sin(ecl_lon) * np.cos(eps), np.cos(ecl_lon))
    dec_ecl = np.arcsin(np.sin(ecl_lon) * np.sin(eps))
    l_ecl, b_ecl = eq_to_gal(ra_ecl, dec_ecl)
    l_ep, b_ep = split_wrap(-l_ecl, b_ecl)
    ax.plot(l_ep, b_ep, color=T["ecliptic"], linewidth=1.6, linestyle='--',
            alpha=0.85, zorder=2.5)
    ax.plot(0, 0, marker='+', color=T["gc"], markersize=18,
            markeredgewidth=2.5, zorder=4)

    # ------------------ Axes labels and title ------------------
    xticks = np.radians([-150, -120, -90, -60, -30, 0, 30, 60, 90, 120, 150])
    ax.set_xticks(xticks)
    ax.set_xticklabels(['150°', '120°', '90°', '60°', '30°', '0°',
                        '330°', '300°', '270°', '240°', '210°'], fontsize=9)
    yticks_deg = [-75, -60, -45, -30, -15, 0, 15, 30, 45, 60, 75]
    ax.set_yticks(np.radians(yticks_deg))
    ax.set_yticklabels([f"{d:+d}°" for d in yticks_deg], fontsize=9)

    beams_title = "  ·  ".join(beam_title(d, o) for d, o in beams)
    ax.set_title(f"CHORD beam{'s' if len(beams) > 1 else ''}: {beams_title} — "
                 f"Equatorial grid in Galactic coordinates\n"
                 f"Mollweide (J2000)  •  "
                 f"{local_now:%Y-%m-%d %H:%M} {tz_label}  "
                 f"({now.utc.iso[:16]} UTC)",
                 fontsize=12, pad=18)
    ax.set_xlabel('Galactic longitude $l$', fontsize=10.5, labelpad=8)
    ax.set_ylabel('Galactic latitude $b$', fontsize=10.5)

    # ------------------ Legend ------------------
    handles = [
        Line2D([0], [0], color=ra_emph, lw=2.2, label='RA = 0ʰ'),
        Line2D([0], [0], color=ra_col, lw=1.0, label='RA meridians (2ʰ)'),
        Line2D([0], [0], color=dec_emph, lw=2.2, label='Celestial equator'),
        Line2D([0], [0], color=dec_col, lw=1.0, label='Dec parallels (15°)'),
        Line2D([0], [0], color=T["ecliptic"], lw=1.6, ls='--',
               label='Ecliptic'),
    ]
    for i, (beam_dec, origin) in enumerate(beams):
        centre = T["beams"][i % len(T["beams"])][0]
        near = nearest_major_source(beam_dec)
        name = f' ≈ {near}' if near else ''
        handles.append(Line2D([0], [0], color=centre, lw=2.6,
                              label=f'Beam Dec={beam_dec:+.1f}°{name}'))
    primary = T["beams"][0]
    handles += [
        Line2D([0], [0], color=primary[1], lw=8, alpha=0.45,
               label=f'HPBW 300 MHz (~{HPBW_300_MHZ:.1f}°)'),
        Line2D([0], [0], color=primary[2], lw=4, alpha=0.65,
               label=f'HPBW 1000 MHz (~{HPBW_1000_MHZ:.1f}°)'),
        Line2D([0], [0], marker='o',
               markerfacecolor=to_rgba(primary[0], 0.35),
               markeredgecolor=T["beam_now_edge"], markersize=11,
               markeredgewidth=1.2, lw=0, label='Beam now (300 MHz HPBW)'),
        Line2D([0], [0], marker='o', color=T["sun"], markersize=10,
               markeredgecolor=T["sun_edge"], markeredgewidth=1.4, lw=0,
               label='Sun (now)'),
        Line2D([0], [0], marker='o', color=T["moon"], markersize=8,
               markeredgecolor=T["moon_edge"], markeredgewidth=1.2, lw=0,
               label='Moon (now)'),
        Line2D([0], [0], marker='o', markerfacecolor='none',
               markeredgecolor=T["bright"], markersize=8, markeredgewidth=1.3,
               lw=0, label=f'Radio sources within {BEAM_FILTER_DEG:.0f}°'),
        Line2D([0], [0], marker='+', color=T["gc"], markersize=12, lw=0,
               markeredgewidth=2, label='Galactic center'),
    ]
    ax.legend(handles=handles, loc='lower left', fontsize=8.5,
              framealpha=0.92, bbox_to_anchor=(0.0, -0.22), ncol=3)

    plt.subplots_adjust(left=0.06, right=0.94, bottom=0.18, top=0.90)

    # Atomic write: choco serves this file, so it must never be readable
    # half-written.  A rename on the same filesystem is atomic.
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    plt.savefig(tmp, format='png', dpi=int(cfg["dpi"]), bbox_inches='tight',
                facecolor=T["face"])
    plt.close(fig)
    os.replace(tmp, out)
    return {
        "sun_ra_h": float(sun.ra.hour), "sun_dec_d": float(sun.dec.deg),
        "moon_ra_h": float(moon.ra.hour), "moon_dec_d": float(moon.dec.deg),
        "lst_h": lst_now_h,
    }


def load_config(path):
    cfg = dict(DEFAULTS)
    if path and os.path.exists(path):
        with open(path) as f:
            loaded = yaml.safe_load(f) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"{path}: top level must be a mapping")
        cfg.update({k: v for k, v in loaded.items() if v is not None})
    return cfg


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", default=None,
                        help="skymap.yaml (defaults apply if absent)")
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args.config)
    except (ValueError, yaml.YAMLError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    # Resolve the beams: list.  A malformed list is a config statement,
    # so exit 1; the live pointing lookup (a "pointing" entry) degrades
    # (exit 2, previous image stays up) when choco is unreachable.
    try:
        parsed = parse_beams(cfg["beams"])
    except ValueError as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    beams = []
    for dec, origin in parsed:
        if dec != POINTING:
            beams.append((dec, origin))
            continue
        try:
            live = fetch_pointings(cfg["choco_url"], cfg["group"])
        except (OSError, ValueError) as e:
            print(f"No pointing available: {e}", file=sys.stderr)
            return 2
        beams.extend((d, f"{g} config") for d, g in live)
    beams = dedup_beams(beams)

    # Day first (the landing card), then night when configured: the two
    # images are the same instant in two palettes, so *now* is fixed
    # before either render.  Each write is atomic on its own; a failure
    # partway leaves whichever images did land, both from this run or
    # both from the last.
    renders = [("day", cfg["output"])]
    if cfg["output_night"]:
        renders.append(("night", cfg["output_night"]))
    now = Time.now()
    try:
        for theme, output in renders:
            plot_skymap(cfg, beams, now=now, theme=theme, output=output)
    except OSError as e:
        print(f"Render failed: {e}", file=sys.stderr)
        return 2

    print(f"Saved {', '.join(o for _, o in renders)} "
          f"({'; '.join(beam_title(d, o) for d, o in beams)})")
    return 0


if __name__ == '__main__':
    sys.exit(main())
