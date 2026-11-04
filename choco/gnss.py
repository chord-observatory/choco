"""The GNSS transit forecast: when satellites cross the beam, and the quiet
windows between them.

kvand's gnss-fengine service (kotekan ``scripts/gnss/site/fengine_gains.py``
with ``transit_windows.py``; the gnss VM, port 12070) forecasts the next
hours of GNSS and GuoWang passes near the boresight and the quiet windows
between them -- no satellite, Sun or bright source near the beam -- as
JSON at ``GET /transit_windows`` (schema ``chord-gnss-transit-windows/0``),
refreshed every 5 min.  Its ``/metrics`` is scraped by Prometheus
(chord-sysops ``grafana_stack``), so Grafana keeps the history; choco
shows the forecast.

:class:`TransitMonitor` polls the forecast on the header strip's cadence
and keeps the latest one.  :func:`summarize` reads it for a moment:
whether the sky is quiet now, the next transit, the quiet windows long
enough for an F-engine gain measurement (what the forecast is for: a
digital-gain table is measured on a quiet sky) and the passes that were
not in the previous forecast.  :func:`timeline` lays it out for the
page's SVG.  The forecast's boresight is fixed by the service's own
arguments, so the monitor checks it against the array's pointing and
reports a disagreement as ``degraded``: the one way the forecast is wrong
without saying so.
"""

from __future__ import annotations

import datetime as dt
import math
import time

from .http import fetch
from .monitor import HttpMonitor

SCHEMA = "chord-gnss-transit-windows/0"
#: A forecast older than this (three missed 5-min refreshes) is stale.
STALE_AFTER_S = 900.0
#: The forecast's boresight may differ from the array's pointing by this much.
BORESIGHT_TOL_DEG = 0.5
#: Two passes of one satellite closer than this are the same pass seen
#: again: a refreshed TLE moves a pass by seconds, not minutes.
SAME_PASS_S = 300.0
#: The timeline starts this long before now.
LOOKBACK_S = 1800.0
#: The lane the Sun and the continuum sources are listed under.
ALL_BAND = "all"
_SOURCE_BAND = "all (continuum source)"
_PASS_KEYS = ("start_unix", "end_unix", "name", "kind", "min_sep_deg", "closest_unix", "bands")


class TransitMonitor(HttpMonitor):
    """The forecast as last fetched, and how that went.

    ``forecast`` is the latest good document (kept through a failed
    poll); ``new_transits`` the passes that were not in the forecast
    before it, ``new_since`` when they appeared.  ``pointing`` returns
    the array's co-elevation in degrees (south of zenith negative) or
    None; the boresight check needs it.
    """

    INTERVAL_S = 60
    DEFAULT_MIN_QUIET_S = 600.0

    def __init__(self, host, port, timeout: float = 5.0, min_quiet_s=None, pointing=None):
        super().__init__(host, port, timeout)
        self.min_quiet_s = float(min_quiet_s or self.DEFAULT_MIN_QUIET_S)
        self._pointing = pointing
        self.forecast: dict | None = None
        self.last_seen: float | None = None
        self.new_transits: list[dict] = []
        self.new_since: float | None = None
        self.boresight_error: str | None = None

    def poll_once(self) -> None:
        self.last_polled = time.time()
        if not self.configured:
            self.health, self.error = "unconfigured", "gnss.host / port not set"
            return
        try:
            doc = fetch("GET", f"{self.base_url}/transit_windows", timeout=self.timeout,
                        retries=1).json()
        except (OSError, ValueError) as e:
            self.health, self.error = "down", f"{self.host}:{self.port}: {e}"
            return
        problem = check_forecast(doc)
        if problem:
            self.health, self.error = "degraded", problem
            return
        now = time.time()
        self.last_seen = now
        previous, self.forecast = self.forecast, doc
        if previous is not None and doc["created_unix"] != previous["created_unix"]:
            new = new_passes(doc["transits"], previous["transits"])
            if new:
                self.new_transits, self.new_since = new, now
        pointing = self._pointing() if self._pointing else None
        self.boresight_error = boresight_check(doc["boresight"], pointing)
        age = now - doc["created_unix"]
        if age > STALE_AFTER_S:
            self.health, self.error = "stale", f"the forecast is {age / 60:.0f} min old"
        elif self.boresight_error:
            self.health, self.error = "degraded", self.boresight_error
        else:
            self.health, self.error = "ok", None

    def summary(self, now: float | None = None) -> dict | None:
        """:func:`summarize` of the current forecast, with the new passes."""
        if self.forecast is None:
            return None
        now = time.time() if now is None else now
        out = summarize(self.forecast, now, self.min_quiet_s)
        out["new_transits"] = [_pass_view(p, now) for p in self.new_transits]
        out["new_since"] = self.new_since
        out["boresight_error"] = self.boresight_error
        return out

    def to_dict(self) -> dict:
        now = time.time()
        s = self.summary(now)
        return {
            "configured": self.configured,
            "host": self.host,
            "port": self.port,
            "health": self.health,
            "error": self.error,
            "last_polled": self.last_polled,
            "last_seen": self.last_seen,
            "forecast_age_s": s["age_s"] if s else None,
            "forecast_end": s["end"] if s else None,
            "quiet_now": s["quiet_now"] if s else None,
            "next_transit": s["next_transit"] if s else None,
            "gain_window": s["gain_windows"][0] if s and s["gain_windows"] else None,
            "n_new": len(self.new_transits),
        }


def check_forecast(doc) -> str | None:
    """Why *doc* is not a forecast this module reads, or None."""
    if not isinstance(doc, dict):
        return "the answer is not a JSON object"
    if doc.get("schema") != SCHEMA:
        return f"schema {doc.get('schema')!r}, not {SCHEMA}"
    for key in ("created_unix", "start_unix", "end_unix"):
        if not isinstance(doc.get(key), (int, float)):
            return f"no {key}"
    if not isinstance(doc.get("boresight"), dict):
        return "no boresight"
    if not isinstance(doc.get("transits"), list):
        return "no transits"
    for p in doc["transits"]:
        if not isinstance(p, dict) or not all(k in p for k in _PASS_KEYS):
            return "a pass lacks a field"
    quiet = doc.get("quiet")
    if not isinstance(quiet, dict) or not isinstance(quiet.get(ALL_BAND), list):
        return "no quiet windows"
    if not isinstance(doc.get("quiet_by_band"), dict):
        return "no quiet windows per band"
    return None


def new_passes(transits: list[dict], previous: list[dict]) -> list[dict]:
    """The passes of *transits* that *previous* did not have: the same
    satellite within :data:`SAME_PASS_S` of the closest approach is the
    same pass."""
    out = []
    for p in transits:
        if not any(q["name"] == p["name"] and abs(q["closest_unix"] - p["closest_unix"]) < SAME_PASS_S
                   for q in previous):
            out.append(p)
    return out


def boresight_check(boresight: dict, coelev) -> str | None:
    """How the forecast's boresight differs from the array's co-elevation
    (``dish_coelev_deg``: degrees from zenith, south negative, so −27.3
    is az 180, el 62.7), or None when they agree or there is no pointing."""
    if coelev is None:
        return None
    try:
        coelev = float(coelev)
        el = float(boresight["el_deg"])
        az = float(boresight.get("az_deg", 180.0))
    except (KeyError, TypeError, ValueError):
        return "the forecast's boresight is unreadable"
    want_el = 90.0 - abs(coelev)
    want_az = 180.0 if coelev < 0 else 0.0
    if abs(el - want_el) > BORESIGHT_TOL_DEG or (
            abs(coelev) > BORESIGHT_TOL_DEG and abs(((az - want_az) + 180.0) % 360.0 - 180.0) > BORESIGHT_TOL_DEG):
        return (f"the forecast is for az {az:g}°, el {el:g}°; the array points at "
                f"az {want_az:g}°, el {want_el:g}° (co-elevation {coelev:g}°)")
    return None


# ---- reading a forecast for one moment ---------------------------------------

def fmt_hm(ts) -> str:
    """``HH:MM`` UTC."""
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%H:%M")


def fmt_utc(ts) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_length(seconds) -> str:
    """``42 min``, ``1 h 05 min``, ``50 s``."""
    seconds = max(0.0, float(seconds))
    if seconds < 90:
        return f"{seconds:.0f} s"
    minutes = int(round(seconds / 60))
    if minutes < 60:
        return f"{minutes} min"
    return f"{minutes // 60} h {minutes % 60:02d} min"


def fmt_in(seconds) -> str:
    """``in 12 min``, ``now``, ``5 min ago``."""
    seconds = float(seconds)
    if abs(seconds) < 30:
        return "now"
    return f"in {fmt_length(seconds)}" if seconds > 0 else f"{fmt_length(-seconds)} ago"


def _pass_view(p: dict, now: float) -> dict:
    return {
        "name": p["name"], "kind": p["kind"], "bands": list(p["bands"]),
        "start": p["start_unix"], "end": p["end_unix"], "closest": p["closest_unix"],
        "min_sep_deg": p["min_sep_deg"],
        "start_fmt": fmt_hm(p["start_unix"]), "end_fmt": fmt_hm(p["end_unix"]),
        "closest_fmt": fmt_hm(p["closest_unix"]),
        "in_fmt": fmt_in(p["start_unix"] - now) if p["start_unix"] > now
        else "now" if now < p["end_unix"] else fmt_in(p["end_unix"] - now),
        "state": "past" if p["end_unix"] <= now else "now" if p["start_unix"] <= now else "ahead",
    }


def _in_window(windows, now: float) -> bool:
    return any(a <= now < b for a, b in windows)


def summarize(forecast: dict, now: float, min_quiet_s: float) -> dict:
    """What the forecast says at *now*: the sky's state, the next transit,
    the quiet windows at least *min_quiet_s* long that are still ahead
    (a window already begun counts its remainder) and every pass with its
    times formatted."""
    quiet_all = forecast["quiet"][ALL_BAND]
    by_band = forecast["quiet_by_band"]
    bands = list(by_band)
    passes = sorted(forecast["transits"], key=lambda p: (p["start_unix"], p["name"]))
    closest = [p for p in passes if p["kind"] == "closest"]
    active = [p for p in closest if p["start_unix"] <= now < p["end_unix"]]
    ahead = [p for p in closest if p["start_unix"] > now]
    gain_windows = []
    for a, b in quiet_all:
        start = max(a, now)
        if b > now and b - start >= min_quiet_s:
            gain_windows.append({
                "start": start, "end": b, "length_s": b - start, "current": a <= now,
                "start_fmt": "now" if a <= now else fmt_hm(start), "end_fmt": fmt_hm(b),
                "length_fmt": fmt_length(b - start), "in_fmt": fmt_in(start - now),
            })
    sun_sep = forecast.get("sun_sep_deg") or []
    sun_el = forecast.get("sun_el_deg") or []
    return {
        "created": forecast["created_unix"], "created_fmt": fmt_utc(forecast["created_unix"]),
        "age_s": now - forecast["created_unix"],
        "start": forecast["start_unix"], "end": forecast["end_unix"],
        "end_fmt": fmt_hm(forecast["end_unix"]),
        "horizon_fmt": fmt_length(forecast["end_unix"] - now),
        "bands": bands,
        "quiet_now": _in_window(quiet_all, now),
        "quiet_now_by_band": {b: _in_window(by_band.get(b) or [], now) for b in bands},
        "active": [_pass_view(p, now) for p in active],
        "next_transit": _pass_view(ahead[0], now) if ahead else None,
        "gain_windows": gain_windows,
        "min_quiet_s": min_quiet_s, "min_quiet_fmt": fmt_length(min_quiet_s),
        "transits": [_pass_view(p, now) for p in passes],
        "n_closest": len(closest),
        "boresight": forecast["boresight"],
        "criteria": forecast.get("criteria") or {},
        "tle_age_days": forecast.get("tle_age_days") or [],
        "sun": {"sep_min": min(sun_sep), "sep_max": max(sun_sep),
                "el_min": min(sun_el), "el_max": max(sun_el)} if sun_sep and sun_el else None,
    }


# ---- the timeline --------------------------------------------------------

#: Geometry of the page's timeline in SVG user units.
TL_WIDTH = 1000
TL_GUTTER = 190
TL_LANE_H = 26
TL_AXIS_H = 22
TL_TOP = 6
TL_TICK_S = 900


def timeline(forecast: dict, now: float, min_quiet_s: float) -> dict:
    """The forecast as lanes for the page's SVG: an ``all`` lane (the quiet
    windows for the whole sky, the Sun and the continuum sources), then one
    per band with its quiet windows and passes.  Positions are SVG x
    coordinates; a pass is labelled when its bar is wide enough."""
    t0 = now - LOOKBACK_S
    t1 = max(forecast["end_unix"], now + 60)
    plot_w = TL_WIDTH - TL_GUTTER - 10

    def x(t):
        return TL_GUTTER + plot_w * min(1.0, max(0.0, (t - t0) / (t1 - t0)))

    def windows(wins, gain=False):
        out = []
        for a, b in wins:
            if b <= t0 or a >= t1:
                continue
            out.append({"x0": x(a), "x1": x(b),
                        "gain": gain and b > now and b - max(a, now) >= min_quiet_s})
        return out

    def bars(passes):
        out = []
        for p in passes:
            if p["end_unix"] <= t0 or p["start_unix"] >= t1:
                continue
            x0, x1 = x(p["start_unix"]), x(p["end_unix"])
            if x1 - x0 < 3:
                x0, x1 = x(p["closest_unix"]) - 1.5, x(p["closest_unix"]) + 1.5
            out.append({
                "x0": x0, "x1": x1, "name": p["name"],
                "kind": "source" if _SOURCE_BAND in p["bands"] else p["kind"],
                "label": p["name"] if x1 - x0 > 8 * len(p["name"]) else "",
                "title": (f"{p['name']}: {p['kind']} pass, {p['min_sep_deg']:.1f}° at "
                          f"{fmt_hm(p['closest_unix'])}, {fmt_hm(p['start_unix'])}–{fmt_hm(p['end_unix'])} UTC"),
            })
        return out

    passes = forecast["transits"]
    lanes = [{"name": ALL_BAND, "label": "all bands",
              "quiet": windows(forecast["quiet"][ALL_BAND], gain=True),
              "bars": bars([p for p in passes if _SOURCE_BAND in p["bands"]])}]
    for band, wins in forecast["quiet_by_band"].items():
        lanes.append({"name": band, "label": band, "quiet": windows(wins),
                      "bars": bars([p for p in passes if band in p["bands"]])})
    ticks = []
    t = math.ceil(t0 / TL_TICK_S) * TL_TICK_S
    while t <= t1:
        ticks.append({"x": x(t), "label": fmt_hm(t)})
        t += TL_TICK_S
    for i, lane in enumerate(lanes):
        lane["y"] = TL_TOP + i * TL_LANE_H
    return {
        "width": TL_WIDTH, "height": TL_TOP + len(lanes) * TL_LANE_H + TL_AXIS_H,
        "gutter": TL_GUTTER, "lane_h": TL_LANE_H, "plot_x1": TL_GUTTER + plot_w,
        "axis_y": TL_TOP + len(lanes) * TL_LANE_H,
        "now_x": x(now), "ticks": ticks, "lanes": lanes,
        "t0_fmt": fmt_hm(t0), "t1_fmt": fmt_hm(t1),
    }
