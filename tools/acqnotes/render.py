"""Write README.md + acq_info.yaml into every subset acquisition directory.

choco's /files table shows each README behind an Info button, next to the
acquisition's file span, and the copy of timeline.yaml in the root behind a
Timeline button; there is no index file.

Joins three sources:
  - the measurements made by extract.py, transit.py, polswap.py and zerovis.py
    (JSON files under --work), i.e. what each acquisition's own files say;
  - timeline.yaml (curated history: label/table changes, swaps, deployments);
  - fixed physical facts about the 2026-09 reversed dish table.

Usage:
  python render.py --work DIR [--root /mnt/cs00/data/kotekan_vis_files/subset] [--dry-run]
"""
import argparse
import datetime
import glob
import json
import os
import re
import shutil
import statistics
from zoneinfo import ZoneInfo

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
PACIFIC = ZoneInfo("America/Vancouver")
UTC = datetime.timezone.utc
LAT = 49.32075144444
FILE_S = 200.0  # one file = 20 bins x 10 s

# The 2026-09-09 .. 2026-10-01 20:18 files used the "Sep 2" dish table, reversed within
# each group of eight inputs (sky audit 2026-09-21).
REVERSED_FROM = datetime.datetime(2026, 9, 9, 4, 0, tzinfo=UTC)
REVERSED_TO = datetime.datetime(2026, 10, 1, 20, 18, tzinfo=UTC)
# Stream map of the chord-build nodes (frequency index mod 16).
FRINGESTOP_STREAMS = {"cx47": (1, 9), "cx52": (3, 11)}

LABEL_RE = re.compile(r"^(RFI)?([A-H])(\d+)(p[12]|[XY])?$")


# ---------------------------------------------------------------- helpers

def ts(name):
    """Start time of an acquisition from its directory name."""
    m = re.match(r"acq_(\d{8})_(\d{6})", name)
    return datetime.datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def file_time(part):
    """A file's start time from its 'YYYYMMDDT_HHMMSS' name part (extract.py keeps from YYYMMDD on)."""
    m = re.search(r"(\d{7,8})T_(\d{6})", part)
    day = m.group(1) if len(m.group(1)) == 8 else "2" + m.group(1)
    return datetime.datetime.strptime(day + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def local(dt):
    return dt.astimezone(PACIFIC)


def fmt_both(dt):
    lt = local(dt)
    return "%s UTC | %s %s" % (dt.strftime("%Y-%m-%d %H:%M"), lt.strftime("%Y-%m-%d %H:%M"), lt.tzname())


def dish_name(row, num, rfi):
    if rfi:
        return "RFI%s%d" % (row, num)
    return "%s%02d" % (row, num)


def parse_label(lab):
    """'A1X', 'A01X', 'B4p1', 'RFIA1Y', 'B4' -> (dish, pol or None); None for Fake/Missing."""
    m = LABEL_RE.match(lab)
    if not m:
        return None
    rfi, row, num, pol = m.groups()
    if pol in ("p1", "X"):
        pol = "X"
    elif pol in ("p2", "Y"):
        pol = "Y"
    return dish_name(row, int(num), bool(rfi)), pol


def physical_from_reversed(dish):
    """Physical dish for a label written with the reversed Sep-2 table."""
    m = LABEL_RE.match(dish)
    rfi, row, num, _ = m.groups()
    num = int(num)
    if rfi:
        # RFI rows were reordered by inference only; assume the same mirror within 8.
        idx = (0 if row == "A" else 4) + num - 1  # RFIA1..RFIB4 -> 0..7
        idx = 7 - idx
        return dish_name("A" if idx < 4 else "B", idx % 4 + 1, True)
    new_row = "B" if row == "A" else "A"
    new_num = 5 - num if num <= 4 else 13 - num
    return dish_name(new_row, new_num, False)


def element_axis(sample, start):
    """Per element of the file's data axis: dict(file_label, dish, pol, phys, note)."""
    ne = int(sample["attrs"]["num_elements"])
    lab = sample.get("im_label")
    il = sample["attrs"].get("input_list")
    out = []
    reversed_table = REVERSED_FROM <= start < REVERSED_TO
    for e in range(ne):
        note = ""
        if lab is None:
            # 8-element spring files: the embedded 6-row table, elements 6-7 unused.
            names = ["A01p1", "A01p2", "A06p1", "A06p2", "A07p1", "A07p2"]
            fl = names[e] if e < len(names) else "unused"
            note = "label from embedded config (file has none)"
        elif len(lab) == ne:
            fl = lab[e]
        elif il is not None and len(lab) == 64:
            fid = il[e]
            fl = lab[fid % 64] + ("p1" if fid < 64 else "p2")
        else:
            fl = lab[e]
        p = parse_label(fl)
        if p is None:
            out.append({"file_label": fl, "dish": None, "pol": None, "phys": None, "note": note})
            continue
        dish, pol = p
        phys = physical_from_reversed(dish) if reversed_table else dish
        out.append({"file_label": fl, "dish": dish, "pol": pol, "phys": phys, "note": note})
    return out


SOURCE_NAMES = {"CygA": "Cyg A", "CasA": "Cas A", "TauA": "Tau A", "VirA": "Vir A", "Sun": "Sun"}


def src_name(s):
    return SOURCE_NAMES.get(s, s)


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def pct(x):
    return "—" if x is None else "%.0f %%" % (100 * x)


# ---------------------------------------------------------------- per-acquisition facts

def config_groups(samples):
    """Distinct cx (X-engine) configs across the sampled files."""
    groups = {}
    for s in samples:
        for c in s.get("configs", []):
            if "dpdkCore" not in c.get("stages", []):
                continue
            k = c["keys"]
            key = (
                c.get("kotekan_version"),
                str(k.get("/n2_accumulate/do_fringestop")),
                str(k.get("/rfi_first_stage_excision_enabled")),
                str(k.get("/rfi_sk_rfimask_sigmas")),
                str(k.get("/rfi_mu_min")),
                str(k.get("/rfi_mu_max")),
                "RfiFrameMask" in c["stages"],
            )
            g = groups.setdefault(key, {"insts": set(), "count": 0})
            names = {i.split(".")[0] for i in c.get("inst", [])}
            g["insts"] |= names
            g["count"] = max(g["count"], sum(1 for cc in s["configs"] if cc.get("json_hash") == c.get("json_hash")))
            g["hashes"] = g.get("hashes", set()) | {c.get("json_hash")}
    out = []
    for key, g in groups.items():
        ver, fs, fse, sig, mumin, mumax, rfi_stage = key
        out.append({
            "kotekan_version": ver,
            "nodes": sorted(g["insts"]) or None,
            "n_distinct_configs": len(g["hashes"]),
            "do_fringestop": fs,
            "rfi_stages": rfi_stage,
            "rfi_first_stage_excision_enabled": fse,
            "rfi_sk_rfimask_sigmas": sig,
            "rfi_mu_min": mumin,
            "rfi_mu_max": mumax,
        })
    return sorted(out, key=lambda g: (g["kotekan_version"] or "", str(g["nodes"])))


def recv_versions(samples):
    vs = set()
    for s in samples:
        for c in s.get("configs", []):
            if "hdf5N2Write" in c.get("stages", []) and c.get("kotekan_version"):
                vs.add(c["kotekan_version"])
    return sorted(vs)


def source_table(tr, axis):
    """Array-level transit response per source."""
    rows = []
    if not tr:
        return rows
    live_idx = [i for i, a in enumerate(axis) if a["dish"] and not a["dish"].startswith("RFI")]
    for t in tr["transits"]:
        r = {"source": t["source"], "dec": t["dec"],
             "transit": datetime.datetime.fromtimestamp(t["transit_unix"], UTC), "bands": []}
        for b in t["bands"]:
            vals, snrs = [], []
            for i in live_idx:
                x = b["rows"][i] if i < len(b["rows"]) else None
                if x and x["rms"] > 0:
                    vals.append(x["bump"])
                    snrs.append(x["bump"] / x["rms"])
            det = [v for v, s_ in zip(vals, snrs) if s_ > 3 and v > 0.02]
            r["bands"].append({
                "band": b["band"],
                "median_rise": median(det) if len(det) >= max(2, len(vals) // 3) else None,
                "n_detected": len(det), "n": len(vals)})
        rows.append(r)
    return rows


def per_element_sky(tr, axis, source="CygA"):
    """Per element: rise at transit (440-460 / 600-620 MHz) and peak hour angle."""
    res = [dict(rise=None, snr=None, ha=None, src=None) for _ in axis]
    if not tr:
        return res
    best = None
    # the in-beam source: the Sun before the summer re-pointing (dishes near Dec +20),
    # Cyg A afterwards (near Dec +41); weaker sources are sidelobe-level and not used
    order = ["Sun", "CygA"] if tr["acq"] < "acq_20260601" else ["CygA", "Sun"]
    n_array = sum(1 for a in axis if a["dish"] and not a["dish"].startswith("RFI"))
    for t in sorted((t for t in tr["transits"] if t["source"] in order), key=lambda t: order.index(t["source"])):
        b = t["bands"][0]["rows"]
        n = sum(1 for x in b if x and x["rms"] > 0 and x["bump"] / x["rms"] > 3 and x["bump"] > 0.02)
        if n >= max(2, 0.4 * n_array):
            best = (n, t)
            break
    if best is None:
        # too few inputs see the in-beam source: the sky test is inconclusive here
        return res
    t = best[1]
    for i in range(len(axis)):
        rises, snrs, has = [], [], []
        for b in t["bands"][:2]:
            x = b["rows"][i] if i < len(b["rows"]) else None
            if x and x["rms"] > 0:
                rises.append(x["bump"])
                snrs.append(x["bump"] / x["rms"])
                if x["ha_peak"] is not None:
                    has.append(x["ha_peak"])
        if rises:
            res[i] = dict(rise=max(rises), snr=max(snrs), ha=median(has), src=t["source"])
    return res, t["source"], t["dec"]


def pol_check(pol, axis, start):
    """Per physical dish: median cross/co amplitude ratio at the transit."""
    if not pol or not pol.get("result"):
        return None
    r = pol["result"]
    amp, contrast = {}, []
    for a, b, x, y in zip(r["prod_a"], r["prod_b"], r["amp_on"], r["amp_off"]):
        amp[(a, b)] = amp[(b, a)] = x
        if a != b and x is not None and y and y > 0:
            contrast.append(x / y)
    by = {}
    for i, a in enumerate(axis):
        if a["phys"] and not a["phys"].startswith("RFI"):
            by.setdefault(a["phys"], {})[a["pol"]] = i
    dishes = [d for d, v in by.items() if "X" in v and "Y" in v]
    out = {}
    for d in dishes:
        co, cr = [], []
        for k in dishes:
            if k == d:
                continue
            c1 = [amp.get((by[d]["X"], by[k]["X"])), amp.get((by[d]["Y"], by[k]["Y"]))]
            c2 = [amp.get((by[d]["X"], by[k]["Y"])), amp.get((by[d]["Y"], by[k]["X"]))]
            if None in c1 or None in c2:
                continue
            co.append(sum(c1) / 2)
            cr.append(sum(c2) / 2)
        if co:
            ratios = [c / o for c, o in zip(cr, co) if o > 0]
            out[d] = {"co": median(co), "cross": median(cr), "ratio": median(ratios)}
    if not out:
        return None
    live_co = median([v["co"] for v in out.values()])
    # The test is conclusive when the transit dominates the fringes, i.e. the typical dish
    # shows cross-pol well below co-pol (healthy feeds give 0.1-0.3).  When noise or diffuse
    # emission dominates, every ratio sits near 1 and the test says nothing.
    c = median(contrast)
    typical = median([v["ratio"] for v in out.values() if v["ratio"] is not None and v["co"] > 0.2 * live_co])
    conclusive = typical is not None and typical < 0.4
    for d, v in out.items():
        v["live"] = max(v["co"], v["cross"]) > 0.2 * live_co  # a swapped dish has weak co-pol
        v["swapped"] = conclusive and v["live"] and v["ratio"] is not None and v["ratio"] > 1.0
    return {"source": r["source"], "transit": datetime.datetime.fromtimestamp(r["transit_unix"], UTC),
            "dishes": out, "contrast": c, "conclusive": conclusive}


def timeline_for(entries, name, start, end):
    out = []
    for e in entries:
        if "acqs" in e:
            if name in e["acqs"]:
                out.append(e)
            continue
        f = e.get("from")
        t = e.get("to")
        f = _as_dt(f)
        t = _as_dt(t) if t else None
        if (t is None or start < t) and (end is None or f is None or end >= f):
            out.append(e)
    return out


def _as_dt(v):
    if v is None:
        return None
    if isinstance(v, datetime.datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)
    if isinstance(v, datetime.date):
        return datetime.datetime(v.year, v.month, v.day, tzinfo=UTC)
    s = str(v)
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(s, fmt).replace(tzinfo=UTC)
        except ValueError:
            pass
    raise ValueError(v)


# ---------------------------------------------------------------- inference across acquisitions

def infer_missing_configs(acqs):
    """Acquisitions whose files embed no cx config borrow the nearest one with the same recv build."""
    for i, a in enumerate(acqs):
        if a["cx_groups"] or not a["samples"]:
            continue
        cands = []
        for j, b in enumerate(acqs):
            if b["cx_groups"] and b["recv_versions"] and set(b["recv_versions"]) & set(a["recv_versions"]):
                cands.append((abs(j - i), b))
        if cands:
            b = min(cands, key=lambda c: c[0])[1]
            a["cx_groups_inferred_from"] = b["name"]
            a["cx_groups_inferred"] = b["cx_groups"]


# ---------------------------------------------------------------- rendering

def analyse(name, root, work, entries, zerovis):
    d = os.path.join(root, name)
    meta = json.load(open(os.path.join(work, "meta", name + ".json")))
    trp = os.path.join(work, "transit", name + ".json")
    tr = json.load(open(trp)) if os.path.exists(trp) else None
    pp = os.path.join(work, "pol", name + ".json")
    pol = json.load(open(pp)) if os.path.exists(pp) else None
    samples = [s for s in meta["samples"] if "error" not in s]
    start = ts(name)
    a = {"name": name, "dir": d, "n_files": meta["n_files"], "bytes": meta["total_bytes"],
         "samples": samples, "start_dir": start}
    times = [file_time(x) for x in meta["file_times"]] if meta["n_files"] else []
    a["first_file"] = times[0] if times else None
    a["last_file_end"] = times[-1] + datetime.timedelta(seconds=FILE_S) if times else None
    gaps = []
    for t0, t1 in zip(times, times[1:]):
        if (t1 - t0).total_seconds() > 1.6 * FILE_S:
            gaps.append((t0 + datetime.timedelta(seconds=FILE_S), t1))
    a["gaps"] = gaps
    a["recv_versions"] = recv_versions(samples)
    a["cx_groups"] = config_groups(samples)
    a["n_configs"] = [len(s.get("configs", [])) for s in samples]
    if samples:
        s0 = samples[len(samples) // 2]
        a["attrs"] = s0["attrs"]
        a["axis"] = element_axis(s0, start)
    else:
        a["attrs"], a["axis"] = {}, []
    a["timeline"] = timeline_for(entries, name, a["first_file"] or start, a["last_file_end"])
    a["sources"] = source_table(tr, a["axis"]) if a["axis"] else []
    sky = per_element_sky(tr, a["axis"]) if a["axis"] else None
    if isinstance(sky, tuple):
        a["sky"], a["sky_source"], a["sky_dec"] = sky
    else:
        a["sky"], a["sky_source"], a["sky_dec"] = [dict(rise=None, snr=None, ha=None, src=None)] * len(a["axis"]), None, None
    a["pol"] = pol_check(pol, a["axis"], start) if a["axis"] else None
    a["zerovis"] = zerovis.get(name)
    return a


def element_status(a):
    """Per element: flagged in file?, alive on the sky?, hot?"""
    axis = a["axis"]
    flags = []
    if a["start_dir"] >= REVERSED_FROM:  # `flags` carries the applied mask from 2026-09-09 on
        for s in a["samples"]:
            f = s.get("flags_zero_frac_per_el")
            # a file with most of the band missing reads as all-flagged: skip it
            if f and (s.get("freq_live_frac") or 0) >= 0.9:
                flags.append(f)
    zero = []
    if a["zerovis"]:
        for z in a["zerovis"]:
            if z["zero_frac"] and any(x and x > 0.5 for x in z["zero_frac"]):
                zero.append(z["zero_frac"])
    autos = [s.get("auto_median_per_el") for s in a["samples"] if s.get("auto_median_per_el")]
    out = []
    live_autos = []
    for i, el in enumerate(axis):
        auto = median([x[i] for x in autos if i < len(x)])
        if el["dish"] and not el["dish"].startswith("RFI"):
            live_autos.append(auto)
    ref = median(live_autos)
    for i, el in enumerate(axis):
        fl = median([f[i] for f in flags]) if flags else None
        changed = bool(flags) and len({f[i] > 0.9 for f in flags}) > 1
        zf = median([z[i] for z in zero]) if zero else None
        flagged = None
        how = None
        if fl is not None:
            flagged = fl > 0.9
            how = "flags"
        elif zf is not None:
            flagged = zf > 0.8
            how = "zeroed baselines"
        sky = a["sky"][i] if i < len(a["sky"]) else {}
        rise = sky.get("rise")
        alive = None
        if rise is not None:
            alive = (sky.get("snr") or 0) > 3 and rise > 0.02
        auto = median([x[i] for x in autos if i < len(x)])
        hot = auto is not None and ref and auto > 4 * ref
        out.append({"flagged": flagged, "flag_record": how, "alive": alive, "rise": rise,
                    "ha": sky.get("ha"), "auto": auto, "hot": hot, "flag_changed": changed})
    return out


def verdict(a, st):
    notes = []
    name = a["name"]
    if a["n_files"] == 0:
        return "Empty: kotekan restarted before it wrote a file."
    for e in a["timeline"]:
        if e["topic"] == "data" and "acqs" in e:
            if "Do not use the cross-correlations" in e["text"]:
                notes.append("do not use the cross-correlations (no fringes)")
            if "one X-engine node" in e["text"]:
                notes.append("only 12 % of the band has data")
    rev = REVERSED_FROM <= a["start_dir"] < REVERSED_TO
    if rev:
        notes.append("dish labels are WRONG (reversed table; use the physical names below)")
    elif a["start_dir"] < datetime.datetime(2026, 5, 21, tzinfo=UTC) and not a["samples"][0].get("im_label"):
        notes.append("the files have no labels (3 dishes: A01, A06, A07)")
    else:
        notes.append("labels are physical")
    swapped = swapped_dishes(a)
    if swapped:
        notes.append("X/Y are swapped on " + ", ".join(swapped))
    fs = {g["do_fringestop"] for g in a["cx_groups"]}
    if "True" in fs and "False" in fs:
        notes.append("only cx47 and cx52 fringestop")
    elif fs == {"True"}:
        notes.append("all nodes fringestop")
    p = pointing_summary(a)
    if p:
        notes.append(p["short"])
    fc = flag_check(a, st)
    if fc["status"] == "correct":
        notes.append("bad-feed flags agree with the sky")
    elif fc["status"] == "wrong":
        if fc["live_flagged"]:
            notes.append("bad-feed mask flags %d live inputs%s" % (
                len(fc["live_flagged"]), " and not %d dead inputs" % len(fc["dead_unflagged"]) if fc["dead_unflagged"] else ""))
        else:
            notes.append("bad-feed mask does not flag %d dead inputs (%s)" % (
                len(fc["dead_unflagged"]), ", ".join(fc["dead_unflagged"])))
    elif fc["status"] == "none" and fc["dead"]:
        notes.append("no bad-feed mask; %d dead inputs have no flag" % len(fc["dead"]))
    return "; ".join(notes) if notes else "No known problems."


def swapped_dishes(a):
    """Physical dishes with swapped feeds: this acquisition's own check when it is conclusive,
    else the curated history."""
    if a["pol"] and a["pol"]["conclusive"]:
        return sorted(d for d, v in a["pol"]["dishes"].items() if v["swapped"])
    out = set()
    for e in a["timeline"]:
        if e["topic"] == "polarization":
            out |= set(e.get("swapped", []))
    return sorted(out)


def flag_check(a, st):
    """Compare the applied mask with which inputs see the sky."""
    arr = [(el, x) for el, x in zip(a["axis"], st) if el["dish"] and not el["dish"].startswith("RFI")]
    rec = {x["flag_record"] for _, x in arr if x["flag_record"]}
    dead = [_phys(el) for el, x in arr if x["alive"] is False]
    if not rec:
        return {"status": "none", "dead": dead, "dead_unflagged": dead, "live_flagged": []}
    du = [_phys(el) for el, x in arr if x["alive"] is False and x["flagged"] is False]
    lf = [_phys(el) for el, x in arr if x["alive"] and x["flagged"] and not x["hot"]]
    if not a["sky_source"]:
        status = "unknown"
    else:
        status = "wrong" if (du or lf) else "correct"
    return {"status": status, "dead": dead, "dead_unflagged": du, "live_flagged": lf}


def pointing_summary(a):
    src = {s["source"]: s for s in a["sources"]}

    def rise(name, k=0):
        s = src.get(name)
        if not s or len(s["bands"]) <= k:
            return None
        return s["bands"][k]["median_rise"]

    cyg = rise("CygA")
    sun = rise("Sun")
    sun_dec = src["Sun"]["dec"] if "Sun" in src else None
    tau = rise("TauA")
    if cyg is not None and cyg >= 0.35:
        return {"dec": "+41 (Cyg A), co-elevation about -8.6",
                "short": "the dishes point near Dec +41 (Cyg A; co-elevation -8.6), not at the config value +22 (-27.3)",
                "why": "Cyg A increases the autocorrelations of the live dishes by %s at 440-460 MHz.  With Cyg A "
                       "in the main beam (October 2026) the increase is 60 %%" % pct(cyg)
                       + (".  Tau A (+22.0) gives %s" % pct(tau) if tau else ".  Tau A (+22.0) gives no signal"
                          if "TauA" in src else "")
                       + (".  The Sun (Dec %+.0f) gives no signal" % sun_dec if "Sun" in src and not sun else "")}
    if sun is not None and sun > 1.0 and sun_dec is not None and sun_dec > 8:
        return {"dec": "about +20 (Sun at %+.0f in the main beam), co-elevation about -29" % sun_dec,
                "short": "the dishes point near Dec +20 (co-elevation -29), near the config value +22 (-27.3)",
                "why": "the Sun (Dec %+.1f) increases the autocorrelations %.0f times at 440-460 MHz, thus it is in "
                       "the main beam.  Cyg A (+40.7) gives only %s, from a sidelobe or from the diffuse "
                       "Cygnus emission" % (sun_dec, sun, pct(cyg))}
    if cyg is not None and cyg >= 0.12 and (sun is None or sun_dec is None or sun_dec < 8):
        return {"dec": "undetermined", "short": "pointing is not known (Cyg A gives only %s)" % pct(cyg),
                "why": "Cyg A increases the autocorrelations by only %s.  No other source gives a "
                       "result" % pct(cyg)}
    return None


def render_readme(a, st, gen_time):
    L = []
    name = a["name"]
    L.append("# %s\n" % name)
    L.append("**Summary:** %s\n" % verdict(a, st))
    L.append("*About these notes: this is a CHORD pathfinder N² subset acquisition (recv1 "
             "`hdf5_N2_write_subset`).  `choco/tools/acqnotes/render.py` wrote these notes on %s.  It uses "
             "the files in this directory and the history in `tools/acqnotes/timeline.yaml`.  Do not edit "
             "the README files.  To change them, edit the timeline or the scripts, then render again.  "
             "`acq_info.yaml` in this directory has the same data in machine-readable form.  Evidence tags: "
             "[file] comes from the HDF5 files of this acquisition; [sky] is measured from the data "
             "(transits, fringes); [history] comes from the kotekan and choco history or from earlier "
             "audits; [inferred] comes from a neighbouring acquisition.*\n" % gen_time.strftime("%Y-%m-%d"))

    # ---- time span
    L.append("## Time span\n")
    if a["n_files"] == 0:
        L.append("kotekan wrote no files.  The directory was made at %s.\n" % fmt_both(a["start_dir"]))
    else:
        dur = a["last_file_end"] - a["first_file"]
        L.append("| | UTC | Local (Penticton) |")
        L.append("|---|---|---|")
        for lbl, t in (("Start (first file)", a["first_file"]), ("End (last file + 200 s)", a["last_file_end"])):
            lt = local(t)
            L.append("| %s | %s | %s %s |" % (lbl, t.strftime("%Y-%m-%d %H:%M"), lt.strftime("%Y-%m-%d %H:%M"),
                                              lt.tzname()))
        L.append("")
        L.append("Duration %.1f h, %d files (20 x 10 s bins each), %.0f GB. [file]" % (
            dur.total_seconds() / 3600, a["n_files"], a["bytes"] / 1e9))
        if a["gaps"]:
            L.append("Gaps between files: " + ", ".join(
                "%s to %s UTC" % (g0.strftime("%m-%d %H:%M"), g1.strftime("%m-%d %H:%M")) for g0, g1 in a["gaps"]) + ".")
        live = [s.get("freq_live_frac") for s in a["samples"] if s.get("freq_live_frac") is not None]
        if live:
            L.append("Part of the 6145 frequencies that has data, in each sampled file: %s. [file]" % ", ".join(
                "%.0f %%" % (100 * x) for x in live))
        L.append("")

    if not a["samples"]:
        _timeline_section(L, a)
        return "\n".join(L) + "\n"

    # ---- software
    L.append("## Software and nodes\n")
    at = a["attrs"]
    L.append("- recv kotekan: %s [file]" % (", ".join("`%s`" % v for v in a["recv_versions"]) or "not recorded"))
    groups = a["cx_groups"]
    inferred = False
    if not groups and a.get("cx_groups_inferred"):
        groups = a["cx_groups_inferred"]
        inferred = True
    if a["cx_groups"]:
        L.append("- Number of X-engine (cx) configs in each sampled file: %s. [file]" % (
            ", ".join(str(sum(1 for c in s.get("configs", []) if "dpdkCore" in c["stages"])) for s in a["samples"])))
    else:
        L.append("- These files contain no X-engine config.  The recv node records the config of a sender "
                 "only if the sender starts after the recv node.%s" % (
                     "  The settings below come from `%s`, which used the same recv build. [inferred]" %
                     a["cx_groups_inferred_from"] if inferred else ""))
    gains = sorted({str(s.get("digital_gains_update_id")) for s in a["samples"] if s.get("digital_gains_update_id")})
    if gains:
        L.append("- F-engine digital gains: %s [file]" % ", ".join("`%s`" % g.strip("[]'\"") for g in gains))
    L.append("")
    if groups:
        L.append("| X-engine build | nodes | fringestop | RFI stages | 1st-stage SK excision | SK sigma | mu min/max |")
        L.append("|---|---|---|---|---|---|---|")
        for g in groups:
            nodes = ", ".join(g["nodes"]) if g["nodes"] else "%d node config(s)%s" % (
                g["n_distinct_configs"], " (chord build: cx47/cx52)" if "chord." in (g["kotekan_version"] or "")
                and any(gg["nodes"] for gg in groups) else "")
            L.append("| `%s` | %s | %s | %s | %s | %s | %s / %s |" % (
                g["kotekan_version"], nodes, _yn(g["do_fringestop"], default_off=True),
                "yes" if g["rfi_stages"] else "no", _yn(g["rfi_first_stage_excision_enabled"]),
                _na(g["rfi_sk_rfimask_sigmas"]), _na(g["rfi_mu_min"]), _na(g["rfi_mu_max"])))
        L.append("")

    # ---- layout + labels
    L.append("## Element axis and labels\n")
    L.append("- %s elements, `n2_layout=%s`, `input_list` %s, `index_map/label` %s, `index_map/pol` %s. [file]" % (
        at.get("num_elements"), at.get("n2_layout"), "present" if at.get("input_list") else "absent",
        "absent" if a["samples"][0].get("im_label") is None else "%d entries" % len(a["samples"][0]["im_label"]),
        "present" if a["samples"][0].get("im_pol") is not None else "absent"))
    for e in a["timeline"]:
        if e["topic"] == "labels":
            L.append("- %s [history: %s]" % (e["text"], e["source"]))
    L.append("")

    # ---- per-element table
    L.append("### Per input\n")
    rev = REVERSED_FROM <= a["start_dir"] < REVERSED_TO
    L.append("Physical: the %s.  Sky response: the fractional increase of the autocorrelation at the %s "
             "transit (440-460 or 600-620 MHz, the larger value).  E-W offset: the east-west position of "
             "the beam centre of this input.  It is the hour angle of the autocorrelation peak during the "
             "transit, minus the array median, times cos(Dec).  A Gaussian fit to the upper half of the peak "
             "gives the hour angle; the value is the median of the two bands.  Positive is west.  The offset "
             "is relative only, because diffuse Cygnus emission moves the peak of the full array by a few "
             "degrees of hour angle.  It agrees with the Cyg A beam fits of the mapmaker on 2026-10-08 (B03 "
             "-0.30 deg, A02 +0.2 deg).  Flagged: the bad-feed mask that kotekan applied, as this file "
             "records it.\n" % (
                 "file label, mirrored in its group of eight (reversed table)" if rev else "file label",
                 src_name(a["sky_source"]) if a["sky_source"] else "brightest"))
    L.append("| element | file label | physical | sky response | E-W offset | flagged | auto (rel.) | status |")
    L.append("|---|---|---|---|---|---|---|---|")
    has = [x["ha"] for x, el in zip(st, a["axis"]) if x["ha"] is not None and el["dish"] and not el["dish"].startswith("RFI")]
    ha_ref = median(has)
    autos = [x["auto"] for x, el in zip(st, a["axis"]) if x["auto"] and el["dish"] and not el["dish"].startswith("RFI")]
    auto_ref = median(autos)
    cosd = __import__("math").cos(__import__("math").radians(a["sky_dec"] or 0))
    for i, (el, x) in enumerate(zip(a["axis"], st)):
        if el["dish"] is None:
            continue
        phys = (el["phys"] + el["pol"]) if el["pol"] else el["phys"]
        ew = "—" if x["ha"] is None or ha_ref is None else "%+.2f°" % ((x["ha"] - ha_ref) * cosd)
        status = _status(el, x, a)
        L.append("| %d | `%s` | %s | %s | %s | %s | %s | %s |" % (
            i, el["file_label"], phys, pct(x["rise"]), ew,
            "—" if x["flagged"] is None else ("**yes**" if x["flagged"] else "no"),
            "—" if not x["auto"] or not auto_ref else "%.1f" % (x["auto"] / auto_ref), status))
    L.append("")

    # ---- polarization
    L.append("## Polarization\n")
    for e in a["timeline"]:
        if e["topic"] == "polarization":
            L.append("- %s [history: %s]" % (e["text"], e["source"]))
    if a["pol"] and not a["pol"]["conclusive"]:
        L.append("- Cross-pol check at the %s transit %s: no result.  The on/off fringe contrast is only "
                 "%.1f, thus the source is not dominant at 610 MHz, and co-pol and cross-pol look the same.  "
                 "Use the history above. [sky]" % (
                     src_name(a["pol"]["source"]), a["pol"]["transit"].strftime("%Y-%m-%d %H:%M UTC"), a["pol"]["contrast"] or 0))
    elif a["pol"]:
        sw = sorted(d for d, v in a["pol"]["dishes"].items() if v["swapped"])
        L.append("- Cross-pol check at the %s transit %s (610 MHz): %s. [sky]" % (
            src_name(a["pol"]["source"]), a["pol"]["transit"].strftime("%Y-%m-%d %H:%M UTC"),
            ("X/Y are swapped on **" + ", ".join(sw) + "**") if sw else "no dish has swapped feeds"))
        L.append("")
        L.append("| dish | co-pol amp | cross-pol amp | cross/co | |")
        L.append("|---|---|---|---|---|")
        for d in sorted(a["pol"]["dishes"], key=_dish_key):
            v = a["pol"]["dishes"][d]
            L.append("| %s | %.3g | %.3g | %.2f | %s |" % (
                d, v["co"], v["cross"], v["ratio"] if v["ratio"] is not None else float("nan"),
                "**swapped**" if v["swapped"] else ("no signal" if not v["live"] else "")))
    else:
        L.append("- This acquisition has no bright transit that is usable for a cross-pol check.")
    L.append("")

    # ---- pointing
    L.append("## Pointing\n")
    coel = at.get("dish_coelev_deg")
    if coel is not None:
        L.append("- Config: `dish_coelev_deg = %s`, thus Dec %+.1f for all dishes (`coelev_disp_deg` is 0 for all). [file]" % (
            coel, LAT + coel))
    L.append("- Co-elevation is the angle between zenith and the pointing, along the meridian: "
             "**co-elevation = Dec - latitude** (latitude +49.32 deg).  A negative value is south of zenith.  "
             "The transit altitude is 90 deg - |co-elevation|.  Dec +22.0 (Tau A) is co-elevation -27.3 "
             "(altitude 62.7).  Dec +40.7 (Cyg A) is -8.6 (altitude 81.4, almost at zenith).  Dec +20 is "
             "about -29.  `coelev_disp_deg` is the offset of one dish from the array value.")
    p = pointing_summary(a)
    if p:
        L.append("- Measured: %s.  %s. [sky]" % (p["short"][0].upper() + p["short"][1:], p["why"][0].upper() + p["why"][1:]))
        if p["dec"].startswith("+41") and coel is not None:
            L.append("- **The config does not agree with the real pointing.**  All software that uses "
                     "`dish_coelev_deg` (kotekan fringestopping, beam models) assumes Dec %+.1f." % (LAT + coel))
    else:
        L.append("- Measured: not known from this acquisition.  No transit gives a clear result. [sky]")
    for e in a["timeline"]:
        if e["topic"] == "pointing" and not e["text"].startswith("Every file records"):
            L.append("- %s [history: %s]" % (e["text"], e["source"]))
    if a["sources"]:
        L.append("")
        L.append("Median increase of the autocorrelations of the live dishes at each transit.  Only inputs "
                 "above 3 sigma are included; '—' is no signal:\n")
        L.append("| source | Dec | transit (UTC) | transit (local) | 440-460 MHz | 600-620 MHz | 1400-1420 MHz |")
        L.append("|---|---|---|---|---|---|---|")
        for s in a["sources"]:
            cells = [pct(b["median_rise"]) + (" (%d/%d)" % (b["n_detected"], b["n"]) if b["median_rise"] else "")
                     for b in s["bands"]]
            while len(cells) < 3:
                cells.append("—")
            lt = local(s["transit"])
            L.append("| %s | %+.1f | %s | %s | %s |" % (
                src_name(s["source"]), s["dec"], s["transit"].strftime("%m-%d %H:%M"), lt.strftime("%m-%d %H:%M %Z"),
                " | ".join(cells)))
        L.append("")
        L.append("The per-input table gives the E-W beam centre of each dish.  With only one source in the "
                 "beam, the autocorrelations cannot separate a north-south offset from the system temperature "
                 "of the input.  Thus a dish with a much lower sky response has an incorrect pointing or low "
                 "sensitivity.")
    L.append("")

    # ---- RFI
    L.append("## RFI flagging\n")
    if groups:
        for g in groups:
            nodes = ", ".join(g["nodes"]) if g["nodes"] else "%s build nodes" % g["kotekan_version"]
            if not g["rfi_stages"]:
                L.append("- %s: the X-engine pipeline has no RFI stages, thus no excision.%s" % (
                    nodes, " [inferred]" if inferred else " [file]"))
            else:
                L.append("- %s: first-stage SK excision %s (sigma %s, mu %s-%s).  The second-stage frame "
                         "mask stage is in the pipeline.%s" % (nodes, _yn(g["rfi_first_stage_excision_enabled"]).lower(),
                                               _na(g["rfi_sk_rfimask_sigmas"]), _na(g["rfi_mu_min"]),
                                               _na(g["rfi_mu_max"]), " [inferred]" if inferred else " [file]"))
    en = sorted({str(x) for s in a["samples"] for x in (s.get("rfi_frame_excision_enabled") or [])})
    if en:
        L.append("- Second-stage frame excision, as the files record it: %s. [file]" % ", ".join(en))
    fr = [s.get("frac_rfi_median") for s in a["samples"] if s.get("frac_rfi_median") is not None]
    frm = [s.get("frac_rfi_mean") for s in a["samples"] if s.get("frac_rfi_mean") is not None]
    if fr:
        applied = any(x > 0 for x in fr)
        L.append("- Measured excised fraction `frac_rfi` (median / mean over frequency and time) in the "
                 "sampled files: %s.  Thus kotekan %s. [file]" % (
                     ", ".join("%.2f %% / %.1f %%" % (100 * m, 100 * n) for m, n in zip(fr, frm)),
                     "**removed RFI**" if applied else "removed no RFI"))
    for e in a["timeline"]:
        if e["topic"] == "rfi":
            L.append("- %s [history: %s]" % (e["text"], e["source"]))
    L.append("")

    # ---- fringestop
    L.append("## Fringestopping\n")
    if groups:
        for g in groups:
            nodes = ", ".join(g["nodes"]) if g["nodes"] else "%s build nodes" % g["kotekan_version"]
            L.append("- %s: `n2_accumulate/do_fringestop` = %s.%s" % (
                nodes, g["do_fringestop"] if g["do_fringestop"] != "None" else "not set (the default is false)",
                " [inferred]" if inferred else " [file]"))
    for e in a["timeline"]:
        if e["topic"] == "fringestop":
            L.append("- %s [history: %s]" % (e["text"], e["source"]))
    L.append("")

    # ---- bad feeds
    L.append("## Bad-feed flagging\n")
    for e in a["timeline"]:
        if e["topic"] == "badfeed":
            L.append("- %s [history: %s]" % (e["text"], e["source"]))
    rec = {x["flag_record"] for x in st if x["flag_record"]}
    fl = [_phys(el) for el, x in zip(a["axis"], st) if x["flagged"] and el["dish"] and not el["dish"].startswith("RFI")]
    flr = [_phys(el) for el, x in zip(a["axis"], st) if x["flagged"] and el["dish"] and el["dish"].startswith("RFI")]
    if rec:
        L.append("- Applied mask (from %s): flagged array inputs: %s%s. [file]" % (
            " and ".join(sorted(rec)), ", ".join(fl) or "none",
            "; flagged RFI antennas: %d of %d" % (len(flr), sum(1 for el in a["axis"] if el["dish"] and el["dish"].startswith("RFI")))
            if flr else ""))
        fc = flag_check(a, st)
        dead_unflagged, live_flagged = fc["dead_unflagged"], fc["live_flagged"]
        changed = [_phys(el) for el, x in zip(a["axis"], st) if x["flag_changed"] and el["dish"]]
        if changed:
            L.append("- These inputs changed their flag state during the acquisition (between sampled "
                     "files): %s.  The table shows the most frequent state. [file]" % ", ".join(changed))
        if a["sky_source"]:
            if not dead_unflagged and not live_flagged:
                L.append("- Sky test (%s transit): all inputs with no sky signal have a flag, and no healthy "
                         "input has a flag.  **The flags are correct.** [sky]" % src_name(a["sky_source"]))
            else:
                if dead_unflagged:
                    L.append("- Sky test (%s transit): **no sky signal, but NO flag:** %s. [sky]" % (
                        src_name(a["sky_source"]), ", ".join(dead_unflagged)))
                if live_flagged:
                    L.append("- Sky test (%s transit): **flagged, but the input has sky signal:** %s. [sky]" % (
                        src_name(a["sky_source"]), ", ".join(live_flagged)))
    else:
        dead = [_phys(el) for el, x in zip(a["axis"], st)
                if x["alive"] is False and el["dish"] and not el["dish"].startswith("RFI")]
        L.append("- These files show no applied mask (no `flags` record, no zeroed baselines). [file]")
        if dead:
            L.append("- Inputs with no sky signal (%s transit).  Do not use them: %s. [sky]" % (
                src_name(a["sky_source"]), ", ".join(dead)))
    hot = [_phys(el) for el, x in zip(a["axis"], st) if x["hot"] and el["dish"]]
    if hot:
        L.append("- Hot inputs (autocorrelation more than 4 times the array median): %s. [file]" % ", ".join(hot))
    L.append("")

    _timeline_section(L, a)
    return "\n".join(L) + "\n"


def _timeline_section(L, a):
    other = [e for e in a["timeline"] if e["topic"] in ("data", "software", "calibration")]
    if other:
        L.append("## Other notes\n")
        for e in other:
            L.append("- %s [history: %s]" % (e["text"], e["source"]))
        L.append("")


def _phys(el):
    return (el["phys"] + el["pol"]) if el["pol"] else el["phys"]


def _status(el, x, a):
    if el["dish"].startswith("RFI"):
        return "RFI antenna"
    if x["hot"]:
        return "**hot**"
    if x["alive"] is False:
        return "**no sky signal**"
    if x["alive"]:
        rises = [y["rise"] for y, e in zip(element_status_cache[a["name"]], a["axis"])
                 if y["alive"] and e["dish"] and not e["dish"].startswith("RFI")]
        ref = median(rises)
        if ref and x["rise"] < 0.5 * ref:
            return "low (%.0f %% of median)" % (100 * x["rise"] / ref)
        return "ok"
    return "—"


def _yn(v, default_off=False):
    if v in ("True", True):
        return "on"
    if v in ("False", False):
        return "off"
    return "off (key absent)" if default_off else "not set"


def _na(v):
    return "—" if v in (None, "None") else v


def _dish_key(d):
    m = LABEL_RE.match(d)
    return (bool(m.group(1)), m.group(2), int(m.group(3)))


element_status_cache = {}


def info_yaml(a, st):
    doc = {
        "acquisition": a["name"],
        "generated_by": "choco tools/acqnotes/render.py",
        "summary": verdict(a, st),
        "n_files": a["n_files"],
        "start_utc": a["first_file"].isoformat() if a["first_file"] else None,
        "end_utc": a["last_file_end"].isoformat() if a["last_file_end"] else None,
        "start_local": local(a["first_file"]).isoformat() if a["first_file"] else None,
        "end_local": local(a["last_file_end"]).isoformat() if a["last_file_end"] else None,
        "recv_kotekan": a.get("recv_versions"),
        "xengine": a["cx_groups"] or None,
        "xengine_inferred_from": a.get("cx_groups_inferred_from"),
        "config_dish_coelev_deg": a["attrs"].get("dish_coelev_deg") if a["attrs"] else None,
        "pointing_measured": (pointing_summary(a) or {}).get("dec") if a["samples"] else None,
        "labels_reversed_table": REVERSED_FROM <= a["start_dir"] < REVERSED_TO,
        "elements": [
            {"index": i, "file_label": el["file_label"], "physical": _phys(el) if el["dish"] else None,
             "flagged": x["flagged"], "sky_rise": None if x["rise"] is None else round(x["rise"], 3),
             "alive": x["alive"], "hot": bool(x["hot"])}
            for i, (el, x) in enumerate(zip(a["axis"], st))] if a["axis"] else [],
        "pol_swapped": swapped_dishes(a) if a["samples"] else None,
        "timeline": [{"topic": e["topic"], "text": e["text"], "source": e["source"]} for e in a["timeline"]],
    }
    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", required=True)
    ap.add_argument("--root", default="/mnt/cs00/data/kotekan_vis_files/subset")
    ap.add_argument("--dry-run", action="store_true", help="write into --work/out instead of --root")
    args = ap.parse_args()
    entries = yaml.safe_load(open(os.path.join(HERE, "timeline.yaml")))["entries"]
    zp = os.path.join(args.work, "zerovis.json")
    zerovis = json.load(open(zp)) if os.path.exists(zp) else {}
    names = sorted(os.path.basename(p) for p in glob.glob(os.path.join(args.root, "acq_*")))
    # acquisitions not yet measured (e.g. still being written) are skipped
    pending = [n for n in names if not os.path.exists(os.path.join(args.work, "meta", n + ".json"))]
    acqs = [analyse(n, args.root, args.work, entries, zerovis) for n in names if n not in pending]
    infer_missing_configs(acqs)
    gen_time = datetime.datetime.now(UTC)
    out_root = os.path.join(args.work, "out") if args.dry_run else args.root
    for a in acqs:
        st = element_status(a) if a["axis"] else []
        element_status_cache[a["name"]] = st
    for a in acqs:
        st = element_status_cache[a["name"]]
        d = os.path.join(out_root, a["name"])
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "README.md"), "w") as fh:
            fh.write(render_readme(a, st, gen_time))
        with open(os.path.join(d, "acq_info.yaml"), "w") as fh:
            fh.write(info_yaml(a, st))
    # choco's /files page shows this copy behind the root's Timeline button
    shutil.copyfile(os.path.join(HERE, "timeline.yaml"), os.path.join(out_root, "timeline.yaml"))
    print("wrote %d acquisitions and timeline.yaml to %s" % (len(acqs), out_root))


if __name__ == "__main__":
    main()
