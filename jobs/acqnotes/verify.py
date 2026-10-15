"""Read every file of one acquisition and record the facts its README states.

Independent of extract.py (which samples five files): this walks all files,
reading only attributes, config_json, one frequency row of flags, and small
per-file datasets.
"""
import glob
import json
import os
import re

import h5py
import hdf5plugin  # noqa: F401
import numpy as np

FNAME = re.compile(r"_(\d{8})T_(\d{6})_")


def cfg_facts(raw):
    j = json.loads(raw)
    c = j.get("config", {})
    stages = set()

    def walk(o):
        if isinstance(o, dict):
            if "kotekan_stage" in o:
                stages.add(o["kotekan_stage"])
            for v in o.values():
                walk(v)
    walk(c)
    if "dpdkCore" not in stages:
        role = "recv" if "hdf5N2Write" in stages else "other"
    else:
        role = "cx"
    inst = set()

    def walk2(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k == "inst" and isinstance(v, str):
                    inst.add(v.split(".")[0])
                walk2(v)
    walk2(c)
    na = c.get("n2_accumulate", {}) if isinstance(c.get("n2_accumulate"), dict) else {}
    return {
        "role": role,
        "version": j.get("kotekan_version", ""),
        "inst": ",".join(sorted(inst)),
        "coelev": (c.get("telescope") or {}).get("dish_coelev_deg"),
        "fringestop": na.get("do_fringestop"),
        "sigma": c.get("rfi_sk_rfimask_sigmas"),
        "first_stage": c.get("rfi_first_stage_excision_enabled"),
        "mu": (c.get("rfi_mu_min"), c.get("rfi_mu_max")),
        "rfi_stage": "RfiFrameMask" in stages,
    }


def analyse(d, previous=None):
    """Facts of every file in acquisition directory *d*.  Files already in
    *previous* (an earlier result for the same directory) are not read again."""
    acq = os.path.basename(d)
    files = sorted(glob.glob(os.path.join(d, "*.h5")))
    seen = {r["name"]: r for r in (previous or {}).get("files", []) if "error" not in r}
    rec = {"acq": acq, "n_files": len(files), "bytes": 0, "files": []}
    for p in files:
        r = {"name": os.path.basename(p)}
        rec["bytes"] += os.path.getsize(p)
        if r["name"] in seen:
            rec["files"].append(seen[r["name"]])
            continue
        try:
            with h5py.File(p, "r") as f:
                a = f.attrs
                r["coelev"] = float(a["dish_coelev_deg"])
                r["ne"] = int(a["num_elements"])
                r["layout"] = str(a.get("n2_layout"))
                r["input_list"] = "input_list" in a
                lab = f["index_map/label"][()] if "index_map/label" in f else None
                r["labels"] = [x.decode() if isinstance(x, bytes) else str(x) for x in lab] if lab is not None else None
                r["pol"] = "index_map/pol" in f
                r["configs"] = [cfg_facts(c) for c in f["config_json"][()]] if "config_json" in f else []
                fr = f["index_map/freq"]["centre"]
                i0 = int(np.searchsorted(fr, 610.0))
                fl = f["flags"][i0, :, :]  # (element, time) at one frequency
                fa = f["frames_added"][i0, :] if "frames_added" in f else None
                live_t = (fa > 0) if fa is not None else np.ones(fl.shape[1], bool)
                r["flagged"] = [bool(x) for x in (fl[:, live_t] == 0).all(axis=1)] if live_t.any() else None
                # inputs whose cross products are all zero at this frequency (a mask
                # applied by zeroing, the only record before 2026-09)
                v = f["vis"][i0, :, :][:, live_t] if live_t.any() else None
                prod = f["index_map/prod"][()]
                if v is not None:
                    nz = (v != 0) & np.isfinite(v)
                    cross = prod["input_a"] != prod["input_b"]
                    # fraction of the input's cross-product cells that are zero; > 0.8 = masked
                    r["zero_frac"] = [round(float(1 - nz[((prod["input_a"] == e) | (prod["input_b"] == e))
                                                         & cross].mean()), 3) for e in range(r["ne"])]
                    r["zeroed"] = [z > 0.8 for z in r["zero_frac"]]
                r["rfi2"] = bool(f["rfi_frame_excision_enabled"][()].any()) if "rfi_frame_excision_enabled" in f else None
                fr_ = f["frac_rfi"][()]
                r["frac_rfi_med"] = float(np.nanmedian(fr_))
                fl_ = f["frac_lost"][()]
                r["live"] = float(np.mean(np.nanmean(fl_, axis=1) < 0.99))
                r["gains"] = str(f["digital_gains/update_id"][()][0]) if "digital_gains/update_id" in f else None
                r["bfm"] = "bad_feed_mask" in f
        except Exception as e:  # noqa
            r["error"] = repr(e)
        rec["files"].append(r)
    return rec
