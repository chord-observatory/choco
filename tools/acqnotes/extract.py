"""Summarize each subset acquisition's metadata into one JSON per acq."""
import glob
import json
import os
import re
import sys

import h5py
import hdf5plugin  # noqa: F401
import numpy as np

ROOT = "/mnt/cs00/data/kotekan_vis_files/subset"
OUT = sys.argv[1]
ONLY = sys.argv[2:]

KEYRE = re.compile(r"fringe|^rfi_|excision|sk_|coelev|dish_inputs|bad_feed|bf_mask|inst$|"
                   r"fringestop|pointing|num_elements|input_list|subset|ha_|dec_deg|feed_mask|"
                   r"updatable|threshold|sigma|n2_layout|num_dishes", re.I)


def flat(o, p=""):
    r = {}
    if isinstance(o, dict):
        for k, v in o.items():
            r.update(flat(v, p + "/" + k))
    else:
        r[p] = o
    return r


def jsonable(v):
    if isinstance(v, np.ndarray):
        return v.tolist()
    if isinstance(v, (np.generic,)):
        return v.item()
    if isinstance(v, bytes):
        return v.decode(errors="replace")
    return v


def summarize_config(raw):
    j = json.loads(raw)
    cfg = j.get("config", {})
    fl = flat(cfg)
    stages = sorted({v for k, v in fl.items() if k.endswith("/kotekan_stage")})
    keys = {k: v for k, v in fl.items()
            if KEYRE.search(k.rsplit("/", 1)[-1]) and not isinstance(v, dict)}
    # drop bulky buffer plumbing
    keys = {k: v for k, v in keys.items()
            if not re.search(r"_buf$|buffer|ringbuffer|/commands|frame_size", k)}
    inst = sorted({str(v) for k, v in fl.items() if k.endswith("/inst")})
    return {
        "json_hash": j.get("json_hash"),
        "kotekan_version": j.get("kotekan_version"),
        "kotekan_git_commit_hash": j.get("kotekan_git_commit_hash"),
        "kotekan_build_branch": j.get("kotekan_build_branch"),
        "stages": stages,
        "inst": inst,
        "keys": {k: (v if len(json.dumps(v)) < 4000 else json.dumps(v)[:4000]) for k, v in keys.items()},
    }


def summarize_file(path):
    s = {"file": os.path.basename(path), "size": os.path.getsize(path)}
    with h5py.File(path, "r") as f:
        s["attrs"] = {k: jsonable(v) for k, v in f.attrs.items()
                      if not k.startswith("EOP") and k not in ("feed_positions_m",)}
        if "feed_positions_m" in f.attrs:
            s["attrs"]["feed_positions_m"] = np.asarray(f.attrs["feed_positions_m"]).round(3).tolist()
        dsets = []
        f.visititems(lambda n, o: dsets.append([n, list(o.shape), str(o.dtype)])
                     if isinstance(o, h5py.Dataset) else None)
        s["datasets"] = dsets
        im = f.get("index_map")
        if im is not None:
            for k in ("label", "pol", "type", "coelev_disp_deg", "dish_idx", "grid_x_idx", "grid_y_idx"):
                if k in im:
                    s["im_" + k] = jsonable(im[k][()])
            if "feed_pos_disp_m" in im:
                s["im_feed_pos_disp_m"] = np.asarray(im["feed_pos_disp_m"][()]).round(3).tolist()
            fr = im["freq"]["centre"]
            s["freq_n"] = int(fr.size)
            s["freq_min"] = float(fr.min())
            s["freq_max"] = float(fr.max())
        for tk in ("time_center_t_inst_ns", "time_center_ut1_ns"):
            if tk in f:
                t = f[tk][()]
                s[tk] = [int(t[0]), int(t[-1]), int(t.size)]
        for tk in ("rfi_frame_excision_enabled",):
            if tk in f:
                s[tk] = sorted(set(jsonable(f[tk][()])))
        for tk in ("rfi_frame_excision_threshold", "rfi_frame_excision_fraction"):
            if tk in f:
                a = f[tk][()]
                s[tk] = {"unique": sorted(set(np.round(a.ravel(), 4).tolist()))[:20],
                         "per_stream_first": np.round(a[0], 4).tolist()}
        if "frac_rfi" in f:
            a = f["frac_rfi"][()]
            s["frac_rfi_median"] = float(np.nanmedian(a))
            s["frac_rfi_mean"] = float(np.nanmean(a))
        if "frac_lost" in f:
            a = f["frac_lost"][()]
            s["frac_lost_median"] = float(np.nanmedian(a))
            s["freq_live_frac"] = float(np.mean(np.nanmean(a, axis=1) < 0.99))
        if "frames_added" in f:
            a = f["frames_added"][()]
            s["frames_added_nonzero_freq_frac"] = float(np.mean(a.max(axis=1) > 0))
        if "flags" in f:
            a = f["flags"][()]
            # per element: fraction of (freq,time) where flag weight is zero
            s["flags_zero_frac_per_el"] = np.round(np.mean(a == 0, axis=(0, 2)), 3).tolist()
        if "bad_feed_mask/mask" in f:
            m = f["bad_feed_mask/mask"][()]  # time, stream, pol, dish
            s["bad_feed_mask_shape"] = list(m.shape)
            s["bad_feed_mask_bad_frac_pol_dish"] = np.round(np.mean(m != 0, axis=(0, 1)), 3).tolist()
            s["bad_feed_mask_unique"] = sorted(set(np.unique(m).tolist()))
        if "flag_updates/bf_mask" in f:
            m = f["flag_updates/bf_mask"][()]
            s["flag_updates_n"] = int(m.shape[0])
            s["flag_updates_masks"] = sorted({tuple(r) for r in m.tolist()})[:5]
            s["flag_updates_masks"] = [list(r) for r in s["flag_updates_masks"]]
        if "gain" in f:
            g = f["gain"][:, :, 0]
            s["gain_is_unity"] = bool(np.allclose(g, 1))
            s["gain_abs_median_per_el"] = np.round(np.nanmedian(np.abs(g), axis=0), 3).tolist()
        if "digital_gains/update_id" in f:
            s["digital_gains_update_id"] = jsonable(f["digital_gains/update_id"][()])
        if "vis" in f and "index_map/prod" in f:
            prod = f["index_map/prod"][()]
            auto = np.where(prod["input_a"] == prod["input_b"])[0]
            nf = f["vis"].shape[0]
            sl = slice(nf // 8, 7 * nf // 8)
            v = np.abs(f["vis"][sl, auto[0]:auto[-1] + 1, 0])[:, auto - auto[0]]
            s["auto_median_per_el"] = [float(x) for x in np.nanmedian(v, axis=0)]
        if "config_json" in f:
            cj = f["config_json"][()]
            s["configs"] = [summarize_config(c) for c in cj]
    return s


def main():
    os.makedirs(OUT, exist_ok=True)
    for d in sorted(glob.glob(ROOT + "/acq_*")):
        name = os.path.basename(d)
        if ONLY and name not in ONLY:
            continue
        files = sorted(glob.glob(d + "/*.h5"))
        r = {"acq": name, "n_files": len(files),
             "files_first_last": [os.path.basename(files[0]), os.path.basename(files[-1])] if files else [],
             "partial": sorted(os.listdir(d + "/.partial")) if os.path.isdir(d + "/.partial") else None,
             "total_bytes": sum(os.path.getsize(p) for p in files),
             "file_times": [os.path.basename(p)[15:40] for p in files],
             "samples": []}
        if files:
            idx = sorted({0, len(files) // 4, len(files) // 2, 3 * len(files) // 4, len(files) - 1})
            for i in idx:
                try:
                    r["samples"].append(summarize_file(files[i]))
                except Exception as e:  # noqa
                    r["samples"].append({"file": os.path.basename(files[i]), "error": repr(e)})
        with open(os.path.join(OUT, name + ".json"), "w") as fh:
            json.dump(r, fh, default=jsonable)
        print(name, len(files), flush=True)


main()
