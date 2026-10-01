"""Tests for the rfi source — run with `pytest`. No network/kotekan needed."""

import json

import numpy as np

from sources import rfi

# Synthetic RfiSKMetrics /sk endpoint payloads: two instances (one per GPU).
# Element 1 is hot on instance 0; element 2 is cold only on instance 1;
# element 3 is out of bounds but has too few valid cells; element 4 was never
# measured (sk null).
_SK_0 = {"num_elements": 5, "ema_frames": 256,
         "sk": [1.01, 3.5, 1.0, 9.0, None],
         "valid_frac": [0.9, 0.9, 0.9, 0.05, 0.0]}
_SK_1 = {"num_elements": 5, "ema_frames": 256,
         "sk": [1.0, 1.0, 0.2, 1.0, None],
         "valid_frac": [0.9, 0.9, 0.9, 0.9, 0.0]}


def _serve(payloads):
    """Monkeypatch-able read_sk replacement serving one payload per URL."""
    def read(url):
        if url not in payloads:
            raise OSError("connection refused")
        data = payloads[url]
        return {e: (sk, vf) for e, (sk, vf) in enumerate(zip(data["sk"], data["valid_frac"]))}
    return read


def _mask(src, labels):
    """rfi.mask without the freshness check (no /metrics in these tests)."""
    return rfi.mask({"max_stale_s": 0, **src}, labels, "n2.h5")


def test_read_sk_parses_endpoint_json(monkeypatch):
    import io
    import urllib.request

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda url, timeout=None: _Resp(json.dumps(_SK_0).encode()))
    readings = rfi.read_sk("http://cx27:12048/rfi_sk_metrics/sk_metrics_0/sk")
    assert readings[0] == (1.01, 0.9)
    assert readings[4] == (None, 0.0)  # never measured
    assert len(readings) == 5


def test_mask_flags_out_of_bounds_sk(monkeypatch):
    monkeypatch.setattr(rfi, "read_sk", _serve({"u0": _SK_0, "u1": _SK_1}))
    labels = np.array(["A1X", "A2X", "A3X", "A4X"])
    good, rep = _mask({"kind": "rfi", "urls": ["u0", "u1"]}, labels)
    # element 1: SK 3.5 on u0 -> bad; element 2: SK 0.2 on u1 -> bad;
    # element 3: out of bounds on u0 but valid_frac 0.05 < min -> left good;
    # element 4: beyond the labelled feeds -> ignored.
    np.testing.assert_array_equal(good, [True, False, False, True])
    assert rep["status"] == "ok" and rep["reason"] is None
    assert rep["n_measured"] == 4            # elements 0..3 judged on some endpoint
    assert [e["ok"] for e in rep["detail"]["endpoints"]] == [True, True]
    assert rep["detail"]["endpoints"][0]["n_flagged"] == 1


def test_mask_bounds_configurable(monkeypatch):
    monkeypatch.setattr(rfi, "read_sk", _serve({"u0": _SK_0}))
    labels = np.array(["A1X", "A2X"])
    src = {"kind": "rfi", "url": "u0", "sk_lo": 0.0, "sk_hi": 10.0}
    good, _ = _mask(src, labels)
    np.testing.assert_array_equal(good, [True, True])


def test_mask_polls_every_url(monkeypatch):
    seen = []

    def fake_read(url):
        seen.append(url)
        return {}

    monkeypatch.setattr(rfi, "read_sk", fake_read)
    labels = np.array(["A1X"])
    good, _ = _mask({"kind": "rfi", "urls": ["u0", "u1"]}, labels)
    assert seen == ["u0", "u1"]
    np.testing.assert_array_equal(good, [True])


# -- choco-derived endpoints ------------------------------------------------

_NODES = [
    {"name": "cx1", "host": "cx1.example", "port": 12048, "started": True},
    {"name": "cx2", "host": "cx2.example", "port": 12048, "started": False},
    {"name": "cx3", "host": "cx3.example", "port": 12000, "started": True},
]


def test_urls_derived_from_choco_group(monkeypatch):
    asked = {}

    def fake_nodes(url, group):
        asked.update(url=url, group=group)
        return _NODES

    monkeypatch.setattr(rfi, "choco_group_nodes", fake_nodes)
    src = {"kind": "rfi", "choco_url": "https://localhost:5000",
           "choco_group": "cx"}
    urls = rfi.resolve_urls(src)
    assert asked == {"url": "https://localhost:5000", "group": "cx"}
    # Started nodes only, each polled at every default sk path.
    assert urls == [
        "http://cx1.example:12048/rfi_sk_metrics/sk_metrics_0/sk",
        "http://cx1.example:12048/rfi_sk_metrics/sk_metrics_1/sk",
        "http://cx3.example:12000/rfi_sk_metrics/sk_metrics_0/sk",
        "http://cx3.example:12000/rfi_sk_metrics/sk_metrics_1/sk",
    ]


def test_explicit_group_and_paths_override(monkeypatch):
    asked = {}
    monkeypatch.setattr(rfi, "choco_group_nodes",
                        lambda url, group: asked.update(group=group) or _NODES[:1])
    src = {"kind": "rfi", "choco_url": "https://localhost:5000",
           "choco_group": "cx", "group": "recv", "sk_paths": ["custom/sk"]}
    urls = rfi.resolve_urls(src)
    assert asked["group"] == "recv"
    assert urls == ["http://cx1.example:12048/custom/sk"]


def test_explicit_urls_win(monkeypatch):
    monkeypatch.setattr(rfi, "choco_group_nodes",
                        lambda url, group: (_ for _ in ()).throw(AssertionError))
    src = {"kind": "rfi", "urls": ["u0"], "choco_url": "x", "choco_group": "g"}
    assert rfi.resolve_urls(src) == ["u0"]


def test_no_urls_and_no_choco_context_raises():
    try:
        rfi.resolve_urls({"kind": "rfi"})
    except ValueError:
        return
    raise AssertionError("expected ValueError without urls or choco context")


def test_unreachable_endpoint_skipped_and_reported(monkeypatch):
    """One X-engine node down: its band goes unmeasured, the others still
    flag, and the report says so (degraded) instead of a journal line
    nobody reads."""
    monkeypatch.setattr(rfi, "read_sk", _serve({"u1": _SK_0}))
    labels = np.array(["A1X", "A2X"])
    good, rep = _mask({"kind": "rfi", "urls": ["u0", "u1"]}, labels)
    # u0 down -> skipped; u1's readings still flag element 1.
    np.testing.assert_array_equal(good, [True, False])
    assert rep["status"] == "degraded"
    assert "1 of 2 /sk endpoints unreachable" in rep["reason"]
    assert rep["n_measured"] == 2
    down = rep["detail"]["endpoints"][0]
    assert down["url"] == "u0" and down["ok"] is False
    assert "connection refused" in down["error"]


def test_all_endpoints_unreachable_abstains(monkeypatch):
    """Nothing reachable: no feed is judged and the report says degraded
    with nothing measured — the core, not this source, decides whether
    the whole run had nothing to measure (the other sources may have)."""
    monkeypatch.setattr(rfi, "read_sk", _serve({}))
    labels = np.array(["A1X"])
    good, rep = _mask({"kind": "rfi", "urls": ["u0", "u1"]}, labels)
    np.testing.assert_array_equal(good, [True])
    assert rep["status"] == "degraded"
    assert rep["n_measured"] == 0
    assert "2 of 2 /sk endpoints unreachable" in rep["reason"]


def test_no_started_nodes_abstains(monkeypatch):
    """A whole group of stopped nodes means nothing measurable here:
    no endpoints, a degraded report with nothing measured."""
    stopped = [dict(n, started=False) for n in _NODES]
    monkeypatch.setattr(rfi, "choco_group_nodes", lambda url, group: stopped)
    src = {"kind": "rfi", "choco_url": "https://localhost:5000",
           "choco_group": "cx"}
    assert rfi.resolve_urls(src) == []
    good, rep = _mask(src, np.array(["A1X"]))
    np.testing.assert_array_equal(good, [True])
    assert rep["status"] == "degraded" and rep["n_measured"] == 0
    assert "no started nodes" in rep["reason"]


def test_nodes_choco_reports_down_or_idle_are_not_polled(monkeypatch):
    """choco's sync loop already knows a node is unreachable or has no
    kotekan running; polling it would only burn the timeout.  Such nodes
    are left out and named in the report."""
    nodes = [dict(_NODES[0], status="started"),
             dict(_NODES[2], status="down"),
             {"name": "cx4", "host": "cx4.example", "port": 12048,
              "started": True, "status": "idle"},
             {"name": "cx5", "host": "cx5.example", "port": 12048,
              "started": True, "status": "unknown"}]
    monkeypatch.setattr(rfi, "choco_group_nodes", lambda url, group: nodes)
    skipped = []
    src = {"kind": "rfi", "choco_url": "https://localhost:5000",
           "choco_group": "cx", "sk_paths": ["sk"]}
    urls = rfi.resolve_urls(src, skipped)
    # started + unknown are polled; down + idle are not
    assert urls == ["http://cx1.example:12048/sk", "http://cx5.example:12048/sk"]
    assert skipped == [{"node": "cx3", "status": "down"},
                       {"node": "cx4", "status": "idle"}]

    monkeypatch.setattr(rfi, "read_sk", _serve({
        "http://cx1.example:12048/sk": _SK_0, "http://cx5.example:12048/sk": _SK_1}))
    good, rep = _mask(src, np.array(["A1X", "A2X"]))
    np.testing.assert_array_equal(good, [True, False])
    assert rep["status"] == "degraded"
    assert "cx3 down" in rep["reason"] and "cx4 idle" in rep["reason"]
    assert rep["detail"]["skipped_nodes"] == skipped


# -- freshness: frozen EMAs from a stage that stopped seeing frames ----------

_METRICS = """# HELP kotekan_rfi_sk_per_feed_valid_frac x
# TYPE kotekan_rfi_sk_per_feed_valid_frac gauge
kotekan_rfi_sk_per_feed_valid_frac{{stage_name="/rfi_sk_metrics/sk_metrics_0",element="0"}} 0.94 {t0}
kotekan_rfi_sk_per_feed_valid_frac{{stage_name="/rfi_sk_metrics/sk_metrics_0",element="1"}} 0.93 {t0b}
kotekan_rfi_sk_per_feed_valid_frac{{stage_name="/rfi_sk_metrics/sk_metrics_1",element="0"}} 0.00 {t1}
kotekan_rfi_sk_per_feed{{stage_name="/rfi_sk_metrics/sk_metrics_0",element="0"}} 1.07 {t0}
kotekan_valve_passed_frames_total{{stage_name="/valve"}} 1156431
"""


def test_split_sk_url():
    assert rfi.split_sk_url("http://cx27:12048/rfi_sk_metrics/sk_metrics_0/sk") == (
        "http://cx27:12048", "/rfi_sk_metrics/sk_metrics_0")


def test_read_sk_freshness_takes_the_newest_gauge_stamp_per_stage(monkeypatch):
    import io
    import urllib.request

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    text = _METRICS.format(t0=1_790_000_000_000, t0b=1_790_000_005_000,
                           t1=1_789_999_000_000)
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda url, timeout=None: _Resp(text.encode()))
    fresh = rfi.read_sk_freshness("http://cx27:12048")
    assert fresh == {"/rfi_sk_metrics/sk_metrics_0": 1_790_000_005.0,
                     "/rfi_sk_metrics/sk_metrics_1": 1_789_999_000.0}


def _with_freshness(monkeypatch, now, ages):
    """Patch time + /metrics so stage i's gauges are ages[i] seconds old."""
    import time as _time
    monkeypatch.setattr(_time, "time", lambda: now)
    stamps = {f"/rfi_sk_metrics/sk_metrics_{i}": now - age for i, age in enumerate(ages)}
    monkeypatch.setattr(rfi, "read_sk_freshness", lambda base: stamps)


def test_stale_endpoint_readings_are_ignored(monkeypatch):
    urls = ["http://cx1:12048/rfi_sk_metrics/sk_metrics_0/sk",
            "http://cx1:12048/rfi_sk_metrics/sk_metrics_1/sk"]
    monkeypatch.setattr(rfi, "read_sk", _serve(dict(zip(urls, [_SK_0, _SK_1]))))
    _with_freshness(monkeypatch, now=1_790_000_000.0, ages=[5.0, 600.0])
    labels = np.array(["A1X", "A2X", "A3X"])
    good, rep = rfi.mask({"kind": "rfi", "urls": urls}, labels, "n2.h5")
    # instance 0 is live and flags element 1; instance 1 froze ten minutes
    # ago, so its SK 0.2 on element 2 is not acted on.
    np.testing.assert_array_equal(good, [True, False, True])
    assert rep["status"] == "degraded"
    assert "1 of 2 /sk endpoints stale" in rep["reason"]
    e0, e1 = rep["detail"]["endpoints"]
    assert e0["age_s"] == 5.0 and "stale" not in e0
    assert e1["stale"] is True and e1["age_s"] == 600.0


def test_freshness_unknown_means_readings_are_used(monkeypatch):
    """No /metrics (older kotekan, or unreachable): nothing to compare
    against, so the readings count and nothing is reported stale."""
    def boom(base):
        raise OSError("no metrics")
    monkeypatch.setattr(rfi, "read_sk_freshness", boom)
    monkeypatch.setattr(rfi, "read_sk", _serve({"http://cx1:12048/a/sk": _SK_0}))
    good, rep = rfi.mask({"kind": "rfi", "urls": ["http://cx1:12048/a/sk"]},
                         np.array(["A1X", "A2X"]), "n2.h5")
    np.testing.assert_array_equal(good, [True, False])
    assert rep["status"] == "ok"
    assert rep["detail"]["endpoints"][0].get("age_s") is None


def test_metrics_read_once_per_node(monkeypatch):
    calls = []

    def fresh(base):
        calls.append(base)
        return {}
    monkeypatch.setattr(rfi, "read_sk_freshness", fresh)
    urls = ["http://cx1:12048/a/sk", "http://cx1:12048/b/sk", "http://cx2:12048/a/sk"]
    monkeypatch.setattr(rfi, "read_sk", _serve({u: _SK_0 for u in urls}))
    rfi.mask({"kind": "rfi", "urls": urls}, np.array(["A1X"]), "n2.h5")
    assert calls == ["http://cx1:12048", "http://cx2:12048"]


def test_empty_explicit_urls_raises():
    try:
        rfi.resolve_urls({"kind": "rfi", "urls": []})
    except ValueError:
        return
    raise AssertionError("expected ValueError for empty 'urls'")
