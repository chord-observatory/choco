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
        data = payloads[url]
        return {e: (sk, vf) for e, (sk, vf) in enumerate(zip(data["sk"], data["valid_frac"]))}
    return read


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
    good = rfi.mask({"kind": "rfi", "urls": ["u0", "u1"]}, labels, "n2.h5")
    # element 1: SK 3.5 on u0 -> bad; element 2: SK 0.2 on u1 -> bad;
    # element 3: out of bounds on u0 but valid_frac 0.05 < min -> left good;
    # element 4: beyond the labelled feeds -> ignored.
    np.testing.assert_array_equal(good, [True, False, False, True])


def test_mask_bounds_configurable(monkeypatch):
    monkeypatch.setattr(rfi, "read_sk", _serve({"u0": _SK_0}))
    labels = np.array(["A1X", "A2X"])
    src = {"kind": "rfi", "url": "u0", "sk_lo": 0.0, "sk_hi": 10.0}
    np.testing.assert_array_equal(rfi.mask(src, labels, "n2.h5"), [True, True])


def test_mask_polls_every_url(monkeypatch):
    seen = []

    def fake_read(url):
        seen.append(url)
        return {}

    monkeypatch.setattr(rfi, "read_sk", fake_read)
    labels = np.array(["A1X"])
    good = rfi.mask({"kind": "rfi", "urls": ["u0", "u1"]}, labels, "n2.h5")
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


def test_unreachable_endpoint_skipped(monkeypatch):
    payloads = {"u1": _SK_0}

    def read(url):
        if url not in payloads:
            raise OSError("connection refused")
        data = payloads[url]
        return {e: (sk, vf) for e, (sk, vf) in enumerate(zip(data["sk"], data["valid_frac"]))}

    monkeypatch.setattr(rfi, "read_sk", read)
    labels = np.array(["A1X", "A2X"])
    good = rfi.mask({"kind": "rfi", "urls": ["u0", "u1"]}, labels, "n2.h5")
    # u0 down -> skipped; u1's readings still flag element 1.
    np.testing.assert_array_equal(good, [True, False])


def test_all_endpoints_unreachable_raises(monkeypatch):
    def read(url):
        raise OSError("connection refused")

    monkeypatch.setattr(rfi, "read_sk", read)
    labels = np.array(["A1X"])
    try:
        rfi.mask({"kind": "rfi", "urls": ["u0", "u1"]}, labels, "n2.h5")
    except OSError:
        return
    raise AssertionError("expected OSError when every endpoint fails")


def test_no_started_nodes_raises(monkeypatch):
    """A whole group of stopped nodes means nothing measurable: fail the
    run (red badge), consistent with every endpoint being unreachable."""
    stopped = [dict(n, started=False) for n in _NODES]
    monkeypatch.setattr(rfi, "choco_group_nodes", lambda url, group: stopped)
    src = {"kind": "rfi", "choco_url": "https://localhost:5000",
           "choco_group": "cx"}
    try:
        rfi.resolve_urls(src)
    except OSError as e:
        assert "no started nodes" in str(e)
        return
    raise AssertionError("expected OSError with no started nodes")


def test_empty_explicit_urls_raises():
    try:
        rfi.resolve_urls({"kind": "rfi", "urls": []})
    except ValueError:
        return
    raise AssertionError("expected ValueError for empty 'urls'")
