"""Tests for the waterfall image tree's read side and its routes.

choco only reads this tree — ``jobs/waterfall`` writes it — so what is
tested here is the reading: that a name from the URL never reaches the
filesystem unchecked, that a missing or broken index costs one row rather
than the page, and that the /files column and the contact sheet render.
"""

import json
import os
import struct
import zlib
from pathlib import Path

import pytest
import yaml

from choco.app import create_app
from tests.helpers import login
from choco.waterfalls import (
    INDEX_CACHE_MAX, WaterfallStore, freq_ticks, open_stream, palette_gradient,
    parse_elements, read_npy_1d, read_png_head, read_times, summarize_dir,
    sweep, time_ticks, triangle, value_ticks,
)


def _chunk(typ, data):
    return (struct.pack(">I", len(data)) + typ + data
            + struct.pack(">I", zlib.crc32(typ + data)))


def png_with_palette(width=64, height=12):
    """The head of an image as wfpng writes it: SIG, IHDR, 256-entry PLTE."""
    pal = bytes(b for i in range(256) for b in (i, 0, 255 - i))
    return (b"\x89PNG\r\n\x1a\n"
            + _chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 3, 0, 0, 0))
            + _chunk(b"PLTE", pal)
            + _chunk(b"IEND", b""))


def npy_bytes(values):
    """A 1-D float64 .npy, byte-for-byte what np.save produces."""
    header = ("{'descr': '<f8', 'fortran_order': False, "
              "'shape': (%d,), }" % len(values))
    header += " " * ((64 - (10 + len(header) + 1) % 64) % 64) + "\n"
    return (b"\x93NUMPY\x01\x00" + struct.pack("<H", len(header))
            + header.encode() + struct.pack("<%dd" % len(values), *values))


def j2000_ns(iso: str) -> int:
    """A UTC instant as ns since J2000 — ``time_center_ut1_ns``'s epoch."""
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() - 946_728_000) * 10**9


#: One scanline every 9.6 s from an arbitrary night.
T0_NS = j2000_ns("2026-08-20T22:00:00")
STEP_NS = 9_600_000_000


def make_index(path, n_elements=3, files=(1, 2), rows=12, rendered_pairs=None,
               source_path="/data/subset", summary=True, side_files=True):
    """An acquisition's index and the images it claims, in miniature."""
    path.mkdir(parents=True, exist_ok=True)
    if side_files:
        (path / "freq.npy").write_bytes(
            npy_bytes([600.0 + 1.5625 * i for i in range(64)]))
        (path / "times.bin").write_bytes(
            struct.pack("<%dq" % rows, *(T0_NS + STEP_NS * r for r in range(rows))))
    pairs = rendered_pairs if rendered_pairs is not None else [
        (a, b) for a in range(n_elements) for b in range(a, n_elements)]
    products = []
    for a, b in pairs:
        name = f"e{a:04d}xe{b:04d}"
        products.append({"name": name, "a": a, "b": b, "rows": rows,
                         "lo": 1.0, "hi": 10.0, "nbytes": 100, "adler": 1,
                         "width": 64})
        shard = path / f"e{a:04d}"
        shard.mkdir(exist_ok=True)
        (shard / f"wf_{name}.png").write_bytes(png_with_palette(64, rows))
        (shard / f"th_{name}.png").write_bytes(b"\x89PNG\r\n\x1a\nth")
    (path / "index.json").write_text(json.dumps({
        "version": 1, "acquisition": path.name, "n_freq": 64,
        "n_prod": len(products), "n_elements": n_elements,
        "labels": [f"A{i}X" for i in range(n_elements)],
        "files": list(files), "skipped": {}, "products": products,
        "source_path": source_path, "updated": 1700000000.0,
    }))
    if summary:
        (path / "summary.json").write_text(json.dumps({
            "rendered": len(files), "products": len(products), "rows": rows,
            "elements": n_elements, "skipped": 0,
            "source_path": source_path, "updated": 1700000000.0,
        }))
    return path


@pytest.fixture
def images(tmp_path):
    root = tmp_path / "waterfalls"
    make_index(root / "subset" / "acq_20260723_232332_046022478")
    return root


@pytest.fixture
def store(images):
    return WaterfallStore(images, ttl_s=0)


ACQ = "acq_20260723_232332_046022478"


# --- reading the tree ----------------------------------------------------

class TestSummaries:
    def test_summarize_one(self, images):
        s = summarize_dir(str(images / "subset" / ACQ))
        assert s["rendered"] == 2 and s["products"] == 6
        assert s["rows"] == 12 and s["elements"] == 3
        assert not s["broken"]

    def test_missing_index_is_not_an_acquisition(self, tmp_path):
        (tmp_path / "empty").mkdir()
        assert summarize_dir(str(tmp_path / "empty")) is None

    def test_unreadable_index_is_flagged_not_raised(self, tmp_path):
        d = tmp_path / "bad"
        d.mkdir()
        (d / "index.json").write_text("{not json")
        s = summarize_dir(str(d))
        assert s["broken"] and s["rendered"] == 0

    def test_sweep_keys_by_root_and_acquisition(self, images):
        assert set(sweep(str(images))) == {("subset", ACQ)}

    def test_sweep_of_a_missing_tree_is_empty(self, tmp_path):
        assert sweep(str(tmp_path / "nope")) == {}

    def test_store_summary(self, store):
        assert store.summary("subset", ACQ)["rendered"] == 2
        assert store.summary("subset", "acq_nope") is None

    def test_ttl_caches_the_sweep(self, images):
        st = WaterfallStore(images, ttl_s=3600)
        assert set(st.summaries()) == {("subset", ACQ)}
        make_index(images / "subset" / "acq_later")
        assert set(st.summaries()) == {("subset", ACQ)}        # cached
        assert ("subset", "acq_later") in st.summaries(force=True)

    def test_unconfigured_store_is_inert(self):
        st = WaterfallStore(None)
        assert not st.configured
        assert st.summaries() == {}
        assert st.acquisition_dir("subset", ACQ) is None


class TestIndex:
    def test_index_is_read_and_reread_on_change(self, store, images):
        idx = store.index("subset", ACQ)
        assert idx["n_elements"] == 3 and len(idx["products"]) == 6

        d = images / "subset" / ACQ
        make_index(d, n_elements=4)
        os.utime(d / "index.json", (2_000_000_000, 2_000_000_000))
        assert store.index("subset", ACQ)["n_elements"] == 4

    def test_missing_index_is_none(self, store):
        assert store.index("subset", "acq_nope") is None

    def test_unreadable_index_is_none(self, store, images):
        (images / "subset" / ACQ / "index.json").write_text("{oops")
        assert store.index("subset", ACQ) is None


# --- never trust the URL -------------------------------------------------

class TestPathValidation:
    @pytest.mark.parametrize("root,acq", [
        ("..", ACQ),
        ("subset", ".."),
        ("subset", "../../etc"),
        ("sub/set", ACQ),
        ("subset", "acq/../.."),
        ("", ACQ),
        ("subset", ""),
        (".hidden", ACQ),
        ("subset", "acq\x00x"),
    ])
    def test_traversal_and_odd_names_are_refused(self, store, root, acq):
        assert store.acquisition_dir(root, acq) is None

    def test_a_real_pair_resolves(self, store, images):
        assert store.acquisition_dir("subset", ACQ) == images / "subset" / ACQ

    @pytest.mark.parametrize("shard,name", [
        ("e0000", "wf_e0000xe0001.png"),
        ("e0031", "th_e0031xe0031.png"),
    ])
    def test_image_names_the_writer_produces_are_accepted(self, store, shard, name):
        assert store.image_file("subset", ACQ, shard, name) is not None

    @pytest.mark.parametrize("shard,name", [
        ("e0000", "../index.json"),
        ("..", "wf_e0000xe0000.png"),
        ("e0000", "index.json"),
        ("e0000", "wf_e0000xe0000.png.bak"),
        ("e0000", "xx_e0000xe0000.png"),
        ("e0", "wf_e0000xe0000.png"),
        ("e0000", "wf_e0xe0.png"),
    ])
    def test_anything_else_is_refused(self, store, shard, name):
        assert store.image_file("subset", ACQ, shard, name) is None


# --- the grid ------------------------------------------------------------

class TestTriangle:
    def test_grid_from_an_index(self, store):
        g = triangle(store.index("subset", ACQ))
        assert g["elements"] == [0, 1, 2]
        assert set(g["cells"]) == {(0, 0), (0, 1), (0, 2), (1, 1), (1, 2), (2, 2)}
        assert g["cells"][(0, 1)]["shard"] == "e0000"
        assert g["cells"][(0, 1)]["name"] == "e0000xe0001"
        assert g["labels"][1] == "A1X"

    def test_unrendered_products_have_no_cell(self, tmp_path):
        d = make_index(tmp_path / "w" / "subset" / ACQ)
        index = json.loads((d / "index.json").read_text())
        index["products"][0]["rows"] = 0            # not yet written
        assert (0, 0) not in triangle(index)["cells"]

    def test_a_sparse_product_set_is_not_padded(self, tmp_path):
        """A subset acquisition is a shape, not a grid with holes."""
        d = make_index(tmp_path / "w" / "subset" / ACQ,
                       n_elements=4, rendered_pairs=[(0, 0), (0, 3), (3, 3)])
        g = triangle(json.loads((d / "index.json").read_text()))
        assert g["elements"] == [0, 3]
        assert set(g["cells"]) == {(0, 0), (0, 3), (3, 3)}

    def test_elements_filter(self, store):
        g = triangle(store.index("subset", ACQ), [2, 0])
        assert g["elements"] == [2, 0]

    def test_a_filter_matching_nothing_falls_back_to_all(self, store):
        g = triangle(store.index("subset", ACQ), [99])
        assert g["elements"] == [0, 1, 2]

    def test_empty_index_is_an_empty_grid(self):
        assert triangle({})["elements"] == []

    @pytest.mark.parametrize("raw,want", [
        ("0,1,2", [0, 1, 2]),
        (" 3 , 4 ", [3, 4]),
        ("", None),
        (None, None),
        ("a,b", None),
        ("1,x,2", [1, 2]),
    ])
    def test_parse_elements(self, raw, want):
        assert parse_elements(raw) == want


# --- config ---------------------------------------------------------------

class TestConfig:
    def test_load_config_keeps_the_waterfall_block(self, tmp_path, images):
        """The block has to survive load_config, not just create_app.

        Every other test here builds the config dict directly, which is
        exactly how an omission in load_config's per-key merge stays
        invisible until the page is loaded for real.
        """
        from choco.app import load_config
        cfg = tmp_path / "config.yaml"
        cfg.write_text(yaml.safe_dump({
            "configs_dir": str(tmp_path),
            "server": {"secret_key": "k" * 32},  # a placeholder key is refused at load
            "waterfall": {"ttl": 5},
        }))
        assert load_config(cfg)["waterfall"]["ttl"] == 5

    def test_images_dir_is_refused(self, tmp_path, images):
        """The tree's location is the job's (waterfall.yaml), read back
        from its state.json: a second copy in config.yaml only drifted."""
        from choco.app import load_config
        cfg = tmp_path / "config.yaml"
        cfg.write_text(yaml.safe_dump({
            "configs_dir": str(tmp_path), "server": {"secret_key": "k" * 32},
            "waterfall": {"images_dir": str(images)}}))
        with pytest.raises(ValueError, match="waterfall job's state.json"):
            load_config(cfg)

    def test_missing_waterfall_block_defaults_to_empty(self, tmp_path):
        from choco.app import load_config
        cfg = tmp_path / "config.yaml"
        cfg.write_text(yaml.safe_dump({"configs_dir": str(tmp_path),
                                        "server": {"secret_key": "k" * 32}}))
        assert load_config(cfg)["waterfall"] == {}


# --- routes --------------------------------------------------------------

def _app(tmp_path, images):
    from choco.app import _DEFAULT_CONFIG
    (tmp_path / "configs").mkdir(exist_ok=True)
    (tmp_path / "configs" / "nodes.yaml").write_text(yaml.safe_dump({"groups": {}}))
    config = dict(_DEFAULT_CONFIG)
    # the scan root is the directory whose *children* are acquisitions,
    # and its last component is the name the image tree is keyed by
    config["vis_files"] = {"roots": [str(tmp_path / "data" / "subset")],
                           "ttl": 30}
    config["waterfall"] = {"ttl": 0}
    app = create_app(configs_dir=tmp_path / "configs", config=config)
    app.config["TESTING"] = True
    app.config["state_dir"] = tmp_path / "state"   # never the host's /var/lib/choco
    _job_says_tree_is(app, images)
    return app


def _job_says_tree_is(app, images):
    """The waterfall job's state.json, naming its waterfalls_dir."""
    d = Path(app.config["state_dir"]) / "waterfall"
    d.mkdir(parents=True, exist_ok=True)
    (d / "state.json").write_text(json.dumps({"waterfalls_dir": str(images)}))


@pytest.fixture
def client(tmp_path, images):
    app = _app(tmp_path, images)
    with app.test_client() as c:
        yield c


class TestRoutes:
    def test_image_is_served(self, client):
        login(client)
        resp = client.get(f"/waterfall/subset/{ACQ}/e0000/th_e0000xe0001.png")
        assert resp.status_code == 200
        assert resp.data.startswith(b"\x89PNG")

    @pytest.mark.parametrize("path", [
        "/waterfall/subset/{acq}/e0000/wf_e0009xe0009.png",   # not written
        "/waterfall/subset/acq_nope/e0000/wf_e0000xe0000.png",
        "/waterfall/subset/{acq}/e0000/index.json",
        "/waterfall/subset/{acq}/nope/wf_e0000xe0000.png",
    ])
    def test_bad_image_requests_are_404(self, client, path):
        login(client)
        assert client.get(path.format(acq=ACQ)).status_code == 404

    def test_triangle_page_renders(self, client):
        login(client)
        resp = client.get(f"/files/subset/{ACQ}/triangle")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert ACQ in body
        assert "th_e0000xe0001.png" in body
        assert "6 products rendered" in body
        assert "2 source files folded in" in body

    def test_triangle_page_honours_the_element_filter(self, client):
        login(client)
        body = client.get(f"/files/subset/{ACQ}/triangle?elements=0,1").data.decode()
        assert "th_e0000xe0001.png" in body
        assert "e0002xe0002" not in body

    def test_triangle_of_an_unknown_acquisition_is_404(self, client):
        login(client)
        assert client.get("/files/subset/acq_nope/triangle").status_code == 404

    def test_triangle_rejects_a_traversal(self, client):
        login(client)
        assert client.get("/files/../../etc/triangle").status_code in (301, 404)

    def test_files_table_links_to_the_triangle(self, tmp_path, images):
        """The Waterfalls column matches an acquisition to its images."""
        data = tmp_path / "data" / "subset" / ACQ
        data.mkdir(parents=True)
        (data / "vis_0.h5").write_bytes(b"x")
        (data / "vis_1.h5").write_bytes(b"x")
        app = _app(tmp_path, images)
        with app.test_client() as c:
            login(c)
            body = c.get("/partials/files").data.decode()
        assert f"/files/subset/{ACQ}/triangle" in body
        assert ">2/2<" in body                      # 2 of 2 files folded in

    def test_files_table_shows_a_dash_when_nothing_is_rendered(self, tmp_path, images):
        data = tmp_path / "data" / "subset" / "acq_unrendered"
        data.mkdir(parents=True)
        (data / "vis_0.h5").write_bytes(b"x")
        app = _app(tmp_path, images)
        with app.test_client() as c:
            login(c)
            body = c.get("/partials/files").data.decode()
        assert "acq_unrendered" in body
        assert "/files/subset/acq_unrendered/triangle" not in body

    def test_the_tree_follows_the_job_state_file(self, tmp_path, images):
        """Nothing until the job has run; then wherever it says it writes,
        re-read when its state file changes; a relative path is ignored."""
        state = tmp_path / "state.json"
        st = WaterfallStore(state_file=lambda: state)
        assert st.images_dir is None and not st.configured
        state.write_text(json.dumps({"waterfalls_dir": str(images)}))
        assert st.images_dir == images
        state.write_text(json.dumps({"waterfalls_dir": "relative/tree", "pad": 1}))
        assert st.images_dir is None
        state.write_text(json.dumps({"waterfalls_dir": str(tmp_path / "moved")}))
        assert st.images_dir == tmp_path / "moved"

    def test_routes_are_inert_without_an_images_dir(self, tmp_path):
        from choco.app import _DEFAULT_CONFIG
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "nodes.yaml").write_text(yaml.safe_dump({"groups": {}}))
        config = dict(_DEFAULT_CONFIG)
        app = create_app(configs_dir=tmp_path / "configs", config=config)
        app.config["TESTING"] = True
        app.config["state_dir"] = tmp_path / "state"
        with app.test_client() as c:
            login(c)
            assert c.get(f"/files/subset/{ACQ}/triangle").status_code == 404
            assert c.get(
                f"/waterfall/subset/{ACQ}/e0000/th_e0000xe0000.png").status_code == 404


# --- fixes from the adversarial review -----------------------------------

class TestSummarySource:
    def test_summary_json_is_preferred_over_the_index(self, images):
        """The index is ~700 KB at 100 elements; the summary is a few hundred."""
        d = images / "subset" / ACQ
        (d / "index.json").write_text("{ this would not parse }")
        s = summarize_dir(str(d))
        assert not s["broken"] and s["rendered"] == 2 and s["elements"] == 3

    def test_index_is_the_fallback_when_no_summary(self, tmp_path):
        d = make_index(tmp_path / "w" / "subset" / ACQ, summary=False)
        assert summarize_dir(str(d))["rendered"] == 2

    def test_source_path_is_reported(self, store):
        assert store.summary("subset", ACQ)["source_path"] == "/data/subset"

    @pytest.mark.parametrize("doc", [
        {"products": ["not-a-dict"], "files": [1]},
        {"products": [{"rows": 1}], "n_elements": "many"},
        {"files": "some"},
        ["a", "list"],
        "a string",
    ])
    def test_a_malformed_index_costs_one_row_not_the_sweep(self, tmp_path, doc):
        """summarize_dir must be total — sweep does not wrap it."""
        d = tmp_path / "w" / "subset" / ACQ
        d.mkdir(parents=True)
        (d / "index.json").write_text(json.dumps(doc))
        assert summarize_dir(str(d))["broken"] is True
        swept = sweep(str(tmp_path / "w"))
        assert swept[("subset", ACQ)]["broken"] is True

    def test_one_bad_acquisition_does_not_hide_the_good_ones(self, tmp_path):
        base = tmp_path / "w" / "subset"
        make_index(base / ACQ)
        bad = base / "acq_bad"
        bad.mkdir(parents=True)
        (bad / "index.json").write_text(json.dumps(["nope"]))
        swept = sweep(str(tmp_path / "w"))
        assert swept[("subset", ACQ)]["rendered"] == 2
        assert swept[("subset", "acq_bad")]["broken"]


class TestIndexReading:
    def test_a_non_mapping_index_is_refused(self, tmp_path, images):
        (images / "subset" / ACQ / "index.json").write_text(json.dumps(["not", "a", "map"]))
        st = WaterfallStore(images, ttl_s=0)
        assert st.index("subset", ACQ) is None

    def test_an_unchanged_index_is_not_reparsed(self, store):
        first = store.index("subset", ACQ)
        assert store.index("subset", ACQ) is first     # same object, one stat

    def test_the_cache_is_bounded(self, images):
        """The store outlives every page; one index is ~700 kB at 100 elements."""
        names = [f"acq_{i:03d}" for i in range(INDEX_CACHE_MAX + 4)]
        for n in names:
            make_index(images / "subset" / n)
        st = WaterfallStore(images, ttl_s=0)
        for n in names:
            assert st.index("subset", n) is not None
        assert len(st._index_cache) == INDEX_CACHE_MAX

    def test_eviction_is_least_recently_used(self, images):
        """A polled page must not be evicted by traffic to other acquisitions."""
        names = [f"acq_{i:03d}" for i in range(INDEX_CACHE_MAX + 1)]
        for n in names:
            make_index(images / "subset" / n)
        st = WaterfallStore(images, ttl_s=0)
        for n in names[:INDEX_CACHE_MAX]:
            st.index("subset", n)
        kept = st.index("subset", names[0])            # keep touching the first
        st.index("subset", names[-1])                  # evicts the LRU, not it
        assert st.index("subset", names[0]) is kept    # never reparsed
        assert len(st._index_cache) == INDEX_CACHE_MAX


class TestShard:
    def test_shard_follows_the_writer(self, tmp_path):
        """store.product_dir shards by a; min(a, b) would diverge if a > b."""
        d = make_index(tmp_path / "w" / "subset" / ACQ, rendered_pairs=[(0, 1)])
        index = json.loads((d / "index.json").read_text())
        index["products"][0].update({"a": 3, "b": 1})   # a > b
        assert triangle(index)["cells"][(1, 3)]["shard"] == "e0003"


class TestStreaming:
    def test_open_stream_yields_the_file(self, images):
        p = images / "subset" / ACQ / "e0000" / "wf_e0000xe0001.png"
        size, mtime, chunks = open_stream(p, chunk=4)
        data = b"".join(chunks)
        assert data == p.read_bytes()
        assert size == len(data) and mtime > 0

    def test_open_stream_of_a_missing_file_is_none(self, tmp_path):
        assert open_stream(tmp_path / "nope.png") is None

    def test_open_stream_gives_up_on_a_wedged_mount(self, tmp_path, monkeypatch):
        import os
        import choco.waterfalls as wf
        r, w = os.pipe()             # a real kernel block, as in test_datafiles

        def wedged(path):
            os.read(r, 1)
            raise OSError("late")

        monkeypatch.setattr(wf, "_open_and_stat", wedged)
        monkeypatch.setattr(wf, "READ_TIMEOUT_S", 0.05)
        try:
            assert open_stream(tmp_path / "x.png") is None
        finally:
            os.write(w, b"x")

    def test_route_streams_and_sets_length(self, client, images):
        login(client)
        p = images / "subset" / ACQ / "e0000" / "wf_e0000xe0001.png"
        resp = client.get(f"/waterfall/subset/{ACQ}/e0000/wf_e0000xe0001.png")
        assert resp.status_code == 200
        assert resp.data == p.read_bytes()
        assert resp.headers["Content-Length"] == str(p.stat().st_size)
        assert resp.headers["Content-Type"] == "image/png"


class TestRootKeying:
    def test_two_roots_sharing_a_basename_do_not_collide(self, tmp_path):
        """The lookup keys on the acquisition's own source path."""
        from choco.app import _DEFAULT_CONFIG
        images = tmp_path / "waterfalls"
        for site in ("cs00", "cs01"):
            make_index(images / site / ACQ,
                       source_path=str(tmp_path / site / "subset"))
            data = tmp_path / site / "subset" / ACQ
            data.mkdir(parents=True)
            (data / "vis_0.h5").write_bytes(b"x")

        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "nodes.yaml").write_text(yaml.safe_dump({"groups": {}}))
        config = dict(_DEFAULT_CONFIG)
        config["vis_files"] = {"roots": [str(tmp_path / "cs00" / "subset"),
                                         str(tmp_path / "cs01" / "subset")], "ttl": 30}
        config["waterfall"] = {"ttl": 0}
        app = create_app(configs_dir=tmp_path / "configs", config=config)
        app.config["TESTING"] = True
        app.config["state_dir"] = tmp_path / "state"
        _job_says_tree_is(app, images)
        with app.test_client() as c:
            login(c)
            body = c.get("/partials/files").data.decode()
        assert f"/files/cs00/{ACQ}/triangle" in body
        assert f"/files/cs01/{ACQ}/triangle" in body


class TestAxisLabels:
    def test_element_names_label_the_axes(self, client):
        """The dish name belongs on the axis, not only in a tooltip."""
        login(client)
        body = client.get(f"/files/subset/{ACQ}/triangle").data.decode()
        assert '<td class="num col" title="element 0">A0X</td>' in body
        assert '<td class="num row" title="element 2">A2X</td>' in body

    def test_axes_fall_back_to_the_index_without_labels(self, tmp_path, images):
        d = images / "subset" / ACQ
        index = json.loads((d / "index.json").read_text())
        index["labels"] = []
        (d / "index.json").write_text(json.dumps(index))
        g = triangle(index)
        assert g["labels"] == {0: "0", 1: "1", 2: "2"}

# --- the viewer's side files and axes ------------------------------------

class TestSideFiles:
    def test_npy_round_trip(self, tmp_path):
        p = tmp_path / "freq.npy"
        p.write_bytes(npy_bytes([600.0, 601.5625, 700.25]))
        assert read_npy_1d(str(p)) == [600.0, 601.5625, 700.25]

    def test_npy_rejects_garbage_and_odd_layouts(self, tmp_path):
        p = tmp_path / "x.npy"
        p.write_bytes(b"not an npy at all")
        with pytest.raises(ValueError):
            read_npy_1d(str(p))
        blob = npy_bytes([1.0])
        p.write_bytes(blob.replace(b"'<f8'", b"'<c8'"))
        with pytest.raises(ValueError):
            read_npy_1d(str(p))

    def test_times_round_trip(self, tmp_path):
        p = tmp_path / "times.bin"
        p.write_bytes(struct.pack("<3q", T0_NS, T0_NS + STEP_NS, -1))
        assert read_times(str(p)) == [T0_NS, T0_NS + STEP_NS, -1]

    def test_png_head_is_read_from_the_fixed_offsets(self, tmp_path):
        p = tmp_path / "wf.png"
        p.write_bytes(png_with_palette(width=64, height=12))
        head = read_png_head(str(p))
        assert (head["width"], head["height"]) == (64, 12)
        pal = head["palette"]
        assert len(pal) == 256
        assert pal[0] == (0, 0, 255)
        assert pal[255] == (255, 0, 0)

    def test_png_head_of_a_junk_file_raises(self, tmp_path):
        p = tmp_path / "wf.png"
        p.write_bytes(b"\x89PNG\r\n\x1a\nwf")
        with pytest.raises(ValueError):
            read_png_head(str(p))

    def test_store_serves_and_caches_side_files(self, store):
        freqs = store.freq_axis("subset", ACQ)
        assert freqs[0] == 600.0 and len(freqs) == 64
        assert store.freq_axis("subset", ACQ) is freqs        # cached
        times = store.times("subset", ACQ)
        assert times[0] == T0_NS and len(times) == 12

    def test_store_side_files_missing_are_none(self, store, images):
        os.remove(images / "subset" / ACQ / "times.bin")
        store._side_cache.clear()
        assert store.times("subset", ACQ) is None
        assert store.times("subset", "acq_nope") is None

    def test_store_reads_an_image_head(self, store):
        head = store.image_head("subset", ACQ, "e0000", "wf_e0000xe0001.png")
        assert head["palette"][0] == (0, 0, 255)
        assert head["height"] == 12
        assert store.image_head("subset", ACQ, "e0000", "th_e0000xe0001.png") is None
        assert store.image_head("subset", ACQ, "nope", "wf_e0000xe0001.png") is None


class TestTicks:
    def test_freq_ticks_label_real_values(self):
        axis = freq_ticks([600.0 + i for i in range(64)], 64, target=4)
        assert axis["unit"] == "MHz"
        assert axis["ticks"][0]["label"] == "600"
        assert axis["ticks"][-1]["label"] == "663"
        assert 0 < axis["ticks"][0]["frac"] < axis["ticks"][-1]["frac"] < 1

    def test_freq_ticks_fall_back_to_channel_numbers(self):
        axis = freq_ticks(None, 8, target=3)
        assert axis["unit"] == "channel"
        assert [t["label"] for t in axis["ticks"]] == ["0", "4", "7"]
        assert freq_ticks([1.0, 2.0], 8)["unit"] == "channel"  # width mismatch

    def test_time_ticks_label_site_local_time(self):
        """The epoch is J2000, not unix — confused, the date is off by ~30 y —
        and labels convert to the site zone (Vancouver: 22:00 UTC = 15:00 PDT)."""
        times = [T0_NS + STEP_NS * r for r in range(12)]
        axis = time_ticks(times, 12, target=3)
        assert axis["unit"] == "PDT"
        assert axis["start"] == "2026-08-20 15:00:00"
        assert axis["ticks"][0]["label"] == "15:00:00"
        # under three hours of span the seconds matter
        assert all(t["label"].count(":") == 2 for t in axis["ticks"])

    def test_time_ticks_honour_a_configured_zone(self):
        times = [T0_NS + STEP_NS * r for r in range(12)]
        axis = time_ticks(times, 12, target=3, tz="UTC")
        assert axis["unit"] == "UTC"
        assert axis["ticks"][0]["label"] == "22:00:00"
        # an unknown zone degrades to UTC rather than a 500
        assert time_ticks(times, 12, target=3, tz="Mars/Olympus")["unit"] == "UTC"

    def test_time_ticks_never_label_presync_padding(self):
        """A zero entry means "no timestamp", not the J2000 epoch."""
        times = [0, 0] + [T0_NS + STEP_NS * r for r in range(10)]
        axis = time_ticks(times, 12, target=3)
        assert axis["unit"] == "PDT"
        assert axis["start"] == "2026-08-20 15:00:00"   # first *real* sample
        # the tick that landed on a padded row is dropped, not fabricated
        assert [t["label"] for t in axis["ticks"]] == ["15:00:38", "15:01:26"]
        assert "2000-01-01" not in json.dumps(axis)

    def test_time_ticks_fall_back_to_scanline_numbers(self):
        for times in (None, [0] * 12):                # missing, all padding
            axis = time_ticks(times, 12, target=3)
            assert axis["unit"] == "scanline"
            assert axis["start"] is None
            assert [t["label"] for t in axis["ticks"]] == ["0", "6", "11"]

    def test_time_ticks_when_the_image_is_an_append_ahead(self):
        """Live skew: the served PNG can be taller than times.bin.  The
        axis stays UT1 over the rows we know; the unlabelled tail gets no
        tick rather than a fabricated one."""
        times = [T0_NS + STEP_NS * r for r in range(10)]
        axis = time_ticks(times, 12, target=3)        # rows 10, 11 unknown
        assert axis["unit"] == "PDT"
        assert axis["end"] == "15:01:26"              # last *known* sample
        assert [t["label"] for t in axis["ticks"]] == ["15:00:00", "15:00:57"]

    def test_value_ticks_are_decades(self):
        ticks = value_ticks(1.0, 1000.0)
        assert [t["label"] for t in ticks] == ["1", "10", "100", "1000"]
        assert ticks[0]["frac"] == 0.0 and ticks[-1]["frac"] == 1.0

    def test_value_ticks_fill_in_a_short_span(self):
        labels = [t["label"] for t in value_ticks(1.0, 10.0)]
        assert labels == ["1", "2", "5", "10"]

    def test_value_ticks_of_an_unusable_scale_are_empty(self):
        assert value_ticks(None, None) == []
        assert value_ticks(10.0, 1.0) == []
        assert value_ticks(0.0, 1.0) == []

    def test_palette_gradient(self):
        pal = [(i, 0, 255 - i) for i in range(256)]
        css = palette_gradient(pal)
        assert css.startswith("linear-gradient(to top, rgb(1,0,254) 0.0%")
        assert css.endswith("rgb(255,0,0) 100.0%)")
        assert palette_gradient(None) is None
        assert palette_gradient(pal[:16]) is None


class TestViewerRoute:
    def test_viewer_renders_axes_and_colorbar(self, client):
        login(client)
        resp = client.get(f"/waterfall/subset/{ACQ}/view/wf_e0000xe0001.png")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "A0X" in body and "A1X" in body
        assert "in MHz" in body and "600" in body
        assert "in local time (PDT)" in body
        assert "linear-gradient(to top" in body
        assert f"/waterfall/subset/{ACQ}/e0000/wf_e0000xe0001.png" in body
        assert f"/files/subset/{ACQ}/triangle" in body

    @pytest.mark.parametrize("path", [
        "/waterfall/subset/{acq}/view/wf_e0009xe0009.png",    # not rendered
        "/waterfall/subset/{acq}/view/th_e0000xe0001.png",    # not a full image
        "/waterfall/subset/acq_nope/view/wf_e0000xe0001.png",
        "/waterfall/subset/{acq}/view/index.json",
        "/waterfall/subset/{acq}/view/wf_..xe0001.png",
    ])
    def test_bad_viewer_requests_are_404(self, client, path):
        login(client)
        assert client.get(path.format(acq=ACQ)).status_code == 404

    def test_viewer_degrades_without_side_files(self, tmp_path, images):
        """No freq.npy, times.bin or readable palette — plainer, never a 500."""
        acq = "acq_20260724_000000_000000000"
        make_index(images / "subset" / acq, side_files=False)
        for f in (images / "subset" / acq).rglob("wf_*.png"):
            f.write_bytes(b"\x89PNG\r\n\x1a\nwf")              # no PLTE inside
        app = _app(tmp_path, images)
        with app.test_client() as client:
            login(client)
            body = client.get(
                f"/waterfall/subset/{acq}/view/wf_e0000xe0001.png").data.decode()
        assert "in channel index" in body
        assert "in scanline number" in body
        assert "linear-gradient" not in body

    def test_triangle_links_to_the_viewer(self, client):
        login(client)
        body = client.get(f"/files/subset/{ACQ}/triangle").data.decode()
        assert f"/waterfall/subset/{ACQ}/view/wf_e0000xe0001.png" in body


# --- the overview: every product at chunk resolution, the matrix, coherence

from choco.waterfalls import (coarse_image, coarse_view_context, encode_png, matrix_context,
                              matrix_image, overview_of, product_row, strips, view_context)

OV_ACQ = "acq_20260725_000000_000000000"


def make_overview_index(path, n_el=3, n_chunk=4, n_band=2, rows=3, labels=None, rpf=None):
    """An acquisition with a selection (autos + element 0's crosses) and an
    overview: the cubes hold recognisable values, one row per file (or
    ``rpf`` rows a file, the layout from 2026-10-27, with the full array)."""
    n_prod_file = n_el * (n_el + 1) // 2
    pairs = [(a, b) for a in range(n_el) for b in range(a, n_el)]
    rendered = [(a, b) for a, b in pairs if a == b or a == 0]
    make_index(path, n_elements=n_el, files=tuple(range(1, rows + 1)), rows=rows * 2,
               rendered_pairs=rendered)
    index = json.loads((path / "index.json").read_text())
    index["labels"] = labels or [f"A{i}X" for i in range(n_el)]
    index["n_prod_file"] = n_prod_file
    index["n_freq_file"] = 64
    index["overview"] = {"chunks": n_chunk, "chunk_rows": 16, "edges": [600.0, 650.0, 700.0][:n_band + 1],
                         "n_el": n_el, "prod_order": "triangle"}
    r_ = rpf or 1
    if rpf:
        index["overview"].update(col_width=8, rows_per_file=rpf, array_full=True)
        index["array_full"] = {"width": 64, "rows": rows * 6, "nbytes": 100, "adler": 1}
        (path / "co_array_full.png").write_bytes(png_with_palette(64, rows * 6))
    index["coarse_rows"] = rows
    index["coarse_bytes"] = rows * r_ * n_prod_file * n_chunk
    index["matrix_bytes"] = rows * n_band * n_el * n_el
    index["coarse_times_bytes"] = rows * r_ * 8
    index["elements"] = [{"e": e, "name": f"e{e:04d}", "width": n_chunk, "rows": rows * r_,
                          "nbytes": 100, "adler": 1} for e in range(n_el)]
    (path / "index.json").write_text(json.dumps(index))
    # coarse: value = product index + 1 for every chunk, so a slice is recognisable
    (path / "coarse.dat").write_bytes(bytes(
        p + 1 for r in range(rows * r_) for p in range(n_prod_file) for c in range(n_chunk)))
    # matrix: value = 10 * band + row + 1, diagonal 255
    (path / "matrix.dat").write_bytes(bytes(
        255 if i == j else 10 * b + r + 1
        for r in range(rows) for b in range(n_band) for i in range(n_el) for j in range(n_el)))
    (path / "coarse_times.bin").write_bytes(struct.pack("<%dq" % (rows * r_), *(
        T0_NS + 400 * 10**9 * r // r_ for r in range(rows * r_))))
    (path / "coarse_freq.npy").write_bytes(npy_bytes([600.0 + 25.0 * c for c in range(n_chunk)]))
    (path / "palette.bin").write_bytes(bytes(b for i in range(256) for b in (i, i, i)))
    for e in range(n_el):
        shard = path / f"e{e:04d}"
        shard.mkdir(exist_ok=True)
        (shard / f"co_e{e:04d}.png").write_bytes(png_with_palette(n_chunk, rows * r_))
    return path


def _decode_rows(png: bytes) -> list[bytes]:
    """The scanlines of an unfiltered 8-bit PNG (what encode_png writes)."""
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    width, height = struct.unpack(">II", png[16:24])
    pos, idat = 8, b""
    while pos < len(png):
        (n,), typ = struct.unpack(">I", png[pos:pos + 4]), png[pos + 4:pos + 8]
        if typ == b"IDAT":
            idat += png[pos + 8:pos + 8 + n]
        pos += 12 + n
    raw = zlib.decompress(idat)
    assert len(raw) == height * (width + 1)
    return [raw[r * (width + 1) + 1:(r + 1) * (width + 1)] for r in range(height)]


@pytest.fixture
def ov_images(tmp_path):
    root = tmp_path / "waterfalls"
    make_index(root / "subset" / ACQ)
    make_overview_index(root / "subset" / OV_ACQ)
    return root


@pytest.fixture
def ov_store(ov_images):
    return WaterfallStore(ov_images, ttl_s=0)


class TestOverviewRecord:
    def test_overview_of_checks_the_record(self, ov_store):
        ov = overview_of(ov_store.index("subset", OV_ACQ))
        assert ov["chunks"] == 4 and ov["n_el"] == 3 and ov["n_band"] == 2 and ov["rows"] == 3
        assert ov["n_prod_file"] == 6 and ov["prod_order"] == "triangle"
        assert overview_of(ov_store.index("subset", ACQ)) is None
        assert overview_of({"overview": {"chunks": "x"}}) is None
        assert overview_of({"overview": {"chunks": 4, "n_el": 3, "edges": [1.0]}, "n_prod_file": 6}) is None

    def test_product_row_follows_kotekan_order(self):
        ov = {"n_el": 4, "n_prod_file": 10, "prod_order": "triangle", "prod_pairs": None}
        pairs = [(a, b) for a in range(4) for b in range(a, 4)]
        assert [product_row(ov, a, b) for a, b in pairs] == list(range(10))
        assert product_row(ov, 3, 1) == product_row(ov, 1, 3) == 6
        assert product_row(ov, 0, 4) is None
        listed = {"n_el": 3, "n_prod_file": 2, "prod_order": "list", "prod_pairs": [[0, 0], [1, 2]]}
        assert product_row(listed, 2, 1) == 1 and product_row(listed, 0, 1) is None

    def test_encode_png_round_trips(self):
        pal = [(i, 0, 0) for i in range(256)]
        png = encode_png(3, [b"\x01\x02\x03", b"\x04\x05\x06"], pal)
        assert _decode_rows(png) == [b"\x01\x02\x03", b"\x04\x05\x06"]
        assert png[41:47] == b"\x00\x00\x00\x01\x00\x00"      # the PLTE at the writer's offset
        with pytest.raises(ValueError):
            encode_png(3, [b"\x01\x02"], pal)


class TestOverviewImages:
    def test_coarse_image_is_the_products_rows(self, ov_store):
        png = coarse_image(ov_store, "subset", OV_ACQ, "wf_e0001xe0002.png")
        rows = _decode_rows(png)
        assert rows == [bytes([5] * 4)] * 3                      # product (1,2) is index 4 -> value 5
        assert coarse_image(ov_store, "subset", OV_ACQ, "wf_e0002xe0001.png") == png
        assert coarse_image(ov_store, "subset", OV_ACQ, "wf_e0000xe0009.png") is None
        assert coarse_image(ov_store, "subset", OV_ACQ, "th_e0000xe0001.png") is None
        assert coarse_image(ov_store, "subset", ACQ, "wf_e0000xe0001.png") is None   # no overview

    def test_matrix_image_is_one_frame(self, ov_store):
        rows = _decode_rows(matrix_image(ov_store, "subset", OV_ACQ, 1, 2))
        assert rows == [bytes([255, 13, 13]), bytes([13, 255, 13]), bytes([13, 13, 255])]
        assert matrix_image(ov_store, "subset", OV_ACQ, 2, 0) is None
        assert matrix_image(ov_store, "subset", OV_ACQ, 0, 3) is None
        assert matrix_image(ov_store, "subset", OV_ACQ, "x", 0) is None

    def test_matrix_context_defaults_to_the_latest_frame(self, ov_store):
        ctx = matrix_context(ov_store, "subset", OV_ACQ, None, None)
        assert (ctx["band"], ctx["row"], ctx["n_rows"], ctx["n_el"]) == (1, 2, 3, 3)
        assert [b["label"] for b in ctx["bands"]] == ["600–650 MHz", "650–700 MHz"]
        assert ctx["matrix_data"]["labels"] == ["A0X", "A1X", "A2X"]
        assert ctx["when"] and ctx["gradient"].startswith("linear-gradient")
        assert matrix_context(ov_store, "subset", OV_ACQ, "0", "1")["row"] == 1
        assert matrix_context(ov_store, "subset", OV_ACQ, "9", None) is None
        assert matrix_context(ov_store, "subset", OV_ACQ, "a", None) is None
        assert matrix_context(ov_store, "subset", ACQ, None, None) is None

    def test_view_contexts_name_their_kind(self, ov_store):
        full = view_context(ov_store, "subset", OV_ACQ, "wf_e0000xe0001.png")
        assert full["kind"] == "full" and full["image_src"].endswith("/e0000/wf_e0000xe0001.png")
        coh = view_context(ov_store, "subset", OV_ACQ, "co_e0002.png")
        assert coh["kind"] == "coherence" and coh["label_a"] == "A2X" and coh["n_freq"] == 4
        assert coh["value_ticks"][-1]["label"] == "1" and coh["freq_axis"]["unit"] == "MHz"
        assert view_context(ov_store, "subset", OV_ACQ, "co_e0007.png") is None
        assert view_context(ov_store, "subset", ACQ, "co_e0000.png") is None
        coarse = coarse_view_context(ov_store, "subset", OV_ACQ, "wf_e0001xe0002.png")
        assert coarse["kind"] == "coarse" and coarse["rows"] == 3 and coarse["n_freq"] == 4
        assert coarse["label_b"] == "A2X" and coarse["image_src"].endswith("/coarse/wf_e0001xe0002.png")
        assert coarse_view_context(ov_store, "subset", ACQ, "wf_e0000xe0001.png") is None

    def test_strips_group_the_selection(self, ov_store):
        out = strips(ov_store.index("subset", OV_ACQ))
        assert [s["title"] for s in out] == ["Autocorrelations", "A0X × the array", "Coherence with the array"]
        assert [c["label"] for c in out[0]["cells"]] == ["A0X", "A1X", "A2X"]
        assert [c["label"] for c in out[1]["cells"]] == ["A0X×A1X", "A0X×A2X"]
        assert out[2]["cells"][1]["src"].endswith("/e0001/co_e0001.png")
        assert strips(ov_store.index("subset", ACQ)) is None            # the whole triangle


class TestOverviewRoutes:
    @pytest.fixture
    def ov_client(self, tmp_path, ov_images):
        app = _app(tmp_path, ov_images)
        with app.test_client() as client:
            login(client)
            yield client

    def test_matrix_page_and_frames(self, ov_client):
        body = ov_client.get(f"/files/subset/{OV_ACQ}/matrix").data.decode()
        assert "650–700 MHz" in body and 'id="matrix-row"' in body and "frame 3 of 3" in body
        assert f"/waterfall/subset/{OV_ACQ}/matrix/1/2.png" in body
        resp = ov_client.get(f"/waterfall/subset/{OV_ACQ}/matrix/1/2.png")
        assert resp.status_code == 200 and resp.data.startswith(b"\x89PNG")
        assert ov_client.get(f"/waterfall/subset/{OV_ACQ}/matrix/1/9.png").status_code == 404
        assert ov_client.get(f"/waterfall/subset/{OV_ACQ}/matrix/x/0.png").status_code == 404
        assert ov_client.get(f"/files/subset/{ACQ}/matrix").status_code == 404

    def test_coarse_images_and_views(self, ov_client):
        resp = ov_client.get(f"/waterfall/subset/{OV_ACQ}/coarse/wf_e0001xe0002.png")
        assert resp.status_code == 200 and _decode_rows(resp.data)[0] == bytes([5] * 4)
        body = ov_client.get(f"/waterfall/subset/{OV_ACQ}/coarse-view/wf_e0001xe0002.png").data.decode()
        assert "cell resolution" in body and "16 channels by a file" in body
        assert f"/waterfall/subset/{OV_ACQ}/coarse/wf_e0001xe0002.png" in body
        assert ov_client.get(f"/waterfall/subset/{OV_ACQ}/coarse/wf_e0000xe0009.png").status_code == 404
        assert ov_client.get(f"/waterfall/subset/{OV_ACQ}/coarse-view/th_e0000xe0001.png").status_code == 404

    def test_two_rows_a_file_and_the_full_array(self, tmp_path):
        root = tmp_path / "waterfalls"
        make_overview_index(root / "subset" / OV_ACQ, rpf=2)
        store = WaterfallStore(root, ttl_s=0)
        index = store.index("subset", OV_ACQ)
        ov = overview_of(index)
        assert ov["rows"] == 3 and ov["rpf"] == 2 and ov["img_rows"] == 6
        assert ov["cell"] == "8 channels by half a file"
        assert len(_decode_rows(coarse_image(store, "subset", OV_ACQ, "wf_e0001xe0002.png"))) == 6
        assert coarse_view_context(store, "subset", OV_ACQ, "wf_e0001xe0002.png")["rows"] == 6
        # a matrix frame is a file: its time is the file's first row's, as
        # in the one-row layout
        make_overview_index(tmp_path / "old" / "subset" / OV_ACQ)
        old = WaterfallStore(tmp_path / "old", ttl_s=0)
        assert matrix_context(store, "subset", OV_ACQ, 0, 1)["when"] == \
            matrix_context(old, "subset", OV_ACQ, 0, 1)["when"] is not None
        last = store.last_file("subset", OV_ACQ)["coarse.dat"]
        assert len(last) == 2
        full = view_context(store, "subset", OV_ACQ, "co_array_full.png")
        assert full["kind"] == "array_full" and full["rows"] == 18 and full["n_freq"] == 64

    def test_visibility_triangle_under_the_array(self, ov_images, ov_client):
        acq_dir = ov_images / "subset" / OV_ACQ
        (acq_dir / "co_matrix.png").write_bytes(encode_png(3, [bytes([9, 8, 7])] * 3, [(i, i, i) for i in range(256)]))
        body = ov_client.get(f"/files/subset/{OV_ACQ}/triangle").data.decode()
        assert "Visibility triangles" in body and 'id="matrix-img"' in body and "waterfall_matrix.js" in body
        assert body.index("Visibility triangles") < body.index('id="dg-co"')
        assert "below: <b>mean of" in body and "above: <b>latest file" in body
        assert '"halves": {"lower": "mean of' in body
        # the key is horizontal and drawn from the triangle's own (equalized) palette
        assert "linear-gradient(to right, rgb(1,1,1)" in body and "histogram-equalized" in body
        resp = ov_client.get(f"/waterfall/subset/{OV_ACQ}/array/co_matrix.png")
        assert resp.status_code == 200 and resp.data.startswith(b"\x89PNG")
        # the wrong size for the acquisition: left out rather than mislabelled
        (acq_dir / "co_matrix.png").write_bytes(encode_png(2, [bytes([9, 8])] * 2, [(i, i, i) for i in range(256)]))
        assert "Visibility triangles" not in ov_client.get(f"/files/subset/{OV_ACQ}/triangle").data.decode()

    def test_coherence_image_and_view(self, ov_client):
        assert ov_client.get(f"/waterfall/subset/{OV_ACQ}/e0002/co_e0002.png").status_code == 200
        body = ov_client.get(f"/waterfall/subset/{OV_ACQ}/view/co_e0002.png").data.decode()
        assert "coherence with the array" in body and "A2X" in body
        assert ov_client.get(f"/waterfall/subset/{OV_ACQ}/e0002/co_e0007.png").status_code == 404

    def test_sheet_stands_for_the_strips_and_links_the_matrix(self, ov_client):
        body = ov_client.get(f"/files/subset/{OV_ACQ}/triangle").data.decode()
        # the sheet's grids take the autos and the coherence images, and no
        # reference strips follow it
        assert 'id="dg-co"' in body and 'id="dg-auto"' in body and "Autocorrelations" in body
        assert 'id="dg-auto" value="autocorrelations" checked' in body     # the default; #coherence picks the other
        assert "A0X × the array" not in body and 'class="strip"' not in body and 'table class="tri"' not in body
        assert f"/files/subset/{OV_ACQ}/matrix" in body
        # no mean matrix rendered yet: no triangle
        assert "Visibility triangles" not in body
        assert "gains are left in" in body and "gains are divided out" not in body

    def test_sheet_says_when_the_gains_are_divided_out(self, ov_images, ov_client):
        path = ov_images / "subset" / OV_ACQ / "index.json"
        index = json.loads(path.read_text())
        path.write_text(json.dumps({**index, "gains": "divided"}))
        body = ov_client.get(f"/files/subset/{OV_ACQ}/triangle").data.decode()
        assert "gains are divided out, so this is the\n        power at the ADCs" in body
        body = ov_client.get(f"/files/subset/{OV_ACQ}/triangle?layout=strips").data.decode()
        assert "A0X × the array" in body and 'class="strip"' in body
        body = ov_client.get(f"/files/subset/{OV_ACQ}/triangle?layout=triangle").data.decode()
        assert 'class="strip"' not in body and 'table class="tri"' in body
        body = ov_client.get(f"/files/subset/{ACQ}/triangle").data.decode()
        assert 'table class="tri"' in body and f"/files/subset/{ACQ}/matrix" not in body


# --- the sheet: array, dish grids, per-dish readings, streams, masks -------

SH_ACQ = "acq_20261023_142451_810159661"
SH_LABELS = ["A01X", "A02X", "B01X", "A01Y", "A02Y", "B01Y"]     # [P][D], 3 dishes


def _diag_line(file, n_bin=4, masked=None, live=None, gains="g1", stream_masked=None, nodes=None):
    rec = {"file": file, "n_bin": n_bin, "n_streams": 2,
           "times": [T0_NS + (400 * (file - 1) + 100 * b) * 10**9 for b in range(n_bin)],
           "flag_live": live or [2] * n_bin, "masked": masked or {},
           "missing": [[0] * n_bin, [0, 0, 5000, 0][:n_bin]],
           "rfi": [[100] * n_bin] * 2, "pl": [[0] * n_bin] * 2,
           "rfi_on": [[10000] * n_bin, [10000, 0, 10000, 10000]], "fringestop": 0, "gains": gains,
           "versions": ["chord+1"], "stream_masked": stream_masked or [[1] * n_bin, [1] * n_bin]}
    if nodes:
        rec["nodes"] = nodes
    return rec


@pytest.fixture
def sh_images(tmp_path):
    """Three files of six feeds.  Autos: level 200 for every product, 100
    for A01Y, nothing for A02Y.  A01X is masked throughout, B01Y partly in file 3; stream 1
    masks one feed fewer in file 2 bin 1; the gains change in file 3."""
    root = tmp_path / "waterfalls"
    path = make_overview_index(root / "subset" / SH_ACQ, n_el=6, labels=SH_LABELS)
    n, rows, n_chunk = 6, 3, 4
    n_prod = n * (n + 1) // 2
    pairs = [(a, b) for a in range(n) for b in range(a, n)]
    level = {(3, 3): 100, (4, 4): 0}
    row = bytes(level.get((a, b), 200) for a, b in pairs for c in range(n_chunk))
    (path / "coarse.dat").write_bytes(row * rows)
    (path / "coarse_scale.npy").write_bytes(npy_bytes_2d([[1.0, 1e6]] * n_prod))
    index = json.loads((path / "index.json").read_text())
    index["array"] = {"width": n_chunk, "rows": rows, "nbytes": 10, "adler": 1}
    (path / "index.json").write_text(json.dumps(index))
    (path / "co_array.png").write_bytes(png_with_palette(n_chunk, rows))
    sm = [[1, 1, 1, 1], [1, 0, 1, 1]]
    lines = [_diag_line(1, masked={"A01X": [[0, 4, 2]]}),
             _diag_line(2, masked={"A01X": [[0, 4, 2]]}, stream_masked=sm),
             _diag_line(3, masked={"A01X": [[0, 4, 2]], "B01Y": [[2, 4, 1]]}, gains="g2",
                        nodes={"0": "cx52", "1": "cx19"})]
    (path / "diag.jsonl").write_text("".join(json.dumps(x) + "\n" for x in lines) + '{"file": 9')
    return root


def npy_bytes_2d(rows):
    import io
    header = "{'descr': '<f4', 'fortran_order': False, 'shape': (%d, %d), }" % (len(rows), len(rows[0]))
    header += " " * (63 - len(header) % 64) + "\n"
    out = io.BytesIO()
    out.write(b"\x93NUMPY\x01\x00" + struct.pack("<H", len(header)) + header.encode())
    for r in rows:
        out.write(struct.pack("<%df" % len(r), *r))
    return out.getvalue()


@pytest.fixture
def sh_store(sh_images):
    return WaterfallStore(sh_images, ttl_s=0)


class TestSheet:
    def test_dish_grid_lays_dishes_out_as_they_stand(self):
        from choco.waterfalls import dish_grid
        g = dish_grid(["A02X", "A01X", "B01X", "A01Y", "RFI1"])
        assert g["cols"] == [1, 2] and [r["name"] for r in g["rows"]] == ["A", "B"]
        assert g["rows"][0]["cells"] == [{"name": "A01", "x": 1, "y": 3},
                                         {"name": "A02", "x": 0, "y": None}]
        assert g["rows"][1]["cells"] == [{"name": "B01", "x": 2, "y": None}, None]
        assert g["other"] == [4]

    def test_npy_pairs_and_diag_tail(self, sh_store):
        assert sh_store.coarse_scale("subset", SH_ACQ)[0] == (1.0, 1e6)
        assert [r["file"] for r in sh_store.diag("subset", SH_ACQ)] == [1, 2, 3]

    def test_per_dish_readings_of_the_last_file(self, sh_store):
        from choco.waterfalls import sheet_context
        ctx = sheet_context(sh_store, "subset", SH_ACQ, node_versions={"cx52": "+1242"}, tz="UTC")
        feeds = {f["label"]: f for row in ctx["grid"]["rows"] for d in row["cells"] if d
                 for f in (d["x"], d["y"]) if f}
        # the cold Y feed reads below its polarization's median, the rest at 1
        assert feeds["A01Y"]["auto_ratio"] < 0.1 and feeds["A01Y"]["auto_class"] == 0
        assert feeds["A01X"]["auto_ratio"] == pytest.approx(1) and feeds["A01X"]["auto_class"] == 3
        assert feeds["A02Y"]["auto_ratio"] is None and feeds["A02Y"]["auto_class"] is None
        assert feeds["A01X"]["co"].endswith("/e0000/co_e0000.png")
        assert feeds["A01X"]["auto_view"].endswith("/view/wf_e0000xe0000.png")
        assert ctx["array"]["src"].endswith("/array/co_array.png") and ctx["array"]["rows"] == 3

    def test_streams_name_nodes_and_count_steps_out(self, sh_store):
        from choco.waterfalls import sheet_context
        st = sheet_context(sh_store, "subset", SH_ACQ, node_versions={"cx52": "+1242"}, tz="UTC")["streams"]
        r0, r1 = st["rows"]
        assert (r0["node"], r0["build"], r1["node"], r1["build"]) == ("cx52", "+1242", "cx19", None)
        assert r1["lost_acq"] == pytest.approx(0.125) and r1["lost_last"] == pytest.approx(0.125)
        assert r0["lost_acq"] == 0
        assert r1["out_of_step"] == 0          # two streams: no majority to stray from
        assert [e["what"] for e in st["events"]] == ["gains g2"] and st["named"]

    def test_out_of_step_needs_a_majority(self):
        from choco.waterfalls import _stream_table, _tzinfo
        rec = _diag_line(1, stream_masked=[[1] * 4, [1] * 4, [1, 0, 1, 1]])
        rec["n_streams"] = 3
        rec["missing"].append([0] * 4)
        rows = _stream_table([rec], {}, _tzinfo("UTC"))["rows"]
        assert [r["out_of_step"] for r in rows] == [0, 0, 1] and rows[2]["last_out"]

    def test_mask_columns_row_for_row(self, sh_store):
        from choco.waterfalls import mask_columns
        # a full image: a row per bin, matched by time (times.bin covers rows*2 scanlines)
        idx = sh_store.index("subset", SH_ACQ)
        times = [T0_NS + (400 * f + 100 * b) * 10**9 for f in range(3) for b in range(4)]
        (sh_store.acquisition_dir("subset", SH_ACQ) / "times.bin").write_bytes(
            struct.pack("<%dq" % len(times), *times))
        cols = mask_columns(sh_store, "subset", SH_ACQ, "full", 5, 5, 12)
        assert [c["title"] for c in cols] == ["B01Y masked", "RFI mask off", "channels lost"]
        assert cols[0]["runs"] == [{"y0": 10, "y1": 12, "v": 0.5}]       # some streams
        assert cols[1]["runs"][0]["y0"] == 1 and cols[1]["runs"][0]["v"] == 0.5
        cols = mask_columns(sh_store, "subset", SH_ACQ, "full", 0, 5, 12)
        assert cols[0]["runs"] == [{"y0": 0, "y1": 12, "v": 1.0}] and cols[1]["title"] == "B01Y masked"
        # a per-file image: the share of each file's bins
        cols = mask_columns(sh_store, "subset", SH_ACQ, "coherence", 5, 5, 3)
        assert cols[0]["runs"] == [{"y0": 2, "y1": 3, "v": 0.25}]
        assert idx is not None

    def test_without_a_record_no_columns(self, sh_store, sh_images):
        from choco.waterfalls import mask_columns
        (sh_images / "subset" / SH_ACQ / "diag.jsonl").unlink()
        assert mask_columns(sh_store, "subset", SH_ACQ, "full", 0, 0, 12) is None

    def test_page_viewer_and_array_image(self, tmp_path, sh_images):
        app = _app(tmp_path, sh_images)
        with app.test_client() as client:
            login(client)
            body = client.get(f"/files/subset/{SH_ACQ}/triangle").data.decode()
            assert "Array coherence" in body and f"/waterfall/subset/{SH_ACQ}/array/co_array.png" in body
            assert 'id="dg-co"' in body and "X-engine streams" in body and "cx52" in body
            assert 'class="d0"' in body                                      # the cold feed
            resp = client.get(f"/waterfall/subset/{SH_ACQ}/array/co_array.png")
            assert resp.status_code == 200 and resp.data.startswith(b"\x89PNG")
            assert client.get(f"/waterfall/subset/{SH_ACQ}/array/index.json").status_code == 404
            view = client.get(f"/waterfall/subset/{SH_ACQ}/view/co_array.png").data.decode()
            assert "median over all elements" in view and 'class="wfv-masks"' in view
            view = client.get(f"/waterfall/subset/{SH_ACQ}/view/co_e0005.png").data.decode()
            assert "B01Y masked" in view
