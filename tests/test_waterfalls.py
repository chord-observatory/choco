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

import pytest
import yaml

from choco.app import create_app
from choco.auth import save_user, _users
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


@pytest.fixture(autouse=True)
def clear_users():
    _users.clear()
    yield
    _users.clear()


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
            "waterfall": {"images_dir": str(images), "ttl": 5,
                          "state_file": "/var/lib/choco/waterfall/state.json"},
        }))
        loaded = load_config(cfg)
        assert loaded["waterfall"]["images_dir"] == str(images)
        assert loaded["waterfall"]["ttl"] == 5

    def test_missing_waterfall_block_defaults_to_empty(self, tmp_path):
        from choco.app import load_config
        cfg = tmp_path / "config.yaml"
        cfg.write_text(yaml.safe_dump({"configs_dir": str(tmp_path)}))
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
    config["waterfall"] = {"images_dir": str(images), "ttl": 0}
    app = create_app(configs_dir=tmp_path / "configs", config=config)
    app.config["TESTING"] = True
    return app


def _login(client):
    user = save_user("cn=tester,dc=example", "tester")
    with client.session_transaction() as sess:
        sess["_user_id"] = user.get_id()


@pytest.fixture
def client(tmp_path, images):
    app = _app(tmp_path, images)
    with app.test_client() as c:
        yield c


class TestRoutes:
    def test_image_is_served(self, client):
        _login(client)
        resp = client.get(f"/waterfall/subset/{ACQ}/e0000/th_e0000xe0001.png")
        assert resp.status_code == 200
        assert resp.data.startswith(b"\x89PNG")

    def test_image_requires_login(self, client):
        resp = client.get(f"/waterfall/subset/{ACQ}/e0000/th_e0000xe0001.png")
        assert resp.status_code in (302, 401)

    @pytest.mark.parametrize("path", [
        "/waterfall/subset/{acq}/e0000/wf_e0009xe0009.png",   # not written
        "/waterfall/subset/acq_nope/e0000/wf_e0000xe0000.png",
        "/waterfall/subset/{acq}/e0000/index.json",
        "/waterfall/subset/{acq}/nope/wf_e0000xe0000.png",
    ])
    def test_bad_image_requests_are_404(self, client, path):
        _login(client)
        assert client.get(path.format(acq=ACQ)).status_code == 404

    def test_triangle_page_renders(self, client):
        _login(client)
        resp = client.get(f"/files/subset/{ACQ}/triangle")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert ACQ in body
        assert "th_e0000xe0001.png" in body
        assert "6 products rendered" in body
        assert "2 source files folded in" in body

    def test_triangle_page_honours_the_element_filter(self, client):
        _login(client)
        body = client.get(f"/files/subset/{ACQ}/triangle?elements=0,1").data.decode()
        assert "th_e0000xe0001.png" in body
        assert "e0002xe0002" not in body

    def test_triangle_page_requires_login(self, client):
        assert client.get(f"/files/subset/{ACQ}/triangle").status_code in (302, 401)

    def test_triangle_of_an_unknown_acquisition_is_404(self, client):
        _login(client)
        assert client.get("/files/subset/acq_nope/triangle").status_code == 404

    def test_triangle_rejects_a_traversal(self, client):
        _login(client)
        assert client.get("/files/../../etc/triangle").status_code in (301, 404)

    def test_files_table_links_to_the_triangle(self, tmp_path, images):
        """The Waterfalls column matches an acquisition to its images."""
        data = tmp_path / "data" / "subset" / ACQ
        data.mkdir(parents=True)
        (data / "vis_0.h5").write_bytes(b"x")
        (data / "vis_1.h5").write_bytes(b"x")
        app = _app(tmp_path, images)
        with app.test_client() as c:
            _login(c)
            body = c.get("/partials/files").data.decode()
        assert f"/files/subset/{ACQ}/triangle" in body
        assert ">2/2<" in body                      # 2 of 2 files folded in

    def test_files_table_shows_a_dash_when_nothing_is_rendered(self, tmp_path, images):
        data = tmp_path / "data" / "subset" / "acq_unrendered"
        data.mkdir(parents=True)
        (data / "vis_0.h5").write_bytes(b"x")
        app = _app(tmp_path, images)
        with app.test_client() as c:
            _login(c)
            body = c.get("/partials/files").data.decode()
        assert "acq_unrendered" in body
        assert "/files/subset/acq_unrendered/triangle" not in body

    def test_routes_are_inert_without_an_images_dir(self, tmp_path):
        from choco.app import _DEFAULT_CONFIG
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "nodes.yaml").write_text(yaml.safe_dump({"groups": {}}))
        config = dict(_DEFAULT_CONFIG)
        app = create_app(configs_dir=tmp_path / "configs", config=config)
        app.config["TESTING"] = True
        with app.test_client() as c:
            _login(c)
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

    def test_route_streams_and_sets_length(self, client, images):
        _login(client)
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
        config["waterfall"] = {"images_dir": str(images), "ttl": 0}
        app = create_app(configs_dir=tmp_path / "configs", config=config)
        app.config["TESTING"] = True
        with app.test_client() as c:
            _login(c)
            body = c.get("/partials/files").data.decode()
        assert f"/files/cs00/{ACQ}/triangle" in body
        assert f"/files/cs01/{ACQ}/triangle" in body


class TestAxisLabels:
    def test_element_names_label_the_axes(self, client):
        """The dish name belongs on the axis, not only in a tooltip."""
        _login(client)
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
        _login(client)
        resp = client.get(f"/waterfall/subset/{ACQ}/view/wf_e0000xe0001.png")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "A0X" in body and "A1X" in body
        assert "in MHz" in body and "600" in body
        assert "in local time (PDT)" in body
        assert "linear-gradient(to top" in body
        assert f"/waterfall/subset/{ACQ}/e0000/wf_e0000xe0001.png" in body
        assert f"/files/subset/{ACQ}/triangle" in body

    def test_viewer_requires_login(self, client):
        resp = client.get(f"/waterfall/subset/{ACQ}/view/wf_e0000xe0001.png")
        assert resp.status_code in (302, 401)

    @pytest.mark.parametrize("path", [
        "/waterfall/subset/{acq}/view/wf_e0009xe0009.png",    # not rendered
        "/waterfall/subset/{acq}/view/th_e0000xe0001.png",    # not a full image
        "/waterfall/subset/acq_nope/view/wf_e0000xe0001.png",
        "/waterfall/subset/{acq}/view/index.json",
        "/waterfall/subset/{acq}/view/wf_..xe0001.png",
    ])
    def test_bad_viewer_requests_are_404(self, client, path):
        _login(client)
        assert client.get(path.format(acq=ACQ)).status_code == 404

    def test_viewer_degrades_without_side_files(self, tmp_path, images):
        """No freq.npy, times.bin or readable palette — plainer, never a 500."""
        acq = "acq_20260724_000000_000000000"
        make_index(images / "subset" / acq, side_files=False)
        for f in (images / "subset" / acq).rglob("wf_*.png"):
            f.write_bytes(b"\x89PNG\r\n\x1a\nwf")              # no PLTE inside
        app = _app(tmp_path, images)
        with app.test_client() as client:
            _login(client)
            body = client.get(
                f"/waterfall/subset/{acq}/view/wf_e0000xe0001.png").data.decode()
        assert "in channel index" in body
        assert "in scanline number" in body
        assert "linear-gradient" not in body

    def test_triangle_links_to_the_viewer(self, client):
        _login(client)
        body = client.get(f"/files/subset/{ACQ}/triangle").data.decode()
        assert f"/waterfall/subset/{ACQ}/view/wf_e0000xe0001.png" in body
