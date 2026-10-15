"""Node pages that read kotekan: the pipeline graph, the buffer plot and
the frame-peek API."""

from unittest.mock import PropertyMock, patch

from choco.state import Node

from tests.helpers import login


class TestPipelinePage:
    """Full-page interactive pipeline view (/pipeline/<key>)."""

    DOT = 'digraph pipeline {\n"n2_buffer" -> "stage_x";\n}'
    # Graphviz-shaped SVG: one held buffer, one without peek_hold.
    SVG = ('<svg xmlns="http://www.w3.org/2000/svg" width="10pt" height="10pt" '
           'viewBox="0 0 10 10"><g class="graph">'
           '<g id="node1" class="node"><title>n2_buffer</title>'
           '<polygon fill="none" stroke="black" points="0,0 5,5"/></g>'
           '<g id="node2" class="node"><title>host_voltage_buffer_0</title>'
           '<polygon fill="none" stroke="black" points="0,0 5,5"/></g>'
           '</g></svg>')
    BUFFERS = {
        "n2_buffer": {"num_full_frame": 1, "peek_hold": True},
        "host_voltage_buffer_0": {"num_full_frame": 0},
    }

    def test_unknown_node_redirects(self, client):
        login(client)
        resp = client.get("/pipeline/cx/nope", follow_redirects=False)
        assert resp.status_code == 302

    def test_page_structure(self, client):
        login(client)
        body = client.get("/pipeline/cx/cx1").data.decode()
        # Standalone full-viewport page with slim nav, not base.html.
        assert 'id="pipeline-graph"' in body
        assert "/partials/node-pipeline-svg/cx/cx1" in body
        assert 'class="brand"' in body and 'href="/"' in body
        assert "/nodes/edit/cx/cx1" in body
        # The status page is gone; nothing may still link to it.
        assert "/status/cx/cx1" not in body
        # Plot popup overlay + layout preset selector + theme toggle.
        assert 'id="buffer-plot"' in body
        assert 'data-node-key="cx/cx1"' in body
        assert 'name="layout"' in body
        # curves is the default layout (first option in the selector).
        assert body.index('value="curves"') < body.index('value="ortho"')
        assert 'id="pg-theme"' in body
        assert 'data-theme="dark"' in body  # dark is the default
        assert "bufferplot.js" in body and "pipeline.js" in body

    def test_page_layout_selection_round_trips(self, client):
        login(client)
        # ?layout= preselects a preset, so a refresh or a shared link
        # comes back with the same routing...
        body = client.get("/pipeline/cx/cx1?layout=ortho").data.decode()
        assert '<option value="ortho" selected>' in body
        assert '<option value="curves">' in body
        # ...and an unknown value falls back to curves, never reaching dot.
        body = client.get("/pipeline/cx/cx1?layout=../evil").data.decode()
        assert '<option value="curves" selected>' in body
        assert "evil" not in body
        # The initial graph fetch includes the selector, so a browser
        # restoring a select value across a reload can't disagree with
        # what gets rendered.
        assert 'hx-include="#pg-layout"' in body

    def test_partial_layout_presets_are_allowlisted(self, client):
        from choco.pipeline_svg import PIPELINE_LAYOUTS
        login(client)
        for requested, expected in (
            ("curves", PIPELINE_LAYOUTS["curves"]),
            ("ortho", PIPELINE_LAYOUTS["ortho"]),
            ("../evil", PIPELINE_LAYOUTS["curves"]),  # unknown -> default
        ):
            with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
                 patch.object(Node, "get_buffers", return_value=self.BUFFERS), \
                 patch("choco.web.render_dot_svg",
                       return_value=self.SVG) as rds:
                resp = client.get(
                    f"/partials/node-pipeline-svg/cx/cx1?layout={requested}")
            assert resp.status_code == 200
            rds.assert_called_once_with(self.DOT, layout_args=expected)

    def test_partial_stamps_only_held_buffers(self, client):
        login(client)
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch.object(Node, "get_buffers", return_value=self.BUFFERS), \
             patch("choco.web.render_dot_svg", return_value=self.SVG):
            resp = client.get("/partials/node-pipeline-svg/cx/cx1")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert 'data-plot-buffer="n2_buffer"' in body
        assert 'data-plot-node="cx/cx1"' in body
        assert 'data-plot-buffer="host_voltage_buffer_0"' not in body
        # Inline SVG, not the base64 <img> of the status page.
        assert "<svg" in body and "data:image/svg+xml" not in body

    def test_partial_notes_when_nothing_is_clickable(self, client):
        login(client)
        # A failed /buffers read: the graph renders, but nothing could be
        # marked — say so rather than silently showing an amber-less graph.
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch.object(Node, "get_buffers", return_value=None), \
             patch("choco.web.render_dot_svg", return_value=self.SVG):
            body = client.get("/partials/node-pipeline-svg/cx/cx1").data.decode()
        assert "buffer list unavailable" in body
        assert "data-plot-buffer" not in body
        # A reachable kotekan with no peek_hold buffers is a different
        # (also worth stating) case.
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch.object(Node, "get_buffers",
                          return_value={"host_voltage_buffer_0": {"num_full_frame": 0}}), \
             patch("choco.web.render_dot_svg", return_value=self.SVG):
            body = client.get("/partials/node-pipeline-svg/cx/cx1").data.decode()
        assert "no <code>peek_hold</code> buffers" in body
        # ...and neither note appears when buffers are clickable.
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch.object(Node, "get_buffers", return_value=self.BUFFERS), \
             patch("choco.web.render_dot_svg", return_value=self.SVG):
            body = client.get("/partials/node-pipeline-svg/cx/cx1").data.decode()
        assert "unavailable" not in body and "nothing is clickable" not in body

    def test_partial_falls_back_to_dot_text(self, client):
        login(client)
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch("choco.web.render_dot_svg", return_value=None):
            resp = client.get("/partials/node-pipeline-svg/cx/cx1")
        body = resp.data.decode()
        assert "graphviz" in body
        assert "&#34;n2_buffer&#34;" in body  # escaped dot text

    def test_partial_unreachable(self, client):
        login(client)
        with patch.object(Node, "get_pipeline_dot", return_value=None):
            resp = client.get("/partials/node-pipeline-svg/cx/cx1")
        assert b"unreachable" in resp.data

    def test_partial_unknown_node_404(self, client):
        login(client)
        assert client.get("/partials/node-pipeline-svg/cx/nope").status_code == 404


class TestPlotPage:
    """Full-viewport single-buffer plot (/plot/<key>?buffer=)."""

    def test_unknown_node_redirects(self, client):
        login(client)
        resp = client.get("/plot/cx/nope?buffer=n2_buffer", follow_redirects=False)
        assert resp.status_code == 302

    def test_page_structure(self, client):
        login(client)
        body = client.get("/plot/cx/cx1?buffer=n2_buffer").data.decode()
        # Standalone page: the same container bufferplot.js renders into
        # on the pipeline page, plus the attributes that name the source
        # and make it open full screen instead of waiting for a click.
        assert 'id="buffer-plot"' in body
        assert 'data-source-url="/api/node-buffer-data/cx/cx1?buffer=n2_buffer"' in body
        assert 'data-source-id="cx/cx1|n2_buffer"' in body
        assert 'data-fullscreen="1"' in body
        assert "bufferplot.js" in body
        # Slim nav, no base.html chrome, dark by default.
        assert 'class="brand"' in body
        assert "/pipeline/cx/cx1" in body and "/nodes/edit/cx/cx1" in body
        assert 'data-theme="dark"' in body
        # The view lives in the fragment, so the server renders nothing
        # for it — no query-string plumbing to get wrong.
        assert "pipeline.js" not in body

    def test_bad_buffer_name_rejected(self, client):
        login(client)
        # Same allowlist as the data API: a name that could break out of
        # an attribute never reaches the template.
        for name in ['"><script>', "../etc", "a b", ""]:
            resp = client.get(
                "/plot/cx/cx1", query_string={"buffer": name},
                follow_redirects=False,
            )
            assert resp.status_code == 302, name
            assert "/pipeline/cx/cx1" in resp.headers["Location"]

    def test_fragment_is_not_the_server_s_business(self, client):
        login(client)
        # A fragment never reaches the server at all, but the route must
        # not care if one is somehow passed through as a query either.
        resp = client.get("/plot/cx/cx1?buffer=n2_buffer&dims=F:x,E:y&zoom=1:9")
        assert resp.status_code == 200


class TestNodeBufferDataApi:
    """The frame-data proxy behind the live buffer plots.

    Requests come from 127.0.0.1 so the localhost bypass applies (no
    login needed), same as the other /api/ routes.
    """

    RAW = bytes(range(16))

    @classmethod
    def frame(cls, **extra):
        import base64
        f = {
            "buffer": "n2_buffer", "frame_id": 7, "frame_size": 100756,
            "data_length": len(cls.RAW),
            "data": base64.b64encode(cls.RAW).decode("ascii"),
            "encoding": "base64",
            "metadata": {"fpga_seq_start": 12345},
            "frame_desc": {"frame_desc_type": "ndarray",
                           "value_type": "int32", "extents": [4, 2]},
        }
        f.update(extra)
        return f

    def test_unknown_node_404(self, client):
        resp = client.get("/api/node-buffer-data/cx/nope?buffer=b")
        assert resp.status_code == 404

    def test_bad_buffer_name_400(self, client):
        for bad in ("", "a/b", "a b", "a%2Fb/../kill"):
            resp = client.get(f"/api/node-buffer-data/cx/cx1?buffer={bad}")
            assert resp.status_code == 400, bad

    def test_bad_len_400(self, client):
        for bad in ("abc", "-1", "1.5"):
            resp = client.get(
                f"/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len={bad}")
            assert resp.status_code == 400, bad

    def test_len_defaults_and_clamps(self, client):
        with patch.object(Node, "get_buffer_frame",
                          return_value=self.frame()) as gbf:
            client.get("/api/node-buffer-data/cx/cx1?buffer=n2_buffer")
        gbf.assert_called_once_with("n2_buffer", length=4 * 1024 * 1024)
        with patch.object(Node, "get_buffer_frame",
                          return_value=self.frame()) as gbf:
            client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len=999999999")
        gbf.assert_called_once_with("n2_buffer", length=32 * 1024 * 1024)

    def test_len_zero_returns_descriptor_json(self, client):
        frame = self.frame()
        del frame["data"], frame["encoding"]
        with patch.object(Node, "get_buffer_frame",
                          return_value=frame) as gbf:
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len=0")
        gbf.assert_called_once_with("n2_buffer", length=0)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["frame_desc"]["value_type"] == "int32"
        assert data["frame_id"] == 7

    DISH_TABLE = [{"dish_idx": 0, "label": "B4", "type": "ArrayDish"},
                  {"dish_idx": 1, "label": "Missing", "type": "Missing"},
                  {"dish_idx": 2, "label": "A1", "type": "ArrayDish"},
                  {"dish_idx": 3, "label": "RFIA1", "type": "RFIDish"}]

    def _desc_reply(self, client, desc, config):
        frame = self.frame(frame_desc=desc)
        del frame["data"], frame["encoding"]
        with patch.object(Node, "get_buffer_frame", return_value=frame), \
                patch.object(Node, "desired_config", new_callable=PropertyMock,
                             return_value=config):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_subset_buffer&len=0")
        assert resp.status_code == 200
        return resp.get_json()["frame_desc"]

    def test_dish_inputs_descriptor_gets_the_implicit_input_list(self, client):
        # kotekan develop sends a compact DishInputs descriptor with only
        # the element count; the identities are implied by the dish table
        # choco pushed, so the reply spells them out for the plotter.
        desc = {"frame_desc_type": "N2", "n2_layout": "DishInputs",
                "num_elements": 6, "num_ev": 0}
        config = {"telescope": {"num_polarizations": 2,
                                "dish_inputs": self.DISH_TABLE}}
        out = self._desc_reply(client, desc, config)
        assert out["input_list"] == [0, 2, 3, 4, 6, 7]
        assert out["num_elements"] == 6

    def test_implicit_input_list_needs_the_count_to_agree(self, client):
        # A table that does not reproduce num_elements means the rule has
        # drifted: better no identities than wrong ones.
        desc = {"frame_desc_type": "N2", "n2_layout": "DishInputs",
                "num_elements": 48, "num_ev": 0}
        config = {"telescope": {"dish_inputs": self.DISH_TABLE}}
        assert "input_list" not in self._desc_reply(client, desc, config)
        # No table at all (config failed to load, or no telescope block).
        assert "input_list" not in self._desc_reply(client, desc, None)
        assert "input_list" not in self._desc_reply(client, desc, {"a": 1})

    def test_explicit_wire_forms_are_left_alone(self, client):
        config = {"telescope": {"dish_inputs": self.DISH_TABLE}}
        # The kv-branch build already names its elements.
        desc = {"frame_desc_type": "N2", "n2_layout": "DishInputs",
                "num_elements": 6, "num_ev": 0, "input_list": [1, 2, 3, 4, 5, 6]}
        assert self._desc_reply(client, desc, config)["input_list"] == \
            [1, 2, 3, 4, 5, 6]
        # Dense layouts and non-N2 descriptors carry no list.
        desc = {"frame_desc_type": "N2", "n2_layout": "FullUpperTri",
                "num_elements": 6, "num_ev": 0}
        assert "input_list" not in self._desc_reply(client, desc, config)
        desc = {"frame_desc_type": "ndarray", "value_type": "int32",
                "extents": [6]}
        assert "input_list" not in self._desc_reply(client, desc, config)

    def test_data_returned_as_raw_bytes(self, client):
        with patch.object(Node, "get_buffer_frame",
                          return_value=self.frame()):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len=16")
        assert resp.status_code == 200
        assert resp.content_type == "application/octet-stream"
        assert resp.data == self.RAW
        assert resp.headers["X-Frame-Id"] == "7"
        assert resp.headers["X-Frame-Size"] == "100756"

    def test_no_full_frame_404_json(self, client):
        with patch.object(Node, "get_buffer_frame",
                          return_value={"error": "no full frame currently in buffer"}):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer")
        assert resp.status_code == 404
        assert "no full frame" in resp.get_json()["error"]

    def test_unreachable_502(self, client):
        with patch.object(Node, "get_buffer_frame", return_value=None):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer")
        assert resp.status_code == 502

    def test_missing_data_field_502(self, client):
        frame = self.frame()
        del frame["data"], frame["encoding"]
        with patch.object(Node, "get_buffer_frame", return_value=frame):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len=16")
        assert resp.status_code == 502
        assert "base64" in resp.get_json()["error"]
