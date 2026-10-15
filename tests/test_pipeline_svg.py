"""choco.pipeline_svg: the dot render and the SVG whitelist."""

from unittest.mock import patch, MagicMock

from choco.pipeline_svg import sanitize_pipeline_svg


def _no_systemctl():
    return patch("choco.hosttool.shutil.which", return_value=None)


def _with_systemctl():
    return patch("choco.hosttool.shutil.which", return_value="/usr/bin/dot")


class TestRenderDotSvg:
    DOT = "digraph pipeline {}\n"
    SVG_DOC = ('<?xml version="1.0" encoding="UTF-8"?>\n'
               '<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"\n'
               ' "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n'
               '<svg xmlns="http://www.w3.org/2000/svg"><g/></svg>\n')

    def _patch_dot(self, stdout: str, returncode: int = 0):
        completed = MagicMock(returncode=returncode, stdout=stdout, stderr="")
        return patch("choco.hosttool.subprocess.run", return_value=completed)

    def test_renders_and_strips_prolog(self):
        from choco.pipeline_svg import render_dot_svg, _DOT_LAYOUT_ARGS
        with _with_systemctl(), self._patch_dot(self.SVG_DOC) as run:
            out = render_dot_svg(self.DOT)
        assert out is not None and out.startswith("<svg")
        assert "DOCTYPE" not in out
        assert run.call_args.kwargs["input"] == self.DOT
        # kotekan's labels carry `×` and `·`: the pipe is UTF-8 by contract,
        # not by whatever locale the systemd unit happens to inherit.
        assert run.call_args.kwargs["encoding"] == "utf-8"
        # First attempt renders with the layout attributes injected.
        assert run.call_count == 1
        for arg in _DOT_LAYOUT_ARGS:
            assert arg in run.call_args.args[0]

    def test_ortho_preset_leaves_nodesep_to_kotekan(self):
        # Widening nodesep aborts graphviz 2.43 in the ortho maze router on a
        # clustered CHORD graph that renders fine at kotekan's own 0.3.  The
        # retry-without-args path hides it: the operator picks ortho and
        # silently gets curves.
        from choco.pipeline_svg import PIPELINE_LAYOUTS
        assert not any(a.startswith("-Gnodesep")
                       for a in PIPELINE_LAYOUTS["ortho"])

    def test_layout_failure_retries_plain(self):
        from choco.pipeline_svg import render_dot_svg, _DOT_LAYOUT_ARGS
        ortho_fail = MagicMock(returncode=1, stdout="", stderr="ortho bug")
        plain_ok = MagicMock(returncode=0, stdout=self.SVG_DOC, stderr="")
        with _with_systemctl(), \
             patch("choco.hosttool.subprocess.run",
                   side_effect=[ortho_fail, plain_ok]) as run:
            out = render_dot_svg(self.DOT)
        assert out is not None and out.startswith("<svg")
        assert run.call_count == 2
        # The retry drops the layout args.
        for arg in _DOT_LAYOUT_ARGS:
            assert arg not in run.call_args_list[1].args[0]

    def test_none_without_dot_binary(self):
        from choco.pipeline_svg import render_dot_svg
        with _no_systemctl():
            assert render_dot_svg(self.DOT) is None

    def test_none_on_render_failure(self):
        from choco.pipeline_svg import render_dot_svg
        with _with_systemctl(), self._patch_dot("", returncode=1):
            assert render_dot_svg(self.DOT) is None

    def test_none_on_svg_less_output(self):
        from choco.pipeline_svg import render_dot_svg
        with _with_systemctl(), self._patch_dot("not svg at all"):
            assert render_dot_svg(self.DOT) is None

    def test_none_on_timeout(self):
        from choco.pipeline_svg import render_dot_svg
        import subprocess as sp
        with _with_systemctl(), \
             patch("choco.hosttool.subprocess.run",
                   side_effect=sp.TimeoutExpired(["dot"], 10)):
            assert render_dot_svg(self.DOT) is None


class TestSanitizePipelineSvg:
    """Whitelist reconstruction of the graphviz pipeline SVG."""

    SVG = """<svg xmlns="http://www.w3.org/2000/svg"
         xmlns:xlink="http://www.w3.org/1999/xlink"
         width="100pt" height="50pt" viewBox="0 0 100 50">
      <g id="graph0" class="graph" transform="translate(4 46)">
        <title>pipeline</title>
        <polygon fill="white" stroke="none" points="-4,4 96,4"/>
        <g id="node1" class="node">
          <title>n2_buffer</title>
          <polygon fill="none" stroke="black" points="0,0 50,20"/>
          <text text-anchor="middle" x="25" y="10" font-size="14.00">n2_buffer</text>
        </g>
        <g id="node2" class="node">
          <title>other_buf</title>
          <ellipse fill="none" stroke="black" cx="10" cy="10" rx="5" ry="5"/>
        </g>
        <script>alert(1)</script>
        <foreignObject><div>html!</div></foreignObject>
        <g id="edge1" class="edge" onclick="alert(2)">
          <title>a-&gt;b</title>
          <path fill="none" stroke="black" d="M0,0 L10,10"/>
        </g>
        <a xlink:href="javascript:alert(3)"><text x="1" y="1">link</text></a>
      </g>
    </svg>"""

    def test_active_content_cannot_survive(self):
        out = sanitize_pipeline_svg(self.SVG, {"n2_buffer"}, "cx/cx1")
        assert out is not None
        # Unlisted elements are unwrapped rather than copied, so none of
        # these reach the DOM -- and neither does anything that would make
        # one do something.
        for banned in ("script", "alert", "foreignObject", "onclick",
                       "xlink", "javascript", "href", "<div", "<a"):
            assert banned not in out, banned
        # The drawing itself survives.
        assert "<polygon" in out and "<path" in out
        assert "n2_buffer" in out

    def test_unlisted_wrappers_are_unwrapped_not_deleted(self):
        # The general rule, and the one that matters: an element nobody
        # listed must not be able to take the drawing down with it.  This is
        # how the graph went blank -- graphviz wraps a node's shape and text
        # in <a> as soon as it has a URL *or* a tooltip, and deleting the
        # subtree left 111 of 223 nodes as empty groups: still in the DOM,
        # still clickable, nothing drawn.
        svg = """<svg xmlns="http://www.w3.org/2000/svg"
             width="100pt" height="50pt" viewBox="0 0 100 50">
          <g id="graph0" class="graph">
            <someNewGraphvizWrapper enabled="yes">
              <polygon fill="none" stroke="black" points="0,0 50,20"/>
              <text x="25" y="10">still here</text>
            </someNewGraphvizWrapper>
          </g>
        </svg>"""
        out = sanitize_pipeline_svg(svg, set(), "cx/cx1")
        assert out is not None
        assert "someNewGraphvizWrapper" not in out and "enabled" not in out
        assert "<polygon" in out and ">still here<" in out

    def test_unwrapping_does_not_carry_over_text(self):
        # Unwrapping keeps child *elements*, never the unlisted element's own
        # text -- that is what keeps a <script> body out of the page.
        svg = """<svg xmlns="http://www.w3.org/2000/svg"
             width="10pt" height="10pt" viewBox="0 0 10 10">
          <g id="graph0" class="graph">
            <script>var pwned = 1;</script>
            <style>* { display: none }</style>
            <foreignObject><div onclick="x()">html!</div></foreignObject>
          </g>
        </svg>"""
        out = sanitize_pipeline_svg(svg, set(), "cx/cx1")
        assert out is not None
        for banned in ("pwned", "display: none", "html!", "onclick",
                       "script", "style", "foreignObject", "<div"):
            assert banned not in out, banned

    def test_link_wrapped_nodes_keep_their_drawing(self):
        # The concrete case of the rule above, in the exact shape graphviz
        # emits it.
        svg = """<svg xmlns="http://www.w3.org/2000/svg"
             xmlns:xlink="http://www.w3.org/1999/xlink"
             width="100pt" height="50pt" viewBox="0 0 100 50">
          <g id="graph0" class="graph">
            <g id="node1" class="node">
              <title>n2_buffer</title>
              <g id="a_node1"><a xlink:href="/buffer_frame?name=n2_buffer"
                                 xlink:title="n2_buffer (ndarray buffer)">
                <polygon fill="none" stroke="black" points="0,0 50,20"/>
                <text x="25" y="10">n2_buffer</text>
              </a></g>
            </g>
          </g>
        </svg>"""
        out = sanitize_pipeline_svg(svg, {"n2_buffer"}, "cx/cx1")
        assert out is not None
        # Shape and label survive; the link and its tooltip do not.
        assert "<polygon" in out
        assert ">n2_buffer<" in out
        assert "href" not in out and "xlink" not in out
        assert "/buffer_frame" not in out
        # ...and the node is still the clickable one.
        assert 'data-plot-buffer="n2_buffer"' in out

    def test_clickable_buffers_are_stamped(self):
        out = sanitize_pipeline_svg(self.SVG, {"n2_buffer"}, "cx/cx1")
        assert 'data-plot-buffer="n2_buffer"' in out
        assert 'data-plot-node="cx/cx1"' in out
        assert "clickable-buffer" in out
        # other_buf has no peek_hold: present but not clickable.
        assert 'data-plot-buffer="other_buf"' not in out

    def test_clickable_buffers_are_keyboard_reachable(self):
        # Stamped groups double as buttons: pipeline.js turns Enter/Space
        # on a focused group into the click bufferplot.js listens for.
        out = sanitize_pipeline_svg(self.SVG, {"n2_buffer"}, "cx/cx1")
        assert 'tabindex="0"' in out
        assert 'role="button"' in out
        assert 'aria-label="plot buffer n2_buffer"' in out
        # Non-clickable nodes stay out of the tab order.
        assert out.count('tabindex="0"') == 1

    def test_edge_groups_not_stamped(self):
        out = sanitize_pipeline_svg(self.SVG, {"a->b"}, "cx/cx1")
        assert "data-plot-buffer" not in out
        assert "tabindex" not in out

    def test_unparseable_input_is_none(self):
        assert sanitize_pipeline_svg("<svg", set(), "cx/cx1") is None
        assert sanitize_pipeline_svg("not xml at all", set(), "cx/cx1") is None

    def test_non_svg_root_is_none(self):
        assert sanitize_pipeline_svg("<html>x</html>", set(), "cx/cx1") is None
