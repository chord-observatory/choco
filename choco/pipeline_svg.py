"""The pipeline page's graph: kotekan's dot rendered by graphviz, and the
SVG rebuilt through a whitelist before it reaches the DOM.

Rendering shells out to ``dot`` (``choco.hosttool``), with the layout
presets the page offers.  The sanitizer is the one way kotekan-supplied
markup enters choco's authenticated UI (CLAUDE.md): a new tree is built
and only known-inert graphviz output is copied over.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET

from .hosttool import run_tool

logger = logging.getLogger(__name__)


# Layout attributes injected at render time.  kotekan ships its own
# rankdir/nodesep/ranksep/mclimit/newrank now, and a -G on the command line
# silently overrides the graph's own value, so anything set here is a
# deliberate override of kotekan's choice.  mclimit raises the crossing-
# minimisation effort and newrank improves ranking — together they laid
# out the 645-line cx19 pipeline ~11% narrower (fewer edge crossings)
# for well under a second of extra layout time.  Splines (curves) are
# the default routing: ortho was the original pick when default splines
# were spaghetti, but mclimit/newrank tamed that, curves lay out ~10%
# more compact, and the operator prefers them; ortho stays a preset.
_DOT_LAYOUT_ARGS = ("-Gsplines=true", "-Granksep=1.0", "-Gnodesep=0.5",
                    "-Gmclimit=8", "-Gnewrank=true")

# Selectable presets for the full-page pipeline view: edge-routing style
# is a taste call on a graph this size, so the page lets the operator
# flip between them live.  Keys are the allowlist for the ``?layout=``
# query parameter — never pass raw values to the dot command line.
#
# ortho deliberately leaves nodesep to kotekan (0.3).  Widening it aborts
# graphviz 2.43 outright — `chkSgraph: Assertion np->cells[0] failed` in
# the ortho maze router — on a clustered CHORD graph that renders fine at
# kotekan's own value; 0.4 survived and 0.5 did not, so the threshold is
# graph-dependent and the safe move is not to override it at all.  The
# crash is caught by the retry-without-args path, but the operator picks
# "ortho" and silently gets curves after a wasted render.
PIPELINE_LAYOUTS = {
    "curves": _DOT_LAYOUT_ARGS,
    "ortho": ("-Gsplines=ortho", "-Granksep=1.2",
              "-Gmclimit=8", "-Gnewrank=true"),
    "polyline": ("-Gsplines=polyline", "-Granksep=1.0", "-Gnodesep=0.5",
                 "-Gmclimit=8", "-Gnewrank=true"),
}


def render_dot_svg(dot_text: str, timeout: float = 10.0,
                   layout_args: tuple[str, ...] = _DOT_LAYOUT_ARGS) -> str | None:
    """Render graphviz dot text to an SVG document string.

    Shells out to the graphviz CLI (same host-tool-over-Python-dependency
    choice as the openssl TLS fallback), injecting ``_DOT_LAYOUT_ARGS``;
    orthogonal routing is the fussiest graphviz code path, so a failed
    render is retried once without the layout args before giving up.
    Returns None when it can't render at all (no ``dot`` binary on the
    host, render failure, timeout) — the caller falls back to showing the
    raw dot text.  The output is sliced down to the ``<svg`` element: the
    XML prolog and DOCTYPE would be invalid embedded in an HTML page.
    """
    for extra_args in (layout_args, ()):
        result = run_tool(["dot", "-Tsvg", *extra_args], input=dot_text,
                          timeout=timeout, log_level=logging.WARNING)
        if result is None:                  # no dot on the host, or it failed
            continue
        if result.returncode != 0:
            logger.warning(f"dot -Tsvg {' '.join(extra_args)} "
                           f"rc={result.returncode}: "
                           f"{result.stderr.strip()[:200]}")
            continue
        start = result.stdout.find("<svg")
        if start < 0:
            logger.warning("dot -Tsvg produced no <svg> element")
            continue
        return result.stdout[start:]
    return None


# --- Pipeline SVG sanitizer (clickable inline graph) ---
#
# The pipeline page needs the SVG *live* in the DOM to make buffer
# nodes clickable, which means kotekan-supplied markup lands in choco's
# authenticated UI.  It goes through this whitelist RECONSTRUCTION: a
# brand-new tree is built and only
# known-inert graphviz output elements/attributes are copied over —
# <script>, event handlers, <foreignObject>, xlink:href and anything
# else unexpected can't survive because nothing is copied by default.
#
# An element that is not on the list is UNWRAPPED, not deleted: it is left
# out of the output and its children are rebuilt in its place.  That keeps
# deny-by-default exactly as strict — the output can still only ever contain
# listed elements carrying listed attributes, and an unknown wrapper reaches
# the DOM no more than a <script> does — while making the failure mode
# lossless.  Deleting the subtree instead is a silent one: graphviz wraps a
# node's shape and text in <a> as soon as the node carries a URL *or* a
# tooltip, kotekan sets a tooltip on every buffer and every stage, and the
# result was 111 of 223 nodes rendering as empty groups — present in the DOM
# and still clickable, but with nothing drawn.  Whichever element graphviz
# starts emitting next, the worst this can now do is lose its styling.

_SVG_NS = "http://www.w3.org/2000/svg"
# Process-global ET state: makes SVG the *default* namespace on
# serialization (``<svg xmlns=...>`` rather than ``<ns0:svg
# xmlns:ns0=...>``, which browsers would still honour but no CSS
# selector here would match).  This is choco's only ElementTree
# user; a second one serializing a different vocabulary would need its
# own register_namespace call.
ET.register_namespace("", _SVG_NS)

_SVG_ALLOWED = {
    "svg": {"width", "height", "viewBox"},
    "g": {"id", "class", "transform"},
    "title": set(),
    "polygon": {"fill", "stroke", "stroke-width", "stroke-dasharray", "points"},
    "polyline": {"fill", "stroke", "stroke-width", "stroke-dasharray", "points"},
    "ellipse": {"fill", "stroke", "stroke-width", "stroke-dasharray",
                "cx", "cy", "rx", "ry"},
    "circle": {"fill", "stroke", "stroke-width", "stroke-dasharray",
               "cx", "cy", "r"},
    "path": {"fill", "stroke", "stroke-width", "stroke-dasharray", "d"},
    "text": {"text-anchor", "x", "y", "font-family", "font-size", "fill"},
}


def _svg_local(tag_or_attr: str) -> str:
    """Local name of a possibly namespace-qualified tag/attribute."""
    return tag_or_attr.rsplit("}", 1)[-1]


def sanitize_pipeline_svg(svg_text: str, clickable: set[str],
                          node_key: str) -> str | None:
    """Rebuild a graphviz pipeline SVG through the whitelist above.

    Graphviz wraps every dot node in ``<g class="node">`` whose first
    ``<title>`` child is the exact dot node name; groups whose title is
    in ``clickable`` (the peek_hold buffer names) are stamped with
    ``data-plot-buffer``/``data-plot-node`` so bufferplot.js's existing
    delegated click handler opens their live plot — no JS changes — plus
    ``tabindex``/``role``/``aria-label`` so they are reachable by
    keyboard (pipeline.js turns Enter/Space into a click).
    Returns the sanitized SVG markup, or None if the input doesn't
    parse (caller falls back to the raw dot text), or if the root is
    not an ``<svg>`` — the root is the one element that cannot be
    unwrapped, since there would be nothing to put the graph in.
    """
    try:
        root = ET.fromstring(svg_text)
    except ET.ParseError as e:
        logger.warning(f"pipeline SVG parse failed: {e}")
        return None
    if _svg_local(root.tag) != "svg":
        return None

    unwrapped: set[str] = set()

    def rebuild(src: ET.Element, dst_parent: ET.Element) -> None:
        """Copy `src` under `dst_parent` if it is listed; else unwrap it."""
        tag = _svg_local(src.tag)
        allowed = _SVG_ALLOWED.get(tag)
        if allowed is None:
            # Not copied, so it never reaches the DOM — but its children
            # get their own turn under the same rule, so a wrapper cannot
            # take the drawing down with it.  Text is deliberately not
            # carried over: that is what keeps <script> contents out.
            unwrapped.add(tag)
            for child in src:
                rebuild(child, dst_parent)
            return
        attrs = {name: value for raw, value in src.attrib.items()
                 if (name := _svg_local(raw)) in allowed}
        dst = ET.SubElement(dst_parent, f"{{{_SVG_NS}}}{tag}", attrs)
        if tag in ("text", "title"):
            dst.text = src.text
        for child in src:
            rebuild(child, dst)

    root_attrs = {name: value for raw, value in root.attrib.items()
                  if (name := _svg_local(raw)) in _SVG_ALLOWED["svg"]}
    out = ET.Element(f"{{{_SVG_NS}}}svg", root_attrs)
    for child in root:
        rebuild(child, out)
    if unwrapped:
        # Not an error -- graphviz is free to wrap things -- but worth
        # saying, since it means the output lost whatever those elements
        # were styling.
        logger.info("pipeline SVG: unwrapped unlisted elements: "
                    f"{', '.join(sorted(unwrapped))}")

    for group in out.iter(f"{{{_SVG_NS}}}g"):
        if "node" not in group.get("class", "").split():
            continue
        title = group.find(f"{{{_SVG_NS}}}title")
        name = (title.text or "").strip() if title is not None else ""
        if name in clickable:
            group.set("data-plot-buffer", name)
            group.set("data-plot-node", node_key)
            group.set("class", group.get("class", "") + " clickable-buffer")
            group.set("tabindex", "0")
            group.set("role", "button")
            group.set("aria-label", f"plot buffer {name}")
    return ET.tostring(out, encoding="unicode")
