"""The dark theme has to cover every colour kotekan's graph can carry.

choco's dark mode is a *mirror* of kotekan's palette: graphviz writes colours
into the SVG as presentation attributes, and ``pipeline.html`` restyles them
with one attribute selector per value.  Nothing links the two, so a palette
change on the kotekan side silently leaves light-painted nodes on the dark
page -- which is how it broke the first time.

This renders the checked-in sample graph and fails naming any colour that has
no dark-mode rule.
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "data" / "pipeline_palette.dot"
TEMPLATE = Path(__file__).parent.parent / "choco" / "templates" / "pipeline.html"

# Painted by graphviz itself rather than by kotekan, and already handled:
# the canvas polygon (its own rule turns it transparent) and "none", which
# paints nothing.
NOT_KOTEKAN_S = {"none", "transparent"}
NOT_KOTEKAN_F = {"none", "white", "transparent"}


def _render() -> str:
    if shutil.which("dot") is None:
        pytest.skip("graphviz 'dot' not installed")
    result = subprocess.run(
        ["dot", "-Tsvg"], input=FIXTURE.read_text(),
        capture_output=True, text=True, encoding="utf-8", timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def _dark_rules(css: str) -> tuple[set[str], set[str]]:
    """Colours the dark theme restyles, as (fills, strokes)."""
    dark = [line for line in css.splitlines() if '[data-theme="dark"]' in line]
    joined = "\n".join(dark)
    fills = set(re.findall(r'\[fill="([^"]+)"\]', joined))
    strokes = set(re.findall(r'\[stroke="([^"]+)"\]', joined))
    return fills, strokes


def test_every_painted_colour_has_a_dark_rule():
    svg = _render()
    css = TEMPLATE.read_text()
    mapped_fills, mapped_strokes = _dark_rules(css)

    painted_fills = set(re.findall(r'fill="([^"]+)"', svg)) - NOT_KOTEKAN_F
    painted_strokes = set(re.findall(r'stroke="([^"]+)"', svg)) - NOT_KOTEKAN_S

    assert not (painted_fills - mapped_fills), (
        "dark mode has no rule for these fills: "
        f"{sorted(painted_fills - mapped_fills)} — add them to pipeline.html "
        "(they will render light-on-dark otherwise)"
    )
    assert not (painted_strokes - mapped_strokes), (
        "dark mode has no rule for these strokes: "
        f"{sorted(painted_strokes - mapped_strokes)} — add them to pipeline.html"
    )


def test_fixture_exercises_the_whole_palette():
    # A fixture that lost a category would make the test above pass by
    # covering less, so pin what it has to contain.
    svg = _render()
    for role, colour in (
        ("buffer fill", "#d6e6f7"), ("compute fill", "#f7edc0"),
        ("gpu fill", "#fbdcc0"), ("io fill", "#e6dcf5"),
        ("memory fill", "#fbd9ea"), ("endpoint fill", "#d3ece4"),
        ("device region fill", "#fdf4ea"), ("ink", "#1c2430"),
        ("edge", "#5c6b7a"), ("cluster line", "#aab4bf"),
        ("backed-up buffer", "#c0392b"),
    ):
        assert colour in svg, f"fixture no longer exercises {role} ({colour})"


def test_rounded_nodes_are_paths_not_polygons():
    # kotekan draws buffers and stages as *rounded* boxes, which graphviz
    # emits as <path>.  The clickable-buffer rules matched only
    # polygon/ellipse for a while and so marked nothing at all.
    svg = _render()
    assert "<path" in svg
    css = TEMPLATE.read_text()
    for rule in ("clickable-buffer path", "clickable-buffer:hover path"):
        assert rule in css, f"missing '{rule}' — rounded nodes would go unmarked"
