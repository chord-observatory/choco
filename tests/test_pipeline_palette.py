"""The dark theme has to cover every colour kotekan's graph can carry.

choco's dark mode is a *mirror* of kotekan's palette: graphviz writes colours
into the SVG as presentation attributes, and ``pipeline.html`` restyles them
with one attribute selector per value.  Nothing links the two, so a palette
change on the kotekan side silently leaves light-painted nodes on the dark
page -- which is how it broke the first time.

Two palettes are checked, because kotekan rolls out to the cluster on its own
schedule and choco meets a mixed fleet until it has: the current one, and the
one served before kotekan's colours were named.
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

DATA = Path(__file__).parent / "data"
FIXTURES = {
    "current": DATA / "pipeline_palette.dot",
    "legacy": DATA / "pipeline_palette_legacy.dot",
}
TEMPLATE = Path(__file__).parent.parent / "choco" / "templates" / "pipeline.html"

# Painted by graphviz itself rather than by kotekan, and already handled: the
# canvas polygon (its own rule turns it transparent) and "none", which paints
# nothing.  Label text is exempt because the dark theme selects it by element
# (`svg text`) -- the older kotekan set no fontcolor, so there is no colour on
# the text for an attribute selector to match.
NOT_MAPPED_BY_COLOUR_S = {"none", "transparent"}
NOT_MAPPED_BY_COLOUR_F = {"none", "white", "transparent"}


def _render(fixture: Path) -> str:
    if shutil.which("dot") is None:
        pytest.skip("graphviz 'dot' not installed")
    result = subprocess.run(
        ["dot", "-Tsvg"], input=fixture.read_text(),
        capture_output=True, text=True, encoding="utf-8", timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def _painted(svg: str) -> tuple[set[str], set[str]]:
    """Colours in the SVG, minus the ones the theme handles by element."""
    text_free = re.sub(r"<text\b[^>]*>", "", svg)
    fills = set(re.findall(r'fill="([^"]+)"', text_free)) - NOT_MAPPED_BY_COLOUR_F
    strokes = set(re.findall(r'stroke="([^"]+)"', text_free)) - NOT_MAPPED_BY_COLOUR_S
    return fills, strokes


def _dark_rules() -> tuple[set[str], set[str]]:
    """Colours the dark theme restyles, as (fills, strokes)."""
    dark = "\n".join(line for line in TEMPLATE.read_text().splitlines()
                     if '[data-theme="dark"]' in line)
    return (set(re.findall(r'\[fill="([^"]+)"\]', dark)),
            set(re.findall(r'\[stroke="([^"]+)"\]', dark)))


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_every_painted_colour_has_a_dark_rule(name):
    painted_fills, painted_strokes = _painted(_render(FIXTURES[name]))
    mapped_fills, mapped_strokes = _dark_rules()
    assert not (painted_fills - mapped_fills), (
        f"[{name} palette] dark mode has no rule for these fills: "
        f"{sorted(painted_fills - mapped_fills)} — add them to pipeline.html "
        "(they will render light-on-dark otherwise)"
    )
    assert not (painted_strokes - mapped_strokes), (
        f"[{name} palette] dark mode has no rule for these strokes: "
        f"{sorted(painted_strokes - mapped_strokes)} — add them to pipeline.html"
    )


def test_label_text_is_recoloured_by_element():
    # The older kotekan sets no fontcolor, so its labels carry no fill at all
    # and no attribute selector can reach them; they were left black on the
    # dark page.  Selecting the element covers both kotekans, since a CSS
    # property outranks a presentation attribute.
    svg = _render(FIXTURES["legacy"])
    assert re.search(r"<text\b(?![^>]*fill=)", svg), \
        "fixture no longer represents a kotekan that omits fontcolor"
    assert 'svg text { fill:' in TEMPLATE.read_text().replace("  ", " "), \
        "dark mode must recolour label text by element, not by colour"


def test_fixture_exercises_the_whole_palette():
    # A fixture that lost a category would make the checks above pass by
    # covering less, so pin what each has to contain.
    current = _render(FIXTURES["current"])
    for role, colour in (
        ("buffer fill", "#d6e6f7"), ("compute fill", "#f7edc0"),
        ("gpu fill", "#fbdcc0"), ("io fill", "#e6dcf5"),
        ("memory fill", "#fbd9ea"), ("endpoint fill", "#d3ece4"),
        ("device region fill", "#fdf4ea"), ("ink", "#1c2430"),
        ("edge", "#5c6b7a"), ("cluster line", "#aab4bf"),
        ("backed-up buffer", "#c0392b"),
    ):
        assert colour in current, f"fixture no longer exercises {role} ({colour})"

    legacy = _render(FIXTURES["legacy"])
    for role, colour in (
        ("buffer fill", "#dce9f7"), ("compute fill", "#fbf3c9"),
        ("gpu fill", "#fde0c2"), ("io fill", "#e9ddf5"),
        ("memory fill", "#ffe9f3"), ("endpoint fill", "#cfe6f5"),
        ("device region fill", "#f0f0f0"), ("backed-up buffer", "#c0392b"),
    ):
        assert colour in legacy, f"legacy fixture no longer exercises {role}"


def test_rounded_nodes_are_paths_not_polygons():
    # kotekan draws buffers and stages as *rounded* boxes, which graphviz
    # emits as <path>.  The clickable-buffer rules matched only
    # polygon/ellipse for a while and so marked nothing at all.  True of both
    # kotekan versions -- the shapes did not change with the palette.
    for fixture in FIXTURES.values():
        assert "<path" in _render(fixture)
    css = TEMPLATE.read_text()
    for rule in ("clickable-buffer path", "clickable-buffer:hover path"):
        assert rule in css, f"missing '{rule}' — rounded nodes would go unmarked"
