"""The menu: its groups, the current page's item and crumb, the partial
that fills the badges, and the pages the power page split into."""

import re
import pytest

from choco.web import MENU, menu_active
from tests.helpers import login


class TestActiveItem:
    @pytest.mark.parametrize("path, group, href", [
        ("/", None, None),
        ("/nodes", "Instrument", "/nodes"),
        ("/nodes/edit", "Config", "/nodes/edit"),
        ("/nodes/edit/cx/cx1", "Instrument", "/nodes"),
        ("/pipeline/cx/cx1", "Instrument", "/nodes"),
        ("/fpga", "Instrument", "/fpga"),
        ("/service/fpga", "Instrument", "/fpga"),
        ("/power/analog", "Power", "/power/analog"),
        ("/service/power", "Power", "/power/analog"),
        ("/power/rack", "Power", "/power/rack"),
        ("/transits", "Positioning", "/transits"),
        ("/service/skymap", "Data", "/service/mapmaker"),
        ("/waterfall/subset/acq_x/view/a.png", "Data", "/files"),
        ("/configs/edit/cx/cx1.yaml", "Config", "/configs"),
        ("/login", None, None),
    ])
    def test_the_longest_match_wins(self, path, group, href):
        assert menu_active(path) == (group, href)

    def test_every_item_is_current_for_its_own_href(self):
        for group, items in MENU:
            for _label, href, _badge, _also in items:
                assert menu_active(href) == (group, href)


class TestMenu:
    def test_every_page_carries_the_groups_and_marks_itself(self, client, jobs):
        login(client)
        body = client.get("/power/rack").data.decode()
        assert re.findall(r'class="menu-group-title">([^<]*)<', body) == [
            "Instrument", "Power", "Positioning", "Calibration", "Data", "Config"]
        assert '<a href="/power/rack" class="tag quiet plain active">' in body
        assert '<span class="crumb">Power</span>' in body
        # the first paint has no health: every item is a plain link
        assert 'aria-label="X-engine' not in body
        # the registry editor styles its own .group; the menu's classes are its own
        assert 'class="group"' not in body
        assert 'id="menu-items"' in body and 'hx-get="/partials/menu"' in body

    def test_the_foot_has_who_is_here_logout_and_the_theme(self, client):
        login(client)
        body = client.get("/").data.decode()
        foot = body[body.index('class="menu-foot"'):body.index("</aside>")]
        assert 'id="presence"' in foot and 'class="logout-icon"' in foot and 'id="theme-toggle"' in foot
        # the small-screen button sits in the head, beside the mark
        head = body[body.index('class="menu-head"'):body.index('id="menu-items"')]
        assert 'id="menu-toggle"' in head and 'aria-controls="menu-items"' in head

    def test_the_three_bar_button_shows_only_on_a_small_screen(self, app):
        """``.icon-btn`` sets display after the menu's rules, so the hidden
        state needs the higher specificity to hold on a monitor."""
        import re as _re
        css = (app.root_path and open(app.root_path + "/static/choco.css").read())
        wide = css[:css.index("@media (max-width: 1023.98px)")]
        narrow = css[css.index("@media (max-width: 1023.98px)"):]
        assert _re.search(r"\.menu-head \.menu-toggle \{ display: none; \}", wide)
        assert _re.search(r"\.menu-head \.menu-toggle \{ display: inline-flex; \}", narrow)

    def test_the_partial_fills_the_badges_and_marks_the_page(self, client, jobs):
        login(client)
        body = client.get("/partials/menu", headers={"HX-Current-URL": "https://x/service/eop"}).data.decode()
        assert 'aria-label="X-engine' in body and 'aria-label="Earth orientation: ok"' in body
        eop = re.search(r'<a href="/service/eop" class="tag ([^"]*)"', body)
        assert eop and "active" in eop.group(1)
        assert body.count(" active") == 1
        # pages with nothing behind them stay plain links
        assert '<a href="/configs" class="tag quiet plain">' in body
        assert '<a href="/service/choco" class="tag quiet plain">' in body

    def test_the_partial_strips_the_proxys_prefix(self, client, jobs):
        login(client)
        body = client.get("/partials/menu", headers={"HX-Current-URL": "https://x/choco/nodes"},
                          base_url="http://localhost/choco").data.decode()
        assert '<a href="/choco/nodes" class="tag tag-off quiet active"' in body

    def test_the_old_strip_route_is_gone(self, client, jobs):
        login(client)
        assert client.get("/partials/services").status_code == 404


class TestPowerPages:
    def test_the_analog_page_has_the_pdb_the_supply_and_the_map(self, client, jobs):
        login(client)
        body = client.get("/power/analog").data.decode()
        assert "Analog power" in body and 'href="/power/rack"' in body
        assert "<h3>PDB</h3>" in body and "Dish supply" in body and "instrument.prometheus_url" in body
        assert "Channel map" in body and "Not cross-checked" in body
        assert "PDU outlets" not in body

    def test_the_rack_page(self, client, jobs):
        login(client)
        body = client.get("/power/rack").data.decode()
        assert "Rack power" in body and "PDU outlets" in body and "instrument.pdu" in body
        assert "SPI bus" not in body and "Dish supply" not in body and 'href="/power/analog"' in body

    def test_the_rack_badge_without_a_pdu_is_absent(self, client, jobs):
        login(client)
        body = client.get("/partials/menu").data.decode()
        assert '<a href="/power/rack" class="tag quiet plain">' in body
