"""Tests for the skymap job: pointing helpers, config, and one render."""

import json
from unittest.mock import patch, Mock

import pytest

import skymap


class TestFindKey:
    def test_top_level(self):
        assert skymap.find_key({"dish_coelev_deg": -27.3},
                               "dish_coelev_deg") == -27.3

    def test_nested_in_dict(self):
        cfg = {"telescope": {"chord": {"dish_coelev_deg": -27.3}}}
        assert skymap.find_key(cfg, "dish_coelev_deg") == -27.3

    def test_nested_in_list(self):
        cfg = {"stages": [{"a": 1}, {"inner": {"dish_coelev_deg": -10.0}}]}
        assert skymap.find_key(cfg, "dish_coelev_deg") == -10.0

    def test_missing_is_none(self):
        assert skymap.find_key({"a": {"b": 1}}, "dish_coelev_deg") is None

    def test_zero_value_is_found(self):
        # 0.0 is a legitimate co-elevation (zenith pointing); the walk
        # must not treat it as "not found".
        assert skymap.find_key({"dish_coelev_deg": 0.0},
                               "dish_coelev_deg") == 0.0


class TestPointingMath:
    def test_dec_from_coelev_matches_tau_a(self):
        # The recv config's -27.3 with its own "Approx Tau A" comment.
        assert skymap.dec_from_coelev(-27.3) == pytest.approx(22.02, abs=0.01)

    def test_parse_dec_deg(self):
        assert skymap._parse_dec_deg('+22d00m52.2s') == pytest.approx(22.0145)
        assert skymap._parse_dec_deg('-29d00m28.1s') == pytest.approx(-29.0078)

    def test_nearest_major_source(self):
        assert skymap.nearest_major_source(22.0) == 'Tau A'
        assert skymap.nearest_major_source(40.7) == 'Cyg A'
        assert skymap.nearest_major_source(0.0) is None


class TestFetchPointing:
    """fetch_pointings reads choco through choco.jobclient.get_json; the
    tests replace that one function with a path -> payload table."""

    @staticmethod
    def _serve(replies):
        def get_json(base_url, path, timeout=None):
            reply = replies[path]
            if isinstance(reply, Exception):
                raise reply
            return reply
        return get_json

    def test_explicit_group(self):
        served = self._serve({
            "/api/config/recv": {"telescope": {"dish_coelev_deg": -27.3}}})
        with patch("skymap.get_json", side_effect=served) as get:
            found = skymap.fetch_pointings("https://localhost:5000", "recv")
        assert len(found) == 1
        dec, group = found[0]
        assert group == "recv"
        assert dec == pytest.approx(22.02, abs=0.01)
        assert get.call_args[0][1] == "/api/config/recv"

    def test_collects_every_pointed_group(self):
        served = self._serve({
            "/api/nodes": {"groups": {"cx": [], "recv": []}},
            "/api/config/cx": {"t": {"dish_coelev_deg": -8.6}},
            "/api/config/recv": {"t": {"dish_coelev_deg": -27.3}},
        })
        with patch("skymap.get_json", side_effect=served):
            found = skymap.fetch_pointings("https://localhost:5000")
        assert [g for _, g in found] == ["cx", "recv"]

    def test_unrenderable_group_is_skipped(self):
        import urllib.error
        served = self._serve({
            "/api/nodes": {"groups": {"cx": [], "recv": []}},
            "/api/config/cx": urllib.error.HTTPError(
                "u", 503, "no config", {}, None),
            "/api/config/recv": {"t": {"dish_coelev_deg": -27.3}},
        })
        with patch("skymap.get_json", side_effect=served):
            found = skymap.fetch_pointings("https://localhost:5000")
        assert [g for _, g in found] == ["recv"]

    def test_no_pointing_anywhere_raises(self):
        served = self._serve({
            "/api/nodes": {"groups": {"cx": []}},
            "/api/config/cx": {"no": "pointing"},
        })
        with patch("skymap.get_json", side_effect=served):
            with pytest.raises(ValueError):
                skymap.fetch_pointings("https://localhost:5000")


class TestBeamResolution:
    def test_pointing_token_and_names_and_decs(self):
        parsed = skymap.parse_beams(["pointing", "Cyg A", 10.0])
        assert parsed[0] == (skymap.POINTING, None)
        assert parsed[1][1] == "Cyg A"
        assert parsed[1][0] == pytest.approx(40.73, abs=0.01)
        assert parsed[2] == (10.0, "configured")

    def test_order_is_preserved(self):
        # The first beam gets the primary palette and the clock labels,
        # so the list order is meaningful.
        parsed = skymap.parse_beams(["Cyg A", "pointing"])
        assert parsed[0][1] == "Cyg A"
        assert parsed[1] == (skymap.POINTING, None)

    def test_unknown_entry_is_config_error(self):
        with pytest.raises(ValueError, match="unknown entry"):
            skymap.parse_beams(["Cyg X-1"])

    def test_empty_or_non_list_is_config_error(self):
        with pytest.raises(ValueError, match="non-empty list"):
            skymap.parse_beams([])
        with pytest.raises(ValueError, match="non-empty list"):
            skymap.parse_beams("pointing")

    def test_dedup_drops_near_duplicates(self):
        # A live Tau A pointing plus an explicit "Tau A" beam is one
        # strip, not two overdrawn ones.
        beams = skymap.dedup_beams([(22.02, "cx config"),
                                    (22.0145, "Tau A"), (40.73, "Cyg A")])
        assert len(beams) == 2
        assert beams[0] == (22.02, "cx config")


class TestLoadConfig:
    def test_defaults_when_no_file(self):
        cfg = skymap.load_config(None)
        assert cfg["beams"] == ["pointing"]
        assert cfg["timezone"] == "America/Vancouver"
        assert cfg["output_night"] == "/var/lib/choco/skymap/skymap-night.png"

    def test_empty_output_night_disables_it(self, tmp_path):
        p = tmp_path / "skymap.yaml"
        p.write_text('output_night: ""\n')
        assert skymap.load_config(str(p))["output_night"] == ""

    def test_file_overrides(self, tmp_path):
        p = tmp_path / "skymap.yaml"
        p.write_text("beams: [30.0, Cyg A]\ndpi: 80\n")
        cfg = skymap.load_config(str(p))
        assert cfg["beams"] == [30.0, "Cyg A"]
        assert cfg["dpi"] == 80
        assert cfg["background_fade"] == 0.42  # default survives

    def test_non_mapping_raises(self, tmp_path):
        p = tmp_path / "skymap.yaml"
        p.write_text("- just\n- a list\n")
        with pytest.raises(ValueError):
            skymap.load_config(str(p))


class TestRender:
    def test_renders_png_atomically(self, tmp_path):
        """One real render at thumbnail dpi: the PNG lands complete and
        the temp file is gone (the atomic-write contract the /skymap.png
        route depends on)."""
        from astropy.time import Time
        cfg = dict(skymap.DEFAULTS)
        out = tmp_path / "skymap.png"
        cfg.update({"output": str(out), "dpi": 40})
        eph = skymap.plot_skymap(cfg, [(22.0, "test"), (40.73, "Cyg A")],
                                 now=Time("2026-08-26T18:00:00"))
        assert out.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
        assert not (tmp_path / "skymap.png.tmp").exists()
        # Sanity on the ephemerides: late-August sun, Dec ~ +10.
        assert eph["sun_dec_d"] == pytest.approx(10.4, abs=1.0)
        assert 0 <= eph["lst_h"] < 24
        assert -90 <= eph["moon_dec_d"] <= 90

    def test_night_theme_darkens_the_page_only(self, tmp_path):
        """The night render is the day render in another palette: same
        pixel geometry (so the two stay directly comparable), a dark
        page instead of a white one, written to its own path."""
        import matplotlib.image as mpimg
        from astropy.time import Time
        cfg = dict(skymap.DEFAULTS)
        day, night = tmp_path / "day.png", tmp_path / "night.png"
        cfg.update({"output": str(day), "dpi": 40})
        now = Time("2026-08-26T18:00:00")
        beams = [(22.0, "test")]
        skymap.plot_skymap(cfg, beams, now=now)
        skymap.plot_skymap(cfg, beams, now=now, theme="night",
                           output=str(night))
        assert night.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
        assert not (tmp_path / "night.png.tmp").exists()
        d, n = mpimg.imread(day), mpimg.imread(night)
        assert d.shape == n.shape
        # The top-left corner is bare page in both.
        assert d[0, 0, :3].mean() > 0.95
        assert n[0, 0, :3].mean() < 0.15

    def test_unknown_theme_is_an_error(self):
        with pytest.raises(ValueError, match="unknown theme"):
            skymap.plot_skymap(dict(skymap.DEFAULTS), [(22.0, "t")],
                               theme="dusk")


class TestMain:
    """main() wires the config to the renders; the render itself is
    covered above, so it is replaced here."""

    def _run(self, tmp_path, yaml_text):
        cfg = tmp_path / "skymap.yaml"
        cfg.write_text(yaml_text)
        with patch("skymap.plot_skymap") as render:
            rc = skymap.main(["--config", str(cfg)])
        return rc, render

    def test_renders_day_then_night_from_one_instant(self, tmp_path):
        rc, render = self._run(
            tmp_path,
            f"beams: [Cyg A]\noutput: {tmp_path}/d.png\n"
            f"output_night: {tmp_path}/n.png\n")
        assert rc == 0
        assert [c.kwargs["theme"] for c in render.call_args_list] == [
            "day", "night"]
        assert [c.kwargs["output"] for c in render.call_args_list] == [
            f"{tmp_path}/d.png", f"{tmp_path}/n.png"]
        # Both images must show the same Sun, Moon and beam-now.
        first, second = (c.kwargs["now"] for c in render.call_args_list)
        assert first is second

    def test_empty_output_night_renders_day_only(self, tmp_path):
        rc, render = self._run(
            tmp_path, f"beams: [Cyg A]\noutput: {tmp_path}/d.png\n"
                      'output_night: ""\n')
        assert rc == 0
        assert render.call_count == 1
        assert render.call_args.kwargs["theme"] == "day"

    def test_render_oserror_is_degraded(self, tmp_path):
        cfg = tmp_path / "skymap.yaml"
        cfg.write_text(f"beams: [Cyg A]\noutput: {tmp_path}/d.png\n")
        with patch("skymap.plot_skymap", side_effect=OSError("disk full")):
            assert skymap.main(["--config", str(cfg)]) == 2
