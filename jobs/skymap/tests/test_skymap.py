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
