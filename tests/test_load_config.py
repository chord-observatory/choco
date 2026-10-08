"""load_config refuses retired keys instead of quietly reading them, and
resolves the one state root the jobs share."""

import pytest

from choco.app import load_config

_VALID = "server:\n  secret_key: 0123456789abcdef0123456789abcdef\n"


def _load(tmp_path, extra: str):
    path = tmp_path / "config.yaml"
    path.write_text(_VALID + extra)
    return load_config(path)


class TestRetiredKeys:
    def test_clean_config_loads(self, tmp_path):
        cfg = _load(tmp_path, "pdb:\n  host: p\n  port: 5000\n"
                              "eop:\n  endpoint: earth_rotation_data\n")
        assert cfg["pdb"]["host"] == "p"

    @pytest.mark.parametrize("extra, fix", [
        ("sync:\n  num_workers: 4\n", "max_concurrent_pushes"),
        ("psu:\n  host: p\n", "pdb:"),
        ("eop:\n  fpga_master_host: chive\n", "fpga_master.host"),
        ("eop:\n  fpga_master_port: 54321\n", "fpga_master.port"),
        # 2026-10: state paths are a convention, not configuration
        ("eop:\n  state_file: /var/lib/choco/eop/state.json\n", "state_dir"),
        ("bffs:\n  state_file: /var/lib/choco/bffs/state.json\n", "state_dir"),
        ("bffs:\n  run_file: /var/lib/choco/bffs/run.json\n", "state_dir"),
        ("bffs:\n  manual_file: /x/manual.yaml\n", "state_dir"),
        ("eigencal:\n  state_file: /x/state.json\n", "state_dir"),
        ("waterfall:\n  state_file: /x/state.json\n", "state_dir"),
        ("skymap:\n  image_file: /x/skymap.png\n", "skymap/skymap.png"),
        ("skymap:\n  night_image_file: /x/n.png\n", "skymap/skymap-night.png"),
        ("mapmaker:\n  state_file: /x/s.json\n", "state_dir"),
        ("mapmaker:\n  image_file: /x/m.png\n", "mapmaker/sky.png"),
    ])
    def test_each_retired_key_is_refused_with_the_fix(self, tmp_path, extra, fix):
        with pytest.raises(ValueError, match=fix):
            _load(tmp_path, extra)

    def test_all_problems_reported_at_once(self, tmp_path):
        with pytest.raises(ValueError) as exc:
            _load(tmp_path, "psu:\n  host: p\nsync:\n  num_workers: 2\n")
        assert "psu" in str(exc.value) and "num_workers" in str(exc.value)


class TestStateDir:
    def test_defaults_to_the_systemd_root(self, tmp_path):
        assert _load(tmp_path, "")["state_dir"] == "/var/lib/choco"

    def test_override_for_a_dev_instance(self, tmp_path):
        assert _load(tmp_path, f"state_dir: {tmp_path}/state\n")["state_dir"] == f"{tmp_path}/state"

    def test_relative_root_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="absolute"):
            _load(tmp_path, "state_dir: dev/state\n")


class TestUpstream:
    """The config library's ``upstream:`` block: defaults to kotekan's
    config/chord, merged with overrides, and a malformed block is a
    startup error rather than a button that pulls from the wrong place."""

    def test_defaults_to_kotekan_chord(self, tmp_path):
        cfg = _load(tmp_path, "")
        assert cfg["upstream"] == {
            "enabled": True, "repo": "kotekan/kotekan", "ref": "chord",
            "path": "config/chord", "into": "chord", "timeout": 20,
        }

    def test_overrides_merge(self, tmp_path):
        cfg = _load(tmp_path, "upstream:\n  ref: develop\n  timeout: 5\n")
        assert cfg["upstream"]["ref"] == "develop"
        assert cfg["upstream"]["timeout"] == 5
        assert cfg["upstream"]["repo"] == "kotekan/kotekan"

    def test_disabled(self, tmp_path):
        from choco.upstream import Upstream
        cfg = _load(tmp_path, "upstream:\n  enabled: false\n")
        assert cfg["upstream"]["enabled"] is False
        assert Upstream.from_config(cfg["upstream"]) is None

    @pytest.mark.parametrize("extra, message", [
        ("upstream:\n  branch: chord\n", "unknown upstream key"),
        ("upstream:\n  repo: kotekan\n", "owner/name"),
        ("upstream:\n  into: ../etc\n", "upstream.into"),
        ("upstream:\n  path: /\n", "upstream.path"),
        ("upstream: chord\n", "mapping"),
        ("upstream: false\n", "mapping"),
    ])
    def test_malformed_block_is_refused(self, tmp_path, extra, message):
        with pytest.raises(ValueError, match=message):
            _load(tmp_path, extra)
