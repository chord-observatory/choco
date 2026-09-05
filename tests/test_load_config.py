"""load_config refuses retired keys instead of quietly reading them."""

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
                              "eop:\n  state_file: /var/lib/choco/eop/state.json\n")
        assert cfg["pdb"]["host"] == "p"

    @pytest.mark.parametrize("extra, fix", [
        ("sync:\n  num_workers: 4\n", "max_concurrent_pushes"),
        ("psu:\n  host: p\n", "pdb:"),
        ("eop:\n  fpga_master_host: chive\n", "fpga_master.host"),
        ("eop:\n  fpga_master_port: 54321\n", "fpga_master.port"),
        ("eop:\n  state_file: eop-state.json\n", "absolute path"),
    ])
    def test_each_retired_key_is_refused_with_the_fix(self, tmp_path, extra, fix):
        with pytest.raises(ValueError, match=fix):
            _load(tmp_path, extra)

    def test_all_problems_reported_at_once(self, tmp_path):
        with pytest.raises(ValueError) as exc:
            _load(tmp_path, "psu:\n  host: p\nsync:\n  num_workers: 2\n")
        assert "psu" in str(exc.value) and "num_workers" in str(exc.value)
