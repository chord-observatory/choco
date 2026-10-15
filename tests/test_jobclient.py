"""choco.jobclient: the loopback JSON client the jobs and the CLI share."""

import json
import urllib.request

import pytest

from choco import jobclient


class _Resp:
    def __init__(self, body: bytes):
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self._body


class TestRequests:
    def test_get_and_post_shapes(self, monkeypatch):
        seen = []

        def fake_urlopen(req, timeout=None, context=None):
            seen.append((req.get_method(), req.full_url, req.data,
                         req.get_header("Content-type"), context, timeout))
            return _Resp(b'{"ok": true}')

        monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
        assert jobclient.get_json("https://localhost:5000/", "/api/status",
                                  timeout=3) == {"ok": True}
        assert jobclient.post_json("http://localhost:5000", "/update/cx",
                                   {"a": 1}) == {"ok": True}
        get, post = seen
        assert get[:4] == ("GET", "https://localhost:5000/api/status", None, None)
        assert get[4] is not None and get[5] == 3          # unverified TLS ctx
        assert post[:2] == ("POST", "http://localhost:5000/update/cx")
        assert json.loads(post[2]) == {"a": 1}
        assert post[3] == "application/json"
        assert post[4] is None                              # plain http: no ctx

    def test_push_updatable_is_a_group_update(self, monkeypatch):
        seen = {}

        def fake_urlopen(req, timeout=None, context=None):
            seen.update(url=req.full_url, body=json.loads(req.data), context=context)
            return _Resp(b"")

        monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
        jobclient.push_updatable("https://localhost:5000", "cx",
                                 "updatable_config/bad_inputs", {"bad_inputs": [3]})
        assert seen["url"] == "https://localhost:5000/update/cx"
        assert seen["body"] == {"action": "updatable_config",
                                "endpoint": "updatable_config/bad_inputs",
                                "values": {"bad_inputs": [3]}}
        assert seen["context"] is not None       # self-signed TLS goes unverified

    def test_empty_body_is_none(self, monkeypatch):
        monkeypatch.setattr(urllib.request, "urlopen",
                            lambda req, timeout=None, context=None: _Resp(b""))
        assert jobclient.post_json("http://h", "/x", {}) is None

    def test_ssl_context_is_unverified_for_https_only(self):
        import ssl
        ctx = jobclient.ssl_context("https://localhost:5000")
        assert ctx.verify_mode == ssl.CERT_NONE and not ctx.check_hostname
        assert jobclient.ssl_context("http://localhost:5000") is None


class TestWriteJsonAtomic:
    def test_writes_and_leaves_no_temp(self, tmp_path):
        path = tmp_path / "deep" / "state.json"
        jobclient.write_json_atomic(path, {"a": [1, 2]})
        assert json.loads(path.read_text()) == {"a": [1, 2]}
        assert list(path.parent.iterdir()) == [path]

    def test_replaces_existing(self, tmp_path):
        path = tmp_path / "s.json"
        path.write_text("old")
        jobclient.write_json_atomic(path, 1, indent=None)
        assert path.read_text() == "1"


class TestSingleRun:
    def test_a_held_lock_yields_false_until_released(self, tmp_path):
        lock = tmp_path / "x.lock"
        with jobclient.single_run(lock) as a:
            assert a
            with jobclient.single_run(lock) as b:
                assert not b
        with jobclient.single_run(lock) as c:
            assert c

    def test_an_unopenable_lock_does_not_stop_the_job(self, tmp_path):
        (tmp_path / "afile").write_text("x")
        with jobclient.single_run(tmp_path / "afile" / "x.lock") as held:
            assert held


class TestAtomicOutput:
    def test_error_keeps_original_and_removes_temp(self, tmp_path):
        path = tmp_path / "nodes.yaml"
        path.write_text("old")
        with pytest.raises(RuntimeError):
            with jobclient.atomic_output(path) as tmp:
                tmp.write_text("half")
                raise RuntimeError("torn")
        assert path.read_text() == "old"
        assert list(tmp_path.iterdir()) == [path]

    def test_suffix_mode_and_fsync(self, tmp_path):
        path = tmp_path / "maps.npz"
        with jobclient.atomic_output(path, suffix=".npz", mode=0o640,
                                     fsync=True) as tmp:
            # hidden, and ignored by the config scan (.tmp before the suffix)
            assert tmp.name.startswith(".maps.npz.") and tmp.name.endswith(".tmp.npz")
            tmp.write_bytes(b"x")
        assert path.read_bytes() == b"x"
        assert path.stat().st_mode & 0o777 == 0o640

    def test_write_text_atomic_default_mode(self, tmp_path):
        path = tmp_path / "a" / "b.yaml"
        jobclient.write_text_atomic(path, "k: 1\n")
        assert path.read_text() == "k: 1\n"
        assert path.stat().st_mode & 0o777 == 0o644
