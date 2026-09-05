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
