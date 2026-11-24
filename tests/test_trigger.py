"""Tests for the baseband trigger relay: ``choco.trigger`` and the route.

An outside telescope's request reaches choco through the site proxy.  The
route admits only the proxy (address and trigger secret), rebuilds the
request from strictly parsed values, and sends it to the running nodes.
"""

import copy
import json

import pytest
import responses

from choco import trigger
from choco.app import _DEFAULT_CONFIG, create_app, load_config
from choco.state import Mode, NodeStatus

PROXY = "10.220.1.13"
SECRET = "trigger-" * 5
NOW = 1_763_400_000.0
BLOCK = {"peer": PROXY, "secret": SECRET, "senders": ["chime"]}
CFG = trigger.parse_config(BLOCK)
BODY = {"event_id": 123456, "start_unix_seconds": int(NOW) - 20, "start_unix_nano": 250000000,
        "duration_nano": 100000000, "dm": 350.0, "dm_error": 0.5,
        "file_path": "2026/11/17/event_123456"}
HEADERS = {trigger.SECRET_HEADER: SECRET, trigger.SENDER_HEADER: "chime"}
FROM_PROXY = {"environ_base": {"REMOTE_ADDR": PROXY}}


def _parse(body=None, sender="chime", **changes):
    data = body if body is not None else json.dumps({**BODY, **changes}).encode()
    return trigger.parse_request(data, CFG, sender, NOW)


class TestParseConfig:
    def test_absent_is_off(self):
        assert trigger.parse_config({}) is None and trigger.parse_config(None) is None

    @pytest.mark.parametrize("change, word", [
        ({"peer": "127.0.0.1"}, "loopback"),
        ({"peer": "proxy"}, "IP address"),
        ({"secret": "short"}, "at least"),
        ({"senders": []}, "senders"),
        ({"senders": ["Bad Name"]}, "senders"),
        ({"max_duration_s": 0}, "max_duration_s"),
        ({"typo": 1}, "unknown key"),
    ])
    def test_a_bad_block_is_refused(self, change, word):
        with pytest.raises(ValueError, match=word):
            trigger.parse_config({**BLOCK, **change})

    def test_the_secret_may_not_be_the_sign_in_secret(self):
        with pytest.raises(ValueError, match="must differ"):
            trigger.parse_config(BLOCK, proxy_secret=SECRET)


class TestParseRequest:
    def test_the_request_is_rebuilt_with_chocos_path(self):
        req = _parse()
        assert req == {**BODY, "file_path": "chime/2025/11/17/event_123456"}
        assert list(req) == list(trigger.FIELDS)

    @pytest.mark.parametrize("change", [
        {"event_id": 0}, {"event_id": 2 ** 64}, {"event_id": 1.0}, {"event_id": True},
        {"event_id": "1"}, {"start_unix_seconds": -1}, {"start_unix_nano": 10 ** 9},
        {"duration_nano": 0}, {"duration_nano": 10 ** 10 + 1}, {"dm": -1}, {"dm": 1e9},
        {"dm": "350"}, {"dm_error": 500.1}, {"dm_error": -0.1},
        {"file_path": "../x"}, {"file_path": "/abs"}, {"file_path": "a//b"},
        {"file_path": "a.b"}, {"file_path": 3}, {"file_path": "a" * 200},
        {"extra": 1},
    ])
    def test_a_bad_field_is_refused(self, change):
        with pytest.raises(trigger.Refused):
            _parse(**change)

    def test_the_limits_themselves_pass(self):
        req = _parse(duration_nano=10 ** 10, dm_error=500, dm=0)
        assert req["duration_nano"] == 10 ** 10 and req["dm_error"] == 500.0

    @pytest.mark.parametrize("body", [
        b"", b"[]", b"null", b"{", b"\xff\xfe",
        b'{"event_id": 1, "event_id": 2}',
        json.dumps({**BODY, "dm": 1}).encode().replace(b'"dm": 1', b'"dm": NaN'),
        json.dumps({**BODY, "dm": 1}).encode().replace(b'"dm": 1', b'"dm": 1e999'),
        b"[" * 1500,
        json.dumps({**BODY, "file_path": "x" * 3000}).encode(),
    ])
    def test_a_bad_body_is_refused(self, body):
        with pytest.raises(trigger.Refused):
            _parse(body=body)

    def test_a_missing_field_is_refused(self):
        body = {k: v for k, v in BODY.items() if k != "dm_error"}
        with pytest.raises(trigger.Refused):
            _parse(body=json.dumps(body).encode())

    @pytest.mark.parametrize("offset", [-121, 11])
    def test_a_start_outside_the_window_is_refused(self, offset):
        with pytest.raises(trigger.Refused) as e:
            _parse(start_unix_seconds=int(NOW) + offset, start_unix_nano=0)
        assert e.value.reason == "outside_window"

    def test_an_unknown_sender_is_refused(self):
        with pytest.raises(trigger.Refused) as e:
            _parse(sender="../etc")
        assert e.value.reason == "unknown_sender"


class TestTriggerLog:
    def test_a_forwarded_event_is_remembered_across_a_restart(self, tmp_path):
        log = trigger.TriggerLog(tmp_path / "t" / "log.jsonl")
        assert log.claim("chime", 7) and not log.claim("chime", 7)
        log.record("forwarded", sender="chime", event_id=7)
        log.record("malformed", sender="chime")
        assert log.counts == {"forwarded": 1, "malformed": 1}
        again = trigger.TriggerLog(tmp_path / "t" / "log.jsonl")
        assert not again.claim("chime", 7) and again.claim("other", 7)


@pytest.fixture
def app(configs_dir, tmp_path, monkeypatch):
    cfg = copy.deepcopy(_DEFAULT_CONFIG)
    cfg["configs_dir"] = str(configs_dir)
    cfg["baseband_trigger"] = dict(BLOCK)
    app = create_app(config=cfg)
    app.config["TESTING"] = True
    app.config["state_dir"] = tmp_path / "state"
    monkeypatch.setattr("choco.web.time.time", lambda: NOW)
    for node in app.config["registry"].nodes.values():
        node.mode = Mode.NORMAL
        node.status = NodeStatus.STARTED
    return app


def _post(client, body=BODY, headers=HEADERS, **kw):
    return client.post("/trigger/baseband", data=json.dumps(body), headers=headers,
                       **(kw or FROM_PROXY))


def _kotekan(app, reply=None):
    for node in app.config["registry"].nodes.values():
        if node.group == "cx":
            responses.add(responses.POST, f"http://{node.host}:{node.port}/baseband",
                          json=reply if reply is not None else {})


def _sent():
    """The POSTs that left choco (the sync workers' probes are GETs)."""
    return [c for c in responses.calls if c.request.method == "POST"]


def _records(app):
    path = app.config["state_dir"] / "server" / "baseband_triggers.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]


class TestRoute:
    def test_absent_without_the_block(self, configs_dir):
        client = create_app(configs_dir=configs_dir).test_client()
        assert _post(client).status_code == 404

    @pytest.mark.parametrize("headers, addr", [
        (HEADERS, "10.222.0.54"),                                  # right secret, wrong peer
        ({**HEADERS, trigger.SECRET_HEADER: "x" * 40}, PROXY),     # wrong secret
        ({trigger.SENDER_HEADER: "chime"}, PROXY),                 # no secret
        (HEADERS, "127.0.0.1"),                                    # loopback is not the proxy
    ])
    @responses.activate
    def test_only_the_proxy_with_the_secret_gets_in(self, client, headers, addr):
        resp = _post(client, headers=headers, environ_base={"REMOTE_ADDR": addr})
        assert resp.status_code == 403 and not _sent()

    @responses.activate
    def test_a_trigger_goes_to_the_running_cx_nodes(self, app, client):
        _kotekan(app, {"12": {"file_name": "baseband_123456_12.h5"}})
        resp = _post(client)
        assert resp.status_code == 202
        assert resp.get_json() == {"event_id": 123456, "nodes": 2, "accepted": 2,
                                   "file_path": "chime/2025/11/17/event_123456"}
        sent = [json.loads(c.request.body) for c in _sent()]
        assert len(sent) == 2 and all(s == {**BODY, "file_path": "chime/2025/11/17/event_123456"}
                                      for s in sent)
        assert all("/cx" in c.request.url and "recv" not in c.request.url for c in _sent())
        (rec,) = _records(app)
        assert rec["result"] == "forwarded" and set(rec["nodes"]) == {"cx/cx1", "cx/cx2"}

    @responses.activate
    def test_a_node_in_maintenance_or_not_running_is_skipped(self, app, client):
        _kotekan(app)
        nodes = app.config["registry"].nodes
        nodes["cx/cx1"].mode = Mode.MAINTENANCE
        nodes["cx/cx2"].status = NodeStatus.IDLE
        resp = _post(client)
        assert resp.status_code == 202 and resp.get_json()["accepted"] == 0
        assert not _sent()

    @responses.activate
    def test_a_development_node_gets_the_trigger(self, app, client):
        _kotekan(app)
        app.config["registry"].nodes["cx/cx1"].mode = Mode.DEVELOPMENT
        assert _post(client).get_json()["accepted"] == 2

    @responses.activate
    def test_a_repeat_is_refused(self, app, client):
        _kotekan(app)
        assert _post(client).status_code == 202
        assert _post(client).status_code == 409
        assert len(_sent()) == 2
        assert [r["result"] for r in _records(app)] == ["forwarded", "duplicate"]

    @responses.activate
    def test_a_bad_request_is_refused_and_recorded(self, app, client):
        resp = _post(client, body={**BODY, "file_path": "../../etc"})
        assert resp.status_code == 400 and not _sent()
        assert _records(app)[0]["result"] == "malformed"
        assert _post(client, headers={**HEADERS, trigger.SENDER_HEADER: "nobody"}).status_code == 400
        assert client.post("/trigger/baseband", data=b"x" * 5000, headers=HEADERS,
                           **FROM_PROXY).status_code == 413

    @responses.activate
    def test_the_counts_reach_metrics(self, app, client):
        _kotekan(app)
        _post(client)
        text = client.get("/metrics").get_data(as_text=True)
        assert 'choco_baseband_triggers_total{result="forwarded"} 1' in text


def test_load_config_checks_the_block(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("server: {secret_key: " + "k" * 40 + "}\n"
                    "baseband_trigger: {peer: 10.220.1.13, secret: short, senders: [chime]}\n")
    with pytest.raises(ValueError, match="baseband_trigger.secret"):
        load_config(path)
