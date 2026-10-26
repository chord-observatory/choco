"""The instrument page: the wiring table, the board layout from kotekan's
config, the Prometheus poll and the page."""

import pytest
import yaml

from choco import instrument
from choco.instrument import (InstrumentMonitor, SwitchEapi, Wiring, WiringFile, WiringRow, board_layout,
                              flow_diagram, host_temps, idrac_state, in_port_state, link_problems,
                              job_tone, job_word, nic_state, out_port_state, outlet_watts, overview,
                              parse_devices, parse_wiring, short_build, worst_tone)
from tests.helpers import login

WIRING = """\
# comment lines are skipped
switch_port,crate_slot,node,nic_port,notes
Ethernet9/1,1,,,
Ethernet10/1,2,,,
Ethernet11/1,3,,,
Ethernet12/1,4,,,
Ethernet1/1,,cx1,0,
Ethernet32/1,,cx1,1,inferred
"""

# Four dishes, two polarizations, four boards of two ADCs: output slot i
# holds pol i // 2 of dish group i % 2; board 4 (source_id 3) is listed silent.
TELESCOPE = {
    "telescope": {
        "num_dishes": 4,
        "num_polarizations": 2,
        "crs_board_remap": [0, 2, 1, 3],
        "dish_inputs": [{"dish_idx": i, "type": "ArrayDish", "label": label}
                        for i, label in enumerate(["A01", "A02", "B01", "B02"])],
    },
    "missing_source_ids": [3],
}


def _result(value, **labels):
    return {"metric": labels, "value": [0, str(value)]}


#: Prometheus replies by the start of the query.
REPLIES = {
    "switch_interface_up": [_result(1, interface=f"Ethernet{p}/1") for p in (9, 10, 11, 1, 32)]
                           + [_result(0, interface="Ethernet12/1")],
    "sum by (interface, direction) (rate(switch_interface_packets_total":
        [_result(1.5e6, interface=f"Ethernet{p}/1", direction="in") for p in (9, 10, 11)]
        + [_result(4e5, interface="Ethernet1/1", direction="out"),
           _result(4e5, interface="Ethernet32/1", direction="out")],
    "sum by (interface, direction) (rate(switch_interface_octets_total":
        [_result(9.7e9, interface="Ethernet9/1", direction="in")],
    "switch_interface_info": [_result(1, interface="Ethernet9/1", type="100GBASE-SR4", speed="100G")],
    "switch_interface_status_info": [_result(1, interface="Ethernet33", status="notconnect")],
    "kotekan_dpdk_nic_link_up": [_result(1, instance="cx1.example:12048", port="0"),
                                 _result(1, instance="cx1.example:12048", port="1")],
    "rate(kotekan_dpdk_nic_rx_packets_total": [_result(4e5, instance="cx1.example:12048", port="0"),
                                               _result(2e5, instance="cx1.example:12048", port="1")],
    "keysight_psu_up": [_result(1, instance="10.222.0.31")],
    "keysight_psu_output_on": [_result(1, instance="10.222.0.31")],
    "keysight_psu_voltage_volts": [_result(48.1, instance="10.222.0.31")],
    "keysight_psu_current_amps": [_result(12.5, instance="10.222.0.31")],
    "switch_monitor_up": [_result(1)],
    'pdu_outlet_sensor_value{sensor="5"}': [_result(750, instance="10.222.0.27", outlet="5", outlet_name="cx1"),
                                            _result(3, instance="10.222.0.27", outlet="6", outlet_name="cx1")],
    'up{job="idrac_snmp"}': [_result(1, instance="cx1i.example")],
    "idrac_system_power_state": [_result(4, instance="cx1i.example")],
    'up{job="node"}': [_result(1, instance="cx1.example:9100")],
    "switch_monitor_last_success_timestamp_seconds": [_result(1e12)],
}


#: _DEVICE_QUERIES results for one server, cx1.
DEVICE_RESULTS = {
    "outlet_w": [_result(747, instance="10.222.0.27", outlet="5", outlet_name="cx1"),
                 _result(3, instance="10.222.0.27", outlet="6", outlet_name="cx1"),
                 _result(98, instance="10.222.0.27", outlet="11", outlet_name="arista")],
    "inlet_w": [_result(8100, instance="10.222.0.27", inlet="1")],
    "idrac_up": [_result(1, instance="cx1i.example")],
    "idrac_temp": [_result(t, instance="cx1i.example", probe_location=loc)
                   for loc, t in (("Inlet Temp", 18), ("CPU1 Temp", 40), ("CPU2 Temp", 41), ("GPU Temp 31", 33))],
    "idrac_temp_status": [_result(3, instance="cx1i.example", probe_location="Inlet Temp")],
    "idrac_status": [_result(3, instance="cx1i.example")],
    "idrac_power": [_result(4, instance="cx1i.example")],
    "idrac_psu": [_result(3, instance="cx1i.example", psu="PS1 Status"),
                  _result(3, instance="cx1i.example", psu="PS2 Status")],
    "host_up": [_result(1, instance="cx1.example:9100")],
    "crate_ready": [_result(1, instance="crs")],
    "crate_boards": [_result(16, instance="crs")],
}


class _Reply:
    def __init__(self, body, text=""):
        self._body, self.text = body, text

    def json(self):
        return self._body


def kotekan_metrics(n: int) -> str:
    """A node's /metrics on its n-th read, 1 s apart: port 0 receives 4e5
    packets/s and drops 100/s on a full ring, port 1 receives 2e5/s."""
    t = 1792977515000 + 1000 * n
    return "\n".join([
        "# HELP kotekan_dpdk_nic_link_up the link",
        f'kotekan_dpdk_nic_link_up{{stage_name="/dpdk",port="0"}} 1.000000 {t}',
        f'kotekan_dpdk_nic_link_up{{stage_name="/dpdk",port="1"}} 1.000000 {t}',
        f'kotekan_dpdk_nic_rx_packets_total{{stage_name="/dpdk",port="0"}} {4e5 * n:.6f} {t}',
        f'kotekan_dpdk_nic_rx_packets_total{{stage_name="/dpdk",port="1"}} {2e5 * n:.6f} {t}',
        f'kotekan_dpdk_distributor_ring_full_dropped_packets_total{{stage_name="/dpdk/handlers/0",port="0"}} {100 * n:.6f} {t}',
        f'kotekan_dpdk_distributor_packets_total{{stage_name="/dpdk/handlers/0",port="0"}} {4e5 * n:.6f} {t}',
        "kotekan_running 1",
    ])


def eapi_results(n: int) -> list[dict]:
    """``_SWITCH_CMDS`` replies on the n-th read, 1 s apart in the counters."""
    return [{"interfaceStatuses": {"Ethernet9/1": {"linkStatus": "connected", "interfaceType": "100GBASE-SR4"},
                                   "Ethernet12/1": {"linkStatus": "notconnect"}}},
            {"interfaces": {"Ethernet9/1": {"inOctets": int(9.7e9 * n), "inUcastPkts": int(1.5e6 * n),
                                            "inMulticastPkts": 0, "outOctets": 0,
                                            "lastUpdateTimestamp": 1792977515.0 + n}}},
            {"interfaces": {"Ethernet9/1": {"inDiscards": 0, "outDiscards": 5 * n}}}]


def fake_fetch(calls, reads=None):
    """Prometheus answers from REPLIES, every kotekan /metrics from
    kotekan_metrics, the switch from eapi_results; *reads* counts the reads
    per URL."""
    reads = {} if reads is None else reads

    def fetch(method, url, *, timeout, params=None, **kw):
        reads[url] = reads.get(url, 0) + 1
        if url.endswith("/metrics"):
            calls.append(url)
            return _Reply(None, kotekan_metrics(reads[url]))
        if method == "POST":
            calls.append(kw["json"]["params"]["cmds"])
            return _Reply({"jsonrpc": "2.0", "id": 1, "result": eapi_results(reads[url])})
        q = params["query"]
        calls.append(q)
        hit = next((v for k, v in REPLIES.items() if q.startswith(k)), [])
        return _Reply({"status": "success", "data": {"resultType": "vector", "result": hit}})
    return fetch


class TestWiring:
    def test_parses_both_kinds_of_row(self):
        rows, errors = parse_wiring(WIRING)
        assert errors == []
        assert rows[0] == WiringRow("Ethernet9/1", crate_slot=1)
        assert rows[-1] == WiringRow("Ethernet32/1", node="cx1", nic_port=1, notes="inferred")

    def test_refuses_bad_rows_one_at_a_time(self):
        text = WIRING + "\n".join([
            "Et5,5,,,",                      # not EthernetN/1
            "Ethernet9/1,6,,,",              # port twice
            "Ethernet13/1,1,,,",             # slot twice
            "Ethernet14/1,17,,,",            # slot out of range
            "Ethernet15/1,7,cx2,0,",         # both kinds
            "Ethernet2/1,,cx1,0,",           # nic twice
            "Ethernet3/1,,cx2,2,",           # nic port not 0/1
            "Ethernet4/1,,,,",               # neither
            "Ethernet16/1,8,,,",             # fine
        ])
        rows, errors = parse_wiring(text)
        assert len(errors) == 8 and errors[0].startswith("row 8:")
        assert rows[-1].switch_port == "Ethernet16/1"

    def test_refuses_a_table_without_its_header(self):
        rows, errors = parse_wiring("port,slot\nEthernet9/1,1\n")
        assert rows == [] and "header" in errors[0]

    def test_file_rereads_on_change_and_reports_a_missing_file(self, tmp_path):
        path = tmp_path / "wiring.csv"
        wf = WiringFile(path)
        assert wf.get().rows == [] and "No such file" in wf.get().errors[0]
        path.write_text(WIRING)
        assert len(wf.get().rows) == 6
        assert WiringFile(None).get().errors == ["no wiring file configured"]


class TestBoards:
    def test_places_every_feed_on_its_board_and_adc(self):
        boards, problem = board_layout(TELESCOPE)
        assert problem is None
        assert [(b["slot"], b["pol"]) for b in boards] == [(1, "X"), (2, "Y"), (3, "X"), (4, "Y")]
        # output slot 1 takes source_id 2 (crate slot 3): pol X of dish group 1
        assert [f["label"] for f in boards[2]["feeds"]] == ["B01X", "B02X"]
        assert boards[2]["feeds"][1]["adc"] == 1
        assert [f["label"] for f in boards[1]["feeds"]] == ["A01Y", "A02Y"]
        assert [b["silent_expected"] for b in boards] == [False, False, False, True]

    def test_says_why_it_cannot(self):
        assert board_layout({})[1] == "the X-engine config has no dish_inputs table"
        cfg = {"telescope": {k: v for k, v in TELESCOPE["telescope"].items() if k != "crs_board_remap"}}
        assert board_layout(cfg)[1] == "the X-engine config has no crs_board_remap"
        cfg = {"telescope": {**TELESCOPE["telescope"], "crs_board_remap": [0, 1, 2]}}
        assert "do not fill" in board_layout(cfg)[1]


class TestStates:
    def test_board_port(self):
        assert in_port_state(None) == ("off", "no data")
        assert in_port_state({"up": 1}, silent_expected=True)[0] == "off"
        assert in_port_state({"up": 0}) == ("bad", "down")
        assert in_port_state({"up": 1, "in_pps": 10}) == ("warn", "no traffic")
        assert in_port_state({"up": 1, "in_pps": 1.5e6, "fec_unc": 3}) == ("warn", "errors")
        assert in_port_state({"up": 1, "in_pps": 1.5e6}) == ("ok", "up")

    def test_node_port_and_nic(self):
        assert out_port_state({"up": 1, "out_pps": 4e5, "out_discards": 2}) == ("warn", "errors")
        assert nic_state(None) == ("off", "no kotekan")
        assert nic_state({"up": 0}) == ("bad", "link down")
        # a few misses per second out of 1.5 Mpps are normal
        assert nic_state({"up": 1, "pps": 1.5e6, "missed": 20}) == ("ok", "up")
        assert nic_state({"up": 1, "pps": 1.5e6, "missed": 500}) == ("warn", "drops")

    def test_problems_name_the_link_and_a_rate_mismatch(self):
        rows, _ = parse_wiring(WIRING)
        switch = {"Ethernet9/1": {"up": 1, "in_pps": 1.5e6}, "Ethernet10/1": {"up": 0},
                  "Ethernet1/1": {"up": 1, "out_pps": 4e5}, "Ethernet32/1": {"up": 1, "out_pps": 4e5}}
        nics = {("cx1", 0): {"up": 1, "pps": 4e5}, ("cx1", 1): {"up": 1, "pps": 2e5}}
        out = link_problems(Wiring(rows, ["row 9: bad"]), switch, nics)
        assert out[0] == "row 9: bad"
        assert "Ethernet10/1 (crate slot 2): down" in out
        assert "Ethernet11/1 (crate slot 3): no data" not in out      # off is not a problem
        assert any(p.startswith("Ethernet32/1 sends 400 kpps but cx1 port 1 receives 200 kpps") for p in out)


class TestOverview:
    def test_rows_join_the_boards_ports_and_nodes(self):
        boards, _ = board_layout(TELESCOPE)
        rows, _ = parse_wiring(WIRING)
        switch = {"Ethernet9/1": {"up": 1, "in_pps": 1.5e6, "in_bps": 7.7e10},
                  "Ethernet1/1": {"up": 1, "out_pps": 4e5, "out_bps": 7.3e10},
                  "Ethernet32/1": {"up": 1, "out_pps": 4e5},
                  "Ethernet33": {"status": "notconnect"}, "Management1": {"up": 1}}
        nics = {("cx1", 0): {"up": 1, "pps": 4e5}, ("cx1", 1): {"up": 1, "pps": 2e5},
                ("cx2", 0): {"up": 1, "pps": 4e5}}
        nodes = {"cx1": {"status": "started"}, "cx2": {"status": "started"}, "recv1": {"status": "idle"}}
        v = overview(boards, Wiring(rows, []), switch, nics, {"A01X": True, "A02X": False}, nodes)
        b1 = v["boards"][0]
        assert (b1["port"], b1["tone"]) == ("Ethernet9/1", "ok")
        assert [(f["power"], f["in_pdb"]) for f in b1["feeds"]] == [(True, True), (False, True)]
        assert v["boards"][3]["word"] == "silent (listed)"
        assert [(x["node"], x["nic_port"]) for x in v["nics"]] == [("cx1", 0), ("cx1", 1)]
        assert v["nics"][1]["mismatch"] and v["nics"][1]["nic_word"] == "rate differs"
        assert v["unmapped"] == [("cx2", 0)]
        assert [d["name"] for d in v["downstream"]] == ["cx2", "recv1"]
        assert [p["name"] for p in v["switch_ports"]][:3] == ["Ethernet1/1", "Ethernet9/1", "Ethernet10/1"]
        assert "Management1" not in [p["name"] for p in v["switch_ports"]]
        assert round(v["in_gbps"], 1) == 77.0


class TestFlow:
    def test_one_column_per_board_and_nic_port_and_a_card_per_node(self):
        boards, _ = board_layout(TELESCOPE)
        rows, _ = parse_wiring(WIRING)
        switch = {"Ethernet9/1": {"up": 1, "in_pps": 1.5e6, "in_bps": 7.7e10},
                  "Ethernet1/1": {"up": 1, "out_pps": 4e5, "out_bps": 7.3e10},
                  "Ethernet32/1": {"up": 0}}
        nics = {("cx1", 0): {"up": 1, "pps": 4e5}}
        nodes = {"cx1": {"status": "started", "group": "cx", "branch": "chord",
                         "version": "chord.2021.10+1242.g340808ce76"},
                 "recv1": {"status": "idle", "group": "recv"}}
        devices = parse_devices(DEVICE_RESULTS)
        v = overview(boards, Wiring(rows, []), switch, nics, {"A01X": True, "A02X": False, "B01X": True}, nodes,
                     devices, {"B01X": "autos: low"})
        stages = {"switch": {"title": "Switch", "tone": "warn", "word": "1/2 ok"},
                  "pdb": {"title": "Feeds", "tone": "ok", "word": "2/2 on"},
                  "power_cards": [{"name": "Dish supply", "line": "on", "tone": "ok"},
                                  {"name": "PDB controller", "line": "answering", "tone": "ok"}]}
        f = flow_diagram(v, stages, "/choco")
        texts = [ln["text"] for c in f["cards"] for ln in c["lines"]]
        assert sum(t.startswith("slot ") for t in texts) == 4
        assert len([t for t in f["ticks"] if t["y"] == f["switch"]["y"]]) == 4
        assert [t["label"] for t in f["ticks"] if t["label"].startswith("p")] == ["p0", "p1"]
        assert [x["text"] for x in f["feeds"][:2]] == ["A01X", "A02X"]
        # the colour is the bffs flag, the fill the PDB power
        tone = {x["text"]: x for x in f["feeds"]}
        assert [tone[k]["tone"] for k in ("A01X", "A02X", "B01X", "A01Y")] == ["ok-on", "ok-off", "bad-on", "ok-none"]
        assert tone["B01X"]["title"].endswith("power on; flagged (autos: low)")
        unknown = overview(boards, Wiring(rows, []), switch, nics, {"A01X": True}, nodes)
        assert flow_diagram(unknown, stages)["feeds"][0]["tone"] == "none-on"
        node = next(c for c in f["cards"] if c["lines"][0]["text"] == "cx1")
        assert node["href"] == "/choco/pipeline/cx/cx1"
        assert "chord +1242" in texts and "iDRAC on · OK" in texts and "CPU 41° GPU 33°" in texts
        assert "750 W · inlet 18°" in texts
        assert any(c["lines"][0]["text"] == "recv1" for c in f["cards"])
        assert f["bus"] and f["bus"]["tone"] == "ok"
        tones = [l["tone"] for l in f["links"]]
        assert "bad" in tones and tones.count("ok") >= 2
        assert next(l for l in f["lanes"] if l["key"] == "switch")["word"] == "1/2 ok"
        assert len(f["switch"]["mesh"]) == 8
        assert f["width"] < 1100 and not f["texts"]       # no control column without control cards

    def test_control_beside_the_xengines_and_jobs_under_the_files(self):
        boards, _ = board_layout(TELESCOPE)
        rows, _ = parse_wiring(WIRING)
        nodes = {"cx1": {"status": "started", "group": "cx"}, "recv1": {"status": "started", "group": "recv"}}
        v = overview(boards, Wiring(rows, []), {}, {}, {}, nodes)
        stages = {"files": {"tone": "ok", "word": "ok"},
                  "control_cards": [{"name": "CHOCO", "tone": "ok"}, {"name": "BFFS", "tone": "warn", "line": "3 feeds flagged"}],
                  "job_cards": [{"name": "WF", "tone": "ok"}, {"name": "MAP", "tone": "bad"}],
                  "processing": {"title": "Processing", "tone": "bad", "word": "1/2 ok"}}
        f = flow_diagram(v, stages)
        names = [c["lines"][0]["text"] for c in f["cards"]]
        assert {"CHOCO", "BFFS", "WF", "MAP"} <= set(names)
        assert any(l["label"] == "subset files" and l["tone"] == "warn" for l in f["links"])
        assert sum("reads the files" in l["title"] for l in f["links"]) == 2
        assert next(l for l in f["lanes"] if l["key"] == "processing")["word"] == "1/2 ok"
        assert f["texts"][0]["text"] == "Control" and f["width"] == 1240


class TestJobs:
    def test_tone_and_word_follow_the_landing_macros(self):
        assert [job_tone(h) for h in ("ok", "degraded", "stale", "failed", "never_run", None)] == \
            ["ok", "warn", "warn", "bad", "off", "off"]
        assert job_tone("unknown", running=True) == "info" and job_word("unknown", running=True) == "running"
        assert job_word("never_run") == "not run"
        assert worst_tone(["ok", "info", "warn"]) == "warn" and worst_tone([]) == "off"


class TestDevices:
    def test_parses_outlets_idracs_and_hosts(self):
        d = parse_devices(DEVICE_RESULTS)
        assert d["inlet_w"] == 8100 and outlet_watts(d, "cx1") == 750 and outlet_watts(d, "nope") is None
        assert d["outlets"][0]["pdu"] == "10.222.0.27"
        h = d["hosts"]["cx1"]
        assert h["power"] == 4 and h["psu"] == {"PS1 Status": 3, "PS2 Status": 3} and h["up"] == 1
        assert host_temps(h) == {"inlet": 18, "cpu": 41, "gpu": 33}
        assert d["crate"] == {"ready": 1, "boards": 16}

    def test_idrac_state(self):
        assert idrac_state(None) == ("off", "not scraped")
        assert idrac_state({"idrac_up": 0}) == ("bad", "silent")
        assert idrac_state({"idrac_up": 1, "power": 3}) == ("bad", "power off")
        assert idrac_state({"idrac_up": 1, "power": 4, "status": 3, "psu": {"PS2 Status": 5}}) == \
            ("warn", "PS2 Status not OK")
        assert idrac_state({"idrac_up": 1, "power": 4, "status": 3, "temp_bad": True}) == ("warn", "warning")
        assert idrac_state({"idrac_up": 1, "power": 4, "status": 5}) == ("bad", "critical")

    def test_short_build(self):
        assert short_build("chord", "chord.2021.10+1242.g340808ce76") == "chord +1242"
        assert short_build("kv/proj-rollout", "x+89.gabc") == "kv/proj-rollout"
        assert short_build("a-very-long-branch-name", None) == "a-very-long-br…"
        assert short_build(None, None) == "—"


class TestMonitor:
    def _monitor(self, monkeypatch, tmp_path, **kw):
        calls = []
        monkeypatch.setattr(instrument, "fetch", fake_fetch(calls))
        monkeypatch.setattr(instrument.time, "time", iter(range(1000, 100000, 10)).__next__)
        (tmp_path / "wiring.csv").write_text(WIRING)
        kw.setdefault("url", "http://prom.example:9090/")
        m = InstrumentMonitor(kw.pop("url"), switch_instance="10.0.0.21",
                              wiring=WiringFile(tmp_path / "wiring.csv"),
                              nodes=lambda: [("cx1", "http://cx1.example:12048")], **kw)
        return m, calls

    def test_unconfigured_never_polls(self):
        m = InstrumentMonitor(None)
        assert m.health == "unconfigured" and not m.configured
        m.poll_once()
        assert m.error == "instrument.prometheus_url not set"

    def test_poll_fills_the_snapshot_and_judges_the_links(self, monkeypatch, tmp_path):
        m, calls = self._monitor(monkeypatch, tmp_path)
        m.poll_once()
        assert (m.host, m.port) == ("prom.example", 9090)
        assert 'switch_interface_up{instance="10.0.0.21"}' in calls
        assert m.switch["Ethernet9/1"]["in_pps"] == 1.5e6 and m.switch["Ethernet9/1"]["type"] == "100GBASE-SR4"
        # the NICs come from kotekan itself; a rate needs two readings
        assert m.nics[("cx1", 1)] == {"up": 1.0}
        n_prom = sum(isinstance(c, str) and not c.endswith("/metrics") for c in calls)
        m.poll_once()
        assert m.nics[("cx1", 1)]["pps"] == 2e5
        assert m.nics[("cx1", 0)]["pps"] == 4e5 and m.nics[("cx1", 0)]["ring_full"] == 100
        # Prometheus waits PROMETHEUS_EVERY_S; kotekan is read every poll
        assert sum(isinstance(c, str) and not c.endswith("/metrics") for c in calls) == n_prom
        assert m.sources["kotekan"] == {"ok": True, "n": 1, "of": 1}
        assert m.psu["volts"] == 48.1
        assert outlet_watts(m.devices, "cx1") == 753 and m.devices["hosts"]["cx1"]["power"] == 4
        assert m.health == "degraded"
        assert "Ethernet12/1 (crate slot 4): down" in m.problems
        assert m.to_dict()["n_problems"] == len(m.problems)

    def test_a_stale_switch_monitor_degrades(self, monkeypatch, tmp_path):
        m, _ = self._monitor(monkeypatch, tmp_path)
        monkeypatch.setitem(REPLIES, "switch_monitor_last_success_timestamp_seconds", [_result(1.0)])
        m.poll_once()
        assert m.health == "degraded" and "has not polled the switch" in m.error

    def test_the_switch_from_its_eapi_when_choco_has_an_account(self, monkeypatch, tmp_path):
        eapi = SwitchEapi("https://switch.example/command-api", "ro", "pw")
        m, calls = self._monitor(monkeypatch, tmp_path, switch_eapi=eapi)
        m.poll_once()
        m.poll_once()
        assert ["show interfaces status", "show interfaces counters", "show interfaces counters discards"] in calls
        sw = m.switch["Ethernet9/1"]
        # the counters' change over the 1 s between the readings
        assert sw["in_bps"] == 9.7e9 * 8 and sw["in_pps"] == 1.5e6 and sw["out_discards"] == 5
        assert m.switch["Ethernet12/1"]["up"] == 0.0 and m.sources["switch"]["ok"]
        with pytest.raises(ValueError):
            eapi.run(["configure terminal"])
        with pytest.raises(ValueError):
            eapi.run(["show version; reload"])

    def test_prometheus_down_is_down(self, monkeypatch, tmp_path):
        m, _ = self._monitor(monkeypatch, tmp_path)

        def broken(*a, **kw):
            raise OSError("connection refused")
        monkeypatch.setattr(instrument, "fetch", broken)
        m.poll_once()
        assert m.health == "down" and "connection refused" in m.error


@pytest.fixture
def inst_app(app, configs_dir, monkeypatch, tmp_path):
    """The shared app with a telescope config on cx1/cx2 and the monitor fed
    by the fake Prometheus."""
    for name in ("cx1", "cx2"):
        (configs_dir / "cx" / f"{name}.yaml").write_text(yaml.safe_dump(TELESCOPE))
    for node in app.config["registry"].nodes.values():
        node.load_config()
    monkeypatch.setattr(instrument, "fetch", fake_fetch([]))
    (tmp_path / "wiring.csv").write_text(WIRING)
    m = app.config["instrument_monitor"]
    m.url, m.switch_instance, m.wiring = "http://prom.example:9090", None, WiringFile(tmp_path / "wiring.csv")
    m.poll_once()
    return app


class TestPage:
    def test_page_draws_the_path(self, inst_app):
        client = inst_app.test_client()
        login(client)
        body = client.get("/").data.decode()
        assert "<h3>Signal path" not in body and 'class="fl"' in body and "slot 3" in body and "B01–B02 X" in body
        # no page title; the facts and the description follow the diagram
        assert "<h2>Instrument</h2>" not in body
        assert body.index('class="fl"') < body.index('class="inst-facts"') < body.index('class="note"')
        # the link problems are listed on /switch only; here the diagram shows them
        assert "Ethernet12/1 (crate slot 4): down" not in body
        assert "/pipeline/cx/cx1" in body and "48.1 V" in body
        assert "Processing" in body and "Control" in body and "BFFS" in body
        # the tables live on their own pages; the cards link there
        assert "Ethernet33" not in body and "PDU outlets" not in body
        assert 'href="/switch"' in body and 'href="/service/power#supply"' in body
        part = client.get("/partials/instrument").data.decode()
        assert 'class="fl"' in part and "<html" not in part

    def test_switch_page_shows_each_link_end_to_end(self, inst_app):
        client = inst_app.test_client()
        login(client)
        body = client.get("/switch").data.decode()
        assert "CRS boards → switch" in body and "Switch → X-engines" in body
        assert "Ethernet12/1 (crate slot 4): down" in body
        assert "cx1 · 0" in body and "Ethernet33" in body and "Wiring table" in body
        part = client.get("/partials/switch").data.decode()
        assert "Switch → X-engines" in part and "<html" not in part

    def test_nodes_page_carries_each_server(self, inst_app):
        client = inst_app.test_client()
        login(client)
        body = client.get("/nodes").data.decode()
        assert "Server" in body and "iDRAC" in body and "753 W" in body
        assert "753 W" in client.get("/partials/dashboard-table").data.decode()

    def test_power_page_has_the_supply_and_the_outlets(self, inst_app):
        client = inst_app.test_client()
        login(client)
        assert client.get("/service/pdb?layout=dish").headers["Location"].endswith("/service/power?layout=dish")
        body = client.get("/service/power").data.decode()
        assert "Dish supply" in body and "48.10 V" in body and "PDU outlets" in body and "750 W" in body
        part = client.get("/partials/power-devices").data.decode()
        assert "PDU outlets" in part and "<html" not in part

    def test_badges_appear_in_the_strip_when_configured(self, inst_app, client):
        login(client)
        strip = client.get("/partials/services").data.decode()
        assert 'aria-label="SWITCH: degraded"' in strip and 'href="/switch"' in strip
        assert "PATH" not in strip and 'href="/service/power"' in strip and "POWER" in strip

    def test_unconfigured_page_says_how(self, client):
        login(client)
        body = client.get("/").data.decode()
        assert "instrument.prometheus_url" in body
        assert "SWITCH" not in client.get("/partials/services").data.decode()
        assert "Server" not in client.get("/nodes").data.decode()
