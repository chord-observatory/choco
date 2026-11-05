"""The instrument page: the signal path from the feeds to the files.

Three inputs, all read-only:

- the wiring table (``wiring.csv``): which switch port carries which CRS
  crate slot, and which switch port feeds which X-engine node's NIC port.
  Nothing on the network reports this, so it is kept by hand, like
  ``pdb_map.csv``, and checked against the live packet rates;
- kotekan's own config: ``dish_inputs`` and ``crs_board_remap`` place
  every feed on a board (``board_layout``), and ``missing_source_ids``
  names the boards expected to be silent;
- the devices, polled by :class:`InstrumentMonitor` every few seconds while
  the page is watched: each node's kotekan ``/metrics`` (the DPDK NIC
  counters) and, given a read-only account, the switch's eAPI (``show``
  commands only, :class:`SwitchEapi`); the CRS boards come from choco's
  own fpga_master monitor (its hardware map and per-board samples,
  :func:`board_state`);
- at most every ``SLOW_EVERY_S``, the slower sources, each read directly:
  every host's node_exporter (reachability, network rates, and on the
  transfer host the transfer job's textfile series), the rack PDU over
  SNMP (:class:`PduSnmp`, the ``snmpbulkwalk`` host tool), and over eAPI
  the switch's error counters, PHY (signal, FEC) and optics;
- the dish supply through its exporter (``/metrics?target=``): the
  supply's SCPI socket takes very few sessions, so the exporter holds the
  one and serves Prometheus and choco from it, live;
- Prometheus, on the same cadence, only for the switch until choco has
  its eAPI account (the switch monitor's ``switch_interface_*``).

Stdlib only, apart from ``http.fetch`` for the requests.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .dishlabels import PLACEHOLDER_LABEL, dish_table, find_key
import gevent

from .http import fetch
from .monitor import PolledMonitor

logger = logging.getLogger(__name__)

DEFAULT_WIRING_FILENAME = "wiring.csv"

_PORT_RE = re.compile(r"^Ethernet(\d{1,2})/1$")
_NODE_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")

#: Below this many packets per second a link counts as carrying nothing.
#: A healthy board port carries about 1.5 Mpps, a node port about 0.4 Mpps.
IDLE_PPS = 1000.0
#: A NIC that misses or drops more than this share of its packets is
#: flagged.  Healthy ports miss a few packets per second out of 1.5 Mpps
#: (about 1e-5), which costs nothing in the files.
DROP_FRACTION = 1e-4
#: Switch out-port and node NIC rates further apart than this are flagged.
RATE_MISMATCH = 0.05
#: The switch monitor polls every 30 s; older than this, its numbers are stale.
SWITCH_STALE_S = 180.0
#: The page asks for a poll this often while it is open; the background
#: loop (``InstrumentMonitor.INTERVAL_S``) keeps the header badge fresh.
PAGE_POLL_S = 5.0
#: The slow sources (node_exporter, the PDU, the switch's PHY and optics,
#: Prometheus) are read at most this often.
SLOW_EVERY_S = 30.0
#: fpga_master samples every board about every 11 s; a board whose newest
#: sample is older than this is not being read.
BOARD_STALE_S = 90.0
#: FPGA core temperature (°C): the warning and critical lines of the
#: FPGACoreTemperature alerts.
CORE_TEMP_WARN_C = 75.0
CORE_TEMP_BAD_C = 85.0


def port_key(name: str) -> tuple:
    """Sort key for switch interface names: Ethernet2/1 before Ethernet10/1."""
    return tuple(int(p) if p.isdigit() else p for p in re.split(r"(\d+)", name))


# ---- the wiring table ------------------------------------------------------

@dataclass(frozen=True)
class WiringRow:
    """One switch port: a crate slot (board side) or a node NIC port."""

    switch_port: str
    crate_slot: int | None = None
    node: str | None = None
    nic_port: int | None = None
    notes: str = ""


@dataclass
class Wiring:
    rows: list[WiringRow]
    errors: list[str]
    path: str | None = None
    mtime: float | None = None

    def by_slot(self) -> dict[int, WiringRow]:
        return {r.crate_slot: r for r in self.rows if r.crate_slot is not None}

    def by_nic(self) -> dict[tuple[str, int], WiringRow]:
        return {(r.node, r.nic_port): r for r in self.rows if r.node is not None}

    def by_port(self) -> dict[str, WiringRow]:
        return {r.switch_port: r for r in self.rows}


def parse_wiring(text: str) -> tuple[list[WiringRow], list[str]]:
    """Rows and per-line errors.  A bad row is reported and skipped, so
    one typo costs one link, not the table."""
    rows: list[WiringRow] = []
    errors: list[str] = []
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    need = {"switch_port", "crate_slot", "node", "nic_port"}
    if not reader.fieldnames or not need <= {f.strip() for f in reader.fieldnames}:
        return [], [f"header must name {', '.join(sorted(need))} (and optionally notes)"]
    seen_port: set[str] = set()
    seen_slot: set[int] = set()
    seen_nic: set[tuple[str, int]] = set()
    for n, raw in enumerate(reader, start=2):
        row = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
        port, slot, node, nic = (row.get(k, "") for k in ("switch_port", "crate_slot", "node", "nic_port"))
        where = f"row {n}"
        if not _PORT_RE.match(port):
            errors.append(f"{where}: switch_port {port!r} is not EthernetN/1")
            continue
        if port in seen_port:
            errors.append(f"{where}: {port} is listed twice")
            continue
        if slot and (node or nic):
            errors.append(f"{where}: {port} names both a crate slot and a node")
            continue
        entry = None
        if slot:
            if not slot.isdigit() or not 1 <= int(slot) <= 16:
                errors.append(f"{where}: crate_slot {slot!r} is not 1-16")
                continue
            if int(slot) in seen_slot:
                errors.append(f"{where}: crate slot {slot} is listed twice")
                continue
            seen_slot.add(int(slot))
            entry = WiringRow(port, crate_slot=int(slot), notes=row.get("notes", ""))
        elif node:
            if not _NODE_RE.match(node) or nic not in ("0", "1"):
                errors.append(f"{where}: node {node!r} / nic_port {nic!r} is not a node name and 0 or 1")
                continue
            if (node, int(nic)) in seen_nic:
                errors.append(f"{where}: {node} port {nic} is listed twice")
                continue
            seen_nic.add((node, int(nic)))
            entry = WiringRow(port, node=node, nic_port=int(nic), notes=row.get("notes", ""))
        else:
            errors.append(f"{where}: {port} names neither a crate slot nor a node")
            continue
        seen_port.add(port)
        rows.append(entry)
    return rows, errors


class WiringFile:
    """The table on disk, re-read when its mtime changes."""

    def __init__(self, path):
        self.path = Path(path) if path else None
        self._cached: Wiring | None = None

    def get(self) -> Wiring:
        if self.path is None:
            return Wiring([], ["no wiring file configured"])
        try:
            mtime = self.path.stat().st_mtime
        except OSError as e:
            return Wiring([], [f"{self.path}: {e.strerror or e}"], str(self.path))
        if self._cached is None or self._cached.mtime != mtime:
            try:
                text = self.path.read_text()
            except OSError as e:
                return Wiring([], [f"{self.path}: {e.strerror or e}"], str(self.path))
            rows, errors = parse_wiring(text)
            self._cached = Wiring(rows, errors, str(self.path), mtime)
        return self._cached


# ---- boards, from kotekan's config ------------------------------------------

def board_layout(config) -> tuple[list[dict], str | None]:
    """Each CRS board and the feeds on its ADCs, from a rendered X-engine
    config, in crate-slot order; or ``([], reason)``.

    The element axis is ``[P][D]``: output slot *i* of ``crs_board_remap``
    holds polarization ``i // (n_boards / n_pol)`` of dish group ``i % (n_boards
    / n_pol)``, and takes the board whose source_id is ``crs_board_remap[i]``
    (crate slot = source_id + 1).  ADC *k* of that board carries dish
    ``group * per_board + k``.
    """
    try:
        table = dish_table(config)
    except ValueError as e:
        return [], str(e)
    if table is None:
        return [], "the X-engine config has no dish_inputs table"
    labels, kinds, npol = table
    remap = find_key(config, "crs_board_remap")
    if not isinstance(remap, list) or not remap or not all(isinstance(s, int) for s in remap):
        return [], "the X-engine config has no crs_board_remap"
    nboard = len(remap)
    if nboard % npol or len(labels) % (nboard // npol):
        return [], f"{len(labels)} dishes and {npol} polarizations do not fill {nboard} boards"
    groups = nboard // npol
    per_board = len(labels) // groups
    silent = find_key(config, "missing_source_ids") or []
    silent = {s for s in silent if isinstance(s, int)}
    pols = "XY" if npol == 2 else [str(p) for p in range(npol)]
    boards = []
    for i, source_id in enumerate(remap):
        pol, group = divmod(i, groups)
        dishes = labels[group * per_board:(group + 1) * per_board]
        dkinds = kinds[group * per_board:(group + 1) * per_board]
        boards.append({
            "slot": source_id + 1,
            "source_id": source_id,
            "pol": pols[pol],
            "group": group,
            "feeds": [{"adc": k, "dish": d, "label": f"{d}{pols[pol]}" if d != PLACEHOLDER_LABEL else None,
                       "kind": kind}
                      for k, (d, kind) in enumerate(zip(dishes, dkinds))],
            "silent_expected": source_id in silent,
        })
    boards.sort(key=lambda b: b["slot"])
    return boards, None


# ---- Prometheus ------------------------------------------------------------

#: Instant queries, by the snapshot field they fill.  ``{sw}`` is the
#: switch instance selector, filled in at poll time.
_SWITCH_QUERIES = {
    "up": "switch_interface_up{sw}",
    "signal": "switch_interface_signal_detect{sw}",
    "fec_lock": "switch_interface_fec_alignment_lock{sw}",
    "bps": "sum by (interface, direction) (rate(switch_interface_octets_total{sw}[2m])) * 8",
    "pps": "sum by (interface, direction) (rate(switch_interface_packets_total{sw}[2m]))",
    "errors": "sum by (interface) (rate(switch_interface_errors_total{sw}[5m]))",
    "discards": "sum by (interface, direction) (rate(switch_interface_discards_total{sw}[5m]))",
    "fec_corr": "rate(switch_interface_fec_corrected_codewords_total{sw}[5m])",
    "fec_unc": "rate(switch_interface_fec_uncorrected_codewords_total{sw}[5m])",
    "rx_dbm": "min by (interface) (switch_interface_rx_power_dbm{sw})",
}
#: PDU outlet names of the devices that are not hosts (a host's outlets
#: carry its name).
SWITCH_OUTLET = "arista"
CRATE_OUTLET = "f-engine"
#: The dish supply exporter's series, by the field they fill; any
#: ``keysight_psu_fault{fault=...}`` set makes ``fault`` 1.
_SUPPLY_SERIES = {
    "keysight_psu_up": "up",
    "keysight_psu_output_on": "output_on",
    "keysight_psu_voltage_volts": "volts",
    "keysight_psu_current_amps": "amps",
}
#: The exporter opens the SCPI session and runs ten queries on a cold read.
SUPPLY_TIMEOUT_S = 15.0


#: node_exporter's port and the collectors read: the network counters and
#: the textfile directory, where the transfer host writes the transfer
#: job's series (chord-sysops ``kotekan_vis_transfer``).
NODE_EXPORTER_PORT = 9100
NODE_EXPORTER_PARAMS = {"collect[]": ["netdev", "textfile"]}
#: The transfer job's textfile series, by the field they fill.
_TRANSFER_SERIES = {
    "kotekan_vis_transfer_running": "running",
    "kotekan_vis_transfer_consecutive_failures": "failures",
    "kotekan_vis_transfer_last_success_timestamp_seconds": "last_success",
    "kotekan_vis_transfer_last_start_timestamp_seconds": "last_start",
    "kotekan_vis_transfer_last_run_bytes_transferred": "last_run_bytes",
}
_SAMPLE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)")
#: The Raritan PDU's PDU2-MIB columns (PDU 1): outlet names, outlet and
#: inlet sensor readings with the outlets' decimal digits.  A sensor row's
#: last index is its type; 5 is active power, in watts.
_PDU_OUTLET_NAME = "1.3.6.1.4.1.13742.6.3.5.3.1.3.1"
_PDU_OUTLET_VALUE = "1.3.6.1.4.1.13742.6.5.4.3.1.4.1"
_PDU_OUTLET_DIGITS = "1.3.6.1.4.1.13742.6.3.5.4.1.7.1"
_PDU_INLET_VALUE = "1.3.6.1.4.1.13742.6.5.2.3.1.4.1"
ACTIVE_POWER = 5
#: The thresholds of chord-sysops' KotekanVisTransfer* alerts, so the card
#: and Grafana agree: failing at two runs in a row, stale when idle with
#: no success for 2 h, long when one run has gone 12 h (a baseband dump
#: takes hours).
TRANSFER_FAILING = 2
TRANSFER_STALE_S = 2 * 3600
TRANSFER_LONG_S = 12 * 3600


def transfer_state(t: dict | None, now: float) -> tuple[str, str]:
    """``(tone, word)`` of the transfer from its series (``_TRANSFER_SERIES``
    fields), judged as the chord-sysops alerts judge it."""
    if not t:
        return "off", "no data"
    failures = t.get("failures") or 0
    running = bool(t.get("running"))
    last_success, last_start = t.get("last_success"), t.get("last_start")
    if failures >= TRANSFER_FAILING:
        return "bad", f"{failures:.0f} runs failed"
    if running and last_start and now - last_start > TRANSFER_LONG_S:
        return "warn", "running long"
    if not running and (not last_success or now - last_success > TRANSFER_STALE_S):
        return "warn", "stale"
    if failures:
        return "warn", "last run failed"
    return "ok", "sending" if running else "idle"


def _short_host(instance: str) -> str:
    """``cx27.site.chord-observatory.ca:12048`` -> ``cx27``."""
    return instance.split(":")[0].split(".")[0]


def _pdu_name(instance: str) -> str:
    """A PDU target as Prometheus names it: a hostname shortened, an address kept."""
    host = instance.split(":")[0]
    return host if re.fullmatch(r"[\d.]+", host) else host.split(".")[0]


def parse_node_exporter(text: str) -> dict:
    """node_exporter's text -> ``{"rx", "tx", "transfer"}``: the network
    byte counters summed over the host's interfaces but loopback (None when
    the netdev collector gave none; DPDK traffic does not show here) and
    the transfer job's series where the host writes them."""
    rx = tx = 0.0
    seen = False
    transfer: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            continue
        name, labels, value = m.groups()
        try:
            v = float(value)
        except ValueError:
            continue
        if name in ("node_network_receive_bytes_total", "node_network_transmit_bytes_total"):
            if 'device="lo"' in (labels or ""):
                continue
            seen = True
            if name == "node_network_receive_bytes_total":
                rx += v
            else:
                tx += v
        elif name in _TRANSFER_SERIES:
            transfer[_TRANSFER_SERIES[name]] = v
    return {"rx": rx if seen else None, "tx": tx if seen else None, "transfer": transfer}


def parse_supply(text: str) -> dict | None:
    """The dish supply exporter's answer -> ``{up, output_on, volts, amps,
    fault}``, ``fault`` 1 when any fault bit is set; None without
    ``keysight_psu_up`` (not the exporter's answer for a supply)."""
    out: dict[str, float] = {}
    fault = 0.0
    for line in text.splitlines():
        m = _SAMPLE_RE.match(line) if line and line[0] != "#" else None
        if not m:
            continue
        name, _labels, value = m.groups()
        try:
            v = float(value)
        except ValueError:
            continue
        if name in _SUPPLY_SERIES:
            out[_SUPPLY_SERIES[name]] = v
        elif name == "keysight_psu_fault" and v:
            fault = 1.0
    if "up" not in out:
        return None
    return {**out, "fault": fault}


def parse_snmp_walk(text: str, base: str) -> dict[tuple[int, ...], str]:
    """``snmpbulkwalk -On -Oq`` lines under *base* -> {index: value}, the
    index the OID's components after *base*, a string's quotes removed."""
    out: dict[tuple[int, ...], str] = {}
    prefix = "." + base.lstrip(".") + "."
    for line in text.splitlines():
        oid, _, value = line.strip().partition(" ")
        if not oid.startswith(prefix):
            continue
        try:
            index = tuple(int(x) for x in oid[len(prefix):].split("."))
        except ValueError:
            continue
        value = value.strip()
        out[index] = value[1:-1] if len(value) >= 2 and value[0] == value[-1] == '"' else value
    return out


class PduSnmp:
    """The rack PDU (Raritan, PDU2-MIB) over SNMP v2c, read with net-snmp's
    ``snmpbulkwalk`` (a host tool, like ``dot``); read-only by protocol use:
    only walks are sent."""

    def __init__(self, host: str, community: str = "public", timeout: float = 5.0):
        self.host, self.community, self.timeout = host, community, float(timeout)

    @property
    def name(self) -> str:
        """The PDU as the outlet table names it: a hostname shortened, an address kept."""
        return _pdu_name(self.host)

    def _walk(self, base: str) -> dict[tuple[int, ...], str]:
        from .hosttool import run_tool
        r = run_tool(["snmpbulkwalk", "-v2c", "-c", self.community, "-t", f"{self.timeout:g}", "-r", "1",
                      "-On", "-Oq", self.host, base], timeout=3 * self.timeout + 5)
        if r is None:
            raise OSError("snmpbulkwalk is not installed or did not finish")
        if r.returncode != 0:
            lines = (r.stderr or "").strip().splitlines()
            raise OSError(lines[-1] if lines else f"snmpbulkwalk exit {r.returncode}")
        return parse_snmp_walk(r.stdout, base)

    def read(self) -> dict:
        """``{"outlets": [{outlet, name, pdu, watts}], "inlet_w"}``; raises
        OSError when the PDU does not answer."""
        names = self._walk(_PDU_OUTLET_NAME)
        values = self._walk(_PDU_OUTLET_VALUE)
        digits = self._walk(_PDU_OUTLET_DIGITS)
        inlets = self._walk(_PDU_INLET_VALUE)
        outlets = []
        for (outlet, sensor), v in sorted(values.items()):
            if sensor != ACTIVE_POWER:
                continue
            try:
                watts = float(v) / 10 ** int(digits.get((outlet, sensor), "0"))
            except ValueError:
                continue
            outlets.append({"outlet": outlet, "name": names.get((outlet,), ""), "pdu": self.name, "watts": watts})
        inlet = [float(v) for (_inlet, sensor), v in inlets.items() if sensor == ACTIVE_POWER and v.isdigit()]
        return {"outlets": outlets, "inlet_w": sum(inlet) if inlet else None}


def outlet_watts(devices: dict | None, name: str) -> float | None:
    """The power drawn on the PDU outlets labelled *name*, or None."""
    ws = [o["watts"] for o in (devices or {}).get("outlets", []) if o["name"] == name]
    return sum(ws) if ws else None


def host_health(name: str, devices: dict | None) -> dict:
    """A server's PDU power and its node_exporter: reachable (``host_up``)
    and the network rates, from the monitor's ``devices``."""
    h = (devices or {}).get("hosts", {}).get(name) or {}
    return {"watts": outlet_watts(devices, name), "host_up": h.get("up"),
            "rx_bps": h.get("rx_bps"), "tx_bps": h.get("tx_bps")}


#: kotekan /metrics lines the NIC rows are made of, by the field they fill.
_NIC_METRICS = {
    "kotekan_dpdk_nic_link_up": "up",
    "kotekan_dpdk_nic_rx_packets_total": "packets",
    "kotekan_dpdk_nic_rx_missed_total": "missed_total",
    "kotekan_dpdk_nic_rx_errors_total": "errors_total",
    "kotekan_dpdk_distributor_ring_full_dropped_packets_total": "ring_full_total",
}
_METRIC_RE = re.compile(r'^([a-z_]+)\{([^}]*)\}\s+(\S+)(?:\s+(\d+))?$')
_LABEL_RE = re.compile(r'(\w+)="([^"]*)"')


def parse_nic_metrics(text: str) -> dict[int, dict]:
    """kotekan's ``/metrics`` -> {port: {field: (value, timestamp_s|None)}}
    for the ``_NIC_METRICS`` lines; a counter split over stages is summed."""
    out: dict[int, dict] = {}
    for line in text.splitlines():
        if not line.startswith("kotekan_dpdk_"):
            continue
        m = _METRIC_RE.match(line)
        if not m or m.group(1) not in _NIC_METRICS:
            continue
        port = dict(_LABEL_RE.findall(m.group(2))).get("port", "")
        if not port.isdigit():
            continue
        try:
            v = float(m.group(3))
        except ValueError:
            continue
        ts = int(m.group(4)) / 1e3 if m.group(4) else None
        field = _NIC_METRICS[m.group(1)]
        prev = out.setdefault(int(port), {}).get(field)
        out[int(port)][field] = (v + prev[0], ts) if prev else (v, ts)
    return out


class Rates:
    """Per-second rates from successive counter readings: the first
    reading and a counter that went backwards (a restart) give None."""

    def __init__(self):
        self._last: dict = {}

    def __call__(self, key, value: float, t: float):
        prev = self._last.get(key)
        self._last[key] = (value, t)
        if prev is None or t <= prev[1] or value < prev[0]:
            return None
        return (value - prev[0]) / (t - prev[1])


#: Commands the switch reader may send.  Nothing else ever leaves choco.
_SHOW_RE = re.compile(r"^show [a-z0-9 /|^.-]+$")
_SWITCH_CMDS = ["show interfaces status", "show interfaces counters", "show interfaces counters discards"]
#: Read on the slow cadence: the error counters, the PHY (signal detect,
#: FEC lock and codewords) and the optics, as the switch monitor reads them.
_SWITCH_SLOW_CMDS = ["show interfaces counters errors", "show interfaces phy detail",
                     "show interfaces transceiver"]
#: The error counters summed into ``errors`` (the switch monitor's set).
_ERROR_KEYS = ("fcsErrors", "alignmentErrors", "symbolErrors", "inErrors", "frameTooShorts",
               "frameTooLongs", "outErrors")


class SwitchEapi:
    """The switch's JSON-RPC API, read-only: every command must be a
    ``show``, refused here before anything is sent; no ``enable``."""

    def __init__(self, url: str, username: str, password: str, timeout: float = 5.0):
        self.url, self.auth, self.timeout = url, (username, password), timeout

    def run(self, cmds: list[str]) -> list[dict]:
        for c in cmds:
            if not _SHOW_RE.match(c):
                raise ValueError(f"read-only: refusing to send {c!r} to the switch")
        resp = fetch("POST", self.url, timeout=self.timeout, auth=self.auth, verify=False,
                     json={"jsonrpc": "2.0", "method": "runCmds", "id": 1,
                           "params": {"version": 1, "format": "json", "cmds": cmds}})
        body = resp.json()
        if "error" in body:
            raise ValueError(f"eAPI: {body['error'].get('message', body['error'])}")
        return body["result"]


def parse_switch(results: list[dict], rates: Rates, t: float) -> dict[str, dict]:
    """``_SWITCH_CMDS`` results -> the switch snapshot's fields (``up``,
    ``status``, ``type``, ``in_/out_bps``, ``in_/out_pps``,
    ``in_/out_discards``), rates from the counters' change since the last
    reading, timed by the switch's own ``lastUpdateTimestamp`` where given
    (it refreshes the counters every few seconds, not at each read)."""
    status, counters, discards = results
    out: dict[str, dict] = {}
    for name, d in (status.get("interfaceStatuses") or {}).items():
        out[name] = {"up": 1.0 if d.get("linkStatus") == "connected" else 0.0,
                     "status": d.get("linkStatus"), "type": d.get("interfaceType")}
    for name, c in (counters.get("interfaces") or {}).items():
        e = out.setdefault(name, {})
        tc = c.get("lastUpdateTimestamp") if isinstance(c.get("lastUpdateTimestamp"), (int, float)) else t
        for way in ("in", "out"):
            octets = c.get(f"{way}Octets")
            pkts = sum(c.get(f"{way}{k}Pkts") or 0 for k in ("Ucast", "Multicast", "Broadcast"))
            if isinstance(octets, (int, float)):
                bps = rates((name, way, "octets"), octets, tc)
                if bps is not None:
                    e[f"{way}_bps"] = bps * 8
            pps = rates((name, way, "pkts"), pkts, tc)
            if pps is not None:
                e[f"{way}_pps"] = pps
    for name, c in (discards.get("interfaces") or {}).items():
        e = out.setdefault(name, {})
        tc = (counters.get("interfaces") or {}).get(name, {}).get("lastUpdateTimestamp")
        tc = tc if isinstance(tc, (int, float)) else t
        for way in ("in", "out"):
            v = c.get(f"{way}Discards")
            if isinstance(v, (int, float)):
                r = rates((name, way, "discards"), v, tc)
                if r is not None:
                    e[f"{way}_discards"] = r
    return out


def _eos_value(obj):
    """eAPI's phy-detail fields are ``{"value": x, "changes": n, ...}``."""
    return obj["value"] if isinstance(obj, dict) and "value" in obj else None


def _long_port(name: str) -> str:
    return "Ethernet" + name[2:] if re.match(r"^Et\d", name) else name


def parse_switch_slow(results: list[dict], rates: Rates, t: float) -> dict[str, dict]:
    """``_SWITCH_SLOW_CMDS`` results -> ``errors`` (all error counters, a
    rate), ``signal`` and ``fec_lock`` (1/0), ``fec_corr`` / ``fec_unc``
    (codewords per second) and ``rx_dbm`` (the weakest lane's receive
    power; a four-lane optic listed lane by lane folds into its /1 port)."""
    errors, phy, xcvr = results
    out: dict[str, dict] = {}
    for name, c in (errors.get("interfaceErrorCounters") or {}).items():
        total = sum(v for k, v in c.items() if k in _ERROR_KEYS and isinstance(v, (int, float)))
        r = rates((name, "errors"), total, t)
        if r is not None:
            out.setdefault(name, {})["errors"] = r
    statuses = phy.get("interfacePhyStatuses") or {}
    for name, d in statuses.items():
        ps = (d.get("phyStatuses") or [{}])[0]
        e = out.setdefault(name, {})
        signal = _eos_value((ps.get("pma") or {}).get("signalDetect"))
        if isinstance(signal, bool):
            e["signal"] = float(signal)
        fec = ps.get("fec") or {}
        lock = _eos_value(fec.get("alignmentLock"))
        if isinstance(lock, bool):
            e["fec_lock"] = float(lock)
        for key, field in (("correctedCodewords", "fec_corr"), ("uncorrectedCodewords", "fec_unc")):
            v = _eos_value(fec.get(key))
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                r = rates((name, field), v, t)
                if r is not None:
                    e[field] = r
    for port, d in (xcvr.get("interfaces") or {}).items():
        if not isinstance(d, dict):
            continue
        name = _long_port(port)
        m = re.match(r"^(Ethernet\d+)/\d+$", name)
        if m and name not in statuses and f"{m.group(1)}/1" in statuses:
            name = f"{m.group(1)}/1"
        params = d.get("parameters")
        if isinstance(params, dict):
            chans = (params.get("rxPower") or {}).get("channels") or {}
            vals = [float(v) for ch, v in chans.items() if str(ch).isdigit() and isinstance(v, (int, float))]
        else:
            vals = [float(d["rxPower"])] if isinstance(d.get("rxPower"), (int, float)) else []
        if vals:
            e = out.setdefault(name, {})
            e["rx_dbm"] = min(vals + ([e["rx_dbm"]] if "rx_dbm" in e else []))
    return out


class InstrumentMonitor(PolledMonitor):
    """Reads the X-engine NICs from kotekan, the switch from its eAPI (with
    an account, else Prometheus), each host's node_exporter, the rack PDU
    over SNMP (*pdu*, a :class:`PduSnmp`) and the dish supply through its
    exporter (*supply*: ``(exporter URL, target)``).

    *nodes* returns ``[(short name, kotekan base URL)]``, read at each
    poll; their hosts, and *transfer_host* (the host that runs the
    acquisitions' transfer), are the node_exporters read.  ``health`` is the
    signal path's: ``ok`` when kotekan, the switch and Prometheus (where
    used) answer and the switch numbers are fresh, ``degraded`` when one
    does not or a wired link is down, ``down`` when nothing answers.
    node_exporter, the PDU and the supply are reported in ``sources`` but
    judged by their own pages (the PDU and the supply have their badges).
    """

    INTERVAL_S = 30

    def __init__(self, url: str | None, switch_instance: str | None = None,
                 wiring: WiringFile | None = None, timeout: float = 5.0,
                 nodes=None, switch_eapi: SwitchEapi | None = None,
                 pdu: PduSnmp | None = None, transfer_host: str | None = None,
                 supply: tuple[str, str] | None = None):
        self.url = (url or "").rstrip("/") or None
        self.switch_instance = switch_instance or None
        self.wiring = wiring or WiringFile(None)
        self.timeout = float(timeout)
        self.nodes = nodes or (lambda: [])
        self.eapi = switch_eapi
        self.pdu = pdu
        self.supply = supply
        self.transfer_host = transfer_host or None
        self.switch: dict[str, dict] = {}
        self.nics: dict[tuple[str, int], dict] = {}
        self.psu: dict | None = None
        self.devices: dict = {}
        self.switch_monitor: dict = {}
        self.transfer: dict | None = None
        self.sources: dict[str, dict] = {}
        self.problems: list[str] = []
        self.last_seen: float | None = None
        self._prom_switch: dict[str, dict] = {}
        self._eapi_slow: dict[str, dict] = {}
        self._slow_at: float | None = None
        self._rates = Rates()
        self._busy = False
        super().__init__()

    @property
    def configured(self) -> bool:
        return (bool(self.url) or self.eapi is not None or self.pdu is not None or bool(self.transfer_host)
                or self.supply is not None)

    # host/port for the strip's tooltip, like the other monitors
    @property
    def host(self) -> str | None:
        return urlsplit(self.url).hostname if self.url else None

    @property
    def port(self) -> int | None:
        return urlsplit(self.url).port if self.url else None

    def _query(self, expr: str) -> list[dict]:
        resp = fetch("GET", f"{self.url}/api/v1/query", params={"query": expr},
                     timeout=self.timeout)
        body = resp.json()
        if body.get("status") != "success":
            raise ValueError(f"Prometheus: {body.get('error') or body.get('status')}")
        return body["data"]["result"]

    def poll_once(self) -> None:
        # two watchers' polls, or a watcher's and the loop's, never overlap
        if self._busy:
            return
        self._busy = True
        try:
            self._poll()
        finally:
            self._busy = False

    def _poll(self) -> None:
        now = self.last_polled = time.time()
        if not self.configured:
            self.health, self.error = "unconfigured", "no instrument source configured"
            return
        sources = {}
        nics, kotekan = self._poll_nics()
        if kotekan:
            sources["kotekan"] = kotekan
        slow = self._slow_at is None or now - self._slow_at >= SLOW_EVERY_S
        direct = None
        if self.eapi is not None:
            try:
                direct = parse_switch(self.eapi.run(_SWITCH_CMDS), self._rates, time.time())
                if slow:
                    self._eapi_slow = parse_switch_slow(self.eapi.run(_SWITCH_SLOW_CMDS), self._rates, time.time())
                for name, fields in self._eapi_slow.items():
                    direct.setdefault(name, {}).update(fields)
                sources["switch"] = {"ok": True, "via": "eAPI"}
            except (OSError, ValueError, KeyError, TypeError) as e:
                sources["switch"] = {"ok": False, "via": "eAPI", "error": f"switch eAPI: {e}"}
        if slow:
            sources.update(self._poll_slow())
            self._slow_at = now
        else:
            sources.update({k: v for k, v in self.sources.items()
                            if k in ("prometheus", "node_exporter", "pdu", "supply")})
        switch = {k: dict(v) for k, v in self._prom_switch.items()} if direct is None else {}
        for name, fields in (direct or {}).items():
            switch.setdefault(name, {}).update(fields)
        self.switch, self.nics, self.sources = switch, nics, sources
        if any(src.get("ok") for src in sources.values()):
            self.last_seen = now
        self.problems = link_problems(self.wiring.get(), switch, nics)
        path = {k: v for k, v in sources.items() if k in ("kotekan", "switch", "prometheus")}
        errors = [src["error"] for src in path.values() if not src.get("ok")]
        mon = self.switch_monitor
        age = now - mon["last_success"] if mon.get("last_success") else None
        if direct is None and not errors and (not mon.get("up") or age is None or age > SWITCH_STALE_S):
            errors.append("the switch monitor is down" if not mon.get("up") else
                          "the switch monitor has not polled the switch for " +
                          (f"{age / 60:.0f} min" if age is not None else "ever"))
        if not any(src.get("ok") for src in (path or sources).values()):
            self.health, self.error = "down", "; ".join(errors) or "no source answered"
        elif errors or self.problems:
            self.health, self.error = "degraded", (errors or self.problems)[0]
        else:
            self.health, self.error = "ok", None

    def _poll_nics(self) -> tuple[dict, dict | None]:
        """Every node's kotekan ``/metrics`` at once; the NIC rows and the
        source's state (None without nodes)."""
        nodes = list(self.nodes())
        if not nodes:
            return {}, None

        def get(base):
            try:
                return fetch("GET", f"{base}/metrics", timeout=self.timeout).text
            except OSError:
                return None
        jobs = [gevent.spawn(get, base) for _, base in nodes]
        gevent.joinall(jobs, timeout=self.timeout + 1)
        nics: dict[tuple[str, int], dict] = {}
        answered = 0
        for (name, _), job in zip(nodes, jobs):
            text = job.value if job.successful() else None
            if text is None:
                continue
            answered += 1
            t_poll = time.time()
            for port, fields in parse_nic_metrics(text).items():
                row = {}
                for field, (v, ts) in fields.items():
                    t = ts or t_poll
                    if field == "up":
                        row["up"] = v
                    elif field == "packets":
                        r = self._rates((name, port, field), v, t)
                        if r is not None:
                            row["pps"] = r
                    else:
                        r = self._rates((name, port, field), v, t)
                        if r is not None:
                            row[field[:-6]] = r
                if row:
                    nics[(name, port)] = row
        return nics, {"ok": answered > 0, "n": answered, "of": len(nodes),
                      **({} if answered else {"error": "no kotekan /metrics answered"})}

    def _poll_slow(self) -> dict[str, dict]:
        """The slow sources, each on its own: node_exporter, the PDU, the
        dish supply and (for the switch, without the account) Prometheus;
        their states for ``sources``."""
        sources: dict[str, dict] = {}
        hosts, transfer, state = self._poll_node_exporters()
        if state:
            sources["node_exporter"] = state
        outlets = {"outlets": [], "inlet_w": None}
        if self.pdu is not None:
            try:
                outlets = self.pdu.read()
                sources["pdu"] = {"ok": True, "via": "SNMP"}
            except OSError as e:
                outlets = {k: self.devices.get(k) for k in ("outlets", "inlet_w")} if self.devices else outlets
                sources["pdu"] = {"ok": False, "via": "SNMP", "error": f"PDU {self.pdu.host}: {e}"}
        self.devices = {**outlets, "hosts": hosts}
        self.transfer = transfer
        if self.supply is not None:
            url, target = self.supply
            try:
                text = fetch("GET", f"{url.rstrip('/')}/metrics", params={"target": target},
                             timeout=max(self.timeout, SUPPLY_TIMEOUT_S)).text
                self.psu = parse_supply(text)
                sources["supply"] = ({"ok": True, "via": "exporter"} if self.psu is not None else
                                     {"ok": False, "via": "exporter", "error": f"{url}: not a supply's answer"})
            except OSError as e:
                self.psu = None
                sources["supply"] = {"ok": False, "via": "exporter", "error": f"dish supply exporter {url}: {e}"}
        if self.url and self.eapi is None:
            try:
                self._poll_prometheus()
                sources["prometheus"] = {"ok": True}
            except (OSError, ValueError, KeyError, TypeError) as e:
                sources["prometheus"] = {"ok": False, "error": f"Prometheus at {self.url}: {e}"}
        return sources

    def _poll_node_exporters(self) -> tuple[dict, dict | None, dict | None]:
        """Every host's node_exporter at once: ``hosts`` ({short name: up,
        rx_bps, tx_bps}), the transfer host's transfer series (None when it
        gave none) and the source's state (None without hosts)."""
        targets = [(name, urlsplit(base).hostname) for name, base in self.nodes()]
        if self.transfer_host:
            targets.append((_short_host(self.transfer_host), self.transfer_host))
        targets = [(n, h) for n, h in dict(targets).items() if h]
        if not targets:
            return {}, None, None

        def get(host):
            try:
                return fetch("GET", f"http://{host}:{NODE_EXPORTER_PORT}/metrics", params=NODE_EXPORTER_PARAMS,
                             timeout=self.timeout).text
            except OSError:
                return None
        jobs = [gevent.spawn(get, host) for _, host in targets]
        gevent.joinall(jobs, timeout=self.timeout + 1)
        hosts: dict[str, dict] = {}
        transfer = None
        for (name, _), job in zip(targets, jobs):
            text = job.value if job.successful() else None
            if text is None:
                hosts[name] = {"up": 0.0}
                continue
            got, t = parse_node_exporter(text), time.time()
            h = hosts[name] = {"up": 1.0}
            for way in ("rx", "tx"):
                if got[way] is not None:
                    r = self._rates((name, "host", way), got[way], t)
                    if r is not None:
                        h[f"{way}_bps"] = r * 8
            if self.transfer_host and name == _short_host(self.transfer_host) and got["transfer"]:
                transfer = {**got["transfer"], "host": name}
        answered = sum(1 for h in hosts.values() if h["up"])
        return hosts, transfer, {"ok": answered > 0, "n": answered, "of": len(targets),
                                 **({} if answered else {"error": "no node_exporter answered"})}

    def _poll_prometheus(self) -> None:
        """The switch, while choco has no eAPI account."""
        sw = f'{{instance="{self.switch_instance}"}}' if self.switch_instance else ""
        switch: dict[str, dict] = {}
        for field, q in _SWITCH_QUERIES.items():
            for r in self._query(q.replace("{sw}", sw)):
                m, v = r["metric"], float(r["value"][1])
                entry = switch.setdefault(m.get("interface", "?"), {})
                if "direction" in m:
                    entry[f"{m['direction']}_{field}"] = v
                else:
                    entry[field] = v
        for r in self._query(f"switch_interface_info{sw}"):
            entry = switch.setdefault(r["metric"].get("interface", "?"), {})
            entry["type"] = r["metric"].get("type")
            entry["speed"] = r["metric"].get("speed")
        for r in self._query(f"switch_interface_status_info{sw}"):
            entry = switch.setdefault(r["metric"].get("interface", "?"), {})
            entry["status"] = r["metric"].get("status")
        mon = {}
        for field, q in (("up", f"switch_monitor_up{sw}"),
                         ("last_success", f"switch_monitor_last_success_timestamp_seconds{sw}")):
            for r in self._query(q):
                mon[field] = float(r["value"][1])
        self._prom_switch, self.switch_monitor = switch, mon

    def to_dict(self) -> dict:
        return {
            "configured": self.configured,
            "host": self.host,
            "port": self.port,
            "health": self.health,
            "error": self.error,
            "last_polled": self.last_polled,
            "last_seen": self.last_seen,
            "n_problems": len(self.problems),
            "sources": self.sources,
        }


# ---- judging links -----------------------------------------------------------

def _num(d: dict | None, key: str):
    v = (d or {}).get(key)
    return v if isinstance(v, (int, float)) else None


def in_port_state(stats: dict | None, silent_expected: bool = False) -> tuple[str, str]:
    """(tone, word) for a board-facing switch port."""
    if silent_expected:
        return "off", "silent (listed)"
    if stats is None:
        return "off", "no data"
    if not stats.get("up"):
        return "bad", "down"
    pps = _num(stats, "in_pps")
    if pps is not None and pps < IDLE_PPS:
        return "warn", "no traffic"
    if (_num(stats, "fec_unc") or 0) > 0 or (_num(stats, "errors") or 0) > 0:
        return "warn", "errors"
    return "ok", "up"


def board_state(source_id: int, fpga: dict | None, now: float | None = None) -> tuple[str, str]:
    """(tone, word) for a CRS board as fpga_master sees it.

    *source_id* is fpga_master's slot index (crate slot − 1), the key of
    *fpga*, ``FpgaMonitor.to_dict()``.  ``off`` whenever fpga_master gives
    nothing current about the board: not configured, down, not running,
    the slot not in its hardware map, no samples or samples older than
    :data:`BOARD_STALE_S`.  Otherwise the core temperature and the QSFP
    decide.
    """
    if not fpga or not fpga.get("configured"):
        return "off", "no fpga_master"
    if fpga.get("health") == "down":
        return "off", "fpga_master down"
    if str(source_id) not in (fpga.get("hw_map") or {}):
        state = fpga.get("state")
        return "off", f"fpga_master {state}" if state and state != "on" else "not in the hardware map"
    if not fpga["hw_map"][str(source_id)]:
        return "off", "empty slot"
    b = (fpga.get("boards") or {}).get(str(source_id))
    if not b or not b.get("sample_ts"):
        return "off", "no samples"
    age = (time.time() if now is None else now) - b["sample_ts"]
    if age > BOARD_STALE_S:
        return "off", f"samples {age / 60:.0f} min old"
    temp = b.get("core_temp")
    if temp is not None and temp > CORE_TEMP_BAD_C:
        return "bad", f"core {temp:.0f} °C"
    if temp is not None and temp > CORE_TEMP_WARN_C:
        return "warn", f"core {temp:.0f} °C"
    if b.get("qsfp") is False:
        return "warn", "no QSFP"
    return "ok", "ok"


def board_facts(source_id: int, fpga: dict | None) -> str:
    """One line of what fpga_master knows about the board, for a tooltip."""
    if not fpga:
        return ""
    parts = []
    serials = (fpga.get("hw_map") or {}).get(str(source_id)) or []
    if serials:
        parts.append(", ".join(serials))
    b = (fpga.get("boards") or {}).get(str(source_id)) or {}
    if b.get("core_temp") is not None:
        parts.append(f"core {b['core_temp']:.0f} °C")
    if b.get("board_temp") is not None:
        parts.append(f"board {b['board_temp']:.0f} °C")
    if "qsfp" in b:
        parts.append("QSFP present" if b["qsfp"] else "no QSFP")
    if b.get("clock"):
        parts.append(f"clock {b['clock']}")
    if b.get("adc_overflow") or b.get("fft_overflow"):
        parts.append(f"overflows ADC {b.get('adc_overflow', 0):.0f} FFT {b.get('fft_overflow', 0):.0f}")
    return " · ".join(parts)


def out_port_state(stats: dict | None) -> tuple[str, str]:
    """(tone, word) for a node-facing switch port."""
    if stats is None:
        return "off", "no data"
    if not stats.get("up"):
        return "bad", "down"
    pps = _num(stats, "out_pps")
    if pps is not None and pps < IDLE_PPS:
        return "warn", "no traffic"
    if (_num(stats, "out_discards") or 0) > 0 or (_num(stats, "fec_unc") or 0) > 0:
        return "warn", "errors"
    return "ok", "up"


def nic_state(stats: dict | None) -> tuple[str, str]:
    """(tone, word) for an X-engine NIC port; no series means kotekan is not exporting."""
    if not stats:
        return "off", "no kotekan"
    if not stats.get("up"):
        return "bad", "link down"
    pps = _num(stats, "pps")
    if pps is not None and pps < IDLE_PPS:
        return "warn", "no traffic"
    lost = (_num(stats, "missed") or 0) + (_num(stats, "ring_full") or 0) + (_num(stats, "errors") or 0)
    if pps and lost > DROP_FRACTION * pps:
        return "warn", "drops"
    return "ok", "up"


def rate_mismatch(out_pps, nic_pps) -> bool:
    if out_pps is None or nic_pps is None or max(out_pps, nic_pps) < IDLE_PPS:
        return False
    return abs(out_pps - nic_pps) > RATE_MISMATCH * max(out_pps, nic_pps)


def link_problems(wiring: Wiring, switch: dict, nics: dict) -> list[str]:
    """What the wiring table and the live numbers disagree on, worst first."""
    out = list(wiring.errors)
    for row in sorted(wiring.rows, key=lambda r: port_key(r.switch_port)):
        stats = switch.get(row.switch_port)
        if row.crate_slot is not None:
            tone, word = in_port_state(stats)
            if tone in ("bad", "warn"):
                out.append(f"{row.switch_port} (crate slot {row.crate_slot}): {word}")
        else:
            tone, word = out_port_state(stats)
            if tone in ("bad", "warn"):
                out.append(f"{row.switch_port} ({row.node} port {row.nic_port}): {word}")
            nic = nics.get((row.node, row.nic_port))
            if nic and rate_mismatch(_num(stats, "out_pps"), _num(nic, "pps")):
                out.append(f"{row.switch_port} sends {_num(stats, 'out_pps') / 1e3:.0f} kpps but "
                           f"{row.node} port {row.nic_port} receives {_num(nic, 'pps') / 1e3:.0f} kpps: "
                           f"check the wiring table")
    return out


# ---- the page model ----------------------------------------------------------

def overview(boards: list[dict], wiring: Wiring, switch: dict, nics: dict,
             power: dict, nodes: dict, devices: dict | None = None,
             flags: dict | None = None, fpga: dict | None = None,
             now: float | None = None) -> dict:
    """Rows for the diagram and the tables.

    *boards*: :func:`board_layout`.  *power*: feed label -> True/False/None
    (the PDB channel; absent when the PDB table does not name the feed).
    *nodes*: node name -> ``{"status", "mode", "version", "branch",
    "commit", "group"}`` from choco's registry.  *devices*: the monitor's
    ``devices`` (the PDU's outlets, each host's node_exporter).  *flags*: feed label -> why bffs flags it, for
    the flagged feeds; None when bffs has not said.  *fpga*:
    ``FpgaMonitor.to_dict()``, for each board's own state (``board_tone``
    / ``board_word``, :func:`board_state`) beside its link's (``tone`` /
    ``word``, the switch port).
    """
    by_slot = wiring.by_slot()
    by_port = wiring.by_port()
    left = []
    for b in boards:
        row = by_slot.get(b["slot"])
        stats = switch.get(row.switch_port) if row else None
        tone, word = in_port_state(stats, b["silent_expected"]) if row else ("off", "not in wiring table")
        board_tone, board_word = board_state(b["source_id"], fpga, now)
        left.append({
            **b,
            "feeds": [{**f, "power": power.get(f["label"]) if f["label"] else None,
                       "in_pdb": f["label"] in power,
                       "flagged": None if flags is None or not f["label"] else f["label"] in flags,
                       "flag_why": (flags or {}).get(f["label"]) or ""} for f in b["feeds"]],
            "port": row.switch_port if row else None,
            "notes": row.notes if row else "",
            "tone": tone, "word": word, "stats": stats or {},
            "board_tone": board_tone, "board_word": board_word, "board_facts": board_facts(b["source_id"], fpga),
        })
    right = []
    nic_rows = sorted((r for r in wiring.rows if r.node), key=lambda r: (port_key(r.node), r.nic_port))
    for r in nic_rows:
        stats = switch.get(r.switch_port)
        nic = nics.get((r.node, r.nic_port))
        sw_tone, sw_word = out_port_state(stats)
        nic_tone, nic_word = nic_state(nic)
        mismatch = rate_mismatch(_num(stats, "out_pps"), _num(nic, "pps"))
        if mismatch and nic_tone == "ok":
            nic_tone, nic_word = "warn", "rate differs"
        right.append({
            "node": r.node, "nic_port": r.nic_port, "port": r.switch_port, "notes": r.notes,
            "sw_tone": sw_tone, "sw_word": sw_word, "sw": stats or {},
            "nic_tone": nic_tone, "nic_word": nic_word, "nic": nic or {},
            "mismatch": mismatch, "node_state": nodes.get(r.node),
        })
    wired = {(r.node, r.nic_port) for r in nic_rows}
    unmapped = sorted(k for k in nics if k not in wired and k[0] in nodes)
    xnodes = {r.node for r in nic_rows}
    downstream = [{"name": n, **s} for n, s in sorted(nodes.items()) if n not in xnodes]
    roles = {}
    for r in wiring.rows:
        roles[r.switch_port] = (f"crate slot {r.crate_slot}" if r.crate_slot is not None
                                else f"{r.node} port {r.nic_port}")
    table = []
    for name in sorted(set(switch) | set(by_port), key=port_key):
        if not name.startswith("Ethernet"):
            continue
        stats = switch.get(name)
        row = by_port.get(name)
        if row is None:
            tone, word = ("off", (stats or {}).get("status") or "no data")
        elif row.crate_slot is not None:
            tone, word = in_port_state(stats)
        else:
            tone, word = out_port_state(stats)
        table.append({"name": name, "role": roles.get(name, ""), "tone": tone, "word": word,
                      "stats": stats or {}})
    hosts = {name: {**st, "name": name, **host_health(name, devices)}
             for name, st in sorted(nodes.items(), key=lambda kv: port_key(kv[0]))}
    return {
        "boards": left, "nics": right, "unmapped": unmapped, "downstream": downstream,
        "hosts": hosts, "switch_ports": table,
        "in_gbps": sum((b["stats"].get("in_bps") or 0) for b in left) / 1e9,
        "out_gbps": sum((n["sw"].get("out_bps") or 0) for n in right) / 1e9,
    }


# ---- the flowchart -------------------------------------------------------------

def health_tone(health: str | None) -> str:
    """A monitor's health as a tone, as ``monitor_tone`` in the templates."""
    if health == "ok":
        return "ok"
    if health == "down":
        return "bad"
    if health in (None, "unconfigured", "unknown"):
        return "off"
    return "warn"


def node_tone(state: dict | None) -> str:
    status = (state or {}).get("status")
    return {"started": "ok", "starting": "info", "down": "bad", "unknown": "off",
            None: "off"}.get(status, "warn")


#: A node mode's word on its card and the card's tone, for the modes that
#: show (normal shows nothing): the same words and tones as the templates'
#: mode_word / mode_tone macros.
_MODE_CARD = {"maintenance": ("maint", "maint"), "development": ("dev", "dev")}


def _mode_card(state: dict | None, tone: str) -> tuple[str, str]:
    """The suffix for a node's status line and its card's tone: a node
    not in normal mode says so, and a healthy one takes the mode's tone."""
    word, mode_tone = _MODE_CARD.get((state or {}).get("mode"), ("", tone))
    return (f" · {word}" if word else ""), (mode_tone if tone == "ok" else tone)




def _fmt_gbps(v) -> str:
    return f"{v / 1e9:.1f}" if isinstance(v, (int, float)) else ""


def _link_width(bps) -> float | None:
    """A link's stroke width for its rate: 1.5 px idle to 5 px at 100 Gb/s."""
    return round(1.5 + 3.5 * min(bps / 1e11, 1.0), 2) if isinstance(bps, (int, float)) and bps > 0 else None


def _dish_span(feeds: list[dict]) -> str:
    named = [f["dish"] for f in feeds if f["label"]]
    return f"{named[0]}–{named[-1]}" if len(named) > 1 else (named[0] if named else "—")


_VERSION_RE = re.compile(r"\+(\d+)\.g([0-9a-f]+)")


def short_build(branch: str | None, version: str | None, width: int = 15) -> str:
    """``chord`` + ``chord.2021.10+1242.g340808ce76`` -> ``chord +1242``;
    the branch alone, cut to *width*, when both do not fit."""
    m = _VERSION_RE.search(version or "")
    text = f"{branch} +{m.group(1)}" if branch and m else branch or version or "—"
    if len(text) > width:
        text = branch if branch and len(branch) <= width else text[:width - 1] + "…"
    return text


def ago(ts, now: float) -> str:
    """``12 s ago``, ``5 min ago``, ``3 h ago``; ``never`` without a time."""
    if not ts:
        return "never"
    d = max(0, now - ts)
    return f"{d:.0f} s ago" if d < 120 else f"{d / 60:.0f} min ago" if d < 7200 else f"{d / 3600:.0f} h ago"


def job_tone(health: str | None, running: bool = False) -> str:
    """A job's health as a tone, as ``job_tone`` in the templates."""
    if running and health in ("unknown", "never_run"):
        return "info"
    if health == "ok":
        return "ok"
    if health in ("degraded", "stale"):
        return "warn"
    if health in ("never_run", "unknown", None):
        return "off"
    return "bad"


def job_word(health: str | None, running: bool = False) -> str:
    """A job's health in a word, as ``job_label`` in the templates."""
    if running and health in ("unknown", "never_run"):
        return "running"
    return "not run" if health == "never_run" else health or "unknown"


def worst_tone(tones) -> str:
    """The tone that matters most: bad, warn, running, ok, off."""
    order = ("bad", "warn", "info", "ok", "off")
    tones = [t for t in tones if t in order] or ["off"]
    return min(tones, key=order.index)


#: Geometry of the flowchart, in SVG user units: a gutter of stage labels
#: on the left, one column per board (and per NIC port) to its right, the
#: stages stacked top to bottom.
_W, _GUT, _X1 = 1240, 132, 1030
#: The control column to the right of the X-engines: what choco pushes to them.
_RC, _RCW = 1056, 174
_Y = {"power": 12, "bus": 76, "feeds": 92, "feed_dy": 13, "crate": 206, "board": 214, "board_h": 40,
      "switch": 320, "switch_h": 66, "node": 446, "node_h": 112, "recv": 618, "recv_h": 52,
      "files": 692, "files_h": 48, "jobs": 776, "jobs_h": 48}
_CARD_W = 250


def flow_diagram(view: dict, stages: dict, root: str = "") -> dict:
    """Positions for the flowchart, top to bottom: the analog power, the
    feeds (by PDB power), the CRS boards, the switch, the X-engine nodes, the
    receiver and the files.  One column per board above the switch and one
    per NIC port below it, so every link is a straight drop.

    *view* is :func:`overview`.  *stages* gives each lane's label,
    ``{key: {"title", "tone", "word", "href", "notes"}}`` for ``power``,
    ``pdb``, ``fpga``, ``switch``, ``xengine``, ``recv`` and ``files``, and
    three lists of ``{"name", "sub", "line", "tone", "title", "href"}``
    cards: ``power_cards`` (the first feeds the second), ``control_cards``
    (what choco pushes to the X-engines, beside them; one named ``BFFS``
    gets the loop from the files) and ``job_cards`` (the jobs that read the
    files, under them; lane ``processing``).  *root* prefixes the links to node pages.
    Everything returned is numbers and words; the template only draws.
    """
    Y = _Y
    boards, nics, hosts = view["boards"], view["nics"], view.get("hosts", {})
    x0, x1 = _GUT, _X1
    ncol = max(len(boards), len(nics), 8)
    colw = (x1 - x0) / ncol

    def cx(i):
        return x0 + colw * (i + 0.5)

    cards, links, ticks, feeds = [], [], [], []

    def card(x, y, w, h, tone, lines, title="", href=None, folder=False):
        cards.append({"x": x, "y": y, "w": w, "h": h, "tone": tone, "title": title, "href": href,
                      "folder": folder, "tx": x + w / 2, "lines": lines})

    # power: the first card feeds the second; the PDB card's bus feeds every board column
    pcards = stages.get("power_cards") or []
    pdb_x = None
    for k, pc in enumerate(pcards[:3]):
        x = x1 - _CARD_W if k == 2 else x0 + k * (_CARD_W + 44)
        card(x, Y["power"], _CARD_W, 48, pc.get("tone", "off"),
             [{"dy": 19, "text": pc["name"], "cls": "fl-b", "sub": pc.get("sub", "")},
              {"dy": 36, "text": pc.get("line", ""), "cls": "fl-t muted"}],
             pc.get("title", ""), pc.get("href"))
        if k == 1:
            pdb_x = x + _CARD_W / 2
    if len(pcards) > 1:
        y = Y["power"] + 24
        links.append({"d": f"M{x0 + _CARD_W},{y} L{x0 + _CARD_W + 42},{y}", "tone": pcards[0].get("tone", "off"),
                      "label": "", "title": f"{pcards[0]['name']} → {pcards[1]['name']}"})
    pdb_tone = (stages.get("pdb") or {}).get("tone", "off")
    bus = None
    if boards and pdb_x is not None:
        drops = " ".join(f"M{cx(i)},{Y['bus']} V{Y['feeds'] - 8}" for i in range(len(boards)))
        bus = {"d": f"M{pdb_x},{Y['power'] + 48} V{Y['bus']} M{cx(0)},{Y['bus']} H{cx(len(boards) - 1)} {drops}",
               "tone": pdb_tone}

    # feeds and boards, one column each
    by = Y["board"]
    for i, b in enumerate(boards):
        c = cx(i)
        for k, f in enumerate(b["feeds"]):
            # one dot, two facts: the fill is the PDB power, the colour the flag
            tone = ("none" if f["flagged"] is None else "bad" if f["flagged"] else "ok") + "-" + \
                ("on" if f["power"] else "off" if f["power"] is False else "none")
            state = ("power on" if f["power"] else "power off" if f["power"] is False
                     else "PDB state unknown" if f["in_pdb"] else "not in the PDB table")
            state += ("; bffs: no state" if f["flagged"] is None else
                      f"; flagged ({f['flag_why']})" if f["flagged"] and f["flag_why"] else
                      "; flagged" if f["flagged"] else "; good")
            feeds.append({"x": c, "y": Y["feeds"] + k * Y["feed_dy"], "tone": tone,
                          "text": f["label"] or "—",
                          "title": f"{f['label']} · slot {b['slot']} ADC {f['adc']}: {state}" if f["label"]
                          else f"slot {b['slot']} ADC {f['adc']}: no dish"})
        top = Y["feeds"] + len(b["feeds"]) * Y["feed_dy"] - 6
        links.append({"d": f"M{c},{top} V{by - 2}", "tone": "ok" if b["tone"] == "ok" else "off", "label": "",
                      "title": f"feeds → crate slot {b['slot']}"})
        # the card is the board as fpga_master sees it; the link below it, the switch port
        card(c - colw / 2 + 3, by, colw - 6, Y["board_h"], b["board_tone"],
             [{"dy": 17, "text": f"slot {b['slot']}", "cls": "fl-t" + (" muted" if b["board_tone"] == "off" else "")},
              {"dy": 31, "text": f"{_dish_span(b['feeds'])[:1]} · {b['pol']}", "cls": "fl-sm muted"}],
             f"crate slot {b['slot']} (source_id {b['source_id']}), {_dish_span(b['feeds'])} {b['pol']}: "
             f"{b['board_word']}" + (f" ({b['board_facts']})" if b["board_facts"] else "") + f"; link {b['word']}",
             (stages.get("fpga") or {}).get("href"))
        sy = Y["switch"]
        links.append({"d": f"M{c},{by + Y['board_h']} V{sy - 3}", "tone": b["tone"],
                      "label": _fmt_gbps(b["stats"].get("in_bps")) if b["stats"].get("up") else b["word"],
                      "w": _link_width(b["stats"].get("in_bps")),
                      "lx": c, "ly": (by + Y["board_h"] + sy) / 2 + 3,
                      "title": f"slot {b['slot']} → {b['port'] or '?'}: {b['word']}"})
        ticks.append({"x": c, "y": sy, "tone": b["tone"], "label": (b["port"] or "—").replace("Ethernet", "Et").split("/")[0],
                      "ly": sy + 15})

    # switch → NIC ports → nodes
    sb = Y["switch"] + Y["switch_h"]
    ny = Y["node"]
    groups: list[dict] = []
    for i, x in enumerate(nics):
        c = cx(i)
        ticks.append({"x": c, "y": sb, "tone": x["sw_tone"], "label": x["port"].replace("Ethernet", "Et").split("/")[0],
                      "ly": sb - 7})
        links.append({"d": f"M{c},{sb} V{ny - 3}", "tone": x["sw_tone"] if x["sw_tone"] != "ok" else x["nic_tone"],
                      "label": _fmt_gbps(x["sw"].get("out_bps")) if x["sw"].get("up") else x["sw_word"],
                      "w": _link_width(x["sw"].get("out_bps")),
                      "lx": c, "ly": (sb + ny) / 2 + 3,
                      "title": f"{x['port']} → {x['node']} port {x['nic_port']}: switch {x['sw_word']}, NIC {x['nic_word']}"
                               + (f", {x['nic']['pps'] / 1e6:.2f} Mpps" if isinstance(x["nic"].get("pps"), float) else "")})
        ticks.append({"x": c, "y": ny, "tone": x["nic_tone"], "label": f"p{x['nic_port']}", "ly": ny + 13,
                      "title": f"{x['node']} DPDK port {x['nic_port']}: {x['nic_word']}"})
        if not groups or groups[-1]["name"] != x["node"]:
            groups.append({"name": x["node"], "cols": []})
        groups[-1]["cols"].append(i)
    node_bottoms = []
    for g in groups:
        h = hosts.get(g["name"]) or {"status": None}
        left, right = cx(g["cols"][0]) - colw / 2 + 4, cx(g["cols"][-1]) + colw / 2 - 4
        pline = f"{h['watts']:.0f} W" if h.get("watts") is not None else ""
        suffix, tone = _mode_card(h, node_tone(h))
        status = (h.get("status") or "not in nodes.yaml") + suffix
        card(left, ny, right - left, Y["node_h"], tone,
             [{"dy": 30, "text": g["name"], "cls": "fl-b"},
              {"dy": 44, "text": status, "cls": "fl-sm muted"},
              {"dy": 59, "text": short_build(h.get("branch"), h.get("version")), "cls": "fl-sm mono"},
              {"dy": 74, "text": pline, "cls": "fl-sm muted"}],
             f"{g['name']}: {status}" + (f" · kotekan {h['version']}" if h.get("version") else "")
             + (f" ({h['commit'][:10]})" if h.get("commit") else ""),
             f"{root}/pipeline/{h['group']}/{g['name']}" if h.get("group") else None)
        node_bottoms.append(((left + right) / 2, tone, g["name"]))

    # receiver(s) and files
    downstream = view.get("downstream") or []
    shown = downstream[:3]
    total = len(shown) * _CARD_W + max(len(shown) - 1, 0) * 20
    rx0 = (x0 + x1) / 2 - total / 2
    recv_centres = []
    for k, d in enumerate(shown):
        h = hosts.get(d["name"]) or d
        x = rx0 + k * (_CARD_W + 20)
        rx_bps = h.get("rx_bps")
        line = " · ".join([short_build(h.get("branch"), h.get("version"))] +
                          ([f"receiving {rx_bps / 1e9:.2f} Gb/s"] if isinstance(rx_bps, (int, float)) else []))
        suffix, tone = _mode_card(d, node_tone(d))
        card(x, Y["recv"], _CARD_W, Y["recv_h"], tone,
             [{"dy": 20, "text": d["name"], "cls": "fl-b",
               "sub": (d.get("status") or "?") + suffix},
              {"dy": 38, "text": line, "cls": "fl-sm muted"}],
             f"{d['name']}: {d.get('status') or '?'}" + (f" · kotekan {h['version']}" if h.get("version") else ""),
             f"{root}/pipeline/{d['group']}/{d['name']}" if d.get("group") else None)
        recv_centres.append(x + _CARD_W / 2)
    target = recv_centres[0] if recv_centres else (x0 + x1) / 2
    nb = ny + Y["node_h"]
    for ncx, tone, name in node_bottoms:
        mid = (nb + Y["recv"]) / 2
        links.append({"d": f"M{ncx},{nb} C{ncx},{mid} {target},{mid} {target},{Y['recv'] - 3}",
                      "tone": tone if recv_centres else "off", "label": "",
                      "title": f"{name} → {shown[0]['name'] if shown else 'receiver'}"})
    fs = stages.get("files") or {}
    if recv_centres:
        links.append({"d": f"M{target},{Y['recv'] + Y['recv_h']} V{Y['files'] - 3}", "tone": fs.get("tone", "off"),
                      "label": "", "title": "the receiver writes the N² files"})
    card(target - _CARD_W / 2, Y["files"], _CARD_W, Y["files_h"], fs.get("tone", "off"),
         [{"dy": 20, "text": "N² files", "cls": "fl-b", "sub": fs.get("word", "")},
          {"dy": 37, "text": fs.get("line", "full · subset"), "cls": "fl-sm muted"}],
         f"The N² files: {fs.get('word', '')}", fs.get("href"), folder=True)

    # control: beside the X-engines, an arrow into them from each card
    texts = []
    ccards = stages.get("control_cards") or []
    if ccards:
        texts.append({"x": _RC, "y": ny - 8, "text": "Control", "cls": "ht"})
        ch = (Y["node_h"] - 5 * (len(ccards) - 1)) / len(ccards)
        for k, cc in enumerate(ccards):
            y = ny + k * (ch + 5)
            card(_RC, y, _RCW, ch, cc.get("tone", "off"),
                 [{"dy": min(ch / 2 - 1, 15), "text": cc["name"], "cls": "fl-b", "sub": cc.get("sub", "")},
                  {"dy": min(ch - 6, 29), "text": cc.get("line", ""), "cls": "fl-sm muted"}],
                 cc.get("title", ""), cc.get("href"))
            links.append({"d": f"M{_RC},{y + ch / 2} L{x1},{y + ch / 2}", "tone": cc.get("tone", "off"),
                          "label": "", "title": cc.get("title", "")})
            if cc["name"] == "BFFS":
                fy = Y["files"] + Y["files_h"] / 2
                links.append({"d": f"M{target + _CARD_W / 2},{fy} H{_RC + _RCW / 2} V{y + ch + 3}",
                              "tone": cc.get("tone", "off"), "label": "subset files", "lx": (target + _CARD_W / 2 + _RC + _RCW / 2) / 2,
                              "ly": fy - 5, "title": "bffs reads the subset files and flags feeds in every X-engine"})

    # the jobs that read the files, under them
    jcards = stages.get("job_cards") or []
    if jcards:
        jw = min(_CARD_W, ((x1 - x0) - 16 * (len(jcards) - 1)) / len(jcards))
        jx0 = (x0 + x1) / 2 - (len(jcards) * jw + 16 * (len(jcards) - 1)) / 2
        fb = Y["files"] + Y["files_h"]
        for k, jc in enumerate(jcards):
            x = jx0 + k * (jw + 16)
            card(x, Y["jobs"], jw, Y["jobs_h"], jc.get("tone", "off"),
                 [{"dy": 20, "text": jc["name"], "cls": "fl-b", "sub": jc.get("sub", "")},
                  {"dy": 37, "text": jc.get("line", ""), "cls": "fl-sm muted"}],
                 jc.get("title", ""), jc.get("href"))
            mid = (fb + Y["jobs"]) / 2
            jcx = x + jw / 2
            links.append({"d": f"M{target},{fb} C{target},{mid} {jcx},{mid} {jcx},{Y['jobs'] - 3}",
                          "tone": jc.get("tone", "off") if jc.get("tone") in ("ok", "warn", "bad") else "off",
                          "label": "", "title": f"{jc['name']} reads the files"})

    # stage lanes, in the gutter
    lane_y = {"power": Y["power"], "pdb": Y["bus"], "fpga": Y["crate"], "switch": Y["switch"],
              "xengine": Y["node"], "recv": Y["recv"], "files": Y["files"], "processing": Y["jobs"]}
    lanes = []
    for key, y in lane_y.items():
        s = stages.get(key)
        if not s:
            continue
        word = s.get("word", "")
        lanes.append({"key": key, "y": y, "title": s.get("title", key), "word": word, "tone": s.get("tone", "off"),
                      "href": s.get("href"), "notes": s.get("notes", [])[:3],
                      "pill_w": max(44, 6.4 * len(word) + 20) if word else 0})
    sx, sw = x0 - 4, x1 - x0 + 8
    tops = [t for t in ticks if t["y"] == Y["switch"]]
    bottoms = [t for t in ticks if t["y"] == sb]
    mesh = [f"M{a['x']},{Y['switch'] + 20} L{b['x']},{sb - 20}" for a in tops for b in bottoms] \
        if len(tops) * len(bottoms) <= 400 else []
    return {
        "width": _W if ccards else x1 + 10,
        "height": (Y["jobs"] + Y["jobs_h"] if jcards else Y["files"] + Y["files_h"]) + 12,
        "lanes": lanes, "cards": cards, "texts": texts,
        "feeds": feeds, "links": links, "ticks": ticks, "bus": bus,
        "crate": {"x": x0 - 4, "y": Y["crate"], "w": x1 - x0 + 8, "h": Y["board_h"] + 16} if boards else None,
        "switch": {"x": sx, "y": Y["switch"], "w": sw, "h": Y["switch_h"], "mesh": mesh,
                   "cx": (x0 + x1) / 2, "cy": Y["switch"] + Y["switch_h"] / 2 + 4,
                   "label": (stages.get("switch") or {}).get("label", "")},
    }
