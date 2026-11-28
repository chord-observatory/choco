"""choco.beam: the waterfall frame, its rows by number, the event files."""

import base64
import json

from choco import beam


def frame(first_seq=8192 * 1000, rows=4, columns=3, step=8192, data=None):
    data = bytes(range(rows * columns)) if data is None else data
    return {
        "frame_desc": {"frame_desc_type": "ndarray", "value_type": "int8",
                       "extents": [rows, columns], "dimnames": ["Trfi", "F"]},
        "metadata": {"FPGA_SEQ_NUM": first_seq, "TIME_DOWNSAMPLING_FPGA": step},
        "encoding": "base64",
        "data": base64.b64encode(data).decode(),
    }


class Node:
    key = "recv/recv1"

    def __init__(self, config, frames=()):
        self.desired_config = config
        self.frames = list(frames)
        self.asked = 0

    def get_buffer_frame(self, name, length=None):
        self.asked += 1
        self.length = length
        return self.frames.pop(0) if self.frames else None


SEARCH = {"kotekan_stage": "IncoherentBeamSearch", "freq_id_base": 1536,
          "waterfall_buf": "incoherent_waterfall_buffer",
          "candidate_file": "/data/candidates.jsonl"}


def test_find_search_prefers_the_stage_with_a_waterfall():
    plain = Node({"search": {"kotekan_stage": "IncoherentBeamSearch"}})
    full = Node({"a": {"b": dict(SEARCH)}})
    other = Node({"x": {"kotekan_stage": "bufferRecv"}})
    assert beam.find_search([other, plain, full]) == (full, SEARCH)
    assert beam.find_search([other, plain])[0] is plain
    assert beam.find_search([other, Node(None)]) == (None, None)


def test_freq_axis_from_the_stage():
    axis = beam.freq_axis(SEARCH)
    # Channel 1536 is centred on 300 MHz; a column is 8 channels.
    assert abs(axis["low_mhz"] - (300 - 0.09765625)) < 1e-9
    assert axis["column_mhz"] == 1.5625
    assert beam.freq_axis(dict(SEARCH, waterfall_freq_bin=4))["column_mhz"] == 0.78125
    assert beam.freq_axis(dict(SEARCH, waterfall_freq_bin=0)) is None
    assert beam.freq_axis({"kotekan_stage": "IncoherentBeamSearch"}) is None


def test_decode_waterfall_numbers_the_rows():
    image = beam.decode_waterfall(frame())
    assert image["first"] == 1000 and image["rows"] == 4 and image["columns"] == 3
    assert image["data"] == bytes(range(12))
    # A sequence number off the grid of rows (the stage's phase) keeps the count.
    assert beam.decode_waterfall(frame(first_seq=8192 * 1000 + 7))["first"] == 1000


def test_decode_waterfall_refuses_other_replies():
    for bad in (
        {},
        dict(frame(), encoding="none"),
        dict(frame(), data="!!"),
        dict(frame(), data=base64.b64encode(b"\0" * 11).decode()),
        dict(frame(), metadata={"FPGA_SEQ_NUM": 5}),
        dict(frame(), metadata={"FPGA_SEQ_NUM": True, "TIME_DOWNSAMPLING_FPGA": 8192}),
        dict(frame(), frame_desc={"value_type": "float32", "extents": [4, 3]}),
        dict(frame(), frame_desc={"value_type": "int8", "extents": [4, 0]}),
    ):
        assert beam.decode_waterfall(bad) is None


def test_rows_after_returns_only_what_is_new():
    image = beam.decode_waterfall(frame())       # rows 1000..1003
    assert beam.rows_after(image, None) == (1000, bytes(range(12)))
    assert beam.rows_after(image, 1001) == (1002, bytes(range(6, 12)))
    assert beam.rows_after(image, 1003) == (1004, b"")
    assert beam.rows_after(image, 5000) == (1004, b"")
    # The reader was away longer than the image: everything held, from its first row.
    assert beam.rows_after(image, 10) == (1000, bytes(range(12)))


def test_frame_cache_reads_kotekan_once_per_ttl(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(beam.time, "monotonic", lambda: now[0])
    node = Node({}, [frame(), frame(first_seq=8192 * 1008)])
    cache = beam.FrameCache()
    assert cache.get(node, "wf")["image"]["first"] == 1000
    assert cache.get(node, "wf")["image"]["first"] == 1000
    assert node.asked == 1
    assert node.length == beam.MAX_FRAME_BYTES
    now[0] += beam.FRAME_TTL_S + 0.01
    assert cache.get(node, "wf")["image"]["first"] == 1008
    now[0] += 1
    assert cache.get(node, "wf") == {"error": "kotekan unreachable", "status": 502}


def test_frame_cache_passes_on_kotekans_refusal():
    node = Node({}, [{"error": "no full frame currently in buffer"}, {"frame_desc": {}}])
    assert beam.FrameCache().get(node, "wf")["status"] == 404
    assert beam.FrameCache().get(node, "wf")["status"] == 502


def candidate(unix, **fields):
    return dict({"unix_time": unix, "fpga_seq": int(unix * 1000), "snr": 6.2, "dm": 300.5,
                 "width": 4, "flags": []}, **fields)


def test_read_candidates_lists_dumps_and_unflagged_newest_first(tmp_path):
    path = tmp_path / "candidates.jsonl"
    lines = [
        candidate(10.0),
        candidate(11.0, flags=["low_snr"]),                    # flagged, no dump: not an event
        candidate(12.0, flags=["low_dm"], cutout=7),
        candidate(13.0, flags=["low_snr"]),
    ]
    path.write_text("".join(json.dumps(line) + "\n" for line in lines) + '{"unix_time": 14')
    got = beam.read_candidates(path, 10)
    assert [e["unix"] for e in got["events"]] == [12.0, 10.0]
    assert got["events"][0]["dump"] == 7 and got["events"][0]["flags"] == ["low_dm"]
    assert got["events"][1]["dump"] is None and got["events"][1]["width"] == 4
    # The newest whole line dates the stream, an event or not.
    assert got["clock"] == (13.0, 13000)
    assert [e["unix"] for e in beam.read_candidates(path, 1)["events"]] == [12.0]


def test_read_candidates_reads_only_the_end(tmp_path, monkeypatch):
    path = tmp_path / "candidates.jsonl"
    path.write_text("".join(json.dumps(candidate(float(k))) + "\n" for k in range(200)))
    monkeypatch.setattr(beam, "CANDIDATE_TAIL_BYTES", 1000)
    events = beam.read_candidates(path, 500)["events"]
    assert 0 < len(events) < 15 and events[0]["unix"] == 199.0


def test_events_cache_keeps_the_clock_through_an_unreadable_file(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(beam.time, "monotonic", lambda: now[0])
    cache = beam.EventsCache()
    calls = []

    def read(path):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("stale file handle")
        return {"events": [{"unix": 1.0}], "clock": (1.0, 1000)}

    assert cache.get("/f", read)["events"] and cache.get("/f", read)["error"] is None
    assert len(calls) == 1
    assert cache.clock("/f") == (1.0, 1000) and cache.clock("/other") is None
    now[0] += beam.EVENTS_TTL_S + 1
    got = cache.get("/f", read)
    assert got["events"] == [] and "stale" in got["error"] and got["clock"] == (1.0, 1000)


def test_read_triggers_newest_first(tmp_path):
    path = tmp_path / "baseband_triggers.jsonl"
    records = [
        {"time": "2026-11-27T17:07:40.094+00:00", "result": "forwarded", "sender": "chime",
         "event_id": 10103, "request": {"dm": 242.4},
         "nodes": {"cx/cx19": {}, "cx/cx47": {"1003": {"file_name": "x.h5"}}, "cx/cx51": None}},
        {"time": "2026-11-27T17:09:00.000+00:00", "result": "duplicate", "sender": "chime",
         "event_id": 10103},
        {"time": "bad", "result": "malformed", "sender": "chime", "error": "body too large"},
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    got = beam.read_triggers(path, 10)
    assert [t["result"] for t in got] == ["malformed", "duplicate", "forwarded"]
    assert got[0]["unix"] is None and got[0]["event_id"] is None
    assert got[2]["dm"] == 242.4 and got[2]["dumps"] == 1 and got[2]["asked"] == 3
    assert got[2]["unix"] == 1795799260.094
    assert len(beam.read_triggers(path, 2)) == 2
    assert beam.read_triggers(tmp_path / "none.jsonl", 5) == []


def test_row_unix_dates_a_row_from_a_candidate():
    image = beam.decode_waterfall(frame())
    # 8192 sequence numbers of 5.12 us are 41.94 ms.
    assert abs(beam.FPGA_SEQ_S - 5.12e-6) < 1e-15
    unix = beam.row_unix((5000.0, 8192 * 1000), image, 1100)
    assert abs(unix - (5000.0 + 100 * 8192 * 5.12e-6)) < 1e-9
    assert beam.row_unix(None, image, 1100) is None
