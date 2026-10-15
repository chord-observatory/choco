"""choco.acqtree: the data trees' layout and names, for the jobs and choco."""

import pytest

from choco import acqtree
from choco.jobclient import Degraded


@pytest.fixture
def root(tmp_path):
    for acq in ("acq_20260101_000000_000000000", "acq_20260202_000000_000000000"):
        (tmp_path / acq / ".partial").mkdir(parents=True)
        (tmp_path / acq / "vis_0000000001_20260101T_000000_000000000.h5").write_bytes(b"")
        (tmp_path / acq / ".partial" / "vis_0000000002_x.h5").write_bytes(b"")
    (tmp_path / "scratch").mkdir()
    (tmp_path / "notes.txt").write_text("x")
    return tmp_path


def test_acquisitions_in_time_order_either_way(root):
    assert acqtree.list_acquisitions(str(root)) == [
        "acq_20260101_000000_000000000", "acq_20260202_000000_000000000"]
    assert acqtree.list_acquisitions(str(root), newest_first=True)[0].startswith("acq_20260202")


def test_an_unreadable_root_is_degraded(tmp_path):
    with pytest.raises(Degraded, match="unavailable"):
        acqtree.list_acquisitions(str(tmp_path / "nope"))


def test_partial_files_are_not_sources(root):
    files = acqtree.source_files(str(root / "acq_20260202_000000_000000000"))
    assert len(files) == 1 and ".partial" not in files[0]


@pytest.mark.parametrize("name,index", [
    ("vis_0004202414_20260723T_x.h5", 4202414),
    ("/a/b/vis_7_x.h5", 7),
    ("vis_4214175.h5", None),          # no trailing separator
    ("other.h5", None),
    ("", None),
])
def test_file_index(name, index):
    assert acqtree.file_index(name) == index


def test_file_time():
    name = "vis_0004233671_20261004T_030643_744722230.h5"
    assert acqtree.file_time(name) == pytest.approx(1791083203.7447, abs=1e-3)
    assert acqtree.file_time("vis_7_x.h5") is None


def test_normalize_roots():
    assert acqtree.normalize_roots(["/d/subset", {"path": "/e/full", "name": "f"}]) == [
        {"path": "/d/subset", "name": "subset"}, {"path": "/e/full", "name": "f"}]
    with pytest.raises(ValueError, match="no roots"):
        acqtree.normalize_roots([])
    with pytest.raises(ValueError, match="no path"):
        acqtree.normalize_roots([{"name": "x"}])


def test_waterfall_names_match_their_patterns():
    assert acqtree.shard_name(3) == "e0003" and acqtree.SHARD_RE.match("e0003")
    name = acqtree.product_name(3, 17)
    assert name == "e0003xe0017"
    assert acqtree.IMAGE_RE.match(f"wf_{name}.png").groups() == ("0003", "0017")
