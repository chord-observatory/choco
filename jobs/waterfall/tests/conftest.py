"""Shared fixture: the job's state directory is a scratch directory.

``job_state_dir`` honours systemd's ``$STATE_DIRECTORY``; setting it here
keeps every ``main()`` run in the tests out of /var/lib/choco, where the
host running them may have real job state.  The tests read state.json straight from tmp_path, so the
state directory is tmp_path itself.
"""

import pytest


@pytest.fixture(autouse=True)
def scratch_state_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("STATE_DIRECTORY", str(tmp_path))
    return tmp_path
