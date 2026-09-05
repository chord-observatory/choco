# Dependency policy

Design rationale moved out of CLAUDE.md (2026-09).

Dependencies are deliberately minimal: core = flask (plus jinja2, declared
explicitly because `state.py` imports it directly for config rendering),
flask-login, ldap3, gevent, requests, pyyaml; the scientific stack (astropy,
numpy, h5py, hdf5plugin, matplotlib) lives in the `[jobs]` extra because
nothing in the web process imports it — the timer jobs do, and so does
`choco.h5read`, which the FPGA page's gain card runs as a *subprocess*
precisely so that stays true (`choco.sh` installs `.[jobs]`, dev venvs
`.[dev,jobs]`).  The self-signed TLS fallback shells out to the `openssl` CLI
instead of depending on `cryptography`.  Production installs are **pinned and
hash-locked**: `requirements.lock` (regenerate with `./choco.sh lock`, review
the diff, commit) is what `choco.sh install` feeds pip under `--require-
hashes` — each pin carries the sha256 of every artifact PyPI serves for that
version (fetched from PyPI's JSON API by `cmd_lock` itself, no pip-tools
dependency), so a substituted or tampered file is refused even at the right
version; choco itself installs `--no-deps` on top, and the dev venv installs
the same lock first for parity.  `pip` itself is in the lock (it is the tool
doing the hash verification), and every venv is created with `--upgrade-deps`
so the OS-seeded pip is replaced immediately.  `./choco.sh audit` checks every
pin against the latest PyPI release and the OSV vulnerability database (read-
only, stdlib-only, exits 1 on a known advisory — cron-able); the vendored
browser assets in `choco/static/` are outside the lock and are audited by hand
against OSV's npm ecosystem.  Before adding a dependency, check the feature
isn't a few lines of stdlib or an existing dep away.  Audited 2026-09: the
lock's 37 pins are 20 for the web process (six declared, the rest their
closure), 16 for the jobs extra, plus pip; nothing declared is unused (a
never-imported `pytest-mock` was dropped from `[dev]` then).  The one fat
entry is ``hdf5plugin`` — 181 MB installed, eleven native HDF5 filters all
mapped into the process on import, of which kotekan's files (``subset/`` and
``full/`` alike) use exactly one, bitshuffle (id 32008) — and it stays because
both alternatives were measured against a live file on the choco host and
failed: the PyPI ``bitshuffle`` wheel dies with SIGILL there (the host is a
QEMU virtual CPU with SSE4.2 and no AVX; hdf5plugin's build dispatches on CPU
features, that wheel does not), and Ubuntu's ``bitshuffle`` package ships a
plugin linked against ``libhdf5_serial.so.103``, which would load a second
HDF5 library into a process already running the 2.0 that the h5py wheel
bundles.  Building hdf5plugin from sdist with ``HDF5PLUGIN_STRIP`` would trim
it to the one filter at the cost of a compiler on the install host; not done.
