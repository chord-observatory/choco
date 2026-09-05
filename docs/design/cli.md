# The choco CLI

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## CLI

``choco`` (``choco/cli.py``, the venv's ``choco`` console script; the daemon's
entry point is ``choco-server``, which ``choco.service``, ``choco.sh run`` and
``develop`` exec) is a stdlib-only client for the localhost JSON API, so an
operator, a script or an agent on the choco host can do from a shell what the
dashboard does from a browser: ``status`` and ``nodes`` (the one formatted
table — ``-j`` for the raw body), ``get <path>`` (any GET endpoint printed as
JSON, which is why ``/api/config/<group>``, ``/api/pdb/map``, ``/api/files``
and ``/metrics`` need no subcommand of their own), ``start`` / ``stop`` /
``maint on|off`` over one or more targets, ``push <target> <file|->`` (base
config), ``set <target> <endpoint> <json|@file|->`` (updatable values, curl's
convention), ``oneshot <target> <file|->``, and ``help [<command>]`` (bare
``choco`` prints the same).  A target is a group or a ``<group>/<node>`` key
exactly as choco prints them, and every target is validated before any request
is sent, so a typo in the second cannot half-apply the first.  Output is the
reply body as pretty JSON (``| jq`` and eyes both work); exit codes follow the
jobs' convention — 0 ok, 1 rejected (the server's ``error`` field echoed on
stderr) or misused, 2 choco unreachable — with argparse's own usage-error exit
remapped to 1 so that 2 stays unambiguous in scripts.  It speaks only the
routes decorated ``localhost_or_login_required``, with certificate
verification off exactly as bffs and eigencal do (loopback, self-signed);
``--url`` / ``$CHOCO_URL`` point it at a dev instance (``choco.sh develop``
prints the line), and redirects are *not* followed — the login redirect a non-
loopback caller gets is reported as an error with a hint, where urllib's
default would have printed the login form and exited 0.  Transport failures
are converted to one exception at the request boundary so that an ``OSError``
raised later while printing (``| head`` closing the pipe) cannot be
misreported as choco being down; stdout is flushed inside that handler's reach
for the same reason.  FPGA and PDB controls are deliberately *not* exposed:
they are login+CSRF routes whose audit line names a person, and a localhost
caller has none.  ``choco.sh install`` symlinks the venv script to
``/usr/local/bin/choco`` (uninstall removes the link only if it still points
there); the web process never imports ``cli.py``.  Because the CLI reaches
``set_started`` / ``set_maintenance`` through ``/update``, those JSON actions
enqueue a ``POLL`` exactly as the dashboard toggles do — before the CLI
existed, a scripted stop waited for the node's next scheduled check, up to
``max_retry_interval`` for a backed-off node.  ``tests/test_cli.py`` drives
``cli.main`` with the transport patched to the Flask test client, so every
command exercises the real routes through the same bypass.
