"""``choco`` -- command-line client for choco's localhost JSON API.

A thin wrapper over the endpoints that bypass login for callers on the
choco host (``/update``, ``/oneshot``, ``/api/*``), so an operator -- or
a script, or an agent -- can do from a shell what the dashboard does
from a browser.  Stdlib only: the web process never imports this, and
the jobs talk to choco the same way (``choco.jobclient``).

    choco status                         overall health
    choco nodes [-j]                     per-node table (or the raw JSON)
    choco get /api/config/cx             any GET endpoint, printed as JSON
    choco start|stop  <target>...        desired run state
    choco maint on|off <target>...       maintenance mode
    choco push    <target> <file|->      queue a base config
    choco set     <target> <endpoint> <json|@file|->   queue an updatable push
    choco oneshot <target> <file|->      start an unrecorded config
    choco help [<command>]               this, or one command's usage

A *target* is a group (``cx``) or a node key (``cx/cx19``), exactly as
choco prints them.  start, stop, push and set record *desired* state
for the sync loop to apply, and the loop makes no REST write to a node
in maintenance -- so on a paused node nothing reaches kotekan until
``maint off`` (a choco restart pauses the whole cluster).  Exit status
follows the jobs' convention: 0 ok, 1 the request was rejected (the
server's error on stderr) or misused, 2 choco unreachable.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

from .jobclient import ssl_context

DEFAULT_URL = "https://localhost:5000"


# --- transport -----------------------------------------------------------

class Unreachable(Exception):
    """choco did not answer at all, as opposed to answering with an error."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Surface a 3xx as the reply it is.

    The only redirect choco sends is to the login page, which is what a
    caller *off* the choco host gets instead of the localhost bypass;
    following it would print the login form's HTML and exit 0.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _http(method: str, url: str, body: dict | None = None,
          timeout: float = 60.0) -> tuple[int, str]:
    """One request; ``(status, text)`` for any HTTP reply, including
    error and redirect statuses.  Raises when choco cannot be reached."""
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    # Loopback only: the cert is self-signed and nothing on the wire
    # leaves the host (jobclient.ssl_context, the rule the jobs use too).
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=ssl_context(url)), _NoRedirect())
    try:
        with opener.open(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


class Client:
    def __init__(self, url: str):
        self.url = url.rstrip("/")

    def _call(self, method: str, path: str, body: dict | None = None):
        try:
            return _http(method, self.url + path, body)
        except (OSError, http.client.HTTPException) as e:
            # URLError is an OSError, as are a refused connection and a
            # timeout; a peer that hangs up mid-reply is an HTTPException.
            # Converted here so that an OSError raised *later*, while
            # printing (a closed pipe), cannot masquerade as this.
            reason = getattr(e, "reason", e)
            raise Unreachable(
                f"cannot reach choco at {self.url}: {reason}") from e

    def get(self, path: str) -> tuple[int, str]:
        return self._call("GET", path)

    def post(self, path: str, body: dict) -> tuple[int, str]:
        return self._call("POST", path, body)


# --- helpers -------------------------------------------------------------

def target_path(target: str) -> str:
    """``cx`` -> ``/cx``, ``cx/cx19`` -> ``/cx/cx19``; anything else is
    not a group or node key."""
    parts = target.strip("/").split("/")
    if not 1 <= len(parts) <= 2 or not all(parts):
        raise ValueError(
            f"target must be <group> or <group>/<node>, not {target!r}")
    return "/" + "/".join(urllib.parse.quote(p, safe="") for p in parts)


def read_text(arg: str) -> str:
    """A file's text, or stdin for ``-``."""
    if arg == "-":
        return sys.stdin.read()
    with open(arg, encoding="utf-8") as f:
        return f.read()


def read_json(arg: str):
    """A JSON literal, ``@file``, or ``-`` for stdin (curl's convention)."""
    if arg == "-":
        return json.load(sys.stdin)
    if arg.startswith("@"):
        with open(arg[1:], encoding="utf-8") as f:
            return json.load(f)
    try:
        return json.loads(arg)
    except ValueError as e:
        raise ValueError(
            f"values is not JSON ({e}); to read a file, write @{arg}") from None


def _emit(status: int, text: str) -> int:
    """Print the reply and turn its status into an exit code.

    A JSON body always goes to stdout, pretty-printed (``| jq`` and eyes
    both work); a non-JSON body only when the request succeeded (``/metrics``
    is text, a redirect's HTML is noise).  Anything but 2xx also names
    itself on stderr, with the server's ``error`` field when there is one.
    """
    try:
        body = json.loads(text)
    except ValueError:
        body = None
    if body is not None:
        print(json.dumps(body, indent=2))
    elif status < 300:
        print(text, end="" if text.endswith("\n") else "\n")
    if status < 300:
        return 0
    detail = body.get("error") if isinstance(body, dict) else None
    if status < 400 and detail is None:
        detail = ("redirected, presumably to the login page -- the API "
                  "skips login only for callers on the choco host; "
                  "is --url a loopback address?")
    print(f"error: HTTP {status}" + (f": {detail}" if detail else ""),
          file=sys.stderr)
    return 1


def _emit_each(replies: list[tuple[int, str]]) -> int:
    """Several targets: report every reply, fail if any did."""
    return max(_emit(status, text) for status, text in replies)


def _nodes_table(data: dict) -> str:
    cols = ("KEY", "STATUS", "STARTED", "MAINT", "VERSION", "WORKER", "ERROR")
    rows = []
    for n in data.get("nodes", []):
        worker = n.get("worker") or {}
        rows.append((
            n.get("key", ""),
            n.get("status", ""),
            "yes" if n.get("started") else "no",
            "yes" if n.get("maintenance") else "no",
            str(n.get("version") or ""),
            str(worker.get("phase") or ""),
            str(n.get("error") or ""),
        ))
    widths = [max(len(c), *(len(r[i]) for r in rows)) if rows else len(c)
              for i, c in enumerate(cols)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths[:-1]) + "  {}"
    lines = [fmt.format(*cols)] + [fmt.format(*r) for r in rows]
    summary = data.get("summary") or {}
    if summary:
        counts = ", ".join(f"{k} {v}" for k, v in summary.items())
        lines.append(f"\n{counts}")
    return "\n".join(lines).rstrip() + "\n"


# --- commands ------------------------------------------------------------

def cmd_status(c: Client, a) -> int:
    return _emit(*c.get("/api/status"))


def cmd_nodes(c: Client, a) -> int:
    status, text = c.get("/api/nodes/status")
    if a.json or status != 200:
        return _emit(status, text)
    print(_nodes_table(json.loads(text)), end="")
    return 0


def cmd_get(c: Client, a) -> int:
    if not a.path.startswith("/"):
        raise ValueError(f"path must start with '/', not {a.path!r}")
    return _emit(*c.get(a.path))


def _set_flag(c: Client, targets: list[str], action: str, key: str,
              value: bool) -> int:
    paths = [target_path(t) for t in targets]  # validate all before sending any
    return _emit_each([c.post("/update" + p, {"action": action, key: value})
                       for p in paths])


def cmd_start(c: Client, a) -> int:
    return _set_flag(c, a.targets, "set_started", "started", True)


def cmd_stop(c: Client, a) -> int:
    return _set_flag(c, a.targets, "set_started", "started", False)


def cmd_maint(c: Client, a) -> int:
    return _set_flag(c, a.targets, "set_maintenance", "maintenance",
                     a.state == "on")


def cmd_push(c: Client, a) -> int:
    path = target_path(a.target)
    content = read_text(a.file)
    return _emit(*c.post("/update" + path,
                         {"action": "base_config", "config_content": content}))


def cmd_set(c: Client, a) -> int:
    path = target_path(a.target)
    values = read_json(a.values)
    return _emit(*c.post("/update" + path, {
        "action": "updatable_config", "endpoint": a.endpoint, "values": values,
    }))


def cmd_oneshot(c: Client, a) -> int:
    path = target_path(a.target)
    content = read_text(a.file)
    return _emit(*c.post("/oneshot" + path, {"config_content": content}))


# --- entry point ---------------------------------------------------------

class _Parser(argparse.ArgumentParser):
    """argparse exits 2 on a usage error; here 2 means unreachable.
    (Subparsers are built with the parent's class, so they inherit this.)"""

    def error(self, message):
        self.print_usage(sys.stderr)
        sys.stderr.write(f"{self.prog}: error: {message}\n")
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    doc = (__doc__ or "").split("\n\n", 2) + ["", ""]
    p = _Parser(prog="choco", description=doc[1], epilog=doc[2],
                formatter_class=argparse.RawDescriptionHelpFormatter)
    url_help = "choco base URL (default: $CHOCO_URL, else %s)" % DEFAULT_URL
    p.add_argument("--url", default=os.environ.get("CHOCO_URL", DEFAULT_URL),
                   help=url_help)
    # Accepted after the command too (`choco nodes --url ...`).  SUPPRESS
    # keeps an absent subcommand --url from overriding the one above.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--url", default=argparse.SUPPRESS, help=url_help)

    sub = p.add_subparsers(dest="command", metavar="<command>")
    p.subcommands = sub.choices  # name -> subparser, for `help <command>`

    def add(name, func, hlp):
        # help= is what the command list shows; description= is what
        # `choco help <name>` shows -- argparse does not reuse one for the other.
        s = sub.add_parser(name, help=hlp, description=hlp, parents=[common])
        s.set_defaults(func=func)
        return s

    TARGET = "a group (cx) or a node key (cx/cx19)"
    FILE = "config text, YAML or Jinja2 (- reads stdin)"
    DESIRED = ("desired state, applied by the sync loop -- which skips nodes in "
               "maintenance until `maint off`")

    add("status", cmd_status, "overall health (/api/status), as JSON")
    s = add("nodes", cmd_nodes, "per-node status table (/api/nodes/status)")
    s.add_argument("-j", "--json", action="store_true",
                   help="raw JSON instead of the table")
    s = add("get", cmd_get, "GET any endpoint and print the reply as JSON")
    s.add_argument("path", metavar="<path>",
                   help="e.g. /api/config/cx, /api/pdb/map, /api/files, /metrics")

    s = add("start", cmd_start, f"set nodes' desired state to started ({DESIRED})")
    s.add_argument("targets", nargs="+", metavar="<target>", help=TARGET)
    s = add("stop", cmd_stop,
            f"set nodes' desired state to idle: choco kills kotekan ({DESIRED})")
    s.add_argument("targets", nargs="+", metavar="<target>", help=TARGET)
    s = add("maint", cmd_maint,
            "maintenance mode: on pauses every REST write to the nodes, off resumes")
    s.add_argument("state", choices=("on", "off"), metavar="on|off")
    s.add_argument("targets", nargs="+", metavar="<target>", help=TARGET)

    s = add("push", cmd_push,
            f"queue a base config; the nodes restart onto it ({DESIRED})")
    s.add_argument("target", metavar="<target>", help=TARGET)
    s.add_argument("file", metavar="<file>", help=FILE)
    s = add("set", cmd_set,
            f"queue an updatable-config push, no restart ({DESIRED})")
    s.add_argument("target", metavar="<target>", help=TARGET)
    s.add_argument("endpoint", metavar="<endpoint>",
                   help="kotekan endpoint, e.g. updatable_config/bad_inputs")
    s.add_argument("values", metavar="<json|@file|->",
                   help="JSON literal, @file, or - for stdin")
    s = add("oneshot", cmd_oneshot,
            "start a config now on nodes that are in maintenance and idle, "
            "recording nothing (reverted by the loop once maintenance lifts)")
    s.add_argument("target", metavar="<target>", help=TARGET)
    s.add_argument("file", metavar="<file>", help=FILE)

    s = sub.add_parser("help", help="show help for a command",
                       description="show help for a command")
    s.set_defaults(func=None)
    s.add_argument("name", nargs="?", metavar="<command>")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command in (None, "help"):  # bare `choco`, `help`, `help <command>`
        name = getattr(args, "name", None)
        if name and name not in parser.subcommands:
            parser.error(f"unknown command {name!r}")
        (parser.subcommands[name] if name else parser).print_help()
        return 0
    client = Client(args.url)
    try:
        code = args.func(client, args)
        # Flush here, inside the handler's reach: a reader that has gone
        # away (`| head`) would otherwise surface as EPIPE at interpreter
        # exit -- an "Exception ignored" trace and status 120.
        sys.stdout.flush()
        return code
    except Unreachable as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except BrokenPipeError:
        # `choco nodes | head`: the reader went away, which is not our
        # error.  Point stdout at /dev/null so the interpreter's exit-time
        # flush does not raise a second time.
        try:
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except Exception:
            pass
        return 0
    except KeyboardInterrupt:
        return 130
    except (ValueError, OSError) as e:
        # A bad target, path or JSON literal; a file we cannot read.
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
