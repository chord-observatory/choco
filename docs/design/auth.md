# Authentication, configuration file and dev mode

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## YAML config file

all settings in `config.yaml` (gitignored); `config.yaml.template` checked in
with defaults. No environment variables.

## Sign-in and the session guardrails

choco has no login of its own (since 2026-10): people sign in at the site
proxy ("Behind the site proxy" below), the trusted hosts are signed in by
address, and loopback callers skip login on the API.  ``/login`` is a page
that points to ``proxy_auth.public_url`` and, for when the site is down, an
ssh tunnel to loopback (signed in as ``localhost``).  An ``ldap:`` block is a
startup error naming the replacement.

The session guardrails stand as they were: ``load_config`` **refuses a
placeholder or sub-16-character ``server.secret_key``** outside dev mode (it
signs the session cookie, so a guessable key forges a login; ``choco.sh
install`` seeds a random key), the session cookie is ``Secure`` (tied to
``server.ssl``: the plain-HTTP redirect listener would otherwise see it in
cleartext, and a dev instance over a tunnel would never get it back) plus
``HttpOnly`` and ``SameSite=Lax``, and ``web._next_target`` rejects a
``?next=`` carrying a backslash or control character, because browsers
resolve ``/\host`` in a Location header as ``//host``.

History.  Until 2026-10 choco had its own FreeIPA login: a direct LDAP bind
as the user (no service account), through flask-ldap3-login, then plain
ldap3, then about 200 lines of standard-library BER (``ldapbind.py``) once
ldap3 lost its maintainer.  The 2026-09 audit found ldap3's default TLS
accepted any certificate from any host, which led to verifying chain and
hostname unconditionally.  It was removed once choco sat behind the site
proxy: a direct bind never learns the user's groups, so it could not honour
the read-only role, and it was a password-only way around Authelia's second
factor.  The code is in the git history.

## Dev mode

``./choco.sh develop`` runs a throwaway instance with no FreeIPA, no TLS and
no cluster: it seeds a gitignored ``dev/`` (``config.yaml`` plus ``configs/``
holding a one-node registry pointed at ``127.0.0.1:12048``, for a kotekan you
run locally) and execs choco against it.  Unlike ``run`` it needs **no root**
— dev mode binds loopback, so there are no iptables redirects to install and
no privileged port to claim; invoked under sudo it re-execs as ``$SUDO_USER``
so ``.venv/`` and ``dev/`` don't become root-owned and break ``./choco.sh
test``.  Two config keys do the work, both usable outside ``develop``:
``server.ssl: false`` skips ``_make_ssl_context`` entirely (plain HTTP — a
loopback instance reached through an ssh tunnel is already encrypted on the
wire, and dropping TLS drops the cert warning with it), and ``server.dev_auth:
<username>`` installs a ``before_request`` that logs every request in as a
synthetic user *and* short-circuits ``_check_csrf`` / ``_check_csrf_header``.
The two have to come off together: the CSRF token lives in the session cookie,
so auth-without-CSRF would 403 every toggle, edit and PDB write with no login
flow left to reset it.  The auto-login is re-established **per request**
rather than left to ``login_user``'s cookie for the same reason — dev
instances are reached over ssh tunnels, where the browser's cookie jar is
shared across every ``localhost`` port (cookies ignore the port), so a cookie
minted by a *different* choco on that port would otherwise lock the developer
out.  The guardrail is in ``load_config``, not a warning: ``dev_auth`` with a
non-loopback ``server.host`` **raises** and choco refuses to start, because
dev mode is not a role but the absence of authentication, and a dev instance
answering on ``0.0.0.0`` is an unauthenticated cluster control plane.
``base.html`` carries an orange banner on every page so it can't be mistaken
for production.  The seeded config omits ``fpga_master`` and ``pdb``
deliberately — a dev instance should never poll, let alone control, the real
F-engine or the power boards; those badges render ``unconfigured`` and no
greenlet spawns.  ``tests/test_dev_mode.py`` guards the direction that
matters: with ``dev_auth`` unset the dashboard still redirects to login and
both CSRF checks still raise.

## Trusted hosts

``server.trusted_hosts`` (2026-09) is the production answer to "I am on the
choco box (or on chive) and do not want to sign in": a mapping
of IP address or CIDR network to a label, e.g. ``127.0.0.1: localhost``,
``10.222.0.54: chive``.  A request from a listed peer that carries no login
is logged in by a ``before_request`` in ``init_auth`` as a synthetic
``User`` whose username is the label, so the whole UI works -- ``base.html``
gates htmx, the service strip and every toggle on ``is_authenticated``, which
is why merely swapping ``login_required`` for ``localhost_or_login_required``
on the UI routes would have produced static pages with no polling.  The
label is what ``_audit_user`` records, so an F-engine restart from chive
logs as ``requested by chive`` rather than a bare address.

It is deliberately narrower than dev mode on three points.  **CSRF stays
on**: the session cookie round-trips normally so the token works, and a
hostile page in the operator's browser could otherwise POST to
``localhost:5000`` unchallenged.  **The user is never stored**: it is built
with ``User(...)``, not ``save_user``, so the ``trusted:<addr>`` id in the
cookie it mints loads as nobody on the next request and only a trusted peer
is logged back in -- replaying that cookie from any other address is
worthless, which is what makes a per-address trust decision safe to persist
in a bearer cookie.  **A real login wins**: ``is_authenticated`` is tested
first, so someone signed in through the proxy keeps their own name
in the audit line.  The logout link is hidden for a synthetic user because
logging out would only log it straight back in.

The peer address is trustworthy because gevent's WSGIServer terminates the
TCP connection itself: there is no reverse proxy on the box, and the
iptables 443→5000 rule is PREROUTING only, so ``remote_addr`` is the real
peer.  The site proxy that now fronts choco does not change that: it is
recognised by address and secret, never by ``X-Forwarded-For`` (see
"Behind the site proxy" below).  The list is validated in
``load_config`` (and again in ``init_auth`` for tests that build a config
dict directly): a non-mapping, an unparsable key or an empty label is a
startup error naming the entry.  Entries are matched most-specific first so
a host inside a listed network can carry its own label.  Default is empty,
which is also what keeps the test suite honest -- Flask's test client
presents as ``127.0.0.1``, so a test that wants a remote peer must say so
(``tests/test_route_auth.py`` sweeps every route from ``10.222.0.54``).
``tests/test_trusted_hosts.py`` covers the rest.

## The loopback API from a browser (2026-10)

``localhost_or_login_required`` admits loopback callers -- the jobs and the
CLI -- with no login, no token and no checks beyond the route's own.  Until
2026-10 a logged-in caller from anywhere else got the same treatment, so
the write routes behind it (``/update/*``, ``/oneshot/*``, ``PUT
/api/configs/*``, ``POST /api/configs/pull``) took no CSRF token, and a pull
with an empty body acted (a pull puts the nodes it re-renders in
maintenance).  The ``SameSite=Lax`` cookie kept a cross-*site* page out, but
not a page on a sibling ``chord-observatory.ca`` host or another port of
this one.  No page in choco calls these routes and only this host scripts
them, so a non-loopback write now needs what every other browser write
needs, the session's token (``X-CSRF-Token``), and a JSON body besides:
no HTML form can send one cross-origin, so even a leaked token cannot be
spent by a form.  A trusted-host peer is a browser session like any other
here.  ``/api/fpga/gain-data`` alone is login-only even from loopback:
nothing on the host reads the F-engine's gains.

## Behind the site proxy (2026-10)

choco is reached from outside at ``https://ctc.chord-observatory.ca/choco/``:
nginx on the proxy host asks Authelia (FreeIPA accounts, two-factor) before
every request and passes the signed-in user in ``Remote-User``.  Direct
access on the site network reaches the same ``/login`` page, which points
here; the trusted hosts are unchanged.

**Recognising the proxy.**  ``ProxyFix`` was the obvious tool and the wrong
one: it rewrites ``remote_addr`` from ``X-Forwarded-For``, a header any
client can send, and ``remote_addr`` is what the loopback bypass and
``trusted_hosts`` trust -- a forged ``X-Forwarded-For: 127.0.0.1`` would be
a login and CSRF bypass on the whole API.  Instead ``server.proxy_auth``
names the proxy's address as choco sees it (no NAT between ctc and site,
checked) and a shared secret the proxy adds (``X-Choco-Proxy-Secret``, from
a root-only nginx snippet).  ``auth.ProxyMiddleware`` marks a request
proxied only when both match, strips the secret header from every request,
and never touches ``REMOTE_ADDR``.  ``load_config`` refuses a loopback peer,
a peer inside ``trusted_hosts`` (every proxied request would become that
label) and ``dev_auth`` alongside it.

**Who it signs in.**  A proxied request is signed in as ``Remote-User``
through ``save_user``, a real user, so audit lines name
the person; Authelia has already checked the password, the second factor
and the ``choco-users``/``choco-admins`` group.  The header decides on every
request, so a session minted for someone else is replaced.  A username that
is not plain ``[A-Za-z0-9._-]`` is a 403 rather than a DN built from it.
Logout behind the proxy goes on to Authelia's logout (``logout_url``),
since clearing only choco's session would sign straight back in.  CSRF is
unchanged; a proxied request is not loopback, so an API write needs the
token and JSON like any browser write.

**The prefix.**  nginx strips ``/choco`` and the middleware sets
``SCRIPT_NAME``, so ``url_for`` and ``request.script_root`` produce prefixed
URLs.  About 150 hand-built URLs in the templates and scripts were made
prefix-aware: ``{{ request.script_root }}`` in templates, the page's
``<meta name="choco-root">`` in scripts, and ``?next=`` kept relative to the
app root with the prefix restored by ``_next_target``.
``tests/test_proxy_auth.py`` renders every route as a proxied request and
also checks the template and script sources, because pages that need data
(the waterfalls) do not render in tests.

**The cookie.**  Behind the proxy choco shares a host with padloper, another
Flask app.  Both used Flask's default cookie name ``session``, so each would
overwrite the other's on every visit; choco's is ``choco_session``.  The
rename logged everyone out once.

**Roles.**  The proxy also passes the user's FreeIPA groups
(``Remote-Groups``).  With ``proxy_auth.write_groups`` set, a proxied user in
none of them is read-only: every page, nothing changed.  ``read_only`` is
recomputed from the header on every request, so a group change applies as
soon as Authelia refreshes the user (five minutes at most), and the refusal is
one ``before_request`` for every POST/PUT/PATCH/DELETE rather than a check
per route -- a new write route is covered without anyone remembering to.
The pages keep the controls visible but inert (``.read-only`` in
``choco.css``, plus ``.write-control`` for the one write a script sends) so a
reader sees what exists.  Roles exist only behind the proxy, which is
why choco's own login went (above).  Trusted hosts and loopback callers keep
full access, as before.
