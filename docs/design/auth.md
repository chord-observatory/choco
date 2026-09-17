# Authentication, configuration file and dev mode

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## YAML config file

all settings in `config.yaml` (gitignored); `config.yaml.template` checked in
with defaults. No environment variables.

## LDAP-only auth (FreeIPA)

all routes require login via Flask-Login; users authenticated against FreeIPA
LDAP (no local fallback). No roles yet — all authenticated users have full
access. Authentication is a **direct bind** (``auth.LdapAuthenticator``, plain
``ldap3``): the DN is strung together as
``<login_attr>=<user>,<user_dn>,<base_dn>`` (RDN-escaped) and bound with the
user's own password, so no service account exists to protect.  Two guardrails
matter: the empty-password check (an empty SIMPLE bind is an *anonymous* bind
— it would "succeed") and ``escape_rdn`` on the username.  Legacy config keys
from the flask-ldap3-login era (``bind_dn``, ``bind_password``,
``user_object_filter``, ``user_search_scope``) are read-and-ignored — that
library's search-bind path (the only consumer of them) was unreachable with
choco's settings, which is why the wrapper and its Flask-WTF/WTForms train
could be dropped.  Defaults tuned for FreeIPA: ``cn=users,cn=accounts`` user
DN, ``uid`` login attribute, LDAPS on port 636.  **The LDAPS connection
verifies the server certificate** — found in the 2026-09 dependency audit:
``ldap3.Server(use_ssl=True)`` with no ``tls=`` gets ldap3's default
``Tls()``, whose ``validate`` is ``CERT_NONE``, and a live probe of ipa3
confirmed the old construction accepted a connection to the bare IP with a
hostname mismatch — so anyone on the path to IPA could have answered "bind
succeeded" to any password.  ``LdapAuthenticator`` now passes
``Tls(validate=CERT_REQUIRED, ca_certs_file=<ldap.ca_cert or None>)``; with no
file ldap3 loads the system CA store (an IPA-enrolled host already carries the
IPA CA there), and hostname matching is ldap3's own (it sets
``check_hostname=False`` on the SSLContext and compares the certificate
itself), verified live: by name connects, by IP is refused.  ``init_auth``
refuses to start when ``ldap.ca_cert`` names a missing file, because ldap3
would log an error and silently verify against the system store instead; the
``Tls`` is passed *unconditionally* and the cleartext-password warning keys
off the server's effective mode, because an ``ldaps://`` host URL turns SSL on
inside ldap3 whatever ``use_ssl`` says — the first version built the ``Tls``
only when the flag was true, and the test for that exact config got ldap3's
``CERT_NONE`` default back.  Three guardrails from the same review sit next to
it: ``load_config`` **refuses a placeholder or sub-16-character
``server.secret_key``** outside dev mode (it signs the session cookie, so a
guessable key forges a login; ``choco.sh install`` seeds a random key so a
fresh install never trips it — check a deployed config before the next
restart), the session cookie is ``Secure`` (tied to ``server.ssl``, since the
plain-HTTP redirect listener would otherwise see it in cleartext and a dev
instance over a tunnel would never get it back) plus ``HttpOnly`` and
``SameSite=Lax``, and ``web._next_target`` rejects a ``?next=`` carrying a
backslash or control character, because browsers resolve ``/\host`` in a
Location header as ``//host``.  Not done yet: rate limiting on ``/login``
(FreeIPA's lockout policy is the defence today).  The audit's conclusion on
hand-rolling either ldap3 or flask-login: keep both — every finding above was
configuration around the libraries, none was in them.

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
choco box (or on chive) and do not want to type an LDAP password": a mapping
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
first, so someone who did log in over LDAP from chive keeps their own name
in the audit line.  The logout link is hidden for a synthetic user because
logging out would only log it straight back in.

The peer address is trustworthy because gevent's WSGIServer terminates the
TCP connection itself: there is no reverse proxy on the box, and the
iptables 443→5000 rule is PREROUTING only, so ``remote_addr`` is the real
peer.  Putting a proxy in front of choco would have to come with
``ProxyFix`` and a rethink of this key.  The list is validated in
``load_config`` (and again in ``init_auth`` for tests that build a config
dict directly): a non-mapping, an unparsable key or an empty label is a
startup error naming the entry.  Entries are matched most-specific first so
a host inside a listed network can carry its own label.  Default is empty,
which is also what keeps the test suite honest -- Flask's test client
presents as ``127.0.0.1``, so every ``test_requires_login`` in the suite is
implicitly a test that the default is off.  ``tests/test_trusted_hosts.py``
covers the rest.
