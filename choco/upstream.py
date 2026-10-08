"""Mirror one directory of a GitHub repository into the config library.

The CHORD configs live in kotekan (``config/chord/*.j2`` on the ``chord``
branch) and the library under ``configs_dir`` carries a copy of that
directory for the nodes to render.  "Pull chord from GitHub" on
``/configs`` (and ``choco config pull``) brings the copy up to date:

1. the configured ref is resolved to one commit, so the set of files is
   consistent even if the branch moves during the pull;
2. the directory is listed at that commit through the GitHub contents
   API, which gives every file's name and git blob sha;
3. each listed file whose blob sha differs from the local file's (or that
   has no local file) is fetched by its *validated* name from
   raw.githubusercontent.com at that commit, and the bytes are checked
   against the listing's sha before anything is written -- the listing's
   own ``download_url`` is never followed;
4. the caller (``web._pull_library``) validates the fetched texts as one
   set through every node that renders or includes any of them, writes
   them, and removes the mirror directory's config files the listing no
   longer has -- except one a node still uses, which is kept and
   reported.

Only direct children of the mirror directory with a config suffix are
touched; a listing entry that is not a plain config file name (a
directory, a symlink, a README) is skipped and reported, and never
causes a removal.  Stateless: no clone is kept and nothing is cached.
The repository is public, so no token is used; the anonymous API limit
(60 requests/hour) is far above a pull's cost of two requests plus one
per changed file.  ``requests`` is cooperative under gevent, so the
pull runs inline in the request handler.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

import requests

from .state import (
    _CONFIG_SUFFIXES, _PATH_PART_RE, list_config_files, resolve_config_path,
)

logger = logging.getLogger(__name__)

API_URL = "https://api.github.com"
RAW_URL = "https://raw.githubusercontent.com"

#: Defaults for the ``upstream:`` block of config.yaml.
DEFAULTS = {
    "enabled": True,
    "repo": "kotekan/kotekan",
    "ref": "chord",
    "path": "config/chord",
    "into": "chord",
    "timeout": 20,
}

#: A listed file larger than this is skipped (configs are tens of kB).
MAX_FILE_BYTES = 4 * 1024 * 1024

_REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_REF_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./-]*")
_SHA_RE = re.compile(r"[0-9a-f]{40}")
_HEADERS = {
    "User-Agent": "choco",
    "X-GitHub-Api-Version": "2022-11-28",
}


class UpstreamError(Exception):
    """GitHub could not be reached, or answered with something other
    than the listing or file asked for."""


def git_blob_sha(data: bytes) -> str:
    """The sha git gives *data* as a blob (``git hash-object``), which is
    what the contents API reports per file -- so an unchanged file is
    known without downloading it, and a download is checked against the
    listing before it is written."""
    h = hashlib.sha1(b"blob %d\0" % len(data))
    h.update(data)
    return h.hexdigest()


def _plain_dir(value, key: str) -> str:
    """A relative directory path of plain components (no leading ``/``,
    nothing hidden, no ``..``), as *key* of the upstream block."""
    text = str(value or "").strip("/")
    parts = text.split("/") if text else []
    if not parts or any(not _PATH_PART_RE.fullmatch(p) for p in parts):
        raise ValueError(
            f"{key} must be a relative directory of plain names, not {value!r}")
    return "/".join(parts)


@dataclass
class Plan:
    """What a pull would do: the listing at *commit* classified against
    the mirror directory.  Paths are library-relative (``chord/x.j2``)."""

    commit: str
    add: list[str]
    change: list[str]
    same: list[str]
    remove: list[str]
    skipped: list[tuple[str, str]]   # (listing name, reason)
    names: dict[str, str]            # rel -> name in the listing
    shas: dict[str, str]             # rel -> upstream blob sha

    @property
    def fetch(self) -> list[str]:
        """The files whose text is needed: new and changed."""
        return self.add + self.change

    @property
    def in_sync(self) -> bool:
        return not (self.add or self.change or self.remove)


@dataclass(frozen=True)
class Upstream:
    """The ``upstream:`` block, validated: *path* of *repo* at *ref*,
    mirrored into ``<configs_dir>/<into>/``."""

    repo: str
    ref: str
    path: str
    into: str
    timeout: float = 20.0
    api_url: str = API_URL
    raw_url: str = RAW_URL

    @classmethod
    def from_config(cls, cfg) -> Upstream | None:
        """The block's :class:`Upstream`, or None when ``enabled`` is
        false.  Raises ``ValueError`` on a malformed or unknown key, so
        a typo is a startup error rather than a button that pulls from
        the wrong place."""
        if cfg is None:
            cfg = {}
        if not isinstance(cfg, dict):
            raise ValueError(
                "upstream must be a mapping (enabled: false turns the pull off)")
        unknown = sorted(set(cfg) - set(DEFAULTS))
        if unknown:
            raise ValueError(
                f"unknown upstream key(s) {', '.join(unknown)}; "
                f"the keys are {', '.join(DEFAULTS)}")
        cfg = {**DEFAULTS, **cfg}
        if not cfg["enabled"]:
            return None
        repo = str(cfg["repo"] or "")
        if not _REPO_RE.fullmatch(repo):
            raise ValueError(f"upstream.repo must be owner/name, not {repo!r}")
        ref = str(cfg["ref"] or "")
        if not _REF_RE.fullmatch(ref) or ".." in ref:
            raise ValueError(
                f"upstream.ref must be a branch, tag or commit, not {ref!r}")
        try:
            timeout = float(cfg["timeout"])
        except (TypeError, ValueError):
            timeout = 0.0
        if timeout <= 0:
            raise ValueError("upstream.timeout must be a positive number of seconds")
        return cls(repo=repo, ref=ref,
                   path=_plain_dir(cfg["path"], "upstream.path"),
                   into=_plain_dir(cfg["into"], "upstream.into"),
                   timeout=timeout)

    @property
    def label(self) -> str:
        return f"{self.repo}@{self.ref}:{self.path}"

    # --- GitHub ---

    def _get(self, url: str, *, accept: str) -> requests.Response:
        try:
            resp = requests.get(url, headers={**_HEADERS, "Accept": accept},
                                timeout=self.timeout)
        except (requests.RequestException, OSError) as e:
            # OSError too: gevent can surface a bare ConnectionError.
            raise UpstreamError(f"{url}: {type(e).__name__}: {e}") from e
        if resp.status_code != 200:
            detail = ""
            try:
                message = resp.json().get("message")
                if isinstance(message, str):
                    detail = f": {message}"
            except ValueError:
                pass
            raise UpstreamError(f"HTTP {resp.status_code} for {url}{detail}")
        return resp

    def _api(self, url: str):
        resp = self._get(url, accept="application/vnd.github+json")
        try:
            return resp.json()
        except ValueError as e:
            raise UpstreamError(f"{url}: not JSON") from e

    def resolve_commit(self) -> str:
        """The commit *ref* names right now (a branch tip, a tag, or the
        sha itself), so the listing and every fetch read one tree."""
        url = (f"{self.api_url}/repos/{self.repo}/commits/"
               f"{quote(self.ref, safe='')}")
        data = self._api(url)
        sha = data.get("sha") if isinstance(data, dict) else None
        if not isinstance(sha, str) or not _SHA_RE.fullmatch(sha):
            raise UpstreamError(f"{url}: no commit sha in the reply")
        return sha

    def listing(self, commit: str) -> list[dict]:
        """The entries of *path* at *commit*: name, type, sha, size."""
        url = (f"{self.api_url}/repos/{self.repo}/contents/"
               f"{quote(self.path, safe='/')}?ref={commit}")
        data = self._api(url)
        if isinstance(data, dict):
            raise UpstreamError(f"{self.path} is a file, not a directory")
        if not isinstance(data, list):
            raise UpstreamError(f"{url}: not a directory listing")
        return [e for e in data if isinstance(e, dict)]

    def fetch_text(self, commit: str, name: str, sha: str) -> str:
        """The text of *name* at *commit*, fetched by the name (already
        validated by :meth:`plan`) and checked against the listing's
        blob *sha*; refused if it does not match or is not UTF-8."""
        url = (f"{self.raw_url}/{self.repo}/{commit}/"
               f"{quote(self.path, safe='/')}/{quote(name, safe='')}")
        data = self._get(url, accept="text/plain").content
        if len(data) > MAX_FILE_BYTES:
            raise UpstreamError(f"{name}: {len(data)} bytes, over the limit")
        got = git_blob_sha(data)
        if got != sha:
            raise UpstreamError(
                f"{name}: content does not match the listing "
                f"(blob {got[:10]}, listed {sha[:10]})")
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as e:
            raise UpstreamError(f"{name}: not UTF-8 text ({e})") from e

    # --- the plan ---

    def plan(self, configs_dir: Path, entries: list[dict], commit: str) -> Plan:
        """Classify *entries* (the listing at *commit*) against the mirror
        directory.  A name has to be a plain config file name under
        *into* (``resolve_config_path``) to count at all; anything else is
        skipped with its reason and neither written nor removed."""
        root = Path(configs_dir)
        add, change, same, skipped = [], [], [], []
        names, shas = {}, {}
        seen: set[str] = set()
        for entry in entries:
            name = entry.get("name")
            if not isinstance(name, str) or not name:
                skipped.append(("?", "unnamed entry"))
                continue
            kind = entry.get("type")
            if kind != "file":
                skipped.append((name, f"{kind or 'unknown type'}, not a file"))
                seen.add(f"{self.into}/{name}")
                continue
            if "/" in name:
                skipped.append((name, "not a plain file name"))
                continue
            rel = f"{self.into}/{name}"
            seen.add(rel)
            if not name.endswith(_CONFIG_SUFFIXES):
                skipped.append((name, "not a config file"))
                continue
            try:
                abspath = resolve_config_path(root, rel)
            except ValueError as e:
                skipped.append((name, str(e)))
                continue
            sha = entry.get("sha")
            if not isinstance(sha, str) or not _SHA_RE.fullmatch(sha):
                skipped.append((name, "no blob sha in the listing"))
                continue
            size = entry.get("size")
            if isinstance(size, int) and size > MAX_FILE_BYTES:
                skipped.append((name, f"{size} bytes, over the limit"))
                continue
            names[rel], shas[rel] = name, sha
            try:
                local = git_blob_sha(abspath.read_bytes())
            except FileNotFoundError:
                add.append(rel)
                continue
            (same if local == sha else change).append(rel)
        prefix = self.into + "/"
        remove = [rel for rel in list_config_files(root)
                  if rel.startswith(prefix) and "/" not in rel[len(prefix):]
                  and rel not in seen]
        return Plan(commit=commit, add=add, change=change, same=same,
                    remove=remove, skipped=skipped, names=names, shas=shas)
