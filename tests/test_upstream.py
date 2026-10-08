"""The config library's upstream pull (choco/upstream.py): the ``upstream:``
block validated, the GitHub listing classified against the mirror
directory by git blob sha, names validated before they become paths or
URLs, and every download checked against the listing."""

import pytest
import responses

from choco.upstream import (
    DEFAULTS, MAX_FILE_BYTES, Upstream, UpstreamError, git_blob_sha,
)

API = "https://api.github.com"
RAW = "https://raw.githubusercontent.com"
COMMIT = "b29e72d4e" + "f" * 31


def _entry(name, data=b"", type="file", **extra):
    """A contents-API entry for *name* holding *data* (sha computed as
    GitHub does), with a download_url that must never be fetched."""
    return {"name": name, "type": type, "sha": git_blob_sha(data),
            "size": len(data),
            "download_url": f"https://never.example/{name}", **extra}


class TestFromConfig:
    def test_defaults_are_kotekan_chord(self):
        up = Upstream.from_config(None)
        assert up == Upstream(repo="kotekan/kotekan", ref="chord",
                              path="config/chord", into="chord", timeout=20.0)
        assert up.label == "kotekan/kotekan@chord:config/chord"
        assert Upstream.from_config(dict(DEFAULTS)) == up

    def test_overrides_merge_with_the_defaults(self):
        up = Upstream.from_config({"ref": "develop", "into": "lib/chord"})
        assert (up.repo, up.ref, up.into) == ("kotekan/kotekan", "develop", "lib/chord")

    def test_disabled_is_none(self):
        assert Upstream.from_config({"enabled": False}) is None

    @pytest.mark.parametrize("cfg, message", [
        ({"branch": "chord"}, "unknown upstream key"),
        ({"repo": "kotekan"}, "owner/name"),
        ({"repo": "kotekan/kotekan/config"}, "owner/name"),
        ({"ref": "a..b"}, "upstream.ref"),
        ({"ref": "-rf"}, "upstream.ref"),
        ({"ref": ""}, "upstream.ref"),
        ({"path": "../secrets"}, "upstream.path"),
        ({"into": ".hidden"}, "upstream.into"),
        ({"into": ""}, "upstream.into"),
        ({"timeout": 0}, "upstream.timeout"),
        ({"timeout": "soon"}, "upstream.timeout"),
        ("chord", "mapping"),
    ])
    def test_malformed_block_is_refused(self, cfg, message):
        with pytest.raises(ValueError, match=message):
            Upstream.from_config(cfg)

    def test_directories_lose_their_slashes(self):
        up = Upstream.from_config({"path": "/config/chord/", "into": "chord/"})
        assert (up.path, up.into) == ("config/chord", "chord")


class TestBlobSha:
    def test_matches_git_hash_object(self):
        # `printf 'hello\n' | git hash-object --stdin`
        assert git_blob_sha(b"hello\n") == "ce013625030ba8dba906f756967f9e9ca394464a"
        assert git_blob_sha(b"") == "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"


class TestPlan:
    @pytest.fixture
    def configs(self, tmp_path):
        chord = tmp_path / "chord"
        chord.mkdir()
        (chord / "same.j2").write_bytes(b"a: 1\n")
        (chord / "changed.j2").write_bytes(b"old: 1\n")
        (chord / "extra.j2").write_bytes(b"gone: 1\n")
        (chord / "link.j2").write_bytes(b"link: 1\n")
        (chord / "notes.txt").write_text("not a config\n")
        (chord / "sub").mkdir()
        (chord / "sub" / "deep.j2").write_bytes(b"deep: 1\n")
        (tmp_path / "other").mkdir()
        (tmp_path / "other" / "x.yaml").write_bytes(b"x: 1\n")
        return tmp_path

    def test_classifies_against_the_mirror_by_sha(self, configs):
        up = Upstream.from_config(None)
        entries = [
            _entry("same.j2", b"a: 1\n"),
            _entry("changed.j2", b"new: 1\n"),
            _entry("added.yaml", b"b: 2\n"),
        ]
        plan = up.plan(configs, entries, COMMIT)
        assert plan.commit == COMMIT
        assert plan.same == ["chord/same.j2"]
        assert plan.change == ["chord/changed.j2"]
        assert plan.add == ["chord/added.yaml"]
        assert plan.fetch == ["chord/added.yaml", "chord/changed.j2"]
        # The two local files the listing lacks go; the one in a
        # subdirectory, the other directory and the non-config stay.
        assert plan.remove == ["chord/extra.j2", "chord/link.j2"]
        assert plan.skipped == []
        assert plan.names["chord/added.yaml"] == "added.yaml"
        assert plan.shas["chord/changed.j2"] == git_blob_sha(b"new: 1\n")
        assert not plan.in_sync

    def test_in_sync_when_nothing_differs(self, configs):
        up = Upstream.from_config(None)
        entries = [_entry("same.j2", b"a: 1\n"), _entry("changed.j2", b"old: 1\n"),
                   _entry("extra.j2", b"gone: 1\n"), _entry("link.j2", b"link: 1\n")]
        plan = up.plan(configs, entries, COMMIT)
        assert plan.in_sync and plan.fetch == [] and plan.remove == []

    def test_odd_entries_are_skipped_and_never_cause_a_removal(self, configs):
        up = Upstream.from_config(None)
        entries = [
            _entry("same.j2", b"a: 1\n"),
            _entry("changed.j2", b"old: 1\n"),
            _entry("extra.j2", b"gone: 1\n"),
            _entry("link.j2", b"", type="symlink"),
            _entry("sub", b"", type="dir"),
            _entry("README.md", b"# chord\n"),
            _entry("nested/x.j2", b"x: 1\n"),
            _entry(".hidden.j2", b"h: 1\n"),
            {"name": "nosha.j2", "type": "file", "size": 3},
            {"name": "shortsha.j2", "type": "file", "sha": "abc", "size": 3},
            {"name": "huge.j2", "type": "file", "sha": "a" * 40,
             "size": MAX_FILE_BYTES + 1},
            {"type": "file", "sha": "b" * 40},
        ]
        plan = up.plan(configs, entries, COMMIT)
        assert plan.add == [] and plan.change == [] and plan.remove == []
        reasons = dict(plan.skipped)
        assert "not a file" in reasons["link.j2"] and "symlink" in reasons["link.j2"]
        assert "dir" in reasons["sub"]
        assert reasons["README.md"] == "not a config file"
        assert reasons["nested/x.j2"] == "not a plain file name"
        assert "hidden" in reasons[".hidden.j2"]
        assert reasons["nosha.j2"] == "no blob sha in the listing"
        assert reasons["shortsha.j2"] == "no blob sha in the listing"
        assert "over the limit" in reasons["huge.j2"]
        assert reasons["?"] == "unnamed entry"
        assert "link.j2" not in plan.names   # a skipped name is not fetched


class TestGitHub:
    """The three requests, through ``responses`` (no network)."""

    up = Upstream.from_config(None)

    @responses.activate
    def test_resolve_commit(self):
        responses.get(f"{API}/repos/kotekan/kotekan/commits/chord",
                      json={"sha": COMMIT, "commit": {"message": "x"}})
        assert self.up.resolve_commit() == COMMIT
        sent = responses.calls[0].request
        assert sent.headers["Accept"] == "application/vnd.github+json"
        assert sent.headers["User-Agent"] == "choco"

    @responses.activate
    def test_ref_with_a_slash_is_quoted(self):
        up = Upstream.from_config({"ref": "jbm/config-chord-dir"})
        responses.get(f"{API}/repos/kotekan/kotekan/commits/jbm%2Fconfig-chord-dir",
                      json={"sha": COMMIT})
        assert up.resolve_commit() == COMMIT

    @responses.activate
    @pytest.mark.parametrize("body", [{"sha": "not-a-sha"}, {"message": "x"}, [1]])
    def test_commit_without_a_sha_is_an_error(self, body):
        responses.get(f"{API}/repos/kotekan/kotekan/commits/chord", json=body)
        with pytest.raises(UpstreamError, match="no commit sha"):
            self.up.resolve_commit()

    @responses.activate
    def test_api_error_carries_githubs_message(self):
        responses.get(f"{API}/repos/kotekan/kotekan/commits/chord", status=403,
                      json={"message": "API rate limit exceeded for 1.2.3.4."})
        with pytest.raises(UpstreamError, match="HTTP 403 .*rate limit"):
            self.up.resolve_commit()

    @responses.activate
    def test_unreachable_is_an_error_not_a_crash(self):
        responses.get(f"{API}/repos/kotekan/kotekan/commits/chord",
                      body=ConnectionError("no route"))
        with pytest.raises(UpstreamError, match="ConnectionError"):
            self.up.resolve_commit()

    @responses.activate
    def test_listing_at_the_commit(self):
        entries = [_entry("a.j2", b"a: 1\n"), "junk", _entry("sub", type="dir")]
        responses.get(f"{API}/repos/kotekan/kotekan/contents/config/chord"
                      f"?ref={COMMIT}", json=entries)
        got = self.up.listing(COMMIT)
        assert [e["name"] for e in got] == ["a.j2", "sub"]

    @responses.activate
    def test_listing_of_a_file_is_an_error(self):
        responses.get(f"{API}/repos/kotekan/kotekan/contents/config/chord"
                      f"?ref={COMMIT}", json=_entry("chord", b"x"))
        with pytest.raises(UpstreamError, match="is a file, not a directory"):
            self.up.listing(COMMIT)

    @responses.activate
    def test_fetch_by_validated_name_at_the_commit(self):
        data = "num_dishes: 16  # é\n".encode()
        responses.get(f"{RAW}/kotekan/kotekan/{COMMIT}/config/chord/a%20b.j2",
                      body=data)
        text = self.up.fetch_text(COMMIT, "a b.j2", git_blob_sha(data))
        assert text == "num_dishes: 16  # é\n"
        assert len(responses.calls) == 1   # the listing's download_url is never used

    @responses.activate
    def test_fetch_refuses_content_that_does_not_match_the_listing(self):
        responses.get(f"{RAW}/kotekan/kotekan/{COMMIT}/config/chord/a.j2",
                      body=b"tampered: 1\n")
        with pytest.raises(UpstreamError, match="does not match the listing"):
            self.up.fetch_text(COMMIT, "a.j2", git_blob_sha(b"a: 1\n"))

    @responses.activate
    def test_fetch_refuses_non_utf8(self):
        data = b"\xff\xfe\x00binary"
        responses.get(f"{RAW}/kotekan/kotekan/{COMMIT}/config/chord/a.j2", body=data)
        with pytest.raises(UpstreamError, match="not UTF-8"):
            self.up.fetch_text(COMMIT, "a.j2", git_blob_sha(data))

    @responses.activate
    def test_fetch_http_error(self):
        responses.get(f"{RAW}/kotekan/kotekan/{COMMIT}/config/chord/a.j2", status=404)
        with pytest.raises(UpstreamError, match="HTTP 404"):
            self.up.fetch_text(COMMIT, "a.j2", "a" * 40)
