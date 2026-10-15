"""The config library: the files under the configs directory, which node
renders or includes which, and the writes that change them.

A write is checked before it lands by rendering every node that uses the
file with the new text overlaid on disk (:func:`check_save`, and
:func:`check_saves` for a set that is only consistent as a whole, the
upstream pull), then handed to the sync loop; selecting a node's file is
a nodes.yaml edit and rebuilds the registry (:func:`set_node_configs`).
The routes in :mod:`choco.web` call these with the live registry and
orchestrator; nothing here reads the request or the app.
"""

import logging
from pathlib import Path

import jinja2
import yaml

from .datafiles import utc_minute
from .jobclient import write_text_atomic
from .state import resolve_config_path
from .upstream import UpstreamError

logger = logging.getLogger(__name__)


def file_path(registry, rel: str) -> Path:
    """``configs_dir / rel`` once *rel* passes ``resolve_config_path``
    (``ValueError`` otherwise).  The plain join, deliberately: ``users_of``,
    ``Node.config_abspath`` and ``Orchestrator.file_written`` compare plain
    joins too, so a resolved path would not match them under a symlinked
    configs directory."""
    resolve_config_path(registry.configs_dir, rel)
    return registry.configs_dir / rel


def entry(registry, rel: str) -> dict:
    """One row of the config library: *rel*'s size and mtime plus the
    nodes that render it (``used_by``) or include it (``included_by``)."""
    path = registry.configs_dir / rel
    try:
        st = path.stat()
        size, mtime = st.st_size, st.st_mtime
    except OSError:
        size, mtime = None, None
    direct, includers = registry.users_of(rel)
    return {
        "path": rel,
        "size": size,
        "mtime": mtime,
        "modified": utc_minute(mtime) if mtime else None,     # UTC, like every page
        "used_by": [n.key for n in direct],
        "included_by": [n.key for n in includers],
        # vars.yaml is the shared template context: every node reads it.
        "is_vars": rel == "vars.yaml",
    }


def check_save(registry, rel: str, content: str
                        ) -> tuple[list[str], str | None]:
    """Validate *content* as the new text of library file *rel* without
    writing it: every node that renders or includes the file is rendered
    with the text overlaid on disk.

    Returns ``(affected node keys, error)``; the error is None when every
    user renders.  vars.yaml must parse as a YAML mapping (every node
    reads it); a file no node uses is only parsed as a template, since
    nothing says it must render to a config on its own.
    """
    if rel == "vars.yaml":
        try:
            data = yaml.safe_load(content)
        except yaml.YAMLError as e:
            return [], f"Invalid YAML: {e}"
        if data is not None and not isinstance(data, dict):
            return [], "vars.yaml must be a YAML mapping"
        return [n.key for n in registry.affected_by(rel)], None
    return check_saves(registry, {rel: content})


def check_saves(registry, texts: dict[str, str]
                         ) -> tuple[list[str], str | None]:
    """The save check for several files at once (the upstream pull):
    *texts* maps each library path to its new text, and every node that
    renders or includes any of them is rendered with all of them overlaid
    on disk -- so a set that is only consistent as a whole (an include
    and the file that includes it, both changed) is judged as a whole,
    never one new text against the old text of the others.  Returns
    ``(affected node keys, error)`` like :func:`check_save`.
    """
    overlay = {registry.configs_dir / rel: content
               for rel, content in texts.items()}
    affected: dict[str, None] = {}
    for rel, content in texts.items():
        direct, includers = registry.users_of(rel)
        for node in direct + includers:
            # a direct user renders the new text; an includer its own
            # file, which may itself be in the set
            base = content if node in direct else overlay.get(node.config_abspath, node.base_content)
            if base is None:
                continue
            try:
                node.render(base, overlay=overlay)
            except Exception as e:
                return [], f"{node.key} would not render: {e}"
            affected[node.key] = None
        if not direct and not includers:
            try:
                jinja2.Environment(autoescape=False).parse(content)
            except jinja2.TemplateSyntaxError as e:
                where = f"{rel}: " if len(texts) > 1 else ""
                return [], f"{where}Template syntax: {e}"
    return list(affected), None


def write_file(registry, orchestrator, rel: str, content: str, user: str) -> list[str]:
    """Write library file *rel* atomically and hand the change to the
    sync loop now (the mtime scan would see it within a tick anyway).

    Returns the keys of the nodes re-rendered.  Audit-logged, since one
    library write can change what several nodes run.
    """
    path = registry.configs_dir / rel
    created = not path.exists()
    write_text_atomic(path, content)
    affected = [n.key for n in registry.affected_by(rel)]
    logger.warning(
        f"config library: {rel} {'created' if created else 'replaced'} by "
        f"{user}, {len(content)} bytes; re-rendering {affected or 'no nodes'}")
    orchestrator.file_written(path)
    return affected


def pull(registry, orchestrator, upstream, dry_run: bool, user: str) -> tuple[dict, int]:
    """Mirror the upstream config directory (config.yaml's ``upstream:``,
    kotekan's ``config/chord/`` by default) into the library, as one set.

    The listing is classified against the mirror directory by git blob
    sha (``Upstream.plan``), only new and changed files are fetched, and
    the fetched texts are validated together through every node that
    renders or includes any of them before anything is written: a
    refusal writes nothing.  A file the listing no longer has is removed
    unless a node still renders or includes it -- then it is kept and
    reported, so a rename upstream lands as an addition the operator
    can switch the node to, and the next pull removes the old file.
    Every write is on disk before the sync loop hears of any of them, so
    no node re-renders a half-pulled set, and the nodes re-rendered are
    put in maintenance first (those not already), so a pull never
    restarts anything: the new set is desired state the sync loop pushes
    when the operator lifts maintenance.  With *dry_run* the reply is
    the plan and nothing is written or paused.  Audit-logged like a
    library save.
    """
    if upstream is None:
        return {"error": "no upstream configured (upstream: in config.yaml)"}, 404
    try:
        commit = upstream.resolve_commit()
        plan = upstream.plan(registry.configs_dir, upstream.listing(commit),
                             commit)
        texts = {rel: upstream.fetch_text(commit, plan.names[rel], plan.shas[rel])
                 for rel in plan.fetch}
    except UpstreamError as e:
        return {"error": f"GitHub: {e}"}, 502
    remove, kept = [], []
    for rel in plan.remove:
        direct, includers = registry.users_of(rel)
        if direct or includers:
            kept.append({"path": rel,
                         "used_by": [n.key for n in direct + includers]})
        else:
            remove.append(rel)
    affected, error = check_saves(registry, texts)
    if error:
        return {"error": error, "commit": commit}, 409
    to_pause = [registry.get_node(key) for key in affected
                if not registry.get_node(key).maintenance]
    changing = bool(texts or remove)
    result = {
        "status": ("planned" if dry_run
                   else "pulled" if changing else "unchanged"),
        "dry_run": dry_run,
        "upstream": upstream.label,
        "commit": commit,
        "into": upstream.into,
        "added": plan.add,
        "changed": plan.change,
        "removed": remove,
        "kept": kept,
        "skipped": [{"name": n, "reason": r} for n, r in plan.skipped],
        "unchanged": len(plan.same),
        "reloaded": affected,
        "paused": [n.key for n in to_pause],
    }
    result["summary"] = pull_summary(result)
    if dry_run or not changing:
        return result, 200
    # Paused before the files land: the sync loop then re-renders them
    # onto the new set but makes no REST write until maintenance lifts.
    orchestrator.set_flag(to_pause, "maintenance", True)
    written = []
    for rel in plan.fetch:
        path = registry.configs_dir / rel
        write_text_atomic(path, texts[rel])
        written.append(path)
    for rel in remove:
        path = registry.configs_dir / rel
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        written.append(path)
    logger.warning(
        f"config library: pulled {upstream.label} @ {commit[:10]} into "
        f"{upstream.into}/ by {user}: added {plan.add or 'none'}, changed "
        f"{plan.change or 'none'}, removed {remove or 'none'}"
        + (f", kept (in use) {[k['path'] for k in kept]}" if kept else "")
        + f"; re-rendering {affected or 'no nodes'}"
        + (f", paused {result['paused']}" if to_pause else ""))
    for path in written:
        orchestrator.file_written(path)
    return result, 200


def pull_summary(result: dict) -> str:
    """One line saying what a pull did (the flash; ``summary`` in the
    JSON): the files by name under the mirror directory, the nodes
    re-rendered and paused, then what was kept or skipped and why."""
    into = result["into"] + "/"
    where = f"{result['upstream']} @ {result['commit'][:10]}"

    def names(rels):
        return ", ".join(r.removeprefix(into) for r in rels)

    parts = [f"{verb} {names(result[verb])}"
             for verb in ("added", "changed", "removed") if result[verb]]
    if not parts:
        text = f"{into} is up to date with {where} ({result['unchanged']} files)"
    else:
        verb = "Would pull" if result["dry_run"] else "Pulled"
        text = f"{verb} {where} into {into}: " + "; ".join(parts)
        if result["reloaded"]:
            dry = result["dry_run"]
            nodes = ", ".join(result["reloaded"])
            paused = result.get("paused") or []
            if paused == result["reloaded"]:
                text += ("; would re-render and pause " if dry
                         else "; re-rendered and paused ") + nodes
            elif paused:
                already = [k for k in result["reloaded"] if k not in paused]
                text += (f"; {'would re-render' if dry else 're-rendered'} {nodes}, "
                         f"{'pausing' if dry else 'paused'} {', '.join(paused)} "
                         f"({', '.join(already)} already in maintenance)")
            else:
                text += (f"; {'would re-render' if dry else 're-rendered'} {nodes} "
                         "(already in maintenance)")
            if not dry:
                text += "; lift maintenance to restart them onto it"
    if result["kept"]:
        text += "; kept " + ", ".join(
            f"{k['path']} (removed upstream, used by {', '.join(k['used_by'])})"
            for k in result["kept"])
    if result["skipped"]:
        text += "; skipped " + ", ".join(
            f"{s['name']} ({s['reason']})" for s in result["skipped"])
    return text


def set_node_configs(registry, orchestrator, nodes: list, config: str | None, user: str
                     ) -> tuple[dict, int]:
    """Point *nodes* at library file *config* (None: back to each node's
    per-node file) by rewriting nodes.yaml and rebuilding the registry.

    A registry rebuild is the mechanism, so this carries its cost: the
    whole cluster lands in maintenance until the operator lifts it
    (``Orchestrator.apply_nodes_update``).  The file must exist: the
    selectors offer only existing files, and a node pointed at a missing
    one would just sit on "No config file".
    """
    if config is not None:
        try:
            path = file_path(registry, config)
        except ValueError as e:
            return {"error": str(e)}, 400
        if not path.is_file():
            return {"error": f"No such config file: {config}"}, 404
    nodes_file = registry.configs_dir / "nodes.yaml"
    try:
        data = yaml.safe_load(nodes_file.read_text()) or {}
    except (OSError, yaml.YAMLError) as e:
        return {"error": f"nodes.yaml unreadable: {e}"}, 500
    for node in nodes:
        entry = ((data.get("groups") or {}).get(node.group) or {}).get(node.name)
        if not isinstance(entry, dict):
            return {"error": f"{node.key} is not in nodes.yaml"}, 409
        if config is None:
            entry.pop("config", None)
        else:
            entry["config"] = config
    keys = [n.key for n in nodes]
    logger.warning(
        f"config selection by {user}: {keys} -> {config or 'per-node file'}; "
        "registry rebuilt, cluster in maintenance")
    orchestrator.apply_nodes_update(data)
    return {"status": "reloaded", "config": config, "nodes": keys,
            "maintenance": True}, 200
