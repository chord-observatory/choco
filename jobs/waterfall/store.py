#!/usr/bin/env python3
"""The on-disk waterfall store for one acquisition.

Layout, mirroring the acquisition it describes::

    <waterfalls>/<root>/<acq>/
        index.json          the commit record — see below
        freq.npy            frequency axis, written once
        times.bin           int64 per scanline, appended
        counts.npy          (n_prod, 256) index histogram, for the equalization
        thumbs.dat          (n_files, n_prod, n_bins) uint8, appended
        coarse.dat          (n_rows, n_prod_file, n_chunk) uint8: every product,
                            mean |V| per cell, appended        } the overview,
        matrix.dat          (n_files, n_band, n_el, n_el)      } rows_per_file rows
        coarse_times.bin    int64 per overview row             } per file, one matrix
                                                               } frame (reduce.OverviewSpec)
        diag.jsonl          one JSON line per file: masks and delivery (reduce.read_diag)
        co_array.png        the array's coherence: per cell the median over the elements
                            of their co_eNNNN.png, rows_per_file scanlines per file
        co_array_full.png   the same per channel and bin (overview ``array_full``)
        co_matrix.png       (n_el, n_el) the element x element coherence: below the diagonal
                            averaged over every band and file so far (matrix_mean.npz
                            its running sums), above it the latest file's; the palette
                            equalized over both
        coarse_freq.npy     (n_chunk,) column centres, coarse_scale.npy (n_prod_file, 2)
        palette.bin         the 256 x 3 palette the overview is drawn with
        e0000/wf_e0000xe0000.png
              th_e0000xe0000.png
              co_e0000.png  the element's coherence, one scanline per file

Every product is one PNG that *grows*: kotekan's files land atomically and
never change, so a new file is appended as scanlines and nothing already
written is rewritten.  One triangle of images per acquisition, not one per
source file.

``index.json`` is the commit point and the only authority on what is
done.  It records, per append-only file, the byte length of the last
completed write, and it is written *after* every other write.  So a run
that died partway through leaves files that are too long, never too
short, and the next run truncates them back and redoes the append.  That
is the whole crash-recovery story; there is no repair pass.

Element indices are zero-padded to four digits and sharded by the first
input, so a lexical sort is a numeric sort and no directory holds more
entries than there are elements — which is what keeps the scheme usable
when the array grows from 32 elements to several hundred.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import numpy as np

import wfpng
from choco.acqtree import shard_name
from choco.jobclient import atomic_output
from reduce import (Axes, FileOverview, OverviewSpec, Selection, array_levels, everything,
                    overview_spec, quantize_log, quantize_unit, row_parts, scale_from_sample,
                    selection_from_index, thumb_rows, unquantize_unit)

log = logging.getLogger("waterfall.store")

INDEX_VERSION = 1

#: Frequency bins kept per product for the contact-sheet thumbnails.
THUMB_BINS = 128

#: Most scanlines a thumbnail is allowed; longer acquisitions bin time
#: down to fit, so a thumbnail stays small however long the night runs.
THUMB_MAX_ROWS = 256


def _atomic_write(path: Path, data: bytes) -> None:
    # fsync'd before the rename: the commit order (see ``append``) is the
    # crash-safety contract, and it only holds if each file is durable
    with atomic_output(path, fsync=True) as tmp:
        tmp.write_bytes(data)


def _append_at(path: Path, offset: int, data: bytes) -> int:
    """Append ``data`` at a *recorded* offset, returning the new length.

    Writing at the recorded offset rather than at EOF is what makes this
    idempotent: a torn write from a previous run is simply overwritten.
    """
    mode = "r+b" if path.exists() else "wb"
    with open(path, mode) as f:
        f.seek(0, 2)
        if f.tell() < offset:
            raise ValueError(f"{path}: {f.tell()} bytes, index records {offset}")
        f.seek(offset)
        f.truncate()
        f.write(data)
        return offset + len(data)


class AcquisitionStore:
    """Open (or start) the waterfall store for one acquisition."""

    def __init__(self, path, acq_name: str, source_root: str,
                 source_path: str | None = None):
        self.path = Path(path)
        self.acq_name = acq_name
        self.source_root = source_root
        self.source_path = source_path
        self.index: dict = {}
        self._counts: np.ndarray | None = None
        self._coarse_scale: tuple | None = None
        self._load()

    # --- index ----------------------------------------------------------

    @property
    def index_path(self) -> Path:
        return self.path / "index.json"

    def _load(self) -> None:
        if not self.index_path.exists():
            return
        try:
            self.index = json.loads(self.index_path.read_text())
        except (OSError, ValueError) as e:
            # One unreadable index costs one acquisition, not the run —
            # the same containment Registry.reload applies to configs.
            log.error("%s: unreadable index (%s); acquisition skipped",
                      self.index_path, e)
            self.index = {"broken": True}

    @property
    def broken(self) -> bool:
        return bool(self.index.get("broken"))

    @property
    def started(self) -> bool:
        return bool(self.index) and not self.broken

    @property
    def processed(self) -> set:
        """Source-file indices already folded in."""
        return set(self.index.get("files", []))

    @property
    def gains_divided(self) -> bool:
        """Whether |V| has the digital gains divided out (``start``)."""
        return self.index.get("gains") == "divided"

    @property
    def committed(self) -> bool:
        """Whether any file has been folded in.  Until one has, the index
        records a shape and a scale and no pixel depends on them, so an
        acquisition may be started again under the current config."""
        return bool(self.index.get("files"))

    @property
    def prestart_path(self) -> Path:
        """Skips recorded before the acquisition has an index.

        An acquisition only gets its index once a file can set the value
        scale.  Until then its unusable files (an empty first file, or a
        run with no data at all) still have to be remembered, or every
        run would re-read them and never get past them.  ``start`` folds
        this file into the index and removes it; choco's read side ignores
        a directory that holds only this file.
        """
        return self.path / "skipped.json"

    def _prestart_skipped(self) -> dict:
        try:
            doc = json.loads(self.prestart_path.read_text())
        except (OSError, ValueError):
            return {}
        return doc if isinstance(doc, dict) else {}

    @property
    def skipped(self) -> dict:
        """Source files that can never be folded in, and why.

        Kept so a permanently-unusable file is reported once instead of
        reappearing in the backlog — and in the exit code — on every run.
        """
        if not self.started:
            return self._prestart_skipped()
        return self.index.get("skipped") or {}

    def skip(self, entries: dict) -> None:
        """Record files as permanently unusable and commit.

        Takes a mapping so a batch costs one commit.  Before the index
        exists the entries go to ``skipped.json``; ``start`` adopts them.
        """
        if not entries or self.broken:
            return
        if not self.started:
            merged = {**self._prestart_skipped(), **entries}
            self.path.mkdir(parents=True, exist_ok=True)
            _atomic_write(self.prestart_path, json.dumps(merged, separators=(",", ":")).encode())
            return
        self.index.setdefault("skipped", {}).update(entries)
        self._save_index()

    @property
    def n_rows(self) -> int:
        prods = self.index.get("products") or [{}]
        return int(prods[0].get("rows", 0))

    def _save_index(self) -> None:
        self.index["updated"] = time.time()
        if self.source_path and not self.index.get("source_path"):
            # written by a version that did not record it; fill it in so
            # choco can match this acquisition to its scan root exactly
            self.index["source_path"] = self.source_path
        _atomic_write(self.index_path,
                      json.dumps(self.index, separators=(",", ":")).encode())
        _atomic_write(self.path / "summary.json",
                      json.dumps(self.summary()).encode())

    def summary(self) -> dict:
        """The handful of numbers /files needs.

        Written beside the index because the index carries one record per
        product: at 100 elements it is ~700 KB, and choco's 30 s sweep
        would otherwise pull and parse ~23 MB off NFS to count files.
        """
        products = self.index.get("products") or []
        return {
            "rendered": len(self.index.get("files") or []),
            "products": len(products),
            "rows": int(products[0].get("rows", 0)) if products else 0,
            "elements": int(self.index.get("n_elements") or 0),
            "skipped": len(self.index.get("skipped") or {}),
            "source_path": self.index.get("source_path"),
            "updated": self.index.get("updated"),
        }

    # --- creation -------------------------------------------------------

    def start(self, axes: Axes, lo: np.ndarray, hi: np.ndarray,
              sel: Selection | None = None, overview: OverviewSpec | None = None,
              gains_divided: bool = False, scale_groups: list | None = None) -> None:
        """Record the acquisition's shape and its frozen value scale.

        The scale is fixed here for the acquisition's whole life: it is
        baked into every pixel and there is no way to revise it without
        rewriting every image.  What stays adjustable is the *display*,
        which lives in the palette.  ``sel`` says which products and
        channels of the file are rendered (every one of them by default)
        and is recorded with the same permanence: the products by their
        inputs, the channels by their indices (``freq_idx``, only when
        not every channel), so later files are matched to it by name and
        never by position.  ``overview`` adds the three cubes of
        :class:`reduce.OverviewSpec` (recorded in the index as
        ``overview``; their column frequencies in ``coarse_freq.npy``,
        the palette the web renders them with in ``palette.bin``).
        ``gains_divided`` records that |V| has the digital gains divided
        out (``gains: "divided"``), which every later file must match.
        ``scale_groups`` (``reduce.auto_groups``) lists products that
        share one ``(lo, hi)`` and one palette (``palette_counts``).
        """
        if sel is None:
            sel = everything(axes)
        self.path.mkdir(parents=True, exist_ok=True)
        np.save(self.path / "freq.npy", sel.freq_mhz)
        names = sel.product_names()
        self.index = {
            "version": INDEX_VERSION,
            "acquisition": self.acq_name,
            "source_root": self.source_root,
            "created": time.time(),
            "n_freq": int(sel.n_freq),
            "n_prod": int(sel.n_prod),
            "n_elements": int(axes.n_elements),
            # the acquisition this was rendered from, so choco can match
            # a scan root to its images without guessing from basenames
            "source_path": self.source_path,
            # the *effective* bin count: thumb_rows clamps to the band
            # width, and the stride recorded here has to match what was
            # actually written or thumbs.dat reshapes wrong.
            "thumb_bins": min(THUMB_BINS, int(sel.n_freq)),
            "labels": list(axes.labels),
            "files": [],
            "skipped": self._prestart_skipped(),
            "thumb_stride": 0,
            "thumb_rows": 0,
            "times_bytes": 0,
            "thumbs_bytes": 0,
            "products": [
                {"name": names[p],
                 "a": int(sel.input_a[p]), "b": int(sel.input_b[p]),
                 "lo": (float(lo[p]) if np.isfinite(lo[p]) else None),
                 "hi": (float(hi[p]) if np.isfinite(hi[p]) else None),
                 "width": 0, "rows": 0, "nbytes": 0, "adler": 0}
                for p in range(sel.n_prod)
            ],
        }
        if gains_divided:
            self.index["gains"] = "divided"
        if scale_groups:
            self.index["scale_groups"] = [[int(p) for p in g] for g in scale_groups]
        # the file shape this acquisition accepts; the selection within it
        self.index["n_prod_file"] = int(axes.n_prod)
        self.index["n_freq_file"] = int(axes.n_freq)
        if not sel.is_all_channels(axes):
            self.index["freq_idx"] = [int(i) for i in sel.freq_idx]
        if overview is not None:
            self.index["overview"] = overview.to_index()
            self.index["coarse_bytes"] = 0
            self.index["matrix_bytes"] = 0
            self.index["coarse_times_bytes"] = 0
            self.index["coarse_rows"] = 0
            # one image per element, a scanline per file (reduce.OverviewSpec)
            self.index["elements"] = [
                {"e": e, "name": shard_name(e), "width": 0, "rows": 0, "nbytes": 0, "adler": 0}
                for e in range(overview.n_el)]
            if overview.array_full:
                self.index["array_full"] = {"width": 0, "rows": 0, "nbytes": 0, "adler": 0}
            np.save(self.path / "coarse_freq.npy", np.asarray(overview.chunk_freq, np.float64))
            _atomic_write(self.path / "palette.bin", wfpng.palette().astype(np.uint8).tobytes())
        self._coarse_scale = None
        self._counts = np.zeros((sel.n_prod, 256), np.int64)
        self._save_index()
        try:
            self.prestart_path.unlink()
        except OSError:
            pass

    def relabel(self, labels) -> bool:
        """Replace the element labels and commit; True if they changed.

        Labels are the one thing in the index that is *about* the data
        rather than derived from it, so they are the one thing a later,
        better reading of the source can revise without touching a pixel.
        """
        labels = [str(label) for label in labels]
        if not self.started or labels == list(self.index.get("labels") or []):
            return False
        self.index["labels"] = labels
        self._save_index()
        return True

    def scale(self) -> tuple[np.ndarray, np.ndarray]:
        """The frozen per-product ``(lo, hi)``, nan where unscaled."""
        prods = self.index["products"]
        lo = np.array([np.nan if p["lo"] is None else p["lo"] for p in prods])
        hi = np.array([np.nan if p["hi"] is None else p["hi"] for p in prods])
        return lo, hi

    def selection(self, axes: Axes) -> tuple[Selection | None, str | None]:
        """The recorded selection applied to a file's axes, or why the
        file cannot join this acquisition (:func:`selection_from_index`)."""
        return selection_from_index(axes, self.index)

    @property
    def has_overview(self) -> bool:
        return isinstance(self.index.get("overview"), dict)

    def overview_spec(self, axes: Axes, sel: Selection) -> OverviewSpec | None:
        """The acquisition's overview applied to a file, or None when it
        keeps none.  The columns are rebuilt from the selection and must
        number what the index recorded."""
        rec = self.index.get("overview")
        if not isinstance(rec, dict):
            return None
        spec = overview_spec(axes, sel, edges=rec.get("edges"), col_width=rec.get("col_width"),
                             rows_per_file=rec.get("rows_per_file"), array_full=bool(rec.get("array_full")))
        if spec.n_chunk != int(rec.get("chunks") or 0) or spec.n_el != int(rec.get("n_el") or 0):
            raise ValueError(f"the overview has {rec.get('chunks')} columns for "
                             f"{rec.get('n_el')} elements, this file gives {spec.n_chunk} "
                             f"for {spec.n_el}")
        return spec

    def coarse_scale(self, coarse: np.ndarray | None = None) -> tuple[np.ndarray, np.ndarray]:
        """The per-product ``(lo, hi)`` the coarse cube is quantized with:
        frozen from the first file's chunk means (``coarse_scale.npy``),
        like the images' scale from their first file."""
        if self._coarse_scale is not None:
            return self._coarse_scale
        f = self.path / "coarse_scale.npy"
        if f.exists():
            arr = np.load(f)
            self._coarse_scale = (arr[:, 0].astype(float), arr[:, 1].astype(float))
            return self._coarse_scale
        if coarse is None:
            raise ValueError("no coarse scale yet and no file to set it from")
        lo, hi = scale_from_sample(coarse, min_values=2)     # one value per chunk, few in a test
        _atomic_write(f, _npy_bytes(np.stack([lo, hi], axis=1).astype(np.float32)))
        self._coarse_scale = (lo, hi)
        return self._coarse_scale

    def matches(self, axes: Axes) -> str | None:
        """Why ``axes`` cannot be appended to this store, or None."""
        _, why = self.selection(axes)
        return why

    # --- paths ----------------------------------------------------------

    def product_dir(self, p: int) -> Path:
        return self.path / shard_name(self.index['products'][p]['a'])

    def image_path(self, p: int) -> Path:
        return self.product_dir(p) / f"wf_{self.index['products'][p]['name']}.png"

    def thumb_path(self, p: int) -> Path:
        return self.product_dir(p) / f"th_{self.index['products'][p]['name']}.png"

    def coherence_path(self, e: int) -> Path:
        return self.path / shard_name(e) / f"co_{shard_name(e)}.png"

    @property
    def array_path(self) -> Path:
        return self.path / "co_array.png"

    @property
    def array_full_path(self) -> Path:
        return self.path / "co_array_full.png"

    @property
    def rows_per_file(self) -> int:
        """Overview rows a file (1 before 2026-10-27)."""
        return max(1, int((self.index.get("overview") or {}).get("rows_per_file") or 1))

    @property
    def matrix_mean_path(self) -> Path:
        return self.path / "co_matrix.png"

    # --- the append -----------------------------------------------------

    def palette_counts(self) -> np.ndarray:
        """The histograms the palettes equalize to: each product's own,
        pooled over its ``scale_groups`` group, so a group's images share
        one palette as they share one scale."""
        counts = self.counts()
        groups = self.index.get("scale_groups")
        if not groups:
            return counts
        counts = counts.copy()
        for g in groups:
            counts[g] = counts[g].sum(axis=0)
        return counts

    def counts(self) -> np.ndarray:
        if self._counts is None:
            f = self.path / "counts.npy"
            n_prod = self.index["n_prod"]
            if f.exists():
                try:
                    c = np.load(f)
                    if c.shape == (n_prod, 256):
                        self._counts = c.astype(np.int64)
                except (OSError, ValueError) as e:
                    log.warning("%s: unreadable (%s); histogram restarted", f, e)
            if self._counts is None:
                self._counts = np.zeros((n_prod, 256), np.int64)
        return self._counts

    def add_file(self, axes: Axes, indices: np.ndarray,
                 level: int = wfpng.DEFAULT_LEVEL,
                 overview: FileOverview | None = None,
                 diag: dict | None = None) -> None:
        """Fold one source file in.

        Order matters and is the crash-safety contract: every append
        happens first, from its own recorded offset, and ``index.json``
        is written last.  Until that final write nothing has been
        committed, and re-running redoes exactly the same work.  An
        acquisition with an overview must be given the file's
        (:class:`reduce.FileOverview`): its cubes grow one row per file,
        in step with the images.  ``diag`` (:func:`reduce.read_diag`) is
        appended to ``diag.jsonl``; each line names its file, so a file
        without one leaves a gap rather than misaligning the rest.
        """
        prods = self.index["products"]
        n_prod = len(prods)
        if indices.shape[0] != n_prod:
            raise ValueError(f"{indices.shape[0]} products, index has {n_prod}")
        if self.has_overview and overview is None:
            raise ValueError("this acquisition keeps an overview; the file's is missing")
        if overview is not None and not self.has_overview:
            overview = None                         # started without one: keep none

        pal_default = wfpng.palette()
        new_state = []
        for p in range(n_prod):
            rec = prods[p]
            path = self.image_path(p)
            if rec["rows"] == 0:
                path.parent.mkdir(parents=True, exist_ok=True)
                st = wfpng.create(path, indices[p], pal_default, level=level)
            else:
                st = wfpng.append(path, indices[p], wfpng.WfState.from_dict(rec),
                                  level=level)
            new_state.append(st)

        counts = self.counts()
        wfpng.index_counts(indices, out=counts)

        thumbs = thumb_rows(indices, self.index["thumb_bins"])
        thumbs_bytes = _append_at(self.path / "thumbs.dat",
                                  int(self.index["thumbs_bytes"]), thumbs.tobytes())
        times = (np.asarray(axes.times_ns, np.int64) if axes.times_ns is not None
                 else np.zeros(indices.shape[1], np.int64))
        times_bytes = _append_at(self.path / "times.bin",
                                 int(self.index["times_bytes"]), times.tobytes())

        _atomic_write(self.path / "counts.npy", _npy_bytes(counts))

        cube_bytes = {}
        if overview is not None:
            rec = self.index["overview"]
            n_chunk, n_el = int(rec["chunks"]), int(rec["n_el"])
            n_band = max(1, len(rec["edges"]) - 1)
            r, n_pf = self.rows_per_file, int(self.index["n_prod_file"])
            full_rec = self.index.get("array_full")
            if (overview.coarse.shape != (r, n_pf, n_chunk)
                    or overview.coherence.shape != (n_el, r, n_chunk)
                    or overview.matrix.shape != (n_band, n_el, n_el)
                    or (full_rec is not None and overview.array is None)
                    or len(self.index.get("elements") or []) != n_el):
                raise ValueError("the file's overview does not have the acquisition's shape")
            by_prod = overview.coarse.transpose(1, 0, 2)              # (n_pf, r, n_chunk)
            lo, hi = self.coarse_scale(by_prod)
            q = quantize_log(by_prod.reshape(n_pf, -1), lo, hi).reshape(n_pf, r, n_chunk)
            cube_bytes["coarse_bytes"] = _append_at(
                self.path / "coarse.dat", int(self.index["coarse_bytes"]),
                np.ascontiguousarray(q.transpose(1, 0, 2)).tobytes())
            coh = quantize_unit(overview.coherence)                  # (n_el, r, n_chunk)
            elements = self.index["elements"]
            coh_state = []
            for e in range(n_el):
                rec = elements[e]
                path = self.coherence_path(e)
                if rec["rows"] == 0:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    st = wfpng.create(path, coh[e], pal_default, level=level)
                else:
                    st = wfpng.append(path, coh[e], wfpng.WfState.from_dict(rec),
                                      level=level)
                coh_state.append(st)
            # the array's rows, while the image is in step with the elements'
            array_rec = self.index.get("array")
            done_rows = int(self.index.get("coarse_rows", 0))
            array_state = None
            if array_rec and int(array_rec.get("rows", 0)) == done_rows * r:
                array_state = wfpng.append(self.array_path, array_levels(coh),
                                           wfpng.WfState.from_dict(array_rec), level=level)
            elif not array_rec and done_rows == 0:
                array_state = wfpng.create(self.array_path, array_levels(coh),
                                           pal_default, level=level)
            full_state = None
            if full_rec is not None:
                if int(full_rec.get("rows", 0)) == 0:
                    full_state = wfpng.create(self.array_full_path, overview.array,
                                              pal_default, level=level)
                else:
                    full_state = wfpng.append(self.array_full_path, overview.array,
                                              wfpng.WfState.from_dict(full_rec), level=level)
            cube_bytes["matrix_bytes"] = _append_at(
                self.path / "matrix.dat", int(self.index["matrix_bytes"]),
                quantize_unit(overview.matrix).tobytes())
            # each row's time: its first bin's
            row_times = [int(times[t0]) if t0 < times.size else 0
                         for t0, _ in row_parts(int(times.size), r)]
            cube_bytes["coarse_times_bytes"] = _append_at(
                self.path / "coarse_times.bin", int(self.index["coarse_times_bytes"]),
                np.array(row_times, np.int64).tobytes())

        if diag is not None:
            cube_bytes["diag_bytes"] = _append_at(
                self.path / "diag.jsonl", int(self.index.get("diag_bytes") or 0),
                json.dumps(diag, separators=(",", ":")).encode() + b"\n")

        for p, st in enumerate(new_state):
            prods[p].update(st.to_dict())
        self.index["files"] = sorted(self.processed | {int(axes.file_idx)})
        self.index["thumbs_bytes"] = thumbs_bytes
        self.index["times_bytes"] = times_bytes
        if cube_bytes:
            self.index.update(cube_bytes)
        if overview is not None:
            self.index["coarse_rows"] = int(self.index.get("coarse_rows", 0)) + 1
            for e, st in enumerate(coh_state):
                self.index["elements"][e].update(st.to_dict())
            if array_state is not None:
                self.index["array"] = array_state.to_dict()
            if full_state is not None:
                self.index["array_full"] = full_state.to_dict()
        self._save_index()                      # <- the commit

    def update_array_coherence(self, level: int = wfpng.DEFAULT_LEVEL) -> bool:
        """Rebuild ``co_array.png`` from the elements' images when it is
        behind them: an acquisition started before it existed.  Cheap (one
        scanline per file per element, all already rendered) and only
        when the row counts differ; True if it was rewritten."""
        if not self.started or not self.has_overview:
            return False
        rows = int(self.index.get("coarse_rows") or 0) * self.rows_per_file
        elements = self.index.get("elements") or []
        if rows == 0 or int((self.index.get("array") or {}).get("rows", 0)) == rows:
            return False
        if any(int(rec.get("rows", 0)) != rows for rec in elements):
            return False
        try:
            stack = np.stack([wfpng.read(self.coherence_path(e))[0][:rows]
                              for e in range(len(elements))])
            st = wfpng.create(self.array_path, array_levels(stack), wfpng.palette(), level=level)
        except (OSError, ValueError) as e:
            log.warning("%s: array coherence not rebuilt (%s)", self.path, e)
            return False
        self.index["array"] = st.to_dict()
        self._save_index()
        return True

    def update_matrix_mean(self, level: int = wfpng.DEFAULT_LEVEL) -> bool:
        """Bring ``co_matrix.png`` up to the committed rows of
        ``matrix.dat``: below the diagonal each pair's coherence averaged
        (linearly) over every band and file, above it the latest file's,
        averaged over the bands; its palette equalized over the pairs.  The running sums and the rows they cover
        live in ``matrix_mean.npz``, so a run reads only the new rows; a sum
        ahead of the commit (a crashed run's rows, truncated since) is
        rebuilt.  True if the image was rewritten."""
        if not self.started or not self.has_overview:
            return False
        rec = self.index["overview"]
        n, n_band = int(rec["n_el"]), max(1, len(rec["edges"]) - 1)
        rows = int(self.index.get("coarse_rows") or 0)
        if rows == 0:
            return False
        acc = self.path / "matrix_mean.npz"
        total, count, have = np.zeros((n, n)), np.zeros((n, n)), 0
        try:
            with np.load(acc) as z:
                if z["total"].shape == (n, n) and int(z["rows"]) <= rows:
                    total, count, have = z["total"], z["count"], int(z["rows"])
        except (OSError, ValueError, KeyError):
            pass
        if have == rows and self.matrix_mean_path.exists():
            return False
        def mean_of(v, axis):
            ok = np.isfinite(v)
            return np.where(ok, v, 0.0).sum(axis=axis), ok.sum(axis=axis)

        try:
            data = np.memmap(self.path / "matrix.dat", np.uint8, mode="r",
                             shape=(rows, n_band, n, n))
            for lo in range(have, rows, 64):             # ~8 MB a block at 128 elements
                t, c = mean_of(unquantize_unit(np.asarray(data[lo:min(rows, lo + 64)])), (0, 1))
                total, count = total + t, count + c
            last_total, last_count = mean_of(unquantize_unit(np.asarray(data[rows - 1])), 0)
            del data
            import io
            buf = io.BytesIO()
            np.savez(buf, total=total, count=count, rows=np.int64(rows))
            _atomic_write(acc, buf.getvalue())
            with np.errstate(invalid="ignore", divide="ignore"):
                image = np.where(count > 0, total / np.maximum(count, 1), np.nan)
                last = np.where(last_count > 0, last_total / np.maximum(last_count, 1), np.nan)
            upper = np.triu_indices(n, 1)
            image[upper] = last[upper]
            q = quantize_unit(image)
            # colour spent where the pairs are: equalized over both halves,
            # the diagonal (each element with itself) left out
            counts = np.bincount(q[~np.eye(n, dtype=bool)], minlength=256)
            with atomic_output(self.matrix_mean_path, fsync=True) as tmp:
                wfpng.create(tmp, q, wfpng.palette(wfpng.warp_from_counts(counts)), level=level)
        except (OSError, ValueError) as e:
            log.warning("%s: mean matrix not updated (%s)", self.path, e)
            return False
        return True

    # --- display --------------------------------------------------------

    def refresh_palettes(self) -> int:
        """Re-derive the equalization from the accumulated histogram.

        Constant time per image and it never touches a pixel, so the
        display tracks the whole acquisition even though the pixels were
        written days apart.
        """
        counts = self.palette_counts()
        done = 0
        for p in range(self.index["n_prod"]):
            if self.index["products"][p]["rows"] == 0:
                continue
            try:
                wfpng.set_palette(
                    self.image_path(p),
                    wfpng.palette(warp=wfpng.warp_from_counts(counts[p])))
                done += 1
            except (OSError, ValueError) as e:
                log.warning("%s: palette not updated (%s)", self.image_path(p), e)
        return done

    def update_thumbnails(self, level: int = wfpng.DEFAULT_LEVEL) -> int:
        """Bring every thumbnail up to date, appending where possible.

        Costs O(new files) in the normal case: the stride keeps existing
        scanlines valid, so only newly *complete* bins are appended.  A
        full rebuild happens only when the stride doubles, which is
        ~log2(N/256) times across an acquisition's life.

        Returns how many thumbnails were touched.
        """
        n_prod = self.index["n_prod"]
        bins = self.index["thumb_bins"]
        raw = self.path / "thumbs.dat"
        row_bytes = n_prod * bins
        n_src = int(self.index["thumbs_bytes"]) // row_bytes
        if not raw.exists() or n_src == 0:
            return 0

        stride = thumb_stride(n_src)
        complete = n_src // stride
        rebuilding = stride != self.index.get("thumb_stride")
        have = 0 if rebuilding else int(self.index.get("thumb_rows") or 0)
        if complete <= have and not rebuilding:
            return 0                                  # nothing new is complete

        data = np.memmap(raw, np.uint8, mode="r", shape=(n_src, n_prod, bins))
        lo, hi = have * stride, complete * stride     # only the sources we need
        counts = self.palette_counts()
        prods = self.index["products"]
        done = 0
        for p in range(n_prod):
            if prods[p]["rows"] == 0:
                continue
            rows = bin_fixed(np.asarray(data[lo:hi, p, :]), stride)
            if rows.shape[0] == 0:
                continue
            pal = wfpng.palette(warp=wfpng.warp_from_counts(counts[p]))
            try:
                if rebuilding or not prods[p].get("th"):
                    st = wfpng.create(self.thumb_path(p), rows, pal, level=level)
                else:
                    st = wfpng.append(self.thumb_path(p), rows,
                                      wfpng.WfState.from_dict(prods[p]["th"]),
                                      level=level)
            except (OSError, ValueError) as e:
                log.warning("%s: thumbnail not written (%s)", self.thumb_path(p), e)
                continue
            prods[p]["th"] = st.to_dict()
            done += 1
        self.index["thumb_stride"] = stride
        self.index["thumb_rows"] = complete
        self._save_index()
        return done

    def refresh_thumbnail_palettes(self) -> int:
        """Re-derive the thumbnails' equalization, as for the full images."""
        counts = self.palette_counts()
        done = 0
        for p, rec in enumerate(self.index["products"]):
            if not rec.get("th"):
                continue
            try:
                wfpng.set_palette(
                    self.thumb_path(p),
                    wfpng.palette(warp=wfpng.warp_from_counts(counts[p])))
                done += 1
            except (OSError, ValueError) as e:
                log.warning("%s: palette not updated (%s)", self.thumb_path(p), e)
        return done


def _npy_bytes(a: np.ndarray) -> bytes:
    import io
    buf = io.BytesIO()
    np.save(buf, a)
    return buf.getvalue()


def thumb_stride(n_src: int) -> int:
    """Source files per thumbnail scanline — always a power of two.

    The point of a fixed stride is that bin boundaries never move: bin
    *i* is always sources [i*stride, (i+1)*stride), so a scanline once
    written stays correct as the acquisition grows and new files only
    *append*.  Binning by ``linspace`` over the current length instead
    made every boundary shift on every append, which forced a full
    rebuild per run — O(acquisition length) work for one new file, and
    at 100 elements a 1.07 GB re-read every two minutes.

    The stride doubles when the row cap is passed, so a full rebuild
    happens ~log2(N/256) times over an acquisition rather than N times.
    """
    stride = 1
    while -(-n_src // stride) > THUMB_MAX_ROWS:
        stride *= 2
    return stride


def bin_fixed(block: np.ndarray, stride: int) -> np.ndarray:
    """Reduce whole groups of ``stride`` rows to one, keeping the minimum.

    Minimum for the same reason the frequency binning uses it: a
    thumbnail exists to show that something went wrong, and a mean would
    average a dropout back into its neighbours.  ``MISSING`` is excluded,
    and a bin with nothing valid stays ``MISSING``.  Any trailing partial
    group is dropped — it would change when the next file lands.
    """
    n = (block.shape[0] // stride) * stride
    if n == 0:
        return np.zeros((0,) + block.shape[1:], np.uint8)
    grouped = block[:n].reshape(n // stride, stride, *block.shape[1:])
    valid = grouped != wfpng.MISSING
    masked = np.where(valid, grouped, 255)
    return np.where(valid.any(axis=1), masked.min(axis=1),
                    wfpng.MISSING).astype(np.uint8)
