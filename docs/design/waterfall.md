# Visibility waterfalls

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Visibility waterfalls

``jobs/waterfall/`` renders the kotekan N² files under ``subset/`` as an
**upper triangle of PNGs per acquisition**, one image per correlation product,
surfaced through ``/files``.  The whole design rests on one measured fact:
kotekan writes into ``.partial/vis_<idx>.h5`` and *renames* the finished file
into the acquisition directory (caught live, twice, with the inode preserved
across the move), so any ``.h5`` sitting in an acq dir is complete and
immutable and the file list only ever grows.  Rendering is therefore **append-
only and never recomputed**: each new source file appends its 20 time samples
as *scanlines*.  That is why **rows are time and columns are frequency** —
frequency-major compresses ~10% better and cannot be appended to at all, while
appending costs only 2.5% (crosses) to 15% (autos) over an ideal monolithic
encode and still beats one file per source file.  ``wfpng.py`` does the append
by truncating a fixed 30-byte terminator, writing one new ``IDAT`` holding an
independent raw-deflate segment (``wbits=-15`` ended with ``Z_SYNC_FLUSH``, so
no encoder state has to survive between processes; the first row of a segment
uses Sub rather than Up for the same reason), patching the 4-byte height in
``IHDR``, and writing a fresh terminator carrying the combined adler32 —
verified pixel-exact by a strict independent decoder.  It seeks to the
*recorded* offset rather than to EOF, which is what makes it idempotent from
any committed state and removes the need for a repair pass.  ``reduce.py``
makes **one chunk-aligned sequential pass per file**: ``vis`` is chunked ``(16
freq, 16 prod, 20 time)``, so one product's column touches 385 scattered
chunks and reads at ~6 MB/s where a sequential sweep runs at ~100 MB/s — every
product together costs only ~1.9× one product, and looping products over the
file would cost ~17×.  Quantization happens *inside* that loop (the
intermediate is 260 MB as float32 against 65 MB as the uint8 indices actually
written) and validity comes from ``frames_added``, which is ``(n_freq,
n_time)`` at 0.1 MB and — measured against live frames — is exactly where |V|
is zero; ``vis_weight`` would say more but is 199 MB stored, a ~45% surcharge
on the I/O that dominates.  **Gains are deliberately not divided out**, so a
mid-acquisition gain update draws as a visible step rather than being
normalised away; the per-product value scale is frozen from the acquisition's
first file with usable data (2–99.5 percentile padded outward by half the observed log span)
because it is baked into every pixel and cannot be revised later.  What *can*
be revised is the display: **histogram equalization lives in the palette, not
the pixels**.  Equalizing the indices costs ~15% (it is maximum-entropy by
construction, flattening the histogram to 7.44 bits of 8) and, worse, goes
stale while the acquisition is still growing; carried in the 256-entry
``PLTE`` it is re-derived from an accumulated per-product **index histogram**
— the only statistic that can be maintained incrementally — and rewritten in
place in constant time, since the palette always has 256 entries and therefore
sits at a fixed offset.  Measured against true equalization: ΔE mean 0.7–1.5,
about the just-noticeable-difference threshold.  The colormap is **viridis**
(matplotlib), perceptually uniform with a dark-purple floor that keeps the
opaque-black **missing** entry (index 0) distinguishable from the lowest data
level.  (Earlier trees used batlow with a transparent-missing ``tRNS`` chunk —
``set_palette`` flips that alpha opaque in passing — then briefly magma;
``waterfall.py --repalette`` is the maintenance pass that recolours every
already-rendered acquisition — constant time per image, no pixel touched,
listed from the image tree so it works with the source mount gone; its
sibling ``--relabel`` re-derives the element labels the same way, see
below.)  Greyscale
was rejected as saving nothing (identical 1 byte/pixel; the palette costs 768
bytes and keeps the colour) and 16-bit PNG as a trap (92 KB against 16 KB —
the low bits are noise).  Deflate level is **6**: level 9 runs at 5.9 MB/s
against 20.3 MB/s for 0.58% fewer bytes, which is the difference between a 12
s and a 3.3 s write per source file.  ``store.py`` owns the commit discipline:
``index.json`` records the byte length of the last completed write for every
append-only file (the images, ``thumbs.dat``, ``times.bin``) and is written
*after* all of them, so a run that died partway leaves files that are too
long, never too short, and each append truncates back to the recorded offset
and redoes its work.  One invariant is easy to get wrong and is stated in the
code: the recorded ``thumb_bins`` must be the *effective* bin count, since
``thumb_rows`` clamps to the band width and a mismatch reshapes ``thumbs.dat``
wrong.  The tree is a **sibling of the data, not a subdirectory of each
acquisition** (``kotekan_vis_files/waterfalls/<root>/<acq>/``) because the
acquisition directories are owned by the NFS-squashed writer and are not
writable; it holds only relative paths so it can be moved wholesale when the
data goes to long-term storage.  Element indices are zero-padded to four
digits and **sharded by the first input**, so a lexical sort is a numeric sort
and no directory holds more entries than there are elements — the full
triangle is O(N²) in both bytes and files (32 elements = 37 MB and 1,058 files
per acquisition; 100 elements = 358 MB per source file and ~157 GB per
observing day), which is why ``waterfall.py`` visits acquisitions **newest
first** under a per-run budget: a full backfill is ~24 h and must never starve
the live acquisition.  On choco's side ``waterfalls.py`` is **read-only** and
hub-safe, and finds the tree where the job says it writes it: the
``waterfalls_dir`` it records in its ``state.json`` every run (2026-10; until
then config.yaml carried a second copy, ``waterfall.images_dir``, that had to
be kept in step and is now refused).  The summary sweep, each index read and every open and chunk of a
streamed image go through ``datafiles.run_blocking`` (threadpool, timeout),
for the same reason ``datafiles.py`` does, and the reads are
cached on a TTL and on mtime respectively.  Every path component from the URL
is matched against the pattern the writer uses before it is joined
(``NAME_RE`` / ``SHARD_RE`` / ``IMAGE_RE``), the same never-pass-the-
caller's-string rule as the journalctl allowlist.  ``/files`` gains a
**Waterfalls** column (files folded / files present) linking to the contact
sheet at ``/files/<root>/<acq>/triangle``, whose cells are lazy-loaded
thumbnails built from a tiny accumulator (``thumbs.dat``, one 128-bin row per
product per source file).  A thumbnail is capped at 256 scanlines, so its time
binning uses a **power-of-two stride** rather than a fit-to-length
``linspace``: bin *i* is always sources [i·stride, (i+1)·stride), which means
a scanline once written stays correct and new files only *append*.  Binning to
the current length instead moved every boundary on every append and forced a
full rebuild per run — O(acquisition length) for one new file, measured at
**27.8 s per run** for a 1658-file acquisition and ~267 s at 100 elements,
against a 120 s timer.  With the fixed stride a full rebuild happens only when
the stride doubles (~log2(N/256) times per acquisition, 0.5 s / 5.3 s at those
two scales) and the steady state is nothing at all until a bin completes, then
one appended row per product (0.66 s for 5050 thumbnails).  The axes carry the
**element names** from ``index_map/label`` rather than indices, column headers
rotated so any label length fits a 1/33-wide column.  Those names come from
kotekan's per-element label table (chord.2021.10+988, acquisitions from
2026-09-11 on): ``index_map/label`` has one entry per element of *this
file's* axis — a ``subset/`` file is a ``DishInputs`` frame over the
populated dishes only (16 dishes + 8 RFI antennas × 2 pol = 48 elements) and
carries exactly 48 labels.  Two spellings exist: dish label + ``p1``/``p2``
(+988 up to kotekan PR #1695) and dish label + ``X``/``Y`` (PR #1695,
2026-09-22, which also put the dishes in physical order and renamed them
``A01``; in the subset data from 2026-10-01), and
``dishlabels.file_element_labels`` maps either to choco's ``B4X``/``B4Y``
after checking the count against ``num_elements`` and the suffix against
``index_map/pol``.  The ``X``/``Y`` text is also what the pre-2026-08
per-element files carried, with the wrong element order, so that spelling
is taken only when ``index_map/pol`` (written since +988) is there to vouch
for it.  Earlier layouts are not read: a per-dish table needing expansion, a
whole-telescope table needing the ``input_list`` lookup (the 2026-09-09
subset files, whose ``label[compact]`` named the Y block as the next row of
unpopulated dishes), pre-2026-08 ``A1X`` labels without ``pol`` — anything
that is not the current layout is stored as no labels — the viewer shows
indices — because a wrong name on an axis is worse than none.  The first
acquisitions under PR #1695 (2026-10-01 to 10-03) were rendered before the
``X``/``Y`` spelling was read and sit with no labels until ``--relabel``
rewrites them.  Labels are written once,
at ``store.start``, so ``waterfall.py --relabel`` exists to push a better
reading to finished acquisitions: one source file per acquisition is re-read
and only ``index.json``'s ``labels`` rewritten.  A re-read that resolves
nothing never replaces consistent stored labels (acquisitions labelled under
an earlier layout keep their names); an acquisition whose sources are gone
has its labels cleared if they do not cover its element axis one-to-one
(that mismatch *is* the old bug) and is otherwise left alone.  A cell links to the
**full-image viewer** (``/waterfall/<root>/<acq>/view/<name>``): the PNG is
append-only data, so axes are drawn *around* it at display time, never into
its pixels — a **sticky frequency ruler** on top (the image is one pixel row
per time sample and can be tens of thousands of pixels tall, so the axis
travels with the scroll), time tick labels down the left from ``times.bin``,
shown in **site-local time** (``waterfall.timezone``, default
America/Vancouver, the page naming the abbreviation in force — PDT/PST; DUT1
<0.9 s is ignored at second resolution) — the stored values are kotekan's
``time_center_ut1_ns``, **ns since J2000** (2000-01-01 12:00 UTC), *not* unix
(a ~30-year error if confused; verified against a live file whose name stamps
the UTC), with 0 padding a pre-sync scanline that therefore gets no tick
rather than a fabricated epoch label, all three conventions matching
``~/pathfinder_tools`` — and a |V| colorbar built from the palette read out of
the PNG at its fixed offset (a sub-kilobyte read, taken fresh so a palette
refresh is never mislabelled) with the frozen per-product lo/hi placing the
decade ticks.  Ticks sit at evenly spread pixels labelled with the value
actually recorded there, not at round values interpolated onto the axis,
because neither axis is guaranteed uniform (a subset acquisition's channels
need not be contiguous; a skipped file leaves a time gap) and a label read
from the data cannot lie.  ``freq.npy``, ``times.bin`` and the PLTE are parsed
with the **stdlib** (the web process still never imports numpy), each read
threadpooled and mtime-cached like the index, and each degrading to a plainer
page — channel/scanline numbers, no colorbar — never a 500.  Three geometry
rules came from a live squat image (6145×780 renders ~150 px tall at page
width): the time rail is **anchored to the image box**, never the flex row —
the row is as tall as its tallest member, the ~300 px colorbar column, which
stretched the ticks over twice the image; tick fractions use the **served
PNG's own IHDR height** (read in the same sub-KB head fetch as the palette),
because a live acquisition's image can be an append ahead of the index's
committed rows and ``times.bin`` one behind the image — a row past the end of
times gets no tick, transiently, rather than a fabricated label; and the time
tick count scales with rows/width, since the image renders at page width and
its on-screen height is set by its aspect.  The renderer's health is **half
of the DATA badge** (since 2026-10-05; at first a WF badge of its own, on the
argument that DATA answers "can I see the mounts" and WF "is the renderer
keeping up"): because the job reads the same mounts a failure there exits 2,
and the joint tag keeps that reading in one place — ``down`` is the mounts,
``waterfall degraded`` the renderer behind healthy mounts, ``waterfall
failed`` a renderer bug — with its status, state file and journal on the
files page below the scan (ui.md, the service-strip bullet).  Four things
exist because they were missing the first time and an adversarial review found
them: the run holds an **exclusive ``flock``** (systemd will not stack the
oneshot, but a manual run racing the timer would interleave two processes'
deflate segments into one image, and the recovery discipline replays a
*sequential* history, not an interleaved one); what is pending is decided from
**filenames** and only the files a run will actually render are opened
(kotekan's ``vis_<idx>_`` name carries the index, and opening every pending
file merely to size the backlog measured **454 s against the 10,864-file
archive**, which a 2-minute timer cannot absorb — 8.1 s after); a file that
can *never* be rendered (no ``abs_file_idx``, an empty frame, a shape that
cannot join the acquisition, no usable data) is recorded once in ``index["skipped"]`` rather
than re-reported, because exit 2 means "retries self-heal" and these do not.
Before an acquisition has an index (no file has set its scale yet) the skips
go to ``skipped.json`` and ``start`` adopts them; at most ``SCALE_TRIES`` (3)
files are sampled per run for the scale.  Without this, an empty first file
(common: the first file of a run is often partial) was re-read on every run
and the acquisition never started (2026-10-09);
and a mid-acquisition ``OSError`` **returns rather than raises**, so the files
already committed still count against ``max_files_per_run`` and a flaky mount
cannot let one run render several times the cap.  A small **``summary.json``**
sits beside the index for choco's sweep to read: the index carries one record
per product (~700 KB at 100 elements), and parsing all of them every 30 s
would pull ~23 MB off NFS to count files.  ``index.json`` also records the
acquisition's **``source_path``**, so ``/files`` matches a scan root to its
images exactly instead of guessing from directory basenames, which two roots
could share.  The unit carries **``MemoryHigh=2G``**, which is about page
cache rather than the job: a source file is read once and never revisited, so
the gigabytes a run streams off NFS are cache that can never be hit again —
the first backfill grew it at ~9 MB/s until it had claimed most of a 16 GB box
(each finished run's cgroup reparenting its charge into ``system.slice``) and
pushed 194 MB of idle daemons to swap at ``vm.swappiness=60``, while the job's
own footprint sat flat at ~150 MB anon throughout.  ``MemoryHigh`` reclaims
within the cgroup, but it only *throttles*, so it is not a backstop: clean
page cache reclaims for free while a genuine anon runaway would stall ever
harder in reclaim and still end at the **system** OOM killer, which might pick
choco or a login shell rather than the leaker.  ``MemoryMax=3G`` therefore
sits above it as the hard limit, giving the cgroup its own OOM killer; it
cannot fire on the memmap below, since clean file pages are always reclaimable
and only unreclaimable anon reaches the limit.  Being killed mid-append is
safe by construction and was fuzzed that way — every append writes at the
offset ``index.json`` records and truncates first, the index is written
atomically and last, and the ``flock`` is an fd the kernel drops on death —
and a killed run reports ``Result=oom-kill``, which ``job_status`` renders
red.  The 2G tracks ``thumbs.dat`` and not the job: ``update_thumbnails``
memmaps it and walks one product at a time, so consecutive reads sit
``n_prod*bins`` apart (646 kB at 100 elements) and the pass only stays
sequential-ish because the whole file is cached — 107 MB at 32 elements but 1
GB at 100, so a tighter cap would turn a stride-doubling rebuild into
``n_prod`` re-reads of the whole file.  The unit also caps CPU —
``CPUQuota=200%`` and ``CPUWeight=20`` — so a backfill cannot crowd out the
other services sharing this host or a developer's shell.  The quota is a
ceiling rather than a throttle (the work is single-threaded and I/O bound and
does not come near two cores; what it buys is numpy's unused OpenBLAS pool
spinning, or a future run that parallelises), while ``CPUWeight`` is what
bites day to day: between cgroups the scheduler distributes by *cgroup*
weight, so the unit's ``Nice=10`` orders tasks only within the unit and does
nothing for a sibling cgroup such as ``user.slice``.  On choco's side the
parsed-index cache is **LRU-bounded** (``INDEX_CACHE_MAX``) for the mirror-
image reason: the store lives as long as the process, and one index is ~700 kB
of JSON at 100 elements.

**A selection per acquisition (2026-10-17).**  When the subset grew to all
64 dishes the files became 12 GB per 400 s — 128 elements, 8256 products,
40 bins — and ``vis`` chunked ``(16, 8256, 40)``, 42 MB a chunk.  The
renderer's one pass over every chunk was 16 GB a file over a link that
delivers 36–59 MB/s, its ``(n_prod, n_time, n_freq)`` buffer 2 GB and the
scale sample another gigabyte, so the first run sat for half an hour at
the unit's 3 GB ceiling, 91 % stalled in memory reclaim, and would have
needed more than the cadence even unthrottled: 100 s of deflate for 2 GB
of pixels and 8256 appends over NFS per file.  Two things changed, both
recorded in the index so an acquisition keeps them for life.  ``products``
names what a new acquisition renders (``reduce.select``): the autos, every
same-polarization cross with the reference elements (A01X, A01Y: each
dish's fringe against one reference), named pairs — 254 images where
there were 8256 — recorded by their inputs so later files join by name.
``freq_stride`` keeps one chunk of channels in N (``reduce.strided_channels``,
recorded as ``freq_idx``): a chunk is the unit of cost, so one in four is a
quarter of the read, and every column is still a real channel, which is
why the stride is in chunks and not an average.  A file is matched to a
started acquisition by its shape (``n_prod_file`` / ``n_freq_file``) and
then by product inputs and channel indices (``reduce.selection_from_index``);
an index from before carries every product and channel and reads as such.
An index with nothing folded in yet may be started again under the
current config, since no pixel depends on it — the first 128-element
acquisition had been indexed with every product before any file landed.
One file is now 96 chunks, about 2 GB on the wire and under a minute, in
a 16 MB buffer.

**The overview: every product from one pass (2026-10-17).**  The selection
kept the renderer alive but lost the triangle, and the triangle is what
shows a dead board or a swapped cable at a glance.  Two facts set the
design.  A chunk holds every product, so once the pass opens it for the
254 selected products the other 8002 are in memory for nothing; and the
X-engine streams are a comb (channel index mod 16, checked on the live
file: a down stream is one dead channel in every chunk), so a chunk sees
every node, and reading every chunk is the only way to see every channel.
The pass therefore reads every chunk of every ``file_stride``-th file —
171 s plus 21 s for the scale on a live file, 0.5 GB, inside the 400 s
cadence beside the mapmaker's minute — and keeps three things per file
beside the images (``reduce.OverviewSpec``, ``FileOverview``): the mean
|V| of every product over each chunk (``coarse.dat``, 3.2 MB a file), each
element's mean over its partners of |V_ab| / sqrt(V_aa V_bb) per chunk
(``co_eNNNN.png``, one scanline a file, so the sheet shows 128 of them
without a seek per row), and that coherence per band as an element ×
element matrix (``matrix.dat``, 128 KB a file).  Coherence is an
amplitude mean, not a coherent average: the fringe of a 60 m baseline
turns several times in 400 s, and a health view wants the correlated
power whether or not it is fringe-stopped.  On the live files its noise
floor is ~1e-3 and the sky on a short baseline ~0.1, so the cubes are
quantized on a log scale from 1e-3 to 1 (``reduce.quantize_unit``), where
a linear one spent a level on the floor.  The web renders a slice of a
cube into a PNG on request with the stdlib (``waterfalls.encode_png``;
the job's ``palette.bin`` supplies the colours) — a product's rows are
one seek per file, a matrix frame one read — so the triangle is now a
128 × 128 image with a band selector and a frame slider, and a click on
a cell opens that product's coarse waterfall; the 8256-thumbnail page
is gone.  Time is thinned only in whole files (``file_stride``), since a
chunk spans every bin of its file.  The structural fix stays on the
recv: a second N² writer behind a 16-channel average would hold every
product at this resolution in 0.75 GB per file, and the same pass would
read it in 15 s.

**The feeds panel (2026-10-23).**  The contact sheet showed the data but
not the flags, and the question the page has to answer is which feeds
are bad and whether the flags agree.  So the sheet now opens with one row
per feed (dish rows A–H, X above Y), one column per file, and the value
is measured without regard to the mask: each feed's median coherence
with the feeds of its own polarization in one band, computed by the web
from ``matrix.dat`` (one 16 KB frame per file; the levels are a log
scale, so the median of the levels is the level of the median).  The
median and the same-polarization partners are the point: the mean over
all 127 partners in ``co_eNNNN.png`` is diluted by the cross-polar pairs
and dragged down by dead partners, and it works on acquisitions rendered
before the change.  The masks are drawn over the rows, so a masked feed
keeps its data: a bright hatched row is a stale flag, a dark row with no
mask a miss.  The masks come from the file, not from bffs: ``flags`` is
what kotekan applied, and it agreed with ``bad_feed_mask`` exactly on
the 2026-10-23 files, while bffs's ``history`` records what it sent and
when, and its ``flag_reasons`` only for the current list (the page
shows those as "bffs now").  ``flags`` is per channel, and channel i is
stream i mod 16, so one chunk gives every stream's view of every feed;
the record counts the streams that masked a feed among those that
delivered, and a partial count is drawn as its own hatch.  The first
live record showed why that matters: bffs flagged B01X at 15:15:35 and
cleared it at 15:22:07 UTC, but the streams applied it between 15:15:44
and 15:20:03 and dropped it between 15:22:33 and 15:25:12.  The RFI and
packet-loss masks are per sample and averaged over feeds, so the file
keeps only their fractions per channel (``frac_rfi``, ``frac_pl``,
``frac_rfi_first_stage_enabled``); ``frac_lost`` is their sum and adds
nothing.  They and the per-stream missing channels (``frames_added``)
run under the rows on the same time axis.  The job writes all of it as
one JSON line per file (``diag.jsonl``, ~10 KB, read in 0.1 s), appended
under the index's commit like the cubes; acquisitions folded before the
change have no record and show their data without masks.

**The sheet as a dish grid (2026-10-24).**  The feeds panel above
answered the question in the wrong form: what the page needed was the
waterfalls themselves, arranged as the array is.  It now leads with the
array's coherence (``co_array.png``: per chunk and file, the median over
the elements of their coherence images, written by the job beside them
and rebuilt from them when behind), then each feed's coherence image and
autocorrelation thumbnail on the 8 × 8 dish grid, X beside Y.  The two
grids share one place on the page, with a tab for each (the choice is
remembered per browser); nothing else is switched: no band selector,
and the band is always whole.  The
masks moved to the large viewer as columns beside the image, row for
row (a bin per row for a full waterfall, matched by time; the share of
the file's bins for a per-file image), with the RFI mask's on state and
the lost channels.  Two per-dish readings were tried on the 2026-10-23
files.  The autocorrelation over its polarization's median was kept,
but the F-engine's digital gains bring a dead feed to the level of the
rest (5.6–6.7 for D04, B02, E07 against ~90 for the hot C06Y and
C08Y), so it finds hot feeds, not dead ones; the coherence grid shows
those.  A cross- over same-polar coherence for X/Y swaps was left out:
the band-mean amplitudes in ``matrix.dat`` carry RFI common to both
polarizations, cross-polar coherence came out at 0.9 of same-polar on
normal feeds, and even over neighbouring dishes only, the swapped C05,
D07 and E05 (1.15–1.25) ranked among E01, G01 and G02.  A swap test
needs fringe phase.  The stream table names each stream's node from the
nodes' own ``/metrics`` when a file has just landed (the files carry no
host: the sender configs are identical), and counts the bins in which a
stream masked a different number of feeds than most: on the same files
streams 1, 3, 9 and 11 (two nodes) were out of step for the B01X
episode above.
