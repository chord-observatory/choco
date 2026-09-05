# Pipeline page, frame peeks and live buffer plots

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Pipeline page

``/pipeline/<key>`` is a node's live view, and the only one: the dashboard and
the edit page link straight to it.  (There was a ``/status/<key>`` page
carrying an inert base64-``<img>`` graph, a buffers table and a metadata Peek
button; the pipeline page covers all three — the graph labels carry more per
buffer than the table did, and the plot overlay has its own metadata button
hitting the same ``len=0`` peek — so it was removed rather than maintained in
parallel.)  The graph is fetched on demand, never on a timed poll: kotekan's
``/pipeline_dot`` walks the full buffer/stage graph, and each fetch costs a
``dot`` subprocess here — but the labels embed live fullness (``3/24 (12.5%)``
= full/total frames), so a refresh is a meaningful new snapshot, not a re-
render.  ``Node.get_pipeline_dot`` is a plain GET (a read — allowed in
maintenance mode); ``services.render_dot_svg`` shells out to the graphviz CLI
(``dot -Tsvg``, the same host-tool-over-Python-dependency choice as the
openssl TLS fallback; ``choco.sh install`` prints a note when ``dot`` is
missing) and slices the output down to the ``<svg`` element since an XML
prolog is invalid mid-HTML.  kotekan **does** ship layout hints now
(``rankdir``/``nodesep``/``ranksep``/``mclimit``/``newrank``, tuned for the
tall multi-line node labels it emits) and a ``-G`` on the command line
silently **overrides** the graph's own value, so everything in
``_DOT_LAYOUT_ARGS`` is a deliberate override: splines plus wider rank/node
separation and ``-Gmclimit=8 -Gnewrank=true`` for crossing minimisation.
Layout args are graphviz's fussiest paths (ortho especially), so a failed
render retries once without them before giving up — which is exactly why the
``ortho`` preset's widened ``nodesep`` *looked* fine while **aborting graphviz
2.43** (``chkSgraph: Assertion np->cells[0] failed`` in the ortho maze router)
on a clustered CHORD graph that renders fine at kotekan's own 0.3: the
operator picked ortho and silently got curves after a wasted render.  0.4
survived and 0.5 did not, so the threshold is graph-dependent and ortho now
leaves ``nodesep`` to kotekan.  The ``dot`` pipe sets ``encoding="utf-8"``
explicitly rather than trusting the locale: kotekan's layout lines carry ``×``
and ``·``, and a C-locale interpreter would raise ``UnicodeEncodeError``,
which is not a ``SubprocessError`` and would 500 the route.
``get_pipeline_dot`` forces ``resp.encoding = "utf-8"`` for the same reason on
the way in — kotekan builds before the charset fix label the reply
``text/vnd.graphviz`` with no charset, which HTTP defines as ISO-8859-1 and
``requests`` believes — and asks for ``urls=0``, since kotekan's
``/buffer_frame?name=…`` links are relative to the *node* (they resolve
against choco and 404) and would fight the inline view's own click-to-plot
handler.  The SVG is inlined so buffer nodes can be clicked, which means
kotekan-supplied markup lands in the authenticated UI:
``services.sanitize_pipeline_svg`` rebuilds the graphviz output through a
whitelist RECONSTRUCTION (a new tree; only known-inert elements/attributes are
copied, so script/handlers/foreignObject can't survive by omission).  An
unlisted element is **unwrapped, not deleted** — left out of the output while
its children are rebuilt in its place.  Deny-by-default is exactly as strict
(the output can still only contain listed elements carrying listed attributes,
and unwrapping never carries the element's own *text*, which is what keeps a
``<script>`` body out), but the failure mode stops being silent data loss:
graphviz wraps a node's shape and text in ``<a>`` the moment it carries a URL
*or* a tooltip, kotekan sets a tooltip on every buffer and every stage, and
deleting the subtree rendered **111 of 223 nodes on cx19 as empty groups** —
present in the DOM, still clickable, nothing drawn, while the raw dot text and
any ``<img>`` render of it looked fine, which is what hid the bug for as long
as it did.  Whichever element graphviz emits next, the worst this can now cost
is that element's styling; ``logger.info`` names whatever was unwrapped.  The
rebuild also stamps ``data-plot-buffer``/``data-plot-node`` (plus a persistent
amber outline via the ``clickable-buffer`` class — ID-anchored CSS so it
outranks the dark-mode recolors — and
``tabindex="0"``/``role="button"``/``aria-label`` so the nodes are reachable
by keyboard, pipeline.js turning Enter/Space into a synthetic click since SVG
elements have no ``.click()``) on ``<g class="node">`` groups whose
``<title>`` matches a ``peek_hold`` buffer — bufferplot.js's existing
delegated click handler opens the plot with zero JS changes.  Buffer nodes are
drawn unfilled, so ``pointer-events: all`` on the stamped shapes makes the
whole interior clickable rather than just the outline and label (with an amber
hover tint showing the target area).  Nothing-to-click is stated rather than
implied: the partial distinguishes a failed ``/buffers`` read (``buffers_ok``)
from a pipeline with no ``peek_hold`` buffers, since the page otherwise
promises amber buffers that were never marked.  The page (``pipeline.html`` +
``static/pipeline.js``) is a **standalone** template, not a base.html child —
the graph needs every pixel, so a slim header (CHOCO/edit links + toolbar)
replaces the usual chrome: graph pane with drag-to-pan (the click ending a
drag is swallowed in capture phase so panning over a buffer doesn't open it),
Fit/1:1 modes plus **scroll-wheel zoom around the cursor** (the graph *is* the
page here, so a plain wheel zooms and drag pans; shift+wheel stays native as
an escape hatch, and a trackpad pinch arrives as ctrl+wheel so it works for
free).  Zooming sets an explicit px width on the svg and leaves fit/full mode
— Fit/1:1 clear it — and the width is **re-applied on ``htmx:afterSwap``**,
since a Refresh or layout change delivers a fresh SVG with no inline style and
would otherwise snap a zoomed-in operator to the ~9700 px natural width.  A
**layout preset selector** (``?layout=`` → ``services.PIPELINE_LAYOUTS``
allowlist: **curves is the default** — mclimit/newrank tamed the spline
spaghetti that originally motivated ortho, and curves lays out ~10% more
compact; ortho / polyline stay as presets; all carry ``-Gmclimit=8
-Gnewrank=true`` which measured ~11% narrower output = fewer crossings; never
pass raw values to the dot command line) rounds out the toolbar; its options
are generated *from* the allowlist so the two can't drift, the page's own
``?layout=`` preselects one (bookmarkable, survives a refresh, kept in step by
``history.replaceState``), and the graph's initial fetch carries ``hx-
include="#pg-layout"`` because browsers restore a select's value across a
reload and would otherwise show "ortho" over a curves render.  The live plot
opens as a fixed **popup overlay** bottom-right (hidden via ``:has()`` until a
panel opens) so the graph keeps the full viewport; its ⤢ link opens the same
plot on ``/plot/<key>?buffer=`` with the current view in the fragment.  The
page is **dark by default** (toggle in the header, persisted; applied in
``<head>`` before first paint): because the SVG is inline DOM, graphviz's
presentation attributes are recolored with plain CSS attribute selectors — no
SVG rewriting involved — under one rule: *fills go dark keeping the hue,
outlines brighten keeping the hue*, so each node type stays recognisable in
either theme.  The stylesheet is a **mirror of kotekan's palette**
(``lib/core/PipelineGraph.cpp``: ``graph_style()`` plus the ``graph_ink`` /
``graph_edge_line`` / ``graph_cluster_line`` / ``graph_device_*`` /
``graph_full_line`` constants) with one row per emitted value — buffer blue,
CPU-stage amber, GPU orange, I/O violet, device-memory magenta, endpoint teal,
the device region's warm wash, the section-box outline, the ink, the edge
colour, and the red of a backed-up buffer.  **Both palettes are covered**, in
two blocks: kotekan rolls out to the cluster on its own schedule, so choco
meets a mixed fleet until it has and cannot assume either version — the older
values (endpoints sharing the buffer blue, the ``#f0f0f0`` device slab, black
edges) sit in their own block, deletable once no node serves them.  Nothing
links the two repos, so **``tests/test_pipeline_palette.py``** renders *both*
fixtures (``tests/data/pipeline_palette.dot`` and ``…_legacy.dot``, hand-kept
samples exercising every colour and shape each kotekan emits) and fails naming
any fill or stroke the dark theme doesn't map; that is the guard, because the
first version of this stylesheet was written against a palette kotekan had
already replaced and every node silently kept its light paint on the dark
page.  It earns its keep: it caught that ``[stroke="gray70"]`` matches
nothing, since graphviz resolves its own ``grayNN`` scale to hex
(``#b3b3b3``/``#999999``) on the way out while leaving SVG colour names like
``lightgrey`` alone.  Two things are *not* mirrored: the white canvas polygon,
which graphviz paints itself and which is scoped to ``svg > g.graph >
polygon`` and turned transparent, and **label text, which is selected by
element** (``svg text { fill: … }``) rather than by colour — the older kotekan
set no ``fontcolor``, so its labels carry no fill attribute for any selector
to match, and a CSS property outranks a presentation attribute either way.
The ``clickable-buffer`` rules match ``path`` alongside
``polygon``/``ellipse``: kotekan draws buffers as *rounded* boxes, which
graphviz emits as ``<path>``, so the polygon-only selectors marked nothing at
all.

## Frame peeks and the buffer-data API

buffer inspection now hangs off the pipeline page's plot overlay rather than a
table.  ``/api/node-buffer-data/<key>?buffer=<name>`` is what
``bufferplot.js`` calls: ``len=0`` returns kotekan's JSON reply as-is (frame
descriptor, metadata, frame id — this is also what the overlay's metadata
button shows), ``len>0`` returns the newest frame's leading bytes as raw
``application/octet-stream`` with the frame id in ``X-Frame-Id``.  The buffer
name rides a query parameter (avoids path-converter ambiguity after
``<path:node_key>``) and is validated against ``_BUFFER_NAME_RE`` before
entering the kotekan URL — same never-pass-raw rule as the journalctl
allowlist.  Only ``peek_hold`` buffers are marked clickable on the graph: a
fast-draining buffer without the hold answers "no full frame" almost every
time, and an affordance that mostly errors is noise (the API itself stays open
to any frame buffer; enabling ``peek_hold`` on a buffer's config block is what
makes it clickable).  ``peek_hold`` is a kotekan-side buffer option added
alongside this: recycling of the newest full frame is deferred until the next
one lands — zero-copy — so a fast-draining buffer always has a frame to peek;
it also makes an idle buffer report one full frame, which is the hold, not
backlog.  The kotekan endpoint is ``GET /buffer_frame?name=<buffer>&len=<n>``
— kotekan folded the per-buffer ``/buffer/<name>/frame`` endpoints into this
single query-param form (same reply JSON), and choco speaks **only the new
form**: a node still on a pre-``/buffer_frame`` kotekan 404s every peek and
reads as the no-such-buffer error below until it migrates.  A peek miss is
**not** an outage: kotekan replies HTTP 402 when no full frame is in the
buffer, so ``Node._request`` grew ``accept_statuses`` (and a
``requests.RequestException`` catch-all so rare transport failures degrade
instead of 500ing) and ``get_buffer_frame`` returns ``{"error": ...}`` for it;
buffer reads take **one quick retry** on transport failure, the same rule as
the service monitors, reserving ``None`` for unreachable.  Kotekan 404s are
meaningful replies too: an *idle* kotekan (process up, no pipeline) registers
neither ``/buffers`` nor ``/buffer_frame``, and a running one 404s an unknown
``name``, so ``get_buffers`` maps 404 to ``{}`` and ``get_buffer_frame`` to a
distinct no-such-buffer error — without this every idle node reads as "kotekan
unreachable" while its process is demonstrably up.  Kotekan **500s** are in
the same family (accepted, not retried into a ``None``): a buffer whose
producer attaches a metadata object without populating it (the dpdk-fed
``packet_receipt_bitmap_buffer_*`` on cx19) makes ``chordMetadata::to_json``
read uninitialised dims and throw — a kotekan-side defect about *that* buffer,
so it reads as "could not serialise a frame of '<name>'" rather than sending
an operator after a network problem that isn't there.  All of this is read-
only — allowed in maintenance mode.

## Live buffer plots

each frame buffer's **Plot** button opens a live canvas panel (``#buffer-
plot``, outside the polled table like ``#buffer-peek``) driven by
``static/bufferplot.js`` (vendored, dependency-free — server-side matplotlib
would drag the scientific stack into the gevent process, and a subprocess per
render is too slow at poll cadence; canvas costs choco only a proxied GET and
scales with viewers).  The data rides ``/api/node-buffer-
data/<key>?buffer=&len=``: ``len=0`` passes kotekan's descriptor JSON through
(fetched once per plot), ``len>0`` (default 4 MiB, clamped to 32 MiB server-
side) returns the newest frame's leading bytes decoded from kotekan's base64
to raw ``application/octet-stream`` with the frame id in ``X-Frame-Id``.  The
**prefix contract** is the key trick: every pathfinder buffer is C-order with
time leading, so a byte prefix truncates only the leading dimension — the JS
recomputes that extent from the bytes received (``shapeReceived``) and keeps
all inner structure intact, which is how a 402 MB voltage frame is plottable
from a 256 KiB fetch with no kotekan subsampling stage.  The descriptor drives
everything client-side: a decoder table maps ``value_type`` to typed arrays
(plus nibble-unpack for ``int4x2`` — two offset-encoded values per byte,
appended as an extra ``re/im`` dim — and bit-unpack for ``uint1x`` types),
``extents``/``dimnames`` feed an N-d→2-d reduction in two stages — **fold**
each dimension by its own op, then **compose** the survivors onto the axes
(odometer walk, viridis, 2–98 percentile color range) — and auto mode reads
the result off the axis count (packed low-bit types included: the default
**max** makes voltage read as a saturation map, where mean would wash outliers
out), falling back to histogram when there are no extents at all.  **Every
dimension carries a disposition and an index selection**, set in one ``dims``
dropdown holding a row per dimension: which of its indices are in play (a
range box — ``0``, ``0,3``, ``1-4``, blank for all) and what happens to them
(on the x axis, on the y/series axis, or folded away by ``max | mean | min``).
The two are deliberately separate questions, which is what lets a restricted
dimension still be *several* lines: pinning to one index is only the one-entry
case, and there is consequently no "at index" disposition — a one-entry
selection reaches the fold as an extent-1 dimension, where every op is the
identity, so it costs no special case at all.  Selections apply *before* the
folds (``filterDims``, a gather along one axis that keeps the input's typed-
array kind since nothing has been arithmetic yet), so everything downstream
just sees a smaller array; ``effExtent`` is what the axes count in and
``origIndex`` is what a series label reports, so a line drawn from ``E@0,3,7``
is labelled ``E=3``, not ``E=1``.  A selection that parses to nothing means
the whole dimension, never an empty one — a zero-extent axis has no plot to
draw.  The summary chip spells the whole thing out (``F→x, E→series@0+3, T
mean``), and because the table always assigns every dimension there is no
"still 3-d" state to resolve — **what is drawn follows from how many axes are
left occupied**: two is the one genuinely ambiguous case (a grid of cells or a
family of curves), so that is the only choice ``mode`` still makes; one axis
is a line plot; none is a scalar readout, which is a legitimate answer ("mean
over the whole frame") rather than an error.  ``histogram`` overrides all of
it, being a distribution over values rather than over axes — it bins the
*folded* values, so the table means the same thing there as everywhere else.
**Axes are still composed**: several dimensions can share one, so ``x =
DPhi×DPlo1×DPlo2`` tiles all 9216 dish-pair combinations along one axis
instead of averaging them away (earlier-listed dimension varies slowest,
matching a C-order reshape — the disposition list is the single source of
truth and the axis lists are derived from it *in dimension order*, which is
what makes that true).  Per-dimension folds are what split the two reductions
that used to share one control: **what happens to a dimension is per
dimension** (mean over time is the integration), **what happens to the entries
of an axis sharing a canvas pixel is per plot** — that is ``pixels: max | mean
| min``, formerly ``combine``.  Splitting them also made the pixel rule honest
for heatmaps: the grid used to be handed to the canvas at full size and the
browser's downscaler dropped whole columns, so a narrow spike in a 6550-wide
heatmap vanished at the moment it mattered; ``collapseGrid`` now applies the
same rule the line renderer does.  Folds are applied **outermost first** and
ops of different kinds don't commute (max over T then mean over F is not the
reverse), so the order is fixed and stated in the dropdown rather than left to
look arbitrary; it only shows when two dimensions fold differently, and an
index pick is order-free either way.  Index selection is a range box, not an
option per index — the enumerated version was unusable at 9216 entries and
read as something that *shortened* the dimension rather than an operation on
it; as a box it is honest about shortening the dimension, because that is
exactly what it does.  The dimensions need not be adjacent — ``reduceToGrid``
carries per-dimension multipliers and updates the cell offsets incrementally
as the odometer ticks, so it walks strides rather than memory order at O(1)
per element; the only cost of a non-adjacent composite is interleaving, which
the axis label (``P×D``) makes visible.  Folding across unlike quantities is
the trap the per-dimension ops exist to avoid: ``C=2`` is re/im and ``SK=3``
is three different statistics (measured on a live correlation frame, ``C=0``
real ∈ [−31067, 152741] against ``C=1`` imag ∈ [±24645] — a meaningless blend
when folded together), so those dimensions want ``at 0`` or an axis of their
own, never ``mean``.  An earlier per-dimension control (``reduce | 0 | 1 |
…``) was removed for naming the operation after the dimension and enumerating
one option per extent — a dimension past the 64-entry cap showed a single dead
``reduce (9216)`` — and the disposition row is that capability with both
defects fixed: the options are *operations*, and the index is typed rather
than enumerated.  **A line plot is that same reduction with its rows stroked
instead of painted**: mode ``lines`` hands ``reduceToGrid`` a *series* axis
where a heatmap hands it y, and draws each returned row as a curve — same
composed axes, same folds, same ``pixels`` rule, same layout memory, two
renderers over one machine, which is why the y axis simply relabels itself
``series`` in the dimension table.  Every combination of indices along that
axis is one line, ticked individually in a ``series`` dropdown (all / none /
invert, a ``0-7,64,100-110`` range box, a colour-swatched checkbox per line,
512 rows listed before the rest defer to the range box).  This is multi-select
over the *composite* axis, which is a different thing from a dimension's own
index selection: the table restricts one dimension by index (``C@0+1`` = both
polarizations, two lines per element), the series list picks among the
combinations that composition produces.  Reach for the table when the subset
is per dimension — the series range box cannot say "every E, only C=0", since
that is every other row.  Colour is ``viridis`` across the *drawn* lines
rather than the whole axis: three lines picked out of 9216 want three
distinguishable colours, where keying on the axis index gives them three
neighbouring shades of the same green.  The trade is that toggling recolours
the survivors, which is why the picker doubles as the **legend** — each row
carries the swatch that line is actually drawn in, and a row that is not drawn
shows a flat grey and dimmed text.  Selection changes repaint those rows in
place rather than rebuilding the list, which keeps both the scroll position
and the legend honest.  The default is a **subset** — every line up to 32,
else an evenly spaced stride forced **odd** (``| 1``), because ``num_elements
= 2 × num_dishes`` and an even stride over a pol-fastest axis would sample one
polarization and never show the other — with the count and stride in the
status line, and past 512 selected lines the plot refuses outright rather than
quietly drawing some of them.  Columns are collapsed to canvas pixels by that
same ``pixels`` rule (so a spike survives at max exactly as it does in a
heatmap), a valid sample with no valid neighbour is drawn as a **dot** — a
lone point has no segment, so a scattered fed set would otherwise render
nothing at all — and ``log`` means log-y, with non-positive values dropped
*before* combining rather than after, where a single 0 would take the whole
column with it.  On a heatmap — the N2 vis/weight matrix included — the same
switch is a **log color scale**: cells and the 2–98 percentile range are
mapped through log10, non-positive cells joining the missing-data grey and the
pixel collapse dropping them under the same rule as the line renderer, with
the status note still reporting the range in real values (``color 0.02 … 7.1
(log)``).  It exists because a linear percentile stretch drowns a matrix whose
diagonal sits decades above its off-diagonal — a subset frame's
autocorrelations against its crosses — while a pinned range (a mask's [0, 1])
ignores the switch, being pinned precisely so the mapping can't move.  **Zeros
can be blanked** (``zeros: show | hide``, off by default): only a subset of
frequencies is fed through the pathfinder today, so an
``auto_spectrum_buffer`` frame is mostly *real* zeros rather than gaps
(unarrived rows are NaN — ``N2AutoSpectrum`` fills them that way), and a trace
dragged to the floor between fed channels hides the spectrum.  Blanking is a
validity test **inside** the reduction — in ``foldOne`` and in
``reduceToGrid`` alike — rather than a skip at draw time, for the same reason
the reduction now skips NaN: a value that is neither accumulated nor counted
leaves its cell NaN like any other missing cell, whereas by draw time a pixel
folding one real value and seven zeros is already pinned to 0 by ``min`` and
dragged there by ``mean``.  It is exact ``=== 0``, never an epsilon; heatmaps
get it for free; and the status line always reports the zero fraction, so the
switch is findable without the plot ever deciding for the operator — 0 is the
payload for a mask and a legitimate power reading, which is why it is never on
by default.  The canvas paints its own background, so it follows the page's
theme — an explicit ``data-theme`` (base.html says light, the standalone pages
say dark and carry a toggle) and failing that the system preference;
``beginPlot`` re-reads it every render and a MutationObserver plus a
``prefers-color-scheme`` listener redraw on a change, so the toggle doesn't
leave a dark slab on a light page until the next poll.  Only the frame colours
swap — viridis, the mask ramp and the series colours are the data and stay
put.  Cells no element reached come out **NaN** (drawn as the missing-data
grey) rather than 0 or ±Infinity.  Extent-1 dimensions are **squeezed** —
excluded from the dimension table and folded away, which for extent 1 is a
reshape rather than a reduction and so skips the array copy entirely — because
ten of cx19's buffers lead with ``Tc=1`` and were defaulting to a one-row
hairline; defaults are then the two largest remaining dimensions on the axes
(everything else folding by the same op the pixel rule defaults to, so the
defaults are what the single global combine used to give), the **largest on
x** — the panel's canvas is full-card wide and 300 px tall (the full-screen
page gives it the rest of the window), so the longer dimension gets several
times more pixels (and fewer of its entries collapsed into one cell) lying
along x, which also lands frequency-on-x/time-on-y the way a waterfall is
normally read.  The layout (the per-dimension dispositions and their indices,
the series selection as a range string, the zeros setting) is remembered per
node+buffer in ``sessionStorage``, keyed by dimension *names* so a fetch-size
change (which moves the leading extent) doesn't discard it; recomposing the
series axis re-defaults the selection, since a row index no longer means the
same line.  **Boolean masks get their own treatment** (``uint1x8``: pl_mask,
pl_mask_exp, RFImask).  Packing is the fastest-varying axis, so the bits
extend the *last* extent — for a mask that is a plain C-order reshape back to
the logical array, and kotekan's dimnames confirm the bits are time
(``pl_mask_exp`` [Thi64=128, F, P, D8, Tlo64=8] × 8 bits = 64 → "lo64", and
128×8×8 = 8192 = ``samples_per_data_set``).  The default is nonetheless
**separate axis**: the bits stay a dimension of their own, so the packing is
visible and tickable onto x or y like any other dimension, and the ``bits``
selector keeps "merge into last axis" for reading them back as that real time
axis.  Bit order is LSB-first, matching kotekan's ``uint1x8_t`` (``(val >> n)
& 1``).  Three things then have to change together, because **1 = good** in
kotekan's masks (``gpuSimulateRFISK``: "RFI mask is 1 (good)";
``ProcessPacketMask`` counts missing packets by *inverting* the receipt
bitmap): the colour range is pinned to **[0, 1]** (percentile-stretching an
all-good mask yields a degenerate range and one flat mid-tone —
indistinguishable from all-bad), the palette runs **amber at 0 → quiet slate
at 1** so the bad news draws the eye, and the default op — both the per-
dimension fold and the ``pixels`` rule — becomes **min**, the mirror of max:
on live cx19 frames a single cleared bit is invisible under max (cell still 1)
and under mean (1.0000 vs 0.9997 — the same colour), while min turns its whole
cell amber.  The status line carries ``N% set (1 = good)``, and a grid with
one tiny axis against a long one (8 bits × 384 channels) stretches to fill the
data rect rather than rendering as a two-pixel hairline — every heatmap does
now, since the axis ticks say what the geometry is.  **N2 frames get block-
aware plotting**: ``n2LayoutFromDesc`` mirrors kotekan's
``N2FrameDesc::get_frame_layout`` (ten sequential blocks, no padding: vis
c64×num_prod, weight f32×num_prod, flags f32×n, eval f32×num_ev, evec
c64×num_ev×n, emethod i32, erms f32, chi2 f32×3, gain c64×n, mask u8×n —
offsets validated byte-for-byte against live cx19 frames), a **block**
selector replaces the dimension table and the pixel rule, complex blocks get a
**part** selector (mag/phase/real/imag), vis/weight render as the full n×n
baseline matrix (upper triangle mirrored with conjugate sign flips for
phase/imag), evec as an ev×input grid, the rest as lines, with erms/χ²
appended to the status line; N2 fetches always request the whole frame since a
byte prefix would cut the later blocks off.  Subset layouts (kotekan's
``DishInputs`` — the triangle over the ArrayDish elements only) come in **two
wire forms** and both are spoken, because the fleet is mixed (the same rule as
the pipeline palette): older kotekans keep the per-element blocks at full
width and send an explicit sparse ``product_list`` — ``num_prod`` is its
length and the vis/weight matrix is *scattered* by it rather than walked
densely, cells no product covers staying NaN (the missing-data grey), so an
unwired baseline reads as absent, never as zero correlation — while newer ones
**compact** the frame: ``num_elements`` *is* the subset size, the payload is
byte-for-byte a dense ``FullUpperTri`` over it, and an ``input_list`` maps
each compact element to its fiducial input number (verified against live cx52
frames, 6772/7292 B exact).  The list is labels, not layout: the matrix axes
are ticked **per contiguous run** of it (``bufferplot.js`` ``inputTicks`` — a
mandatory tick at every run's first element so the 15 → 64 jump is drawn at
the cell where it happens, round *fiducial* values inside each run mapped back
to their compact positions, and a round tick yielding to a boundary label it
would crowd) and the status line carries the subset as ranges (``inputs
0-15,64-79``), because compact index 20 silently reading as input 20 (it is
68) is the lie the list exists to prevent — and a compact tick sequence merely
relabelled (0, 5, 10, 15, 68, 73, 78) both hides the break between two ticks
and reads as an axis running to 80.  That wire change shipped once before
choco knew the form — every N2 subset frame silently fell back to a histogram
— so the computed block layout is now **cross-checked against kotekan's
reported ``frame_size``**: a mismatch drops to generic rendering with the two
sizes named in the status line, rather than decoding every block at the wrong
offset.  **Every plot draws into a framed rect** on a canvas whose backing
store is its CSS size × ``devicePixelRatio`` with the context scaled to match
(``beginPlot``), so the code draws in CSS pixels and the output is at the
display's real resolution; the margins are where the tick labels live.  Ticks
come from ``niceTicks`` (1/2/5 × 10^k, forced to a whole-number step for an
index axis — half a frequency channel is not a place) or ``logTicks``
(decades, with 2×/5× filled in under two decades), and heatmaps get the ticks
without gridlines, which would hide the cells they cross.  A heatmap's cells
are painted at grid resolution on an offscreen canvas and blitted into the
rect with smoothing off — still one crisp block per cell, but the canvas is no
longer *sized* to the grid, which is what leaves room for axes; the old
``object-fit`` stretch-vs-square rule went with it, since the ticks now say
what the geometry is.  **Zoom is a data window, not a canvas transform**:
wheel zooms about the cursor, drag pans, double-click resets (the gestures the
pipeline graph already uses; shift+wheel stays native).  The window is held in
*axis index* coordinates and applied between composition and the pixel
collapse, so narrowing it puts fewer samples in each pixel — zooming in
resolves real detail instead of magnifying blur, which is the whole point when
6550 channels share 800 px — and the same window means the same thing at any
canvas size, which is what makes it serialisable.  A line plot's y is a value,
not an index, so it auto-ranges to the window rather than zooming.  **The
full-screen page** ``/plot/<key>?buffer=<name>`` (``plot.html``, standalone
like ``pipeline.html``) is the same panel given the whole window:
``bufferplot.js`` renders into ``#buffer-plot`` either way and a ``data-
buffer`` attribute is what tells it to open full screen instead of waiting for
a click, so there is no second copy of anything — the canvas just takes
``flex: 1`` and re-renders on resize.  The buffer rides a query parameter
(node keys are ``<group>/<node>`` paths, so a trailing segment after
``<path:node_key>`` is ambiguous) and is validated against the same
``_BUFFER_NAME_RE`` allowlist the data API uses.  **The view rides in the URL
fragment** — ``#dims=F:x,E:y,T:mean@0-99,C:y@0+1&mode=lines&px=mean&series=0-
31&zeros=hide&log=1&zoom=100:400`` — which never reaches the server, costs no
round trip when it changes, and is bookmarkable and shareable;
``history.replaceState`` keeps it in step the way ``pipeline.js`` does for
``?layout=``, and the overlay's ⤢ link carries the current view across so full
screen opens on exactly what was on screen.  Dimensions are keyed by *name*
for the same reason the storage key is; ``:``, ``,``, ``@`` and ``+`` are left
unescaped because a fragment allows them and they carry the structure (``+``
separates the entries of a selection because ``,`` already separates the
dimensions, and ``parseRanges`` takes either); a ``C:at3`` from before the
split still parses, as a one-entry selection; and the strided default series
selection is *omitted* (the other end recomputes it) so a shared link isn't 26
indices of noise.  One record is the view (``viewState`` / ``applyViewState``)
and both backends read and write it — the fragment and the per-buffer
``sessionStorage`` memory — so a state one can express and the other can't is
not representable; ``persistView`` runs off the render, which is why no
control handler has to remember to save.  ``applyViewState`` is idempotent and
skips its dimension half until a shape is known, so it runs once at panel
build (for the controls that decide what to *fetch*) and again once the first
frame is shaped.  Polling (5 s) pauses when the tab is hidden or the panel
scrolled away (IntersectionObserver — the client-side ``poll_if_stale``), and
a **Meta** button re-fetches ``len=0`` on demand and shows the frame's
metadata/descriptor JSON in the panel (fresh each click, textContent only); a
frame id that stops advancing overlays "no new frame since …" — with
``peek_hold`` the same held frame would otherwise be re-served and the plot
would silently lie about being live.  The Plot buttons carry ``data-plot-*``
attributes with a **delegated** document-level click handler so htmx table
morphs can't detach them; the panel DOM is built entirely with
``createElement``/``textContent`` (kotekan-supplied names never enter
innerHTML — same rule as the base64-img pipeline graph).  The watched buffer
is remembered per node in ``sessionStorage`` and auto-reopened on the next
page load (no scroll), so the plot behaves as always-on once a buffer is
chosen; an explicit Close clears the memory — the button is a *selector* among
~40 frame buffers, not a render trigger.
