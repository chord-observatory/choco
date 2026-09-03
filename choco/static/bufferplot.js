/* Live buffer plots for node edit pages.
 *
 * Opens from the Plot buttons in the buffers table (delegated click on
 * [data-plot-buffer], so htmx table swaps can't detach the handler).
 * Fetches /api/node-buffer-data/<node>?buffer=<name>: once with len=0
 * for the frame descriptor (value type, extents, dimension names), then
 * polls raw prefix bytes every 5 s and renders on a <canvas>.
 *
 * Every pathfinder buffer is C-order with time leading, so a byte
 * prefix truncates only the leading dimension: the renderer just
 * recomputes that extent from the bytes received and keeps all inner
 * structure intact.
 *
 * Polling pauses while the tab is hidden or the panel is scrolled out
 * of view.  The X-Frame-Id header is compared between polls: a frame
 * id that stops advancing means a stalled pipeline serving the same
 * held frame, and the status line says so instead of pretending the
 * plot is live.
 */
(function () {
    "use strict";

    var POLL_MS = 5000;
    var FETCH_CHOICES = [
        [262144, "256 KiB"], [1048576, "1 MiB"],
        [4194304, "4 MiB"], [16777216, "16 MiB"],
    ];
    var DEFAULT_FETCH = 4194304;

    // Sampled viridis, interpolated at render time.
    var VIRIDIS = [
        [68, 1, 84], [71, 44, 122], [59, 81, 139], [44, 113, 142],
        [33, 144, 141], [39, 173, 129], [92, 200, 99], [170, 220, 50],
        [253, 231, 37],
    ];

    // --- value decoding -------------------------------------------------
    // Each entry: bytes per descriptor element, and a decoder from
    // ArrayBuffer to a numeric array.  `sub` values per element > 1
    // (packed types) append an extra trailing dimension.
    var TYPED = {
        int8: Int8Array, uint8: Uint8Array, char: Uint8Array,
        int16: Int16Array, uint16: Uint16Array,
        int32: Int32Array, uint32: Uint32Array,
        float32: Float32Array, float64: Float64Array,
    };

    function decodeFloat16(buf) {
        var u = new Uint16Array(buf, 0, Math.floor(buf.byteLength / 2));
        var out = new Float32Array(u.length);
        for (var i = 0; i < u.length; i++) {
            var h = u[i], s = (h & 0x8000) ? -1 : 1;
            var e = (h >> 10) & 0x1f, m = h & 0x3ff;
            if (e === 0) out[i] = s * m * Math.pow(2, -24);
            else if (e === 31) out[i] = m ? NaN : s * Infinity;
            else out[i] = s * (1 + m / 1024) * Math.pow(2, e - 15);
        }
        return out;
    }

    function decodeBigInt(buf, Ctor) {
        var b = new Ctor(buf, 0, Math.floor(buf.byteLength / 8));
        var out = new Float64Array(b.length);
        for (var i = 0; i < b.length; i++) out[i] = Number(b[i]);
        return out;
    }

    // 4-bit offset-encoded pairs: two values per byte, stored + 8.
    // Component order (re/im) is not significant for these renderers.
    function decodeInt4x2(buf) {
        var u = new Uint8Array(buf);
        var out = new Int8Array(u.length * 2);
        for (var i = 0; i < u.length; i++) {
            out[2 * i] = (u[i] & 0x0f) - 8;
            out[2 * i + 1] = (u[i] >> 4) - 8;
        }
        return out;
    }

    // Boolean masks: n one-bit values packed per byte, LSB first —
    // kotekan's uint1x8_t is ((v0 & 1) << 0) | ((v1 & 1) << 1) | …, and
    // its operator[](n) reads (val >> n) & 1, so bit 0 is the *first*
    // value.  Getting this backwards would mirror the mask within every
    // group of 8 without changing any summary statistic, so it is worth
    // stating: this order is the one kotekan writes.
    function decodeUint1xN(n) {
        return function (buf) {
            var u = new Uint8Array(buf);
            var out = new Uint8Array(u.length * n);
            for (var i = 0; i < u.length; i++)
                for (var b = 0; b < n; b++)
                    out[n * i + b] = (u[i] >> b) & 1;
            return out;
        };
    }

    function decoderFor(valueType) {
        var t = valueType || "";
        if (TYPED[t]) {
            var Ctor = TYPED[t];
            return {
                bytes: Ctor.BYTES_PER_ELEMENT, sub: 1, subName: null,
                integer: t.indexOf("float") !== 0,
                decode: function (buf) {
                    var n = Math.floor(buf.byteLength / Ctor.BYTES_PER_ELEMENT);
                    return new Ctor(buf, 0, n);
                },
            };
        }
        if (t === "float16")
            return { bytes: 2, sub: 1, subName: null, integer: false,
                     decode: decodeFloat16 };
        if (t === "int64")
            return { bytes: 8, sub: 1, subName: null, integer: true,
                     decode: function (b) { return decodeBigInt(b, BigInt64Array); } };
        if (t === "uint64")
            return { bytes: 8, sub: 1, subName: null, integer: true,
                     decode: function (b) { return decodeBigInt(b, BigUint64Array); } };
        // Complex: the pair is the fastest-varying axis, exactly like
        // the packed types below, so `sub: 2` makes re/im a trailing
        // dimension.  Unlike those, a *derived* view (magnitude, phase)
        // is usually what an operator wants, which is what the `part`
        // selector collapses the pair into — see applyPart.
        if (t === "complex64" || t === "complex128") {
            var wide = t === "complex128";
            return {
                bytes: wide ? 16 : 8, sub: 2, subName: "C", integer: false,
                complex: true,
                decode: function (b) {
                    var Ctor = wide ? Float64Array : Float32Array;
                    return new Ctor(b, 0, Math.floor(
                        b.byteLength / Ctor.BYTES_PER_ELEMENT) & ~1);
                },
            };
        }
        if (t.indexOf("int4x2") === 0)
            return { bytes: 1, sub: 2, subName: "re/im", integer: true,
                     decode: decodeInt4x2 };
        // uint1x8 today, but the name encodes the packing factor, so
        // take it from the type rather than hard-coding 8.
        var packed = /^uint1x(\d+)$/.exec(t);
        if (packed && +packed[1] >= 1 && +packed[1] <= 8)
            return { bytes: 1, sub: +packed[1], subName: "bit", integer: true,
                     mask: true, decode: decodeUint1xN(+packed[1]) };
        // Unknown value type: plot the raw bytes rather than nothing.
        return { bytes: 1, sub: 1, subName: null, integer: true,
                 decode: function (b) { return new Uint8Array(b); } };
    }

    // A complex buffer is decoded as interleaved pairs; `part` decides
    // whether the operator sees those components as their own dimension
    // ("components") or a derived scalar.  Magnitude and phase are what
    // a gain table is actually read as — real and imaginary separately
    // are a phase ramp chopped into two — so this is not a nicety.
    function complexPart(f32, count, part) {
        var out = new Float32Array(count);
        for (var i = 0; i < count; i++) {
            var re = f32[2 * i], im = f32[2 * i + 1];
            out[i] = part === "real" ? re : part === "imag" ? im :
                     part === "phase" ? Math.atan2(im, re) : Math.hypot(re, im);
        }
        return out;
    }

    // --- N2 frames --------------------------------------------------------
    // Mirror of kotekan's N2FrameDesc::get_frame_layout: ten sequential
    // blocks, no padding.  Offsets validated against live cx19 frames
    // (n2_buffer 100756 B, n2_eigen_buffer 7292 B — exact size matches,
    // and the decoded autocorrelations come out real and positive).
    // Subset layouts come in two wire forms and both are spoken (the
    // fleet is mixed).  Older kotekans keep the per-element blocks at
    // full num_elements width and send an explicit sparse product_list
    // — only the product space compacts.  Newer ones compact DishInputs
    // outright: num_elements *is* the subset size, the frame is a dense
    // upper triangle over it, and input_list maps each compact element
    // to its fiducial input number (labels only — the byte layout is
    // exactly FullUpperTri's; sizes verified against live cx52 frames,
    // 6772 B / 7292 B exact).
    function n2LayoutFromDesc(desc) {
        var n = desc.num_elements, nev = desc.num_ev || 0;
        var layout = desc.n2_layout;
        var inputs = Array.isArray(desc.input_list) &&
                     desc.input_list.length === n ? desc.input_list : null;
        var numProd, prods = null;
        if (layout === "FullUpperTri") numProd = n * (n + 1) / 2;
        else if (layout === "Autocorrelations") numProd = n;
        else if (Array.isArray(desc.product_list)) {
            prods = desc.product_list;
            numProd = prods.length;
        }
        else if (layout === "DishInputs" && inputs)
            numProd = n * (n + 1) / 2; // compact form: dense over the subset
        else return null; // unknown layout: generic fallback rendering
        var defs = [
            ["vis", 8 * numProd, "c64"],
            ["weight", 4 * numProd, "f32"],
            ["flags", 4 * n, "f32"],
            ["eval", 4 * nev, "f32"],
            ["evec", 8 * nev * n, "c64"],
            ["emethod", 4, "i32"],
            ["erms", 4, "f32"],
            ["chi2", 12, "f32"],
            ["gain", 8 * n, "c64"],
            ["mask", n, "u8"],
        ];
        var blocks = {}, off = 0;
        defs.forEach(function (d) {
            blocks[d[0]] = { off: off, bytes: d[1], type: d[2] };
            off += d[1];
        });
        return { n: n, nev: nev, layout: layout, numProd: numProd,
                 prods: prods, inputs: inputs, blocks: blocks, total: off };
    }

    // Unpack the packed products into a full n×n grid; the lower triangle
    // is the conjugate mirror, so the odd parts flip sign.  A dense
    // FullUpperTri frame walks the triangle in order; a subset layout
    // hands over its product list and the values are scattered by it —
    // cells no product covers stay NaN, the missing-data grey, so an
    // unwired baseline reads as absent rather than as zero correlation.
    function n2Grid(tri, n, part, prods) {
        var grid, i, j, k, v;
        var neg = part === "phase" || part === "imag";
        if (prods) {
            grid = new Float64Array(n * n).fill(NaN);
            for (k = 0; k < prods.length && k < tri.length; k++) {
                i = prods[k][0]; j = prods[k][1];
                v = tri[k];
                grid[i * n + j] = v;
                grid[j * n + i] = neg ? -v : v;
            }
            return grid;
        }
        grid = new Float64Array(n * n);
        k = 0;
        for (i = 0; i < n; i++)
            for (j = i; j < n; j++, k++) {
                v = tri[k];
                grid[i * n + j] = v;
                grid[j * n + i] = neg ? -v : v;
            }
        return grid;
    }

    // --- helpers --------------------------------------------------------

    function el(tag, attrs, text) {
        var e = document.createElement(tag);
        for (var k in attrs || {}) e.setAttribute(k, attrs[k]);
        if (text != null) e.textContent = text;
        return e;
    }

    function fmtBytes(n) {
        if (n == null || isNaN(n)) return "?";
        var units = ["B", "KiB", "MiB", "GiB"];
        for (var i = 0; i < units.length; i++) {
            if (n < 1024 || i === units.length - 1)
                return (i ? n.toFixed(1) : n.toFixed(0)) + " " + units[i];
            n /= 1024;
        }
    }

    function fmtVal(v) {
        if (!isFinite(v)) return String(v);
        if (v !== 0 && (Math.abs(v) >= 1e5 || Math.abs(v) < 1e-3))
            return v.toExponential(2);
        return String(Math.round(v * 1000) / 1000);
    }

    function viridis(t) {
        var x = Math.max(0, Math.min(1, t)) * (VIRIDIS.length - 1);
        var i = Math.min(Math.floor(x), VIRIDIS.length - 2), f = x - i;
        var a = VIRIDIS[i], b = VIRIDIS[i + 1];
        return [a[0] + f * (b[0] - a[0]), a[1] + f * (b[1] - a[1]),
                a[2] + f * (b[2] - a[2])];
    }

    // Boolean-mask ramp.  Polarity matters: kotekan's masks are 1 =
    // *good* (gpuSimulateRFISK documents "RFI mask is 1 (good)", and
    // ProcessPacketMask counts missing packets by inverting the receipt
    // bitmap), so 0 is the bad news and has to be what draws the eye —
    // set bits stay quiet slate, clear bits glow amber, fractions ramp
    // between.  The range is fixed to [0, 1] rather than percentile-
    // stretched: an all-good mask has a degenerate percentile range and
    // would otherwise render as one flat mid-tone, indistinguishable
    // from an all-bad one.
    function maskColor(t) {
        var x = Math.max(0, Math.min(1, t));
        return [255 + x * (28 - 255), 150 + x * (42 - 150), 90 + x * (56 - 90)];
    }

    // Robust color range: 2nd..98th percentile of the finite cells.
    function robustRange(arr) {
        var vals = [];
        for (var i = 0; i < arr.length; i++)
            if (isFinite(arr[i])) vals.push(arr[i]);
        if (!vals.length) return [0, 1];
        vals.sort(function (a, b) { return a - b; });
        var lo = vals[Math.floor(0.02 * (vals.length - 1))];
        var hi = vals[Math.ceil(0.98 * (vals.length - 1))];
        if (lo === hi) { lo = vals[0]; hi = vals[vals.length - 1]; }
        if (lo === hi) { lo -= 1; hi += 1; }
        return [lo, hi];
    }

    // --- folding ----------------------------------------------------------
    // Every dimension carries its own disposition: on the x axis, on the
    // y/series axis, or folded away by its own operation (max / mean /
    // min / one index).  Folding happens *before* the axes are composed,
    // which splits the two reductions that used to share one control:
    // what happens to a dimension is per dimension (mean over time is
    // the integration), what happens to the entries of an axis sharing a
    // canvas pixel is per plot (max there keeps a spike visible).
    //
    // Folds are applied outermost first.  Ops of different kinds do not
    // commute — max over T then mean over F is not the reverse — so the
    // order is fixed and stated rather than left to look arbitrary; it
    // only shows when two dimensions fold with *different* ops, and an
    // index pick is order-free either way.

    // Restrict dimension `k` to the chosen indices: a gather along one
    // axis that leaves every other extent alone, so what follows sees an
    // ordinary (smaller) array and neither the folds nor the axes need
    // to know a selection happened.  The output keeps the input's type —
    // this runs before any arithmetic, so there is nothing to widen for.
    function pickOne(values, dims, k, idx) {
        var outer = 1, inner = 1, i, o, j;
        for (i = 0; i < k; i++) outer *= dims[i];
        var n = dims[k], m = idx.length;
        for (i = k + 1; i < dims.length; i++) inner *= dims[i];
        var out = new values.constructor(outer * m * inner);
        for (o = 0; o < outer; o++)
            for (j = 0; j < m; j++) {
                var src = (o * n + idx[j]) * inner, dst = (o * m + j) * inner;
                for (i = 0; i < inner; i++) out[dst + i] = values[src + i];
            }
        return out;
    }

    // Apply every index selection.  Dimensions are not removed here, so
    // indices stay aligned with the disposition list.
    function filterDims(values, dims, picks) {
        var out = values, outDims = dims.slice();
        for (var d = 0; d < dims.length; d++) {
            var idx = picks[d];
            if (!idx || idx.length === dims[d]) continue;
            out = pickOne(out, outDims, d, idx);
            outDims[d] = idx.length;
        }
        return { values: out, dims: outDims };
    }

    // Fold dimension `k` out of `dims`, treating the array as
    // outer × dims[k] × inner.  Invalid values are skipped exactly as
    // they are in the composition pass, so a cell fed nothing valid
    // comes out NaN instead of 0 or ±Infinity.
    function foldOne(values, dims, k, how, blankZeros) {
        var outer = 1, inner = 1, i, o, j, c;
        for (i = 0; i < k; i++) outer *= dims[i];
        var n = dims[k];
        for (i = k + 1; i < dims.length; i++) inner *= dims[i];
        var out = new Float64Array(outer * inner);
        var cnt = new Int32Array(outer * inner);
        if (how === "max") out.fill(-Infinity);
        else if (how === "min") out.fill(Infinity);
        for (o = 0; o < outer; o++) {
            for (j = 0; j < n; j++) {
                var src = (o * n + j) * inner;
                for (i = 0; i < inner; i++) {
                    var v = values[src + i];
                    if (!isFinite(v) || (blankZeros && v === 0)) continue;
                    c = o * inner + i;
                    if (how === "max") { if (v > out[c]) out[c] = v; }
                    else if (how === "min") { if (v < out[c]) out[c] = v; }
                    else out[c] += v;
                    cnt[c]++;
                }
            }
        }
        for (c = 0; c < out.length; c++) {
            if (!cnt[c]) out[c] = NaN;
            else if (how === "mean") out[c] /= cnt[c];
        }
        return out;
    }

    // Apply every fold, returning the surviving array plus a map from
    // original dimension index to its index in the folded shape (-1 for
    // the ones that are gone).
    function foldDims(values, dims, names, disp, blankZeros) {
        var out = values, outDims = dims.slice();
        var outNames = (names || []).slice();
        var map = [], d, e;
        for (d = 0; d < dims.length; d++) map.push(d);
        for (d = 0; d < dims.length; d++) {
            if (disp[d] === "x" || disp[d] === "y") continue;
            var k = map[d];
            // Folding an extent-1 dimension is a reshape, not a
            // reduction — dropping it without touching the data saves a
            // copy of the whole array, and ten of cx19's buffers lead
            // with Tc=1.  A dimension picked down to a single index
            // arrives here that way too, which is what makes "pin to one
            // index" free rather than a special case.
            if (outDims[k] > 1)
                out = foldOne(out, outDims, k, disp[d], blankZeros);
            outDims.splice(k, 1);
            outNames.splice(k, 1);
            map[d] = -1;
            for (e = 0; e < dims.length; e++) if (map[e] > k) map[e]--;
        }
        return { values: out, dims: outDims, names: outNames, map: map };
    }

    // --- renderers ------------------------------------------------------

    // Reduce an N-d array onto a (y, x) grid with an odometer walk —
    // one pass over the values, no per-element div/mod chains.
    //
    // yDims/xDims are *lists* of dimension indices: an axis can be
    // composed of several dimensions (P×D8), which tiles every
    // combination along it instead of averaging them away.  The
    // dimensions need not be adjacent in the array — the odometer walks
    // strides, not memory order — so the only cost of a non-adjacent
    // composite is that it interleaves, which the axis label spells out.
    // Within a composite the earlier-listed dimension varies slowest,
    // matching what a C-order reshape of adjacent dims would give.
    //
    // A dimension is all-or-nothing: either it is on an axis (every
    // index of it drawn) or it is combined away by `how`.  There is no
    // pinning to a single index — an index pick is a dimension of
    // length 1, which is the one thing an axis composition cannot say.
    //
    // opts.blankZeros treats an exact 0 as missing.  The frequencies no
    // data has been fed to hold real zeros, not gaps, and this has to
    // happen *inside* the reduction: at draw time it would come too
    // late, because a cell folding one real value and seven zeros is
    // already pinned to 0 by min and dragged towards it by mean.
    // opts.rowMap (row index -> compact row, -1 to drop) keeps only the
    // selected series, so the grid stays proportional to what is drawn
    // rather than to the whole axis.
    function reduceToGrid(values, dims, yDims, xDims, how, opts) {
        opts = opts || {};
        var blankZeros = !!opts.blankZeros, rowMap = opts.rowMap || null;
        var nd = dims.length, d;
        var mulY = new Int32Array(nd), mulX = new Int32Array(nd);
        var h = 1, w = 1;
        for (d = yDims.length - 1; d >= 0; d--) {
            mulY[yDims[d]] = h;
            h *= dims[yDims[d]];
        }
        for (d = xDims.length - 1; d >= 0; d--) {
            mulX[xDims[d]] = w;
            w *= dims[xDims[d]];
        }
        if (rowMap) h = opts.nRows;

        var acc = new Float64Array(h * w);
        var cnt = new Float64Array(h * w);
        if (how === "max") acc.fill(-Infinity);
        if (how === "min") acc.fill(Infinity);
        var idx = new Int32Array(nd);
        // Cell offsets are carried incrementally: the odometer only ever
        // changes a suffix of the index, so this stays O(1) per element.
        var yCell = 0, xCell = 0;
        // Clamp to whole slices: a trailing partial slice would wrap the
        // odometer back to cell 0 and pollute the grid.
        var total = 1;
        for (d = 0; d < nd; d++) total *= dims[d];
        var n = Math.min(values.length, total);
        for (var i = 0; i < n; i++) {
            var yc = rowMap ? rowMap[yCell] : yCell;
            var v = values[i];
            // Invalid values are not folded in and not counted, so a
            // cell nothing valid reached ends up NaN — the same missing
            // state a cell no element reached gets, and the same one the
            // renderers already draw as a gap.
            if (yc >= 0 && isFinite(v) && !(blankZeros && v === 0)) {
                var cell = yc * w + xCell;
                if (how === "max") {
                    if (v > acc[cell]) acc[cell] = v;
                } else if (how === "min") {
                    if (v < acc[cell]) acc[cell] = v;
                } else {
                    acc[cell] += v;
                }
                cnt[cell]++;
            }
            for (d = nd - 1; d >= 0; d--) {
                if (++idx[d] < dims[d]) {
                    yCell += mulY[d];
                    xCell += mulX[d];
                    break;
                }
                idx[d] = 0;
                yCell -= mulY[d] * (dims[d] - 1);
                xCell -= mulX[d] * (dims[d] - 1);
            }
        }
        if (how !== "max" && how !== "min")
            for (var c = 0; c < acc.length; c++)
                if (cnt[c]) acc[c] /= cnt[c];
        // Cells that no valid element reached (the received bytes
        // stopping short of a whole grid, every value under the cell
        // blanked) must read as missing, not as 0 or ±Infinity.
        for (var e = 0; e < acc.length; e++)
            if (!cnt[e]) acc[e] = NaN;
        return { grid: acc, h: h, w: w };
    }

    // --- the plot frame ---------------------------------------------------
    // Every renderer draws into a margined rect on a device-pixel-scaled
    // canvas: the backing store is CSS size × devicePixelRatio and the
    // context is scaled by the same factor, so all drawing below is in
    // CSS pixels while text and strokes come out at the display's real
    // resolution.  The margins are what the tick labels live in.

    var PAD = { l: 66, r: 12, t: 12, b: 34 };

    // The canvas paints its own background, so it has to follow the
    // page's theme or it sits as a dark slab on a light page.  Three
    // states, the same three the pages have: an explicit data-theme
    // (base.html says light, the standalone pages say dark and let the
    // operator toggle), and failing that the system preference.
    var DARK = { bg: "#1a1f24", ink: "#adb5bd",
                 grid: "rgba(173, 181, 189, 0.18)", missing: 40,
                 line: "#4dabf7", muted: "#5c6b7a" };
    var LIGHT = { bg: "#ffffff", ink: "#495057",
                  grid: "rgba(73, 80, 87, 0.16)", missing: 208,
                  line: "#1c7ed6", muted: "#adb5bd" };
    var THEME = DARK;

    function isDarkTheme() {
        var explicit = document.documentElement.getAttribute("data-theme");
        if (explicit === "dark") return true;
        if (explicit === "light") return false;
        return !!(window.matchMedia &&
                  window.matchMedia("(prefers-color-scheme: dark)").matches);
    }

    function refreshTheme() {
        THEME = isDarkTheme() ? DARK : LIGHT;
    }

    // A canvas whose CSS size the layout decides (the panel gives it a
    // height, the full-screen page lets it fill).  Returns the drawing
    // context plus the rect the data occupies.
    function beginPlot(canvas) {
        refreshTheme();
        var dpr = window.devicePixelRatio || 1;
        var W = Math.max(240, Math.round(canvas.clientWidth || 800));
        var H = Math.max(160, Math.round(canvas.clientHeight || 300));
        canvas.width = Math.round(W * dpr);
        canvas.height = Math.round(H * dpr);
        canvas.style.imageRendering = "auto";
        var ctx = canvas.getContext("2d");
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.fillStyle = THEME.bg;
        ctx.fillRect(0, 0, W, H);
        ctx.font = "11px monospace";
        return {
            ctx: ctx, dpr: dpr, W: W, H: H,
            rect: { l: PAD.l, t: PAD.t,
                    w: Math.max(20, W - PAD.l - PAD.r),
                    h: Math.max(20, H - PAD.t - PAD.b) },
        };
    }

    // Tick values at 1/2/5 × 10^k covering [lo, hi].  `integer` forces a
    // whole-number step, which is what an index axis wants — half a
    // frequency channel is not a place.
    function niceTicks(lo, hi, target, integer) {
        if (!(hi > lo)) return [lo];
        var raw = (hi - lo) / Math.max(1, target);
        var mag = Math.pow(10, Math.floor(Math.log10(raw)));
        var norm = raw / mag;
        var step = (norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10) * mag;
        if (integer) step = Math.max(1, Math.round(step));
        var out = [];
        for (var v = Math.ceil(lo / step) * step; v <= hi + step * 1e-9; v += step)
            out.push(Math.abs(v) < step * 1e-9 ? 0 : v);
        return out;
    }

    // Decades for a log axis, thinned to keep the labels legible, with
    // 2× and 5× filled in when the range spans less than two decades.
    function logTicks(lo, hi) {
        var out = [], d, decades = Math.floor(hi) - Math.ceil(lo) + 1;
        var stride = Math.max(1, Math.ceil(decades / 8));
        for (d = Math.ceil(lo) - 1; d <= Math.floor(hi) + 1; d += stride) {
            var base = Math.pow(10, d);
            [1, 2, 5].forEach(function (m) {
                if (m !== 1 && (decades > 2 || stride > 1)) return;
                var t = Math.log10(m * base);
                if (t >= lo && t <= hi) out.push(t);
            });
        }
        return out.length ? out : [lo, hi];
    }

    // Frame, ticks and labels around the data rect.  Scales map a data
    // value to a CSS pixel; `xTicks`/`yTicks` carry {v, text} pairs so
    // the caller decides what a tick *means* (index, value, decade).
    // `gridlines` is off for heatmaps, where the cells are the data and
    // a rule drawn across them hides some of it; the ticks outside the
    // rect carry the scale there.
    function drawFrame(f, xTicks, yTicks, xScale, yScale, names, gridlines) {
        var ctx = f.ctx, r = f.rect, i, p;
        if (gridlines) {
            ctx.strokeStyle = THEME.grid;
            ctx.lineWidth = 1;
            ctx.beginPath();
            for (i = 0; i < xTicks.length; i++) {
                p = Math.round(xScale(xTicks[i].v)) + 0.5;
                if (p < r.l || p > r.l + r.w) continue;
                ctx.moveTo(p, r.t);
                ctx.lineTo(p, r.t + r.h);
            }
            for (i = 0; i < yTicks.length; i++) {
                p = Math.round(yScale(yTicks[i].v)) + 0.5;
                if (p < r.t || p > r.t + r.h) continue;
                ctx.moveTo(r.l, p);
                ctx.lineTo(r.l + r.w, p);
            }
            ctx.stroke();
        }
        // Tick marks outside the rect: the scale stays readable whether
        // or not the gridlines are drawn.
        ctx.strokeStyle = THEME.ink;
        ctx.beginPath();
        for (i = 0; i < xTicks.length; i++) {
            p = Math.round(xScale(xTicks[i].v)) + 0.5;
            if (p < r.l || p > r.l + r.w) continue;
            ctx.moveTo(p, r.t + r.h);
            ctx.lineTo(p, r.t + r.h + 4);
        }
        for (i = 0; i < yTicks.length; i++) {
            p = Math.round(yScale(yTicks[i].v)) + 0.5;
            if (p < r.t || p > r.t + r.h) continue;
            ctx.moveTo(r.l - 4, p);
            ctx.lineTo(r.l, p);
        }
        ctx.stroke();
        ctx.strokeStyle = THEME.ink;
        ctx.globalAlpha = 0.5;
        ctx.strokeRect(r.l + 0.5, r.t + 0.5, r.w, r.h);
        ctx.globalAlpha = 1;

        ctx.fillStyle = THEME.ink;
        ctx.textAlign = "right";
        for (i = 0; i < yTicks.length; i++) {
            p = yScale(yTicks[i].v);
            if (p < r.t - 1 || p > r.t + r.h + 1) continue;
            ctx.fillText(yTicks[i].text, r.l - 6, p + 4);
        }
        ctx.textAlign = "center";
        for (i = 0; i < xTicks.length; i++) {
            p = xScale(xTicks[i].v);
            if (p < r.l - 1 || p > r.l + r.w + 1) continue;
            ctx.fillText(xTicks[i].text, p, r.t + r.h + 15);
        }
        if (names && names.x)
            ctx.fillText(names.x, r.l + r.w / 2, f.H - 4);
        if (names && names.y) {
            ctx.save();
            ctx.translate(11, r.t + r.h / 2);
            ctx.rotate(-Math.PI / 2);
            ctx.textAlign = "center";
            ctx.fillText(names.y, 0, 0);
            ctx.restore();
        }
        ctx.textAlign = "left";
    }

    // Index ticks for an axis of `n` entries spanning [i0, i1).
    function indexTicks(i0, i1, pixels) {
        return niceTicks(i0, i1 - 1, Math.max(2, Math.round(pixels / 90)), true)
            .filter(function (v) { return v >= i0 && v <= i1 - 1; })
            .map(function (v) { return { v: v, text: String(v) }; });
    }

    function valueTicks(lo, hi, pixels, logY) {
        var target = Math.max(2, Math.round(pixels / 44));
        if (logY)
            return logTicks(lo, hi).map(function (t) {
                return { v: t, text: fmtVal(Math.pow(10, t)) };
            });
        return niceTicks(lo, hi, target, false).map(function (v) {
            return { v: v, text: fmtVal(v) };
        });
    }

    // opts.range fixes the colour range (masks: [0, 1] — never
    // percentile-stretched); opts.color swaps the palette.  opts.log
    // switches to a log colour scale: cells and the percentile range
    // are mapped through log10, non-positive cells joining the
    // missing-data grey — the same rule the line renderer applies to a
    // log y axis.  Linear percentile stretch drowns a matrix whose
    // diagonal sits decades above its off-diagonal (a subset frame's
    // autocorrelations against its crosses); log spreads the decades
    // evenly.  A pinned range ignores it — a mask's [0, 1] is pinned
    // precisely so the mapping can't move.  The same opts object is
    // handed to reduceToGrid, which reads blankZeros from it and
    // ignores the rest.
    //
    // The cells are painted at grid resolution on an offscreen canvas
    // and blitted into the data rect with smoothing off, so one cell is
    // still one crisp block — the difference from before is that the
    // canvas is no longer *sized* to the grid, which is what leaves room
    // for the axes.
    function drawGrid(f, grid, h, w, opts) {
        opts = opts || {};
        var logC = !!opts.log && !opts.range;
        if (logC) {
            var lg = new Float64Array(grid.length);
            for (var k = 0; k < grid.length; k++)
                lg[k] = grid[k] > 0 ? Math.log10(grid[k]) : NaN;
            grid = lg;
        }
        var range = opts.range || robustRange(grid);
        var lo = range[0], hi = range[1];
        var color = opts.color || viridis;
        var off = document.createElement("canvas");
        off.width = w;
        off.height = h;
        var octx = off.getContext("2d");
        var img = octx.createImageData(w, h);
        for (var i = 0; i < grid.length; i++) {
            var v = grid[i], p = 4 * i;
            if (!isFinite(v)) {
                img.data[p] = img.data[p + 1] = img.data[p + 2] = THEME.missing;
            } else {
                var c = color((v - lo) / (hi - lo));
                img.data[p] = c[0]; img.data[p + 1] = c[1]; img.data[p + 2] = c[2];
            }
            img.data[p + 3] = 255;
        }
        octx.putImageData(img, 0, 0);
        var ctx = f.ctx, r = f.rect;
        ctx.imageSmoothingEnabled = false;
        ctx.drawImage(off, 0, 0, w, h, r.l, r.t, r.w, r.h);
        ctx.imageSmoothingEnabled = true;
        // The note reports real values either way — the log is how the
        // ramp is spread, not what the numbers are.
        if (logC)
            return "color " + fmtVal(Math.pow(10, lo)) + " … " +
                   fmtVal(Math.pow(10, hi)) + " (log)";
        return "color " + fmtVal(lo) + " … " + fmtVal(hi);
    }

    // Collapse a grid to at most maxH × maxW cells with `how`.  Without
    // this the canvas is sized to the grid and the browser's downscaler
    // decides which cells survive — it drops whole columns, so a narrow
    // spike in a 6550-wide heatmap disappears at exactly the moment it
    // matters.  This is the same pixel rule the line renderer applies —
    // dropNonPos included, mirroring collapseRow: with a log colour
    // scale a 0 sharing a cell would pin min and drag mean before the
    // log ever sees the cell.
    function collapseGrid(grid, h, w, maxH, maxW, how, dropNonPos) {
        if (h <= maxH && w <= maxW) return { grid: grid, h: h, w: w };
        var H = Math.min(h, maxH), W = Math.min(w, maxW);
        var out = new Float64Array(H * W), cnt = new Int32Array(H * W);
        var x, y, c;
        if (how === "max") out.fill(-Infinity);
        else if (how === "min") out.fill(Infinity);
        for (y = 0; y < h; y++) {
            var ry = H === h ? y : Math.floor(y * H / h);
            for (x = 0; x < w; x++) {
                var v = grid[y * w + x];
                if (!isFinite(v) || (dropNonPos && v <= 0)) continue;
                c = ry * W + (W === w ? x : Math.floor(x * W / w));
                if (how === "max") { if (v > out[c]) out[c] = v; }
                else if (how === "min") { if (v < out[c]) out[c] = v; }
                else out[c] += v;
                cnt[c]++;
            }
        }
        for (c = 0; c < out.length; c++) {
            if (!cnt[c]) out[c] = NaN;
            else if (how === "mean") out[c] /= cnt[c];
        }
        return { grid: out, h: H, w: W };
    }

    // Slice the zoom window out of a composed grid.  Zooming narrows the
    // *data* window rather than magnifying pixels: fewer samples then
    // share each pixel, so zooming in resolves detail instead of
    // enlarging blur — which is the whole point when 6550 channels are
    // sharing 800 px.
    function windowGrid(grid, h, w, win) {
        if (win.x0 === 0 && win.x1 === w && win.y0 === 0 && win.y1 === h)
            return { grid: grid, h: h, w: w };
        var W = win.x1 - win.x0, H = win.y1 - win.y0;
        var out = new Float64Array(W * H);
        for (var y = 0; y < H; y++)
            for (var x = 0; x < W; x++)
                out[y * W + x] = grid[(win.y0 + y) * w + win.x0 + x];
        return { grid: out, h: H, w: W };
    }

    function renderHeatmap(canvas, values, dims, yDims, xDims, how, opts) {
        var r = reduceToGrid(values, dims, yDims, xDims, how, opts);
        var win = resolveWindow(r.w, r.h);
        var s = windowGrid(r.grid, r.h, r.w, win);
        var f = beginPlot(canvas);
        // Collapse to *device* pixels, not CSS pixels: on a 2× display
        // that is twice the cells across the same rect.
        var g = collapseGrid(s.grid, s.h, s.w,
                             Math.round(f.rect.h * f.dpr),
                             Math.round(f.rect.w * f.dpr), how,
                             !!opts.log && !opts.range);
        var note = drawGrid(f, g.grid, g.h, g.w, opts);
        var xAt = function (v) {
            return f.rect.l + ((v + 0.5 - win.x0) / (win.x1 - win.x0)) * f.rect.w;
        };
        var yAt = function (v) {
            return f.rect.t + ((v + 0.5 - win.y0) / (win.y1 - win.y0)) * f.rect.h;
        };
        drawFrame(f, indexTicks(win.x0, win.x1, f.rect.w),
                  indexTicks(win.y0, win.y1, f.rect.h),
                  xAt, yAt, opts.names, false);
        setMapping(f, win, false, { w: r.w, h: r.h });
        if (g.h !== s.h || g.w !== s.w)
            note += ", " + s.h + "×" + s.w + " → " + g.h + "×" + g.w +
                    " px (" + how + ")";
        return note + windowNote(win, r.w, r.h);
    }

    function renderHistogram(canvas, values, integer, logY, blank) {
        var min = Infinity, max = -Infinity, i;
        var valid = function (v) {
            return isFinite(v) && !(blank && v === 0);
        };
        for (i = 0; i < values.length; i++) {
            var v = values[i];
            if (!valid(v)) continue;
            if (v < min) min = v;
            if (v > max) max = v;
        }
        if (min > max) { min = 0; max = 1; }
        var oneBinPerInt = integer && (max - min) < 256;
        var nbins = oneBinPerInt ? Math.max(1, Math.round(max - min) + 1) : 64;
        var counts = new Float64Array(nbins);
        var scale = nbins / ((max - min) || 1);
        for (i = 0; i < values.length; i++) {
            var x = values[i];
            if (!valid(x)) continue;
            var b = oneBinPerInt ? Math.round(x - min)
                                 : Math.min(nbins - 1, Math.floor((x - min) * scale));
            counts[b]++;
        }
        var peak = 0;
        for (i = 0; i < nbins; i++) {
            if (logY) counts[i] = counts[i] ? Math.log10(1 + counts[i]) : 0;
            if (counts[i] > peak) peak = counts[i];
        }
        var f = beginPlot(canvas);
        var r = f.rect;
        var xAt = function (v) { return r.l + ((v - min) / ((max - min) || 1)) * r.w; };
        var yAt = function (v) { return r.t + (1 - v / (peak || 1)) * r.h; };
        drawFrame(f, valueTicks(min, max, r.w, false),
                  valueTicks(0, peak || 1, r.h, false).map(function (t) {
                      return { v: t.v, text: logY ? fmtVal(Math.pow(10, t.v) - 1)
                                                  : t.text };
                  }),
                  xAt, yAt, { x: "value", y: logY ? "count (log)" : "count" }, true);
        var ctx = f.ctx;
        ctx.save();
        ctx.beginPath();
        ctx.rect(r.l, r.t, r.w, r.h);
        ctx.clip();
        ctx.fillStyle = THEME.line;
        var bw = r.w / nbins;
        for (i = 0; i < nbins; i++) {
            var bh = peak ? (counts[i] / peak) * r.h : 0;
            ctx.fillRect(r.l + i * bw, r.t + r.h - bh, Math.max(1, bw - 1), bh);
        }
        ctx.restore();
        setMapping(f, null, false, null);
        return "range " + fmtVal(min) + " … " + fmtVal(max) +
            ", " + nbins + " bins" + (logY ? ", log counts" : "");
    }

    // --- line plots -------------------------------------------------------
    // A line plot is the heatmap reduction with its rows stroked as
    // curves instead of painted as pixels: rows are the series axis,
    // columns the x axis, and everything else is combined away exactly
    // as it is for a heatmap.

    // Collapse one grid row to canvas columns with the same `how` the
    // reduction uses — what an operator picks for values sharing a
    // heatmap cell is what they get for samples sharing a line pixel,
    // so a spike survives at max exactly as it does there.  Under log y
    // a non-positive value is invalid and is dropped here rather than
    // after combining, where a single 0 would take the column with it.
    function collapseRow(grid, base, w, cols, how, logY) {
        var out = new Float64Array(cols), cnt = new Int32Array(cols), c;
        if (how === "max") out.fill(-Infinity);
        else if (how === "min") out.fill(Infinity);
        for (c = 0; c < w; c++) {
            var v = grid[base + c];
            if (!isFinite(v) || (logY && v <= 0)) continue;
            var p = cols === w ? c : Math.floor(c * cols / w);
            if (how === "max") { if (v > out[p]) out[p] = v; }
            else if (how === "min") { if (v < out[p]) out[p] = v; }
            else out[p] += v;
            cnt[p]++;
        }
        for (c = 0; c < cols; c++) {
            if (!cnt[c]) out[c] = NaN;
            else if (how !== "max" && how !== "min") out[c] /= cnt[c];
        }
        return out;
    }

    // opts: how (column collapse), logY, colorOf(row), names, win.
    function drawLines(canvas, grid, h, w, opts) {
        opts = opts || {};
        var how = opts.how || "max", logY = !!opts.logY;
        var win = opts.win || { x0: 0, x1: w };
        var srcW = win.x1 - win.x0;
        var f = beginPlot(canvas);
        var r = f.rect;
        // One column per *device* pixel: on a 2× display the trace
        // carries twice the detail across the same rect.
        var cols = Math.max(2, Math.min(srcW, Math.round(r.w * f.dpr)));
        var rows = [], min = Infinity, max = -Infinity, i, c, v;
        for (i = 0; i < h; i++) {
            var row = collapseRow(grid, i * w + win.x0, srcW, cols, how, logY);
            rows.push(row);
            for (c = 0; c < cols; c++) {
                v = row[c];
                if (!isFinite(v)) continue;
                if (v < min) min = v;
                if (v > max) max = v;
            }
        }
        var empty = min > max;
        if (empty) { min = 0; max = 1; }
        var lo = logY ? Math.log10(min) : min;
        var hi = logY ? Math.log10(max) : max;
        if (lo === hi) { lo -= 1; hi += 1; }

        // Ticks are drawn against the *index* axis, the data against
        // collapsed columns; both span the rect edge to edge.
        var xIndexAt = function (idx) {
            return r.l + ((idx - win.x0) / Math.max(1, srcW - 1)) * r.w;
        };
        var yPos = function (t) { return r.t + (1 - (t - lo) / (hi - lo)) * r.h; };
        var xAt = function (col) {
            return r.l + (cols > 1 ? col / (cols - 1) : 0.5) * r.w;
        };
        var yAt = function (val) { return yPos(logY ? Math.log10(val) : val); };
        drawFrame(f, indexTicks(win.x0, win.x1, r.w),
                  valueTicks(lo, hi, r.h, logY), xIndexAt, yPos,
                  opts.names, true);

        var ctx = f.ctx;
        ctx.save();
        ctx.beginPath();
        ctx.rect(r.l - 1, r.t - 1, r.w + 2, r.h + 2);
        ctx.clip();
        ctx.lineWidth = 1;
        for (i = 0; i < h; i++) {
            var rw = rows[i];
            ctx.strokeStyle = ctx.fillStyle =
                opts.colorOf ? opts.colorOf(i) : THEME.line;
            ctx.beginPath();
            var prev = false;
            for (c = 0; c < cols; c++) {
                v = rw[c];
                if (!isFinite(v)) { prev = false; continue; }
                var px = xAt(c), py = yAt(v);
                if (prev) ctx.lineTo(px, py);
                else ctx.moveTo(px, py);
                // A valid sample with no valid neighbour has no segment
                // to draw: where the fed frequencies are scattered
                // rather than contiguous, breaking the trace at every
                // gap would otherwise render nothing at all.
                if (!prev && (c + 1 >= cols || !isFinite(rw[c + 1])))
                    ctx.fillRect(px - 1, py - 1, 2, 2);
                prev = true;
            }
            ctx.stroke();
        }
        ctx.restore();
        setMapping(f, { x0: win.x0, x1: win.x1, y0: 0, y1: 1 }, true,
                   { w: w, h: 1 });

        if (empty) return "nothing to draw (no valid values)";
        return "range " + fmtVal(min) + " … " + fmtVal(max) +
            (logY ? ", log y" : "") +
            (cols < srcW ? ", " + srcW + " → " + cols + " px (" + how + ")" : "");
    }

    // One row of everything: N2 blocks and buffers whose descriptor
    // carries no extents have no axes to compose.
    function drawSingleLine(canvas, values) {
        var win = resolveWindow(values.length, 1);
        return drawLines(canvas, values, 1, values.length,
                         { how: "max", logY: plot.controls.log.checked,
                           win: win, names: { x: "index" } }) +
               windowNote(win, values.length, 1);
    }

    // --- the panel ------------------------------------------------------

    var plot = null; // single open panel

    // The watched buffer is remembered per node for the browser session,
    // so revisiting the page reopens the live plot without a click.
    var STORE_PREFIX = "chocoBufferPlot:";
    // Per-buffer axis layout memory.
    var LAYOUT_PREFIX = "chocoBufferAxes:";

    function rememberOpen(nodeKey, buffer) {
        try { sessionStorage.setItem(STORE_PREFIX + nodeKey, buffer); } catch (e) {}
    }

    function forgetOpen(nodeKey) {
        try { sessionStorage.removeItem(STORE_PREFIX + nodeKey); } catch (e) {}
    }

    function savedOpen(nodeKey) {
        try { return sessionStorage.getItem(STORE_PREFIX + nodeKey); } catch (e) { return null; }
    }

    function closePlot() {
        if (!plot) return;
        clearInterval(plot.timer);
        clearTimeout(plot.resizeTimer);
        if (plot.observer) plot.observer.disconnect();
        document.removeEventListener("visibilitychange", plot.onVis);
        if (plot.onResize) window.removeEventListener("resize", plot.onResize);
        if (plot.themeObserver) plot.themeObserver.disconnect();
        if (plot.themeQuery && plot.themeQuery.removeEventListener)
            plot.themeQuery.removeEventListener("change", plot.onTheme);
        plot.root.remove();
        plot = null;
    }

    function setStatus(text) {
        if (plot) plot.status.textContent = text;
    }

    function dimLabel(i) {
        var name = plot.dimNames && plot.dimNames[i];
        var picked = plot.picks && plot.picks[i];
        return (name ? name : "dim" + i) + " (" +
               (picked ? picked.length + " of " + plot.dims[i] : plot.dims[i]) +
               ")";
    }

    function dimName(i) {
        var name = plot.dimNames && plot.dimNames[i];
        return name ? name : "dim" + i;
    }

    // --- index selection ---------------------------------------------------
    // A dimension can be restricted to some of its indices *before*
    // anything else happens to it.  This is deliberately separate from
    // the disposition: "which entries are in play" and "what becomes of
    // them" are independent questions, and keeping them apart is what
    // lets ``C@0+1`` be two lines rather than only ever one — pinning to
    // a single index is just the one-entry case of it, which is why
    // there is no longer an "at index" disposition at all.

    // The parsed form of plot.pick, recomputed whenever the specs change
    // (seriesLabel walks it per row, so it is not re-parsed per call).
    function syncPicks() {
        plot.picks = [];
        for (var d = 0; d < plot.dims.length; d++) {
            var spec = plot.pick[d];
            var idx = spec ? parseRanges(spec, plot.dims[d]) : [];
            // A spec that selects nothing (a typo, or a range entirely
            // past the extent) means the whole dimension, never an empty
            // one: a zero-extent axis has no plot to draw.
            plot.picks.push(idx.length && idx.length < plot.dims[d] ? idx : null);
        }
    }

    // The extent a dimension actually contributes, which is what every
    // axis and label has to count in.
    function effExtent(d) {
        var picked = plot.picks && plot.picks[d];
        return picked ? picked.length : plot.dims[d];
    }

    // Position along a (possibly restricted) dimension -> the index it
    // stands for in the original array, which is what a label must say.
    function origIndex(d, k) {
        var picked = plot.picks && plot.picks[d];
        return picked ? picked[k] : k;
    }

    // Dimensions of extent 1 carry no information: they are excluded
    // from the axis controls and left to the reduction, which is exactly
    // a squeeze.  Without this, `[Tc=1, F=384]` buffers default to a
    // one-row hairline — ten of cx19's buffers look like that.
    function usableDims() {
        var out = [];
        if (!plot.dims) return out;
        for (var i = 0; i < plot.dims.length; i++)
            if (plot.dims[i] > 1) out.push(i);
        return out;
    }

    // Composite axis label: "DPhi×DPlo1×DPlo2 (9216)".
    function axisLabel(list) {
        if (!list || !list.length) return "—";
        var n = 1, parts = [];
        for (var i = 0; i < list.length; i++) {
            parts.push(dimName(list[i]));
            n *= effExtent(list[i]);
        }
        return parts.join("×") + " (" + n + ")";
    }

    // --- series selection -------------------------------------------------
    // In lines mode the y axis is the *series* axis: every combination
    // of indices along its dimensions is one line, and each line can be
    // drawn or not.  That is also where index picking lives now — the
    // per-dimension index control was removed for reading as something
    // that shortened a dimension, but a list of lines you tick is
    // exactly what an index pick means once the entries are curves.

    var SERIES_DEFAULT_MAX = 32;  // lines drawn before defaulting to a subset
    var SERIES_LIST_MAX = 512;    // rows listed before deferring to the range box
    var MAX_LINES = 512;          // lines drawn before refusing outright

    function seriesCount() {
        var n = 1;
        for (var i = 0; i < plot.axisY.length; i++) n *= effExtent(plot.axisY[i]);
        return n;
    }

    // A row index decomposes into one index per composing dimension,
    // earlier-listed varying slowest — the same order the reduction
    // walks: "E=37", "P=1·D=19".
    function seriesLabel(row) {
        var list = plot.axisY, parts = [];
        for (var i = list.length - 1; i >= 0; i--) {
            var d = list[i], n = effExtent(d);
            parts.unshift(dimName(d) + "=" + origIndex(d, row % n));
            row = Math.floor(row / n);
        }
        return parts.join("·") || "all";
    }

    // Colour spans the *drawn* lines, not the whole axis: three lines
    // picked out of 9216 want three distinguishable colours, where
    // keying on the axis index would give them three neighbouring
    // shades of the same green.  The cost is that toggling recolours
    // the survivors — which is why the picker doubles as the legend and
    // carries the same swatches.
    function seriesColorAt(rank, count) {
        var c = viridis(count > 1 ? rank / (count - 1) : 0.5);
        return "rgb(" + Math.round(c[0]) + "," + Math.round(c[1]) + "," +
               Math.round(c[2]) + ")";
    }

    // Default selection: every line while that is still readable,
    // otherwise an evenly spaced subset.  The stride is forced odd
    // (`| 1` sets the low bit) because num_elements = 2 × num_dishes —
    // on a pol-fastest ([D][P], kotekan's CHORDEarly) element axis an
    // even stride samples one polarization and never shows the other.
    // The 2026-08 fiducial order is [P][D] (pol slowest), where any
    // stride below num_dishes crosses both blocks and the odd stride is
    // merely harmless; it stays for buffers still in the old order.
    function resetSeries() {
        var n = seriesCount(), i;
        plot.series = [];
        plot.seriesStride = n <= SERIES_DEFAULT_MAX
            ? 1 : (Math.round(n / SERIES_DEFAULT_MAX) | 1);
        for (i = 0; i < n; i += plot.seriesStride) plot.series.push(i);
    }

    // "0-7,64,100-110" — what the range box takes and the layout memory
    // stores.  Unparseable pieces are skipped rather than rejected: the
    // box is edited by hand, and half a valid selection beats none.
    // `+` is accepted alongside `,` because the URL form uses it: a
    // comma already separates the dimensions in a `dims=` spec, so the
    // list inside one of them needs its own separator.
    function parseRanges(text, n) {
        var out = [], seen = {};
        String(text).split(/[,+]/).forEach(function (part) {
            var m = /^\s*(\d+)\s*(?:-\s*(\d+)\s*)?$/.exec(part);
            if (!m) return;
            var a = +m[1], b = m[2] === undefined ? a : +m[2];
            for (var i = Math.min(a, b); i <= Math.max(a, b) && i < n; i++)
                if (!seen[i]) { seen[i] = 1; out.push(i); }
        });
        return out.sort(function (x, y) { return x - y; });
    }

    function formatRanges(sel) {
        var out = [], i = 0;
        while (i < sel.length) {
            var j = i;
            while (j + 1 < sel.length && sel[j + 1] === sel[j] + 1) j++;
            out.push(i === j ? String(sel[i]) : sel[i] + "-" + sel[j]);
            i = j + 1;
        }
        return out.join(",");
    }

    // No DOM rebuild: the rows are repainted in place, which keeps the
    // operator's scroll position halfway down a 512-row list and is
    // also what keeps the legend honest — since colour now spans the
    // drawn set, ticking one line changes every other line's swatch.
    function setSeries(sel) {
        plot.series = sel.slice().sort(function (a, b) { return a - b; });
        plot.seriesStride = 0;  // no longer the default spacing
        updateSeriesSummary();
        rerender();  // persistView() rides on the render
    }

    // Repaint the listed rows from the current selection: which are
    // ticked, what colour each drawn line is, and which are greyed out
    // because they are not drawn at all.
    function paintSeriesRows() {
        var pick = plot.controls.seriesPick;
        if (!pick.rows) return;
        var ranks = seriesRanks(), count = plot.series.length;
        Object.keys(pick.rows).forEach(function (key) {
            var row = pick.rows[key], drawn = ranks[key] !== undefined;
            row.box.checked = drawn;
            row.swatch.style.background = drawn
                ? seriesColorAt(ranks[key], count) : "rgba(128,128,128,0.35)";
            row.text.style.opacity = drawn ? "" : "0.55";
        });
    }

    function updateSeriesSummary() {
        var pick = plot.controls.seriesPick;
        paintSeriesRows();
        pick.summary.textContent =
            "series: " + plot.series.length + " of " + seriesCount();
        // Never while it is being typed into: this also runs off the 5 s
        // poll, and rewriting the box mid-edit would eat the selection
        // the operator is halfway through spelling out.
        if (pick.rangeBox && document.activeElement !== pick.rangeBox)
            pick.rangeBox.value = formatRanges(plot.series);
    }

    // Membership as a lookup, built once per pass: the selection runs to
    // the length of the series axis, and an indexOf per listed row would
    // be 512 × that on every rebuild.
    function seriesRanks() {
        var s = {};
        for (var i = 0; i < plot.series.length; i++) s[plot.series[i]] = i;
        return s;
    }

    function seriesRow(body, row, ranks, count) {
        var label = el("label", {
            style: "display: flex; align-items: center; gap: 0.4em; " +
                   "margin: 0 0 0.2em 0; white-space: nowrap; font-weight: normal;",
        });
        var drawn = ranks[row] !== undefined;
        var box = el("input", { type: "checkbox", style: "margin: 0;" });
        box.checked = drawn;
        box.addEventListener("change", function () {
            var sel = plot.series.slice(), at = sel.indexOf(row);
            if (box.checked && at < 0) sel.push(row);
            else if (!box.checked && at >= 0) sel.splice(at, 1);
            setSeries(sel);
        });
        label.appendChild(box);
        // The swatch is the legend: the colour a line is actually
        // drawn in, or a flat grey when it is not drawn at all.
        label.appendChild(el("span", {
            style: "width: 0.85em; height: 0.85em; border-radius: 2px; " +
                   "flex: none; background: " +
                   (drawn ? seriesColorAt(ranks[row], count)
                          : "rgba(128,128,128,0.35)") + ";",
        }));
        var text = el("span", { style: drawn ? "" : "opacity: 0.55;" },
                      seriesLabel(row));
        label.appendChild(text);
        body.appendChild(label);
        return { box: box, swatch: label.children[1], text: text };
    }

    function renderSeriesPicker() {
        var pick = plot.controls.seriesPick, n = seriesCount(), i;
        var ranks = seriesRanks(), drawnCount = plot.series.length;
        pick.body.textContent = "";
        var bar = el("div", {
            style: "display: flex; gap: 0.3em; margin-bottom: 0.4em;",
        });
        [["all", function () { var s = []; for (var k = 0; k < n; k++) s.push(k); return s; }],
         ["none", function () { return []; }],
         ["invert", function () {
             var s = [];
             for (var k = 0; k < n; k++)
                 if (ranks[k] === undefined) s.push(k);
             return s;
         }]].forEach(function (b) {
            var btn = el("button", {
                type: "button", class: "outline",
                style: "width: auto; margin: 0; padding: 0 0.5em; font-size: 0.9em;",
            }, b[0]);
            btn.addEventListener("click", function () { setSeries(b[1]()); });
            bar.appendChild(btn);
        });
        pick.body.appendChild(bar);
        var box = el("input", {
            type: "text", placeholder: "0-7,64,100-110",
            style: "margin: 0 0 0.4em 0; padding: 0.1em 0.4em; font-size: 1em;",
        });
        box.value = formatRanges(plot.series);
        box.addEventListener("change", function () {
            setSeries(parseRanges(box.value, n));
        });
        pick.rangeBox = box;
        pick.body.appendChild(box);
        pick.rows = {};
        for (i = 0; i < Math.min(n, SERIES_LIST_MAX); i++)
            pick.rows[i] = seriesRow(pick.body, i, ranks, drawnCount);
        if (n > SERIES_LIST_MAX)
            pick.body.appendChild(el("small", {}, "… " + (n - SERIES_LIST_MAX) +
                                   " more — use the range box"));
        updateSeriesSummary();
    }

    // A dropdown of checkboxes: <details> gives the disclosure for free,
    // and the panel floats over the canvas rather than reflowing it.
    function checkboxDropdown(name) {
        var det = el("details", { style: "position: relative; margin: 0;" });
        // text-align: left overrides pico's right-aligned summary; its
        // ::after chevron still floats right, which reads as a dropdown.
        var sum = el("summary", {
            style: "cursor: pointer; list-style: none; padding: 0.1em 0.5em; " +
                   "border: 1px solid rgba(128,128,128,0.4); border-radius: 4px; " +
                   "white-space: nowrap; text-align: left;",
        }, name + ": —");
        var body = el("div", {
            style: "position: absolute; z-index: 60; top: 1.9em; left: 0; " +
                   "min-width: 11em; max-height: 14em; overflow: auto; " +
                   "padding: 0.4em 0.6em; border-radius: 6px; " +
                   "border: 1px solid rgba(128,128,128,0.4); " +
                   "background: var(--pico-card-background-color, " +
                   "var(--card-background-color, #fff));",
        });
        det.appendChild(sum);
        det.appendChild(body);
        return { root: det, summary: sum, body: body, name: name };
    }

    // The renderer follows from how many axes the dimension table leaves
    // occupied — the table always assigns every dimension, so there is
    // no "still 3-d" state to resolve.  Two axes is the one genuinely
    // ambiguous case (a grid of cells or a family of curves), so that is
    // the only choice `mode` still makes; histogram overrides all of it,
    // being a distribution over values rather than over axes.
    function effectiveMode() {
        var m = plot.controls.mode.value;
        if (m !== "auto") return m;
        if (!plot.dims) return "histogram";  // no extents: N2, unknown types
        var axes = (plot.axisY.length ? 1 : 0) + (plot.axisX.length ? 1 : 0);
        if (axes >= 2) return "heatmap";
        if (axes === 1) return "lines";
        return "scalar";
    }

    // Exact zero only, never an epsilon: the option exists because
    // unfed frequencies read as a true 0, and a threshold would start
    // eating small real values instead.
    function blankZeros() {
        return plot.controls.zeros.value === "hide";
    }

    // --- zoom -------------------------------------------------------------
    // The window is held in *axis index* coordinates rather than as a
    // canvas transform: narrowing the data window means fewer samples
    // share each pixel, so zooming in resolves real detail instead of
    // magnifying blur, and the window means the same thing whatever the
    // canvas size — which is also what makes it serialisable.

    function clampIndex(v, lo, hi, dflt) {
        if (v == null || !isFinite(v)) return dflt;
        return Math.max(lo, Math.min(hi, Math.round(v)));
    }

    function resolveWindow(w, h) {
        var z = plot.zoom;
        var x0 = clampIndex(z.x0, 0, Math.max(0, w - 1), 0);
        var x1 = clampIndex(z.x1, x0 + 1, w, w);
        var y0 = clampIndex(z.y0, 0, Math.max(0, h - 1), 0);
        var y1 = clampIndex(z.y1, y0 + 1, h, h);
        return { x0: x0, x1: x1, y0: y0, y1: y1 };
    }

    function resetZoom() {
        plot.zoom = { x0: null, x1: null, y0: null, y1: null };
    }

    function windowNote(win, w, h) {
        if (win.x0 === 0 && win.x1 === w && win.y0 === 0 && win.y1 === h)
            return "";
        return ", zoom x " + win.x0 + "–" + (win.x1 - 1) +
            (h > 1 && (win.y0 || win.y1 < h)
                ? " y " + win.y0 + "–" + (win.y1 - 1) : "");
    }

    // What the last render put where, so a pointer event can be turned
    // back into data indices.  `yIsValue` marks a y axis that is not an
    // index (a line plot's values), which auto-ranges and so is never
    // zoomed directly.
    function setMapping(f, win, yIsValue, full) {
        plot.view = { rect: f.rect, win: win, yIsValue: yIsValue };
        plot.fullExtent = full || null;
    }

    // Zoom one axis about the cursor, keeping the anchored index under
    // the pointer and the window inside the axis.
    function zoomAxis(a0, a1, frac, factor, limit) {
        var span = Math.max(2, Math.min(limit, Math.round((a1 - a0) * factor)));
        var lo = Math.round(a0 + frac * (a1 - a0) - frac * span);
        lo = Math.max(0, Math.min(limit - span, lo));
        return [lo, lo + span];
    }

    function canvasFraction(e) {
        var v = plot.view;
        if (!v) return null;
        var box = plot.canvas.getBoundingClientRect();
        var r = v.rect;
        var fx = (e.clientX - box.left - r.l) / r.w;
        var fy = (e.clientY - box.top - r.t) / r.h;
        if (fx < 0 || fx > 1 || fy < 0 || fy > 1) return null;
        return { fx: fx, fy: fy };
    }

    // Wheel zooms about the cursor and drag pans — the gestures the
    // pipeline graph already uses.  shift+wheel is left to the browser
    // as the escape hatch for scrolling the page underneath.
    function onWheel(e) {
        var at = canvasFraction(e), v = plot.view;
        if (!at || e.shiftKey || !v || !v.win) return;
        e.preventDefault();
        var factor = e.deltaY > 0 ? 1.25 : 0.8;
        var full = plot.fullExtent || { w: v.win.x1, h: v.win.y1 };
        var x = zoomAxis(v.win.x0, v.win.x1, at.fx, factor, full.w);
        plot.zoom.x0 = x[0];
        plot.zoom.x1 = x[1];
        if (!v.yIsValue && full.h > 1) {
            var y = zoomAxis(v.win.y0, v.win.y1, at.fy, factor, full.h);
            plot.zoom.y0 = y[0];
            plot.zoom.y1 = y[1];
        }
        rerender();
    }

    function onDragStart(e) {
        var v = plot.view;
        if (!v || !v.win || e.button !== 0) return;
        var start = { x: e.clientX, y: e.clientY, win: v.win, rect: v.rect,
                      yIsValue: v.yIsValue };
        var moved = false;
        var move = function (ev) {
            var full = plot.fullExtent || { w: start.win.x1, h: start.win.y1 };
            var spanX = start.win.x1 - start.win.x0;
            var dx = Math.round(-(ev.clientX - start.x) / start.rect.w * spanX);
            if (!moved && Math.abs(ev.clientX - start.x) < 3 &&
                Math.abs(ev.clientY - start.y) < 3) return;
            moved = true;
            var x0 = Math.max(0, Math.min(full.w - spanX, start.win.x0 + dx));
            plot.zoom.x0 = x0;
            plot.zoom.x1 = x0 + spanX;
            if (!start.yIsValue && full.h > 1) {
                var spanY = start.win.y1 - start.win.y0;
                var dy = Math.round(-(ev.clientY - start.y) / start.rect.h * spanY);
                var y0 = Math.max(0, Math.min(full.h - spanY, start.win.y0 + dy));
                plot.zoom.y0 = y0;
                plot.zoom.y1 = y0 + spanY;
            }
            rerender();
        };
        var up = function () {
            document.removeEventListener("mousemove", move);
            document.removeEventListener("mouseup", up);
        };
        document.addEventListener("mousemove", move);
        document.addEventListener("mouseup", up);
    }

    // --- the view as data -------------------------------------------------
    // One record *is* the view, and both backends read and write it: the
    // per-buffer sessionStorage memory the overlay uses, and the URL
    // fragment the full-screen page keeps in step.  A state that one can
    // express and the other can't is therefore not representable.
    //
    // Dimensions are keyed by *name* (``F:x,E:y,T:mean,C:at3``) for the
    // same reason the storage key is: the leading extent moves with the
    // fetch size, and a positional list would mean something different
    // after a resize.

    var VIEW_DEFAULTS = {
        dims: "", mode: "auto", px: "max", series: "", zeros: "show",
        log: "0", fetch: String(DEFAULT_FETCH), bits: "axis", zoom: "",
    };

    function zoomSpec() {
        var z = plot.zoom;
        if (z.x0 == null || z.x1 == null) return "";
        var s = z.x0 + ":" + z.x1;
        if (z.y0 != null && z.y1 != null) s += "," + z.y0 + ":" + z.y1;
        return s;
    }

    function viewState() {
        var dims = [], i;
        for (i = 0; i < plot.disp.length; i++)
            dims.push(dimName(i) + ":" + plot.disp[i] +
                      (plot.pick[i] ? "@" + plot.pick[i].replace(/,/g, "+") : ""));
        return {
            dims: plot.dims ? dims.join(",") : "",
            mode: plot.controls.mode.value,
            px: plot.controls.combine.value,
            // Only an operator's own selection is worth carrying: while
            // it is still the strided default (seriesStride > 0) the
            // other end recomputes the same thing, and spelling out 26
            // indices would dominate the URL for no information.
            series: plot.seriesStride ? "" : formatRanges(plot.series),
            zeros: plot.controls.zeros.value,
            log: plot.controls.log.checked ? "1" : "0",
            fetch: plot.controls.fetchLen.value,
            bits: plot.bitsMode,
            zoom: zoomSpec(),
        };
    }

    // These four are legal in a fragment and are what carries the
    // structure — dimension:disposition, dimension list, @pick, pick
    // list — so they stay readable; everything else is percent-encoded.
    var VIEW_LITERALS = { "%3A": ":", "%2C": ",", "%40": "@", "%2B": "+" };

    function encView(s) {
        return encodeURIComponent(s).replace(/%3A|%2C|%40|%2B/g, function (m) {
            return VIEW_LITERALS[m];
        });
    }

    function serializeView() {
        var v = viewState(), parts = [], k;
        for (k in v)
            if (v[k] !== "" && v[k] !== VIEW_DEFAULTS[k])
                parts.push(k + "=" + encView(v[k]));
        return parts.join("&");
    }

    function parseView(text) {
        var out = {};
        String(text || "").replace(/^[#?]/, "").split("&").forEach(function (part) {
            if (!part) return;
            var eq = part.indexOf("=");
            if (eq < 1) return;
            try {
                out[decodeURIComponent(part.slice(0, eq))] =
                    decodeURIComponent(part.slice(eq + 1));
            } catch (e) {}  // a hand-edited fragment shouldn't blank the page
        });
        return out;
    }

    function setSelectIfValid(sel, value) {
        if (value == null) return;
        for (var i = 0; i < sel.options.length; i++)
            if (sel.options[i].value === value) { sel.value = value; return; }
    }

    // "F:x,E:y,T:mean,C:at3" — names not present in the current shape
    // are ignored rather than rejected, so a spec written for a sibling
    // buffer still sets what it can.
    function applyDimSpec(spec) {
        spec.split(",").forEach(function (part) {
            // "F:x", "C:y@0+1", "T:mean@0-99" — and "C:at3" from a view
            // written before the pick and the disposition came apart.
            var m = /^([^:]+):(x|y|max|mean|min|at\d+)(?:@([\d+,\-]+))?$/.exec(part);
            if (!m) return;
            for (var d = 0; d < plot.dims.length; d++) {
                if (dimName(d) !== m[1]) continue;
                var legacy = /^at(\d+)$/.exec(m[2]);
                plot.disp[d] = legacy ? "max" : m[2];
                plot.pick[d] = legacy ? legacy[1] : (m[3] || "").replace(/\+/g, ",");
                return;
            }
        });
        syncPicks();
        syncAxes();
    }

    function applyZoomSpec(spec) {
        resetZoom();
        if (!spec) return;
        var axes = spec.split(",");
        var xy = /^(\d+):(\d+)$/.exec(axes[0]);
        if (!xy) return;
        plot.zoom.x0 = +xy[1];
        plot.zoom.x1 = +xy[2];
        var yy = axes[1] && /^(\d+):(\d+)$/.exec(axes[1]);
        if (yy) {
            plot.zoom.y0 = +yy[1];
            plot.zoom.y1 = +yy[2];
        }
    }

    // Idempotent, and safe before the shape is known: the dimension
    // parts are skipped until there are dimensions to name.  That is
    // what lets it run once at panel build (for the controls that decide
    // *what to fetch*) and again once the first frame has been shaped.
    function applyViewState(v) {
        if (!v) return;
        setSelectIfValid(plot.controls.mode, v.mode);
        setSelectIfValid(plot.controls.combine, v.px);
        setSelectIfValid(plot.controls.zeros, v.zeros);
        setSelectIfValid(plot.controls.fetchLen, v.fetch);
        setSelectIfValid(plot.controls.bits, v.bits);
        plot.bitsMode = plot.controls.bits.value;
        if (v.log != null) plot.controls.log.checked = v.log === "1";
        if (!plot.dims) return;
        if (v.dims) applyDimSpec(v.dims);
        resetSeries();  // the dim spec may have recomposed the series axis
        if (v.series) {
            var sel = parseRanges(v.series, seriesCount());
            if (sel.length) { plot.series = sel; plot.seriesStride = 0; }
        }
        applyZoomSpec(v.zoom);
    }

    function storedView() {
        try {
            var raw = sessionStorage.getItem(layoutKey());
            return raw ? parseView(raw) : null;
        } catch (e) { return null; }
    }

    // Both backends, driven from one place: called after every render,
    // it writes only when the view actually changed, so the 5 s poll
    // costs nothing and every control gets persistence for free instead
    // of each handler remembering to ask for it.
    function persistView() {
        if (!plot) return;
        var s = serializeView();
        if (s === plot.lastViewSig) return;
        plot.lastViewSig = s;
        // The link and the URL work for any frame; only the storage key
        // needs a shape to be keyed by (N2 frames have no dimensions).
        if (plot.fullLink && plot.source.page)
            plot.fullLink.setAttribute("href",
                plot.source.page + (s ? "#" + s : ""));
        if (plot.urlState && typeof history.replaceState === "function")
            history.replaceState(null, "",
                location.pathname + location.search + (s ? "#" + s : ""));
        if (!plot.dims) return;
        try { sessionStorage.setItem(layoutKey(), s); } catch (e) {}
    }

    // The array as the dimension table leaves it: folded down to the
    // axis dimensions, with the axis lists remapped onto the new shape.
    function foldedView() {
        // A shape change always goes through rebuildDimControls first,
        // which re-derives the table — but a disposition list that is
        // out of step with the dimensions would fold with an unknown op
        // and silently produce a wrong plot, so it is checked, not
        // assumed.
        if (plot.disp.length !== plot.dims.length ||
            plot.picks.length !== plot.dims.length) defaultDisp();
        var p = filterDims(plot.lastValues, plot.dims, plot.picks);
        var f = foldDims(p.values, p.dims, plot.dimNames,
                         plot.disp, blankZeros());
        var remap = function (d) { return f.map[d]; };
        return { values: f.values, dims: f.dims, names: f.names,
                 y: plot.axisY.map(remap), x: plot.axisX.map(remap) };
    }

    // Every fold that can turn integers into fractions; `mean` is the
    // only one, and the histogram bins integers one-per-value.
    function foldsKeepIntegers() {
        for (var i = 0; i < plot.disp.length; i++)
            if (plot.disp[i] === "mean") return false;
        return true;
    }

    // Lines mode: compose only the selected series, then stroke the rows.
    function renderLines(view) {
        var n = seriesCount(), i;
        var vis = [];
        for (i = 0; i < plot.series.length; i++)
            if (plot.series[i] < n) vis.push(plot.series[i]);
        if (!vis.length) return "no series selected";
        // One "all" click on a large series axis would otherwise ask for
        // a grid of rows × x values — gigabytes for a pair axis.  Say so
        // and draw nothing rather than quietly plotting a subset: past a
        // few hundred curves the plot is unreadable anyway.
        if (vis.length > MAX_LINES)
            return vis.length + " series selected — at most " + MAX_LINES +
                   " can be drawn; narrow the selection";
        var map = new Int32Array(n).fill(-1);
        for (i = 0; i < vis.length; i++) map[vis[i]] = i;
        var how = plot.controls.combine.value;
        var r = reduceToGrid(view.values, view.dims, view.y, view.x,
                             how, { blankZeros: blankZeros(), rowMap: map,
                                    nRows: vis.length });
        // Only x is windowed here: the rows *are* the series axis, and
        // choosing which of those to draw is the series list's job.
        var win = resolveWindow(r.w, 1);
        return drawLines(plot.canvas, r.grid, r.h, r.w, {
            how: how, logY: plot.controls.log.checked, win: win,
            names: { x: axisLabel(plot.axisX), y: "value" },
            colorOf: function (row) { return seriesColorAt(row, vis.length); },
        }) + windowNote(win, r.w, 1);
    }

    // Everything folded away: one number, which is a legitimate readout
    // ("mean over the whole frame") rather than an error state.
    function renderScalar(view) {
        var f = beginPlot(plot.canvas);
        var v = view.values.length ? view.values[0] : NaN;
        var ctx = f.ctx;
        ctx.fillStyle = THEME.ink;
        ctx.font = "28px monospace";
        ctx.textAlign = "center";
        ctx.fillText(fmtVal(v), f.W / 2, f.H / 2 + 10);
        ctx.textAlign = "left";
        ctx.font = "11px monospace";
        setMapping(f, null, false, null);
        return "every dimension folded — one value";
    }

    function rerender() {
        if (!plot || !plot.lastValues) return;
        if (plot.n2 && plot.lastBuf) {
            var n2note = renderN2(plot.controls.mode.value);
            setStatus("frame " + plot.frameId + ", " + fmtBytes(plot.lastBytes) +
                      (plot.frameSize ? " of " + fmtBytes(plot.frameSize) : "") +
                      " — " + n2note + staleSuffix());
            showZoomReset();  // an N2 block plotted as lines zooms too
            persistView();    // ...and its view is just as shareable
            return;
        }
        var mode = effectiveMode();
        // Every mode below reads the folded array, so what the dimension
        // table says happens to a dimension happens once, here, whatever
        // is drawn afterwards.
        var view = plot.dims ? foldedView() : null;
        var note;
        if (mode === "heatmap" && view) {
            if (!plot.axisY.length || !plot.axisX.length) {
                setStatus("set a dimension to each of the y and x axes");
                return;
            }
            var opts = { blankZeros: blankZeros(),
                         log: plot.controls.log.checked,
                         names: { x: axisLabel(plot.axisX),
                                  y: axisLabel(plot.axisY) } };
            if (plot.dec.mask) { opts.range = [0, 1]; opts.color = maskColor; }
            note = renderHeatmap(plot.canvas, view.values, view.dims,
                                 view.y, view.x,
                                 plot.controls.combine.value, opts);
            note = "y: " + axisLabel(plot.axisY) + ", x: " + axisLabel(plot.axisX) +
                   " — " + note;
        } else if (mode === "lines" && view) {
            if (!plot.axisX.length) {
                setStatus("set a dimension to the x axis");
                return;
            }
            note = "series: " + axisLabel(plot.axisY) + " — " +
                   plot.series.length + " of " + seriesCount() + " shown" +
                   (plot.seriesStride > 1 ? ", stride " + plot.seriesStride : "") +
                   ", x: " + axisLabel(plot.axisX) + " — " + renderLines(view);
        } else if (mode === "scalar" && view) {
            note = renderScalar(view);
        } else if (mode === "lines") {
            note = drawSingleLine(plot.canvas, plot.lastValues);
        } else {
            note = renderHistogram(plot.canvas,
                                   view ? view.values : plot.lastValues,
                                   plot.dec.integer &&
                                       (!view || foldsKeepIntegers()),
                                   plot.controls.log.checked, blankZeros());
        }
        var got = plot.lastBytes;
        var frameText = "frame " + plot.frameId + ", " + fmtBytes(got) +
            (plot.frameSize ? " of " + fmtBytes(plot.frameSize) : "");
        if (plot.shapeNote)
            frameText += ", " + plot.shapeNote;
        if (plot.descNote)
            frameText += ", " + plot.descNote;
        // For a mask the single most useful number is how much of it is
        // set, over everything fetched — and the polarity has to be
        // spelled out, since "100% set" is the *healthy* reading.
        if (plot.dec.mask && plot.maskFrac !== null &&
            plot.maskFrac !== undefined)
            frameText += ", " + (100 * plot.maskFrac).toFixed(2) +
                         "% set (1 = good)";
        // The discoverability half of the zeros control: an operator who
        // sees "91.4% zero" knows the switch is worth flipping, without
        // the plot ever deciding that for them.
        if (plot.zeroFrac)
            frameText += ", " + (100 * plot.zeroFrac).toFixed(1) + "% zero" +
                         (blankZeros() ? " (blanked)" : "");
        setStatus(frameText + " — " + note + staleSuffix());
        showZoomReset();
        persistView();
    }

    function staleSuffix() {
        if (!plot || !plot.staleSince) return "";
        return " — no new frame since " +
            plot.staleSince.toLocaleTimeString();
    }

    // Decode one N2 block from the raw frame bytes (block offsets are
    // all 4-byte aligned, so typed-array views work directly).
    function n2BlockValues(buf, key, part) {
        var b = plot.n2.blocks[key];
        if (!b || b.bytes === 0 || buf.byteLength < b.off + b.bytes) return null;
        if (b.type === "c64")
            return complexPart(new Float32Array(buf, b.off, b.bytes / 4),
                               b.bytes / 8, part);
        if (b.type === "f32") return new Float32Array(buf, b.off, b.bytes / 4);
        if (b.type === "i32") return new Int32Array(buf, b.off, b.bytes / 4);
        return new Uint8Array(buf, b.off, b.bytes);
    }

    function n2Scalars(buf) {
        var e = n2BlockValues(buf, "erms", null);
        var c = n2BlockValues(buf, "chi2", null);
        if (!e || !c) return "";
        return " — erms " + fmtVal(e[0]) + ", χ²(XX,XY,YY) " +
            fmtVal(c[0]) + "/" + fmtVal(c[1]) + "/" + fmtVal(c[2]);
    }

    // Ticks for a compact-element axis labelled through input_list.
    // The positions stay compact — that is the geometry of the grid —
    // and only the labels speak fiducial, so a label is always the
    // input truly at that spot: compact index 20 silently reading as
    // input 20 (it is 68 on the pathfinder subset) is the lie the list
    // exists to prevent.  But a tick sequence chosen over compact
    // positions and merely relabelled reads 0, 5, 10, 15, 68, 73, 78:
    // odd numbers after the break, and the break itself falling
    // between two ticks.  So the axis is ticked per contiguous run of
    // the list (0-15, then 64-79): a mandatory tick at every run's
    // first element, so the jump is drawn at the cell where it happens,
    // plus nice round *fiducial* values inside the run mapped back to
    // their compact positions; a round tick that would crowd a boundary
    // label yields to it.  Runs are shorter than the axis, so the
    // spacing is tighter than a plain index axis's.  Without a list
    // this is that plain index axis.
    function inputTicks(inputs, n, pixels) {
        if (!inputs) return indexTicks(0, n, pixels);
        var spacing = 60, minGap = spacing / 2, px = pixels / n;
        var runs = [], i = 0;
        while (i < n) {
            var j = i;
            while (j + 1 < n && inputs[j + 1] === inputs[j] + 1) j++;
            runs.push([i, j]);
            i = j + 1;
        }
        var bounds = runs.map(function (r) { return r[0]; });
        var crowded = function (v) {
            return bounds.some(function (b) {
                return Math.abs(b - v) * px < minGap;
            });
        };
        var out = [];
        runs.forEach(function (r) {
            var i0 = r[0], i1 = r[1], v0 = inputs[i0];
            out.push({ v: i0, text: String(v0) });
            var target = Math.max(1, Math.round((i1 - i0 + 1) * px / spacing));
            niceTicks(v0, inputs[i1], target, true).forEach(function (val) {
                var v = i0 + (val - v0);
                if (v <= i0 || v > i1 || crowded(v)) return;
                out.push({ v: v, text: String(val) });
            });
        });
        return out;
    }

    // An N2 block has a fixed shape, so it gets the frame and its index
    // ticks but none of the dimension machinery.  The log toggle still
    // applies — as a log colour scale, which is what lets the crosses
    // show against a diagonal sitting decades above them.
    function drawN2Grid(grid, h, w, names, xInputs, yInputs) {
        var f = beginPlot(plot.canvas);
        var note = drawGrid(f, grid, h, w,
                            { log: plot.controls.log.checked });
        var xAt = function (v) {
            return f.rect.l + ((v + 0.5) / w) * f.rect.w;
        };
        var yAt = function (v) {
            return f.rect.t + ((v + 0.5) / h) * f.rect.h;
        };
        drawFrame(f, inputTicks(xInputs, w, f.rect.w),
                  inputTicks(yInputs, h, f.rect.h),
                  xAt, yAt, names, false);
        setMapping(f, null, false, null);
        return note;
    }

    function renderN2(mode) {
        var n2 = plot.n2, buf = plot.lastBuf;
        var key = plot.controls.block.value || "vis";
        var b = n2.blocks[key];
        var part = plot.controls.part.value;
        var vals = n2BlockValues(buf, key, part);
        if (!vals || !b)
            return "block '" + key + "' not in received bytes";
        var label = key + (b.type === "c64" ? "(" + part + ")" : "");
        var note;
        if (mode === "histogram")
            note = renderHistogram(plot.canvas, vals,
                                   b.type === "u8" || b.type === "i32",
                                   plot.controls.log.checked);
        else if (mode === "lines")
            note = drawSingleLine(plot.canvas, vals);
        else if ((key === "vis" || key === "weight") &&
                 n2.layout !== "Autocorrelations")
            note = drawN2Grid(n2Grid(vals, n2.n, b.type === "c64" ? part : null,
                                     n2.prods),
                              n2.n, n2.n, { x: "input", y: "input" },
                              n2.inputs, n2.inputs) +
                   ", " + n2.n + "×" + n2.n + " inputs" +
                   (n2.prods ? " (" + n2.numProd + " products)" : "");
        else if (key === "evec")
            note = drawN2Grid(vals, n2.nev, n2.n, { x: "input", y: "ev" },
                              n2.inputs, null) +
                   ", ev × input";
        else
            note = drawSingleLine(plot.canvas, vals);
        // A compact subset frame names the fiducial inputs it actually
        // correlates — reported for every block, since the per-element
        // lines (flags, gain, mask) index compactly too.
        var subset = n2.inputs ? " — inputs " + formatRanges(n2.inputs) : "";
        return label + " — " + note + n2Scalars(buf) + subset;
    }

    // Build decoded dims from the descriptor extents and the bytes
    // actually received.  A C-order byte prefix truncates the leading
    // dimension — and when it cuts *inside* one leading slice (e.g. the
    // correlation buffer's extents [1, 384, 36, 16, 16, 2]: one Tc slice
    // is the whole 28 MB frame), the prefix is still a whole-slice
    // prefix one level down, so descend until at least one complete
    // slice of the remaining inner dims fits.
    function shapeReceived(byteLength) {
        plot.dims = null;
        plot.dimNames = null;
        plot.shapeNote = null;
        var ext = plot.extents;
        if (!ext || !ext.length) return;
        var allNames = plot.descDimNames || [];
        for (var k = 0; k < ext.length; k++) {
            var inner = 1;
            for (var i = k + 1; i < ext.length; i++) inner *= ext[i];
            var nLead = Math.floor(byteLength / (inner * plot.dec.bytes));
            if (nLead < 1) continue;
            var lead = Math.min(nLead, ext[k]);
            var dims = [lead].concat(ext.slice(k + 1));
            var names = allNames.slice(k, ext.length);
            if (plot.dec.sub > 1) {
                // Packed values are always the fastest-varying axis, so
                // they extend the *last* extent.  The default is to keep
                // them as a dimension of their own: the 8 bits are then
                // pickable as an axis, and the packing is visible rather
                // than folded into a neighbour.  "merge into last axis"
                // is the other reading, and for a boolean mask it is a
                // plain C-order reshape back to the logical array —
                // kotekan's own dim names say so: pl_mask_exp is
                // [Thi64, F, P, D8, Tlo64=8] × 8 bits = 64 time samples
                // ("lo64"), and 128 × 8 × 8 = 8192 = samples_per_data_set.
                // Component packing (int4x2's re/im) is never merged —
                // interleaving components into a dish axis would be a
                // lie about what the axis is.
                if (plot.dec.mask && plot.bitsMode === "merge") {
                    dims[dims.length - 1] *= plot.dec.sub;
                    if (names.length === dims.length)
                        names[names.length - 1] =
                            (names[names.length - 1] || "dim") + "×" + plot.dec.sub;
                } else if (!(plot.dec.complex && derivesPart())) {
                    // A derived part (magnitude, phase) turns each pair
                    // into one number, so the component dimension is
                    // gone by the time the axes are composed.
                    dims.push(plot.dec.sub);
                    names = names.concat([plot.dec.subName]);
                }
            }
            plot.dims = dims;
            plot.dimNames = names;
            if (lead < ext[k] || k > 0)
                plot.shapeNote = lead + "/" + ext[k] + " " +
                    (allNames[k] || "dim" + k) + " slices" +
                    (k > 0 ? " (within one " + (allNames[k - 1] || "outer") + ")" : "");
            return;
        }
    }

    // The axis layout is remembered per node+buffer for the browsing
    // session, keyed by the dimension *names* — the leading extent moves
    // with the fetch size, so keying on extents would throw the layout
    // away every time the fetch length changed.
    function layoutKey() {
        var names = [];
        for (var i = 0; i < plot.dims.length; i++) names.push(dimName(i));
        return LAYOUT_PREFIX + plot.source.id + ":" + names.join(",");
    }
    // plot.disp holds one disposition per dimension and is the single
    // source of truth; axisY/axisX are derived from it, in dimension
    // order, which is what makes the earlier-listed dimension of a
    // composite axis the slowest-varying one.
    function syncAxes() {
        plot.axisY = [];
        plot.axisX = [];
        for (var i = 0; i < plot.dims.length; i++) {
            if (plot.disp[i] === "y") plot.axisY.push(i);
            else if (plot.disp[i] === "x") plot.axisX.push(i);
        }
    }

    // Defaults: the two largest usable dimensions take the axes, the
    // *largest on x* — the canvas is wider than it is tall in both
    // contexts, so the longer dimension gets more pixels (and fewer of
    // its entries collapsed into one) lying along x, which also lands
    // frequency-on-x/time-on-y the way a waterfall is read.
    // Everything else folds, with the same op the pixel rule defaults to
    // (max, or min for a 1 = good mask), so the defaults are what the
    // single global combine used to give.
    function defaultDisp() {
        var use = usableDims().slice(), i;
        use.sort(function (a, b) { return plot.dims[b] - plot.dims[a]; });
        plot.disp = [];
        plot.pick = [];
        for (i = 0; i < plot.dims.length; i++) {
            plot.disp.push(plot.dec.mask ? "min" : "max");
            plot.pick.push("");
        }
        if (use.length) plot.disp[use[0]] = "x";
        if (use.length > 1) plot.disp[use[1]] = "y";
        syncPicks();
        syncAxes();
    }

    // Rebuild the dimension controls for the current shape.  A shape
    // whose dimension names are unchanged keeps the operator's layout
    // (only the labels are refreshed); a genuinely new shape starts from
    // the defaults and then takes whatever the remembered view — the URL
    // fragment on the full-screen page, sessionStorage otherwise — has
    // to say about it.
    function rebuildDimControls() {
        if (!plot.dims) {
            updateControls();
            return;
        }
        var key = layoutKey();
        if (plot.layoutKey !== key) {
            plot.layoutKey = key;
            defaultDisp();
            resetSeries();
            resetZoom();
            applyViewState(plot.pendingView || storedView());
            plot.pendingView = null;
        }
        if (plot.dispSig !== dimsSignature()) renderDimsPicker();
        // The series list is up to 512 rows and this runs on every poll,
        // so it is rebuilt only when what it lists actually changed —
        // otherwise a dropdown left open would reset its scroll position
        // every 5 s.  A shrinking count (a smaller fetch cutting the
        // leading extent) drops the rows that no longer exist.
        var sig = plot.axisY.join(",") + ":" + seriesCount();
        if (plot.seriesSig !== sig) {
            plot.seriesSig = sig;
            var count = seriesCount();
            plot.series = plot.series.filter(function (i) { return i < count; });
            if (!plot.series.length) resetSeries();
            renderSeriesPicker();
        } else {
            updateSeriesSummary();
        }
        updateControls();
    }

    // The y axis is the *series* axis in lines mode: same dimensions,
    // same layout memory, drawn as curves instead of rows.
    function yLabel() {
        return effectiveMode() === "lines" ? "series" : "y";
    }

    // "F→x, E→series, T mean, C@0" — the whole disposition of the array
    // in the dropdown's summary, so what the plot is showing is legible
    // without opening anything.
    function dispSummary() {
        var parts = [];
        usableDims().forEach(function (d) {
            var k = plot.disp[d];
            parts.push(dimName(d) +
                       (k === "x" ? "→x" : k === "y" ? "→" + yLabel() : " " + k) +
                       (plot.pick[d] ? "@" + plot.pick[d] : ""));
        });
        var text = parts.join(", ");
        if (!text) return "—";
        return text.length > 44 ? text.slice(0, 43) + "…" : text;
    }

    var DISP_OPTIONS = [["x", "x axis"], ["y", "y axis"], ["max", "max"],
                        ["mean", "mean"], ["min", "min"]];

    // One row per dimension: which of its indices are in play, and what
    // happens to them.  A dimension can only hold one disposition, so a
    // dimension on both axes — which would index the same values twice —
    // is unrepresentable rather than guarded against.
    function dimRow(body, d) {
        var row = el("div", {
            style: "display: flex; align-items: center; gap: 0.4em; " +
                   "margin: 0 0 0.25em 0; white-space: nowrap;",
        });
        row.appendChild(el("span", { style: "min-width: 8em;" }, dimLabel(d)));
        var sel = el("select", {
            style: "width: auto; margin: 0; padding: 0.05em 1.4em 0.05em 0.3em; " +
                   "font-size: 0.95em;",
        });
        DISP_OPTIONS.forEach(function (o) {
            sel.appendChild(el("option", { value: o[0] },
                               o[0] === "y" ? yLabel() + " axis" : o[1]));
        });
        sel.value = plot.disp[d];
        sel.addEventListener("change", function () { setDisp(d, sel.value); });
        row.appendChild(sel);
        // The same range syntax as the series box, and for the same
        // reason: an option per index was unusable at 9216 entries.  One
        // entry pins the dimension, several keep it — on the series axis
        // that is several lines, which is the whole point of it being a
        // list rather than a number.
        var box = el("input", {
            type: "text", placeholder: "all", title:
                "Indices to keep, e.g. 0 or 0,3 or 1-4 (blank = all)",
            style: "width: 6.5em; margin: 0; padding: 0.05em 0.3em; " +
                   "font-size: 0.95em;",
        });
        box.value = plot.pick[d];
        box.addEventListener("change", function () {
            setPick(d, box.value);
        });
        row.appendChild(box);
        body.appendChild(row);
    }

    // Everything the table displays.  Like the series list, this is
    // rebuilt only when it would actually change: the rows carry a text
    // input, and replacing the DOM under a poll would drop focus
    // mid-edit and close an open select every 5 s.
    function dimsSignature() {
        return plot.dims.join("×") + ":" + plot.disp.join(",") + ":" +
               plot.pick.join(";") + ":" + yLabel();
    }

    function renderDimsPicker() {
        var pick = plot.controls.dimsPick;
        pick.summary.textContent = "dims: " + dispSummary();
        pick.body.textContent = "";
        usableDims().forEach(function (d) { dimRow(pick.body, d); });
        pick.body.appendChild(el("small", { style: "opacity: 0.7;" },
                                "folds apply outermost first"));
        plot.dispSig = dimsSignature();
    }

    function setDisp(d, kind) {
        var seriesBefore = plot.axisY.join(",");
        plot.disp[d] = kind;
        syncAxes();
        // Recomposing the series axis changes what a row index means, so
        // the selection is re-defaulted rather than carried across.
        if (plot.axisY.join(",") !== seriesBefore) resetSeries();
        // A recomposed axis makes the old window's indices meaningless.
        resetZoom();
        rebuildDimControls();
        rerender();
    }

    // Restricting a dimension changes the length of whatever axis it is
    // on, so the series selection and the zoom window are re-derived for
    // the same reason a disposition change re-derives them: their
    // indices would otherwise point at entries that moved.
    function setPick(d, spec) {
        plot.pick[d] = String(spec || "").trim();
        syncPicks();
        resetSeries();
        resetZoom();
        rebuildDimControls();
        rerender();
    }

    // --- data sources -----------------------------------------------------
    // What the panel plots is a *source*: an id (for the per-view memory
    // and the full-screen link), a title, and a URL that takes the same
    // ?len= prefix protocol.  A kotekan buffer and an F-engine gain
    // dataset differ only here — everything downstream sees a
    // descriptor and bytes and cannot tell them apart.

    function bufferSource(nodeKey, buffer) {
        return {
            id: nodeKey + "|" + buffer,
            title: buffer,
            url: "/api/node-buffer-data/" + nodeKey +
                 "?buffer=" + encodeURIComponent(buffer),
            page: "/plot/" + nodeKey + "?buffer=" + encodeURIComponent(buffer),
        };
    }

    function urlSource(url, id, title, opts) {
        opts = opts || {};
        return { id: id || url, title: title || "data", url: url,
                 page: opts.page || null, pollMs: opts.pollMs,
                 defaultFetch: opts.defaultFetch };
    }

    function dataUrl(len) {
        return plot.source.url + "&len=" + len;
    }

    // Which controls apply depends on the frame and the mode: N2 frames
    // have fixed block shapes and so no dimensions to dispose of, the
    // bit axis only exists for packed masks, the series list only means
    // something for curves, and the pixel rule only bites where an axis
    // is longer than the canvas.  The dimension table and the zeros
    // switch apply to every mode, histogram included — folding is what
    // produces the values it bins.  One owner for all of it: three
    // functions each hiding an overlapping subset drifted apart.
    function updateControls() {
        if (!plot) return;
        var c = plot.controls, isN2 = !!plot.n2, mode = effectiveMode();
        var shaped = !isN2 && !!plot.dims;
        var blk = isN2 ? plot.n2.blocks[c.block.value] : null;
        show(c.block.parentElement, isN2);
        show(c.part.parentElement,
             isN2 ? (!!blk && blk.type === "c64")
                  : !!(plot.dec && plot.dec.complex));
        show(c.bits.parentElement,
             !isN2 && !!(plot.dec && plot.dec.mask && plot.dec.sub > 1));
        show(c.dimsPick.root, shaped);
        show(c.seriesPick.root, shaped && mode === "lines");
        show(c.combine.parentElement,
             shaped && (mode === "heatmap" || mode === "lines"));
        show(c.zeros.parentElement, !isN2);
    }

    function show(node, on) {
        node.style.display = on ? "" : "none";
    }

    // The reset only exists while there is something to reset — a live
    // control that does nothing most of the time reads as broken.
    function showZoomReset() {
        if (!plot || !plot.controls.zoomReset) return;
        var z = plot.zoom;
        show(plot.controls.zoomReset,
             z.x0 != null || z.x1 != null || z.y0 != null || z.y1 != null);
    }

    // True when the part selector is collapsing complex pairs into one
    // derived number rather than leaving them as a dimension.
    function derivesPart() {
        return !!(plot.dec && plot.dec.complex && plot.n2 === null &&
                  plot.controls.part.value !== "components");
    }

    // Bytes -> the numbers the reduction sees.  The only step between
    // the decoder table and the dimension machinery, and the one place
    // a derived complex part is applied.
    function decodeValues(buf) {
        var values = plot.dec.decode(buf);
        if (!derivesPart()) return values;
        return complexPart(values, values.length >> 1,
                           plot.controls.part.value);
    }

    // Mean of the decoded values — for a mask that is the set fraction.
    // Computed once per fetch (not per render): the arrays run to tens
    // of millions of entries.
    function meanOf(values) {
        var sum = 0;
        for (var i = 0; i < values.length; i++) sum += values[i];
        return values.length ? sum / values.length : 0;
    }

    // How much of the frame is exactly zero (same once-per-fetch rule).
    function zeroFracOf(values) {
        var z = 0;
        for (var i = 0; i < values.length; i++) if (values[i] === 0) z++;
        return values.length ? z / values.length : 0;
    }

    // Load the frame descriptor (len=0 JSON) if not yet loaded.  Called
    // before every data fetch so a failed first attempt (node briefly
    // unreachable, pipeline restarting) retries on the next poll instead
    // of leaving the panel without axes and decoder forever.
    function ensureDesc() {
        if (!plot || plot.descLoaded) return Promise.resolve();
        var me = plot;
        return fetch(dataUrl(0))
            .then(function (resp) { return resp.json(); })
            .then(function (frame) {
                if (plot !== me) return;
                if (frame.error) throw new Error(frame.error);
                var desc = frame.frame_desc || {};
                plot.dec = decoderFor(desc.value_type);
                plot.extents = Array.isArray(desc.extents) ? desc.extents : null;
                plot.descDimNames = Array.isArray(desc.dimnames) ? desc.dimnames : null;
                if (typeof frame.frame_size === "number")
                    plot.frameSize = frame.frame_size;
                plot.n2 = desc.frame_desc_type === "N2"
                    ? n2LayoutFromDesc(desc) : null;
                // The layout mirror has been bitten by a wire-format
                // change once (DishInputs went from sparse product_list
                // to compact + input_list), so don't trust it blindly:
                // a frame whose real size disagrees with the computed
                // block layout would decode every block at the wrong
                // offset.  Fall back to generic rendering and say why.
                plot.descNote = null;
                if (plot.n2 && typeof frame.frame_size === "number" &&
                        frame.frame_size !== plot.n2.total) {
                    plot.descNote = "N2 layout mismatch (computed " +
                        plot.n2.total + " B, frame " + frame.frame_size +
                        " B) — generic rendering";
                    plot.n2 = null;
                }
                if (plot.n2) {
                    var bsel = plot.controls.block;
                    bsel.textContent = "";
                    ["vis", "weight", "flags"]
                        .concat(plot.n2.nev > 0 ? ["eval", "evec"] : [])
                        .concat(["gain", "mask"])
                        .forEach(function (k) {
                            bsel.appendChild(el("option", { value: k }, k));
                        });
                }
                if (plot.dec.mask) {
                    // Combining a 1 = good mask: max is degenerate (a
                    // cell reads 1 unless *everything* under it is bad)
                    // and mean buries a single lost packet in thousands
                    // of good ones — 1.0000 vs 0.9997 is the same
                    // colour.  min is the sensitive one: a cell goes
                    // dark-to-amber the moment any bit under it is
                    // clear, which is what a mask is for.  Set once at
                    // descriptor load, when the panel has just opened,
                    // so it can't override an operator's own pick.
                    plot.controls.combine.value = "min";
                }
                updateControls();
                plot.descLoaded = true;
            });
    }

    // `force` (fetch-size change, Refresh click) processes the reply even
    // when the frame id hasn't advanced — a poll tick would otherwise
    // discard the freshly fetched bytes as "same frame".
    function fetchData(force) {
        if (!plot || plot.inflight) return;
        plot.inflight = true;
        var me = plot;
        ensureDesc()
            .then(function () {
                if (plot !== me) return;
                var len = +plot.controls.fetchLen.value;
                // N2 blocks are sequential, so a prefix would cut the
                // later blocks off — always fetch the whole (small)
                // frame.  The server still caps at its 32 MiB limit.
                if (plot.n2 && plot.frameSize)
                    len = Math.max(len, plot.frameSize);
                return fetch(dataUrl(len));
            })
            .then(function (resp) {
                if (!resp || plot !== me) return;
                var ctype = resp.headers.get("Content-Type") || "";
                if (ctype.indexOf("json") >= 0)
                    return resp.json().then(function (j) {
                        throw new Error(j.error || ("HTTP " + resp.status));
                    });
                if (!resp.ok) throw new Error("HTTP " + resp.status);
                // Anything else non-binary is not frame data — most
                // likely the login page after session expiry; without
                // this check it would render as a garbage histogram.
                if (ctype.indexOf("octet-stream") < 0)
                    throw new Error("unexpected reply (session expired? reload the page)");
                var frameId = resp.headers.get("X-Frame-Id");
                var frameSize = parseInt(resp.headers.get("X-Frame-Size"), 10);
                return resp.arrayBuffer().then(function (buf) {
                    if (plot !== me) return;
                    if (isFinite(frameSize)) plot.frameSize = frameSize;
                    var same = frameId === plot.frameId && plot.lastValues;
                    if (same && !force) {
                        if (!plot.staleSince) plot.staleSince = new Date();
                        rerender(); // refresh the staleness note only
                        return;
                    }
                    if (!same) plot.staleSince = null;
                    plot.frameId = frameId;
                    plot.lastBytes = buf.byteLength;
                    plot.lastBuf = buf;
                    plot.lastValues = decodeValues(buf);
                    plot.maskFrac = plot.dec.mask ? meanOf(plot.lastValues) : null;
                    plot.zeroFrac = plot.dec.mask ? null
                                                  : zeroFracOf(plot.lastValues);
                    shapeReceived(buf.byteLength);
                    rebuildDimControls();
                    rerender();
                });
            })
            .catch(function (err) {
                if (plot === me) setStatus("fetch failed: " + err.message);
            })
            .finally(function () {
                if (plot === me) plot.inflight = false;
            });
    }

    function tick() {
        if (!plot || document.hidden || !plot.visible) return;
        fetchData();
    }

    function buildPanel(container, source, opts) {
        opts = opts || {};
        // Full screen: the panel *is* the page below its header, so it
        // becomes a flex column and the canvas takes whatever is left.
        var root = el("article", { style: opts.fullscreen
            ? "margin: 0; padding: 0.4em 0.6em; flex: 1; display: flex; " +
              "flex-direction: column; min-height: 0;"
            : "margin: 0.5em 0 0 0; padding: 0.75em 1em;" });
        var header = el("header", { style: "margin: 0 0 0.5em 0; padding: 0; display: flex; align-items: center; gap: 0.75em;" });
        var title = el("small");
        title.appendChild(el("code", {}, source.title));
        title.appendChild(document.createTextNode(
            " — live plot (" + Math.round(pollMsFor(source) / 1000) + " s)"));
        header.appendChild(title);
        var fullLink = null;
        if (!opts.fullscreen && source.page) {
            // Carries the current view across, so full screen opens on
            // exactly what the overlay was showing (persistView keeps
            // the href in step).
            fullLink = el("a", {
                // A working link before the first frame lands, too;
                // persistView adds the view to it from then on.
                href: source.page,
                title: "Open this plot full screen",
                style: "margin: 0 0 0 auto; text-decoration: none; font-size: 1.1em;",
            }, "⤢");
            var closeBtn = el("button", {
                type: "button", class: "outline",
                style: "width: auto; margin: 0; padding: 0.05em 0.6em; font-size: 0.9em;",
            }, "Close");
            closeBtn.addEventListener("click", function () {
                if (opts.nodeKey) forgetOpen(opts.nodeKey);  // stop auto-reopen
                closePlot();
            });
            header.appendChild(fullLink);
            header.appendChild(closeBtn);
        }
        root.appendChild(header);

        var controls = el("div", { style: "display: flex; flex-wrap: wrap; gap: 0.5em; align-items: center; margin-bottom: 0.5em; font-size: 0.85em;" });
        function select(labelText, opts) {
            var wrap = el("label", { style: "display: flex; align-items: center; gap: 0.3em; margin: 0;" }, labelText);
            var s = el("select", { style: "width: auto; margin: 0; padding: 0.1em 1.6em 0.1em 0.4em; font-size: 1em;" });
            (opts || []).forEach(function (o) {
                s.appendChild(el("option", { value: o[0] }, o[1]));
            });
            wrap.appendChild(s);
            controls.appendChild(wrap);
            return s;
        }
        var mode = select("mode", [["auto", "auto"], ["heatmap", "heatmap"],
                                   ["lines", "lines"], ["histogram", "histogram"]]);
        // N2-only controls (shown when the descriptor says N2): which
        // frame block to plot, and which complex component.
        var block = select("block", []);
        var part = select("part", [["mag", "mag"], ["phase", "phase"],
                                   ["real", "real"], ["imag", "imag"],
                                   ["components", "re/im as a dimension"]]);
        block.parentElement.style.display = "none";
        part.parentElement.style.display = "none";
        // Mask-only: how to treat the packed bit axis (see shapeReceived).
        var bits = select("bits", [["axis", "separate axis"],
                                   ["merge", "merge into last axis"]]);
        bits.parentElement.style.display = "none";
        // One row per dimension: on an axis (axes are still *composed* —
        // several dimensions can share one) or folded away by its own
        // operation.
        var dimsPick = checkboxDropdown("dims");
        // Lines mode only: which of the series axis' lines are drawn.
        var seriesPick = checkboxDropdown("series");
        controls.appendChild(dimsPick.root);
        controls.appendChild(seriesPick.root);
        // What the many entries of an axis longer than the canvas do
        // when they share a pixel — the *display* reduction, as distinct
        // from the per-dimension folds above.  max first (the default):
        // outliers and saturation are usually what an operator is
        // looking for; mean washes them out.  min is max's mirror and is
        // what masks need — with 1 = good, a pixel reads 0 as soon as
        // any value under it is bad.
        var combine = select("pixels", [["max", "max"], ["mean", "mean"],
                                        ["min", "min"]]);
        // Only a subset of frequencies is fed through the system today,
        // so most of an auto-spectrum frame is a real 0 rather than a
        // gap.  Blanking is off by default — 0 is the payload for a mask
        // and a legitimate reading for power — and the status line
        // reports the zero fraction so the switch is findable.
        var zeros = select("zeros", [["show", "show"], ["hide", "hide"]]);
        var fetchLen = select("fetch", FETCH_CHOICES.map(function (c) {
            return [String(c[0]), c[1]];
        }));
        // A source's own default only applies until the view record
        // says otherwise (applyViewState runs right after this).

        fetchLen.value = String(source.defaultFetch || DEFAULT_FETCH);
        var logWrap = el("label", {
            style: "display: flex; align-items: center; gap: 0.3em; margin: 0;",
            title: "lines: log y — heatmaps: log color scale — histogram: log counts",
        });
        var log = el("input", { type: "checkbox", style: "margin: 0;" });
        logWrap.appendChild(log);
        logWrap.appendChild(document.createTextNode("log"));
        controls.appendChild(logWrap);
        var refreshBtn = el("button", {
            type: "button", class: "outline",
            style: "width: auto; margin: 0; padding: 0.05em 0.6em; font-size: 1em;",
        }, "Refresh");
        controls.appendChild(refreshBtn);
        var metaBtn = el("button", {
            type: "button", class: "outline",
            title: "Show the newest frame's metadata",
            style: "width: auto; margin: 0; padding: 0.05em 0.6em; font-size: 1em;",
        }, "Meta");
        controls.appendChild(metaBtn);
        var zoomReset = el("button", {
            type: "button", class: "outline",
            title: "Back to the whole axis",
            style: "display: none; width: auto; margin: 0; " +
                   "padding: 0.05em 0.6em; font-size: 1em;",
        }, "Reset zoom");
        zoomReset.addEventListener("click", function () {
            resetZoom();
            rerender();
        });
        controls.appendChild(zoomReset);
        root.appendChild(controls);

        // The canvas is sized by CSS and reads its own box each render
        // (beginPlot scales the backing store by devicePixelRatio), so
        // the two contexts differ only in what height they hand it: a
        // fixed one in the panel, everything that's left full screen.
        var canvas = el("canvas", { style: opts.fullscreen
            ? "width: 100%; flex: 1; min-height: 0; border-radius: 4px; cursor: crosshair;"
            : "width: 100%; height: 300px; border-radius: 4px; cursor: crosshair;" });
        canvas.addEventListener("wheel", onWheel, { passive: false });
        canvas.addEventListener("mousedown", onDragStart);
        canvas.addEventListener("dblclick", function () {
            resetZoom();
            rerender();
        });
        root.appendChild(canvas);
        var status = el("p", { style: "margin: 0.4em 0 0 0; font-size: 0.8em;" }, "loading…");
        root.appendChild(status);
        // Metadata viewer: hidden until the Meta button is clicked; each
        // click re-fetches len=0 so the metadata shown is current, not
        // the panel-open snapshot.  textContent only — kotekan-supplied
        // strings never enter innerHTML.
        var metaPre = el("pre", {
            style: "display: none; max-height: 16em; overflow: auto; " +
                   "margin: 0.4em 0 0 0; padding: 0.5em 0.75em; font-size: 0.72em;",
        });
        root.appendChild(metaPre);
        metaBtn.addEventListener("click", function () {
            if (metaPre.style.display !== "none") {
                metaPre.style.display = "none";
                return;
            }
            metaPre.style.display = "block";
            metaPre.textContent = "fetching metadata…";
            var me = plot;
            fetch(dataUrl(0))
                .then(function (r) { return r.json(); })
                .then(function (frame) {
                    if (plot !== me) return;
                    metaPre.textContent = frame.error ? frame.error :
                        JSON.stringify({ frame_id: frame.frame_id,
                                         frame_desc: frame.frame_desc,
                                         metadata: frame.metadata }, null, 2);
                })
                .catch(function (err) {
                    if (plot === me)
                        metaPre.textContent = "metadata fetch failed: " + err.message;
                });
        });
        container.appendChild(root);

        plot = {
            source: source, root: root, canvas: canvas,
            status: status, visible: true, inflight: false,
            frameId: null, frameSize: null, staleSince: null,
            lastValues: null, lastBuf: null, lastBytes: 0,
            dims: null, dimNames: null, descDimNames: null, extents: null,
            shapeNote: null, descLoaded: false, dec: decoderFor(null),
            n2: null, maskFrac: null, zeroFrac: null,
            controls: { mode: mode, block: block, part: part, bits: bits,
                        dimsPick: dimsPick, seriesPick: seriesPick,
                        combine: combine, zeros: zeros,
                        fetchLen: fetchLen, log: log, zoomReset: zoomReset },
            bitsMode: "axis",
            disp: [], pick: [], picks: [], axisY: [], axisX: [],
            layoutKey: null, dispSig: null,
            series: [], seriesStride: 1, seriesSig: null,
            zoom: { x0: null, x1: null, y0: null, y1: null },
            view: null, fullExtent: null,
            fullscreen: !!opts.fullscreen, urlState: !!opts.urlState,
            fullLink: fullLink, pendingView: null, lastViewSig: null,
        };
        combine.addEventListener("change", rerender);
        part.addEventListener("change", function () {
            if (!plot) return;
            if (!plot.n2 && plot.lastBuf) {
                // A derived part changes both the values and the shape,
                // so re-decode the bytes already in hand rather than
                // refetching — the same rule the bits selector follows.
                plot.lastValues = decodeValues(plot.lastBuf);
                plot.zeroFrac = plot.dec.mask ? null
                                              : zeroFracOf(plot.lastValues);
                shapeReceived(plot.lastBytes);
                rebuildDimControls();
            }
            rerender();
        });
        // The mode decides which controls apply and whether the y axis
        // is called "series", so it redraws the bar as well as the canvas.
        mode.addEventListener("change", function () {
            if (plot.dims) renderDimsPicker();
            updateControls();
            rerender();
        });
        zeros.addEventListener("change", rerender);
        // Re-shaping the same bytes: no refetch, just a different view
        // of the array that is already decoded.
        bits.addEventListener("change", function () {
            if (!plot) return;
            plot.bitsMode = bits.value;
            if (plot.lastBytes) {
                shapeReceived(plot.lastBytes);
                rebuildDimControls();
                rerender();
            }
        });
        block.addEventListener("change", function () {
            updateControls();
            rerender();
        });
        log.addEventListener("change", rerender);
        // Not passed directly as handlers: the event object would read as
        // a truthy `force` — and these two genuinely want force anyway.
        fetchLen.addEventListener("change", function () { fetchData(true); });
        refreshBtn.addEventListener("click", function () { fetchData(true); });

        if (typeof IntersectionObserver !== "undefined") {
            plot.observer = new IntersectionObserver(function (entries) {
                if (!plot) return;
                plot.visible = entries[0].isIntersecting;
                // The observer also does the *first* fetch, when the
                // panel is actually on-screen — an auto-reopened panel
                // below the fold shouldn't cost a fetch on page load.
                if (plot.visible && !plot.lastValues && !plot.inflight)
                    fetchData();
            });
            plot.observer.observe(root);
        }
        plot.onVis = function () {
            if (plot && !document.hidden && plot.visible) fetchData();
        };
        document.addEventListener("visibilitychange", plot.onVis);
        // The canvas takes its size from the layout, so a resized window
        // is a different plot area — re-render from the bytes already in
        // hand rather than waiting for the next poll.
        plot.onResize = function () {
            clearTimeout(plot.resizeTimer);
            plot.resizeTimer = setTimeout(function () {
                if (plot) rerender();
            }, 150);
        };
        window.addEventListener("resize", plot.onResize);
        // The theme decides what the canvas paints, and the standalone
        // pages carry a toggle — without this the plot would keep its
        // old background until the next poll, which for gains is 30 s.
        plot.onTheme = function () { if (plot) rerender(); };
        if (typeof MutationObserver !== "undefined") {
            plot.themeObserver = new MutationObserver(plot.onTheme);
            plot.themeObserver.observe(document.documentElement,
                                       { attributes: true,
                                         attributeFilter: ["data-theme"] });
        }
        if (window.matchMedia) {
            plot.themeQuery = window.matchMedia("(prefers-color-scheme: dark)");
            if (plot.themeQuery.addEventListener)
                plot.themeQuery.addEventListener("change", plot.onTheme);
        }
    }

    // The one way in.  opts: container, fullscreen, urlState, view
    // (a parsed view record to apply), noScroll.
    function openPlot(source, opts) {
        opts = opts || {};
        closePlot();
        var container = opts.container || document.getElementById("buffer-plot");
        if (!container) return;
        buildPanel(container, source, opts);
        if (!opts.fullscreen && opts.nodeKey)
            rememberOpen(opts.nodeKey, opts.buffer);
        // Applied twice on purpose: now, for the controls that decide
        // what to fetch (fetch size, bit packing), and again once the
        // first frame has been shaped, for everything keyed to the
        // dimensions.  applyViewState is idempotent and skips the
        // dimension half until there are dimensions.
        plot.pendingView = opts.view || null;
        applyViewState(plot.pendingView);
        // The IntersectionObserver fires the first fetch (descriptor +
        // data, chained inside fetchData) once the panel is on-screen;
        // without observer support, fetch immediately.
        if (!plot.observer)
            fetchData();
        plot.timer = setInterval(tick, pollMsFor(source));
        if (!opts.noScroll)
            root_scroll(container);
    }

    function pollMsFor(source) {
        return source && source.pollMs ? source.pollMs : POLL_MS;
    }

    function root_scroll(node) {
        if (node.scrollIntoView)
            node.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }

    document.addEventListener("click", function (e) {
        var btn = e.target.closest && e.target.closest("[data-plot-buffer]");
        if (!btn) return;
        var nodeKey = btn.getAttribute("data-plot-node");
        var buffer = btn.getAttribute("data-plot-buffer");
        openPlot(bufferSource(nodeKey, buffer),
                 { nodeKey: nodeKey, buffer: buffer });
    });

    // The module's only export: enough for a page to point the panel at
    // a source of its own (the FPGA page's dataset selector), and no
    // more — everything else stays private.
    window.chocoPlot = { open: openPlot, urlSource: urlSource };

    // Two ways a page can start with a plot open: the full-screen page
    // names its buffer and carries the view in the fragment, and a page
    // with the overlay reopens whatever was last watched there (no
    // scroll: don't yank the viewport on load).
    (function () {
        var container = document.getElementById("buffer-plot");
        if (!container) return;
        // Full screen: the page names its own source (a node buffer or
        // an F-engine gain dataset — the panel cannot tell) and carries
        // the view in the fragment.
        var url = container.getAttribute("data-source-url");
        if (url) {
            var full = container.getAttribute("data-fullscreen") === "1";
            openPlot(urlSource(url,
                               container.getAttribute("data-source-id"),
                               container.getAttribute("data-source-title"),
                               { pollMs: +container.getAttribute("data-poll-ms") ||
                                         undefined,
                                 defaultFetch:
                                     +container.getAttribute("data-fetch") ||
                                     undefined }),
                     { container: container, fullscreen: full,
                       urlState: full, noScroll: true,
                       view: parseView(location.hash) });
            return;
        }
        // Overlay: reopen whatever was last watched on this node.
        var nodeKey = container.getAttribute("data-node-key");
        var saved = nodeKey && savedOpen(nodeKey);
        if (saved)
            openPlot(bufferSource(nodeKey, saved),
                     { noScroll: true, nodeKey: nodeKey, buffer: saved });
    })();
})();
