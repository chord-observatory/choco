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

    // --- N2 frames --------------------------------------------------------
    // Mirror of kotekan's N2FrameDesc::get_frame_layout: ten sequential
    // blocks, no padding.  Offsets validated against live cx19 frames
    // (n2_buffer 100756 B, n2_eigen_buffer 7292 B — exact size matches,
    // and the decoded autocorrelations come out real and positive).
    function n2LayoutFromDesc(desc) {
        var n = desc.num_elements, nev = desc.num_ev || 0;
        var layout = desc.n2_layout;
        var numProd;
        if (layout === "FullUpperTri") numProd = n * (n + 1) / 2;
        else if (layout === "Autocorrelations") numProd = n;
        else if (Array.isArray(desc.product_list)) numProd = desc.product_list.length;
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
                 blocks: blocks, total: off };
    }

    function complexPart(f32, count, part) {
        var out = new Float32Array(count);
        for (var i = 0; i < count; i++) {
            var re = f32[2 * i], im = f32[2 * i + 1];
            out[i] = part === "real" ? re : part === "imag" ? im :
                     part === "phase" ? Math.atan2(im, re) : Math.hypot(re, im);
        }
        return out;
    }

    // Unpack the packed upper triangle into a full n×n grid; the lower
    // triangle is the conjugate mirror, so the odd parts flip sign.
    function n2Grid(tri, n, part) {
        var grid = new Float64Array(n * n);
        var neg = part === "phase" || part === "imag";
        var k = 0;
        for (var i = 0; i < n; i++)
            for (var j = i; j < n; j++, k++) {
                var v = tri[k];
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
    function reduceToGrid(values, dims, yDims, xDims, how) {
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
            var v = values[i];
            var cell = yCell * w + xCell;
            if (how === "max") {
                if (v > acc[cell]) acc[cell] = v;
            } else if (how === "min") {
                if (v < acc[cell]) acc[cell] = v;
            } else {
                acc[cell] += v;
            }
            cnt[cell]++;
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
        // Cells that no element reached (the received bytes stopping
        // short of a whole grid) must read as missing, not as 0 or
        // ±Infinity.
        for (var e = 0; e < acc.length; e++)
            if (!cnt[e]) acc[e] = NaN;
        return { grid: acc, h: h, w: w };
    }

    // opts.range fixes the colour range (masks: [0, 1] — never
    // percentile-stretched); opts.color swaps the palette.
    function drawGrid(canvas, grid, h, w, opts) {
        opts = opts || {};
        var range = opts.range || robustRange(grid);
        var lo = range[0], hi = range[1];
        var color = opts.color || viridis;
        canvas.width = w;
        canvas.height = h;
        canvas.style.imageRendering = "pixelated";
        // A grid with one tiny axis against a long one (a mask's 8 bits
        // against 384 channels) is a hairline under aspect-preserving
        // fit — 8 rows over a couple of pixels.  Stretch just those to
        // the panel; a small *square-ish* grid (8 bits against 8 dish
        // groups) keeps square cells, as does anything large.
        var aspect = Math.max(h, w) / Math.max(1, Math.min(h, w));
        canvas.style.objectFit =
            (Math.min(h, w) <= 16 && aspect > 4) ? "fill" : "contain";
        var ctx = canvas.getContext("2d");
        var img = ctx.createImageData(w, h);
        for (var i = 0; i < grid.length; i++) {
            var v = grid[i], p = 4 * i;
            if (!isFinite(v)) {
                img.data[p] = img.data[p + 1] = img.data[p + 2] = 40;
            } else {
                var c = color((v - lo) / (hi - lo));
                img.data[p] = c[0]; img.data[p + 1] = c[1]; img.data[p + 2] = c[2];
            }
            img.data[p + 3] = 255;
        }
        ctx.putImageData(img, 0, 0);
        return "color " + fmtVal(lo) + " … " + fmtVal(hi);
    }

    function renderHeatmap(canvas, values, dims, yDims, xDims, how, opts) {
        var r = reduceToGrid(values, dims, yDims, xDims, how);
        return drawGrid(canvas, r.grid, r.h, r.w, opts) + " (" + how + ")";
    }

    function renderHistogram(canvas, values, integer, logY) {
        var min = Infinity, max = -Infinity, i;
        for (i = 0; i < values.length; i++) {
            var v = values[i];
            if (!isFinite(v)) continue;
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
            if (!isFinite(x)) continue;
            var b = oneBinPerInt ? Math.round(x - min)
                                 : Math.min(nbins - 1, Math.floor((x - min) * scale));
            counts[b]++;
        }
        var peak = 0;
        for (i = 0; i < nbins; i++) {
            if (logY) counts[i] = counts[i] ? Math.log10(1 + counts[i]) : 0;
            if (counts[i] > peak) peak = counts[i];
        }
        var W = 800, H = 260, pad = 4;
        canvas.width = W;
        canvas.height = H;
        canvas.style.imageRendering = "auto";
        var ctx = canvas.getContext("2d");
        ctx.fillStyle = "#1a1f24";
        ctx.fillRect(0, 0, W, H);
        ctx.fillStyle = "#4dabf7";
        var bw = (W - 2 * pad) / nbins;
        for (i = 0; i < nbins; i++) {
            var bh = peak ? (counts[i] / peak) * (H - 2 * pad) : 0;
            ctx.fillRect(pad + i * bw, H - pad - bh, Math.max(1, bw - 1), bh);
        }
        ctx.fillStyle = "#adb5bd";
        ctx.font = "12px monospace";
        ctx.fillText(fmtVal(min), pad + 2, H - 8);
        ctx.textAlign = "right";
        ctx.fillText(fmtVal(max), W - pad - 2, H - 8);
        ctx.textAlign = "left";
        return "range " + fmtVal(min) + " … " + fmtVal(max) +
            (logY ? ", log counts" : "");
    }

    function renderLine(canvas, values) {
        var n = Math.min(values.length, 8192);
        var min = Infinity, max = -Infinity, i;
        for (i = 0; i < n; i++) {
            var v = values[i];
            if (!isFinite(v)) continue;
            if (v < min) min = v;
            if (v > max) max = v;
        }
        if (min > max) { min = 0; max = 1; }
        if (min === max) { min -= 1; max += 1; }
        var W = 800, H = 260, pad = 4;
        canvas.width = W;
        canvas.height = H;
        canvas.style.imageRendering = "auto";
        var ctx = canvas.getContext("2d");
        ctx.fillStyle = "#1a1f24";
        ctx.fillRect(0, 0, W, H);
        ctx.strokeStyle = "#4dabf7";
        ctx.beginPath();
        for (i = 0; i < n; i++) {
            var x = pad + (i / (n - 1 || 1)) * (W - 2 * pad);
            var y = H - pad - ((values[i] - min) / (max - min)) * (H - 2 * pad);
            if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        }
        ctx.stroke();
        return "range " + fmtVal(min) + " … " + fmtVal(max) +
            (values.length > n ? ", first " + n + " values" : "");
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
        if (plot.observer) plot.observer.disconnect();
        document.removeEventListener("visibilitychange", plot.onVis);
        plot.root.remove();
        plot = null;
    }

    function setStatus(text) {
        if (plot) plot.status.textContent = text;
    }

    function dimLabel(i) {
        var name = plot.dimNames && plot.dimNames[i];
        return (name ? name : "dim" + i) + " (" + plot.dims[i] + ")";
    }

    function dimName(i) {
        var name = plot.dimNames && plot.dimNames[i];
        return name ? name : "dim" + i;
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
            n *= plot.dims[list[i]];
        }
        return parts.join("×") + " (" + n + ")";
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

    function effectiveMode() {
        var m = plot.controls.mode.value;
        if (m !== "auto") return m;
        // No descriptor extents (N2 frames, unknown types): histogram.
        if (!plot.dims) return "histogram";
        // Count only dimensions of extent > 1: `[Tc=1, F=384]` is a
        // spectrum, and drawing it as a 1×384 heatmap (which is what a
        // raw dims.length test gives) wastes the panel on one row of
        // pixels.  With the extent-1 dimensions squeezed, anything still
        // multi-dimensional is a heatmap — the max reduction makes even
        // packed low-bit voltage read as a saturation map — and a single
        // remaining axis is a line.
        if (usableDims().length >= 2) return "heatmap";
        return "line";
    }

    function rerender() {
        if (!plot || !plot.lastValues) return;
        if (plot.n2 && plot.lastBuf) {
            var n2note = renderN2(plot.controls.mode.value);
            setStatus("frame " + plot.frameId + ", " + fmtBytes(plot.lastBytes) +
                      (plot.frameSize ? " of " + fmtBytes(plot.frameSize) : "") +
                      " — " + n2note + staleSuffix());
            return;
        }
        var mode = effectiveMode();
        var note;
        if (mode === "heatmap" && plot.dims) {
            if (!plot.axisY.length || !plot.axisX.length) {
                setStatus("tick at least one dimension for each of y and x");
                return;
            }
            note = renderHeatmap(plot.canvas, plot.lastValues, plot.dims,
                                 plot.axisY, plot.axisX,
                                 plot.controls.combine.value,
                                 plot.dec.mask ? { range: [0, 1], color: maskColor }
                                               : null);
            note = "y: " + axisLabel(plot.axisY) + ", x: " + axisLabel(plot.axisX) +
                   " — " + note;
        } else if (mode === "line") {
            note = renderLine(plot.canvas, plot.lastValues);
        } else {
            note = renderHistogram(plot.canvas, plot.lastValues,
                                   plot.dec.integer, plot.controls.log.checked);
        }
        var got = plot.lastBytes;
        var frameText = "frame " + plot.frameId + ", " + fmtBytes(got) +
            (plot.frameSize ? " of " + fmtBytes(plot.frameSize) : "");
        if (plot.shapeNote)
            frameText += ", " + plot.shapeNote;
        // For a mask the single most useful number is how much of it is
        // set, over everything fetched — and the polarity has to be
        // spelled out, since "100% set" is the *healthy* reading.
        if (plot.dec.mask && plot.maskFrac !== null &&
            plot.maskFrac !== undefined)
            frameText += ", " + (100 * plot.maskFrac).toFixed(2) +
                         "% set (1 = good)";
        setStatus(frameText + " — " + note + staleSuffix());
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
        else if (mode === "line")
            note = renderLine(plot.canvas, vals);
        else if ((key === "vis" || key === "weight") && n2.layout === "FullUpperTri")
            note = drawGrid(plot.canvas,
                            n2Grid(vals, n2.n, b.type === "c64" ? part : null),
                            n2.n, n2.n) + ", " + n2.n + "×" + n2.n + " inputs";
        else if (key === "evec")
            note = drawGrid(plot.canvas, vals, n2.nev, n2.n) + ", ev × input";
        else
            note = renderLine(plot.canvas, vals);
        return label + " — " + note + n2Scalars(buf);
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
                } else {
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
        return LAYOUT_PREFIX + plot.nodeKey + ":" + plot.buffer + ":" +
               names.join(",");
    }
    function saveLayout() {
        try {
            sessionStorage.setItem(layoutKey(), JSON.stringify({
                y: plot.axisY, x: plot.axisX,
            }));
        } catch (e) {}
    }
    function loadLayout() {
        try {
            var raw = sessionStorage.getItem(layoutKey());
            if (!raw) return null;
            var v = JSON.parse(raw);
            if (!v || !Array.isArray(v.y) || !Array.isArray(v.x)) return null;
            return v;
        } catch (e) { return null; }
    }

    // Default axes: the two largest usable dimensions, the *largest on
    // x*.  The panel is wide and its height is capped (max-height: 30em)
    // while the width is the full card, so the longer dimension gets
    // several times more pixels — and fewer of its entries collapsed
    // into one cell — lying along x.
    function defaultAxes() {
        var use = usableDims().slice();
        use.sort(function (a, b) { return plot.dims[b] - plot.dims[a]; });
        plot.axisX = use.length ? [use[0]] : [];
        plot.axisY = use.length > 1 ? [use[1]] : [];
    }

    function validLayout(v) {
        if (!v) return false;
        var seen = {}, all = v.y.concat(v.x);
        for (var i = 0; i < all.length; i++) {
            var d = all[i];
            if (typeof d !== "number" || d < 0 || d >= plot.dims.length) return false;
            if (seen[d]) return false;  // a dimension can't be on both axes
            seen[d] = true;
        }
        return !!(v.y.length && v.x.length);
    }

    // Rebuild the axis pickers for the current shape.  A shape whose
    // dimension names are unchanged keeps the operator's layout (only
    // the labels are refreshed); a genuinely new shape falls back to the
    // remembered layout for that buffer, then to the defaults.
    function rebuildAxisControls() {
        var c = plot.controls;
        if (!plot.dims) {
            c.yPick.root.style.display = "none";
            c.xPick.root.style.display = "none";
            return;
        }
        c.yPick.root.style.display = "";
        c.xPick.root.style.display = "";
        var key = layoutKey();
        if (plot.layoutKey !== key) {
            plot.layoutKey = key;
            var saved = loadLayout();
            if (validLayout(saved)) {
                plot.axisY = saved.y;
                plot.axisX = saved.x;
            } else {
                defaultAxes();
            }
        }
        renderAxisPicker(c.yPick, plot.axisY, plot.axisX);
        renderAxisPicker(c.xPick, plot.axisX, plot.axisY);
    }

    function renderAxisPicker(pick, mine, theirs) {
        pick.summary.textContent = pick.name + ": " + axisLabel(mine);
        pick.body.textContent = "";
        usableDims().forEach(function (d) {
            var row = el("label", {
                style: "display: flex; align-items: center; gap: 0.4em; " +
                       "margin: 0 0 0.2em 0; white-space: nowrap; font-weight: normal;",
            });
            var box = el("input", { type: "checkbox", style: "margin: 0;" });
            box.checked = mine.indexOf(d) >= 0;
            box.addEventListener("change", function () {
                toggleAxisDim(pick === plot.controls.yPick, d, box.checked);
            });
            row.appendChild(box);
            row.appendChild(document.createTextNode(dimLabel(d) +
                (theirs.indexOf(d) >= 0 ? " — on " + (pick.name === "y" ? "x" : "y") : "")));
            pick.body.appendChild(row);
        });
    }

    // Ticking a dimension on one axis takes it off the other: a
    // dimension on both axes would index the same values twice.
    function toggleAxisDim(isY, d, on) {
        var mine = isY ? plot.axisY : plot.axisX;
        var other = isY ? plot.axisX : plot.axisY;
        var at = mine.indexOf(d);
        if (on && at < 0) {
            mine.push(d);
            mine.sort(function (a, b) { return a - b; });  // array order = slowest first
            var o = other.indexOf(d);
            if (o >= 0) other.splice(o, 1);
        } else if (!on && at >= 0) {
            mine.splice(at, 1);
        }
        rebuildAxisControls();
        saveLayout();
        rerender();
    }

    function dataUrl(len) {
        return "/api/node-buffer-data/" + plot.nodeKey +
            "?buffer=" + encodeURIComponent(plot.buffer) + "&len=" + len;
    }

    // Show the N2 block/part selectors only for N2 frames, and hide the
    // axis/combine controls there (blocks have fixed shapes).  The part
    // selector only applies to complex blocks.
    function updateN2Controls() {
        if (!plot) return;
        var c = plot.controls;
        var isN2 = !!plot.n2;
        c.block.parentElement.style.display = isN2 ? "" : "none";
        c.yPick.root.style.display = isN2 ? "none" : "";
        c.xPick.root.style.display = isN2 ? "none" : "";
        c.combine.parentElement.style.display = isN2 ? "none" : "";
        var blk = isN2 ? plot.n2.blocks[c.block.value] : null;
        c.part.parentElement.style.display =
            (blk && blk.type === "c64") ? "" : "none";
    }

    // The bits selector only means anything for packed boolean masks.
    function updateMaskControls() {
        if (!plot) return;
        var isMask = !!(plot.dec && plot.dec.mask && plot.dec.sub > 1);
        plot.controls.bits.parentElement.style.display =
            isMask && !plot.n2 ? "" : "none";
    }

    // Mean of the decoded values — for a mask that is the set fraction.
    // Computed once per fetch (not per render): the arrays run to tens
    // of millions of entries.
    function meanOf(values) {
        var sum = 0;
        for (var i = 0; i < values.length; i++) sum += values[i];
        return values.length ? sum / values.length : 0;
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
                updateN2Controls();
                updateMaskControls();
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
                    plot.lastValues = plot.dec.decode(buf);
                    plot.maskFrac = plot.dec.mask ? meanOf(plot.lastValues) : null;
                    shapeReceived(buf.byteLength);
                    rebuildAxisControls();
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

    function buildPanel(container, nodeKey, buffer) {
        var root = el("article", { style: "margin: 0.5em 0 0 0; padding: 0.75em 1em;" });
        var header = el("header", { style: "margin: 0 0 0.5em 0; padding: 0; display: flex; align-items: center; gap: 0.75em;" });
        var title = el("small");
        title.appendChild(el("code", {}, buffer));
        title.appendChild(document.createTextNode(" — live plot (5 s)"));
        var closeBtn = el("button", {
            type: "button", class: "outline",
            style: "width: auto; margin: 0 0 0 auto; padding: 0.05em 0.6em; font-size: 0.9em;",
        }, "Close");
        closeBtn.addEventListener("click", function () {
            forgetOpen(nodeKey); // an explicit close also stops auto-reopen
            closePlot();
        });
        header.appendChild(title);
        header.appendChild(closeBtn);
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
                                   ["histogram", "histogram"], ["line", "line"]]);
        // N2-only controls (shown when the descriptor says N2): which
        // frame block to plot, and which complex component.
        var block = select("block", []);
        var part = select("part", [["mag", "mag"], ["phase", "phase"],
                                   ["real", "real"], ["imag", "imag"]]);
        block.parentElement.style.display = "none";
        part.parentElement.style.display = "none";
        // Mask-only: how to treat the packed bit axis (see shapeReceived).
        var bits = select("bits", [["axis", "separate axis"],
                                   ["merge", "merge into last axis"]]);
        bits.parentElement.style.display = "none";
        // Axes are *composed*: tick one or more dimensions for each.
        // Everything not ticked is combined away — a dimension is
        // all-or-nothing, there is no picking one index out of it.
        var yPick = checkboxDropdown("y");
        var xPick = checkboxDropdown("x");
        controls.appendChild(yPick.root);
        controls.appendChild(xPick.root);
        // How the values landing in one cell become that cell — every
        // dimension off the axes, plus the many entries of a long axis
        // sharing a pixel.  max first (the default): outliers and
        // saturation are usually what an operator is looking for; mean
        // washes them out.  min is max's mirror and is what masks need —
        // with 1 = good, a cell reads 0 as soon as any value under it is
        // bad.
        var combine = select("combine", [["max", "max"], ["mean", "mean"],
                                         ["min", "min"]]);
        var fetchLen = select("fetch", FETCH_CHOICES.map(function (c) {
            return [String(c[0]), c[1]];
        }));
        fetchLen.value = String(DEFAULT_FETCH);
        var logWrap = el("label", { style: "display: flex; align-items: center; gap: 0.3em; margin: 0;" });
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
        root.appendChild(controls);

        // object-fit keeps the aspect ratio when max-height binds on a
        // tall heatmap (width and height constrained together would
        // otherwise stretch the pixels).
        var canvas = el("canvas", { style: "width: 100%; max-height: 30em; object-fit: contain; border-radius: 4px;" });
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
            nodeKey: nodeKey, buffer: buffer, root: root, canvas: canvas,
            status: status, visible: true, inflight: false,
            frameId: null, frameSize: null, staleSince: null,
            lastValues: null, lastBuf: null, lastBytes: 0,
            dims: null, dimNames: null, descDimNames: null, extents: null,
            shapeNote: null, descLoaded: false, dec: decoderFor(null),
            n2: null,
            controls: { mode: mode, block: block, part: part, bits: bits,
                        yPick: yPick, xPick: xPick,
                        combine: combine, fetchLen: fetchLen, log: log },
            bitsMode: "axis",
            axisY: [], axisX: [], layoutKey: null,
        };
        [mode, part, combine].forEach(function (c) {
            c.addEventListener("change", rerender);
        });
        // Re-shaping the same bytes: no refetch, just a different view
        // of the array that is already decoded.
        bits.addEventListener("change", function () {
            if (!plot) return;
            plot.bitsMode = bits.value;
            if (plot.lastBytes) {
                shapeReceived(plot.lastBytes);
                rebuildAxisControls();
                rerender();
            }
        });
        block.addEventListener("change", function () {
            updateN2Controls();
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
    }

    function openPlot(nodeKey, buffer, noScroll) {
        closePlot();
        var container = document.getElementById("buffer-plot");
        if (!container) return;
        buildPanel(container, nodeKey, buffer);
        rememberOpen(nodeKey, buffer);
        // The IntersectionObserver fires the first fetch (descriptor +
        // data, chained inside fetchData) once the panel is on-screen;
        // without observer support, fetch immediately.
        if (!plot.observer)
            fetchData();
        plot.timer = setInterval(tick, POLL_MS);
        if (!noScroll)
            root_scroll(container);
    }

    function root_scroll(node) {
        if (node.scrollIntoView)
            node.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }

    document.addEventListener("click", function (e) {
        var btn = e.target.closest && e.target.closest("[data-plot-buffer]");
        if (!btn) return;
        openPlot(btn.getAttribute("data-plot-node"),
                 btn.getAttribute("data-plot-buffer"));
    });

    // Auto-reopen the buffer watched last time this node page was open
    // (no scroll: don't yank the viewport on page load).
    (function () {
        var container = document.getElementById("buffer-plot");
        var nodeKey = container && container.getAttribute("data-node-key");
        var saved = nodeKey && savedOpen(nodeKey);
        if (saved) openPlot(nodeKey, saved, true);
    })();
})();
