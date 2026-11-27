// beam.js -- the beam page's live waterfall.
//
// kotekan's IncoherentBeamSearch writes the samples of the last seconds,
// binned for a display, to a peek-held buffer once per search pass; every
// frame is the whole image.  choco serves the rows after a given row
// number (/beam/waterfall?after=), so this script asks a few times a
// second for what it has not drawn yet and misses no row as long as it
// asks more often than the image is long (20 s).  When it could not (the
// tab slept, the network stalled), the jump in the row numbers is drawn
// as a line and counted on the page.
//
// Rows arrive in bursts of a pass (8 rows); they are drawn at their own
// rate, one row of pixels per row, a few rows behind the newest, so the
// picture moves steadily.  Frequency runs across, time down, the newest
// row at the top.  A value is the bin's sum in sigma; a bin with nothing
// in use (masked, no data) is left blank.
(function () {
    "use strict";

    var box = document.getElementById("beam-waterfall");
    if (!box) return;
    var ROOT = (document.querySelector('meta[name="choco-root"]') || {}).content || "";
    var canvas = box.querySelector(".beam-frame canvas");
    var ctx = canvas.getContext("2d");
    var stateEl = box.querySelector(".beam-state");
    var clockEl = box.querySelector(".beam-clock");
    var missedEl = box.querySelector(".beam-missed");
    var freqEl = box.querySelector(".beam-axis");
    var timeEl = box.querySelector(".beam-time");

    var POLL_MS = +box.dataset.pollMs || 300;
    var RETRY_MS = 3000;
    var SHOWN = 600;        // rows on the screen (25 s at 42 ms a row)
    var LEAD = 16;          // rows held back so the scroll does not wait for a poll
    var NO_DATA = -128;     // the stage's mark of a bin with nothing in use
    var PER_SIGMA = 16;     // the stage's units per sigma
    var RANGE = 4;          // sigma at the ends of the colour scale
    var GAP = [179, 54, 46];

    // One colour per int8 value: blue below zero, grey at zero, pale
    // yellow above; a blank (transparent) pixel for no data.
    var LUT = new Uint8ClampedArray(256 * 4);
    (function () {
        var zero = [70, 76, 88], low = [22, 52, 104], high = [247, 232, 160];
        for (var i = 0; i < 256; i++) {
            var v = i < 128 ? i : i - 256;
            if (v === NO_DATA) continue;
            var t = Math.max(-1, Math.min(1, v / PER_SIGMA / RANGE));
            var to = t < 0 ? low : high, a = Math.abs(t);
            for (var k = 0; k < 3; k++) LUT[i * 4 + k] = zero[k] + (to[k] - zero[k]) * a;
            LUT[i * 4 + 3] = 255;
        }
    })();
    (function () {
        var key = box.querySelector(".beam-key canvas");
        if (!key) return;
        var kctx = key.getContext("2d"), img = kctx.createImageData(key.width, 1);
        for (var x = 0; x < key.width; x++) {
            var v = Math.round((x / (key.width - 1) * 2 - 1) * RANGE * PER_SIGMA) & 255;
            for (var k = 0; k < 4; k++) img.data[x * 4 + k] = LUT[v * 4 + k];
        }
        kctx.putImageData(img, 0, 0);
    })();

    var columns = 0;        // of the image; set by the first reply
    var rowS = 0;           // seconds per row
    var last = null;        // number of the newest row received
    var queue = [];         // rows received and not yet drawn: {n, data} or {gap}
    var missed = 0;         // rows that were gone before they were asked for
    var clock = null;       // unix time at which row 0 would start, when kotekan's time is known
    var drawn = null;       // number of the newest row on the screen

    function label(parent, text, style) {
        var span = document.createElement("span");
        span.textContent = text;
        for (var k in style) span.style[k] = style[k];
        parent.appendChild(span);
    }

    function buildAxes() {
        freqEl.textContent = "";
        timeEl.textContent = "";
        var low = parseFloat(box.dataset.lowMhz), width = parseFloat(box.dataset.columnMhz);
        if (isFinite(low) && width > 0) {
            var span = columns * width, step = span > 800 ? 100 : span > 300 ? 50 : 20;
            for (var f = Math.ceil(low / step) * step; f <= low + span; f += step)
                label(freqEl, f + (f + step > low + span ? " MHz" : ""),
                      {left: ((f - low) / span * 100) + "%"});
        }
        for (var s = 0; s <= SHOWN * rowS; s += 5)
            label(timeEl, s ? "−" + s + " s" : "now", {top: (s / rowS / SHOWN * 100) + "%"});
    }

    function setColumns(n, seconds) {
        if (n === columns && seconds === rowS) return;
        columns = n;
        rowS = seconds;
        canvas.width = columns;
        canvas.height = SHOWN;
        queue = [];
        drawn = null;
        buildAxes();
    }

    // Takes one reply: the rows after `last`, oldest first.
    function take(headers, buffer) {
        var first = parseInt(headers.get("X-Beam-First"), 10);
        var n = parseInt(headers.get("X-Beam-Columns"), 10);
        var newest = parseInt(headers.get("X-Beam-Newest"), 10);
        var seconds = parseFloat(headers.get("X-Beam-Row-Seconds"));
        if (!(n > 0) || !(seconds > 0) || !isFinite(first) || !isFinite(newest)
                || buffer.byteLength % n !== 0)
            throw new Error("unexpected reply (signed out?)");
        setColumns(n, seconds);
        var end = parseFloat(headers.get("X-Beam-End-Unix"));
        clock = isFinite(end) ? end - (newest + 1) * rowS : null;
        if (last !== null && newest < last) {
            // The row numbers went back: the stream started again.
            last = null;
            return;
        }
        var rows = buffer.byteLength / n;
        if (!rows) return;
        if (last !== null && first > last + 1) {
            missed += first - last - 1;
            queue.push({gap: true});
        }
        var bytes = new Uint8Array(buffer);
        for (var r = 0; r < rows; r++)
            queue.push({n: first + r, data: bytes.subarray(r * n, (r + 1) * n)});
        last = first + rows - 1;
    }

    // Moves the picture down and puts `rows` (oldest first) at the top.
    function draw(rows) {
        var k = Math.min(rows.length, SHOWN);
        rows = rows.slice(rows.length - k);
        if (k < SHOWN) {
            ctx.globalCompositeOperation = "copy";
            ctx.drawImage(canvas, 0, 0, columns, SHOWN - k, 0, k, columns, SHOWN - k);
            ctx.globalCompositeOperation = "source-over";
        }
        var img = ctx.createImageData(columns, k), out = img.data;
        for (var r = 0; r < k; r++) {
            var row = rows[k - 1 - r], o = r * columns * 4;
            for (var c = 0; c < columns; c++, o += 4) {
                if (row.gap) {
                    out[o] = GAP[0]; out[o + 1] = GAP[1]; out[o + 2] = GAP[2]; out[o + 3] = 255;
                } else {
                    var i = row.data[c] * 4;
                    out[o] = LUT[i]; out[o + 1] = LUT[i + 1]; out[o + 2] = LUT[i + 2]; out[o + 3] = LUT[i + 3];
                }
            }
            if (!row.gap) drawn = row.n;
        }
        ctx.putImageData(img, 0, 0);
    }

    // Rows leave the queue at their own rate, a little faster when it is
    // long and slower when it is short, so it stays near LEAD rows; a
    // queue far beyond that (the first reply, a tab that slept) is drawn
    // at once.
    var due = 0, before = null;
    function frame(now) {
        requestAnimationFrame(frame);
        var dt = before === null ? 0 : Math.min((now - before) / 1000, 1);
        before = now;
        if (!rowS || !queue.length) { due = 0; return; }
        if (queue.length > 4 * LEAD) {
            draw(queue.splice(0, queue.length - LEAD));
            due = 0;
            return;
        }
        var speed = queue.length > 1.5 * LEAD ? 1.15 : queue.length < 0.5 * LEAD ? 0.9 : 1;
        due += dt / rowS * speed;
        var k = Math.min(Math.floor(due), queue.length);
        if (k < 1) return;
        due -= k;
        draw(queue.splice(0, k));
    }

    function pad(v) { return (v < 10 ? "0" : "") + v; }

    function showStatus() {
        if (clock !== null && drawn !== null) {
            var unix = clock + (drawn + 1) * rowS, d = new Date(unix * 1000);
            clockEl.textContent = pad(d.getUTCHours()) + ":" + pad(d.getUTCMinutes()) + ":"
                + pad(d.getUTCSeconds()) + " UTC · " + (Date.now() / 1000 - unix).toFixed(1) + " s behind";
        } else {
            clockEl.textContent = "";
        }
        missedEl.textContent = missed ? missed + " rows missed" : "";
    }

    function poll() {
        var url = ROOT + "/beam/waterfall" + (last !== null ? "?after=" + last : "");
        fetch(url, {cache: "no-store", credentials: "same-origin"}).then(function (resp) {
            if (!resp.ok)
                return resp.json().catch(function () { return {}; }).then(function (body) {
                    throw new Error(body.error || "HTTP " + resp.status);
                });
            return resp.arrayBuffer().then(function (buffer) { take(resp.headers, buffer); });
        }).then(function () {
            stateEl.textContent = "live";
            setTimeout(poll, POLL_MS);
        }, function (e) {
            stateEl.textContent = "no data: " + e.message;
            setTimeout(poll, RETRY_MS);
        });
    }

    poll();
    requestAnimationFrame(frame);
    setInterval(showStatus, 250);
})();
