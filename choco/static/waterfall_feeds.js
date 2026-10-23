// The feeds panel on the acquisition page: the cell under the pointer is
// named in a tooltip (feed, file time, coherence, mask state).  The DOM is
// read, never built from markup: the tooltip's text goes through
// textContent.
(function () {
    "use strict";
    var box = document.getElementById("feeds");
    var data = document.getElementById("feeds-data");
    var tip = document.getElementById("feeds-tip");
    if (!box || !data || !tip) return;
    var cfg = JSON.parse(data.textContent);
    var svg = box.querySelector("svg");
    var vb = svg.viewBox.baseVal;

    function coherence(level) {
        if (!level) return "no data";
        return Math.pow(10, -3 + 3 * (level - 1) / 254).toPrecision(2);
    }
    function pad(n) { return (n < 10 ? "0" : "") + n; }
    // the bin under the pointer, in site-local time (the column's offset)
    function clock(col, frac) {
        var c = cfg.cols[col];
        if (!c) return "file " + (col + 1) + ", no time";
        var bin = Math.min(c[3] - 1, Math.floor(frac * c[3]));
        var d = new Date((c[0] + bin * c[1] + c[2]) * 1000);
        return pad(d.getUTCMonth() + 1) + "-" + pad(d.getUTCDate()) + " " + pad(d.getUTCHours())
            + ":" + pad(d.getUTCMinutes()) + ":" + pad(d.getUTCSeconds());
    }
    function maskAt(label, x) {
        var segs = cfg.masks[label] || [];
        for (var i = 0; i < segs.length; i++) {
            if (x >= segs[i][0] && x < segs[i][1]) return segs[i][2] ? "masked on some streams" : "masked";
        }
        return "not masked";
    }
    function hit(ev) {
        var r = svg.getBoundingClientRect();
        var ux = (ev.clientX - r.left) / r.width * vb.width;
        var uy = (ev.clientY - r.top) / r.height * vb.height;
        var x = (ux - cfg.x0) / cfg.w * cfg.n_cols;
        if (x < 0 || x >= cfg.n_cols) return null;
        for (var i = 0; i < cfg.rows.length; i++) {
            var row = cfg.rows[i];
            if (uy >= row[0] && uy < row[0] + cfg.sub) return {row: row, x: x, col: Math.floor(x)};
        }
        return null;
    }
    svg.addEventListener("mousemove", function (ev) {
        var h = hit(ev);
        if (!h) { tip.style.display = "none"; return; }
        var label = h.row[1], e = cfg.order[h.row[2]];
        var level = (cfg.levels[h.col] || [])[e];
        tip.textContent = label + " · " + clock(h.col, h.x - h.col)
            + " · coherence " + coherence(level) + " · " + maskAt(label, h.x);
        var b = box.getBoundingClientRect();
        var left = ev.clientX - b.left + 12;
        tip.style.display = "block";
        if (left + tip.offsetWidth > b.width) left = ev.clientX - b.left - tip.offsetWidth - 12;
        left = Math.max(0, left);
        tip.style.left = left + "px";
        tip.style.top = (ev.clientY - b.top + 14) + "px";
    });
    svg.addEventListener("mouseleave", function () { tip.style.display = "none"; });
})();
