// The element x element matrix (its page, and the acquisition page's
// visibility triangles): the cell under the pointer is named, with its half
// when the image has two, a click opens that product's coarse waterfall,
// and the row slider navigates.  The DOM is read, never built: labels go
// through textContent and the links are computed from element indices.
(function () {
    "use strict";
    var img = document.getElementById("matrix-img");
    var data = document.getElementById("matrix-data");
    if (!img || !data) return;
    var cfg = JSON.parse(data.textContent);
    // cfg's links are relative to the app root; behind the site proxy the
    // page's <meta name="choco-root"> carries the prefix ("" directly).
    var root = (document.querySelector('meta[name="choco-root"]') || {}).content || "";
    var n = cfg.n_el, labels = cfg.labels;
    var readout = document.getElementById("matrix-readout");

    function pad(i) { return ("0000" + i).slice(-4); }
    function cell(ev) {
        var r = img.getBoundingClientRect();
        var x = Math.floor((ev.clientX - r.left) / r.width * n);
        var y = Math.floor((ev.clientY - r.top) / r.height * n);
        if (x < 0 || y < 0 || x >= n || y >= n) return null;
        return [y, x];
    }
    img.addEventListener("mousemove", function (ev) {
        var c = cell(ev);
        if (!c || !readout) return;
        var half = !cfg.halves || c[0] === c[1] ? "" : (c[0] > c[1] ? cfg.halves.lower : cfg.halves.upper) + ": ";
        readout.textContent = half + (labels[c[0]] || c[0]) + " \u00d7 " + (labels[c[1]] || c[1])
            + "  (elements " + c[0] + ", " + c[1] + ")";
    });
    img.addEventListener("click", function (ev) {
        var c = cell(ev);
        if (!c) return;
        var a = Math.min(c[0], c[1]), b = Math.max(c[0], c[1]);
        window.location.href = root + cfg.coarse_view + "/wf_e" + pad(a) + "xe" + pad(b) + ".png";
    });
    var range = document.getElementById("matrix-row");
    if (range) {
        range.addEventListener("change", function () {
            window.location.href = root + cfg.page + "?band=" + cfg.band + "&row=" + range.value;
        });
    }
})();
