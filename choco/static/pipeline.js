/* Full-page pipeline view: pan/zoom for the inline SVG, keyboard
 * activation of buffer nodes, and the dark/light theme toggle.
 *
 * Clicks on buffer nodes are handled by bufferplot.js's delegated
 * [data-plot-buffer] listener (the plot opens in the fixed popup
 * overlay) — this file only makes sure the click that ends a drag
 * doesn't count as one, and that Enter/Space reach that same listener.
 */
(function () {
    "use strict";

    var graph = document.getElementById("pipeline-graph");
    if (!graph) return;

    function on(id, event, handler) {
        var el = document.getElementById(id);
        if (el) el.addEventListener(event, handler);
        return el;
    }

    // --- theme toggle (persisted; default dark, set in <head>) ---
    on("pg-theme", "click", function () {
        var root = document.documentElement;
        var next = root.dataset.theme === "dark" ? "light" : "dark";
        root.dataset.theme = next;
        try { localStorage.setItem("chocoPipelineTheme", next); } catch (e) {}
    });

    // --- zoom: Fit / 1:1 buttons + scroll wheel ---
    // The zoom width outlives an htmx swap (Refresh, layout change): a
    // new SVG arrives with no inline style, and the container has no
    // fit/full class while zoomed, so without this the graph would snap
    // to its natural ~9700 px width and strand the operator mid-pan.
    var zoomWidth = null;

    function clearInlineZoom() {
        zoomWidth = null;
        var svg = graph.querySelector("svg");
        if (!svg) return;
        svg.style.width = "";
        svg.style.height = "";
        svg.style.maxWidth = "";
        svg.style.maxHeight = "";
    }
    function applyZoom(svg, width) {
        graph.classList.remove("fit", "full");
        svg.style.maxWidth = "none";
        svg.style.maxHeight = "none";
        svg.style.width = width + "px";
        svg.style.height = "auto";
    }
    on("pg-fit", "click", function () {
        clearInlineZoom();
        graph.classList.add("fit");
        graph.classList.remove("full");
    });
    on("pg-full", "click", function () {
        clearInlineZoom();
        graph.classList.add("full");
        graph.classList.remove("fit");
    });

    // Re-apply the zoom to freshly swapped-in SVG markup.
    document.body.addEventListener("htmx:afterSwap", function (e) {
        if (e.target !== graph) return;
        var svg = graph.querySelector("svg");
        if (!svg) return;
        if (zoomWidth) applyZoom(svg, zoomWidth);
        else if (!graph.classList.contains("full")) graph.classList.add("fit");
    });

    // The graph is the whole page here, so the wheel zooms rather than
    // scrolls — panning is drag (or shift+wheel, kept native as an
    // escape hatch).  ctrl+wheel lands here too, which is how a trackpad
    // pinch arrives, so pinch-to-zoom works for free.  Zooming sets an
    // explicit pixel width on the svg and leaves fit/full mode.
    graph.addEventListener("wheel", function (e) {
        if (e.shiftKey) return;
        var svg = graph.querySelector("svg");
        if (!svg) return;
        e.preventDefault();
        var srect = svg.getBoundingClientRect();
        if (!srect.width || !srect.height) return;
        // Natural width in px (graphviz emits pt; 1pt = 4/3 px), for
        // sane zoom clamping.
        var natural = parseFloat(svg.getAttribute("width")) || 1000;
        if (/pt$/.test(svg.getAttribute("width") || "")) natural *= 4 / 3;
        var factor = Math.exp(-e.deltaY * 0.0015);
        var newW = Math.min(Math.max(srect.width * factor, 150), natural * 8);
        // Keep the content point under the cursor fixed: remember the
        // cursor's relative position in the svg, resize, then correct
        // the pane scroll by how far that point moved.
        var relX = (e.clientX - srect.left) / srect.width;
        var relY = (e.clientY - srect.top) / srect.height;
        applyZoom(svg, newW);
        zoomWidth = newW;
        var nrect = svg.getBoundingClientRect();
        graph.scrollLeft += (nrect.left + relX * nrect.width) - e.clientX;
        graph.scrollTop += (nrect.top + relY * nrect.height) - e.clientY;
    }, { passive: false });

    // --- drag to pan ---
    var down = null, dragged = false;
    graph.addEventListener("pointerdown", function (e) {
        if (e.button !== 0) return;
        down = { x: e.clientX, y: e.clientY,
                 l: graph.scrollLeft, t: graph.scrollTop };
        dragged = false;
    });
    graph.addEventListener("pointermove", function (e) {
        if (!down) return;
        var dx = e.clientX - down.x, dy = e.clientY - down.y;
        if (!dragged && Math.abs(dx) + Math.abs(dy) > 6) {
            dragged = true;
            graph.classList.add("dragging");
        }
        if (dragged) {
            graph.scrollLeft = down.l - dx;
            graph.scrollTop = down.t - dy;
        }
    });
    ["pointerup", "pointercancel", "pointerleave"].forEach(function (ev) {
        graph.addEventListener(ev, function () {
            down = null;
            graph.classList.remove("dragging");
        });
    });
    // Swallow the click that ends a drag (capture phase runs before
    // bufferplot.js's document-level handler) so panning over a buffer
    // node doesn't open its plot.
    graph.addEventListener("click", function (e) {
        if (dragged) {
            e.stopPropagation();
            e.preventDefault();
            dragged = false;
        }
    }, true);

    // --- keyboard activation of buffer nodes ---
    // The sanitizer stamps tabindex/role="button" on clickable buffers;
    // SVG elements have no .click(), so synthesise the event that
    // bufferplot.js's delegated listener is already waiting for.
    graph.addEventListener("keydown", function (e) {
        if (e.key !== "Enter" && e.key !== " " && e.key !== "Spacebar") return;
        var target = e.target.closest && e.target.closest("[data-plot-buffer]");
        if (!target) return;
        e.preventDefault();
        target.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    });

    // --- keep ?layout= in the URL in step with the selector ---
    // so a refresh or a shared link comes back with the same routing.
    on("pg-layout", "change", function (e) {
        try {
            var url = new URL(window.location.href);
            url.searchParams.set("layout", e.target.value);
            window.history.replaceState(null, "", url);
        } catch (err) {}
    });
})();
