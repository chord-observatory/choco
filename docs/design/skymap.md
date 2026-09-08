# Sky-map strip plot

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Sky-map strip plot

``jobs/skymap/`` renders a Mollweide all-sky view (equatorial grid in galactic
coordinates over a faded radio-sky backdrop) every 5 minutes: the CHORD drift-
scan strip(s) with HPBW bands, one per entry of skymap.yaml's ``beams:`` list
— each entry a source name, a declination, or the token ``pointing`` (the live
pointing(s) read from choco; every group's distinct pointing becomes a beam),
each beam in its own colour with list order picking the primary — the
**current** Sun and Moon positions (single markers, not trajectories), a
translucent "beam now" disk per beam — the 300 MHz HPBW footprint on the sky,
projected honestly, so it stretches near the map edge, splits across the seam,
and closes into a cap when it swallows a galactic pole (which a dec +22° beam
does daily; the NGP sits at dec +27.1°) — where the meridian crosses its
strip, and local-time labels (primary strip only) for where the beams will
point over the next 24 h — exact by construction, since the beam always points
at RA = LST, which is also what replaced the original standalone script's
solar-transit search.  The pointing is read live from choco
(``/api/config/<group>`` → a recursive search for ``dish_coelev_deg``;
declination = DRAO latitude + co-elevation, 49.32° − 27.3° ≈ +22° = Tau A,
matching the config's own comment), with every group read when ``skymap.yaml``
names none, near-duplicate beams (<0.1°) drawn once; ``nearest_major_source``
names the source in the title when one lies within 1.5°.  IERS auto-download
is off with ``auto_max_age=None`` — a render must never block on the network,
and the bundled tables' stale predictive tail costs milliseconds of dUT1,
invisible at plot scale (eop is the job whose business is fresh IERS data).
The PNG is written **atomically** (savefig to a ``.tmp`` sibling with an
explicit ``format='png'``, since matplotlib infers format from the suffix,
then ``os.replace``), because choco serves the same file: ``/skymap.png`` is,
with ``/metrics``, one of only two **unauthenticated** routes (wall displays
can't do LDAP sessions; the image's only cluster fact is the pointing),
answering conditional GETs with 304s off the file mtime.  The landing page
shows it below the service table as an htmx card (``/partials/skymap``, every
5 min) whose ``<img>`` URL carries the file mtime as ``?v=`` so a swap fetches
exactly when a new render landed.  choco's ``skymap:`` config block holds only
``image_file`` (where to read the PNG back — must match ``skymap.yaml``'s
``output``); the job's own settings live in ``/etc/choco/skymap.yaml``, seeded
by install.  A failed pointing lookup exits 2 and leaves the previous image up
— staleness is visible in the image's own title timestamp.  matplotlib joined
the ``[jobs]`` extra for this job; the render is ~3.5 s and the unit carries
the usual caps.

## Night mode (2026-09)

Every run renders the same instant twice: the **day** image (white page, the
landing card) and a **night** image (dark navy page, for wall displays in a
dim control room), written to ``skymap.yaml``'s ``output`` and
``output_night`` and served at ``/skymap.png`` and ``/skymap-night.png``.
The two differ only in palette — every colour the plot uses is a named role
in ``THEMES`` (page, label box, marker halo, grid, RA/Dec ink, the beam
palettes, and an ``rc`` dict for what matplotlib styles itself: title, ticks,
projection outline, legend box), and ``plot_skymap`` takes the theme name —
so geometry, label placement and pixel dimensions are identical and the two
images are directly comparable (a test checks the shapes match and the
corner pixel is white in one and dark in the other).  The sky backdrop fades
toward the page colour rather than toward white, so ``background_fade`` keeps
one meaning for both.  ``now`` is fixed once in ``main`` before either render
so Sun, Moon and beam-now agree; the night render is a second ~3 s of CPU,
well inside the unit's caps, and each write stays atomic on its own.  An
empty ``output_night`` skips the second render.  choco reads the night PNG
back from the ``skymap:`` block's optional ``night_image_file``; the route
is unauthenticated for the same wall-display reason as ``/skymap.png`` and
exposes the same single cluster fact.  The web UI itself is pinned to the
light theme, so the landing card shows the day image and links the night one
(``?v=`` mtime-busted like the day image); a browser-side ``<picture>``
switch would never fire.
