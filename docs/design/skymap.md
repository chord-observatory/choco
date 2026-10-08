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
exactly when a new render landed.  choco reads the PNG back from
``<state_dir>/skymap/skymap.png``, where the job writes it — its state
directory, so neither side configures the path (2026-10; ``skymap.image_file``
and the job's ``output`` are refused); the job's own settings live in
``/etc/choco/skymap.yaml``, seeded by install.  A failed pointing lookup exits 2 and leaves the previous image up
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
well inside the unit's caps, and each write stays atomic on its own.
``night: false`` skips the second render.  choco reads the night PNG back
from ``skymap-night.png`` beside the day image; the route
is unauthenticated for the same wall-display reason as ``/skymap.png`` and
exposes the same single cluster fact.  The web UI itself is pinned to the
light theme, so the landing card shows the day image and links the night one
(``?v=`` mtime-busted like the day image); a browser-side ``<picture>``
switch would never fire.

## Backdrop: the 408 MHz HEALPix map (2026-10)

The backdrop was a pre-rendered 862×431 PNG of unknown provenance.  It is now
drawn from data: the destriped 408 MHz all-sky map (Haslam et al. 1982,
reprocessed by Remazeilles et al. 2015, MNRAS 451, 4311; sources kept), a
12.6 MB HEALPix FITS from NASA LAMBDA: nside 512 (6.9′ pixels, 56′ beam),
RING order, Galactic coordinates, kelvin.  It is **fetched by
`choco.sh install`** (`fetch_sky_map`, pinned by sha256) into
`/opt/choco/jobs/skymap/`, not committed: it would double the repository, and
install's `rsync` of `jobs/` has no `--delete`, so the copy survives later
installs.  A hand run from the tree needs the file next to `skymap.py`
(gitignored there).

The reading and the pixel lookup live in `choco/healpix.py`, shared with
the mapmaker's context image (jobs only: numpy, astropy's FITS reader on
demand; the web process never imports it).  There is no healpy.
`ang2pix_ring` is the HEALPix RING formula (Górski et al. 2005), checked in
`tests/test_healpix.py` against an independent transcription of
`pix2ang_ring` at every pixel centre.  The skymap's own `mollweide_raster`
inverts the Mollweide projection onto the frame the overlay draws in: x = −l,
so longitude increases to the left with the Galactic centre in the middle,
and north is at the top.  A test pins that orientation.  The raster
(1800×900) is built once per run and shared by the day and night renders,
about 0.25 s.

Colour is log T_b, linear between the 1st and 99.7th sky percentiles, through
matplotlib's inferno.  The candidates (jet, turbo, viridis, inferno,
cubehelix and gist_earth, each with the log stretch and with histogram
equalisation) were compared on 2026-10-08.  Equalisation paints a broad band
around the plane in the top colours and competes with the overlays.  Inferno
on log keeps the plane and the bright sources as the only bright things.
`background_fade` went from 0.42 to 0.55 at the same time: inferno's dark
bottom washes out at the old fade.

`background_map` names the file (`""` for a plain page); the old
`background_image` key is refused, naming its replacement.  A missing or
unreadable map still renders both images, on a plain page, and exits 2.  A
file in another layout (NESTED, not Galactic, not a full sky) is a config
error, exit 1: the projection assumes exactly this one.
