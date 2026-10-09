# The data-file page

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Data-file page

``/files`` answers "what has kotekan written to disk": for each configured
root (``vis_files.roots``, an explicit list of paths — no base-path guessing,
so a new data area is a deliberate config edit), every immediate subdirectory
with the **number, total size and newest mtime** of the ``.h5`` files sitting
directly in it, newest acquisition first.  The scan is **one level deep by
design**, which is a statement about the data rather than a shortcut: a root's
children are acquisitions and an acquisition's files sit directly inside it,
so the count means "files in this acquisition".  That deliberately excludes
kotekan's ``.partial`` staging subdirectory — a file still being written is
not part of the acquisition and counting it would report a directory one file
larger than it can be read as — and it means a root whose files nest deeper
(the archived ``old/`` layout) honestly reports zeros instead of silently
descending into a different shape.  Three implementation choices carry the
weight.  The walk runs in **gevent's threadpool**
(``gevent.get_hub().threadpool.map``, one thread per root so two mounts are
walked at once): these roots are NFS, so ``stat`` is not the cheap local call
it looks like — 20k of them cost ~300 ms warm — and a wedged mount blocks for
as long as it likes, which *in the hub* would freeze the sync loop, the
monitors and every other request.  Measured: a direct scan lets a 5 ms
heartbeat greenlet tick **zero** times in 114 ms, the threadpooled one keeps
it ticking with a 5.7 ms worst stall.  It is the h5py-subprocess reasoning one
step down in weight — a blocking syscall to isolate, but no C extension, so a
thread suffices.  The result is **cached** (``DataFileScan``, 30 s, serialised
behind a ``BoundedSemaphore`` exactly as ``GainArchive`` is) so several
viewers cost one walk, with an explicit Rescan button passing ``?refresh=1``.
And the table is a **lazily loaded partial** (``/partials/files``, ``hx-
trigger="load"``, no poll) for the same reason the FPGA gain card is —
measured against the live mount the page paints in 6 ms and the table follows
in 186 ms cold, 1 ms cached — with no timed refresh at all, since file counts
change on the timescale of an acquisition and a timer would keep touching the
mount for nobody.  Failures are collected the way ``Registry.reload`` collects
them: a missing or unreadable root costs that section (an error row, named),
an unreadable subdirectory costs that row, and a file that vanishes mid-scan
costs nothing at all — kotekan rotating a file out is not an error.
``/api/files`` serves the same scan as JSON in raw bytes and unix timestamps;
the page's ``filesize`` filter is display, not data.  The page is reached from
a **DATA badge** in the header strip (green up / red down, linking to
``/files``) rather than a dashboard button, which puts it alongside the other
services and makes a dead mount visible from every page instead of only the
one nobody opens when the filesystem is fine.  Since 2026-10-05 the badge
and the page also carry the **waterfall renderer**, the job that reads these
mounts: the tag shows the worse of the two halves (ui.md, the service-strip
bullet) and the page ends with the renderer's status, state file and
journal, so a mount problem and the renderer's reaction to it are read in
one place.  The mounts' health is a **separate,
deliberately tiny probe** (``DataFileScan.check_once``, its own 30 s greenlet
like the hardware monitors): one ``readdir`` per root, never a walk, because
the strip polls on every page.  A bare ``stat`` would not do — on NFS it is
routinely served from the attribute cache and keeps answering long after the
server has stopped — so the probe reads a single directory entry to force a
round trip while staying O(1).  Roots partly up read ``degraded`` (yellow)
rather than ``down``: the box is fine, one mount is not.  The wedged-mount
case is the one that shapes the code: a blocked NFS syscall cannot be
interrupted, so the probe runs in the threadpool and the *greenlet* gives up
after ``CHECK_TIMEOUT_S``, reporting ``down``; the thread stays stuck until
the mount recovers, and ``_probing`` is what stops each subsequent tick from
piling another blocked thread behind it — a stuck probe *is* the answer, so it
is reported without waiting again.

### Span and acquisition notes (2026-10)

Each row also shows the **span** of its files — the oldest and newest ``.h5``
mtime, in UTC — taken from the same ``scandir`` pass, so it costs nothing extra.
An mtime is when a file was last written, so the span starts one file (~200 s)
after the acquisition did; close enough to place a run in time.  The same pass
notices a ``README.md`` in the acquisition (written by ``tools/acqnotes``) and
the row grows an info button.  The README is **not** read by the scan: the
button's popover fetches ``/files/notes/<root index>/<acq>`` on first open
(``hx-trigger="toggle once"``).  That route never turns the caller's string
into a path: the root is an index into the configured roots and the
acquisition must be a row the cached scan listed with notes; the read runs in
the threadpool with a 5 s timeout and a 256 KiB cap.  The Markdown is rendered
by ``datafiles.notes_html``, a deliberately small subset (headings, paragraphs,
``-`` lists, pipe tables, `code`, **bold**, *emphasis*) that escapes every line before
recognising anything, so a README can produce only those elements — a Markdown
library would be a new dependency and would pass raw HTML through by default.
The same overlay shows the root's ``timeline.yaml`` behind a Timeline button
in the root header: ``tools/acqnotes/render.py`` copies the curated history
into the data root, because production choco runs from the installed package
and cannot see the repository.  ``/files/timeline/<root index>`` reads it the
same way (allowlisted by the scan, threadpool, timeout, size cap), parses it
with ``yaml.safe_load`` and renders one row per entry, oldest first, every
text field through the same escape-first inline formatter.  The overlay dims
the page through the popover's ``::backdrop`` and has an explicit close
button outside the swapped body, so the load cannot replace it.
