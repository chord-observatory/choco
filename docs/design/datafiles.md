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
one nobody opens when the filesystem is fine.  Its health is a **separate,
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
