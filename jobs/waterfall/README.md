# waterfall

Per-acquisition upper-triangle visibility waterfalls, rendered from the
kotekan N² files under `/mnt/cs00/data/kotekan_vis_files/subset/` and
served through choco's `/files` page.

One PNG per correlation product per acquisition, which **grows** as
kotekan writes: each new source file appends its time samples as
scanlines. Nothing is recomputed and no written byte is rewritten.

```sh
./waterfall.sh /etc/choco/waterfall.yaml          # one pass
python waterfall.py -c waterfall.yaml -n          # what is pending
python waterfall.py -c waterfall.yaml --acq acq_20260723_232332_046022478
python waterfall.py -c waterfall.yaml --max-files 0 --level 4   # backfill
```

## Why it is shaped this way

**Files are immutable once they appear.** kotekan writes into
`.partial/vis_<idx>.h5` and *renames* the finished file into the
acquisition directory — verified live, with the inode preserved across
the move. So the file list only ever grows, which is what makes the
whole pipeline append-only. `.partial` is never read.

**One sequential pass per file.** `vis` is chunked
`(16 freq, 16 prod, 20 time)`. Pulling a single product's column touches
385 chunks scattered through 700 MB and runs at ~6 MB/s; a sequential
sweep runs at ~100 MB/s. Every product together costs ~1.9× what one
product costs, so the reducer never loops products over a file.

**Rows are time.** Frequency-major compresses ~10% better and cannot be
appended to at all. Appending costs 2.5% (crosses) to 15% (autos) over an
ideal monolithic encode, and still beats one file per source file.

**The equalization lives in the palette.** Histogram-equalizing the
*pixels* costs ~15% (it is maximum-entropy by construction) and, worse,
goes stale while the acquisition is still growing. Instead the pixels
hold a linear log stretch and the 256-entry `PLTE` carries the
equalization, re-derived from an accumulated index histogram and
rewritten in place in constant time. Measured against true equalization:
ΔE mean 0.7–1.5, about the just-noticeable-difference threshold.

**Gains are not divided out.** |V| carries the raw instrumental scale, so
a mid-acquisition gain update draws as a visible step rather than being
normalised away. Because the value scale is frozen per acquisition from
its first file (padded outward by half the observed log span), that step
stays on-scale.

**Newest acquisition first, capped work per run.** A full backfill is
~24 h; the live acquisition must never wait behind it.

## Layout

```
<waterfalls_dir>/<root>/<acq>/
    index.json      the commit record — shape, scale, per-image state
    freq.npy        frequency axis, written once
    times.bin       int64 per scanline, appended
    counts.npy      (n_prod, 256) index histogram, drives the equalization
    thumbs.dat      (n_files, n_prod, n_bins) uint8, appended
    e0000/wf_e0000xe0000.png    th_e0000xe0000.png
```

Element indices are zero-padded to four digits and sharded by the first
input, so a lexical sort is a numeric sort and no directory holds more
entries than there are elements.

The tree is a **sibling of the data, not a subdirectory of each
acquisition**: the acquisition directories are owned by the NFS-squashed
writer and are not writable by anyone else. It contains only relative
paths, so it can be moved or copied wholesale with the data.

## Crash safety

`index.json` is the commit point and the only authority on what is done.
It records the byte length of the last completed write for every
append-only file, and is written *after* every other write. A run that
died partway through therefore leaves files that are too long, never too
short; each append seeks to the recorded offset, truncates, and redoes
its work. There is no repair pass, and re-running is always safe.

## Cost

Measured on choco.site against live files, 32 elements / 528 products:

| | |
|---|---|
| per source file | 36.7 MB across 528 images |
| read + write per file | ~4.5 s + ~3.3 s |
| peak RSS | ~200 MB (the index buffer is `n_prod × n_time × n_freq`) |
| whole `subset` archive | ~200 GB in ~16,000 files, 8.1% of the source |

Storage is O(N²) in element count: 100 elements is ~358 MB per source
file and ~157 GB per observing day. `max_files_per_run` bounds the work,
not the size.

## Files

- `wfpng.py` — the append-only palette PNG: encode, append, in-place
  palette swap, a strict verifying decoder, quantization, equalization.
- `reduce.py` — one file to scanlines: axes, value scale, the streaming
  quantizer, thumbnail rows.
- `store.py` — the per-acquisition store and its commit discipline.
- `waterfall.py` — the job: discovery, budget, state file, exit codes.
