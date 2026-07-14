# Review criteria

Checks to apply to any change in this repo. Each is one rule, with the precedent
that produced it in italics. `bffs` is a small, modular run-once feed-flagging
script: a core (`bffs.py`), a kotekan reader (`kotekan_io.py`), and one module per
source under `sources/` (see [README](README.md)). These criteria keep it small
and consistent.

### 1. Earn every function
- A single-use function is worth keeping **only if** it is unit-tested, shared by
  ≥2 callers, a distinct stage, an external-I/O boundary, or an accessor clearer
  than its inline form. Otherwise inline it, with a comment where the name carried
  meaning. *(inlined `flag`, `_read_overrides`, `_payload`, `load_state`,
  `save_state`, `_as_str`, `_read_block`)*
- Length alone never justifies a function. *(merged `_read_block` into
  `read_autocorr`)*

### 2. No speculative generality
- Every config key, parameter, and branch has a real caller **today** — not a
  hypothetical one. *(removed `meta_path`, `align_good`, the four-name field probe)*
- **One source of truth**: never read the same fact two ways or keep two
  representations of it. *(one `kotekan_file`; `index_map/input` is the only feed
  order; `load_map`/`project` shared instead of copied per source)*

### 3. Simplest tool that works
- Prefer the stdlib or a plain construct; add machinery only when it earns its
  keep. *(argparse over click, urllib over requests, dataclass over pydantic,
  numpy over scipy. The source dispatch grew from an inline if/elif to a lazy
  `sources.get` registry only once there were four sources.)*
- Every dependency is **load-bearing** — name what breaks without it. *(PyYAML
  earns it: commentable config)*

### 4. No self-defeating feedback
- An input to a decision must not depend on that decision's own output. *(dropped
  the kotekan-`enabled` connectivity source — it was downstream of our flags;
  live connectivity now comes from the independent `power` source)*
- Prefer signals that recover on their own over state that stays stuck.
  *(power-outlier / power / fpga all self-heal; the latch never did)*

### 5. Precise, lean names
- A name states what the thing **is or does** — never a near-synonym.
  *(`power-outlier` not `data-variance` — nothing computes a variance; `power` not
  `amp`; "feed labels" not "feed axis" — a list, not a dimension)*
- Precise but not verbose; spell out a convention that could be read two ways.
  *(`True` = good, the opposite of numpy's `.ma`)*

### 6. Comments and claims
- Comment the non-obvious; never narrate the obvious. *(the `np.divide(..., where=)`
  skip; the degenerate-MAD branch)*
- Claim only what you've verified; otherwise state the weaker claim that's still
  true. *(provisional FPGA metric names / power-channel numbering are flagged as
  unverified until the hardware is live; "data timestamp" → "moment")*

### 7. Docs and tests track the code
- README, example, and tests match the current shape; no stale reference survives
  a change. *(the package refactor swept the README, example, tests, and this file)*
- Tests assert observable behavior, not internal helpers; a cut feature's tests
  migrate to the surviving mechanism. *(corrupt-state via `run()`; per-module tests
  in `tests/test_<module>.py`, shared fixtures in `tests/testhelpers.py`)*

### 8. Consistent source layout
- Every flagging source is a module under `sources/` exposing
  `mask(src, labels, kotekan_file) -> good-mask` (`True` = good); `bffs` dispatches
  via `sources.get(kind)`. *(manual, power-outlier, power, fpga)*
- Shared machinery lives once: kotekan reads in `kotekan_io.py`; the CSV map loader
  and axis projection in `sources/common.py`. A source's map (CSV) sits beside it
  in `sources/`; tests live in `tests/` as `test_<module>.py`.
- An external-service source (polls a service + needs a padloper map) is
  **provisional** until the map and hardware land, and stays runnable standalone
  (`python -m sources.<name>`). *(power, fpga)*
