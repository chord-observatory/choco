# The instrument page

`/instrument` draws the signal path: power → feeds → CRS boards (ADCs) →
switch in-ports → switch out-ports → X-engine NIC ports → the receiver →
files, with the state of every hop.  It runs top to bottom, because pages
scroll vertically: one column per board above the switch and one per NIC
port below it, so every link is a straight drop and each feed has room for
its label.  It started (2026-10-25) as a switch
monitoring page and grew into an overview that may later absorb the PDB
page and the nodes table.

## The devices where choco can, Prometheus for the rest

Since 2026-10-26 the page is the front page, polled every 5 s while it is
open (a 30 s background loop keeps the header badge fresh), and reads the
devices directly where it can, for latency:

- each node's kotekan `/metrics`, for the NIC counters: the rates are the
  change between two readings, timed by kotekan's own sample timestamps;
- the switch's eAPI, given an account whose EOS role allows only `show`.
  choco refuses any other command before it sends it (`SwitchEapi`), but
  the account is what makes the reader read-only.  The rates are timed by
  the switch's `lastUpdateTimestamp`, since it refreshes its counters every
  few seconds, not at each read.  Until the account exists the switch comes
  from Prometheus, and the FEC counters and optics always do (they need
  `phy detail` and `transceiver`, which the switch monitor already runs).

Prometheus, read at most every 30 s, keeps what choco cannot read itself:

- the Keysight supply: its SCPI socket may take one client, which the
  exporter holds;
- the PDU and the iDRACs: SNMP, already scraped; slow-changing;
- node_exporter: it accepts Prometheus alone (`IPAddressAllow`);
- the CRS crate controller's `crs_control_*`.

choco keeps talking to kotekan, power_db and fpga_master directly, as
before; the page uses choco's registry for node state, the PDB monitor for
feed power and bffs's state file for the feed flags.

The PDU outlets are matched to devices by their labels on the PDU: a host's
outlets carry its name (two per server, one per supply), `arista` is the
switch and `f-engine` the CRS crate.  An outlet's onOff sensor value does
not say whether it is switched on, so the page shows power, not state.

## A feed's dot

One dot carries two facts: the fill is the PDB channel (solid on, hollow
off, pale unknown), the colour bffs's verdict (green good, red flagged,
grey when bffs has no state).  The four common cases stay apart at a
glance: solid green is normal, hollow red is an unpowered feed bffs has
flagged (expected), solid red is a powered feed bffs flags (look at it),
hollow green is an unpowered feed bffs has not flagged yet.

## Where the wiring comes from

- **Feed → board, ADC:** kotekan's `dish_inputs` (the D axis) and
  `crs_board_remap` (output slot *i* takes source_id `remap[i]`; slots 0–7
  are pol 0 and 8–15 pol 1 of dish groups 0–7).  The 2026-10-17 cabling has
  one board per (dish group, pol), X on the odd crate slot, eight ADCs per
  board.  `missing_source_ids` marks a board as expected silent.
- **Board → switch port:** the operator's 2026-10-17 table (crate slot ↔
  Ethernet port), in `wiring.csv`.
- **Switch port → node NIC port:** nothing recorded it.  The first table
  (2026-10-25) was inferred from Prometheus history: the 16 node-facing
  ports fall into eight pairs with identical link-change records, and each
  pair was matched to a node by the times its kotekan went down alone (the
  fleet-wide restarts match every node and were left out).  cx27 is
  certain (Et1/1 in July 2026, Et32/1 down until its port 1 was fixed on
  2026-10-23); the others are marked "inferred" and which port of a pair is
  NUMA 0 is assumed.  The switch's static multicast MAC table (one group
  per stream, `0123.3467.8900`–`890f`) would settle it once the monitor or a
  read-only account can read it.

Because the switch wiring is kept by hand, the page checks it: a
node-facing switch port whose sent packet rate differs by more than 5 %
from the receive rate of the NIC port the table puts behind it is
flagged.

## Thresholds

- Idle: below 1000 packets/s (a board port carries about 1.5 Mpps, a NIC
  port about 1.5 Mpps of one stream from every board).
- Drops: missed + ring-full + errors above 1e-4 of the receive rate.  On
  2026-10-25 every NIC missed 5–30 packets/s out of 1.46 Mpps (about 1e-5),
  which the files do not show; "any drop" would have painted every port.
- The switch monitor is stale after 180 s (it polls every 30 s).
