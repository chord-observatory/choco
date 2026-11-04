# The instrument page

`/instrument` draws the signal path: power → feeds → CRS boards (ADCs) →
switch in-ports → switch out-ports → X-engine NIC ports → the receiver →
files, with the state of every hop.  It runs top to bottom, because pages
scroll vertically: one column per board above the switch and one per NIC
port below it, so every link is a straight drop and each feed has room for
its label.  It started (2026-10-25) as a switch
monitoring page and grew into an overview.

## The tables on their own pages (2026-10-26)

The front page keeps the diagram; each table moved to the page of the
thing it describes, and the diagram's cards link there:

- `/switch`: the board links, the X-engine links end to end (the switch
  port's send rate and discards beside the NIC's receive rate and drops,
  so a wiring or drop problem reads along one row), the ports the wiring
  table does not name, and the wiring table's state.  It shares the
  front page's view (`web._instrument_view`) and its 5 s poll.  Its
  badge, SWITCH, replaced PATH: the CHOCO mark already leads to the
  front page.
- `/nodes`: each node's server (whether its node_exporter answers, its
  outlet power; `instrument.host_health`) as extra columns, from the
  monitor's last read, so the 2 s table poll costs no request.
- `/power/analog` (was `/service/pdb`, then `/service/power`, both of
  which redirect; the controls keep their `/service/pdb/set*` URLs): the
  PDB grid, the Keysight dish supply and the channel map with its
  cross-check; `/power/rack` the Raritan PDU's outlets and inlet (since
  2026-11-04, the analog chain and the rack apart).  The Analog power
  item is the PDB controller's health, made worse when the dish supply
  reports a fault, output off or no answer; the Rack power item is the
  PDU as Prometheus last read it (`web._rack_badge`).

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
  `phy detail` and `transceiver`, which the switch monitor already runs);
- fpga_master, through choco's own `FpgaMonitor` (2026-11-04): beside
  `/status` it reads `/get-hw-map` (the crate and the board in each slot)
  and `/get-monitoring-data` (fpga_master's Prometheus text: each board's
  core and rail temperatures, QSFP, PLL input and overflow counts, with
  fpga_master's own sample times).  The text is `Metrics(latest_only=True)`
  on fpga_master's side, so reading it beside the Prometheus exporter
  takes nothing from the exporter (two reads 2 s apart carried the same
  1387 samples).  This replaced the `crs_control_*` Prometheus queries,
  whose exporter only curled the same two endpoints.

Since 2026-11-04 (user's wish: "everything from sources if possible") the
slow sources are read directly too, every 30 s (`SLOW_EVERY_S`):

- each kotekan host's node_exporter, and the transfer host's
  (`vis_files.archive.host`, where the transfer job writes its textfile
  series): `?collect[]=netdev&collect[]=textfile` keeps the answer small;
  rates come from successive readings like the NICs'.  node_exporter
  allows choco's address (chord-sysops `node_exporter_allowed_sources`).
  The transfer card shows the host's send rate during a run and what the
  last run sent otherwise: a 24 h total would need history choco does
  not keep.
- the rack PDU over SNMP v2c: four `snmpbulkwalk` walks of PDU2-MIB
  (outlet names, outlet sensor values and digits, inlet values; sensor
  type 5 is active power, 0 decimals on this PDU), under a second from
  the site network.  SNMP rather than the JSON-RPC API: the read
  community already exists and needs no account; `snmpbulkwalk` is a host
  tool (the `snmp` package), like `dot`.
- the switch's error counters, PHY and optics over the same eAPI, the
  commands and fields the switch monitor uses (`show interfaces counters
  errors`, `phy detail`, `transceiver`); with the account, nothing of the
  switch comes from Prometheus.

The iDRACs are no longer read (user, 2026-11-04): the X-engine page and
the node cards lost the server state and the temperatures with them.
node_exporter and the PDU are reported in the monitor's `sources` but do
not set its health, which is the signal path's (the Switch item): the PDU
has its own item, the hosts their own column.

Prometheus keeps two things: the Keysight supply (its SCPI socket takes
very few sessions, slow to clean up, and the exporter holds one; choco
would have to own it and publish it for Prometheus instead) and the
switch until choco has its account.

choco keeps talking to kotekan, power_db and fpga_master directly, as
before; the page uses choco's registry for node state, the PDB monitor for
feed power and bffs's state file for the feed flags.

The PDU outlets are matched to devices by their labels on the PDU: a host's
outlets carry its name (two per server, one per supply), `arista` is the
switch and `f-engine` the CRS crate.  An outlet's onOff sensor value does
not say whether it is switched on, so the page shows power, not state.

## A board's card

Until 2026-11-04 a board's card was painted from its switch port's
receive rate, so with fpga_master down every card stayed green: the
boards stream whether or not their controller is up.  Now the card
carries two facts, like a feed's dot: the card itself is the board as
fpga_master sees it (`board_state`: in the hardware map, sampled within
`BOARD_STALE_S`, core temperature under the FPGACoreTemperature alert
lines, QSFP present), and the link below it is the switch port.  The card
goes grey, with the reason as its word, whenever fpga_master has nothing
current to say: not configured, down, not running, the slot not in its
map, an empty slot, no samples, or samples that stopped (fpga_master's
rounds froze once on 2026-10-31 while it kept answering `/status`).  The
"CRS boards" lane counts the boards fpga_master reports ok and keeps the
links as a note; its tone is fpga_master's health when that is not ok.
fpga_master's slot index is kotekan's source_id (crate slot − 1); the
cards and the wiring table say crate slots, the FPGA page's board table
shows both.

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
