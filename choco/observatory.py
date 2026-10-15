"""The observatory's fixed facts, shared by the jobs: where CHORD stands,
and where the bright radio sources the jobs calibrate on and mark are.

Stdlib only, like ``choco.dishlabels``.  A job's own YAML may still
override any of these (eigencal's ``observer``, the mapmaker's
``calibrators``); these are the defaults, written once.
"""

#: The site as kotekan's hdf5N2Write records it in every file
#: (``itrs_lat_deg`` / ``itrs_lon_deg``): the frame the correlator's own
#: geometry uses.  Height above the ellipsoid, metres.
SITE_LAT_DEG = 49.32075144444
SITE_LON_DEG = -119.62081125
SITE_HEIGHT_M = 545.0

#: Bright sources, J2000 (ICRS) right ascension and declination in degrees
#: (SIMBAD).  The calibrators the mapmaker can self-calibrate on, eigencal's
#: default source, and what skymap labels.
SOURCES = {
    "Cyg A": (299.86815, 40.73392),     # 3C 405, radio galaxy
    "Cas A": (350.86642, 58.81178),     # supernova remnant
    "Tau A": (83.63308, 22.01450),      # Crab Nebula
    "Vir A": (187.70593, 12.39112),     # M87
    "Sgr A*": (266.41683, -29.00781),   # the Galactic centre
}
