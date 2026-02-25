"""
Data and image partitioning utilities.

Uses ``casatools.synthesisutils`` to divide data for continuum
(row-based) and cube (frequency-based) parallelism, and also
provides pure-Python fallback partitioners.
"""

from __future__ import annotations

import copy
import logging
import math
import re
from typing import Any, Dict, List, Optional, Tuple, Union

from pclean.params import PcleanParams

log = logging.getLogger(__name__)

_casatools = None


def _ct():
    global _casatools
    if _casatools is None:
        import casatools as ct
        _casatools = ct
    return _casatools


# ======================================================================
# Frequency / quantity parsing helpers
# ======================================================================

_FREQ_UNITS: Dict[str, float] = {
    "hz": 1.0,
    "khz": 1e3,
    "mhz": 1e6,
    "ghz": 1e9,
    "thz": 1e12,
}

_QTY_RE = re.compile(r"^([+-]?[\d.eE+-]+)\s*([a-zA-Z/]+)$")


def _parse_freq_hz(val: Union[int, float, str]) -> Optional[float]:
    """Parse a frequency quantity string to Hz.  Returns *None* if
    the value cannot be interpreted as a frequency."""
    if isinstance(val, (int, float)):
        return None  # bare number = channel index, not a frequency
    val = str(val).strip()
    if not val:
        return None
    m = _QTY_RE.match(val)
    if m is None:
        return None
    number, unit = float(m.group(1)), m.group(2).lower()
    factor = _FREQ_UNITS.get(unit)
    if factor is None:
        return None
    return number * factor


def _format_freq_ghz(hz: float) -> str:
    """Format a value in Hz as a GHz string."""
    return f"{hz / 1e9:.10f}GHz"


# ======================================================================
# Continuum (row-based) partitioning
# ======================================================================


def partition_continuum(
    params: PcleanParams,
    nparts: int,
) -> List[PcleanParams]:
    """
    Partition data by visibility rows for parallel continuum imaging.

    Uses ``synthesisutils.contdatapartition()`` to split each MS across
    *nparts* workers.  Each returned ``PcleanParams`` has selection
    parameters narrowed to its row chunk and a unique partial image name.

    Parameters
    ----------
    params : PcleanParams
        Original (full) parameter set.
    nparts : int
        Number of partitions.

    Returns
    -------
    list[PcleanParams]
        One ``PcleanParams`` per worker.
    """
    ct = _ct()
    su = ct.synthesisutils()

    try:
        partselpars = su.contdatapartition(
            selpars=params.allselpars, npart=nparts,
        )
    finally:
        su.done()

    result: List[PcleanParams] = []
    for part_idx in range(nparts):
        # contdatapartition returns keys like '0.0', '0.1', ... for
        # MS-0 partition-0, MS-0 partition-1, etc.
        sub_sel: Dict[str, dict] = {}
        for ms_key in sorted(params.allselpars.keys()):
            pkey = f"{ms_key}.{part_idx}"
            if pkey in partselpars:
                sub_sel[ms_key] = partselpars[pkey]
            else:
                # Fallback — use full selection for this MS
                sub_sel[ms_key] = copy.deepcopy(params.allselpars[ms_key])
        sub = params.make_rowchunk_params(sub_sel, str(part_idx))
        result.append(sub)

    log.info("Partitioned continuum data into %d chunks", len(result))
    return result


# ======================================================================
# Cube (frequency-based) partitioning
# ======================================================================


def partition_cube(
    params: PcleanParams,
    nparts: int,
) -> List[PcleanParams]:
    """
    Partition the output cube by frequency channels for parallel cube
    imaging.

    Uses ``synthesisutils.cubedataimagepartition()`` when possible,
    falling back to an even-split heuristic.

    Parameters
    ----------
    params : PcleanParams
        Original (full) parameter set.
    nparts : int
        Number of partitions.

    Returns
    -------
    list[PcleanParams]
        One ``PcleanParams`` per worker, covering a non-overlapping
        range of output channels.
    """
    nchan = _resolve_nchan(params)
    if nchan <= 0:
        nchan = 1

    # Try the casatools utility first
    try:
        return _partition_cube_via_su(params, nparts, nchan)
    except Exception as exc:
        log.debug("synthesisutils cube partition failed (%s); "
                   "using even-split fallback", exc)

    return _partition_cube_even(params, nparts, nchan)


def _resolve_nchan(params: PcleanParams) -> int:
    """Best-effort determination of the total output channel count."""
    nchan = params.allimpars["0"].get("nchan", -1)
    if nchan > 0:
        return nchan
    # If nchan == -1 the user wants "all channels".  We can't know the
    # exact number without inspecting the MS, so return a large sentinel
    # and let the caller clamp later.
    return -1


def _partition_cube_via_su(
    params: PcleanParams,
    nparts: int,
    nchan: int,
) -> List[PcleanParams]:
    csys = params.allimpars["0"].get("csys", {})
    if not csys:
        raise RuntimeError(
            "No coordinate system (csys) available in impars; "
            "cannot use synthesisutils for cube partitioning"
        )

    ct = _ct()
    su = ct.synthesisutils()
    try:
        # This returns per-partition selpars and impars.
        allpars = su.cubedataimagepartition(
            selpars=params.allselpars,
            incsys=csys,
            npart=nparts,
            nchannel=nchan,
        )
    finally:
        su.done()

    result: List[PcleanParams] = []
    total_sub_nchan = 0
    for pidx in range(nparts):
        p = params.clone()
        # Update selection / image params from synthesisutils output
        for ms_key in sorted(params.allselpars.keys()):
            pkey = f"{ms_key}.{pidx}"
            if pkey in allpars:
                p.allselpars[ms_key] = allpars[pkey]
        imp_key = f"0.{pidx}"
        if imp_key in allpars:
            for k, v in allpars[imp_key].items():
                p.allimpars["0"][k] = v
        sub_nc = p.allimpars["0"].get("nchan", nchan)
        total_sub_nchan += sub_nc
        new_name = f"{params.imagename}.subcube.{pidx}"
        p.allimpars["0"]["imagename"] = new_name
        p.allnormpars["0"]["imagename"] = new_name
        p.allgridpars["0"]["imagename"] = new_name
        if "allimages" in p.iterpars:
            p.iterpars["allimages"]["0"]["imagename"] = new_name
        result.append(p)

    # Validate: total subcube channels must equal original nchan
    if nchan > 0 and total_sub_nchan != nchan:
        raise RuntimeError(
            f"synthesisutils partition produced {total_sub_nchan} total "
            f"channels across {nparts} subcubes, expected {nchan}"
        )

    return result


def _partition_cube_even(
    params: PcleanParams,
    nparts: int,
    nchan: int,
) -> List[PcleanParams]:
    """Simple even partition of channels across *nparts* workers.

    When ``start`` and ``width`` are both frequency strings we compute
    per-subcube frequency starts so that each worker images a
    non-overlapping slice of the output cube.  Otherwise we fall back
    to channel-index based partitioning.
    """
    if nchan <= 0:
        log.warning("nchan unknown — falling back to single partition")
        nparts = 1
        nchan = -1

    imp = params.allimpars["0"]
    orig_start = imp.get("start", "")
    orig_width = imp.get("width", "")

    start_hz = _parse_freq_hz(orig_start)
    width_hz = _parse_freq_hz(orig_width)

    # Greedy distribution: first (nchan % nparts) subcubes get one
    # extra channel, matching CASA's C++ cubedataimagepartition.
    chans_per_base = nchan // nparts
    remainder = nchan % nparts

    result: List[PcleanParams] = []
    chan_offset = 0
    for i in range(nparts):
        nc = chans_per_base + (1 if i < remainder else 0)
        if nc <= 0:
            break

        if start_hz is not None and width_hz is not None:
            # Frequency-based start for this subcube
            sub_start_hz = start_hz + chan_offset * width_hz
            sub_start = _format_freq_ghz(sub_start_hz)
        else:
            # Channel-index fallback
            sub_start = str(chan_offset)

        log.info("  subcube %d: start=%s  nchan=%d  (chan_offset=%d)",
                 i, sub_start, nc, chan_offset)
        sub = params.make_subcube_params(sub_start, nc, str(i))
        result.append(sub)
        chan_offset += nc

    log.info("Even-split cube partition: %d sub-cubes, total_chan=%d",
             len(result), chan_offset)
    return result


# ======================================================================
# Helpers for partial-image naming
# ======================================================================


def partial_image_name(base: str, part_index: int) -> str:
    """Return the partial-image path for a given partition index."""
    workdir = f"{base}.workdirectory"
    return f"{workdir}/{base}.n{part_index}"
