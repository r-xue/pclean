"""Data and image partitioning utilities.

Uses ``casatools.synthesisutils`` to divide data for continuum
(row-based) and cube (frequency-based) parallelism, and also
provides pure-Python fallback partitioners.
"""

from __future__ import annotations

import copy
import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pclean.config import PcleanConfig

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

_FREQ_UNITS: dict[str, float] = {
    'hz': 1.0,
    'khz': 1e3,
    'mhz': 1e6,
    'ghz': 1e9,
    'thz': 1e12,
}

_QTY_RE = re.compile(r'^([+-]?[\d.eE+-]+)\s*([a-zA-Z/]+)$')


def _parse_freq_hz(val: int | float | str) -> float | None:
    """Parse a frequency quantity string to Hz.

    Returns *None* if the value cannot be interpreted as a frequency.
    """
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
    return f'{hz / 1e9:.10f}GHz'


# ======================================================================
# Continuum (row-based) partitioning
# ======================================================================


def partition_continuum(
    config: PcleanConfig,
    nparts: int,
) -> list[dict]:
    """Partition data by visibility rows for parallel continuum imaging.

    Uses ``synthesisutils.contdatapartition()`` to split each MS across
    *nparts* workers.  Each returned dict is a CASA-native parameter
    bundle with selection narrowed to its row chunk and a unique partial
    image name.

    Args:
        config: Full imaging configuration.
        nparts: Number of partitions.

    Returns:
        One CASA-native bundle (dict) per worker.
    """
    ct = _ct()
    su = ct.synthesisutils()

    base_selpars = config.to_casa_selpars()

    try:
        partselpars = su.contdatapartition(
            selpars=base_selpars,
            npart=nparts,
        )
    finally:
        su.done()

    base_bundle = config.to_casa_bundle()
    result: list[dict] = []
    for part_idx in range(nparts):
        bundle = copy.deepcopy(base_bundle)
        # contdatapartition returns a nested dict:
        #   {'0': {'ms0': {selpars}, 'ms1': ...}, '1': ...}
        # outer key = partition index, inner keys = 'ms0', 'ms1', ...
        part_key = str(part_idx)
        for ms_key in sorted(base_selpars):
            if part_key in partselpars and ms_key in partselpars[part_key]:
                bundle['allselpars'][ms_key] = partselpars[part_key][ms_key]
        # Override imagename in all CASA dict groups
        new_name = f'{config.imagename}.part.{part_idx}'
        bundle['allimpars']['0']['imagename'] = new_name
        bundle['allnormpars']['0']['imagename'] = new_name
        bundle['allgridpars']['0']['imagename'] = new_name
        if 'allimages' in bundle['iterpars']:
            bundle['iterpars']['allimages']['0']['imagename'] = new_name
        result.append(bundle)

    log.info('Partitioned continuum data into %d chunks', len(result))
    return result


# ======================================================================
# Cube (frequency-based) partitioning
# ======================================================================


def partition_cube(
    config: PcleanConfig,
    nparts: int,
) -> list[PcleanConfig]:
    """Partition the output cube by frequency channels for parallel cube imaging.

    Uses ``synthesisutils.cubedataimagepartition()`` when possible,
    falling back to an even-split heuristic.

    Args:
        config: Full imaging configuration.
        nparts: Number of partitions.

    Returns:
        One ``PcleanConfig`` per worker, covering a non-overlapping
        range of output channels.
    """
    nchan = config.image.nchan
    if nchan <= 0:
        nchan = 1

    # Try the casatools utility first
    try:
        return _partition_cube_via_su(config, nparts, nchan)
    except Exception as exc:
        log.debug('synthesisutils cube partition failed (%s); using even-split fallback', exc)

    return _partition_cube_even(config, nparts, nchan)


def _partition_cube_via_su(
    config: PcleanConfig,
    nparts: int,
    nchan: int,
) -> list[PcleanConfig]:
    """Partition cube using ``synthesisutils.cubedataimagepartition``.

    Requires a coordinate system (csys) to be available in impars,
    which is typically not the case before imaging starts.
    """
    impars = config.to_casa_impars()
    csys = impars['0'].get('csys', {})
    if not csys:
        raise RuntimeError(
            'No coordinate system (csys) available; '
            'cannot use synthesisutils for cube partitioning'
        )

    ct = _ct()
    su = ct.synthesisutils()
    selpars = config.to_casa_selpars()
    try:
        allpars = su.cubedataimagepartition(
            selpars=selpars,
            incsys=csys,
            npart=nparts,
            nchannel=nchan,
        )
    finally:
        su.done()

    result: list[PcleanConfig] = []
    total_sub_nchan = 0
    for pidx in range(nparts):
        part_key = str(pidx)
        if part_key not in allpars:
            continue
        part_rec = allpars[part_key]
        sub_nc = part_rec.get('nchan', nchan)
        sub_start = str(part_rec.get('start', pidx))
        sub = config.make_subcube_config(sub_start, sub_nc, str(pidx))
        total_sub_nchan += sub_nc
        result.append(sub)

    if nchan > 0 and total_sub_nchan != nchan:
        raise RuntimeError(
            f'synthesisutils partition produced {total_sub_nchan} total '
            f'channels across {nparts} subcubes, expected {nchan}'
        )

    return result


def _partition_cube_even(
    config: PcleanConfig,
    nparts: int,
    nchan: int,
) -> list[PcleanConfig]:
    """Simple even partition of channels across *nparts* workers.

    When ``start`` and ``width`` are both frequency strings we compute
    per-subcube frequency starts so that each worker images a
    non-overlapping slice of the output cube.  Otherwise we fall back
    to channel-index based partitioning.
    """
    if nchan <= 0:
        log.warning('nchan unknown — falling back to single partition')
        nparts = 1
        nchan = -1

    orig_start = config.image.start
    orig_width = config.image.width

    start_hz = _parse_freq_hz(orig_start)
    width_hz = _parse_freq_hz(orig_width)

    # Greedy distribution: first (nchan % nparts) subcubes get one
    # extra channel, matching CASA's C++ cubedataimagepartition.
    chans_per_base = nchan // nparts
    remainder = nchan % nparts

    result: list[PcleanConfig] = []
    chan_offset = 0
    for i in range(nparts):
        nc = chans_per_base + (1 if i < remainder else 0)
        if nc <= 0:
            break

        if start_hz is not None and width_hz is not None:
            sub_start_hz = start_hz + chan_offset * width_hz
            sub_start = _format_freq_ghz(sub_start_hz)
        else:
            sub_start = str(chan_offset)

        log.info('  subcube %d: start=%s  nchan=%d  (chan_offset=%d)', i, sub_start, nc, chan_offset)
        sub = config.make_subcube_config(sub_start, nc, str(i))
        result.append(sub)
        chan_offset += nc

    log.info('Even-split cube partition: %d sub-cubes, total_chan=%d', len(result), chan_offset)
    return result


# ======================================================================
# Helpers for partial-image naming
# ======================================================================


def partial_image_name(base: str, part_index: int) -> str:
    """Return the partial-image path for a given partition index."""
    workdir = f'{base}.workdirectory'
    return f'{workdir}/{base}.n{part_index}'
