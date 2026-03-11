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


def _resolve_frequency_grid(
    config: PcleanConfig,
    nchan: int,
) -> list[float] | None:
    """Compute the actual CASA output frequency grid for the full cube.

    Creates a temporary ``synthesisimager``, calls ``selectdata`` +
    ``defineimage`` with the full *nchan*, and reads back the
    per-channel frequencies that ``MSTransformRegridder::calcChanFreqs``
    produces.  This gives us the *exact* grid that a monolithic
    ``tclean(nchan=N)`` would use, so that subcube start frequencies
    are consistent with the regridded data channels.

    Returns:
        A list of *nchan* channel centre frequencies in Hz, or *None*
        if the grid could not be resolved.
    """
    import shutil
    import tempfile

    ct = _ct()

    # Build a unique temporary imagename so that concurrent calls
    # (e.g. tests) do not collide.
    tmpdir = tempfile.mkdtemp(prefix='pclean_freqgrid_')
    imgname = f'{tmpdir}/_freqgrid'

    si = None
    sn = None
    try:
        si = ct.synthesisimager()
        selpars = config.to_casa_selpars()
        for ms_key in sorted(selpars):
            selrec = dict(selpars[ms_key])
            selrec.setdefault('usescratch', False)
            selrec.setdefault('readonly', True)
            log.debug(
                '_resolve_frequency_grid selectdata[%s]: msname=%r  type=%s',
                ms_key,
                selrec.get('msname'),
                type(selrec.get('msname')).__name__,
            )
            si.selectdata(selpars=selrec)

        # Disable cube gridding so makepsf runs in-process (no
        # sub-imager / normalizer setup needed for the grid query).
        si.setcubegridding(False)

        impars = dict(config.to_casa_impars()['0'])
        impars['imagename'] = imgname
        # Use a tiny spatial grid — we only need the spectral axis.
        impars['imsize'] = [32, 32]
        impars['nchan'] = nchan
        impars['restart'] = False

        gridpars = dict(config.to_casa_gridpars()['0'])
        gridpars['imagename'] = imgname

        si.defineimage(impars=impars, gridpars=gridpars)

        # We need makepsf to materialise the image on disk so we can
        # read its coordinate system.  A normalizer is required for
        # makepsf to succeed (it gathers/divides PSF weights).
        sn = ct.synthesisnormalizer()
        normpars = dict(config.to_casa_normpars()['0'])
        normpars['imagename'] = imgname
        sn.setupnormalizer(normpars=normpars)

        si.makepsf()
        sn.gatherpsfweight()
        sn.dividepsfbyweight()

        ia = ct.image()
        cs = None
        try:
            ia.open(imgname + '.psf')
            cs = ia.coordsys()
            shape = ia.shape()
            n = int(shape[3])
            freqs = [float(cs.toworld([0, 0, 0, i])['numeric'][3]) for i in range(n)]
        finally:
            if cs is not None:
                try:
                    cs.done()
                except Exception:
                    pass
            try:
                ia.done()
            except Exception:
                pass

        if n != nchan:
            log.warning(
                'Frequency grid resolution produced %d channels '
                '(expected %d) — falling back to arithmetic grid',
                n, nchan,
            )
            return None

        log.info(
            'Resolved frequency grid: %d channels, '
            'freq[0]=%.6f GHz, delta=%.6f MHz',
            n, freqs[0] / 1e9,
            (freqs[1] - freqs[0]) / 1e6 if n > 1 else 0.0,
        )
        return freqs

    except Exception as exc:
        log.debug(
            'Could not resolve frequency grid via defineImage: %s',
            exc,
        )
        return None
    finally:
        if si is not None:
            try:
                si.done()
            except Exception:
                pass
        if sn is not None:
            try:
                sn.done()
            except Exception:
                pass
        shutil.rmtree(tmpdir, ignore_errors=True)


def _partition_cube_even(
    config: PcleanConfig,
    nparts: int,
    nchan: int,
) -> list[PcleanConfig]:
    """Simple even partition of channels across *nparts* workers.

    When ``start`` and ``width`` are both frequency strings we first
    resolve the *actual* output frequency grid via a lightweight
    ``defineImage(nchan=full)`` call.  This ensures subcube start
    frequencies match the grid that ``MSTransformRegridder::calcChanFreqs``
    produces for the full cube, avoiding the per-channel alignment
    drift that occurs when each single-channel subcube independently
    calls ``calcChanFreqs``.

    Falls back to arithmetic ``start + i * width`` when the grid
    cannot be resolved.
    """
    if nchan <= 0:
        log.warning('nchan unknown — falling back to single partition')
        nparts = 1
        nchan = -1

    orig_start = config.image.start
    orig_width = config.image.width

    start_hz = _parse_freq_hz(orig_start)
    width_hz = _parse_freq_hz(orig_width)

    # Try to resolve the actual frequency grid that CASA would produce
    # for a monolithic nchan-channel image.
    resolved_freqs: list[float] | None = None
    if start_hz is not None and width_hz is not None and nchan > 1:
        resolved_freqs = _resolve_frequency_grid(config, nchan)

    # For briggsbwtaper: pre-compute fracbw from the *full* cube so that
    # single-channel subcubes inherit a valid fractional bandwidth.
    # Without this, nchan=1 subcubes get fracbw=0 and CASA's
    # BriggsCubeWeightor rejects the value.
    if (
        config.weight.weighting == 'briggsbwtaper'
        and config.weight.fracbw is None
        and nchan > 1
    ):
        if resolved_freqs is not None and len(resolved_freqs) >= 2:
            min_f = min(resolved_freqs)
            max_f = max(resolved_freqs)
            config.weight.fracbw = 2.0 * (max_f - min_f) / (max_f + min_f)
        elif start_hz is not None and width_hz is not None:
            end_f = start_hz + (nchan - 1) * width_hz
            min_f = min(start_hz, end_f)
            max_f = max(start_hz, end_f)
            config.weight.fracbw = 2.0 * (max_f - min_f) / (max_f + min_f)
        else:
            # Integer start/width: resolve frequency grid to get fracbw only.
            # Do not assign to resolved_freqs — the user asked for channel-based
            # partitioning, so subcube starts should remain channel indices.
            freqs = _resolve_frequency_grid(config, nchan)
            if freqs is not None and len(freqs) >= 2:
                min_f = min(freqs)
                max_f = max(freqs)
                config.weight.fracbw = 2.0 * (max_f - min_f) / (max_f + min_f)
        if config.weight.fracbw is not None:
            log.info(
                'Pre-computed fracbw=%.6g for briggsbwtaper from full cube',
                config.weight.fracbw,
            )

    # Greedy distribution: first (nchan % nparts) subcubes get one
    # extra channel, matching CASA's C++ cubedataimagepartition.
    chans_per_base = nchan // nparts
    remainder = nchan % nparts

    # Compute the frequency-domain channel width so that subcubes whose
    # ``start`` is a frequency string also carry a matching ``width``.
    # Without this, CASA rejects the mixed unit types (e.g. start in
    # GHz but width as a bare channel count).
    freq_width: str | None = None
    if resolved_freqs is not None and len(resolved_freqs) >= 2:
        freq_width = _format_freq_ghz(resolved_freqs[1] - resolved_freqs[0])
    elif start_hz is not None and width_hz is not None:
        freq_width = _format_freq_ghz(width_hz)

    result: list[PcleanConfig] = []
    chan_offset = 0
    for i in range(nparts):
        nc = chans_per_base + (1 if i < remainder else 0)
        if nc <= 0:
            break

        if resolved_freqs is not None:
            # Use the exact frequency from the resolved grid.
            sub_start = _format_freq_ghz(resolved_freqs[chan_offset])
            sub_width = freq_width
        elif start_hz is not None and width_hz is not None:
            sub_start_hz = start_hz + chan_offset * width_hz
            sub_start = _format_freq_ghz(sub_start_hz)
            sub_width = freq_width
        else:
            sub_start = str(chan_offset)
            sub_width = None

        log.info(
            '  subcube %d: start=%s  nchan=%d  (chan_offset=%d)',
            i,
            sub_start,
            nc,
            chan_offset,
        )
        sub = config.make_subcube_config(sub_start, nc, str(i), width=sub_width)
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
