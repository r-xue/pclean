"""Dask worker task functions.

Each function in this module is a **pure top-level function** that can be
serialised by Dask and executed on a remote worker.  They accept plain
dicts (serialised ``PcleanConfig`` or CASA-native bundles) in order to
avoid pickle issues and instantiate ``casatools`` objects on the worker
side.

Design rationale:

* Workers must import ``casatools`` locally -- the C++ tool objects
  cannot be pickled or transferred between processes.
* All file I/O (images, MSes) uses shared-filesystem paths so the
  coordinator can later gather partial products.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


# ======================================================================
# setweighting compatibility wrapper
# ======================================================================


def _safe_setweighting(si, wp: dict) -> None:
    """Call ``si.setweighting(**wp)`` with a fallback for unpatched casatools.

    If *wp* contains ``fracbw`` and the installed ``casatools`` does not
    support it (i.e. the ``CAS-14520`` patch has not been applied),
    the call will raise ``TypeError: unexpected keyword argument
    'fracbw'``.  In that case we:

    1. Drop the ``fracbw`` key.
    2. If ``rmode == 'bwtaper'`` (briggsbwtaper), downgrade to
       ``rmode='norm'`` (standard Briggs) so the C++ layer does not
       receive an unrecognised mode string.
    3. Log a warning so the user knows the weighting has been degraded.

    This keeps pclean functional with *any* casatools build while still
    using the optimal weighting when the patch is present.
    """
    try:
        si.setweighting(**wp)
    except TypeError as exc:
        if 'fracbw' not in str(exc):
            raise
        wp = dict(wp)  # don't mutate caller's dict
        wp.pop('fracbw', None)
        if wp.get('rmode') == 'bwtaper':
            wp['rmode'] = 'norm'
            log.warning(
                'casatools does not support the fracbw parameter '
                '(CAS-14520 patch not applied) — falling back from '
                'briggsbwtaper to standard briggs weighting',
            )
        else:
            log.warning(
                'casatools does not support the fracbw parameter '
                '(CAS-14520 patch not applied) — dropping fracbw',
            )
        si.setweighting(**wp)


# ======================================================================
# Table cache helpers
# ======================================================================


def _flush_table_cache(imagename: str) -> None:
    """Best-effort cleanup of the casacore table cache after a subcube task.

    casacore maintains a process-global ``TableCache`` that persists
    across Dask tasks executed in the same worker process.  Stale
    cache entries from a previous subcube can cause the next subcube's
    ``SetupNewTable`` call to fail with *"is already opened (is in
    the table cache)"*.

    This helper:
    1. Calls ``.done()`` on temporary casatools table objects to release
       any lingering C++ references.
    2. Clears table lock files via ``tb.clearlocks()``.
    3. Forces Python garbage collection so C++ shared-pointer ref
       counts drop to zero, allowing the table cache to evict entries.
    """
    import gc
    import os

    gc.collect()

    try:
        from casatools import table as tbtool

        tb = tbtool()

        # Release lock files for all image products of this subcube.
        _EXTENSIONS = (
            '.psf',
            '.residual',
            '.model',
            '.image',
            '.pb',
            '.sumwt',
            '.weight',
            '.mask',
            '.image.pbcor',
        )
        for ext in _EXTENSIONS:
            img_path = imagename + ext
            if os.path.isdir(img_path):
                try:
                    tb.clearlocks(img_path)
                except Exception:
                    pass

        # Log residual cache entries (debug-level) so developers can
        # see if entries are accumulating across tasks.
        cached = tb.showcache(verbose=False)
        if cached:
            log.debug(
                'Table cache has %d entries after subcube task (%s …)',
                len(cached),
                cached[0] if cached else '',
            )

        tb.done()
    except Exception:
        pass

    gc.collect()


# ======================================================================
# Full sub-cube imaging (for cube parallelism)
# ======================================================================


def run_subcube(config_dict: dict) -> dict:
    """Run a complete imaging + deconvolution pipeline on a frequency sub-cube.

    Invoked as a Dask task in the cube-parallel engine.

    Each worker operates in its own temporary directory so that
    CASA's deterministic temp files (``IMAGING_WEIGHT_*``) do not
    collide across concurrent workers sharing the same filesystem.

    Args:
        config_dict: Serialised ``PcleanConfig`` (from ``.model_dump()``).

    Returns:
        Summary with convergence info, image name, etc.
    """
    import os

    from pclean.config import PcleanConfig
    from pclean.imaging.serial_imager import SerialImager
    from pclean.utils.check_adios2 import force_omp_single_thread

    # Force OMP_NUM_THREADS=1 (env + ctypes) before casatools is imported
    # lazily by SerialImager.  SerialImager._detect_adios2() handles
    # per-MS ADIOS2 detection and logging during setup().
    force_omp_single_thread()

    config = PcleanConfig.model_validate(config_dict)

    log.debug('run_subcube: vis=%r  imagename=%r  cwd=%s', config.selection.vis, config.imagename, os.getcwd())

    # Resolve imagename to absolute *before* we chdir so that output
    # images always land in the user's original working directory.
    abs_imgname = os.path.abspath(config.imagename)
    # Update config with absolute path
    data = config.model_dump(mode='python')
    data['image']['imagename'] = abs_imgname
    config = PcleanConfig.model_validate(data)

    # Create a per-subcube working directory next to the output image
    # so that CASA's deterministic temp files (IMAGING_WEIGHT_*) don't
    # collide between concurrent workers.
    img_dir = os.path.dirname(abs_imgname) or os.getcwd()
    img_base = os.path.basename(abs_imgname)
    workdir = os.path.join(img_dir, f'.{img_base}.tmpdir')
    os.makedirs(workdir, exist_ok=True)

    orig_cwd = os.getcwd()
    try:
        os.chdir(workdir)
        imager = SerialImager(config, init_iter_control=True)
        result = imager.run()
        return result
    finally:
        os.chdir(orig_cwd)
        # Clean up casacore table cache entries left by this task.
        _flush_table_cache(abs_imgname)


# ======================================================================
# Partial-gridding worker (for continuum row-chunk parallelism)
# ======================================================================


def make_partial_psf(bundle: dict) -> str:
    """Create a ``synthesisimager`` on the worker, compute a partial PSF.

    Args:
        bundle: CASA-native parameter bundle (from ``PcleanConfig.to_casa_bundle()``).

    Returns:
        The partial image name.
    """
    import casatools as ct

    si = ct.synthesisimager()
    try:
        _select_and_define(si, bundle)
        _safe_setweighting(si, bundle['weightpars'])
        si.makepsf()
    finally:
        si.done()
    return bundle['allimpars']['0']['imagename']


def run_partial_major_cycle(
    bundle: dict,
    controls: dict | None = None,
) -> str:
    """Execute one major cycle on the worker's data partition.

    Args:
        bundle: CASA-native parameter bundle.
        controls: Iteration control record from ``iterbotsink``.

    Returns:
        The partial image name.
    """
    import casatools as ct

    si = ct.synthesisimager()
    try:
        _select_and_define(si, bundle)
        _safe_setweighting(si, bundle['weightpars'])
        si.executemajorcycle(controls=controls or {})
    finally:
        si.done()
    return bundle['allimpars']['0']['imagename']


def make_partial_pb(bundle: dict) -> str:
    """Compute partial primary beam on the worker.

    Args:
        bundle: CASA-native parameter bundle.

    Returns:
        The partial image name.
    """
    import casatools as ct

    si = ct.synthesisimager()
    try:
        _select_and_define(si, bundle)
        try:
            si.makepb()
        except Exception:
            pass
    finally:
        si.done()
    return bundle['allimpars']['0']['imagename']


# ======================================================================
# Persistent-worker gridder (keeps synthesisimager alive across cycles)
# ======================================================================


class _WorkerGridder:
    """Persistent ``synthesisimager`` wrapper for Dask actors.

    Holds a ``synthesisimager`` on a Dask worker across multiple
    major-cycle calls so that FTMachine setup cost is paid only once.

    This is used by the continuum-parallel engine via Dask actors.

    Args:
        bundle: CASA-native parameter bundle (from ``PcleanConfig.to_casa_bundle()``).
    """

    def __init__(self, bundle: dict):
        import casatools as ct

        self._bundle = bundle
        self._imagename: str = bundle['allimpars']['0']['imagename']
        self.si = ct.synthesisimager()
        _select_and_define(self.si, bundle)
        _safe_setweighting(self.si, bundle['weightpars'])

    def make_psf(self) -> str:
        self.si.makepsf()
        return self._imagename

    def make_pb(self) -> str:
        try:
            self.si.makepb()
        except Exception:
            pass
        return self._imagename

    def execute_major_cycle(self, controls: dict | None = None) -> str:
        self.si.executemajorcycle(controls=controls or {})
        return self._imagename

    def done(self) -> None:
        self.si.done()


# ======================================================================
# Internal helpers
# ======================================================================


def _select_and_define(si, bundle: dict) -> None:
    """Configure a ``synthesisimager`` from a CASA-native parameter *bundle*.

    Args:
        si: A ``casatools.synthesisimager`` instance.
        bundle: Dict with ``'allselpars'``, ``'allimpars'``, ``'allgridpars'``,
            and ``'allnormpars'`` entries.
    """
    for ms_key in sorted(bundle['allselpars']):
        si.selectdata(selpars=dict(bundle['allselpars'][ms_key]))
    for fld in sorted(bundle['allimpars']):
        si.defineimage(
            impars=dict(bundle['allimpars'][fld]),
            gridpars=dict(bundle['allgridpars'][fld]),
        )
    # Tell the imager about normalizer params (needed for mtmfs image creation)
    si.normalizerinfo(dict(bundle['allnormpars']['0']))
