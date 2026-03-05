"""Serial synthesis imager -- the single-process imaging engine.

Wraps the four CASA synthesis C++ tools (``synthesisimager``,
``synthesisdeconvolver``, ``synthesisnormalizer``, ``iterbotsink``)
into a clean Python lifecycle that can be driven standalone **or**
by the Dask-parallel engines.

Public API::

    SerialImager(config)
    .setup()           -- wires up all C++ tools
    .make_psf()        -- compute PSF
    .make_pb()         -- compute primary beam
    .run_major_cycle() -- one major cycle (grid / degrid)
    .run_minor_cycle() -- one round of deconvolution
    .has_converged()   -- check convergence
    .restore()         -- restore images
    .pbcor()           -- PB-correct images
    .teardown()        -- release tools
    .run()             -- full end-to-end pipeline
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pclean.config import PcleanConfig

log = logging.getLogger(__name__)

# We import casatools lazily so that the module can be imported on a
# machine that does not have casatools installed (e.g. for tests).
_casatools = None


def _ct():
    """Lazy-load casatools and return the module."""
    global _casatools
    if _casatools is None:
        import casatools as ct  # type: ignore

        _casatools = ct
    return _casatools


# ======================================================================
class SerialImager:
    """Single-process CLEAN imaging pipeline.

    Args:
        config: Validated hierarchical configuration.
        init_iter_control: Whether to create an ``iterbotsink`` tool.  Set
            to ``False`` when this imager is driven externally (e.g. by a
            Dask coordinator that manages convergence itself).
    """

    def __init__(
        self,
        config: PcleanConfig,
        init_iter_control: bool = True,
    ):
        self.config = config
        self._init_iter = init_iter_control

        # Pre-compute CASA-native dicts from config
        self._selpars = config.to_casa_selpars()
        self._impars = config.to_casa_impars()
        self._gridpars = config.to_casa_gridpars()
        self._weightpars = config.to_casa_weightpars()
        self._decpars = config.to_casa_decpars()
        self._normpars = config.to_casa_normpars()
        self._iterpars = config.to_casa_iterpars()
        self._miscpars = config.to_casa_miscpars()

        # C++ tool handles (created in setup())
        self.si_tool = None  # synthesisimager
        self.sd_tools: dict = {}  # {field_id: synthesisdeconvolver}
        self.sn_tools: dict = {}  # {field_id: synthesisnormalizer}
        self.ib_tool = None  # iterbotsink

        self._major_count = 0
        self._converged = False
        self._adios2_detected = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setup(self) -> None:
        """Create and configure all synthesis tools."""
        self._detect_adios2()
        self._init_imager()
        self._init_normalizers()
        self._set_weighting()
        if self.config.niter > 0:
            self._init_deconvolvers()
        if self._init_iter:
            self._init_iteration_control()

    def teardown(self) -> None:
        """Release all C++ tool resources."""
        if self.si_tool is not None:
            self.si_tool.done()
            self.si_tool = None
        for sd in self.sd_tools.values():
            sd.done()
        self.sd_tools.clear()
        for sn in self.sn_tools.values():
            sn.done()
        self.sn_tools.clear()
        if self.ib_tool is not None:
            self.ib_tool.done()
            self.ib_tool = None

    # ------------------------------------------------------------------
    # Imaging steps (public, individually callable)
    # ------------------------------------------------------------------

    def make_psf(self) -> None:
        """Compute the PSF (and gather/normalize for MFS)."""
        log.info('Computing PSF …')
        self.si_tool.makepsf()
        self._normalize_psf()

    def make_pb(self) -> None:
        """Compute the primary beam."""
        log.info('Computing PB …')
        try:
            self.si_tool.makepb()
        except Exception:
            log.debug('makepb() not available or not applicable; skipping.')
        self._normalize_pb()

    def run_major_cycle(self, is_first: bool = False) -> None:
        """Execute one major cycle.

        Args:
            is_first: If ``True`` this is the initial residual computation
                (model is zero).
        """
        log.info('Major cycle %d …', self._major_count)
        if self._is_mfs:
            self._pre_major_normalize()

        last = False
        if self.ib_tool is not None:
            last = self.ib_tool.cleanComplete(lastcyclecheck=True)

        # Work around casacore table-cache bug: the mask0 subtable
        # created on the residual image during a previous major cycle
        # can stay in the process-global table cache even after
        # SIImageStore::removeMask deletes it.  The subsequent
        # SetupNewTable for the same path then fails with
        # 'is already opened (is in the table cache)'.
        # Removing the stale mask0 directory from disk lets
        # SetupNewTable succeed because the path no longer exists.
        if not is_first and self._major_count > 0:
            self._evict_residual_mask()

        controls = {'lastcycle': last}
        self.si_tool.executemajorcycle(controls=controls)
        self._major_count += 1

        if self.ib_tool is not None:
            self.ib_tool.endmajorcycle()

        if self._is_mfs:
            self._post_major_normalize()

    def run_minor_cycle(self) -> bool:
        """Execute one round of minor-cycle deconvolution on all fields.

        Returns:
            ``True`` if iterations were performed.
        """
        if self.ib_tool is None:
            raise RuntimeError('No iterbotsink — cannot run minor cycle')
        iterbotrec = self.ib_tool.getminorcyclecontrols()
        did_work = False
        for fld in self.sd_tools:
            exrec = self.sd_tools[fld].executeminorcycle(iterbotrecord=iterbotrec)
            self.ib_tool.mergeexecrecord(exrec, int(fld))
            if exrec.get('iterdone', 0) > 0:
                did_work = True
        return did_work

    def has_converged(self, nmajor_limit: int = -1) -> bool:
        """Check convergence (peak residual, niter, threshold ...).

        Returns:
            ``True`` when cleaning should stop.
        """
        if self.ib_tool is None:
            return True
        self.ib_tool.resetminorcycleinfo()
        for fld in self.sd_tools:
            initrec = self.sd_tools[fld].initminorcycle()
            self.ib_tool.mergeinitrecord(initrec, int(fld))
        reached_major = nmajor_limit > 0 and self._major_count >= nmajor_limit
        flag = self.ib_tool.cleanComplete(reachedMajorLimit=reached_major)
        self._converged = flag
        return flag

    def update_mask(self) -> None:
        """Update auto-multithresh mask (if configured)."""
        for fld in self.sd_tools:
            self.sd_tools[fld].setupmask()

    def restore(self) -> None:
        """Restore the final CLEAN images."""
        log.info('Restoring images …')
        for fld in self.sd_tools:
            self.sd_tools[fld].restore()

    def pbcor(self) -> None:
        """Apply primary-beam correction."""
        log.info('PB-correcting images …')
        for fld in self.sd_tools:
            self.sd_tools[fld].pbcor()

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """Run the full imaging + deconvolution pipeline.

        Returns:
            Convergence summary.
        """
        try:
            t_total = time.monotonic()

            t0 = time.monotonic()
            self.setup()
            log.info('setup:          %.1fs', time.monotonic() - t0)

            t0 = time.monotonic()
            self.make_psf()
            log.info('make_psf:       %.1fs', time.monotonic() - t0)

            t0 = time.monotonic()
            self.make_pb()
            log.info('make_pb:        %.1fs', time.monotonic() - t0)

            # Initial residual (dirty image)
            if self._miscpars.get('calcres', True):
                t0 = time.monotonic()
                self.run_major_cycle(is_first=True)
                log.info('major_cycle(0): %.1fs', time.monotonic() - t0)

            if self.config.niter > 0:
                nmajor = self.config.iteration.nmajor
                # CASA order: hasConverged (initminorcycle) → updateMask
                # (setupmask) → hasConverged (re-check after mask)
                converged = self.has_converged(nmajor)
                self.update_mask()
                converged = self.has_converged(nmajor)
                while not converged:
                    did = self.run_minor_cycle()
                    if did:
                        self.run_major_cycle()
                    self.update_mask()
                    converged = self.has_converged(nmajor) or (not did)

                if self.config.deconvolution.restoration:
                    t0 = time.monotonic()
                    self.restore()
                    log.info('restore:        %.1fs', time.monotonic() - t0)
                if self.config.deconvolution.pbcor:
                    t0 = time.monotonic()
                    self.pbcor()
                    log.info('pbcor:          %.1fs', time.monotonic() - t0)

            log.info('run total:      %.1fs', time.monotonic() - t_total)
            return self._summary()
        finally:
            self.teardown()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @property
    def _is_mfs(self) -> bool:
        return self.config.is_mfs

    # -- tool initialization -------------------------------------------

    def _detect_adios2(self) -> None:
        """Check whether any input MS uses Adios2StMan.

        Sets ``_adios2_detected`` so that ``_init_imager()`` can
        conditionally call ``setcubegridding(False)`` for single-channel
        subcubes.  Also forces ``OMP_NUM_THREADS=1`` as a general
        thread-safety precaution for ADIOS2-backed storage managers.
        """
        self._adios2_detected = False
        vis = self.config.selection.vis
        vis_list = [vis] if isinstance(vis, str) else list(vis)
        for ms_path in vis_list:
            if not ms_path:
                continue
            try:
                from pclean.utils.check_adios2 import force_omp_single_thread, ms_uses_adios2

                if ms_uses_adios2(ms_path):
                    self._adios2_detected = True
                    force_omp_single_thread()
                    log.info(
                        'ADIOS2-backed MS detected (%s) — '
                        'forcing OMP_NUM_THREADS=1',
                        ms_path,
                    )
                    return
            except Exception:
                log.debug('Could not check ADIOS2 status for %s', ms_path, exc_info=True)

    def _init_imager(self) -> None:
        ct = _ct()
        self.si_tool = ct.synthesisimager()

        # Disable cube gridding only for single-channel (or MFS) subcubes.
        # With nchan<=1 the CubeMajorCycleAlgorithm path is pure overhead
        # (a redundant secondary imager that re-opens the MS) and triggers
        # the ADIOS2 SetSelection shape mismatch on indirect-array columns.
        # For multi-channel subcubes we must keep cube gridding enabled so
        # the C++ layer can chunk channels within available RAM.
        nchan_this = self.config.image.nchan
        if getattr(self, '_adios2_detected', False) and nchan_this <= 1:
            try:
                self.si_tool.setcubegridding(False)
                log.info('Disabled cube gridding (nchan=%d, ADIOS2 MS)', nchan_this)
            except AttributeError:
                log.debug(
                    'synthesisimager.setcubegridding() not available '
                    '— falling back to OMP workaround only',
                )

        # Select data from each MS
        for ms_key in sorted(self._selpars.keys()):
            selrec = dict(self._selpars[ms_key])
            self.si_tool.selectdata(selpars=selrec)

        # Define images for each field
        for fld in sorted(self._impars.keys()):
            self.si_tool.defineimage(
                impars=dict(self._impars[fld]),
                gridpars=dict(self._gridpars[fld]),
            )

        # Tell the imager about normalizer params so it creates the
        # correct image products on disk (e.g. .tt0/.tt1 for mtmfs).
        self.si_tool.normalizerinfo(dict(self._normpars['0']))

    def _init_deconvolvers(self) -> None:
        ct = _ct()
        for fld in sorted(self._decpars.keys()):
            sd = ct.synthesisdeconvolver()
            decpars = dict(self._decpars[fld])
            decpars['imagename'] = self._impars[fld]['imagename']
            sd.setupdeconvolution(decpars=decpars)
            self.sd_tools[fld] = sd

    def _init_normalizers(self) -> None:
        ct = _ct()
        for fld in sorted(self._normpars.keys()):
            sn = ct.synthesisnormalizer()
            sn.setupnormalizer(normpars=dict(self._normpars[fld]))
            self.sn_tools[fld] = sn

    def _set_weighting(self) -> None:
        # Only pass keys accepted by setweighting()
        _WEIGHT_KEYS = {
            'type',
            'rmode',
            'noise',
            'robust',
            'fieldofview',
            'npixels',
            'multifield',
            'usecubebriggs',
            'uvtaper',
            'fracbw',
        }
        wp = {k: v for k, v in self._weightpars.items() if k in _WEIGHT_KEYS}
        if 'fracbw' in wp:
            log.info('setweighting: fracbw=%.6g (briggsbwtaper taper)', wp['fracbw'])
        self.si_tool.setweighting(**wp)

    def _init_iteration_control(self) -> None:
        ct = _ct()
        self.ib_tool = ct.iterbotsink()
        self.ib_tool.setupiteration(iterpars=dict(self._iterpars))

    # -- normalization helpers -----------------------------------------

    def _evict_residual_mask(self) -> None:
        """Remove stale ``mask0`` subtable from residual images.

        The casacore ``TableCache`` is process-global.  When
        ``SIImageStore::copyMask`` creates ``mask0`` inside the
        residual image during one major cycle, the subtable enters
        the cache.  On the *next* major cycle, ``removeMask`` deletes
        ``mask0`` from the image metadata but the cache entry lingers
        (a casacore bug).  The subsequent ``SetupNewTable`` for the
        same path then fails with *"is already opened (is in the
        table cache)"*.

        By deleting the ``mask0`` directory from disk *before*
        ``executemajorcycle`` is called, we ensure ``SetupNewTable``
        can create a fresh subtable even if the cache still has a
        stale entry (casacore's ``SetupNewTable`` only blocks when
        the path physically exists AND is in the cache).
        """
        imagename = self._impars['0']['imagename']
        nterms = self._decpars.get('0', {}).get('nterms', 1)
        extensions = ['.residual']
        if nterms > 1:
            extensions = [f'.residual.tt{t}' for t in range(nterms)]
        for ext in extensions:
            mask_dir = f'{imagename}{ext}/mask0'
            if os.path.isdir(mask_dir):
                try:
                    shutil.rmtree(mask_dir)
                    log.debug('Removed stale mask subtable: %s', mask_dir)
                except OSError:
                    log.debug('Could not remove mask subtable: %s', mask_dir, exc_info=True)

    def _normalize_psf(self) -> None:
        for fld in self.sn_tools:
            self.sn_tools[fld].gatherpsfweight()
            self.sn_tools[fld].dividepsfbyweight()
            self.sn_tools[fld].makepsfbeamset()
            self.sn_tools[fld].divideweightbysumwt()

    def _normalize_pb(self) -> None:
        for fld in self.sn_tools:
            try:
                self.sn_tools[fld].normalizeprimarybeam()
            except Exception:
                pass

    def _pre_major_normalize(self) -> None:
        """Normalize model before major cycle (MFS only)."""
        for fld in self.sn_tools:
            self.sn_tools[fld].dividemodelbyweight()
            self.sn_tools[fld].scattermodel()

    def _post_major_normalize(self) -> None:
        """Normalize residual after major cycle (MFS only)."""
        for fld in self.sn_tools:
            self.sn_tools[fld].gatherresidual()
            self.sn_tools[fld].divideresidualbyweight()
            self.sn_tools[fld].multiplymodelbyweight()

    # -- summary -------------------------------------------------------

    def _summary(self) -> dict:
        return {
            'converged': self._converged,
            'major_cycles': self._major_count,
            'imagename': self.config.imagename,
        }
