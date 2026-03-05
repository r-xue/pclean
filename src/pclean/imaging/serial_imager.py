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
        log.info('%s Computing PSF …', self._tag)
        self.si_tool.makepsf()
        self._normalize_psf()

    def make_pb(self) -> None:
        """Compute the primary beam."""
        log.info('%s Computing PB …', self._tag)
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
        log.info('%s Major cycle %d …', self._tag, self._major_count)
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

    def _init_minor_cycle(self) -> None:
        """Run ``initminorcycle`` on each deconvolver and merge records.

        This populates the image statistics that ``setupmask()`` and
        ``cleanComplete()`` rely on.  It must be called before
        ``update_mask()`` and before ``has_converged()``.
        """
        if self.ib_tool is None:
            return
        self.ib_tool.resetminorcycleinfo()
        for fld in self.sd_tools:
            initrec = self.sd_tools[fld].initminorcycle()
            # ---- Work around CASA cleanComplete bug ----
            # When nsigma=0 the user does *not* want nsigma-based
            # stopping, but CASA computes NsigmaThreshold = 0*rms +
            # median ≈ median (which can be slightly negative for
            # noise-dominated channels).  In cleanComplete() the
            # tolerance check
            #   fabs(PeakRes - NsigmaThreshold) / NsigmaThreshold < tol
            # divides by this negative value, producing a large
            # negative quotient that is ALWAYS < tol → falsely
            # triggers stop code 8.  The guard
            #   ``if (NsigmaThreshold != 0.0)``
            # was meant to prevent this, but it tests the *computed*
            # threshold (≠ 0), not the user's nsigma parameter.
            #
            # Force nsigmathreshold = 0.0 so that:
            #   (a) the tolerance check becomes fabs(…)/0.0 = inf,
            #       which is NOT < tol → outer else-if not entered;
            #   (b) the inner guard ``NsigmaThreshold != 0.0`` also
            #       evaluates to False → stopCode 8 is never set.
            if self.config.iteration.nsigma == 0.0:
                initrec['nsigmathreshold'] = 0.0
            self.ib_tool.mergeinitrecord(initrec, int(fld))

    _STOP_REASONS = {
        0: 'not converged',
        1: 'iteration limit',
        2: 'threshold',
        3: 'force stop',
        4: 'no change in peak residual across two major cycles',
        5: 'peak residual increased by more than 3x from previous cycle',
        6: 'peak residual increased by more than 3x from minimum',
        7: 'zero mask',
        8: 'n-sigma threshold',
        9: 'reached nmajor',
    }

    def has_converged(self, nmajor_limit: int = -1) -> bool:
        """Check convergence (peak residual, niter, threshold ...).

        Calls ``initminorcycle`` internally (idempotent if already called
        via :meth:`_init_minor_cycle`) then evaluates ``cleanComplete``.

        Returns:
            ``True`` when cleaning should stop.
        """
        if self.ib_tool is None:
            return True
        self._init_minor_cycle()
        reached_major = nmajor_limit > 0 and self._major_count >= nmajor_limit
        flag = self.ib_tool.cleanComplete(reachedMajorLimit=reached_major)
        if flag > 0:
            reason = self._STOP_REASONS.get(flag, f'unknown ({flag})')
            log.info('%s Reached stopping criterion: %s', self._tag, reason)
        self._converged = flag
        return flag

    def update_mask(self) -> None:
        """Update auto-multithresh mask (if configured)."""
        for fld in self.sd_tools:
            self.sd_tools[fld].setupmask()

    def restore(self) -> None:
        """Restore the final CLEAN images."""
        log.info('%s Restoring images …', self._tag)
        for fld in self.sd_tools:
            self.sd_tools[fld].restore()

    def pbcor(self) -> None:
        """Apply primary-beam correction."""
        log.info('%s PB-correcting images …', self._tag)
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
            log.info('%s setup:          %.1fs', self._tag, time.monotonic() - t0)

            t0 = time.monotonic()
            self.make_psf()
            log.info('%s make_psf:       %.1fs', self._tag, time.monotonic() - t0)

            t0 = time.monotonic()
            self.make_pb()
            log.info('%s make_pb:        %.1fs', self._tag, time.monotonic() - t0)

            # Initial residual (dirty image)
            if self._miscpars.get('calcres', True):
                t0 = time.monotonic()
                self.run_major_cycle(is_first=True)
                log.info('%s major_cycle(0): %.1fs', self._tag, time.monotonic() - t0)

            if self.config.niter > 0:
                nmajor = self.config.iteration.nmajor
                # The correct sequence for auto-multithresh masking is:
                #   1. initminorcycle()  — compute image statistics
                #   2. setupmask()       — create/update mask using those stats
                #   3. initminorcycle()  — recompute stats *with* the new mask
                #   4. cleanComplete()   — evaluate convergence
                #
                # Step 2 before step 1 triggers "Initminor Cycle has not been
                # called yet".  Omitting step 2 entirely causes the v1 bug
                # (empty mask → peak=0 → premature cleanComplete).
                # CASA's tclean runs the same four-step sequence internally.
                self._init_minor_cycle()   # step 1
                self.update_mask()         # step 2
                converged = self.has_converged(nmajor)  # steps 3 + 4
                while not converged:
                    did = self.run_minor_cycle()
                    if did:
                        self.run_major_cycle()
                    self._init_minor_cycle()
                    self.update_mask()
                    converged = self.has_converged(nmajor) or (not did)

                if self.config.deconvolution.restoration:
                    t0 = time.monotonic()
                    self.restore()
                    log.info('%s restore:        %.1fs', self._tag, time.monotonic() - t0)
                if self.config.deconvolution.pbcor:
                    t0 = time.monotonic()
                    self.pbcor()
                    log.info('%s pbcor:          %.1fs', self._tag, time.monotonic() - t0)

            log.info('%s run total:      %.1fs', self._tag, time.monotonic() - t_total)
            return self._summary()
        finally:
            self.teardown()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @property
    def _tag(self) -> str:
        """Short identifier for log messages (e.g. ``[subcube.ch042]``)."""
        name = os.path.basename(self.config.imagename)
        # For subcubes the imagename looks like <base>.subcube.<suffix>.
        # Extract "subcube.<suffix>" so workers are easy to tell apart.
        idx = name.find('.subcube.')
        if idx >= 0:
            return f'[{name[idx + 1:]}]'
        return f'[{name}]'

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

        # Disable cube gridding for single-channel (or MFS) subcubes.
        # With nchan<=1 the CubeMajorCycleAlgorithm path is pure overhead:
        #
        #   1. It creates a redundant secondary imager that re-opens the MS
        #      (unnecessary when there is only one channel to grid).
        #   2. Its internal copyMask/SetupNewTable for the residual mask0
        #      subtable hits a casacore table-cache bug on the 2nd+ major
        #      cycle — the mask0 created during gridding is still in the
        #      process-global table cache when copyMask tries to SetupNew-
        #      Table for the same path, producing a noisy WARN per subcube
        #      per major cycle.
        #   3. For ADIOS2-backed MS files, the secondary imager triggers
        #      a SetSelection shape mismatch on indirect-array columns.
        #
        # For multi-channel subcubes we keep cube gridding enabled so the
        # C++ layer can chunk channels within available RAM.
        nchan_this = self.config.image.nchan
        if nchan_this <= 1:
            try:
                self.si_tool.setcubegridding(False)
                log.info('%s Disabled cube gridding (nchan=%d)', self._tag, nchan_this)
            except AttributeError:
                log.debug(
                    'synthesisimager.setcubegridding() not available '
                    '— falling back to default cube gridding path',
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

        from pclean.parallel.worker_tasks import _safe_setweighting

        _safe_setweighting(self.si_tool, wp)

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

    @property
    def _needs_python_normalization(self) -> bool:
        """Whether Python-level normalizer calls are needed.

        For cube specmodes the C++ ``CubeMajorCycleAlgorithm`` handles
        PSF normalization, weight division, beam fitting, and residual
        normalization internally during ``makepsf()`` and
        ``executemajorcycle()``.  Calling the Python normalizer on top
        of that would **double-normalise** the weight image, inflating
        the residual by a factor of *sumwt* (~10^5–10^8 for typical
        ALMA / VLA data).

        Only MFS (and MTMFS, which implies MFS) require the explicit
        Python-side normalizer calls — matching the ``divideInPython``
        guard in CASA's ``tclean`` (``imager_base.py``).
        """
        deconv = self._decpars.get('0', {}).get('deconvolver', '')
        return self._is_mfs or deconv == 'mtmfs'

    def _normalize_psf(self) -> None:
        """Gather PSF weight, divide, fit beam, normalize weight image.

        Skipped for cube specmodes — the C++ layer already handles this
        inside ``makepsf()`` via ``CubeMajorCycleAlgorithm``.
        """
        if not self._needs_python_normalization:
            return
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
