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

import numpy as np

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
        self._cube_gridding_disabled = False

        # Python auto-multithresh state
        self._use_python_automask = (
            config.deconvolution.python_automask
            and config.deconvolution.usemask == 'auto-multithresh'
        )
        self._automask_cfg = None  # AutoMaskConfig, created in setup()
        self._automask_state: dict = {}  # {field_id: AutoMaskState}
        self._beam_area_pix: float = 0.0
        self._sidelobe_level: float = 0.0
        self._beam_sigma_pix: tuple[float, float] = (0.0, 0.0)
        self._beam_pa_rad: float = 0.0

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
        if self._use_python_automask:
            self._init_python_automask()

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
        if self._use_python_automask:
            self._extract_beam_info()

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
        if self._needs_python_normalization:
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

        if self._needs_python_normalization:
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
        """Update auto-multithresh mask (if configured).

        When ``python_automask`` is enabled, runs the pure-numpy
        automasking algorithm and writes the result to ``.mask``.
        Otherwise delegates to the C++ ``setupmask()``.
        """
        if self._use_python_automask:
            self._update_mask_python()
        else:
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

            # Pre-create .mask so the first Python automask can write to it
            if self._use_python_automask:
                self._create_mask_image()

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
                self._cube_gridding_disabled = True
                log.info('%s Disabled cube gridding (nchan=%d)', self._tag, nchan_this)
            except AttributeError:
                log.debug(
                    'synthesisimager.setcubegridding() not available '
                    '— falling back to default cube gridding path',
                )

        # Select data from each MS
        for ms_key in sorted(self._selpars.keys()):
            selrec = dict(self._selpars[ms_key])
            log.debug(
                '%s selectdata selpars[%s]: msname=%r  cwd=%s', self._tag, ms_key, selrec.get('msname'), os.getcwd()
            )
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

        Python-side normalizer calls are needed when:
          - specmode is MFS (or MTMFS),  OR
          - cube gridding was explicitly disabled via
            ``setcubegridding(False)`` for single-channel subcubes,
            because the ``CubeMajorCycleAlgorithm`` is no longer active
            and the C++ layer behaves like the non-cube path.
        """
        deconv = self._decpars.get('0', {}).get('deconvolver', '')
        return self._is_mfs or deconv == 'mtmfs' or self._cube_gridding_disabled

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

    # -- Python automasking helpers ------------------------------------

    def _init_python_automask(self) -> None:
        """Create ``AutoMaskConfig`` and per-field ``AutoMaskState``."""
        from pclean.imaging.automask import AutoMaskConfig, AutoMaskState

        self._automask_cfg = AutoMaskConfig.from_pclean_config(
            self.config.deconvolution,
        )
        for fld in sorted(self._decpars.keys()):
            self._automask_state[fld] = AutoMaskState()
        log.info('%s Python automasking enabled', self._tag)

    def _extract_beam_info(self) -> None:
        """Read beam parameters from the PSF image on disk.

        Populates ``_beam_area_pix``, ``_sidelobe_level``,
        ``_beam_sigma_pix``, and ``_beam_pa_rad`` needed by
        ``automask_plane()``.
        """
        imagename = self._impars['0']['imagename']
        ct = _ct()
        ia = ct.image()
        try:
            psf_name = f'{imagename}.psf'
            ia.open(psf_name)
            beam_info = ia.restoringbeam()
            cs = ia.coordsys()
            incr = cs.increment(type='direction', format='n')['numeric']
            cell_rad = abs(incr[0])  # radians per pixel
            cs.done()

            # restoringbeam() returns either a single beam dict
            #   {'major': {...}, 'minor': {...}, 'positionangle': {...}}
            # or a per-channel dict for multi-channel images
            #   {'beams': {'*0': {'*0': {...}}, ...}, 'nChannels': N, ...}
            # Extract the median-channel beam (matches SIImageStore behaviour).
            if 'beams' in beam_info:
                nchan = beam_info.get('nChannels', 1)
                mid = nchan // 2
                beam = beam_info['beams'][f'*{mid}']['*0']
            else:
                beam = beam_info

            # Beam FWHM in pixels
            major_val = beam['major']['value']
            minor_val = beam['minor']['value']
            major_unit = beam['major'].get('unit', 'rad')
            minor_unit = beam['minor'].get('unit', major_unit)

            def _fwhm_to_rad(value: float, unit: str) -> float:
                """Convert a FWHM value from the given angular unit to radians."""
                if unit == 'rad':
                    return float(value)
                if unit == 'deg':
                    return float(np.deg2rad(value))
                if unit == 'arcmin':
                    return float(np.deg2rad(value / 60.0))
                if unit == 'arcsec':
                    return float(np.deg2rad(value / 3600.0))
                log.error('Unknown restoring beam unit: %s', unit)
                raise ValueError(f'Unknown restoring beam unit: {unit}')

            major_rad = _fwhm_to_rad(major_val, major_unit)
            minor_rad = _fwhm_to_rad(minor_val, minor_unit)

            major_pix = major_rad / cell_rad
            minor_pix = minor_rad / cell_rad
            # Position angle
            pa_deg = beam.get('positionangle', {}).get('value', 0.0)
            pa_unit = beam.get('positionangle', {}).get('unit', 'deg')
            self._beam_pa_rad = float(
                np.deg2rad(pa_deg) if pa_unit == 'deg' else pa_deg
            )

            fwhm_to_sigma = 1.0 / (2.0 * np.sqrt(2.0 * np.log(2.0)))
            sigma_major = major_pix * fwhm_to_sigma
            sigma_minor = minor_pix * fwhm_to_sigma
            self._beam_sigma_pix = (sigma_major, sigma_minor)
            self._beam_area_pix = float(
                np.pi * major_pix * minor_pix / (4.0 * np.log(2.0))
            )

            ia.close()

            # Sidelobe level from the PSF — matches SIImageStore::getPSFSidelobeLevel().
            #
            # Build an analytic rotated-Gaussian PSF model (with PA), then
            #   delobed = PSF − Gaussian_model
            #   sidelobe = max(|min(PSF)|, max(delobed))
            from pclean.imaging.automask import _make_gaussian_psf

            ia.open(psf_name)
            psf_data = ia.getchunk()
            ia.close()
            psf_plane = psf_data[:, :, 0, 0].astype(np.float32)
            del psf_data
            peak = float(psf_plane.max())
            if peak > 0:
                psf_plane /= np.float32(peak)

            # Record |min(PSF)| before subtracting the model in-place.
            psf_min_abs = abs(float(np.min(psf_plane)))

            gaussian_model = _make_gaussian_psf(
                psf_plane.shape, sigma_major, sigma_minor, self._beam_pa_rad,
            )
            psf_plane -= gaussian_model  # delobed in-place
            del gaussian_model
            self._sidelobe_level = float(max(
                psf_min_abs, float(np.max(psf_plane)),
            ))
            del psf_plane

            log.info(
                '%s beam info: area=%.1f pix, sidelobe=%.4f, '
                'sigma=(%.2f, %.2f) pix, PA=%.1f deg',
                self._tag, self._beam_area_pix, self._sidelobe_level,
                self._beam_sigma_pix[0], self._beam_sigma_pix[1],
                np.rad2deg(self._beam_pa_rad),
            )
        finally:
            ia.done()

    def _create_mask_image(self) -> None:
        """Create blank ``.mask`` images for each field (all zeros).

        Must be called after the first major cycle so that ``.residual``
        exists to serve as a template.  Without this the first
        ``_update_mask_python()`` cannot write the computed mask,
        leaving the C++ deconvolver with no mask constraint —
        it then cleans the entire image, preventing convergence.
        """
        ct = _ct()
        for fld in self.sd_tools:
            imagename = self._impars[fld]['imagename']
            mask_path = f'{imagename}.mask'
            if os.path.isdir(mask_path):
                continue  # already exists
            residual_path = f'{imagename}.residual'
            if not os.path.isdir(residual_path):
                continue  # nothing to copy from
            ia = ct.image()
            try:
                ia.open(residual_path)
                ia.subimage(outfile=mask_path, dropdeg=False, overwrite=True)
                ia.close()
                # Fill mask with zeros (no pixels selected for cleaning)
                ia.open(mask_path)
                ia.set(0.0)
                ia.close()
            except Exception:
                log.warning(
                    '%s Could not create mask image %s',
                    self._tag, mask_path,
                )
            finally:
                ia.done()
        log.debug('%s Created blank .mask images', self._tag)

    def _update_mask_python(self) -> None:
        """Compute the auto-multithresh mask in Python and write to .mask.

        For each field, reads the residual, computes the mask via
        ``automask_plane()``, and writes it to the ``.mask`` image.
        Then calls ``setupmask()`` so the C++ deconvolver picks up
        the externally written mask.

        Skips recomputation when ``state.skip`` is True (mask change
        was below ``minpercentchange`` on the previous iteration).
        """
        from pclean.imaging.automask import automask_plane, read_plane, write_plane

        cfg = self._automask_cfg
        pblimit = self.config.normalization.pblimit

        for fld in self.sd_tools:
            imagename = self._impars[fld]['imagename']

            state = self._automask_state.get(fld)
            if state is None:
                from pclean.imaging.automask import AutoMaskState
                state = AutoMaskState()
                self._automask_state[fld] = state

            # Skip mask recomputation if flagged as stable
            if state.skip:
                log.info(
                    '%s Python automask (field %s): skipped '
                    '(mask stable, change < minpercentchange)',
                    self._tag, fld,
                )
                continue

            t0 = time.monotonic()

            # Read residual plane
            residual = read_plane(f'{imagename}.residual')

            # Read PB if available
            pb = None
            pb_path = f'{imagename}.pb'
            if os.path.isdir(pb_path):
                try:
                    pb = read_plane(pb_path)
                except Exception:
                    log.debug('Could not read PB image %s', pb_path)

            # Compute the mask
            mask = automask_plane(
                residual=residual,
                sidelobe_level=self._sidelobe_level,
                beam_area_pix=self._beam_area_pix,
                beam_sigma_pix=self._beam_sigma_pix,
                cfg=cfg,
                state=state,
                pb=pb,
                pblimit=pblimit,
                beam_pa_rad=self._beam_pa_rad,
            )

            # Write mask to .mask image
            mask_path = f'{imagename}.mask'
            if os.path.isdir(mask_path):
                write_plane(mask_path, mask)
            else:
                log.warning(
                    '%s Mask image %s does not exist; '
                    'skipping Python automask write',
                    self._tag, mask_path,
                )

            dt = time.monotonic() - t0
            log.info(
                '%s Python automask (field %s): %.3fs, '
                '%d / %d mask pixels (%.1f%%)',
                self._tag, fld, dt,
                int(mask.sum()),
                residual.size,
                100.0 * mask.sum() / residual.size if residual.size > 0 else 0.0,
            )

        # Let C++ deconvolver pick up the written mask (usemask='user')
        for fld in self.sd_tools:
            self.sd_tools[fld].setupmask()

    # -- summary -------------------------------------------------------

    def _summary(self) -> dict:
        return {
            'converged': self._converged,
            'major_cycles': self._major_count,
            'imagename': self.config.imagename,
        }
