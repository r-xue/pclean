"""
Serial synthesis imager — the single-process imaging engine.

Wraps the four CASA synthesis C++ tools (``synthesisimager``,
``synthesisdeconvolver``, ``synthesisnormalizer``, ``iterbotsink``)
into a clean Python lifecycle that can be driven standalone **or**
by the Dask-parallel engines.

Public API
----------
* ``SerialImager(params)``
* ``.setup()``          — wires up all C++ tools
* ``.make_psf()``       — compute PSF
* ``.make_pb()``        — compute primary beam
* ``.run_major_cycle()``— one major cycle (grid / degrid)
* ``.run_minor_cycle()``— one round of deconvolution
* ``.has_converged()``  — check convergence
* ``.restore()``        — restore images
* ``.pbcor()``          — PB-correct images
* ``.teardown()``       — release tools
* ``.run()``            — full end-to-end pipeline
"""

from __future__ import annotations

import logging

from pclean.params import PcleanParams

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
    """
    Single-process CLEAN imaging pipeline.

    Parameters
    ----------
    params : PcleanParams
        Validated parameter set.
    init_iter_control : bool
        Whether to create an ``iterbotsink`` tool.  Set to ``False``
        when this imager is driven externally (e.g. by a Dask
        coordinator that manages convergence itself).
    """

    def __init__(
        self,
        params: PcleanParams,
        init_iter_control: bool = True,
    ):
        self.params = params
        self._init_iter = init_iter_control

        # C++ tool handles (created in setup())
        self.si_tool = None          # synthesisimager
        self.sd_tools: dict = {}     # {field_id: synthesisdeconvolver}
        self.sn_tools: dict = {}     # {field_id: synthesisnormalizer}
        self.ib_tool = None          # iterbotsink

        self._major_count = 0
        self._converged = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setup(self) -> None:
        """Create and configure all synthesis tools."""
        self._init_imager()
        self._init_normalizers()
        self._set_weighting()
        if self.params.niter > 0:
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
        log.info("Computing PSF …")
        self.si_tool.makepsf()
        self._normalize_psf()

    def make_pb(self) -> None:
        """Compute the primary beam."""
        log.info("Computing PB …")
        try:
            self.si_tool.makepb()
        except Exception:
            log.debug("makepb() not available or not applicable; skipping.")
        self._normalize_pb()

    def run_major_cycle(self, is_first: bool = False) -> None:
        """
        Execute one major cycle.

        Parameters
        ----------
        is_first : bool
            If ``True`` this is the initial residual computation
            (model is zero).
        """
        log.info("Major cycle %d …", self._major_count)
        if self._is_mfs:
            self._pre_major_normalize()

        last = False
        if self.ib_tool is not None:
            last = self.ib_tool.cleanComplete(lastcyclecheck=True)

        controls = {"lastcycle": last}
        self.si_tool.executemajorcycle(controls=controls)
        self._major_count += 1

        if self.ib_tool is not None:
            self.ib_tool.endmajorcycle()

        if self._is_mfs:
            self._post_major_normalize()

    def run_minor_cycle(self) -> bool:
        """
        Execute one round of minor-cycle deconvolution on all fields.

        Returns ``True`` if iterations were performed.
        """
        if self.ib_tool is None:
            raise RuntimeError("No iterbotsink — cannot run minor cycle")
        iterbotrec = self.ib_tool.getminorcyclecontrols()
        did_work = False
        for fld in self.sd_tools:
            exrec = self.sd_tools[fld].executeminorcycle(
                iterbotrecord=iterbotrec
            )
            self.ib_tool.mergeexecrecord(exrec, int(fld))
            if exrec.get("iterdone", 0) > 0:
                did_work = True
        return did_work

    def has_converged(self, nmajor_limit: int = -1) -> bool:
        """
        Check convergence (peak residual, niter, threshold …).

        Returns ``True`` when cleaning should stop.
        """
        if self.ib_tool is None:
            return True
        self.ib_tool.resetminorcycleinfo()
        for fld in self.sd_tools:
            initrec = self.sd_tools[fld].initminorcycle()
            self.ib_tool.mergeinitrecord(initrec, int(fld))
        reached_major = (
            nmajor_limit > 0 and self._major_count >= nmajor_limit
        )
        flag = self.ib_tool.cleanComplete(reachedMajorLimit=reached_major)
        self._converged = flag
        return flag

    def update_mask(self) -> None:
        """Update auto-multithresh mask (if configured)."""
        for fld in self.sd_tools:
            self.sd_tools[fld].setupmask()

    def restore(self) -> None:
        """Restore the final CLEAN images."""
        log.info("Restoring images …")
        for fld in self.sd_tools:
            self.sd_tools[fld].restore()

    def pbcor(self) -> None:
        """Apply primary-beam correction."""
        log.info("PB-correcting images …")
        for fld in self.sd_tools:
            self.sd_tools[fld].pbcor()

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """
        Run the full imaging + deconvolution pipeline.

        Returns
        -------
        dict
            Convergence summary.
        """
        try:
            self.setup()
            self.make_psf()
            self.make_pb()

            # Initial residual (dirty image)
            if self.params.miscpars.get("calcres", True):
                self.run_major_cycle(is_first=True)

            if self.params.niter > 0:
                nmajor = self.params.iterpars.get("nmajor", -1)
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

                if self.params.alldecpars["0"].get("restoration", True):
                    self.restore()
                if self.params.alldecpars["0"].get("pbcor", False):
                    self.pbcor()

            return self._summary()
        finally:
            self.teardown()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @property
    def _is_mfs(self) -> bool:
        return self.params.specmode == "mfs"

    # -- tool initialization -------------------------------------------

    def _init_imager(self) -> None:
        ct = _ct()
        self.si_tool = ct.synthesisimager()

        # Select data from each MS
        for ms_key in sorted(self.params.allselpars.keys()):
            selrec = dict(self.params.allselpars[ms_key])
            self.si_tool.selectdata(selpars=selrec)

        # Define images for each field
        for fld in sorted(self.params.allimpars.keys()):
            self.si_tool.defineimage(
                impars=dict(self.params.allimpars[fld]),
                gridpars=dict(self.params.allgridpars[fld]),
            )

        # Tell the imager about normalizer params so it creates the
        # correct image products on disk (e.g. .tt0/.tt1 for mtmfs).
        self.si_tool.normalizerinfo(dict(self.params.allnormpars["0"]))

    def _init_deconvolvers(self) -> None:
        ct = _ct()
        for fld in sorted(self.params.alldecpars.keys()):
            sd = ct.synthesisdeconvolver()
            decpars = dict(self.params.alldecpars[fld])
            decpars["imagename"] = self.params.allimpars[fld]["imagename"]
            sd.setupdeconvolution(decpars=decpars)
            self.sd_tools[fld] = sd

    def _init_normalizers(self) -> None:
        ct = _ct()
        for fld in sorted(self.params.allnormpars.keys()):
            sn = ct.synthesisnormalizer()
            sn.setupnormalizer(normpars=dict(self.params.allnormpars[fld]))
            self.sn_tools[fld] = sn

    def _set_weighting(self) -> None:
        # Only pass keys accepted by setweighting()
        _WEIGHT_KEYS = {
            "type", "rmode", "noise", "robust", "fieldofview",
            "npixels", "multifield", "usecubebriggs", "uvtaper",
        }
        wp = {k: v for k, v in self.params.weightpars.items() if k in _WEIGHT_KEYS}
        self.si_tool.setweighting(**wp)

    def _init_iteration_control(self) -> None:
        ct = _ct()
        self.ib_tool = ct.iterbotsink()
        self.ib_tool.setupiteration(iterpars=dict(self.params.iterpars))

    # -- normalization helpers -----------------------------------------

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
            "converged": self._converged,
            "major_cycles": self._major_count,
            "imagename": self.params.imagename,
        }
