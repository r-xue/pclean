"""
Image normalizer wrapper.

Provides a standalone ``Normalizer`` class that handles the
gather / scatter / divide-by-weight operations required for
both serial and parallel imaging.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

_casatools = None


def _ct():
    global _casatools
    if _casatools is None:
        import casatools as ct
        _casatools = ct
    return _casatools


class Normalizer:
    """
    Wrapper around ``synthesisnormalizer``.

    In parallel continuum mode the normalizer gathers partial images
    produced by different workers, divides by weight, and scatters
    the model back.

    Parameters
    ----------
    normpars : dict
        Parameters for ``setupnormalizer`` (imagename, pblimit, …).
    partimagenames : list[str], optional
        Paths to the partial (per-worker) images.  When set, the
        normalizer will register them for gather/scatter operations.
    """

    def __init__(
        self,
        normpars: dict,
        partimagenames: list[str] | None = None,
    ):
        self.normpars = dict(normpars)
        self.partimagenames = partimagenames or []
        self._sn = None

    def setup(self) -> None:
        ct = _ct()
        self._sn = ct.synthesisnormalizer()
        pars = dict(self.normpars)
        if self.partimagenames:
            pars["partimagenames"] = self.partimagenames
        self._sn.setupnormalizer(normpars=pars)

    def teardown(self) -> None:
        if self._sn is not None:
            self._sn.done()
            self._sn = None

    # -- PSF normalization ---------------------------------------------

    def normalize_psf(self) -> None:
        """Gather PSF weight, divide, fit beam, normalize weight image."""
        self._sn.gatherpsfweight()
        self._sn.dividepsfbyweight()
        self._sn.makepsfbeamset()
        self._sn.divideweightbysumwt()

    # -- Residual normalization ----------------------------------------

    def gather_residual(self) -> None:
        self._sn.gatherresidual()

    def divide_residual_by_weight(self) -> None:
        self._sn.divideresidualbyweight()

    # -- Model normalization -------------------------------------------

    def divide_model_by_weight(self) -> None:
        self._sn.dividemodelbyweight()

    def multiply_model_by_weight(self) -> None:
        self._sn.multiplymodelbyweight()

    def scatter_model(self) -> None:
        self._sn.scattermodel()

    # -- Primary beam --------------------------------------------------

    def normalize_pb(self) -> None:
        try:
            self._sn.normalizeprimarybeam()
        except Exception:
            log.debug("normalizeprimarybeam not available; skipping.")

    # -- Weight density ------------------------------------------------

    def gather_weight_density(self) -> None:
        self._sn.gatherweightdensity()

    def scatter_weight_density(self) -> str:
        return self._sn.scatterweightdensity()

    # -- Convenience combos --------------------------------------------

    def post_major_mfs(self) -> None:
        """Post-major-cycle normalization for MFS imaging."""
        self.gather_residual()
        self.divide_residual_by_weight()
        self.multiply_model_by_weight()

    def pre_major_mfs(self) -> None:
        """Pre-major-cycle normalization for MFS imaging."""
        self.divide_model_by_weight()
        self.scatter_model()
