"""
Deconvolution wrapper.

Provides a standalone ``Deconvolver`` class that can be used
independently of the full imaging pipeline — for example when
the residual / PSF already exist on disk and only minor-cycle
deconvolution is needed.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

_casatools = None


def _ct():
    global _casatools
    if _casatools is None:
        import casatools as ct
        _casatools = ct
    return _casatools


class Deconvolver:
    """
    Thin wrapper around ``synthesisdeconvolver``.

    Parameters
    ----------
    imagename : str
        Image name prefix (images must already exist on disk).
    decpars : dict
        Deconvolution parameters (deconvolver, scales, nterms, …).
    iterpars : dict, optional
        Iteration control parameters.  If ``None``, the caller must
        drive the minor cycle externally.
    """

    def __init__(
        self,
        imagename: str,
        decpars: dict,
        iterpars: Optional[dict] = None,
    ):
        self.imagename = imagename
        self.decpars = dict(decpars)
        self.decpars["imagename"] = imagename
        self.iterpars = iterpars

        self._sd = None
        self._ib = None

    # ------------------------------------------------------------------

    def setup(self) -> None:
        ct = _ct()
        self._sd = ct.synthesisdeconvolver()
        self._sd.setupdeconvolution(decpars=self.decpars)
        if self.iterpars is not None:
            self._ib = ct.iterbotsink()
            self._ib.setupiteration(iterpars=self.iterpars)

    def teardown(self) -> None:
        if self._sd is not None:
            self._sd.done()
            self._sd = None
        if self._ib is not None:
            self._ib.done()
            self._ib = None

    # ------------------------------------------------------------------

    def init_minor(self) -> dict:
        """Return peak-residual info for the iterbot."""
        return self._sd.initminorcycle()

    def execute_minor(self, iterbotrecord: Optional[dict] = None) -> dict:
        """
        Run one minor cycle.

        If *iterbotrecord* is not given and an ``iterbotsink`` was
        created, the record is obtained automatically.
        """
        if iterbotrecord is None and self._ib is not None:
            iterbotrecord = self._ib.getminorcyclecontrols()
        return self._sd.executeminorcycle(iterbotrecord=iterbotrecord)

    def setup_mask(self) -> None:
        self._sd.setupmask()

    def restore(self) -> None:
        self._sd.restore()

    def pbcor(self) -> None:
        self._sd.pbcor()

    # ------------------------------------------------------------------

    def run_loop(self) -> dict:
        """
        Run the full minor-cycle loop (requires ``iterpars``).

        Returns
        -------
        dict
            Summary with iteration count, peak residual, etc.
        """
        if self._ib is None:
            raise RuntimeError("No iterpars — cannot run standalone loop")
        total_iter = 0
        while True:
            self._ib.resetminorcycleinfo()
            rec = self.init_minor()
            self._ib.mergeinitrecord(rec)
            if self._ib.cleanComplete():
                break
            exrec = self.execute_minor()
            self._ib.mergeexecrecord(exrec, 0)
            total_iter += exrec.get("iterdone", 0)
        return {"iterdone": total_iter}
