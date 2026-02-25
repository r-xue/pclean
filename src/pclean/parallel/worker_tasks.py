"""
Dask worker task functions.

Each function in this module is a **pure top-level function** that can be
serialised by Dask and executed on a remote worker.  They accept plain
dicts (serialised ``PcleanParams``) in order to avoid pickle issues and
instantiate ``casatools`` objects on the worker side.

Design rationale
----------------
* Workers must import ``casatools`` locally — the C++ tool objects
  cannot be pickled or transferred between processes.
* All file I/O (images, MSes) uses shared-filesystem paths so the
  coordinator can later gather partial products.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

log = logging.getLogger(__name__)


# ======================================================================
# Full sub-cube imaging (for cube parallelism)
# ======================================================================


def run_subcube(params_dict: dict) -> dict:
    """
    Run a complete imaging + deconvolution pipeline on a frequency
    sub-cube.  Invoked as a Dask task in the cube-parallel engine.

    Each worker operates in its own temporary directory so that
    CASA's deterministic temp files (``IMAGING_WEIGHT_*``) do not
    collide across concurrent workers sharing the same filesystem.

    Parameters
    ----------
    params_dict : dict
        Serialised ``PcleanParams`` (from ``.to_dict()``).

    Returns
    -------
    dict
        Summary with convergence info, image name, etc.
    """
    import os

    from pclean.params import PcleanParams
    from pclean.imaging.serial_imager import SerialImager

    params = PcleanParams.from_dict(params_dict)

    # Resolve imagename to absolute *before* we chdir so that output
    # images always land in the user's original working directory.
    abs_imgname = os.path.abspath(params.imagename)
    params.allimpars["0"]["imagename"] = abs_imgname
    params.allnormpars["0"]["imagename"] = abs_imgname
    params.allgridpars["0"]["imagename"] = abs_imgname
    if "allimages" in params.iterpars:
        params.iterpars["allimages"]["0"]["imagename"] = abs_imgname

    # Create a per-subcube working directory next to the output image
    # so that CASA's deterministic temp files (IMAGING_WEIGHT_*) don't
    # collide between concurrent workers.
    img_dir = os.path.dirname(abs_imgname) or os.getcwd()
    img_base = os.path.basename(abs_imgname)
    workdir = os.path.join(img_dir, f".{img_base}.tmpdir")
    os.makedirs(workdir, exist_ok=True)

    orig_cwd = os.getcwd()
    try:
        os.chdir(workdir)
        imager = SerialImager(params, init_iter_control=True)
        return imager.run()
    finally:
        os.chdir(orig_cwd)


# ======================================================================
# Partial-gridding worker (for continuum row-chunk parallelism)
# ======================================================================


def make_partial_psf(params_dict: dict) -> str:
    """
    Create a ``synthesisimager`` on the worker, select the partition's
    data, and compute a partial PSF.

    Returns the partial image name.
    """
    import casatools as ct
    from pclean.params import PcleanParams

    params = PcleanParams.from_dict(params_dict)
    si = ct.synthesisimager()
    try:
        _select_and_define(si, params)
        si.setweighting(**params.weightpars)
        si.makepsf()
    finally:
        si.done()
    return params.imagename


def run_partial_major_cycle(
    params_dict: dict,
    controls: dict | None = None,
) -> str:
    """
    Execute one major cycle on the worker's data partition.

    Returns the partial image name.
    """
    import casatools as ct
    from pclean.params import PcleanParams

    params = PcleanParams.from_dict(params_dict)
    si = ct.synthesisimager()
    try:
        _select_and_define(si, params)
        si.setweighting(**params.weightpars)
        si.executemajorcycle(controls=controls or {})
    finally:
        si.done()
    return params.imagename


def make_partial_pb(params_dict: dict) -> str:
    """Compute partial primary beam on the worker."""
    import casatools as ct
    from pclean.params import PcleanParams

    params = PcleanParams.from_dict(params_dict)
    si = ct.synthesisimager()
    try:
        _select_and_define(si, params)
        try:
            si.makepb()
        except Exception:
            pass
    finally:
        si.done()
    return params.imagename


# ======================================================================
# Persistent-worker gridder (keeps synthesisimager alive across cycles)
# ======================================================================


class _WorkerGridder:
    """
    Holds a ``synthesisimager`` on a Dask worker across multiple
    major-cycle calls so that FTMachine setup cost is paid only once.

    This is used by the continuum-parallel engine via Dask actors.
    """

    def __init__(self, params_dict: dict):
        import casatools as ct
        from pclean.params import PcleanParams

        self.params = PcleanParams.from_dict(params_dict)
        self.si = ct.synthesisimager()
        _select_and_define(self.si, self.params)
        self.si.setweighting(**self.params.weightpars)

    def make_psf(self) -> str:
        self.si.makepsf()
        return self.params.imagename

    def make_pb(self) -> str:
        try:
            self.si.makepb()
        except Exception:
            pass
        return self.params.imagename

    def execute_major_cycle(self, controls: dict | None = None) -> str:
        self.si.executemajorcycle(controls=controls or {})
        return self.params.imagename

    def done(self) -> None:
        self.si.done()


# ======================================================================
# Internal helpers
# ======================================================================


def _select_and_define(si, params) -> None:
    """Configure a ``synthesisimager`` from *params*."""
    for ms_key in sorted(params.allselpars.keys()):
        si.selectdata(selpars=dict(params.allselpars[ms_key]))
    for fld in sorted(params.allimpars.keys()):
        si.defineimage(
            impars=dict(params.allimpars[fld]),
            gridpars=dict(params.allgridpars[fld]),
        )
    # Tell the imager about normalizer params (needed for mtmfs image creation)
    si.normalizerinfo(dict(params.allnormpars["0"]))
