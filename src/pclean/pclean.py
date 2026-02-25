"""
``pclean`` — tclean-compatible interface with Dask parallelism.

This is the primary user-facing module.  The ``pclean()`` function
accepts the same parameters as CASA ``tclean`` (plus a handful of
Dask-specific extras) and dispatches to the appropriate engine:

* ``parallel=False``  → ``SerialImager``
* ``parallel=True, specmode='cube'``  → ``ParallelCubeImager``
* ``parallel=True, specmode='mfs'``   → ``ParallelContinuumImager``

Examples
--------
>>> from pclean import pclean
>>> pclean(vis='my.ms', imagename='test', imsize=[512,512],
...        cell='1arcsec', specmode='cube', niter=500,
...        parallel=True, nworkers=8)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Sequence, Union

from pclean.params import PcleanParams

log = logging.getLogger(__name__)


def pclean(
    # --- Data selection ------------------------------------------------
    vis: Union[str, Sequence[str]] = "",
    selectdata: bool = True,
    field: str = "",
    spw: str = "",
    timerange: str = "",
    uvrange: str = "",
    antenna: str = "",
    scan: str = "",
    observation: str = "",
    intent: str = "",
    datacolumn: str = "corrected",
    # --- Image definition ---------------------------------------------
    imagename: str = "",
    imsize: Any = [100],
    cell: Any = "1arcsec",
    phasecenter: str = "",
    stokes: str = "I",
    projection: str = "SIN",
    startmodel: str = "",
    # --- Spectral definition ------------------------------------------
    specmode: str = "mfs",
    reffreq: str = "",
    nchan: int = -1,
    start: str = "",
    width: str = "",
    outframe: str = "LSRK",
    veltype: str = "radio",
    restfreq: Any = [],
    interpolation: str = "linear",
    perchanweightdensity: bool = True,
    # --- Gridding -----------------------------------------------------
    gridder: str = "standard",
    facets: int = 1,
    wprojplanes: int = 1,
    vptable: str = "",
    mosweight: bool = True,
    aterm: bool = True,
    psterm: bool = False,
    wbawp: bool = True,
    conjbeams: bool = False,
    cfcache: str = "",
    usepointing: bool = False,
    computepastep: float = 360.0,
    rotatepastep: float = 360.0,
    pointingoffsetsigdev: Any = [],
    pblimit: float = 0.2,
    normtype: str = "flatnoise",
    psfphasecenter: str = "",
    # --- Deconvolution ------------------------------------------------
    deconvolver: str = "hogbom",
    scales: Any = [],
    nterms: int = 2,
    smallscalebias: float = 0.0,
    fusedthreshold: float = 0.0,
    largestscale: int = -1,
    restoration: bool = True,
    restoringbeam: Any = [],
    pbcor: bool = False,
    outlierfile: str = "",
    # --- Weighting ----------------------------------------------------
    weighting: str = "natural",
    robust: float = 0.5,
    noise: str = "1.0Jy",
    npixels: int = 0,
    uvtaper: Any = [],
    # --- Iteration control --------------------------------------------
    niter: int = 0,
    gain: float = 0.1,
    threshold: str = "0.0mJy",
    nsigma: float = 0.0,
    cycleniter: int = -1,
    cyclefactor: float = 1.0,
    minpsffraction: float = 0.05,
    maxpsffraction: float = 0.8,
    interactive: bool = False,
    nmajor: int = -1,
    fullsummary: bool = False,
    # --- Masking ------------------------------------------------------
    usemask: str = "user",
    mask: str = "",
    pbmask: float = 0.0,
    sidelobethreshold: float = 3.0,
    noisethreshold: float = 5.0,
    lownoisethreshold: float = 1.5,
    negativethreshold: float = 0.0,
    smoothfactor: float = 1.0,
    minbeamfrac: float = 0.3,
    cutthreshold: float = 0.01,
    growiterations: int = 100,
    dogrowprune: bool = True,
    minpercentchange: float = 0.0,
    verbose: bool = False,
    fastnoise: bool = True,
    # --- Misc ---------------------------------------------------------
    restart: bool = True,
    savemodel: str = "none",
    calcres: bool = True,
    calcpsf: bool = True,
    psfcutoff: float = 0.35,
    # --- pclean-specific (Dask) ---------------------------------------
    parallel: bool = False,
    nworkers: Optional[int] = None,
    scheduler_address: Optional[str] = None,
    threads_per_worker: int = 1,
    memory_limit: str = "auto",
    local_directory: Optional[str] = None,
) -> dict:
    """
    Parallel CLEAN imaging — tclean-compatible interface.

    Parameters are identical to CASA ``tclean`` with the following
    additions:

    * **parallel** — enable Dask-distributed parallelism.
    * **nworkers** — number of Dask workers (default: CPU count).
    * **scheduler_address** — connect to an existing Dask scheduler.
    * **threads_per_worker** — threads per Dask worker (default 1).
    * **memory_limit** — per-worker memory cap.
    * **local_directory** — Dask scratch directory.

    Returns
    -------
    dict
        Imaging summary (convergence, major-cycle count, image names).
    """

    # Collect all explicit arguments into a kwargs dict for PcleanParams
    kwargs = dict(
        field=field, spw=spw, timerange=timerange, uvrange=uvrange,
        antenna=antenna, scan=scan, observation=observation, intent=intent,
        datacolumn=datacolumn,
        imagename=imagename, imsize=imsize, cell=cell,
        phasecenter=phasecenter, stokes=stokes, projection=projection,
        startmodel=startmodel,
        specmode=specmode, reffreq=reffreq, nchan=nchan, start=start,
        width=width, outframe=outframe, veltype=veltype, restfreq=restfreq,
        interpolation=interpolation,
        perchanweightdensity=perchanweightdensity,
        gridder=gridder, facets=facets, wprojplanes=wprojplanes,
        vptable=vptable, mosweight=mosweight, aterm=aterm, psterm=psterm,
        wbawp=wbawp, conjbeams=conjbeams, cfcache=cfcache,
        usepointing=usepointing, computepastep=computepastep,
        rotatepastep=rotatepastep,
        pointingoffsetsigdev=pointingoffsetsigdev,
        pblimit=pblimit, normtype=normtype, psfphasecenter=psfphasecenter,
        deconvolver=deconvolver, scales=scales, nterms=nterms,
        smallscalebias=smallscalebias, fusedthreshold=fusedthreshold,
        largestscale=largestscale,
        restoration=restoration, restoringbeam=restoringbeam, pbcor=pbcor,
        weighting=weighting, robust=robust, noise=noise, npixels=npixels,
        uvtaper=uvtaper,
        niter=niter, gain=gain, threshold=threshold, nsigma=nsigma,
        cycleniter=cycleniter, cyclefactor=cyclefactor,
        minpsffraction=minpsffraction, maxpsffraction=maxpsffraction,
        interactive=interactive, nmajor=nmajor, fullsummary=fullsummary,
        usemask=usemask, mask=mask, pbmask=pbmask,
        sidelobethreshold=sidelobethreshold,
        noisethreshold=noisethreshold,
        lownoisethreshold=lownoisethreshold,
        negativethreshold=negativethreshold,
        smoothfactor=smoothfactor, minbeamfrac=minbeamfrac,
        cutthreshold=cutthreshold, growiterations=growiterations,
        dogrowprune=dogrowprune, minpercentchange=minpercentchange,
        verbose=verbose, fastnoise=fastnoise,
        restart=restart, savemodel=savemodel, calcres=calcres,
        calcpsf=calcpsf, psfcutoff=psfcutoff,
        parallel=parallel, nworkers=nworkers,
        scheduler_address=scheduler_address,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit, local_directory=local_directory,
    )

    params = PcleanParams(vis=vis, **kwargs)

    # ---- dispatch to the right engine --------------------------------
    if not params.parallel:
        return _run_serial(params)
    elif params.is_cube:
        return _run_parallel_cube(params)
    else:
        return _run_parallel_continuum(params)


# ======================================================================
# Dispatch helpers
# ======================================================================


def _run_serial(params: PcleanParams) -> dict:
    from pclean.imaging.serial_imager import SerialImager
    log.info("Running serial imaging (specmode=%s)", params.specmode)
    imager = SerialImager(params)
    return imager.run()


def _run_parallel_cube(params: PcleanParams) -> dict:
    from pclean.parallel.cluster import DaskClusterManager
    from pclean.parallel.cube_parallel import ParallelCubeImager

    pp = params.parallelpars
    log.info("Running parallel cube imaging (nworkers=%s)", pp.get("nworkers"))

    with DaskClusterManager(
        nworkers=pp.get("nworkers"),
        scheduler_address=pp.get("scheduler_address"),
        threads_per_worker=pp.get("threads_per_worker", 1),
        memory_limit=pp.get("memory_limit", "auto"),
        local_directory=pp.get("local_directory"),
    ) as cluster:
        engine = ParallelCubeImager(params, cluster)
        return engine.run()


def _run_parallel_continuum(params: PcleanParams) -> dict:
    from pclean.parallel.cluster import DaskClusterManager
    from pclean.parallel.continuum_parallel import ParallelContinuumImager

    pp = params.parallelpars
    log.info("Running parallel continuum imaging (nworkers=%s)",
             pp.get("nworkers"))

    with DaskClusterManager(
        nworkers=pp.get("nworkers"),
        scheduler_address=pp.get("scheduler_address"),
        threads_per_worker=pp.get("threads_per_worker", 1),
        memory_limit=pp.get("memory_limit", "auto"),
        local_directory=pp.get("local_directory"),
    ) as cluster:
        engine = ParallelContinuumImager(params, cluster)
        return engine.run()
