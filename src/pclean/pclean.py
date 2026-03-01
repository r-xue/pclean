"""``pclean`` -- tclean-compatible interface with Dask parallelism.

This is the primary user-facing module.  The ``pclean()`` function
accepts the same parameters as CASA ``tclean`` (plus a handful of
Dask-specific extras) and dispatches to the appropriate engine:

* ``parallel=False``  -> ``SerialImager``
* ``parallel=True, specmode='cube'``  -> ``ParallelCubeImager``
* ``parallel=True, specmode='mfs'``   -> ``ParallelContinuumImager``

Examples::

    >>> from pclean import pclean
    >>> pclean(vis='my.ms', imagename='test', imsize=[512, 512],
    ...        cell='1arcsec', specmode='cube', niter=500,
    ...        parallel=True, nworkers=8)
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from pclean.config import PcleanConfig

log = logging.getLogger(__name__)


def pclean(
    # --- Config overlay ------------------------------------------------
    config: PcleanConfig | str | Path | None = None,
    # --- Data selection ------------------------------------------------
    vis: str | Sequence[str] = '',
    selectdata: bool = True,
    field: str = '',
    spw: str = '',
    timerange: str = '',
    uvrange: str = '',
    antenna: str = '',
    scan: str = '',
    observation: str = '',
    intent: str = '',
    datacolumn: str = 'corrected',
    # --- Image definition ---------------------------------------------
    imagename: str = '',
    imsize: list[int] | int = [100],
    cell: list[str] | str = '1arcsec',
    phasecenter: str = '',
    stokes: str = 'I',
    projection: str = 'SIN',
    startmodel: str = '',
    # --- Spectral definition ------------------------------------------
    specmode: str = 'mfs',
    reffreq: str = '',
    nchan: int = -1,
    start: str = '',
    width: str = '',
    outframe: str = 'LSRK',
    veltype: str = 'radio',
    restfreq: list[str] | str = [],
    interpolation: str = 'linear',
    perchanweightdensity: bool = True,
    # --- Gridding -----------------------------------------------------
    gridder: str = 'standard',
    facets: int = 1,
    wprojplanes: int = 1,
    vptable: str = '',
    mosweight: bool = True,
    aterm: bool = True,
    psterm: bool = False,
    wbawp: bool = True,
    conjbeams: bool = False,
    cfcache: str = '',
    usepointing: bool = False,
    computepastep: float = 360.0,
    rotatepastep: float = 360.0,
    pointingoffsetsigdev: list[float] = [],
    pblimit: float = 0.2,
    normtype: str = 'flatnoise',
    psfphasecenter: str = '',
    # --- Deconvolution ------------------------------------------------
    deconvolver: str = 'hogbom',
    scales: list[int] = [],
    nterms: int = 2,
    smallscalebias: float = 0.0,
    fusedthreshold: float = 0.0,
    largestscale: int = -1,
    restoration: bool = True,
    restoringbeam: list[str] = [],
    pbcor: bool = False,
    outlierfile: str = '',
    # --- Weighting ----------------------------------------------------
    weighting: str = 'natural',
    robust: float = 0.5,
    noise: str = '1.0Jy',
    npixels: int = 0,
    uvtaper: list[str] = [],
    # --- Iteration control --------------------------------------------
    niter: int = 0,
    gain: float = 0.1,
    threshold: str = '0.0mJy',
    nsigma: float = 0.0,
    cycleniter: int = -1,
    cyclefactor: float = 1.0,
    minpsffraction: float = 0.05,
    maxpsffraction: float = 0.8,
    interactive: bool = False,
    nmajor: int = -1,
    fullsummary: bool = False,
    # --- Masking ------------------------------------------------------
    usemask: str = 'user',
    mask: str = '',
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
    savemodel: str = 'none',
    calcres: bool = True,
    calcpsf: bool = True,
    psfcutoff: float = 0.35,
    # --- pclean-specific (Dask) ---------------------------------------
    parallel: bool = False,
    nworkers: int | None = None,
    scheduler_address: str | None = None,
    threads_per_worker: int = 1,
    memory_limit: str = '0',
    local_directory: str | None = None,
    cube_chunksize: int = -1,
    keep_subcubes: bool = False,
    keep_partimages: bool = False,
    # Cluster backend
    cluster_type: str = 'local',
    # SLURM options (cluster_type='slurm')
    slurm_queue: str | None = None,
    slurm_account: str | None = None,
    slurm_walltime: str = '04:00:00',
    slurm_job_mem: str = '20GB',
    slurm_cores_per_job: int = 1,
    slurm_job_extra_directives: list[str] | None = None,
    slurm_python: str | None = None,
    slurm_local_directory: str | None = None,
    slurm_log_directory: str = 'logs',
    slurm_job_script_prologue: list[str] | None = None,
) -> dict:
    """Parallel CLEAN imaging -- tclean-compatible interface.

    Parameters are identical to CASA ``tclean`` with the following
    additions:

    * **parallel** -- enable Dask-distributed parallelism.
    * **nworkers** -- number of Dask workers (default: CPU count).
    * **scheduler_address** -- connect to an existing Dask scheduler.
    * **threads_per_worker** -- threads per Dask worker (default 1).
    * **memory_limit** -- per-worker memory cap (``'0'`` disables;
      see ``docs/memory_management.md``).
    * **local_directory** -- Dask scratch directory.
    * **cube_chunksize** -- channels per sub-cube task for cube parallelism.
      ``-1`` (default) sets ``nparts = nworkers``;
      ``1`` creates one task per channel (maximum load balancing);
      ``N`` groups N channels per task.
    * **keep_subcubes** -- if ``True``, preserve intermediate sub-cube
      images and per-worker temp directories after concatenation.
      Useful for debugging or downstream per-channel analysis.
      Default ``False`` removes them to save disk space.
    * **keep_partimages** -- if ``True``, preserve partial images
      produced by each worker during continuum (MFS) imaging.
      Default ``False`` removes them after gathering.

    * **config** -- a :class:`~pclean.config.PcleanConfig` instance, a
      path to a YAML config file, or ``None``.  When provided, the
      hierarchical config is used as the base and any explicit keyword
      arguments override it.

    Returns:
        Imaging summary (convergence, major-cycle count, image names).
    """
    # ---- Build PcleanConfig from flat kwargs -------------------------
    kwargs: dict[str, Any] = dict(
        field=field,
        spw=spw,
        timerange=timerange,
        uvrange=uvrange,
        antenna=antenna,
        scan=scan,
        observation=observation,
        intent=intent,
        datacolumn=datacolumn,
        imagename=imagename,
        imsize=imsize,
        cell=cell,
        phasecenter=phasecenter,
        stokes=stokes,
        projection=projection,
        startmodel=startmodel,
        specmode=specmode,
        reffreq=reffreq,
        nchan=nchan,
        start=start,
        width=width,
        outframe=outframe,
        veltype=veltype,
        restfreq=restfreq,
        interpolation=interpolation,
        perchanweightdensity=perchanweightdensity,
        gridder=gridder,
        facets=facets,
        wprojplanes=wprojplanes,
        vptable=vptable,
        mosweight=mosweight,
        aterm=aterm,
        psterm=psterm,
        wbawp=wbawp,
        conjbeams=conjbeams,
        cfcache=cfcache,
        usepointing=usepointing,
        computepastep=computepastep,
        rotatepastep=rotatepastep,
        pointingoffsetsigdev=pointingoffsetsigdev,
        pblimit=pblimit,
        normtype=normtype,
        psfphasecenter=psfphasecenter,
        deconvolver=deconvolver,
        scales=scales,
        nterms=nterms,
        smallscalebias=smallscalebias,
        fusedthreshold=fusedthreshold,
        largestscale=largestscale,
        restoration=restoration,
        restoringbeam=restoringbeam,
        pbcor=pbcor,
        weighting=weighting,
        robust=robust,
        noise=noise,
        npixels=npixels,
        uvtaper=uvtaper,
        niter=niter,
        gain=gain,
        threshold=threshold,
        nsigma=nsigma,
        cycleniter=cycleniter,
        cyclefactor=cyclefactor,
        minpsffraction=minpsffraction,
        maxpsffraction=maxpsffraction,
        interactive=interactive,
        nmajor=nmajor,
        fullsummary=fullsummary,
        usemask=usemask,
        mask=mask,
        pbmask=pbmask,
        sidelobethreshold=sidelobethreshold,
        noisethreshold=noisethreshold,
        lownoisethreshold=lownoisethreshold,
        negativethreshold=negativethreshold,
        smoothfactor=smoothfactor,
        minbeamfrac=minbeamfrac,
        cutthreshold=cutthreshold,
        growiterations=growiterations,
        dogrowprune=dogrowprune,
        minpercentchange=minpercentchange,
        verbose=verbose,
        fastnoise=fastnoise,
        restart=restart,
        savemodel=savemodel,
        calcres=calcres,
        calcpsf=calcpsf,
        psfcutoff=psfcutoff,
        parallel=parallel,
        nworkers=nworkers,
        scheduler_address=scheduler_address,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        local_directory=local_directory,
        cube_chunksize=cube_chunksize,
        keep_subcubes=keep_subcubes,
        keep_partimages=keep_partimages,
        cluster_type=cluster_type,
        slurm_queue=slurm_queue,
        slurm_account=slurm_account,
        slurm_walltime=slurm_walltime,
        slurm_job_mem=slurm_job_mem,
        slurm_cores_per_job=slurm_cores_per_job,
        slurm_job_extra_directives=slurm_job_extra_directives,
        slurm_python=slurm_python,
        slurm_local_directory=slurm_local_directory,
        slurm_log_directory=slurm_log_directory,
        slurm_job_script_prologue=slurm_job_script_prologue,
    )

    flat_cfg = PcleanConfig.from_flat_kwargs(vis=vis, **kwargs)

    # ---- If a config file/object was provided, use it as the base ----
    if config is not None:
        if isinstance(config, (str, Path)):
            base_cfg = PcleanConfig.from_yaml(config)
        else:
            base_cfg = config
        # Merge: base from file, explicit kwargs override (non-defaults)
        cfg = PcleanConfig.merge(base_cfg, flat_cfg)
    else:
        cfg = flat_cfg

    # ---- Dispatch to the right engine --------------------------------
    return _dispatch(cfg)


# ======================================================================
# Dispatch + engine wiring
# ======================================================================


def _dispatch(config: PcleanConfig) -> dict:
    """Route to serial, cube-parallel, or continuum-parallel engine."""
    if not config.parallel:
        return _run_serial(config)
    elif config.is_cube:
        return _run_parallel_cube(config)
    else:
        return _run_parallel_continuum(config)


def _run_serial(config: PcleanConfig) -> dict:
    from pclean.imaging.serial_imager import SerialImager

    log.info('Running serial imaging (specmode=%s)', config.specmode)
    imager = SerialImager(config)
    return imager.run()


def _run_parallel_cube(config: PcleanConfig) -> dict:
    from pclean.parallel.cluster import DaskClusterManager
    from pclean.parallel.cube_parallel import ParallelCubeImager

    log.info('Running parallel cube imaging (nworkers=%s)', config.cluster.nworkers)

    with DaskClusterManager(**_cluster_kwargs(config)) as cluster:
        engine = ParallelCubeImager(config, cluster)
        return engine.run()


def _run_parallel_continuum(config: PcleanConfig) -> dict:
    from pclean.parallel.cluster import DaskClusterManager
    from pclean.parallel.continuum_parallel import ParallelContinuumImager

    log.info('Running parallel continuum imaging (nworkers=%s)', config.cluster.nworkers)

    with DaskClusterManager(**_cluster_kwargs(config)) as cluster:
        engine = ParallelContinuumImager(config, cluster)
        return engine.run()


def _cluster_kwargs(config: PcleanConfig) -> dict:
    """Build ``DaskClusterManager`` keyword arguments from ``PcleanConfig``.

    Extracts cluster and SLURM configuration directly from the config
    object instead of reading from a flat ``parallelpars`` dict.

    Args:
        config: The active imaging configuration.

    Returns:
        Kwargs dict suitable for ``DaskClusterManager(**...)``..
    """
    c = config.cluster
    s = c.slurm
    return dict(
        nworkers=c.nworkers,
        scheduler_address=c.scheduler_address,
        threads_per_worker=c.threads_per_worker,
        memory_limit=c.memory_limit,
        local_directory=c.local_directory,
        cluster_type=c.type,
        slurm_queue=s.queue,
        slurm_account=s.account,
        slurm_walltime=s.walltime,
        slurm_job_mem=s.job_mem,
        slurm_cores_per_job=s.cores_per_job,
        slurm_job_extra_directives=s.job_extra_directives or None,
        slurm_python=s.python,
        slurm_local_directory=s.local_directory,
        slurm_log_directory=s.log_directory,
        slurm_job_script_prologue=s.job_script_prologue or None,
    )
