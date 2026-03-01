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

from pclean.params import PcleanParams

log = logging.getLogger(__name__)


def pclean(
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

    Returns:
        Imaging summary (convergence, major-cycle count, image names).
    """

    # Collect all explicit arguments into a kwargs dict for PcleanParams
    kwargs = dict(
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

    log.info('Running serial imaging (specmode=%s)', params.specmode)
    imager = SerialImager(params)
    return imager.run()


def _run_parallel_cube(params: PcleanParams) -> dict:
    from pclean.parallel.cluster import DaskClusterManager
    from pclean.parallel.cube_parallel import ParallelCubeImager

    pp = params.parallelpars
    log.info('Running parallel cube imaging (nworkers=%s)', pp.get('nworkers'))

    with DaskClusterManager(**_cluster_kwargs(pp)) as cluster:
        engine = ParallelCubeImager(params, cluster)
        return engine.run()


def _run_parallel_continuum(params: PcleanParams) -> dict:
    from pclean.parallel.cluster import DaskClusterManager
    from pclean.parallel.continuum_parallel import ParallelContinuumImager

    pp = params.parallelpars
    log.info('Running parallel continuum imaging (nworkers=%s)', pp.get('nworkers'))

    with DaskClusterManager(**_cluster_kwargs(pp)) as cluster:
        engine = ParallelContinuumImager(params, cluster)
        return engine.run()


def _cluster_kwargs(pp: dict) -> dict:
    """Build ``DaskClusterManager`` keyword arguments from parallel parameters.

    This helper adapts a dictionary of parallel/cluster configuration values,
    typically ``PcleanParams.parallelpars``, into a dictionary that can be
    passed directly to :class:`pclean.parallel.cluster.DaskClusterManager`
    using keyword argument expansion.

    The input dictionary is expected to contain items that describe how the
    Dask cluster should be created or connected to. Only a subset of keys is
    inspected; unknown keys in ``pp`` are ignored. For inspected keys, this
    function uses ``dict.get`` to read values and applies sensible defaults
    when no value is provided.

    Recognized keys include:

    * ``nworkers``: Number of Dask workers to start or expect.
    * ``scheduler_address``: Address of an existing Dask scheduler to connect
      to instead of creating a new local cluster.
    * ``threads_per_worker``: Number of threads per worker. Defaults to ``1``.
    * ``memory_limit``: Per-worker memory limit. Defaults to ``'0'`` (no
      explicit limit).
    * ``local_directory``: Local directory for Dask worker scratch space.
    * ``cluster_type``: Cluster backend type, for example ``'local'`` or a
      batch-system-backed type. Defaults to ``'local'``.
    * ``slurm_queue``: SLURM partition/queue name for batch workers.
    * ``slurm_account``: SLURM account to charge for jobs.
    * ``slurm_walltime``: Requested wall-clock time per job. Defaults to
      ``'04:00:00'``.
    * ``slurm_job_mem``: Memory requested per SLURM job. Defaults to
      ``'20GB'``.
    * ``slurm_cores_per_job``: Number of cores requested per SLURM job.
      Defaults to ``1``.
    * ``slurm_job_extra_directives``: Additional raw SLURM directives to
      inject into the job script.
    * ``slurm_python``: Python executable to use inside SLURM jobs.
    * ``slurm_local_directory``: Local scratch directory path on SLURM
      compute nodes.
    * ``slurm_log_directory``: Directory for SLURM job log files. Defaults to
      ``'logs'``.
    * ``slurm_job_script_prologue``: Shell commands to prepend to the SLURM
      job script before starting the worker.

    Args:
        pp: Dictionary of parallel parameters from which cluster configuration
            should be extracted. Typically this is
            ``PcleanParams.parallelpars``.

    Returns:
        A dictionary containing only the cluster-related configuration keys
        and their values (or defaults) suitable for passing to
        ``DaskClusterManager`` via keyword expansion.
    """
    return dict(
        nworkers=pp.get('nworkers'),
        scheduler_address=pp.get('scheduler_address'),
        threads_per_worker=pp.get('threads_per_worker', 1),
        memory_limit=pp.get('memory_limit', '0'),
        local_directory=pp.get('local_directory'),
        cluster_type=pp.get('cluster_type', 'local'),
        slurm_queue=pp.get('slurm_queue'),
        slurm_account=pp.get('slurm_account'),
        slurm_walltime=pp.get('slurm_walltime', '04:00:00'),
        slurm_job_mem=pp.get('slurm_job_mem', '20GB'),
        slurm_cores_per_job=pp.get('slurm_cores_per_job', 1),
        slurm_job_extra_directives=pp.get('slurm_job_extra_directives'),
        slurm_python=pp.get('slurm_python'),
        slurm_local_directory=pp.get('slurm_local_directory'),
        slurm_log_directory=pp.get('slurm_log_directory', 'logs'),
        slurm_job_script_prologue=pp.get('slurm_job_script_prologue'),
    )
