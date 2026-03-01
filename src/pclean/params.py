"""Parameter container and validation for pclean.

Mirrors CASA's ``ImagerParameters`` but adds Dask-specific options and
exposes each parameter group as a plain dict for easy serialisation to
Dask workers.
"""

from __future__ import annotations

import copy
from collections.abc import Sequence

# ---------------------------------------------------------------------------
# Defaults (matching tclean where possible)
# ---------------------------------------------------------------------------

_DEFAULT_SEL = dict(
    msname='',
    field='',
    spw='',
    timestr='',
    uvdist='',
    antenna='',
    scan='',
    obs='',
    state='',
    taql='',
    datacolumn='corrected',
)

# Mapping from tclean user-facing parameter names to CASA internal names
_SEL_KEY_ALIASES: dict[str, str] = {
    'timerange': 'timestr',
    'uvrange': 'uvdist',
    'observation': 'obs',
    'intent': 'state',
}

_DEFAULT_IMG = dict(
    imagename='',
    imsize=[100],
    cell=['1arcsec'],
    phasecenter='',
    stokes='I',
    projection='SIN',
    specmode='mfs',
    reffreq='',
    nchan=-1,
    start='',
    width='',
    outframe='LSRK',
    veltype='radio',
    restfreq=[],
    interpolation='linear',
    perchanweightdensity=True,
    startmodel='',
    # These must be in impars so defineimage() creates the correct
    # image products (e.g. .tt0/.tt1 for mtmfs).  Matches CASA's
    # ImagerParameters.allimpars.
    nterms=2,
    deconvolver='hogbom',
    restart=True,
)

_DEFAULT_GRID = dict(
    gridder='standard',
    facets=1,
    wprojplanes=1,
    vptable='',
    mosweight=True,
    aterm=True,
    psterm=False,
    wbawp=True,
    conjbeams=False,
    cfcache='',
    usepointing=False,
    computepastep=360.0,
    rotatepastep=360.0,
    pointingoffsetsigdev=[],
    pblimit=0.2,
    normtype='flatnoise',
    psfphasecenter='',
)

_DEFAULT_WEIGHT = dict(
    type='natural',
    rmode='none',
    robust=0.5,
    noise='1.0Jy',
    npixels=0,
    fieldofview='',
    uvtaper=[],
    multifield=False,
    usecubebriggs=True,
)

_DEFAULT_DEC = dict(
    deconvolver='hogbom',
    scales=[],
    nterms=2,
    smallscalebias=0.0,
    fusedthreshold=0.0,
    largestscale=-1,
    restoration=True,
    restoringbeam=[],
    pbcor=False,
    usemask='user',
    mask='',
    pbmask=0.0,
    sidelobethreshold=3.0,
    noisethreshold=5.0,
    lownoisethreshold=1.5,
    negativethreshold=0.0,
    smoothfactor=1.0,
    minbeamfrac=0.3,
    cutthreshold=0.01,
    growiterations=100,
    dogrowprune=True,
    minpercentchange=0.0,
    verbose=False,
    fastnoise=True,
    fullsummary=False,
)

_DEFAULT_NORM = dict(
    pblimit=0.2,
    normtype='flatnoise',
    psfcutoff=0.35,
)

_DEFAULT_ITER = dict(
    niter=0,
    loopgain=0.1,
    threshold='0.0mJy',
    nsigma=0.0,
    cycleniter=-1,
    cyclefactor=1.0,
    minpsffraction=0.05,
    maxpsffraction=0.8,
    interactive=False,
    nmajor=-1,
    fullsummary=False,
    savemodel='none',
)

_DEFAULT_MISC = dict(
    restart=True,
    calcres=True,
    calcpsf=True,
)

# ---------------------------------------------------------------------------
# Dask-specific defaults
# ---------------------------------------------------------------------------

_DEFAULT_PARALLEL = dict(
    parallel=False,
    nworkers=None,  # auto-detect from available CPU cores
    scheduler_address=None,  # if set, connect to an existing Dask cluster
    threads_per_worker=1,
    memory_limit='0',  # '0' disables Dask's per-worker memory limit
    local_directory=None,  # scratch directory for spilling; defaults to system tmpdir
    cube_chunksize=-1,  # -1: nparts=nworkers, 1: one channel per task, N: N channels per task
    keep_subcubes=False,  # retain per-worker subcube images after concatenation
    keep_partimages=False,  # retain partial images after continuum gather
    # Cluster backend selection
    cluster_type='local',  # 'local' | 'slurm' | 'address'
    # SLURM-specific (only used when cluster_type='slurm')
    slurm_queue=None,  # SLURM partition name (--partition)
    slurm_account=None,  # SLURM account (--account)
    slurm_walltime='04:00:00',  # per-job wall time (--time)
    slurm_job_mem='20GB',  # per-job memory (--mem)
    slurm_cores_per_job=1,  # CPUs per SLURM job (--cpus-per-task)
    slurm_job_extra_directives=None,  # list[str] of extra #SBATCH lines
    slurm_python=None,  # Python executable path on compute nodes
    slurm_local_directory=None,  # worker scratch dir on compute nodes
    slurm_log_directory='logs',  # SLURM stdout/stderr log directory
    slurm_job_script_prologue=None,  # list[str] of shell commands before worker start
)

_ALLOW_BRIGGS_BW_TAPER = True


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _merge(defaults: dict, overrides: dict) -> dict:
    """Return *defaults* updated with non-None entries from *overrides*."""
    out = dict(defaults)
    for k, v in overrides.items():
        if k in out:
            out[k] = v
    return out


def _ensure_list(val: object) -> list:
    if isinstance(val, str):
        return [val]
    try:
        return list(val)
    except TypeError:
        return [val]


# ---------------------------------------------------------------------------
# PcleanParams
# ---------------------------------------------------------------------------


class PcleanParams:
    """Validated, serialisable parameter container for *pclean*.

    Organises all tclean-compatible parameters into sub-dicts that map
    directly to the ``casatools`` synthesis-tool APIs, plus Dask-specific
    settings.

    Args:
        vis: Measurement set path(s).
        **kwargs: Any parameter accepted by CASA ``tclean`` plus the extra
            ``nworkers``, ``scheduler_address``, ``threads_per_worker``,
            ``memory_limit``, and ``local_directory`` keys.
    """

    def __init__(self, vis: str | Sequence[str] = '', **kwargs):
        vis = _ensure_list(vis) if vis else ['']

        # ---- selection (one dict per MS, keyed as 'ms0', 'ms1', ...) -----
        self.allselpars: dict[str, dict] = {}
        for idx, msname in enumerate(vis):
            key = f'ms{idx}'
            sel = dict(_DEFAULT_SEL)
            sel['msname'] = msname
            for k in _DEFAULT_SEL:
                if k in kwargs:
                    sel[k] = kwargs[k]
            # Also translate tclean user-facing aliases (e.g. timerange -> timestr)
            for alias, internal in _SEL_KEY_ALIASES.items():
                if alias in kwargs:
                    sel[internal] = kwargs[alias]
            self.allselpars[key] = sel

        # ---- image definition (field 0) ---------------------------------
        self.allimpars: dict[str, dict] = {}
        imp = _merge(_DEFAULT_IMG, kwargs)
        imp['imsize'] = _ensure_list(imp['imsize'])
        if len(imp['imsize']) == 1:
            imp['imsize'] = imp['imsize'] * 2
        imp['cell'] = _ensure_list(imp['cell'])
        if len(imp['cell']) == 1:
            imp['cell'] = imp['cell'] * 2
        imp['restfreq'] = _ensure_list(imp.get('restfreq', []))
        self.allimpars['0'] = imp

        # ---- gridding ---------------------------------------------------
        self.allgridpars: dict[str, dict] = {}
        gp = _merge(_DEFAULT_GRID, kwargs)
        # C++ SynthesisParamsGrid expects these keys in gridpars too
        gp['imagename'] = imp['imagename']
        gp['deconvolver'] = kwargs.get('deconvolver', 'hogbom')
        gp['interpolation'] = imp.get('interpolation', 'linear')
        self.allgridpars['0'] = gp

        # ---- weighting ---------------------------------------------------
        wp = _merge(_DEFAULT_WEIGHT, kwargs)
        # tclean uses 'weighting' but casatools uses 'type'
        weighting = kwargs.get('weighting', wp.get('type', 'natural'))
        # Translate composite weighting names to C++ type + rmode
        if weighting == 'briggsbwtaper':
            wp['type'] = 'briggs'
            wp['rmode'] = 'bwtaper'
            if _ALLOW_BRIGGS_BW_TAPER:
                # Pre-compute fractional bandwidth from full cube so each
                # parallel sub-cube worker can apply the correct taper even
                # when it images only a single channel (fracBW=0 fallback).
                from pclean.utils.partition import _parse_freq_hz

                start_hz = _parse_freq_hz(kwargs.get('start', ''))
                width_hz = _parse_freq_hz(kwargs.get('width', ''))
                nchan_full = kwargs.get('nchan', -1)
                if start_hz is not None and width_hz is not None and nchan_full > 1:
                    min_freq = start_hz
                    max_freq = start_hz + (nchan_full - 1) * abs(width_hz)
                    if min_freq > max_freq:
                        min_freq, max_freq = max_freq, min_freq
                    wp['fracbw'] = 2.0 * (max_freq - min_freq) / (max_freq + min_freq)
            else:
                wp.pop('fracbw', None)  # ensure fracbw is not set if bwtaper is disabled
        elif weighting == 'briggsabs':
            wp['type'] = 'briggs'
            wp['rmode'] = 'abs'
        elif weighting == 'briggs':
            wp['type'] = 'briggs'
            wp['rmode'] = 'norm'
        else:
            wp['type'] = weighting
            wp.setdefault('rmode', 'none')
        # mosweight → multifield flag expected by setweighting()
        wp['multifield'] = kwargs.get('mosweight', False)
        wp['usecubebriggs'] = kwargs.get('perchanweightdensity', True)
        self.weightpars: dict = wp

        # ---- deconvolution -----------------------------------------------
        self.alldecpars: dict[str, dict] = {}
        dp = _merge(_DEFAULT_DEC, kwargs)
        dp['scales'] = _ensure_list(dp.get('scales', []))
        dp['restoringbeam'] = _ensure_list(dp.get('restoringbeam', []))
        # fullsummary must be consistent between iterbotsink and deconvolver
        dp['fullsummary'] = kwargs.get('fullsummary', False)
        self.alldecpars['0'] = dp

        # ---- normalizer --------------------------------------------------
        self.allnormpars: dict[str, dict] = {}
        np_ = _merge(_DEFAULT_NORM, kwargs)
        np_['imagename'] = imp['imagename']
        np_['nterms'] = dp['nterms'] if dp['deconvolver'] == 'mtmfs' else 1
        np_['deconvolver'] = dp['deconvolver']
        np_['specmode'] = imp['specmode']
        self.allnormpars['0'] = np_

        # ---- iteration control -------------------------------------------
        ip = _merge(_DEFAULT_ITER, kwargs)
        # tclean uses 'gain' but iterbotsink expects 'loopgain'
        if 'gain' in kwargs and 'loopgain' not in kwargs:
            ip['loopgain'] = kwargs['gain']
        # The C++ iterbotsink requires an 'allimages' sub-record
        ip['allimages'] = {}
        for fld in self.allimpars:
            ip['allimages'][fld] = {
                'imagename': self.allimpars[fld]['imagename'],
                'multiterm': (self.alldecpars[fld]['deconvolver'] == 'mtmfs'),
            }
        self.iterpars: dict = ip

        # ---- misc --------------------------------------------------------
        self.miscpars: dict = _merge(_DEFAULT_MISC, kwargs)

        # ---- dask parallel -----------------------------------------------
        self.parallelpars: dict = _merge(_DEFAULT_PARALLEL, kwargs)

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def specmode(self) -> str:
        return self.allimpars['0']['specmode']

    @property
    def imagename(self) -> str:
        return self.allimpars['0']['imagename']

    @property
    def parallel(self) -> bool:
        return self.parallelpars.get('parallel', False)

    @property
    def niter(self) -> int:
        return self.iterpars.get('niter', 0)

    @property
    def nfields(self) -> int:
        return len(self.allimpars)

    @property
    def nms(self) -> int:
        return len(self.allselpars)

    @property
    def deconvolver(self) -> str:
        return self.alldecpars['0']['deconvolver']

    @property
    def is_cube(self) -> bool:
        return self.specmode in ('cube', 'cubedata', 'cubesource')

    @property
    def is_mfs(self) -> bool:
        return self.specmode == 'mfs'

    # ------------------------------------------------------------------
    # Serialization helpers (for Dask)
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Return a plain-dict snapshot that can be sent to a Dask worker."""
        return dict(
            allselpars=copy.deepcopy(self.allselpars),
            allimpars=copy.deepcopy(self.allimpars),
            allgridpars=copy.deepcopy(self.allgridpars),
            weightpars=copy.deepcopy(self.weightpars),
            alldecpars=copy.deepcopy(self.alldecpars),
            allnormpars=copy.deepcopy(self.allnormpars),
            iterpars=copy.deepcopy(self.iterpars),
            miscpars=copy.deepcopy(self.miscpars),
            parallelpars=copy.deepcopy(self.parallelpars),
        )

    @classmethod
    def from_dict(cls, d: dict) -> PcleanParams:
        """Re-hydrate from a plain dict (inverse of ``to_dict``)."""
        obj = cls.__new__(cls)
        obj.allselpars = d['allselpars']
        obj.allimpars = d['allimpars']
        obj.allgridpars = d['allgridpars']
        obj.weightpars = d['weightpars']
        obj.alldecpars = d['alldecpars']
        obj.allnormpars = d['allnormpars']
        obj.iterpars = d['iterpars']
        obj.miscpars = d['miscpars']
        obj.parallelpars = d['parallelpars']
        return obj

    def clone(self) -> PcleanParams:
        return PcleanParams.from_dict(self.to_dict())

    # ------------------------------------------------------------------
    # Partition helpers (used by parallel engines)
    # ------------------------------------------------------------------

    def make_subcube_params(
        self,
        start: int | str,
        nchan: int,
        image_suffix: str,
    ) -> PcleanParams:
        """Return a copy tuned for a channel sub-range (cube parallelism).

        Args:
            start: Start channel (int) or frequency/velocity string for the subcube.
            nchan: Number of channels in this subcube.
            image_suffix: Suffix appended to the base imagename.
        """
        p = self.clone()
        imp = p.allimpars['0']
        imp['nchan'] = nchan
        imp['start'] = start if isinstance(start, str) else str(start)
        imp['imagename'] = f'{self.imagename}.subcube.{image_suffix}'
        # All param groups must track the new image name
        p.allnormpars['0']['imagename'] = imp['imagename']
        p.allgridpars['0']['imagename'] = imp['imagename']
        if 'allimages' in p.iterpars:
            p.iterpars['allimages']['0']['imagename'] = imp['imagename']
        return p

    def make_rowchunk_params(
        self,
        partition_selpars: dict,
        image_suffix: str,
    ) -> PcleanParams:
        """Return a copy with selection pars limited to a row chunk
        (continuum parallelism)."""
        p = self.clone()
        p.allselpars = copy.deepcopy(partition_selpars)
        imp = p.allimpars['0']
        imp['imagename'] = f'{self.imagename}.part.{image_suffix}'
        p.allnormpars['0']['imagename'] = imp['imagename']
        p.allgridpars['0']['imagename'] = imp['imagename']
        if 'allimages' in p.iterpars:
            p.iterpars['allimages']['0']['imagename'] = imp['imagename']
        return p
