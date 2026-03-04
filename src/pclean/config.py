"""Hierarchical configuration with pydantic v2 models.

Provides a single-source-of-truth :class:`PcleanConfig` that can be
built from:

* Direct Python construction (``PcleanConfig(image=ImageConfig(...))``).
* A YAML file (``PcleanConfig.from_yaml('config.yaml')``).
* Layered composition (``PcleanConfig.merge(base, overlay)``).
* The flat ``pclean()`` kwargs for backward compatibility
  (``PcleanConfig.from_flat_kwargs(...)``).

The resulting config can be converted to CASA-native dicts via the
``to_casa_*()`` bridge methods, or to the legacy ``PcleanParams`` via
:meth:`PcleanConfig.to_params` (deprecated).
"""

from __future__ import annotations

import copy
import logging
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

log = logging.getLogger(__name__)

# ======================================================================
# Sub-config models
# ======================================================================


class SelectionConfig(BaseModel):
    """Data selection parameters."""

    vis: str | list[str] = ''

    @field_validator('vis', mode='before')
    @classmethod
    def _coerce_vis(cls, v: Any) -> str | list[str]:
        """Accept ``Path`` objects and coerce to ``str``."""
        if isinstance(v, Path):
            return str(v)
        if isinstance(v, (list, tuple)):
            return [str(item) for item in v]
        return v
    field: str = ''
    spw: str | list[str] = ''
    timerange: str = ''
    uvrange: str = ''
    antenna: str = ''
    scan: str = ''
    observation: str = ''
    intent: str = ''
    datacolumn: str = 'corrected'


class ImageConfig(BaseModel):
    """Image definition parameters."""

    imagename: str = ''
    imsize: list[int] = Field(default_factory=lambda: [100])
    cell: list[str] | str = '1arcsec'
    phasecenter: str = ''
    stokes: str = 'I'
    projection: str = 'SIN'
    startmodel: str = ''
    specmode: str = 'mfs'
    reffreq: str = ''
    nchan: int = -1
    start: str = ''
    width: str = ''
    outframe: str = 'LSRK'
    veltype: str = 'radio'
    restfreq: list[str] = Field(default_factory=list)
    interpolation: str = 'linear'
    perchanweightdensity: bool = True
    nterms: int = 2


class GridConfig(BaseModel):
    """Gridding parameters."""

    gridder: str = 'standard'
    facets: int = 1
    wprojplanes: int = 1
    vptable: str = ''
    mosweight: bool = True
    aterm: bool = True
    psterm: bool = False
    wbawp: bool = True
    conjbeams: bool = False
    cfcache: str = ''
    usepointing: bool = False
    computepastep: float = 360.0
    rotatepastep: float = 360.0
    pointingoffsetsigdev: list[float] = Field(default_factory=list)
    pblimit: float = 0.2
    normtype: str = 'flatnoise'
    psfphasecenter: str = ''


class WeightConfig(BaseModel):
    """Weighting parameters."""

    weighting: str = 'natural'
    robust: float = 0.5
    noise: str = '1.0Jy'
    npixels: int = 0
    uvtaper: list[str] = Field(default_factory=list)
    fracbw: float | None = None  # pre-computed fractional bandwidth for briggsbwtaper


class DeconvolutionConfig(BaseModel):
    """Deconvolution and masking parameters."""

    deconvolver: str = 'hogbom'
    scales: list[int] = Field(default_factory=list)
    nterms: int = 2
    smallscalebias: float = 0.0
    fusedthreshold: float = 0.0
    largestscale: int = -1
    restoration: bool = True
    restoringbeam: list[str] | str = Field(default_factory=list)
    pbcor: bool = False

    @field_validator('restoringbeam', mode='before')
    @classmethod
    def _coerce_restoringbeam(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            return [v]
        return list(v)
    # Masking
    usemask: str = 'user'
    mask: str = ''
    pbmask: float = 0.0
    sidelobethreshold: float = 3.0
    noisethreshold: float = 5.0
    lownoisethreshold: float = 1.5
    negativethreshold: float = 0.0
    smoothfactor: float = 1.0
    minbeamfrac: float = 0.3
    cutthreshold: float = 0.01
    growiterations: int = 100
    dogrowprune: bool = True
    minpercentchange: float = 0.0
    verbose: bool = False
    fastnoise: bool = True


class IterationConfig(BaseModel):
    """Iteration control parameters."""

    niter: int = 0
    gain: float = 0.1
    threshold: str = '0.0mJy'
    nsigma: float = 0.0
    cycleniter: int = -1
    cyclefactor: float = 1.0
    minpsffraction: float = 0.05
    maxpsffraction: float = 0.8
    interactive: bool = False
    nmajor: int = -1
    fullsummary: bool = False


class MiscConfig(BaseModel):
    """Miscellaneous parameters."""

    restart: bool = True
    savemodel: str = 'none'
    calcres: bool = True
    calcpsf: bool = True
    psfcutoff: float = 0.35


class NormConfig(BaseModel):
    """Normalization parameters."""

    pblimit: float = 0.2
    normtype: str = 'flatnoise'
    psfcutoff: float = 0.35


class SlurmConfig(BaseModel):
    """SLURM batch-job parameters (used when ``cluster.type == 'slurm'``)."""

    queue: str | None = None
    account: str | None = None
    walltime: str = '04:00:00'
    job_mem: str = '20GB'
    cores_per_job: int = 1
    job_extra_directives: list[str] = Field(default_factory=list)
    python: str | None = None
    local_directory: str | None = None
    log_directory: str = 'logs'
    job_script_prologue: list[str] = Field(default_factory=list)

    @field_validator('job_extra_directives', 'job_script_prologue', mode='before')
    @classmethod
    def _coerce_none_to_list(cls, v: Any) -> list[str]:
        if v is None:
            return []
        return list(v)


class ClusterConfig(BaseModel):
    """Dask cluster configuration."""

    type: Literal['local', 'slurm', 'address'] = 'local'
    nworkers: int | None = None
    scheduler_address: str | None = None
    threads_per_worker: int = 1
    memory_limit: str = '0'
    local_directory: str | None = None
    parallel: bool = False
    cube_chunksize: int = -1
    keep_subcubes: bool = False
    keep_partimages: bool = False
    slurm: SlurmConfig = Field(default_factory=SlurmConfig)


# ======================================================================
# Top-level config
# ======================================================================


class PcleanConfig(BaseModel):
    """Top-level hierarchical configuration for pclean.

    All parameters are grouped into logical sub-configs.  This is the
    single source of truth for the application.
    """

    selection: SelectionConfig = Field(default_factory=SelectionConfig)
    image: ImageConfig = Field(default_factory=ImageConfig)
    grid: GridConfig = Field(default_factory=GridConfig)
    weight: WeightConfig = Field(default_factory=WeightConfig)
    deconvolution: DeconvolutionConfig = Field(default_factory=DeconvolutionConfig)
    iteration: IterationConfig = Field(default_factory=IterationConfig)
    normalization: NormConfig = Field(default_factory=NormConfig)
    misc: MiscConfig = Field(default_factory=MiscConfig)
    cluster: ClusterConfig = Field(default_factory=ClusterConfig)

    # ------------------------------------------------------------------
    # YAML I/O
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: str | Path) -> PcleanConfig:
        """Load a config from a YAML file.

        Args:
            path: Path to the YAML file.
        """
        import yaml

        p = Path(path)
        log.info('Loading config from %s', p)
        with p.open() as fh:
            data = yaml.safe_load(fh) or {}
        return cls.model_validate(data)

    def to_yaml(self, path: str | Path) -> None:
        """Dump the config to a YAML file.

        Args:
            path: Destination file path.
        """
        import yaml

        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        data = self.model_dump(mode='python')
        with p.open('w') as fh:
            yaml.dump(data, fh, default_flow_style=False, sort_keys=False)
        log.info('Config written to %s', p)

    # ------------------------------------------------------------------
    # Merge / composition
    # ------------------------------------------------------------------

    @classmethod
    def merge(cls, *configs: PcleanConfig) -> PcleanConfig:
        """Deep-merge multiple configs, later values override earlier ones.

        Args:
            configs: Two or more configs to merge left-to-right.
        """
        if not configs:
            return cls()

        merged = configs[0].model_dump(mode='python')
        for cfg in configs[1:]:
            overlay = cfg.model_dump(mode='python', exclude_defaults=True)
            _deep_update(merged, overlay)
        return cls.model_validate(merged)

    # ------------------------------------------------------------------
    # Build from flat kwargs (backward compat with pclean() signature)
    # ------------------------------------------------------------------

    @classmethod
    def from_flat_kwargs(
        cls,
        vis: str | list[str] = '',
        **kwargs: Any,
    ) -> PcleanConfig:
        """Build a ``PcleanConfig`` from the flat ``pclean()`` keyword arguments.

        This is the backward-compatibility shim that maps the 80+ flat
        keyword arguments into the hierarchical structure.

        Args:
            vis: Measurement set path(s).
            **kwargs: Flat keyword arguments matching the ``pclean()`` signature.
        """
        sel = dict(vis=vis)
        img: dict[str, Any] = {}
        grd: dict[str, Any] = {}
        wgt: dict[str, Any] = {}
        dec: dict[str, Any] = {}
        itr: dict[str, Any] = {}
        msc: dict[str, Any] = {}
        nrm: dict[str, Any] = {}
        clu: dict[str, Any] = {}
        slm: dict[str, Any] = {}

        # Selection fields
        _sel_keys = {
            'field', 'spw', 'timerange', 'uvrange', 'antenna',
            'scan', 'observation', 'intent', 'datacolumn',
        }
        # Image fields
        _img_keys = {
            'imagename', 'imsize', 'cell', 'phasecenter', 'stokes',
            'projection', 'startmodel', 'specmode', 'reffreq', 'nchan',
            'start', 'width', 'outframe', 'veltype', 'restfreq',
            'interpolation', 'perchanweightdensity', 'nterms',
        }
        # Grid fields
        _grd_keys = {
            'gridder', 'facets', 'wprojplanes', 'vptable', 'mosweight',
            'aterm', 'psterm', 'wbawp', 'conjbeams', 'cfcache',
            'usepointing', 'computepastep', 'rotatepastep',
            'pointingoffsetsigdev', 'pblimit', 'normtype', 'psfphasecenter',
        }
        # Weight fields
        _wgt_keys = {'weighting', 'robust', 'noise', 'npixels', 'uvtaper'}
        # Deconvolution fields
        _dec_keys = {
            'deconvolver', 'scales', 'smallscalebias', 'fusedthreshold',
            'largestscale', 'restoration', 'restoringbeam', 'pbcor',
            'usemask', 'mask', 'pbmask', 'sidelobethreshold',
            'noisethreshold', 'lownoisethreshold', 'negativethreshold',
            'smoothfactor', 'minbeamfrac', 'cutthreshold', 'growiterations',
            'dogrowprune', 'minpercentchange', 'verbose', 'fastnoise',
        }
        # Note: nterms appears in both image and deconvolution
        _dec_keys.add('nterms')
        # Iteration fields
        _itr_keys = {
            'niter', 'gain', 'threshold', 'nsigma', 'cycleniter',
            'cyclefactor', 'minpsffraction', 'maxpsffraction', 'interactive',
            'nmajor', 'fullsummary',
        }
        # Misc fields
        _msc_keys = {'restart', 'savemodel', 'calcres', 'calcpsf', 'psfcutoff'}
        # Norm fields
        _nrm_keys = {'pblimit', 'normtype', 'psfcutoff'}
        # Cluster flat keys -> structured
        _clu_keys = {
            'parallel', 'nworkers', 'scheduler_address',
            'threads_per_worker', 'memory_limit', 'local_directory',
            'cube_chunksize', 'keep_subcubes', 'keep_partimages',
        }
        # Cluster type
        _clu_type_key = 'cluster_type'
        # SLURM flat keys (slurm_* prefix -> nested slurm.*)
        _slurm_prefix = 'slurm_'

        for k, v in kwargs.items():
            if k in _sel_keys:
                sel[k] = v
            if k in _img_keys:
                img[k] = v
            if k in _grd_keys:
                grd[k] = v
            if k in _wgt_keys:
                wgt[k] = v
            if k in _dec_keys:
                dec[k] = v
            if k in _itr_keys:
                itr[k] = v
            if k in _msc_keys:
                msc[k] = v
            if k in _nrm_keys:
                nrm[k] = v
            if k in _clu_keys:
                clu[k] = v
            if k == _clu_type_key:
                clu['type'] = v
            if k.startswith(_slurm_prefix):
                slurm_field = k[len(_slurm_prefix):]
                slm[slurm_field] = v

        if slm:
            clu['slurm'] = slm

        data: dict[str, Any] = {}
        if sel:
            data['selection'] = sel
        if img:
            data['image'] = img
        if grd:
            data['grid'] = grd
        if wgt:
            data['weight'] = wgt
        if dec:
            data['deconvolution'] = dec
        if itr:
            data['iteration'] = itr
        if msc:
            data['misc'] = msc
        if nrm:
            data['normalization'] = nrm
        if clu:
            data['cluster'] = clu

        return cls.model_validate(data)

    # ------------------------------------------------------------------
    # Convert to flat kwargs (inverse of from_flat_kwargs)
    # ------------------------------------------------------------------

    def to_flat_kwargs(self) -> dict[str, Any]:
        """Convert back to the flat keyword dict accepted by ``pclean()``.

        Returns:
            Flat dictionary with all parameters.
        """
        kw: dict[str, Any] = {}
        # Selection
        sel = self.selection.model_dump()
        vis = sel.pop('vis', '')
        kw['vis'] = vis
        kw.update(sel)
        # Image
        kw.update(self.image.model_dump())
        # Grid
        kw.update(self.grid.model_dump())
        # Weight
        kw.update(self.weight.model_dump())
        # Deconvolution
        kw.update(self.deconvolution.model_dump())
        # Iteration
        kw.update(self.iteration.model_dump())
        # Misc
        kw.update(self.misc.model_dump())
        # Cluster -> flat
        clu = self.cluster.model_dump()
        slurm = clu.pop('slurm', {})
        clu_type = clu.pop('type', 'local')
        kw['cluster_type'] = clu_type
        kw.update(clu)
        for sk, sv in slurm.items():
            kw[f'slurm_{sk}'] = sv
        return kw

    # ------------------------------------------------------------------
    # CASA-native dict builders
    # ------------------------------------------------------------------

    def to_casa_selpars(self) -> dict[str, dict]:
        """Build CASA ``selectdata``-compatible multi-MS selection dicts.

        Returns:
            Dict keyed ``'ms0'``, ``'ms1'``, etc. with CASA-internal field names.
        """
        sel = self.selection
        vis = sel.vis
        vis_list = [vis] if isinstance(vis, str) else list(vis)
        if not vis_list:
            vis_list = ['']

        spw = sel.spw
        spw_list = [spw] * len(vis_list) if isinstance(spw, str) else list(spw)
        if len(spw_list) != len(vis_list):
            raise ValueError(
                f'spw list length ({len(spw_list)}) must match '
                f'vis list length ({len(vis_list)})'
            )

        result: dict[str, dict] = {}
        for idx, msname in enumerate(vis_list):
            result[f'ms{idx}'] = {
                'msname': msname,
                'field': sel.field,
                'spw': spw_list[idx],
                'timestr': sel.timerange,
                'uvdist': sel.uvrange,
                'antenna': sel.antenna,
                'scan': sel.scan,
                'obs': sel.observation,
                'state': sel.intent,
                'taql': '',
                'datacolumn': sel.datacolumn,
            }
        return result

    def to_casa_impars(self) -> dict[str, dict]:
        """Build CASA ``defineimage``-compatible image parameter dicts.

        Returns:
            Dict keyed ``'0'`` (single field) with image parameters.
        """
        img = self.image
        imsize = list(img.imsize)
        if len(imsize) == 1:
            imsize = imsize * 2
        cell = [img.cell] if isinstance(img.cell, str) else list(img.cell)
        if len(cell) == 1:
            cell = cell * 2
        restfreq = list(img.restfreq) if img.restfreq else []

        return {
            '0': {
                'imagename': img.imagename,
                'imsize': imsize,
                'cell': cell,
                'phasecenter': img.phasecenter,
                'stokes': img.stokes,
                'projection': img.projection,
                'specmode': img.specmode,
                'reffreq': img.reffreq,
                'nchan': img.nchan,
                'start': img.start,
                'width': img.width,
                'outframe': img.outframe,
                'veltype': img.veltype,
                'restfreq': restfreq,
                'interpolation': img.interpolation,
                'perchanweightdensity': img.perchanweightdensity,
                'startmodel': img.startmodel,
                'nterms': img.nterms,
                'deconvolver': self.deconvolution.deconvolver,
                'restart': self.misc.restart,
            },
        }

    def to_casa_gridpars(self) -> dict[str, dict]:
        """Build CASA ``defineimage`` grid parameter dicts."""
        grd = self.grid
        return {
            '0': {
                'gridder': grd.gridder,
                'facets': grd.facets,
                'wprojplanes': grd.wprojplanes,
                'vptable': grd.vptable,
                'mosweight': grd.mosweight,
                'aterm': grd.aterm,
                'psterm': grd.psterm,
                'wbawp': grd.wbawp,
                'conjbeams': grd.conjbeams,
                'cfcache': grd.cfcache,
                'usepointing': grd.usepointing,
                'computepastep': grd.computepastep,
                'rotatepastep': grd.rotatepastep,
                'pointingoffsetsigdev': list(grd.pointingoffsetsigdev),
                'pblimit': grd.pblimit,
                'normtype': grd.normtype,
                'psfphasecenter': grd.psfphasecenter,
                'imagename': self.image.imagename,
                'deconvolver': self.deconvolution.deconvolver,
                'interpolation': self.image.interpolation,
            },
        }

    def to_casa_weightpars(self) -> dict:
        """Build CASA ``setweighting``-compatible weight parameter dict."""
        wgt = self.weight
        weighting = wgt.weighting

        wp: dict[str, Any] = {
            'robust': wgt.robust,
            'noise': wgt.noise,
            'npixels': wgt.npixels,
            'fieldofview': '',
            'uvtaper': list(wgt.uvtaper),
        }

        if weighting == 'briggsbwtaper':
            wp['type'] = 'briggs'
            wp['rmode'] = 'bwtaper'
            # Use pre-computed fracbw if available (e.g. inherited from
            # the parent config when this is a sub-cube).  Otherwise
            # compute it from the current image start/width/nchan.
            if wgt.fracbw is not None and wgt.fracbw > 0:
                wp['fracbw'] = wgt.fracbw
            else:
                from pclean.utils.partition import _parse_freq_hz

                start_hz = _parse_freq_hz(self.image.start)
                width_hz = _parse_freq_hz(self.image.width)
                nchan_full = self.image.nchan
                if start_hz is not None and width_hz is not None and nchan_full > 1:
                    min_freq = start_hz
                    max_freq = start_hz + (nchan_full - 1) * abs(width_hz)
                    if min_freq > max_freq:
                        min_freq, max_freq = max_freq, min_freq
                    wp['fracbw'] = 2.0 * (max_freq - min_freq) / (max_freq + min_freq)
        elif weighting == 'briggsabs':
            wp['type'] = 'briggs'
            wp['rmode'] = 'abs'
        elif weighting == 'briggs':
            wp['type'] = 'briggs'
            wp['rmode'] = 'norm'
        else:
            wp['type'] = weighting
            wp['rmode'] = 'none'

        wp['multifield'] = self.grid.mosweight
        wp['usecubebriggs'] = self.image.perchanweightdensity
        return wp

    def to_casa_decpars(self) -> dict[str, dict]:
        """Build CASA ``setupdeconvolution``-compatible deconvolution dicts."""
        dec = self.deconvolution
        return {
            '0': {
                'deconvolver': dec.deconvolver,
                'scales': list(dec.scales),
                'nterms': dec.nterms,
                'smallscalebias': dec.smallscalebias,
                'fusedthreshold': dec.fusedthreshold,
                'largestscale': dec.largestscale,
                'restoration': dec.restoration,
                'restoringbeam': list(dec.restoringbeam),
                'pbcor': dec.pbcor,
                'usemask': dec.usemask,
                'mask': dec.mask,
                'pbmask': dec.pbmask,
                'sidelobethreshold': dec.sidelobethreshold,
                'noisethreshold': dec.noisethreshold,
                'lownoisethreshold': dec.lownoisethreshold,
                'negativethreshold': dec.negativethreshold,
                'smoothfactor': dec.smoothfactor,
                'minbeamfrac': dec.minbeamfrac,
                'cutthreshold': dec.cutthreshold,
                'growiterations': dec.growiterations,
                'dogrowprune': dec.dogrowprune,
                'minpercentchange': dec.minpercentchange,
                'verbose': dec.verbose,
                'fastnoise': dec.fastnoise,
                'fullsummary': self.iteration.fullsummary,
            },
        }

    def to_casa_normpars(self) -> dict[str, dict]:
        """Build CASA ``setupnormalizer``-compatible normalization dicts."""
        nrm = self.normalization
        dec = self.deconvolution
        nterms = dec.nterms if dec.deconvolver == 'mtmfs' else 1
        return {
            '0': {
                'pblimit': nrm.pblimit,
                'normtype': nrm.normtype,
                'psfcutoff': nrm.psfcutoff,
                'imagename': self.image.imagename,
                'nterms': nterms,
                'deconvolver': dec.deconvolver,
                'specmode': self.image.specmode,
            },
        }

    def to_casa_iterpars(self) -> dict:
        """Build CASA ``setupiteration``-compatible iteration parameter dict."""
        itr = self.iteration
        return {
            'niter': itr.niter,
            'loopgain': itr.gain,
            'threshold': itr.threshold,
            'nsigma': itr.nsigma,
            'cycleniter': itr.cycleniter,
            'cyclefactor': itr.cyclefactor,
            'minpsffraction': itr.minpsffraction,
            'maxpsffraction': itr.maxpsffraction,
            'interactive': itr.interactive,
            'nmajor': itr.nmajor,
            'fullsummary': itr.fullsummary,
            'savemodel': self.misc.savemodel,
            'allimages': {
                '0': {
                    'imagename': self.image.imagename,
                    'multiterm': (self.deconvolution.deconvolver == 'mtmfs'),
                },
            },
        }

    def to_casa_miscpars(self) -> dict:
        """Build miscellaneous parameter dict."""
        return {
            'restart': self.misc.restart,
            'calcres': self.misc.calcres,
            'calcpsf': self.misc.calcpsf,
        }

    def to_casa_bundle(self) -> dict:
        """Produce a serializable dict of all CASA-native parameter dicts.

        This is the worker-facing payload for continuum-parallel actors.
        Cube-parallel workers receive a serialized ``PcleanConfig`` instead.

        Returns:
            Dict with keys matching the former ``PcleanParams.to_dict()`` format.
        """
        return {
            'allselpars': self.to_casa_selpars(),
            'allimpars': self.to_casa_impars(),
            'allgridpars': self.to_casa_gridpars(),
            'weightpars': self.to_casa_weightpars(),
            'alldecpars': self.to_casa_decpars(),
            'allnormpars': self.to_casa_normpars(),
            'iterpars': self.to_casa_iterpars(),
            'miscpars': self.to_casa_miscpars(),
        }

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def specmode(self) -> str:
        """Spectral mode (``'mfs'``, ``'cube'``, etc.)."""
        return self.image.specmode

    @property
    def imagename(self) -> str:
        """Output image name prefix."""
        return self.image.imagename

    @property
    def parallel(self) -> bool:
        """Whether Dask parallelism is enabled."""
        return self.cluster.parallel

    @property
    def niter(self) -> int:
        """Maximum number of CLEAN iterations."""
        return self.iteration.niter

    @property
    def is_cube(self) -> bool:
        """True if specmode indicates cube imaging."""
        return self.image.specmode in ('cube', 'cubedata', 'cubesource')

    @property
    def is_mfs(self) -> bool:
        """True if specmode is multi-frequency synthesis."""
        return self.image.specmode == 'mfs'

    @property
    def nfields(self) -> int:
        """Number of image fields (currently always 1)."""
        return 1

    @property
    def nms(self) -> int:
        """Number of measurement sets."""
        vis = self.selection.vis
        if isinstance(vis, str):
            return 1 if vis else 0
        return len(vis)

    # ------------------------------------------------------------------
    # Partition helpers
    # ------------------------------------------------------------------

    def make_subcube_config(
        self,
        start: int | str,
        nchan: int,
        image_suffix: str,
    ) -> PcleanConfig:
        """Return a copy tuned for a channel sub-range (cube parallelism).

        Args:
            start: Start channel (int) or frequency/velocity string.
            nchan: Number of channels in this subcube.
            image_suffix: Suffix appended to the base imagename.
        """
        data = self.model_dump(mode='python')
        data['image']['nchan'] = nchan
        data['image']['start'] = start if isinstance(start, str) else str(start)
        data['image']['imagename'] = f'{self.imagename}.subcube.{image_suffix}'
        # Pre-compute fracbw from the *parent* (full-cube) config so that
        # subcubes with nchan=1 still get the correct fractional bandwidth
        # for briggsbwtaper weighting.
        if self.weight.weighting == 'briggsbwtaper' and self.weight.fracbw is None:
            parent_wp = self.to_casa_weightpars()
            if 'fracbw' in parent_wp:
                data['weight']['fracbw'] = parent_wp['fracbw']
        return PcleanConfig.model_validate(data)

    # ------------------------------------------------------------------
    # Deprecated bridge to PcleanParams
    # ------------------------------------------------------------------

    def to_params(self) -> Any:
        """Convert to the legacy ``PcleanParams`` used by engines.

        .. deprecated::
            Use the ``to_casa_*()`` methods or pass ``PcleanConfig`` directly
            to engines instead.

        Returns:
            A fully constructed ``PcleanParams`` instance.
        """
        import warnings

        from pclean.params import PcleanParams

        warnings.warn(
            'PcleanConfig.to_params() is deprecated; use to_casa_*() methods '
            'or pass PcleanConfig directly to engines.',
            DeprecationWarning,
            stacklevel=2,
        )
        kw = self.to_flat_kwargs()
        vis = kw.pop('vis', '')
        if isinstance(vis, str):
            vis = [vis] if vis else ['']
        return PcleanParams(vis=vis, **kw)


# ======================================================================
# Helpers
# ======================================================================


def _deep_update(base: dict, overlay: dict) -> dict:
    """Recursively update *base* with values from *overlay* (in-place).

    Args:
        base: Base dictionary to update.
        overlay: Dictionary whose values override *base*.

    Returns:
        The mutated *base* dictionary.
    """
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = copy.deepcopy(v)
    return base


def _resolve_package_yaml(rel_path: str) -> Path | None:
    """Resolve a YAML file bundled inside the ``pclean`` package.

    Uses :mod:`importlib.resources` so the lookup works both in editable
    installs and after ``pip install``.

    Args:
        rel_path: Path relative to the ``pclean`` package root
            (e.g. ``'configs/presets/vlass.yaml'``).

    Returns:
        Resolved filesystem :class:`Path`, or ``None`` if not found.
    """
    from importlib.resources import as_file, files

    pkg_resource = files('pclean').joinpath(rel_path)
    try:
        # as_file() ensures the resource is available on the filesystem
        # (important for zip-packaged distributions).
        ctx = as_file(pkg_resource)
        resolved = ctx.__enter__()  # noqa: PLC2801
        if resolved.exists():
            return resolved
    except (FileNotFoundError, TypeError):
        pass
    return None


def load_defaults() -> PcleanConfig:
    """Load the bundled ``defaults.yaml`` reference snapshot.

    This is equivalent to ``PcleanConfig()`` (all pydantic defaults)
    but read from the packaged YAML file for verification purposes.

    Returns:
        A ``PcleanConfig`` with the reference default values.
    """
    pkg_path = _resolve_package_yaml('configs/defaults.yaml')
    if pkg_path is not None:
        log.info('Loading bundled defaults from %s', pkg_path)
        return PcleanConfig.from_yaml(pkg_path)

    # Fallback: CWD
    cwd_path = Path('configs') / 'defaults.yaml'
    if cwd_path.exists():
        return PcleanConfig.from_yaml(cwd_path)

    log.warning('defaults.yaml not found; returning pydantic defaults')
    return PcleanConfig()


def load_preset(name: str) -> PcleanConfig:
    """Load a named preset from the bundled ``configs/presets/`` directory.

    Searches first inside the installed package, then falls back to
    CWD-relative paths.

    Args:
        name: Preset name (without ``.yaml`` extension).
    """
    # 1. Packaged preset (works after pip install)
    pkg_path = _resolve_package_yaml(f'configs/presets/{name}.yaml')
    if pkg_path is not None:
        log.info('Loading preset %s from package: %s', name, pkg_path)
        return PcleanConfig.from_yaml(pkg_path)

    # 2. CWD fallback paths
    candidates = [
        Path('configs') / 'presets' / f'{name}.yaml',
        Path(f'{name}.yaml'),
    ]
    for p in candidates:
        if p.exists():
            log.info('Loading preset %s from %s', name, p)
            return PcleanConfig.from_yaml(p)

    searched = ['<package>/configs/presets/'] + [str(c) for c in candidates]
    raise FileNotFoundError(f'Preset {name!r} not found; searched: {", ".join(searched)}')
