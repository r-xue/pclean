"""CLI parser builder from pydantic config models (single source of truth).

All argparse defaults are read at build time from the corresponding pydantic
field via ``_d(ModelClass, 'field_name')``.  Changing a default in config.py
automatically propagates to the CLI — no manual update needed here.

Adding a new settable parameter:
  1. Add the field to the appropriate sub-model in config.py.
  2. Add the matching ``_str/_int/_float/_bool_opt/_list_*`` call below using
     the correct ModelClass and field name.
  3. If the argparse dest name differs from what ``PcleanConfig.from_flat_kwargs``
     expects (e.g., SLURM prefix), add a rename in ``_cli_to_flat_kwargs`` in
     ``__main__.py``.
"""

from __future__ import annotations

import argparse
from typing import Any

from pydantic import BaseModel


def _d(model: type[BaseModel], field: str) -> Any:
    """Return the default value for a pydantic model field.

    Never hardcode a default in the parser — always read it from here.
    """
    fi = model.model_fields[field]
    if fi.default_factory is not None:
        return fi.default_factory()
    # fi.default is PydanticUndefined only when no default exists; all our
    # fields have defaults so this always returns something sensible.
    return fi.default


# ---------------------------------------------------------------------------
# Helpers for common argparse patterns
# ---------------------------------------------------------------------------

def _bool_opt(p: argparse.ArgumentParser, flag: str, model: type[BaseModel],
               field: str, **extra: Any) -> None:
    """Add ``--flag`` / ``--no-flag`` pair with default from pydantic model."""
    dest = flag.replace('-', '_')
    p.add_argument(f'--{flag}', dest=dest,
                   action=argparse.BooleanOptionalAction,
                   default=_d(model, field), **extra)


def _str(p: argparse.ArgumentParser, flag: str, model: type[BaseModel],
          field: str, **extra: Any) -> None:
    """Add a string argument with default from pydantic model."""
    dest = flag.replace('-', '_')
    p.add_argument(f'--{flag}', dest=dest, default=_d(model, field), **extra)


def _int(p: argparse.ArgumentParser, flag: str, model: type[BaseModel],
          field: str, **extra: Any) -> None:
    """Add an int argument with default from pydantic model."""
    dest = flag.replace('-', '_')
    p.add_argument(f'--{flag}', dest=dest, type=int,
                   default=_d(model, field), **extra)


def _float(p: argparse.ArgumentParser, flag: str, model: type[BaseModel],
            field: str, **extra: Any) -> None:
    """Add a float argument with default from pydantic model."""
    dest = flag.replace('-', '_')
    p.add_argument(f'--{flag}', dest=dest, type=float,
                   default=_d(model, field), **extra)


def _list_str(p: argparse.ArgumentParser, flag: str, model: type[BaseModel],
               field: str, nargs: str = '*', **extra: Any) -> None:
    """Add a string-list argument with default from pydantic model."""
    dest = flag.replace('-', '_')
    p.add_argument(f'--{flag}', dest=dest, nargs=nargs,
                   default=_d(model, field), **extra)


def _list_int(p: argparse.ArgumentParser, flag: str, model: type[BaseModel],
               field: str, nargs: str = '*', **extra: Any) -> None:
    """Add an int-list argument with default from pydantic model."""
    dest = flag.replace('-', '_')
    p.add_argument(f'--{flag}', dest=dest, nargs=nargs, type=int,
                   default=_d(model, field), **extra)


def _list_float(p: argparse.ArgumentParser, flag: str, model: type[BaseModel],
                 field: str, nargs: str = '*', **extra: Any) -> None:
    """Add a float-list argument with default from pydantic model."""
    dest = flag.replace('-', '_')
    p.add_argument(f'--{flag}', dest=dest, nargs=nargs, type=float,
                   default=_d(model, field), **extra)


# ---------------------------------------------------------------------------
# Main argument builder
# ---------------------------------------------------------------------------

def add_pclean_args(parser: argparse.ArgumentParser) -> None:
    """Populate *parser* with all pclean arguments.

    Every default is derived from the corresponding pydantic model field.
    The ``# subconfig: FieldName`` comments cross-reference config.py.
    """
    from pclean.config import (
        ClusterConfig,
        DeconvolutionConfig,
        GridConfig,
        ImageConfig,
        IterationConfig,
        MiscConfig,
        SelectionConfig,
        SlurmConfig,
        WeightConfig,
    )

    # ------------------------------------------------------------------ #
    # Data selection  (SelectionConfig)                                   #
    # ------------------------------------------------------------------ #
    # vis is special: CLI accepts multiple values, model stores str | list[str]
    parser.add_argument('--vis', nargs='+', default=None,
                        help='Measurement set path(s)')
    _str(parser, 'field',       SelectionConfig, 'field')
    _str(parser, 'spw',         SelectionConfig, 'spw')
    _str(parser, 'timerange',   SelectionConfig, 'timerange')
    _str(parser, 'uvrange',     SelectionConfig, 'uvrange')
    _str(parser, 'antenna',     SelectionConfig, 'antenna')
    _str(parser, 'scan',        SelectionConfig, 'scan')
    _str(parser, 'observation', SelectionConfig, 'observation')
    _str(parser, 'intent',      SelectionConfig, 'intent')
    _str(parser, 'datacolumn',  SelectionConfig, 'datacolumn')

    # ------------------------------------------------------------------ #
    # Image definition  (ImageConfig)                                     #
    # ------------------------------------------------------------------ #
    _str(parser,       'imagename',            ImageConfig, 'imagename')
    # imsize: list[int], coerced to [x, x] if single value in config
    _list_int(parser,  'imsize',               ImageConfig, 'imsize', nargs='+')
    # cell: list[str] | str in model, but CLI takes a scalar string (model
    # accepts a plain string and expands it to [cell, cell] internally)
    _str(parser,       'cell',                 ImageConfig, 'cell')
    _str(parser,       'phasecenter',          ImageConfig, 'phasecenter')
    _str(parser,       'stokes',               ImageConfig, 'stokes')
    _str(parser,       'projection',           ImageConfig, 'projection')
    _str(parser,       'startmodel',           ImageConfig, 'startmodel')
    _str(parser,       'specmode',             ImageConfig, 'specmode')
    _str(parser,       'reffreq',              ImageConfig, 'reffreq')
    _int(parser,       'nchan',                ImageConfig, 'nchan')
    _str(parser,       'start',                ImageConfig, 'start')
    _str(parser,       'width',                ImageConfig, 'width')
    _str(parser,       'outframe',             ImageConfig, 'outframe')
    _str(parser,       'veltype',              ImageConfig, 'veltype')
    _list_str(parser,  'restfreq',             ImageConfig, 'restfreq')
    _str(parser,       'interpolation',        ImageConfig, 'interpolation')
    _bool_opt(parser,  'perchanweightdensity', ImageConfig, 'perchanweightdensity')
    _int(parser,       'nterms',               ImageConfig, 'nterms')

    # ------------------------------------------------------------------ #
    # Gridding  (GridConfig)                                              #
    # ------------------------------------------------------------------ #
    _str(parser,       'gridder',              GridConfig, 'gridder')
    _int(parser,       'facets',               GridConfig, 'facets')
    _int(parser,       'wprojplanes',          GridConfig, 'wprojplanes')
    _str(parser,       'vptable',              GridConfig, 'vptable')
    _bool_opt(parser,  'mosweight',            GridConfig, 'mosweight')
    _bool_opt(parser,  'aterm',                GridConfig, 'aterm')
    _bool_opt(parser,  'psterm',               GridConfig, 'psterm')
    _bool_opt(parser,  'wbawp',                GridConfig, 'wbawp')
    _bool_opt(parser,  'conjbeams',            GridConfig, 'conjbeams')
    _str(parser,       'cfcache',              GridConfig, 'cfcache')
    _bool_opt(parser,  'usepointing',          GridConfig, 'usepointing')
    _float(parser,     'computepastep',        GridConfig, 'computepastep')
    _float(parser,     'rotatepastep',         GridConfig, 'rotatepastep')
    _list_float(parser,'pointingoffsetsigdev', GridConfig, 'pointingoffsetsigdev')
    _float(parser,     'pblimit',              GridConfig, 'pblimit')
    _str(parser,       'normtype',             GridConfig, 'normtype')
    _str(parser,       'psfphasecenter',       GridConfig, 'psfphasecenter')

    # ------------------------------------------------------------------ #
    # Weighting  (WeightConfig)                                           #
    # ------------------------------------------------------------------ #
    _str(parser,       'weighting',            WeightConfig, 'weighting')
    _float(parser,     'robust',               WeightConfig, 'robust')
    _str(parser,       'noise',                WeightConfig, 'noise')
    _int(parser,       'npixels',              WeightConfig, 'npixels')
    _list_str(parser,  'uvtaper',              WeightConfig, 'uvtaper')
    # fracbw is computed internally from start/width/nchan; not a user-facing
    # CLI flag.  If needed it can be passed via --pconfig YAML.

    # ------------------------------------------------------------------ #
    # Deconvolution & masking  (DeconvolutionConfig)                      #
    # ------------------------------------------------------------------ #
    _str(parser,       'deconvolver',          DeconvolutionConfig, 'deconvolver')
    _list_int(parser,  'scales',               DeconvolutionConfig, 'scales')
    _float(parser,     'smallscalebias',       DeconvolutionConfig, 'smallscalebias')
    _float(parser,     'fusedthreshold',       DeconvolutionConfig, 'fusedthreshold')
    _int(parser,       'largestscale',         DeconvolutionConfig, 'largestscale')
    _bool_opt(parser,  'restoration',          DeconvolutionConfig, 'restoration')
    _list_str(parser,  'restoringbeam',        DeconvolutionConfig, 'restoringbeam')
    _bool_opt(parser,  'pbcor',                DeconvolutionConfig, 'pbcor')
    _str(parser,       'usemask',              DeconvolutionConfig, 'usemask')
    _str(parser,       'mask',                 DeconvolutionConfig, 'mask')
    _float(parser,     'pbmask',               DeconvolutionConfig, 'pbmask')
    _float(parser,     'sidelobethreshold',    DeconvolutionConfig, 'sidelobethreshold')
    _float(parser,     'noisethreshold',       DeconvolutionConfig, 'noisethreshold')
    _float(parser,     'lownoisethreshold',    DeconvolutionConfig, 'lownoisethreshold')
    _float(parser,     'negativethreshold',    DeconvolutionConfig, 'negativethreshold')
    _float(parser,     'smoothfactor',         DeconvolutionConfig, 'smoothfactor')
    _float(parser,     'minbeamfrac',          DeconvolutionConfig, 'minbeamfrac')
    _float(parser,     'cutthreshold',         DeconvolutionConfig, 'cutthreshold')
    _int(parser,       'growiterations',       DeconvolutionConfig, 'growiterations')
    _bool_opt(parser,  'dogrowprune',          DeconvolutionConfig, 'dogrowprune')
    _float(parser,     'minpercentchange',     DeconvolutionConfig, 'minpercentchange')
    _bool_opt(parser,  'verbose',              DeconvolutionConfig, 'verbose')
    _bool_opt(parser,  'fastnoise',            DeconvolutionConfig, 'fastnoise')
    _bool_opt(parser,  'python-automask',      DeconvolutionConfig, 'python_automask',
              help='Use Python automasking instead of C++ SDMaskHandler')

    # ------------------------------------------------------------------ #
    # Iteration control  (IterationConfig)                                #
    # ------------------------------------------------------------------ #
    _int(parser,       'niter',                IterationConfig, 'niter')
    _float(parser,     'gain',                 IterationConfig, 'gain')
    _str(parser,       'threshold',            IterationConfig, 'threshold')
    _float(parser,     'nsigma',               IterationConfig, 'nsigma')
    _int(parser,       'cycleniter',           IterationConfig, 'cycleniter')
    _float(parser,     'cyclefactor',          IterationConfig, 'cyclefactor')
    _float(parser,     'minpsffraction',       IterationConfig, 'minpsffraction')
    _float(parser,     'maxpsffraction',       IterationConfig, 'maxpsffraction')
    _bool_opt(parser,  'interactive',          IterationConfig, 'interactive')
    _int(parser,       'nmajor',               IterationConfig, 'nmajor')
    _bool_opt(parser,  'fullsummary',          IterationConfig, 'fullsummary')

    # ------------------------------------------------------------------ #
    # Miscellaneous  (MiscConfig)                                         #
    # ------------------------------------------------------------------ #
    _str(parser,       'savemodel',            MiscConfig, 'savemodel')
    _bool_opt(parser,  'restart',              MiscConfig, 'restart')
    _bool_opt(parser,  'calcres',              MiscConfig, 'calcres')
    _bool_opt(parser,  'calcpsf',              MiscConfig, 'calcpsf')
    _float(parser,     'psfcutoff',            MiscConfig, 'psfcutoff')

    # ------------------------------------------------------------------ #
    # Cluster  (ClusterConfig)                                            #
    # ------------------------------------------------------------------ #
    # parallel: purely additive (--parallel enables, --no-parallel rarely
    # needed), so keep as store_true rather than BooleanOptionalAction.
    parser.add_argument('--parallel', action='store_true',
                        default=_d(ClusterConfig, 'parallel'))
    parser.add_argument('--nworkers', type=int,
                        default=_d(ClusterConfig, 'nworkers'))
    _str(parser,       'scheduler-address',    ClusterConfig, 'scheduler_address')
    _int(parser,       'threads-per-worker',   ClusterConfig, 'threads_per_worker')
    _str(parser,       'memory-limit',         ClusterConfig, 'memory_limit')
    _str(parser,       'local-directory',      ClusterConfig, 'local_directory')
    # cluster.type exposed as --cluster-type; choices from Literal annotation
    parser.add_argument('--cluster-type', dest='cluster_type',
                        default=_d(ClusterConfig, 'type'),
                        choices=['local', 'slurm', 'address'],
                        help='Dask cluster backend')
    _int(parser,       'cube-chunksize',       ClusterConfig, 'cube_chunksize')
    _bool_opt(parser,  'keep-subcubes',        ClusterConfig, 'keep_subcubes')
    _bool_opt(parser,  'keep-partimages',      ClusterConfig, 'keep_partimages')
    # concat_mode: choices from Literal annotation
    parser.add_argument('--concat-mode', dest='concat_mode',
                        default=_d(ClusterConfig, 'concat_mode'),
                        choices=['auto', 'paged', 'virtual', 'movevirtual'])

    # ------------------------------------------------------------------ #
    # SLURM  (SlurmConfig, accessed via --slurm-* prefix)                #
    # dest uses slurm_ prefix so _cli_to_flat_kwargs maps them correctly  #
    # ------------------------------------------------------------------ #
    _str(parser,       'slurm-queue',              SlurmConfig, 'queue',
         help='SLURM partition name')
    _str(parser,       'slurm-account',            SlurmConfig, 'account',
         help='SLURM account')
    _str(parser,       'slurm-walltime',           SlurmConfig, 'walltime',
         help='Per-job wall time')
    _str(parser,       'slurm-job-mem',            SlurmConfig, 'job_mem',
         help='Per-job memory')
    _int(parser,       'slurm-cores-per-job',      SlurmConfig, 'cores_per_job',
         help='CPUs per SLURM job')
    _str(parser,       'slurm-job-name',           SlurmConfig, 'job_name',
         help='SLURM job name')
    _list_str(parser,  'slurm-job-extra-directives', SlurmConfig, 'job_extra_directives',
              help='Extra #SBATCH directives')
    _str(parser,       'slurm-python',             SlurmConfig, 'python',
         help='Python path on compute nodes')
    _str(parser,       'slurm-local-directory',    SlurmConfig, 'local_directory',
         help='Worker scratch directory')
    _str(parser,       'slurm-log-directory',      SlurmConfig, 'log_directory',
         help='SLURM log directory')
    _list_str(parser,  'slurm-job-script-prologue', SlurmConfig, 'job_script_prologue',
              help='Shell lines prepended to the worker job script')

    # ------------------------------------------------------------------ #
    # pclean meta-flags (not part of any config sub-model)               #
    # ------------------------------------------------------------------ #
    parser.add_argument('--pconfig', default=None,
                        help='Path to a YAML configuration file')
    parser.add_argument('--preset', action='append', default=None,
                        help='Named preset(s) to load (repeatable; later override earlier)')
    parser.add_argument('--dump-config', metavar='PATH', default=None,
                        help='Dump the effective (merged) config to YAML and exit')
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])


# ---------------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------------

def build_cli_parser() -> argparse.ArgumentParser:
    """Return the fully built pclean CLI parser.

    All argument defaults are derived from PcleanConfig pydantic sub-models,
    ensuring the CLI is always in sync with config.py.
    """
    p = argparse.ArgumentParser(
        prog='pclean',
        description='Parallel CLEAN imaging with Dask and CASA tools',
    )
    add_pclean_args(p)

    # ------------------------------------------------------------------ #
    # Subcommands                                                         #
    # ------------------------------------------------------------------ #
    sub = p.add_subparsers(dest='subcommand')

    # pclean submit ------------------------------------------------------- #
    from pclean.config import SubmitConfig

    sp = sub.add_parser(
        'submit',
        help='Generate and submit a SLURM coordinator job',
        description=(
            'Submit a pclean YAML config as a SLURM coordinator job. '
            'The coordinator activates the pixi environment, runs '
            'python -m pclean --pconfig <config>, and dask-jobqueue '
            'spawns worker jobs automatically.'
        ),
    )
    sp.add_argument('submit_config', metavar='CONFIG', help='Path to pclean YAML config')
    sp.add_argument('--workdir', default=_d(SubmitConfig, 'workdir'),
                    help='Working directory for imaging output')
    sp.add_argument('--pixi-project-dir', dest='pixi_project_dir',
                    default=_d(SubmitConfig, 'pixi_project_dir'),
                    help='Root of the pclean pixi project')
    sp.add_argument('--pixi-env', dest='pixi_env',
                    default=_d(SubmitConfig, 'pixi_env'),
                    help='Pixi environment name')
    sp.add_argument('--coordinator-mem', dest='coordinator_mem',
                    default=_d(SubmitConfig, 'coordinator_mem'),
                    help='Coordinator job memory')
    sp.add_argument('--coordinator-cpus', dest='coordinator_cpus', type=int,
                    default=_d(SubmitConfig, 'coordinator_cpus'),
                    help='Coordinator job CPUs')
    sp.add_argument('--coordinator-walltime', dest='coordinator_walltime',
                    default=_d(SubmitConfig, 'coordinator_walltime'),
                    help='Coordinator wall time')
    sp.add_argument('--coordinator-job-name', dest='coordinator_job_name',
                    default=_d(SubmitConfig, 'coordinator_job_name'),
                    help='Coordinator SLURM job name')
    sp.add_argument('--log-dir', dest='log_dir',
                    default=_d(SubmitConfig, 'log_dir'),
                    help='Log directory')
    sp.add_argument('--psrecord', action=argparse.BooleanOptionalAction,
                    default=_d(SubmitConfig, 'psrecord'),
                    help='Wrap in psrecord')
    sp.add_argument('--dry-run', dest='dry_run', action='store_true', default=False,
                    help='Print sbatch script without submitting')

    return p
