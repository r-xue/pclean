"""CLI entry point for pclean.

Usage::

    python -m pclean --vis my.ms --imagename out --specmode cube \
        --imsize 512 512 --cell 1arcsec --niter 1000 \
        --parallel --nworkers 8

    # Or with a YAML config file:
    python -m pclean --config pclean_config.yaml --cluster.nworkers 48

All tclean parameters are supported as ``--<name> <value>`` flags.
When ``--config`` is given, the YAML file provides the base and any
CLI flags override it.
"""

from __future__ import annotations

import argparse
import json
import logging


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog='pclean',
        description='Parallel CLEAN imaging with Dask and CASA tools',
    )
    # Config file
    p.add_argument(
        '--config',
        default=None,
        help='Path to a YAML configuration file',
    )
    p.add_argument(
        '--preset',
        action='append',
        default=None,
        help='Named preset(s) to load (repeatable; later presets override earlier ones)',
    )
    p.add_argument(
        '--dump-config',
        default=None,
        metavar='PATH',
        help='Dump the effective (merged) configuration to a YAML file and exit',
    )
    # Data selection
    p.add_argument('--vis', nargs='+', default=[''])
    p.add_argument('--field', default='')
    p.add_argument('--spw', default='')
    p.add_argument('--timerange', default='')
    p.add_argument('--uvrange', default='')
    p.add_argument('--antenna', default='')
    p.add_argument('--scan', default='')
    p.add_argument('--observation', default='')
    p.add_argument('--intent', default='')
    p.add_argument('--datacolumn', default='corrected')
    # Image
    p.add_argument('--imagename', default='')
    p.add_argument('--imsize', nargs='+', type=int, default=[100])
    p.add_argument('--cell', default='1arcsec')
    p.add_argument('--phasecenter', default='')
    p.add_argument('--stokes', default='I')
    p.add_argument('--projection', default='SIN')
    # Spectral
    p.add_argument('--specmode', default='mfs')
    p.add_argument('--nchan', type=int, default=-1)
    p.add_argument('--start', default='')
    p.add_argument('--width', default='')
    p.add_argument('--outframe', default='LSRK')
    p.add_argument('--restfreq', nargs='*', default=[])
    p.add_argument('--interpolation', default='linear')
    # Gridder
    p.add_argument('--gridder', default='standard')
    p.add_argument('--wprojplanes', type=int, default=1)
    p.add_argument('--pblimit', type=float, default=0.2)
    # Deconvolver
    p.add_argument('--deconvolver', default='hogbom')
    p.add_argument('--scales', nargs='*', type=int, default=[])
    p.add_argument('--nterms', type=int, default=2)
    # Weighting
    p.add_argument('--weighting', default='natural')
    p.add_argument('--robust', type=float, default=0.5)
    p.add_argument('--uvtaper', nargs='*', default=[])
    # Iteration
    p.add_argument('--niter', type=int, default=0)
    p.add_argument('--gain', type=float, default=0.1)
    p.add_argument('--threshold', default='0.0mJy')
    p.add_argument('--nsigma', type=float, default=0.0)
    p.add_argument('--cycleniter', type=int, default=-1)
    p.add_argument('--cyclefactor', type=float, default=1.0)
    p.add_argument('--nmajor', type=int, default=-1)
    # Masking
    p.add_argument('--usemask', default='user')
    p.add_argument('--mask', default='')
    p.add_argument('--pbmask', type=float, default=0.0)
    p.add_argument('--python-automask', dest='python_automask',
                   action='store_true', default=True,
                   help='Use Python automasking instead of C++ (default)')
    p.add_argument('--no-python-automask', dest='python_automask',
                   action='store_false',
                   help='Use C++ automasking (CASA default)')
    # Restoration
    p.add_argument('--restoration', action='store_true', default=True)
    p.add_argument('--no-restoration', dest='restoration', action='store_false')
    p.add_argument('--pbcor', action='store_true', default=False)
    # Misc
    p.add_argument('--savemodel', default='none')
    p.add_argument('--restart', action='store_true', default=True)
    p.add_argument('--no-restart', dest='restart', action='store_false')
    # Dask parallel
    p.add_argument('--parallel', action='store_true', default=False)
    p.add_argument('--nworkers', type=int, default=None)
    p.add_argument('--scheduler-address', default=None)
    p.add_argument('--threads-per-worker', type=int, default=1)
    p.add_argument('--memory-limit', default='auto')
    p.add_argument('--local-directory', default=None)
    # Cluster backend
    p.add_argument(
        '--cluster-type',
        default='local',
        choices=['local', 'slurm', 'address'],
        help='Dask cluster backend (default: local)',
    )
    # SLURM options (only used with --cluster-type slurm)
    p.add_argument('--slurm-queue', default=None, help='SLURM partition name')
    p.add_argument('--slurm-account', default=None, help='SLURM account')
    p.add_argument('--slurm-walltime', default='04:00:00', help='Per-job wall time')
    p.add_argument('--slurm-job-mem', default='20GB', help='Per-job memory')
    p.add_argument('--slurm-cores-per-job', type=int, default=1, help='CPUs per SLURM job')
    p.add_argument('--slurm-python', default=None, help='Python path on compute nodes')
    p.add_argument('--slurm-local-directory', default=None, help='Worker scratch dir')
    p.add_argument('--slurm-log-directory', default='logs', help='SLURM log directory')
    # Logging
    p.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    return p


def main(argv=None):
    """Parse CLI arguments, configure logging, and run pclean.

    Args:
        argv: Command-line arguments to parse. Defaults to ``sys.argv[1:]``
            when *None*.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    from pclean import CustomFormatter

    handler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter())
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        handlers=[handler],
    )

    config_path = args.config
    presets = args.preset
    dump_config = args.dump_config

    # ------------------------------------------------------------------
    # Config-file path: build from YAML + presets, override with CLI
    # ------------------------------------------------------------------
    if config_path is not None or presets is not None:
        from pclean.config import PcleanConfig, load_preset

        layers: list[PcleanConfig] = []
        if config_path is not None:
            layers.append(PcleanConfig.from_yaml(config_path))
        for name in (presets or []):
            layers.append(load_preset(name))

        # CLI overrides as a flat-kwargs overlay
        cli_kwargs = _cli_to_flat_kwargs(args)
        if cli_kwargs:
            layers.append(PcleanConfig.from_flat_kwargs(**cli_kwargs))

        cfg = PcleanConfig.merge(*layers) if len(layers) > 1 else layers[0]

        if dump_config:
            cfg.to_yaml(dump_config)
            print(f'Config written to {dump_config}')
            return

        from pclean.pclean import pclean
        result = pclean(config=cfg)
        print(json.dumps(result, indent=2, default=str))
        return

    # ------------------------------------------------------------------
    # Legacy flat-kwargs path (no --config)
    # ------------------------------------------------------------------
    if dump_config:
        from pclean.config import PcleanConfig
        cli_kwargs = _cli_to_flat_kwargs(args)
        vis = cli_kwargs.pop('vis', '')
        cfg = PcleanConfig.from_flat_kwargs(vis=vis, **cli_kwargs)
        cfg.to_yaml(dump_config)
        print(f'Config written to {dump_config}')
        return

    from pclean.pclean import pclean

    kwargs = _cli_to_flat_kwargs(args)
    vis = kwargs.pop('vis')
    result = pclean(vis=vis, **kwargs)
    print(json.dumps(result, indent=2, default=str))


def _cli_to_flat_kwargs(args: argparse.Namespace) -> dict:
    """Convert parsed CLI args to the flat kwargs dict for ``pclean()``."""
    kwargs = vars(args).copy()
    # Remove meta-args
    kwargs.pop('log_level', None)
    kwargs.pop('config', None)
    kwargs.pop('preset', None)
    kwargs.pop('dump_config', None)
    # Normalise CLI names to Python names
    kwargs['scheduler_address'] = kwargs.pop('scheduler_address', None)
    kwargs['threads_per_worker'] = kwargs.pop('threads_per_worker', 1)
    kwargs['memory_limit'] = kwargs.pop('memory_limit', '0')
    kwargs['local_directory'] = kwargs.pop('local_directory', None)
    kwargs['cluster_type'] = kwargs.pop('cluster_type', 'local')
    kwargs['slurm_queue'] = kwargs.pop('slurm_queue', None)
    kwargs['slurm_account'] = kwargs.pop('slurm_account', None)
    kwargs['slurm_walltime'] = kwargs.pop('slurm_walltime', '04:00:00')
    kwargs['slurm_job_mem'] = kwargs.pop('slurm_job_mem', '20GB')
    kwargs['slurm_cores_per_job'] = kwargs.pop('slurm_cores_per_job', 1)
    kwargs['slurm_python'] = kwargs.pop('slurm_python', None)
    kwargs['slurm_local_directory'] = kwargs.pop('slurm_local_directory', None)
    kwargs['slurm_log_directory'] = kwargs.pop('slurm_log_directory', 'logs')
    return kwargs


if __name__ == '__main__':
    main()
