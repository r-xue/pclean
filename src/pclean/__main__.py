"""CLI entry point for pclean.

Usage::

    python -m pclean --vis my.ms --imagename out --specmode cube \
        --imsize 512 512 --cell 1arcsec --niter 1000 \
        --parallel --nworkers 8

    # Or with a YAML config file:
    python -m pclean --pconfig pclean_config.yaml --cluster.nworkers 48
    # Submit a SLURM coordinator job:
    python -m pclean submit config.yaml --workdir /scratch/run_01
All tclean parameters are supported as ``--<name> <value>`` flags.
When ``--pconfig`` is given, the YAML file provides the base and any
CLI flags override it.
"""

from __future__ import annotations

import argparse
import json
import logging

from pclean._cli_parser import build_cli_parser


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser from the centralized config-derived builder.
    
    This ensures all defaults match PcleanConfig pydantic models,
    preventing discrepancies between CLI and YAML/Python API entry points.
    """
    return build_cli_parser()


def main(argv=None):
    """Parse CLI arguments, configure logging, and run pclean.

    Args:
        argv: Command-line arguments to parse. Defaults to ``sys.argv[1:]``
            when *None*.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Reconfigure the package-level logger (set up in pclean.__init__)
    # instead of adding a second handler via basicConfig on the root logger.
    pclean_logger = logging.getLogger('pclean')
    pclean_logger.setLevel(getattr(logging, args.log_level))

    # ------------------------------------------------------------------
    # Subcommand: pclean submit
    # ------------------------------------------------------------------
    if args.subcommand == 'submit':
        from pclean.config import PcleanConfig, SubmitConfig
        from pclean.parallel.submit import submit_pclean_slurm

        # Load submit section from the YAML config as base, then overlay
        # any CLI flags that the user explicitly provided.
        yaml_cfg = PcleanConfig.from_yaml(args.submit_config)
        base = yaml_cfg.cluster.submit.model_dump()

        # CLI overrides (only apply non-default values)
        cli_overrides: dict = {}
        if args.pixi_project_dir is not None:
            cli_overrides['pixi_project_dir'] = args.pixi_project_dir
        if args.pixi_env != 'forge':
            cli_overrides['pixi_env'] = args.pixi_env
        if args.coordinator_mem != '8G':
            cli_overrides['coordinator_mem'] = args.coordinator_mem
        if args.coordinator_cpus != 2:
            cli_overrides['coordinator_cpus'] = args.coordinator_cpus
        if args.coordinator_walltime != '24:00:00':
            cli_overrides['coordinator_walltime'] = args.coordinator_walltime
        if args.coordinator_job_name != 'pclean-coordinator':
            cli_overrides['coordinator_job_name'] = args.coordinator_job_name
        if args.log_dir is not None:
            cli_overrides['log_dir'] = args.log_dir
        if args.psrecord is not True:
            cli_overrides['psrecord'] = args.psrecord
        if args.workdir is not None:
            cli_overrides['workdir'] = args.workdir

        base.update(cli_overrides)
        submit_cfg = SubmitConfig(**base)

        job_id = submit_pclean_slurm(
            config=args.submit_config,
            submit_cfg=submit_cfg,
            dry_run=args.dry_run,
        )
        if job_id is not None:
            print(f'Submitted coordinator job: {job_id}')
        return

    config_path = args.pconfig
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
    # Legacy flat-kwargs path (no --pconfig)
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
    vis = kwargs.pop('vis', '')
    result = pclean(vis=vis, **kwargs)
    print(json.dumps(result, indent=2, default=str))


def _cli_to_flat_kwargs(args: argparse.Namespace) -> dict:
    """Convert parsed CLI args to the flat kwargs dict for ``pclean()``."""
    kwargs = vars(args).copy()
    # Remove meta-args
    kwargs.pop('log_level', None)
    kwargs.pop('pconfig', None)
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
    kwargs['slurm_walltime'] = kwargs.pop('slurm_walltime', '24:00:00')
    kwargs['slurm_job_mem'] = kwargs.pop('slurm_job_mem', '20GB')
    kwargs['slurm_cores_per_job'] = kwargs.pop('slurm_cores_per_job', 1)
    kwargs['slurm_python'] = kwargs.pop('slurm_python', None)
    kwargs['slurm_local_directory'] = kwargs.pop('slurm_local_directory', None)
    kwargs['slurm_log_directory'] = kwargs.pop('slurm_log_directory', 'logs')
    # Strip None values so argparse defaults don't override YAML config
    return {k: v for k, v in kwargs.items() if v is not None}


if __name__ == '__main__':
    main()
