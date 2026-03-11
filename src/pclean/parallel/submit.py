"""Generate and submit SLURM coordinator jobs for pclean.

The coordinator job runs on a single SLURM allocation and uses
dask-jobqueue to spawn per-channel worker jobs automatically
(Option A architecture — see ``notes/slurm_job_architecture_guide.md``).

Usage from Python::

    from pclean.config import PcleanConfig
    from pclean.parallel.submit import submit_pclean_slurm

    cfg = PcleanConfig.from_yaml('my_config.yaml')
    job_id = submit_pclean_slurm(
        config='my_config.yaml',
        submit_cfg=cfg.cluster.submit,
    )

Or from the CLI::

    pclean submit my_config.yaml
    pclean submit my_config.yaml --workdir /scratch/run_01  # override
"""

from __future__ import annotations

import logging
import re
import subprocess
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import os

    from pclean.config import SubmitConfig

log = logging.getLogger(__name__)

_SBATCH_TEMPLATE = textwrap.dedent("""\
    #!/bin/bash
    #SBATCH --job-name={coordinator_job_name}
    #SBATCH --output={log_dir}/{coordinator_job_name}-%j.out
    #SBATCH --error={log_dir}/{coordinator_job_name}-%j.err
    #SBATCH --ntasks=1
    #SBATCH --cpus-per-task={coordinator_cpus}
    #SBATCH --mem={coordinator_mem}
    #SBATCH --time={coordinator_walltime}
    {extra_sbatch_lines}

    # ---- Environment setup ----
    eval "$(pixi shell-hook -e {pixi_env} --manifest-path {manifest_path})"

    # ---- Working directory ----
    mkdir -p "{workdir}" && cd "{workdir}"
    mkdir -p "{log_dir}"

    # ---- Run ----
    {run_command}
""")


def generate_sbatch_script(
    config: str | os.PathLike,
    workdir: str | os.PathLike | None = None,
    submit_cfg: SubmitConfig | None = None,
) -> str:
    """Generate an sbatch script string for a pclean coordinator job.

    Args:
        config: Path to a pclean YAML config file.
        workdir: Working directory for the imaging run (output images go here).
            Falls back to ``submit_cfg.workdir`` if not given.
        submit_cfg: Coordinator job parameters.  When *None*, a default
            :class:`~pclean.config.SubmitConfig` is used.

    Returns:
        The sbatch script as a string.

    Raises:
        ValueError: If *workdir* is not supplied and ``submit_cfg.workdir``
            is also ``None``.
    """
    if submit_cfg is None:
        from pclean.config import SubmitConfig
        submit_cfg = SubmitConfig()

    config = Path(config).resolve()

    # Resolve workdir: explicit arg > submit_cfg.workdir
    resolved_workdir = workdir if workdir is not None else submit_cfg.workdir
    if resolved_workdir is None:
        raise ValueError(
            'workdir must be provided either as an argument or '
            'in submit_cfg.workdir'
        )
    workdir = Path(resolved_workdir).resolve()

    pixi_project_dir = (
        Path(submit_cfg.pixi_project_dir).resolve()
        if submit_cfg.pixi_project_dir is not None
        else config.parent
    )
    manifest_path = pixi_project_dir / 'pyproject.toml'

    log_dir = (
        Path(submit_cfg.log_dir).resolve()
        if submit_cfg.log_dir is not None
        else pixi_project_dir / 'logs'
    )

    # Build the core command: python -m pclean --config <path>
    pclean_cmd = f'python -m pclean --config {config}'
    log_base = log_dir / config.stem

    if submit_cfg.psrecord:
        run_command = (
            f'psrecord \\\n'
            f'    --log "{log_base}.rec" \\\n'
            f'    --include-children --include-io --include-cache --use-timestamp \\\n'
            f'    --include-dir "{workdir}" \\\n'
            f'    "{pclean_cmd} > {log_base}.log 2>&1"'
        )
    else:
        run_command = f'{pclean_cmd} > {log_base}.log 2>&1'

    extra_sbatch_lines = '\n'.join(
        f'#SBATCH {line}' for line in (submit_cfg.extra_sbatch or [])
    )

    return _SBATCH_TEMPLATE.format(
        coordinator_job_name=submit_cfg.coordinator_job_name,
        coordinator_cpus=submit_cfg.coordinator_cpus,
        coordinator_mem=submit_cfg.coordinator_mem,
        coordinator_walltime=submit_cfg.coordinator_walltime,
        extra_sbatch_lines=extra_sbatch_lines,
        pixi_env=submit_cfg.pixi_env,
        manifest_path=manifest_path,
        workdir=workdir,
        log_dir=log_dir,
        run_command=run_command,
    )


def submit_pclean_slurm(
    config: str | os.PathLike,
    workdir: str | os.PathLike | None = None,
    submit_cfg: SubmitConfig | None = None,
    dry_run: bool = False,
) -> str | None:
    """Generate and submit a SLURM coordinator job for pclean.

    This creates the coordinator sbatch script and submits it via
    ``sbatch``.  The coordinator job activates the pixi environment,
    runs ``python -m pclean --config <config>``, and dask-jobqueue
    submits the worker jobs automatically.

    Args:
        config: Path to a pclean YAML config file.
        workdir: Working directory for the imaging run.  Falls back to
            ``submit_cfg.workdir`` if not given.
        submit_cfg: Coordinator job parameters.  When *None*, a default
            :class:`~pclean.config.SubmitConfig` is used.
        dry_run: If ``True``, print the script and return without submitting.

    Returns:
        The SLURM job ID string, or ``None`` if *dry_run* is ``True``.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If *workdir* is not supplied and ``submit_cfg.workdir``
            is also ``None``.
        RuntimeError: If ``sbatch`` fails.
    """
    config = Path(config).resolve()
    if not config.exists():
        raise FileNotFoundError(f'Config file not found: {config}')

    script = generate_sbatch_script(
        config=config,
        workdir=workdir,
        submit_cfg=submit_cfg,
    )

    if dry_run:
        print(script)
        return None

    # Resolve workdir for writing the script (generate_sbatch_script
    # already validated that a workdir is available).
    resolved_workdir = workdir if workdir is not None else (
        submit_cfg.workdir if submit_cfg is not None else None
    )
    workdir = Path(resolved_workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    script_path = workdir / 'submit.sh'
    script_path.write_text(script)
    script_path.chmod(0o755)
    log.info('Wrote sbatch script to %s', script_path)

    result = subprocess.run(
        ['sbatch', str(script_path)],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f'sbatch failed (exit {result.returncode}):\n{result.stderr.strip()}'
        )

    # Parse "Submitted batch job 12345"
    match = re.search(r'Submitted batch job (\d+)', result.stdout)
    job_id = match.group(1) if match else result.stdout.strip()
    log.info('Submitted coordinator job %s', job_id)
    return job_id
