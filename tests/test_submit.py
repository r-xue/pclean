"""Tests for pclean.parallel.submit — sbatch script generation and submission."""

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pclean.config import SubmitConfig
from pclean.parallel.submit import generate_sbatch_script, submit_pclean_slurm


# ---------------------------------------------------------------------------
# generate_sbatch_script
# ---------------------------------------------------------------------------

class TestGenerateSbatchScript:
    """Test sbatch script generation."""

    def test_default_config(self, tmp_path):
        """Default SubmitConfig produces a valid script."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        script = generate_sbatch_script(cfg_file, tmp_path / 'work')
        assert '#!/bin/bash' in script
        assert '#SBATCH --job-name=pclean-coordinator' in script
        assert '#SBATCH --mem=8G' in script
        assert '#SBATCH --cpus-per-task=2' in script
        assert '#SBATCH --time=24:00:00' in script
        assert 'pixi shell-hook -e forge' in script
        assert 'python -m pclean --config' in script

    def test_custom_submit_config(self, tmp_path):
        """Custom SubmitConfig values appear in the script."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        submit_cfg = SubmitConfig(
            coordinator_mem='16G',
            coordinator_cpus=4,
            coordinator_walltime='48:00:00',
            coordinator_job_name='my-job',
            pixi_env='dev',
        )
        script = generate_sbatch_script(cfg_file, tmp_path / 'work', submit_cfg)
        assert '#SBATCH --mem=16G' in script
        assert '#SBATCH --cpus-per-task=4' in script
        assert '#SBATCH --time=48:00:00' in script
        assert '#SBATCH --job-name=my-job' in script
        assert 'pixi shell-hook -e dev' in script

    def test_pixi_project_dir(self, tmp_path):
        """Explicit pixi_project_dir sets the manifest path."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        project_dir = tmp_path / 'myproject'
        project_dir.mkdir()
        submit_cfg = SubmitConfig(pixi_project_dir=str(project_dir))
        script = generate_sbatch_script(cfg_file, tmp_path / 'work', submit_cfg)
        assert f'--manifest-path {project_dir}/pyproject.toml' in script

    def test_pixi_project_dir_defaults_to_config_parent(self, tmp_path):
        """When pixi_project_dir is None, defaults to config file's parent."""
        subdir = tmp_path / 'configs'
        subdir.mkdir()
        cfg_file = subdir / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        script = generate_sbatch_script(cfg_file, tmp_path / 'work')
        assert f'--manifest-path {subdir}/pyproject.toml' in script

    def test_log_dir_explicit(self, tmp_path):
        """Explicit log_dir is used in script."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        log_dir = tmp_path / 'my_logs'
        submit_cfg = SubmitConfig(log_dir=str(log_dir))
        script = generate_sbatch_script(cfg_file, tmp_path / 'work', submit_cfg)
        assert str(log_dir) in script

    def test_log_dir_default(self, tmp_path):
        """Default log_dir is <pixi_project_dir>/logs."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        script = generate_sbatch_script(cfg_file, tmp_path / 'work')
        # config.parent/logs
        expected_log_dir = str(cfg_file.parent.resolve() / 'logs')
        assert expected_log_dir in script

    def test_psrecord_enabled(self, tmp_path):
        """psrecord=True wraps the command."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        submit_cfg = SubmitConfig(psrecord=True)
        script = generate_sbatch_script(cfg_file, tmp_path / 'work', submit_cfg)
        assert 'psrecord' in script
        assert '--include-children' in script

    def test_psrecord_disabled(self, tmp_path):
        """psrecord=False runs command directly."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        submit_cfg = SubmitConfig(psrecord=False)
        script = generate_sbatch_script(cfg_file, tmp_path / 'work', submit_cfg)
        # The psrecord command wrapper should not appear
        assert 'psrecord \\\n' not in script
        assert 'python -m pclean --config' in script

    def test_extra_sbatch_directives(self, tmp_path):
        """Extra sbatch directives appear in the script."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        submit_cfg = SubmitConfig(
            extra_sbatch=['--partition=gpu', '--gres=gpu:1']
        )
        script = generate_sbatch_script(cfg_file, tmp_path / 'work', submit_cfg)
        assert '#SBATCH --partition=gpu' in script
        assert '#SBATCH --gres=gpu:1' in script

    def test_workdir_in_script(self, tmp_path):
        """Working directory is mkdir'd and cd'd in the script."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        workdir = tmp_path / 'work' / 'deep'
        script = generate_sbatch_script(cfg_file, workdir)
        assert f'mkdir -p "{workdir.resolve()}"' in script

    def test_workdir_from_submit_config(self, tmp_path):
        """workdir falls back to submit_cfg.workdir when arg is None."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        cfg_workdir = tmp_path / 'cfg_work'
        submit_cfg = SubmitConfig(workdir=str(cfg_workdir))
        script = generate_sbatch_script(cfg_file, submit_cfg=submit_cfg)
        assert str(cfg_workdir.resolve()) in script

    def test_explicit_workdir_overrides_config(self, tmp_path):
        """Explicit workdir arg takes precedence over submit_cfg.workdir."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        cfg_workdir = tmp_path / 'cfg_work'
        explicit_workdir = tmp_path / 'explicit_work'
        submit_cfg = SubmitConfig(workdir=str(cfg_workdir))
        script = generate_sbatch_script(cfg_file, explicit_workdir, submit_cfg)
        assert str(explicit_workdir.resolve()) in script
        assert str(cfg_workdir) not in script

    def test_no_workdir_raises(self, tmp_path):
        """ValueError when neither workdir arg nor submit_cfg.workdir is set."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        with pytest.raises(ValueError, match='workdir must be provided'):
            generate_sbatch_script(cfg_file)


# ---------------------------------------------------------------------------
# submit_pclean_slurm
# ---------------------------------------------------------------------------

class TestSubmitPcleanSlurm:
    """Test the submission function."""

    def test_dry_run_prints_script(self, tmp_path, capsys):
        """dry_run=True prints script and returns None."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        result = submit_pclean_slurm(
            config=cfg_file,
            workdir=tmp_path / 'work',
            dry_run=True,
        )
        assert result is None
        captured = capsys.readouterr()
        assert '#!/bin/bash' in captured.out
        assert '#SBATCH' in captured.out

    def test_config_not_found_raises(self, tmp_path):
        """Missing config file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match='Config file not found'):
            submit_pclean_slurm(
                config=tmp_path / 'nonexistent.yaml',
                workdir=tmp_path / 'work',
            )

    def test_sbatch_success(self, tmp_path):
        """Successful sbatch returns the job ID."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = 'Submitted batch job 12345678\n'
        mock_result.stderr = ''

        with patch('pclean.parallel.submit.subprocess.run', return_value=mock_result):
            job_id = submit_pclean_slurm(
                config=cfg_file,
                workdir=tmp_path / 'work',
            )
        assert job_id == '12345678'
        # Verify submit.sh was written
        assert (tmp_path / 'work' / 'submit.sh').exists()

    def test_sbatch_failure_raises(self, tmp_path):
        """sbatch failure raises RuntimeError."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ''
        mock_result.stderr = 'sbatch: error: Batch job submission failed'

        with patch('pclean.parallel.submit.subprocess.run', return_value=mock_result):
            with pytest.raises(RuntimeError, match='sbatch failed'):
                submit_pclean_slurm(
                    config=cfg_file,
                    workdir=tmp_path / 'work',
                )

    def test_submit_script_is_executable(self, tmp_path):
        """The written submit.sh has executable permissions."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = 'Submitted batch job 99999\n'
        mock_result.stderr = ''

        with patch('pclean.parallel.submit.subprocess.run', return_value=mock_result):
            submit_pclean_slurm(
                config=cfg_file,
                workdir=tmp_path / 'work',
            )
        script_path = tmp_path / 'work' / 'submit.sh'
        assert script_path.stat().st_mode & 0o755

    def test_sbatch_unparseable_stdout(self, tmp_path):
        """If sbatch stdout doesn't match expected pattern, return raw stdout."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = 'Job submitted successfully\n'
        mock_result.stderr = ''

        with patch('pclean.parallel.submit.subprocess.run', return_value=mock_result):
            job_id = submit_pclean_slurm(
                config=cfg_file,
                workdir=tmp_path / 'work',
            )
        assert job_id == 'Job submitted successfully'

    def test_dry_run_with_custom_config(self, tmp_path, capsys):
        """dry_run with custom SubmitConfig includes custom values."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text('selection:\n  vis: test.ms\n')
        submit_cfg = SubmitConfig(
            coordinator_mem='32G',
            coordinator_job_name='big-run',
            psrecord=False,
        )
        result = submit_pclean_slurm(
            config=cfg_file,
            workdir=tmp_path / 'work',
            submit_cfg=submit_cfg,
            dry_run=True,
        )
        assert result is None
        captured = capsys.readouterr()
        assert '#SBATCH --mem=32G' in captured.out
        assert '#SBATCH --job-name=big-run' in captured.out
        assert 'psrecord' not in captured.out
