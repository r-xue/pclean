"""Tests for pclean.__main__ — CLI entry point."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pclean.__main__ import _build_parser, _cli_to_flat_kwargs, main


# ---------------------------------------------------------------------------
# Parser construction
# ---------------------------------------------------------------------------

class TestBuildParser:
    """Verify _build_parser creates a complete argparse parser."""

    def test_parser_exists(self):
        p = _build_parser()
        assert p.prog == 'pclean'

    def test_basic_args_parse(self):
        p = _build_parser()
        args = p.parse_args(['--vis', 'test.ms', '--niter', '100'])
        assert args.vis == ['test.ms']
        assert args.niter == 100

    def test_config_arg(self):
        p = _build_parser()
        args = p.parse_args(['--config', 'my.yaml'])
        assert args.config == 'my.yaml'

    def test_preset_repeatable(self):
        p = _build_parser()
        args = p.parse_args(['--preset', 'vlass', '--preset', 'custom'])
        assert args.preset == ['vlass', 'custom']

    def test_dump_config_arg(self):
        p = _build_parser()
        args = p.parse_args(['--dump-config', 'out.yaml'])
        assert args.dump_config == 'out.yaml'

    def test_cluster_type_choices(self):
        p = _build_parser()
        args = p.parse_args(['--cluster-type', 'slurm'])
        assert args.cluster_type == 'slurm'

    def test_submit_subcommand(self):
        p = _build_parser()
        args = p.parse_args([
            'submit', 'config.yaml', '--workdir', '/tmp/work',
        ])
        assert args.subcommand == 'submit'
        assert args.submit_config == 'config.yaml'
        assert args.workdir == '/tmp/work'

    def test_submit_all_flags(self):
        p = _build_parser()
        args = p.parse_args([
            'submit', 'config.yaml',
            '--workdir', '/tmp/work',
            '--pixi-project-dir', '/home/pclean',
            '--pixi-env', 'dev',
            '--coordinator-mem', '16G',
            '--coordinator-cpus', '4',
            '--coordinator-walltime', '48:00:00',
            '--coordinator-job-name', 'my-job',
            '--log-dir', '/tmp/logs',
            '--no-psrecord',
            '--dry-run',
        ])
        assert args.pixi_project_dir == '/home/pclean'
        assert args.pixi_env == 'dev'
        assert args.coordinator_mem == '16G'
        assert args.coordinator_cpus == 4
        assert args.coordinator_walltime == '48:00:00'
        assert args.coordinator_job_name == 'my-job'
        assert args.log_dir == '/tmp/logs'
        assert args.psrecord is False
        assert args.dry_run is True

    def test_boolean_optional_action(self):
        p = _build_parser()
        args = p.parse_args(['--python-automask'])
        assert args.python_automask is True
        args2 = p.parse_args(['--no-python-automask'])
        assert args2.python_automask is False

    def test_no_subcommand(self):
        p = _build_parser()
        args = p.parse_args([])
        assert args.subcommand is None


# ---------------------------------------------------------------------------
# _cli_to_flat_kwargs
# ---------------------------------------------------------------------------

class TestCliToFlatKwargs:
    """Verify the CLI-to-flat-kwargs conversion."""

    def test_removes_meta_args(self):
        p = _build_parser()
        args = p.parse_args(['--config', 'file.yaml', '--log-level', 'DEBUG'])
        kw = _cli_to_flat_kwargs(args)
        assert 'log_level' not in kw
        assert 'config' not in kw
        assert 'preset' not in kw
        assert 'dump_config' not in kw

    def test_slurm_keys_mapped(self):
        p = _build_parser()
        args = p.parse_args([
            '--slurm-queue', 'gpu',
            '--slurm-account', 'myaccount',
            '--slurm-job-mem', '32GB',
        ])
        kw = _cli_to_flat_kwargs(args)
        assert kw['slurm_queue'] == 'gpu'
        assert kw['slurm_account'] == 'myaccount'
        assert kw['slurm_job_mem'] == '32GB'

    def test_cluster_type_mapped(self):
        p = _build_parser()
        args = p.parse_args(['--cluster-type', 'slurm'])
        kw = _cli_to_flat_kwargs(args)
        assert kw['cluster_type'] == 'slurm'

    def test_vis_preserved(self):
        p = _build_parser()
        args = p.parse_args(['--vis', 'a.ms', 'b.ms'])
        kw = _cli_to_flat_kwargs(args)
        assert kw['vis'] == ['a.ms', 'b.ms']


# ---------------------------------------------------------------------------
# main() — submit subcommand
# ---------------------------------------------------------------------------

class TestMainSubmit:
    """Test main() with the submit subcommand."""

    def test_submit_dry_run(self, tmp_path, capsys):
        """submit with --dry-run prints script and returns."""
        # Create a minimal YAML config
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text(
            'selection:\n  vis: test.ms\n'
            'cluster:\n  submit:\n    coordinator_mem: 4G\n'
        )
        main([
            'submit', str(cfg_file),
            '--workdir', str(tmp_path / 'work'),
            '--dry-run',
        ])
        captured = capsys.readouterr()
        assert '#!/bin/bash' in captured.out
        assert '#SBATCH --mem=4G' in captured.out

    def test_submit_cli_overrides_yaml(self, tmp_path, capsys):
        """CLI flags override YAML submit config."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text(
            'selection:\n  vis: test.ms\n'
            'cluster:\n  submit:\n    coordinator_mem: 4G\n'
        )
        main([
            'submit', str(cfg_file),
            '--workdir', str(tmp_path / 'work'),
            '--coordinator-mem', '32G',
            '--coordinator-job-name', 'override-job',
            '--dry-run',
        ])
        captured = capsys.readouterr()
        assert '#SBATCH --mem=32G' in captured.out
        assert '#SBATCH --job-name=override-job' in captured.out


# ---------------------------------------------------------------------------
# main() — --config path
# ---------------------------------------------------------------------------

class TestMainConfigPath:
    """Test main() with --config YAML file."""

    def test_config_file(self, tmp_path, capsys):
        """--config loads YAML and calls pclean()."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text(
            'selection:\n  vis: test.ms\n'
            'image:\n  imagename: out\n'
        )
        mock_result = {'imagename': 'out', 'nchan': 1}
        with patch('pclean.pclean.pclean', return_value=mock_result) as mock_pclean:
            main(['--config', str(cfg_file)])
            mock_pclean.assert_called_once()
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert output['imagename'] == 'out'

    def test_dump_config_from_yaml(self, tmp_path, capsys):
        """--config + --dump-config writes merged YAML."""
        cfg_file = tmp_path / 'config.yaml'
        cfg_file.write_text(
            'selection:\n  vis: test.ms\n'
            'image:\n  imagename: myimg\n'
        )
        out_file = tmp_path / 'dumped.yaml'
        main(['--config', str(cfg_file), '--dump-config', str(out_file)])
        assert out_file.exists()
        captured = capsys.readouterr()
        assert 'Config written to' in captured.out


# ---------------------------------------------------------------------------
# main() — legacy flat-kwargs path
# ---------------------------------------------------------------------------

class TestMainLegacy:
    """Test main() with legacy flat kwargs (no --config)."""

    def test_flat_kwargs_calls_pclean(self, tmp_path, capsys):
        """Flat kwargs path calls pclean() with parsed arguments."""
        mock_result = {'imagename': 'out'}
        with patch('pclean.pclean.pclean', return_value=mock_result) as mock_pclean:
            main(['--vis', 'test.ms', '--imagename', 'out', '--niter', '100'])
            mock_pclean.assert_called_once()
            call_kwargs = mock_pclean.call_args
            assert call_kwargs.kwargs['imagename'] == 'out'
            assert call_kwargs.kwargs['niter'] == 100

    def test_dump_config_no_yaml(self, tmp_path, capsys):
        """--dump-config without --config still works."""
        out_file = tmp_path / 'dumped.yaml'
        main([
            '--vis', 'test.ms', '--imagename', 'out',
            '--dump-config', str(out_file),
        ])
        assert out_file.exists()
        captured = capsys.readouterr()
        assert 'Config written to' in captured.out
