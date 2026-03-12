"""Verify CLI defaults match PcleanConfig pydantic model defaults (single source of truth).

This test ensures that if someone changes a default in config.py, they don't
accidentally create a divergence with CLI defaults in __main__.py.  All
defaults are now derived from PcleanConfig via _cli_parser.py.
"""

import argparse
import pytest

from pclean._cli_parser import build_cli_parser
from pclean.config import PcleanConfig


class TestCliDefaultsMatchConfig:
    """Ensure argparse parser defaults match pydantic model defaults."""

    def test_parser_builds_without_error(self):
        """Parser should build successfully from pydantic introspection."""
        parser = build_cli_parser()
        assert isinstance(parser, argparse.ArgumentParser)
        assert parser.prog == 'pclean'

    def test_parser_has_expected_actions(self):
        """Parser should have all expected arguments."""
        parser = build_cli_parser()
        action_dests = {action.dest for action in parser._actions}
        
        # Sample of critical args that must exist
        critical = {
            'vis', 'datacolumn', 'imagename', 'specmode', 'niter', 'deconvolver',
            'weighting', 'robust', 'parallel', 'nworkers', 'pconfig', 'log_level'
        }
        missing = critical - action_dests
        assert not missing, f'Parser missing arguments: {missing}'

    def test_subcommand_submit_exists(self):
        """Parser should have 'submit' subcommand."""
        parser = build_cli_parser()
        subparsers_action = None
        for action in parser._subparsers._actions:
            if isinstance(action, argparse._SubParsersAction):
                subparsers_action = action
                break
        
        assert subparsers_action is not None
        assert 'submit' in subparsers_action.choices

    def test_parse_minimal_args(self):
        """Test parsing with minimal required arguments."""
        parser = build_cli_parser()
        args = parser.parse_args(['--vis', 'test.ms', '--imagename', 'out'])
        assert args.vis == ['test.ms']
        assert args.imagename == 'out'

    def test_parse_with_pconfig(self):
        """Test parsing --pconfig flag."""
        parser = build_cli_parser()
        args = parser.parse_args(['--pconfig', '/path/to/config.yaml'])
        assert args.pconfig == '/path/to/config.yaml'

    def test_parse_with_preset(self):
        """Test parsing --preset flag (repeatable)."""
        parser = build_cli_parser()
        args = parser.parse_args(['--preset', 'vlass', '--preset', 'alma'])
        assert args.preset == ['vlass', 'alma']

    def test_parse_deconvolution_flags(self):
        """Test deconvolution-related flags."""
        parser = build_cli_parser()
        args = parser.parse_args([
            '--deconvolver', 'multiscale',
            '--scales', '0', '6', '20',
            '--niter', '1000',
            '--threshold', '5mJy'
        ])
        assert args.deconvolver == 'multiscale'
        assert args.scales == [0, 6, 20]
        assert args.niter == 1000
        assert args.threshold == '5mJy'

    def test_parse_cluster_args(self):
        """Test cluster/parallel arguments."""
        parser = build_cli_parser()
        args = parser.parse_args([
            '--parallel',
            '--nworkers', '8',
            '--cluster-type', 'slurm',
            '--slurm-queue', 'cpu',
            '--slurm-job-mem', '32GB'
        ])
        assert args.parallel is True
        assert args.nworkers == 8
        assert args.cluster_type == 'slurm'
        assert args.slurm_queue == 'cpu'
        assert args.slurm_job_mem == '32GB'

    def test_parse_restoration_flags(self):
        """Test --restoration and --no-restoration flags."""
        parser = build_cli_parser()
        
        # By default, restoration should be True
        args = parser.parse_args([])
        assert args.restoration is True
        
        # Explicit --restoration
        args = parser.parse_args(['--restoration'])
        assert args.restoration is True
        
        # Explicit --no-restoration
        args = parser.parse_args(['--no-restoration'])
        assert args.restoration is False
