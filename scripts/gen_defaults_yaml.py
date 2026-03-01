#!/usr/bin/env python
"""Regenerate ``configs/defaults.yaml`` from ``PcleanConfig`` pydantic defaults.

Usage::

    pixi run -e dev gen-defaults
    # or directly:
    python scripts/gen_defaults_yaml.py

This ensures ``defaults.yaml`` never drifts from the pydantic field
definitions in ``config.py``.  Run this after changing any default
value in a sub-config model and commit the result.
"""

from __future__ import annotations

from pathlib import Path

from pclean.config import PcleanConfig

# Resolve output path relative to this script's location
_SCRIPT_DIR = Path(__file__).resolve().parent
_OUT = _SCRIPT_DIR.parent / 'src' / 'pclean' / 'configs' / 'defaults.yaml'


def main() -> None:
    """Write a fresh ``defaults.yaml`` from the pydantic defaults."""
    cfg = PcleanConfig()
    _OUT.parent.mkdir(parents=True, exist_ok=True)

    # Use PcleanConfig.to_yaml() for consistent serialisation, then
    # prepend a header comment.
    header = (
        '# pclean default configuration  (AUTO-GENERATED)\n'
        '#\n'
        '# This file is produced by  scripts/gen_defaults_yaml.py\n'
        '# Do NOT edit manually — change the pydantic defaults in\n'
        '# src/pclean/config.py and re-run:\n'
        '#\n'
        '#   pixi run -e dev gen-defaults\n'
        '#\n'
        '# All values shown here match the built-in defaults.\n'
        '# Copy this file and modify only the fields you need.\n'
        '\n'
    )

    import yaml

    data = cfg.model_dump(mode='python')
    body = yaml.dump(data, default_flow_style=False, sort_keys=False)

    _OUT.write_text(header + body)
    print(f'defaults.yaml written to {_OUT}')


if __name__ == '__main__':
    main()
