"""
pclean — Parallel CLEAN imaging with Dask and CASA tools.

Provides a ``pclean()`` function with a tclean-compatible interface and
transparent Dask-based parallelism for cube (channel) and continuum
(visibility-row) imaging.
"""

from pclean.pclean import pclean  # noqa: F401

__version__ = '0.1.0'
__all__ = ['pclean']
