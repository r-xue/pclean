"""pclean — Parallel CLEAN imaging with Dask and CASA tools.

Provides a ``pclean()`` function with a tclean-compatible interface and
transparent Dask-based parallelism for cube (channel) and continuum
(visibility-row) imaging.
"""

import logging

from pclean.pclean import pclean

try:
    from pclean._version import version as __version__
except ModuleNotFoundError:  # editable install without build
    __version__ = '0.0.0.dev0'

__all__ = ['pclean']

# Configure a default stream handler so that log messages from pclean
# (and its submodules) are visible without extra user setup.
_log = logging.getLogger(__name__)
if not _log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s [%(name)s] %(message)s'))
    _log.addHandler(_handler)
    _log.setLevel(logging.INFO)
