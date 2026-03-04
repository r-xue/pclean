"""pclean — Parallel CLEAN imaging with Dask and CASA tools.

Provides a ``pclean()`` function with a tclean-compatible interface and
transparent Dask-based parallelism for cube (channel) and continuum
(visibility-row) imaging.
"""

import logging
import time
from typing import Literal

from pclean.pclean import pclean

try:
    from pclean._version import version as __version__
except ModuleNotFoundError:  # editable install without build
    __version__ = '0.0.0.dev0'

__all__ = ['pclean', 'CustomFormatter']


class CustomFormatter(logging.Formatter):
    """Custom logging formatter that defaults to CASA-style output and UTC time.

    Format: YYYY-MM-DD HH:MM:SS LEVEL LoggerName    Message
    """

    # Enforce UTC time conversion at the class level
    converter = time.gmtime

    def __init__(self, fmt: str | None = None, datefmt: str | None = None, style: Literal['%', '{', '$'] = '%') -> None:
        """Initialize with optional format/datefmt, falling back to defaults.

        Both arguments default to the strings used in :mod:`pclean` if not
        supplied.  The caller can override either or both selections; the
        formatter still enforces UTC timing via the class-level ``converter``
        attribute.
        """
        # Set the default format strings if none are explicitly provided
        if fmt is None:
            fmt = '%(asctime)s %(levelname)-7s %(name)s    %(message)s'
        if datefmt is None:
            datefmt = '%Y-%m-%d %H:%M:%S'
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)


# Configure a default stream handler so that log messages from pclean
# (and its submodules) are visible without extra user setup.
def _configure_default_logging() -> None:
    _log = logging.getLogger(__name__)
    if _log.handlers:
        return  # already configured by the caller; don't override
    _handler = logging.StreamHandler()
    _handler.setFormatter(CustomFormatter())
    _log.addHandler(_handler)
    _log.setLevel(logging.INFO)  # ensure INFO is visible by default


_configure_default_logging()
