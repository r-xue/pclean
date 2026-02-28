"""Shared fixtures for pclean tests."""

import sys
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _reset_casatools_cache():
    """Clear the lazy-load cache between tests."""
    import pclean.imaging.serial_imager as si_mod
    import pclean.imaging.deconvolver as dc_mod
    import pclean.imaging.normalizer as nm_mod

    yield
    si_mod._casatools = None
    dc_mod._casatools = None
    nm_mod._casatools = None
