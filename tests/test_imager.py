"""
Tests for pclean dispatch logic (mocked casatools).
"""

import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_casatools():
    """Provide a mock casatools module so tests run without CASA."""
    mock_ct = MagicMock()
    # synthesisimager mock
    mock_si = MagicMock()
    mock_ct.synthesisimager.return_value = mock_si
    # synthesisdeconvolver mock
    mock_sd = MagicMock()
    mock_sd.initminorcycle.return_value = {'peakresidual': 0.001}
    mock_sd.executeminorcycle.return_value = {'iterdone': 10}
    mock_ct.synthesisdeconvolver.return_value = mock_sd
    # synthesisnormalizer mock
    mock_sn = MagicMock()
    mock_ct.synthesisnormalizer.return_value = mock_sn
    # iterbotsink mock
    mock_ib = MagicMock()
    mock_ib.cleanComplete.return_value = True
    mock_ib.getminorcyclecontrols.return_value = {}
    mock_ct.iterbotsink.return_value = mock_ib
    return mock_ct


class TestSerialImager:
    """SerialImager lifecycle with mocked casatools."""

    def test_setup_teardown(self, mock_casatools):
        with patch.dict('sys.modules', {'casatools': mock_casatools}):
            # Reset the lazy cache
            import pclean.imaging.serial_imager as mod

            mod._casatools = None

            from pclean.imaging.serial_imager import SerialImager
            from pclean.params import PcleanParams

            params = PcleanParams(
                vis='test.ms',
                imagename='test_img',
                niter=100,
                specmode='mfs',
            )
            imager = SerialImager(params)
            imager.setup()

            assert imager.si_tool is not None
            assert len(imager.sn_tools) == 1

            imager.teardown()
            assert imager.si_tool is None

    def test_run_completes(self, mock_casatools):
        with patch.dict('sys.modules', {'casatools': mock_casatools}):
            import pclean.imaging.serial_imager as mod

            mod._casatools = None

            from pclean.imaging.serial_imager import SerialImager
            from pclean.params import PcleanParams

            params = PcleanParams(
                vis='test.ms',
                imagename='test_run',
                niter=100,
                specmode='mfs',
            )
            result = SerialImager(params).run()
            assert 'imagename' in result
            assert result['imagename'] == 'test_run'


class TestDispatch:
    """pclean() dispatch based on specmode and parallel flag."""

    def test_serial_dispatch(self, mock_casatools):
        with patch.dict('sys.modules', {'casatools': mock_casatools}):
            import pclean.imaging.serial_imager as mod

            mod._casatools = None

            from pclean.pclean import pclean as pclean_fn

            result = pclean_fn(
                vis='test.ms',
                imagename='serial_test',
                niter=0,
                specmode='mfs',
                parallel=False,
            )
            assert result['imagename'] == 'serial_test'
