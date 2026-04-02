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
            from pclean.config import PcleanConfig

            params = PcleanConfig.from_flat_kwargs(
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
            from pclean.config import PcleanConfig

            params = PcleanConfig.from_flat_kwargs(
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


class TestConvergenceOrder:
    """The correct auto-multithresh sequence is:

    1. initminorcycle()  — compute image statistics
    2. setupmask()       — create/update mask from those stats
    3. initminorcycle()  — recompute stats with the new mask
    4. cleanComplete()   — evaluate convergence

    Step 2 before step 1 triggers "Initminor Cycle has not been called yet".
    Omitting step 2 causes the v1 bug (empty mask → peak=0 → premature stop).
    """

    def test_setupmask_called_before_first_clean_complete(self, mock_casatools):
        """initminorcycle → setupmask → cleanComplete on the first iteration."""
        call_order: list[str] = []

        mock_casatools.synthesisdeconvolver.return_value.initminorcycle.side_effect = (
            lambda: call_order.append('initminorcycle') or {}
        )
        mock_casatools.synthesisdeconvolver.return_value.setupmask.side_effect = (
            lambda: call_order.append('setupmask')
        )

        def _clean_complete(**kw):
            if 'reachedMajorLimit' in kw:
                call_order.append('cleanComplete')
            return True

        mock_casatools.iterbotsink.return_value.cleanComplete.side_effect = _clean_complete

        with patch.dict('sys.modules', {'casatools': mock_casatools}):
            import pclean.imaging.serial_imager as mod

            mod._casatools = None

            from pclean.imaging.serial_imager import SerialImager
            from pclean.config import PcleanConfig

            params = PcleanConfig.from_flat_kwargs(
                vis='test.ms',
                imagename='conv_test',
                niter=100,
                specmode='mfs',
            )
            SerialImager(params).run()

        assert 'initminorcycle' in call_order, 'initminorcycle() was never called'
        assert 'setupmask' in call_order, 'setupmask() was never called'
        assert 'cleanComplete' in call_order, 'cleanComplete() was never called'

        first_init = call_order.index('initminorcycle')
        first_setupmask = call_order.index('setupmask')
        first_clean_complete = call_order.index('cleanComplete')

        assert first_init < first_setupmask, (
            f'initminorcycle (pos {first_init}) must precede '
            f'setupmask (pos {first_setupmask}); order: {call_order}'
        )
        assert first_setupmask < first_clean_complete, (
            f'setupmask (pos {first_setupmask}) must precede '
            f'cleanComplete (pos {first_clean_complete}); order: {call_order}'
        )

    def test_niter_zero_skips_setupmask(self, mock_casatools):
        """When niter=0 the deconvolution block is skipped entirely."""
        call_order: list[str] = []
        mock_casatools.synthesisdeconvolver.return_value.setupmask.side_effect = (
            lambda: call_order.append('setupmask')
        )

        with patch.dict('sys.modules', {'casatools': mock_casatools}):
            import pclean.imaging.serial_imager as mod

            mod._casatools = None

            from pclean.imaging.serial_imager import SerialImager
            from pclean.config import PcleanConfig

            params = PcleanConfig.from_flat_kwargs(
                vis='test.ms',
                imagename='niter0_test',
                niter=0,
                specmode='mfs',
            )
            SerialImager(params).run()

        assert 'setupmask' not in call_order, (
            'setupmask() should not be called when niter=0'
        )

    def test_niter_zero_with_restoration(self, mock_casatools):
        """restoration=True must still call restore() even when niter=0."""
        call_order: list[str] = []
        mock_casatools.synthesisdeconvolver.return_value.restore.side_effect = (
            lambda: call_order.append('restore')
        )
        mock_casatools.synthesisdeconvolver.return_value.setupmask.side_effect = (
            lambda: call_order.append('setupmask')
        )

        with patch.dict('sys.modules', {'casatools': mock_casatools}):
            import pclean.imaging.serial_imager as mod

            mod._casatools = None

            from pclean.imaging.serial_imager import SerialImager
            from pclean.config import PcleanConfig

            params = PcleanConfig.from_flat_kwargs(
                vis='test.ms',
                imagename='niter0_restore_test',
                niter=0,
                restoration=True,
                specmode='mfs',
            )
            SerialImager(params).run()

        assert 'restore' in call_order, (
            'restore() must be called when restoration=True even with niter=0'
        )
        assert 'setupmask' not in call_order, (
            'setupmask() should not be called when niter=0'
        )
