"""
Tests for pclean.utils.image_concat — concat_images() and concat_subcubes().

All tests mock casatools so no CASA installation is needed.
"""

from __future__ import annotations

import os
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_ic_cache():
    """Clear the lazy casatools cache between tests."""
    import pclean.utils.image_concat as mod

    mod._casatools = None
    yield
    mod._casatools = None


@pytest.fixture
def mock_ct():
    """Return a mock casatools module with an image() tool."""
    ct = MagicMock()
    ia = MagicMock()
    ct.image.return_value = ia
    return ct, ia


# ---------------------------------------------------------------------------
# concat_images — mode parameter forwarding
# ---------------------------------------------------------------------------


class TestConcatImagesMode:
    """concat_images() passes the correct mode to ia.imageconcat()."""

    def _run(self, mock_ct_pair, mode):
        ct, ia = mock_ct_pair
        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_images

            concat_images('out.image', ['a.image', 'b.image'], mode=mode)

        ia.imageconcat.assert_called_once()
        kwargs = ia.imageconcat.call_args.kwargs
        assert kwargs['mode'] == mode
        assert kwargs['outfile'] == 'out.image'
        assert kwargs['infiles'] == ['a.image', 'b.image']
        assert kwargs['tempclose'] is False
        assert kwargs['reorder'] is False
        # ia.done() must always be called
        ia.done.assert_called_once()

    def test_paged_mode(self, mock_ct):
        self._run(mock_ct, 'paged')

    def test_nomovevirtual_mode(self, mock_ct):
        self._run(mock_ct, 'nomovevirtual')

    def test_movevirtual_mode(self, mock_ct):
        self._run(mock_ct, 'movevirtual')

    def test_default_mode_is_paged(self, mock_ct):
        ct, ia = mock_ct
        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_images

            concat_images('out.image', ['a.image'])

        kwargs = ia.imageconcat.call_args.kwargs
        assert kwargs['mode'] == 'paged'


class TestConcatImagesTableLock:
    """All modes hold _tablelock to prevent concurrent imageconcat() segfaults."""

    def _check_lock_held(self, mode: str, expect_lock: bool):
        ct, ia = MagicMock(), MagicMock()
        ct.image.return_value = ia

        lock_was_held: list[bool] = []

        original_imageconcat = ia.imageconcat

        import pclean.utils.image_concat as mod

        mod._casatools = None

        def _spy_imageconcat(**kw):
            # Check whether _tablelock is currently acquired
            acquired = not mod._tablelock.acquire(blocking=False)
            if not acquired:
                mod._tablelock.release()
            lock_was_held.append(acquired)

        ia.imageconcat.side_effect = _spy_imageconcat

        with patch.dict('sys.modules', {'casatools': ct}):
            mod._casatools = None
            from pclean.utils.image_concat import concat_images

            concat_images('out', ['a'], mode=mode)

        assert bool(lock_was_held[0]) == expect_lock, (
            f'mode={mode!r}: expected lock_held={expect_lock}, got {lock_was_held[0]}'
        )

    def test_paged_holds_lock(self):
        # ia.imageconcat() is not thread-safe in any mode; paged must also
        # serialise through _tablelock to prevent CI segfaults.
        self._check_lock_held('paged', expect_lock=True)

    def test_nomovevirtual_holds_lock(self):
        self._check_lock_held('nomovevirtual', expect_lock=True)

    def test_movevirtual_holds_lock(self):
        self._check_lock_held('movevirtual', expect_lock=True)

    def test_lock_released_after_paged(self, mock_ct):
        """_tablelock must not be held after concat_images() returns."""
        import pclean.utils.image_concat as mod

        ct, ia = mock_ct
        with patch.dict('sys.modules', {'casatools': ct}):
            mod._casatools = None
            from pclean.utils.image_concat import concat_images

            concat_images('out', ['a'], mode='paged')

        # Should be acquirable again immediately
        acquired = mod._tablelock.acquire(blocking=False)
        assert acquired, '_tablelock was not released after paged concat'
        mod._tablelock.release()

    def test_lock_released_after_virtual(self, mock_ct):
        import pclean.utils.image_concat as mod

        ct, ia = mock_ct
        with patch.dict('sys.modules', {'casatools': ct}):
            mod._casatools = None
            from pclean.utils.image_concat import concat_images

            concat_images('out', ['a'], mode='nomovevirtual')

        acquired = mod._tablelock.acquire(blocking=False)
        assert acquired, '_tablelock was not released after virtual concat'
        mod._tablelock.release()

    def test_lock_released_on_exception(self, mock_ct):
        """_tablelock must be released even when ia.imageconcat raises."""
        import pclean.utils.image_concat as mod

        ct, ia = mock_ct
        ia.imageconcat.side_effect = RuntimeError('CASA error')
        with patch.dict('sys.modules', {'casatools': ct}):
            mod._casatools = None
            from pclean.utils.image_concat import concat_images

            with pytest.raises(RuntimeError):
                concat_images('out', ['a'], mode='nomovevirtual')

        acquired = mod._tablelock.acquire(blocking=False)
        assert acquired, '_tablelock leaked after exception in virtual concat'
        mod._tablelock.release()


# ---------------------------------------------------------------------------
# concat_subcubes — file discovery
# ---------------------------------------------------------------------------


class TestConcatSubcubesDiscovery:
    """concat_subcubes() only concatenates extensions that exist on disk."""

    def test_discovers_existing_dirs(self, tmp_path, mock_ct):
        ct, ia = mock_ct
        base = str(tmp_path / 'cube')
        # Create only .image and .psf subcubes for 2 parts
        for i in range(2):
            os.makedirs(f'{base}.subcube.{i}.image')
            os.makedirs(f'{base}.subcube.{i}.psf')

        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_subcubes

            concat_subcubes(base, nparts=2, extensions=['.image', '.psf', '.residual'],
                            _pool_cls=ThreadPoolExecutor)

        # imageconcat called twice (image + psf), NOT for .residual
        assert ia.imageconcat.call_count == 2
        called_outfiles = {c.kwargs['outfile'] for c in ia.imageconcat.call_args_list}
        assert called_outfiles == {f'{base}.image', f'{base}.psf'}

    def test_no_files_warns_and_returns(self, tmp_path, mock_ct, caplog):
        ct, ia = mock_ct
        base = str(tmp_path / 'empty')
        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_subcubes

            import logging

            with caplog.at_level(logging.WARNING, logger='pclean.utils.image_concat'):
                concat_subcubes(base, nparts=3, extensions=['.image'])

        ia.imageconcat.assert_not_called()
        assert 'No subcube files found' in caplog.text

    def test_infiles_ordering(self, tmp_path, mock_ct):
        """Input file list must be sorted by subcube index."""
        ct, ia = mock_ct
        base = str(tmp_path / 'ordered')
        nparts = 5
        for i in range(nparts):
            os.makedirs(f'{base}.subcube.{i}.image')

        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_subcubes

            concat_subcubes(base, nparts=nparts, extensions=['.image'],
                            _pool_cls=ThreadPoolExecutor)

        infiles = ia.imageconcat.call_args.kwargs['infiles']
        expected = [f'{base}.subcube.{i}.image' for i in range(nparts)]
        assert infiles == expected


# ---------------------------------------------------------------------------
# concat_subcubes — mode forwarding
# ---------------------------------------------------------------------------


class TestConcatSubcubesMode:
    def _run_mode(self, tmp_path, mock_ct, mode):
        ct, ia = mock_ct
        base = str(tmp_path / 'cube')
        os.makedirs(f'{base}.subcube.0.image')
        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_subcubes

            concat_subcubes(base, nparts=1, extensions=['.image'], mode=mode,
                            _pool_cls=ThreadPoolExecutor)
        return ia.imageconcat.call_args.kwargs['mode']

    def test_paged_mode_forwarded(self, tmp_path, mock_ct):
        assert self._run_mode(tmp_path, mock_ct, 'paged') == 'paged'

    def test_nomovevirtual_mode_forwarded(self, tmp_path, mock_ct):
        assert self._run_mode(tmp_path, mock_ct, 'nomovevirtual') == 'nomovevirtual'

    def test_movevirtual_mode_forwarded(self, tmp_path, mock_ct):
        assert self._run_mode(tmp_path, mock_ct, 'movevirtual') == 'movevirtual'

    def test_default_mode_is_paged(self, tmp_path, mock_ct):
        """concat_subcubes() mode defaults to 'paged'."""
        ct, ia = mock_ct
        base = str(tmp_path / 'cube')
        os.makedirs(f'{base}.subcube.0.image')
        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_subcubes

            concat_subcubes(base, nparts=1, extensions=['.image'],
                            _pool_cls=ThreadPoolExecutor)
        assert ia.imageconcat.call_args.kwargs['mode'] == 'paged'# ---------------------------------------------------------------------------
# concat_subcubes — deprecated virtual= kwarg
# ---------------------------------------------------------------------------


class TestConcatSubcubesDeprecatedVirtual:
    def _run_virtual(self, tmp_path, mock_ct, virtual_val):
        ct, ia = mock_ct
        base = str(tmp_path / 'cube')
        os.makedirs(f'{base}.subcube.0.image')
        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_subcubes

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter('always')
                concat_subcubes(base, nparts=1, extensions=['.image'], virtual=virtual_val,
                                _pool_cls=ThreadPoolExecutor)
        return ia.imageconcat.call_args.kwargs['mode'], w

    def test_virtual_true_emits_deprecation_warning(self, tmp_path, mock_ct):
        _, w = self._run_virtual(tmp_path, mock_ct, True)
        assert any(issubclass(x.category, DeprecationWarning) for x in w)

    def test_virtual_false_emits_deprecation_warning(self, tmp_path, mock_ct):
        _, w = self._run_virtual(tmp_path, mock_ct, False)
        assert any(issubclass(x.category, DeprecationWarning) for x in w)

    def test_virtual_true_maps_to_nomovevirtual(self, tmp_path, mock_ct):
        mode, _ = self._run_virtual(tmp_path, mock_ct, True)
        assert mode == 'nomovevirtual'

    def test_virtual_false_maps_to_paged(self, tmp_path, mock_ct):
        mode, _ = self._run_virtual(tmp_path, mock_ct, False)
        assert mode == 'paged'

    def test_explicit_mode_wins_over_virtual(self, tmp_path, mock_ct):
        """Explicit mode= must not be overridden by virtual=False."""
        ct, ia = mock_ct
        base = str(tmp_path / 'cube')
        os.makedirs(f'{base}.subcube.0.image')
        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_subcubes

            with warnings.catch_warnings(record=True):
                warnings.simplefilter('always')
                concat_subcubes(
                    base,
                    nparts=1,
                    extensions=['.image'],
                    mode='nomovevirtual',
                    virtual=False,  # should not override explicit mode
                )
        assert ia.imageconcat.call_args.kwargs['mode'] == 'nomovevirtual'


# ---------------------------------------------------------------------------
# concat_subcubes — parallel execution and error handling
# ---------------------------------------------------------------------------


class TestConcatSubcubesParallel:
    def test_multiple_extensions_all_called(self, tmp_path, mock_ct):
        """All extensions with existing subcubes are concatenated."""
        ct, ia = mock_ct
        base = str(tmp_path / 'cube')
        exts = ['.image', '.residual', '.psf']
        for ext in exts:
            os.makedirs(f'{base}.subcube.0{ext}')
            os.makedirs(f'{base}.subcube.1{ext}')

        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_subcubes

            concat_subcubes(base, nparts=2, extensions=exts, max_workers=3,
                            _pool_cls=ThreadPoolExecutor)

        assert ia.imageconcat.call_count == 3

    def test_failed_extension_does_not_abort_others(self, tmp_path, caplog):
        """A failing extension is logged; other extensions still complete."""
        ct = MagicMock()
        call_count = 0
        call_lock = threading.Lock()

        def _side_effect(**kw):
            nonlocal call_count
            with call_lock:
                call_count += 1
                if '.image' in kw['outfile']:
                    raise RuntimeError('simulated failure')

        ia = MagicMock()
        ia.imageconcat.side_effect = _side_effect
        ct.image.return_value = ia

        base = str(tmp_path / 'cube')
        exts = ['.image', '.residual']
        for ext in exts:
            os.makedirs(f'{base}.subcube.0{ext}')

        with patch.dict('sys.modules', {'casatools': ct}):
            import pclean.utils.image_concat as mod

            mod._casatools = None
            from pclean.utils.image_concat import concat_subcubes

            import logging

            with caplog.at_level(logging.WARNING, logger='pclean.utils.image_concat'):
                concat_subcubes(base, nparts=1, extensions=exts, max_workers=2,
                                _pool_cls=ThreadPoolExecutor)

        # Both were attempted
        assert call_count == 2
        assert 'Failed' in caplog.text

    def test_max_workers_capped_at_work_count(self, tmp_path, mock_ct):
        """max_workers > number of extensions uses only as many processes as needed."""
        ct, ia = mock_ct
        base = str(tmp_path / 'cube')
        os.makedirs(f'{base}.subcube.0.image')  # only 1 extension

        submitted: list[int] = []
        original = __import__('concurrent.futures', fromlist=['ProcessPoolExecutor']).ProcessPoolExecutor

        class _CountingPool:
            def __init__(self, max_workers=None, **kw):
                submitted.append(max_workers)
                # Use ThreadPoolExecutor underneath so mocks work
                self._pool = ThreadPoolExecutor(max_workers=max_workers)

            def __enter__(self):
                self._pool.__enter__()
                return self._pool

            def __exit__(self, *a):
                return self._pool.__exit__(*a)

        with patch('pclean.utils.image_concat.ProcessPoolExecutor', _CountingPool):
            with patch.dict('sys.modules', {'casatools': ct}):
                import pclean.utils.image_concat as mod

                mod._casatools = None
                from pclean.utils.image_concat import concat_subcubes

                concat_subcubes(base, nparts=1, extensions=['.image'], max_workers=8)

        # Should have been capped at min(8, 1) = 1
        assert submitted[0] == 1
