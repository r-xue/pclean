"""
Tests for pclean.params — parameter container & validation.
"""

import copy
import os
import pytest
from pclean.params import PcleanParams


class TestPcleanParamsConstruction:
    """Basic construction and attribute access."""

    def test_single_vis(self):
        p = PcleanParams(vis='test.ms', imagename='out', imsize=[256, 256])
        assert p.nms == 1
        assert p.allselpars['ms0']['msname'] == 'test.ms'
        assert p.imagename == 'out'

    def test_multi_vis(self):
        p = PcleanParams(vis=['a.ms', 'b.ms'])
        assert p.nms == 2
        assert p.allselpars['ms0']['msname'] == 'a.ms'
        assert p.allselpars['ms1']['msname'] == 'b.ms'

    def test_imsize_scalar(self):
        p = PcleanParams(vis='a.ms', imsize=512)
        assert p.allimpars['0']['imsize'] == [512, 512]

    def test_imsize_pair(self):
        p = PcleanParams(vis='a.ms', imsize=[512, 256])
        assert p.allimpars['0']['imsize'] == [512, 256]

    def test_cell_scalar(self):
        p = PcleanParams(vis='a.ms', cell='0.5arcsec')
        assert p.allimpars['0']['cell'] == ['0.5arcsec', '0.5arcsec']

    def test_specmode_cube(self):
        p = PcleanParams(vis='a.ms', specmode='cube')
        assert p.is_cube
        assert not p.is_mfs

    def test_specmode_mfs(self):
        p = PcleanParams(vis='a.ms', specmode='mfs')
        assert p.is_mfs
        assert not p.is_cube

    def test_weighting_alias(self):
        p = PcleanParams(vis='a.ms', weighting='briggs', robust=0.0)
        assert p.weightpars['type'] == 'briggs'
        assert p.weightpars['robust'] == 0.0

    def test_mtmfs_impars(self):
        """nterms and deconvolver must flow into allimpars for defineimage()."""
        p = PcleanParams(vis='a.ms', deconvolver='mtmfs', nterms=2)
        assert p.allimpars['0']['deconvolver'] == 'mtmfs'
        assert p.allimpars['0']['nterms'] == 2

    def test_mtmfs_normpars(self):
        """Normalizer must also get the correct nterms for mtmfs."""
        p = PcleanParams(vis='a.ms', deconvolver='mtmfs', nterms=3)
        assert p.allnormpars['0']['nterms'] == 3
        assert p.allnormpars['0']['deconvolver'] == 'mtmfs'

    def test_non_mtmfs_normpars_nterms(self):
        """For non-mtmfs deconvolvers, normalizer nterms should be 1."""
        p = PcleanParams(vis='a.ms', deconvolver='hogbom')
        assert p.allnormpars['0']['nterms'] == 1


class TestPcleanParamsSerialization:
    """Round-trip serialization via to_dict / from_dict."""

    def test_roundtrip(self):
        p = PcleanParams(
            vis='test.ms',
            imagename='roundtrip',
            specmode='cube',
            nchan=128,
            niter=500,
            deconvolver='multiscale',
            scales=[0, 5, 15],
            parallel=True,
            nworkers=4,
        )
        d = p.to_dict()
        p2 = PcleanParams.from_dict(d)
        assert p2.imagename == 'roundtrip'
        assert p2.specmode == 'cube'
        assert p2.niter == 500
        assert p2.parallelpars['nworkers'] == 4
        assert p2.alldecpars['0']['scales'] == [0, 5, 15]


class TestSubcubeParams:
    """Channel sub-range parameter generation."""

    def test_subcube(self):
        p = PcleanParams(vis='a.ms', imagename='full', nchan=128)
        sub = p.make_subcube_params(start=32, nchan=32, image_suffix='1')
        assert sub.allimpars['0']['nchan'] == 32
        assert sub.allimpars['0']['start'] == '32'
        assert 'subcube.1' in sub.imagename

    def test_subcube_freq_start(self):
        """make_subcube_params preserves frequency strings as start."""
        p = PcleanParams(vis='a.ms', imagename='full', nchan=128)
        sub = p.make_subcube_params(start='214.45GHz', nchan=24, image_suffix='0')
        assert sub.allimpars['0']['nchan'] == 24
        assert sub.allimpars['0']['start'] == '214.45GHz'

    def test_subcube_independence(self):
        p = PcleanParams(vis='a.ms', imagename='full', nchan=128)
        s1 = p.make_subcube_params(0, 64, '0')
        s2 = p.make_subcube_params(64, 64, '1')
        assert s1.imagename != s2.imagename
        # Modifying one must not affect the other
        s1.allimpars['0']['nchan'] = 999
        assert s2.allimpars['0']['nchan'] == 64


class TestFreqPartition:
    """Frequency-based cube partition logic."""

    def test_freq_partition_splits_channels(self):
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        p = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=117,
            start='214.4501854310GHz',
            width='15.6245970MHz',
        )
        subs = _partition_cube_even(p, nparts=5, nchan=117)
        assert len(subs) == 5
        # Greedy distribution: first 117%5=2 subcubes get 24 chans,
        # remaining 3 get 23.
        nchans = [s.image.nchan for s in subs]
        assert nchans == [24, 24, 23, 23, 23]
        assert sum(nchans) == 117
        # Each subcube must have a frequency start string, not a channel #
        for s in subs:
            assert 'GHz' in s.image.start
        # Subcube starts must be distinct and increasing
        starts = [s.image.start for s in subs]
        assert len(set(starts)) == 5

    def test_channel_partition_fallback(self):
        """When start is a channel index, fallback to channel-based split."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        p = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=100,
            start='0',
            width='1',
        )
        subs = _partition_cube_even(p, nparts=4, nchan=100)
        assert len(subs) == 4
        assert all('GHz' not in s.image.start for s in subs)


class TestRowChunkParams:
    """Row-chunk parameter generation."""

    def test_rowchunk_imagename(self):
        p = PcleanParams(vis='a.ms', imagename='cont')
        sub_sel = {'ms0': {'msname': 'a.ms', 'taql': 'ROWID() < 1000'}}
        rp = p.make_rowchunk_params(sub_sel, '0')
        assert 'part.0' in rp.imagename
        assert rp.allselpars['ms0']['taql'] == 'ROWID() < 1000'


class TestCubeChunksize:
    """cube_chunksize parameter and nparts computation."""

    def test_default_chunksize(self):
        p = PcleanParams(vis='a.ms', parallel=True)
        assert p.parallelpars['cube_chunksize'] == -1

    def test_custom_chunksize(self):
        p = PcleanParams(vis='a.ms', parallel=True, cube_chunksize=1)
        assert p.parallelpars['cube_chunksize'] == 1

    def test_chunksize_serialization(self):
        p = PcleanParams(vis='a.ms', parallel=True, cube_chunksize=4)
        d = p.to_dict()
        p2 = PcleanParams.from_dict(d)
        assert p2.parallelpars['cube_chunksize'] == 4

    def test_nparts_from_chunksize(self):
        """_compute_nparts returns ceil(nchan / chunksize)."""
        import math
        from pclean.config import PcleanConfig

        class FakeCluster:
            client = None
            worker_count = 5

        p = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            specmode='cube',
            nchan=117,
            parallel=True,
            cube_chunksize=1,
        )
        from pclean.parallel.cube_parallel import ParallelCubeImager

        engine = ParallelCubeImager(p, FakeCluster())
        # chunksize=1 → 117 tasks (one per channel)
        assert engine._compute_nparts(5) == 117

    def test_nparts_default_is_nworkers(self):
        """chunksize=-1 falls back to nparts=nworkers."""
        from pclean.config import PcleanConfig

        class FakeCluster:
            client = None
            worker_count = 5

        p = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            specmode='cube',
            nchan=117,
            parallel=True,
            cube_chunksize=-1,
        )
        from pclean.parallel.cube_parallel import ParallelCubeImager

        engine = ParallelCubeImager(p, FakeCluster())
        assert engine._compute_nparts(5) == 5

    def test_nparts_grouped(self):
        """chunksize=10 → ceil(117/10) = 12 tasks."""
        import math
        from pclean.config import PcleanConfig

        class FakeCluster:
            client = None
            worker_count = 5

        p = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            specmode='cube',
            nchan=117,
            parallel=True,
            cube_chunksize=10,
        )
        from pclean.parallel.cube_parallel import ParallelCubeImager

        engine = ParallelCubeImager(p, FakeCluster())
        assert engine._compute_nparts(5) == math.ceil(117 / 10)


class TestKeepSubcubes:
    """keep_subcubes parameter and cleanup behaviour."""

    def test_default_keep_subcubes_false(self):
        p = PcleanParams(vis='a.ms', parallel=True)
        assert p.parallelpars['keep_subcubes'] is False

    def test_keep_subcubes_true(self):
        p = PcleanParams(vis='a.ms', parallel=True, keep_subcubes=True)
        assert p.parallelpars['keep_subcubes'] is True

    def test_keep_subcubes_serialization(self):
        p = PcleanParams(vis='a.ms', parallel=True, keep_subcubes=True)
        d = p.to_dict()
        p2 = PcleanParams.from_dict(d)
        assert p2.parallelpars['keep_subcubes'] is True

    def test_cleanup_removes_subcube_dirs(self, tmp_path):
        """_cleanup_subcubes removes .subcube.N images and tmpdirs."""
        from pclean.parallel.cube_parallel import ParallelCubeImager

        base = str(tmp_path / 'testimg')
        nparts = 3
        extensions = ['.image', '.residual', '.psf']

        # Create fake subcube directories and tmpdirs
        created = []
        for i in range(nparts):
            for ext in extensions:
                d = f'{base}.subcube.{i}{ext}'
                os.makedirs(d)
                created.append(d)
            tmpdir = str(tmp_path / f'.testimg.subcube.{i}.tmpdir')
            os.makedirs(tmpdir)
            created.append(tmpdir)

        ParallelCubeImager._cleanup_subcubes(base, nparts)

        for d in created:
            assert not os.path.exists(d), f'{d} should have been removed'

    def test_cleanup_ignores_missing(self, tmp_path):
        """_cleanup_subcubes does not fail when artifacts don't exist."""
        from pclean.parallel.cube_parallel import ParallelCubeImager

        base = str(tmp_path / 'noimg')
        # Should not raise
        ParallelCubeImager._cleanup_subcubes(base, 4)


class TestConcatModeConfig:
    """concat_mode field in ClusterConfig / PcleanConfig."""

    def test_default_is_auto(self):
        from pclean.config import PcleanConfig

        p = PcleanConfig.from_flat_kwargs(vis='a.ms')
        assert p.cluster.concat_mode == 'auto'

    def test_concat_mode_paged(self):
        from pclean.config import PcleanConfig

        p = PcleanConfig.from_flat_kwargs(vis='a.ms', concat_mode='paged')
        assert p.cluster.concat_mode == 'paged'

    def test_concat_mode_virtual(self):
        from pclean.config import PcleanConfig

        p = PcleanConfig.from_flat_kwargs(vis='a.ms', concat_mode='virtual')
        assert p.cluster.concat_mode == 'virtual'

    def test_concat_mode_movevirtual(self):
        from pclean.config import PcleanConfig

        p = PcleanConfig.from_flat_kwargs(vis='a.ms', concat_mode='movevirtual')
        assert p.cluster.concat_mode == 'movevirtual'

    def test_invalid_concat_mode_raises(self):
        from pclean.config import PcleanConfig
        import pydantic

        with pytest.raises((pydantic.ValidationError, ValueError)):
            PcleanConfig.from_flat_kwargs(vis='a.ms', concat_mode='badvalue')

    def test_concat_mode_serialization_roundtrip(self):
        from pclean.config import PcleanConfig

        p = PcleanConfig.from_flat_kwargs(vis='a.ms', concat_mode='movevirtual')
        d = p.model_dump()
        p2 = PcleanConfig.model_validate(d)
        assert p2.cluster.concat_mode == 'movevirtual'


class TestResolveConcatMode:
    """_resolve_concat_mode() maps user-level concat_mode to ia.imageconcat mode."""

    def _resolve(self, user_mode, keep_subcubes):
        from pclean.parallel.cube_parallel import _resolve_concat_mode

        return _resolve_concat_mode(user_mode, keep_subcubes)

    def test_auto_keep_false_gives_paged(self):
        mode, keep = self._resolve('auto', False)
        assert mode == 'paged'
        assert keep is False

    def test_auto_keep_true_gives_nomovevirtual(self):
        mode, keep = self._resolve('auto', True)
        assert mode == 'nomovevirtual'
        assert keep is True

    def test_virtual_gives_nomovevirtual(self):
        mode, keep = self._resolve('virtual', True)
        assert mode == 'nomovevirtual'

    def test_virtual_forces_keep_true(self):
        """concat_mode='virtual' must force keep=True even if False was passed."""
        _, keep = self._resolve('virtual', False)
        assert keep is True

    def test_virtual_keep_false_logs_warning(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING, logger='pclean.parallel.cube_parallel'):
            self._resolve('virtual', False)
        assert 'forcing keep_subcubes=True' in caplog.text

    def test_movevirtual_gives_movevirtual(self):
        mode, keep = self._resolve('movevirtual', False)
        assert mode == 'movevirtual'
        assert keep is False  # movevirtual consumes subcubes; keep unchanged

    def test_paged_explicit(self):
        mode, keep = self._resolve('paged', False)
        assert mode == 'paged'

    def test_unknown_mode_falls_back_to_paged(self):
        """Unrecognised future values must default to safe 'paged' mode."""
        mode, _ = self._resolve('supersonic', False)
        assert mode == 'paged'


class TestFreqWidthPropagation:
    """Frequency-based partitions must give subcubes a frequency width, not a channel count."""

    def test_freq_subcubes_have_freq_width(self):
        """When start/width are frequency strings, every subcube.image.width
        should be a frequency quantity (e.g. ending in 'GHz'), not a bare
        channel count like '1'."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        cfg = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=40,
            start='214.4501854310GHz',
            width='15.6245970MHz',
        )
        subs = _partition_cube_even(cfg, nparts=8, nchan=40)
        for sub in subs:
            assert sub.image.width is not None
            assert 'GHz' in sub.image.width or 'MHz' in sub.image.width, (
                f'subcube width should be a frequency quantity, got {sub.image.width!r}'
            )

    def test_freq_width_consistent_across_subcubes(self):
        """All subcubes should carry the same frequency width."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        cfg = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=20,
            start='268.5GHz',
            width='0.2441382MHz',
        )
        subs = _partition_cube_even(cfg, nparts=5, nchan=20)
        widths = {s.image.width for s in subs}
        assert len(widths) == 1, f'all subcubes should share the same width, got {widths}'

    def test_single_chan_subcube_has_freq_width(self):
        """chunksize=1 → nchan=1 subcubes must still get a frequency width."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        cfg = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=4,
            start='268.5GHz',
            width='0.2441382MHz',
        )
        subs = _partition_cube_even(cfg, nparts=4, nchan=4)
        assert len(subs) == 4
        for sub in subs:
            assert sub.image.nchan == 1
            assert 'GHz' in sub.image.width or 'MHz' in sub.image.width

    def test_channel_start_keeps_channel_width(self):
        """When start is a channel index, width should remain channel-based."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        cfg = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=20,
            start='0',
            width='1',
        )
        subs = _partition_cube_even(cfg, nparts=4, nchan=20)
        for sub in subs:
            # Channel-based start → width should NOT be a frequency string
            assert 'GHz' not in (sub.image.width or '')


class TestBriggsBwtaperFracbw:
    """briggsbwtaper partitions with nchan=1 subcubes must produce a positive fracbw."""

    def test_fracbw_precomputed_on_parent(self):
        """_partition_cube_even should set config.weight.fracbw before
        creating subcubes so that nchan=1 children inherit it."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        cfg = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=10,
            start='268.5GHz',
            width='0.2441382MHz',
            weighting='briggsbwtaper',
            robust=0.5,
        )
        assert cfg.weight.fracbw is None  # not set yet
        _partition_cube_even(cfg, nparts=10, nchan=10)
        # After partitioning, parent config should have fracbw populated
        assert cfg.weight.fracbw is not None
        assert cfg.weight.fracbw > 0

    def test_single_chan_subcube_has_positive_fracbw(self):
        """nchan=1 subcubes with briggsbwtaper must have fracbw > 0 in
        to_casa_weightpars(), not the zero that nchan=1 alone would produce."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        cfg = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=10,
            start='268.5GHz',
            width='0.2441382MHz',
            weighting='briggsbwtaper',
            robust=0.5,
        )
        subs = _partition_cube_even(cfg, nparts=10, nchan=10)
        for sub in subs:
            assert sub.image.nchan == 1
            wp = sub.to_casa_weightpars()
            assert 'fracbw' in wp, 'weightpars must contain fracbw'
            assert wp['fracbw'] > 0, f'fracbw must be positive, got {wp["fracbw"]}'

    def test_fracbw_value_is_reasonable(self):
        """fracbw should be consistent with 2*(fmax-fmin)/(fmax+fmin)
        computed from the full cube frequency span."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        nchan = 20
        start_ghz = 268.5
        width_mhz = 0.2441382
        cfg = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=nchan,
            start=f'{start_ghz}GHz',
            width=f'{width_mhz}MHz',
            weighting='briggsbwtaper',
            robust=0.5,
        )
        subs = _partition_cube_even(cfg, nparts=nchan, nchan=nchan)
        fracbw = subs[0].to_casa_weightpars()['fracbw']

        # Expected: 2*(fmax - fmin)/(fmax + fmin) from the *full* cube
        fmin = start_ghz * 1e9
        fmax = fmin + (nchan - 1) * width_mhz * 1e6
        expected = 2.0 * (fmax - fmin) / (fmax + fmin)
        assert abs(fracbw - expected) / expected < 1e-4, (
            f'fracbw={fracbw} differs from expected={expected}'
        )

    def test_all_subcubes_share_same_fracbw(self):
        """Every subcube in a briggsbwtaper partition should get the same fracbw."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        cfg = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=12,
            start='268.5GHz',
            width='0.2441382MHz',
            weighting='briggsbwtaper',
            robust=0.5,
        )
        subs = _partition_cube_even(cfg, nparts=4, nchan=12)
        fracbws = [s.to_casa_weightpars()['fracbw'] for s in subs]
        assert all(f == fracbws[0] for f in fracbws), (
            f'all subcubes must share the same fracbw, got {fracbws}'
        )

    def test_non_briggsbwtaper_no_fracbw(self):
        """Non-briggsbwtaper weighting should not produce a fracbw key."""
        from pclean.config import PcleanConfig
        from pclean.utils.partition import _partition_cube_even

        cfg = PcleanConfig.from_flat_kwargs(
            vis='a.ms',
            imagename='cube',
            specmode='cube',
            nchan=10,
            start='268.5GHz',
            width='0.2441382MHz',
            weighting='briggs',
            robust=0.5,
        )
        subs = _partition_cube_even(cfg, nparts=10, nchan=10)
        for sub in subs:
            wp = sub.to_casa_weightpars()
            assert 'fracbw' not in wp

