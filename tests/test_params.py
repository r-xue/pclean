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
        p = PcleanParams(vis="test.ms", imagename="out", imsize=[256, 256])
        assert p.nms == 1
        assert p.allselpars["0"]["msname"] == "test.ms"
        assert p.imagename == "out"

    def test_multi_vis(self):
        p = PcleanParams(vis=["a.ms", "b.ms"])
        assert p.nms == 2
        assert p.allselpars["0"]["msname"] == "a.ms"
        assert p.allselpars["1"]["msname"] == "b.ms"

    def test_imsize_scalar(self):
        p = PcleanParams(vis="a.ms", imsize=512)
        assert p.allimpars["0"]["imsize"] == [512, 512]

    def test_imsize_pair(self):
        p = PcleanParams(vis="a.ms", imsize=[512, 256])
        assert p.allimpars["0"]["imsize"] == [512, 256]

    def test_cell_scalar(self):
        p = PcleanParams(vis="a.ms", cell="0.5arcsec")
        assert p.allimpars["0"]["cell"] == ["0.5arcsec", "0.5arcsec"]

    def test_specmode_cube(self):
        p = PcleanParams(vis="a.ms", specmode="cube")
        assert p.is_cube
        assert not p.is_mfs

    def test_specmode_mfs(self):
        p = PcleanParams(vis="a.ms", specmode="mfs")
        assert p.is_mfs
        assert not p.is_cube

    def test_weighting_alias(self):
        p = PcleanParams(vis="a.ms", weighting="briggs", robust=0.0)
        assert p.weightpars["type"] == "briggs"
        assert p.weightpars["robust"] == 0.0


class TestPcleanParamsSerialization:
    """Round-trip serialization via to_dict / from_dict."""

    def test_roundtrip(self):
        p = PcleanParams(
            vis="test.ms",
            imagename="roundtrip",
            specmode="cube",
            nchan=128,
            niter=500,
            deconvolver="multiscale",
            scales=[0, 5, 15],
            parallel=True,
            nworkers=4,
        )
        d = p.to_dict()
        p2 = PcleanParams.from_dict(d)
        assert p2.imagename == "roundtrip"
        assert p2.specmode == "cube"
        assert p2.niter == 500
        assert p2.parallelpars["nworkers"] == 4
        assert p2.alldecpars["0"]["scales"] == [0, 5, 15]


class TestSubcubeParams:
    """Channel sub-range parameter generation."""

    def test_subcube(self):
        p = PcleanParams(vis="a.ms", imagename="full", nchan=128)
        sub = p.make_subcube_params(start=32, nchan=32,
                                     image_suffix="1")
        assert sub.allimpars["0"]["nchan"] == 32
        assert sub.allimpars["0"]["start"] == "32"
        assert "subcube.1" in sub.imagename

    def test_subcube_freq_start(self):
        """make_subcube_params preserves frequency strings as start."""
        p = PcleanParams(vis="a.ms", imagename="full", nchan=128)
        sub = p.make_subcube_params(start="214.45GHz", nchan=24,
                                     image_suffix="0")
        assert sub.allimpars["0"]["nchan"] == 24
        assert sub.allimpars["0"]["start"] == "214.45GHz"

    def test_subcube_independence(self):
        p = PcleanParams(vis="a.ms", imagename="full", nchan=128)
        s1 = p.make_subcube_params(0, 64, "0")
        s2 = p.make_subcube_params(64, 64, "1")
        assert s1.imagename != s2.imagename
        # Modifying one must not affect the other
        s1.allimpars["0"]["nchan"] = 999
        assert s2.allimpars["0"]["nchan"] == 64


class TestFreqPartition:
    """Frequency-based cube partition logic."""

    def test_freq_partition_splits_channels(self):
        from pclean.utils.partition import _partition_cube_even
        p = PcleanParams(
            vis="a.ms", imagename="cube", specmode="cube",
            nchan=117, start="214.4501854310GHz", width="15.6245970MHz",
        )
        subs = _partition_cube_even(p, nparts=5, nchan=117)
        assert len(subs) == 5
        # Greedy distribution: first 117%5=2 subcubes get 24 chans,
        # remaining 3 get 23.
        nchans = [s.allimpars["0"]["nchan"] for s in subs]
        assert nchans == [24, 24, 23, 23, 23]
        assert sum(nchans) == 117
        # Each subcube must have a frequency start string, not a channel #
        for s in subs:
            assert "GHz" in s.allimpars["0"]["start"]
        # Subcube starts must be distinct and increasing
        starts = [s.allimpars["0"]["start"] for s in subs]
        assert len(set(starts)) == 5

    def test_channel_partition_fallback(self):
        """When start is a channel index, fallback to channel-based split."""
        from pclean.utils.partition import _partition_cube_even
        p = PcleanParams(
            vis="a.ms", imagename="cube", specmode="cube",
            nchan=100, start=0, width=1,
        )
        subs = _partition_cube_even(p, nparts=4, nchan=100)
        assert len(subs) == 4
        assert all(
            "GHz" not in s.allimpars["0"]["start"] for s in subs
        )


class TestRowChunkParams:
    """Row-chunk parameter generation."""

    def test_rowchunk_imagename(self):
        p = PcleanParams(vis="a.ms", imagename="cont")
        sub_sel = {"0": {"msname": "a.ms", "taql": "ROWID() < 1000"}}
        rp = p.make_rowchunk_params(sub_sel, "0")
        assert "part.0" in rp.imagename
        assert rp.allselpars["0"]["taql"] == "ROWID() < 1000"


class TestCubeChunksize:
    """cube_chunksize parameter and nparts computation."""

    def test_default_chunksize(self):
        p = PcleanParams(vis="a.ms", parallel=True)
        assert p.parallelpars["cube_chunksize"] == -1

    def test_custom_chunksize(self):
        p = PcleanParams(vis="a.ms", parallel=True, cube_chunksize=1)
        assert p.parallelpars["cube_chunksize"] == 1

    def test_chunksize_serialization(self):
        p = PcleanParams(vis="a.ms", parallel=True, cube_chunksize=4)
        d = p.to_dict()
        p2 = PcleanParams.from_dict(d)
        assert p2.parallelpars["cube_chunksize"] == 4

    def test_nparts_from_chunksize(self):
        """_compute_nparts returns ceil(nchan / chunksize)."""
        import math

        class FakeCluster:
            client = None
            worker_count = 5

        p = PcleanParams(
            vis="a.ms", specmode="cube", nchan=117,
            parallel=True, cube_chunksize=1,
        )
        from pclean.parallel.cube_parallel import ParallelCubeImager
        engine = ParallelCubeImager(p, FakeCluster())
        # chunksize=1 → 117 tasks (one per channel)
        assert engine._compute_nparts(5) == 117

    def test_nparts_default_is_nworkers(self):
        """chunksize=-1 falls back to nparts=nworkers."""

        class FakeCluster:
            client = None
            worker_count = 5

        p = PcleanParams(
            vis="a.ms", specmode="cube", nchan=117,
            parallel=True, cube_chunksize=-1,
        )
        from pclean.parallel.cube_parallel import ParallelCubeImager
        engine = ParallelCubeImager(p, FakeCluster())
        assert engine._compute_nparts(5) == 5

    def test_nparts_grouped(self):
        """chunksize=10 → ceil(117/10) = 12 tasks."""
        import math

        class FakeCluster:
            client = None
            worker_count = 5

        p = PcleanParams(
            vis="a.ms", specmode="cube", nchan=117,
            parallel=True, cube_chunksize=10,
        )
        from pclean.parallel.cube_parallel import ParallelCubeImager
        engine = ParallelCubeImager(p, FakeCluster())
        assert engine._compute_nparts(5) == math.ceil(117 / 10)


class TestKeepSubcubes:
    """keep_subcubes parameter and cleanup behaviour."""

    def test_default_keep_subcubes_false(self):
        p = PcleanParams(vis="a.ms", parallel=True)
        assert p.parallelpars["keep_subcubes"] is False

    def test_keep_subcubes_true(self):
        p = PcleanParams(vis="a.ms", parallel=True, keep_subcubes=True)
        assert p.parallelpars["keep_subcubes"] is True

    def test_keep_subcubes_serialization(self):
        p = PcleanParams(vis="a.ms", parallel=True, keep_subcubes=True)
        d = p.to_dict()
        p2 = PcleanParams.from_dict(d)
        assert p2.parallelpars["keep_subcubes"] is True

    def test_cleanup_removes_subcube_dirs(self, tmp_path):
        """_cleanup_subcubes removes .subcube.N images and tmpdirs."""
        from pclean.parallel.cube_parallel import ParallelCubeImager

        base = str(tmp_path / "testimg")
        nparts = 3
        extensions = [".image", ".residual", ".psf"]

        # Create fake subcube directories and tmpdirs
        created = []
        for i in range(nparts):
            for ext in extensions:
                d = f"{base}.subcube.{i}{ext}"
                os.makedirs(d)
                created.append(d)
            tmpdir = str(tmp_path / f".testimg.subcube.{i}.tmpdir")
            os.makedirs(tmpdir)
            created.append(tmpdir)

        ParallelCubeImager._cleanup_subcubes(base, nparts)

        for d in created:
            assert not os.path.exists(d), f"{d} should have been removed"

    def test_cleanup_ignores_missing(self, tmp_path):
        """_cleanup_subcubes does not fail when artifacts don't exist."""
        from pclean.parallel.cube_parallel import ParallelCubeImager
        base = str(tmp_path / "noimg")
        # Should not raise
        ParallelCubeImager._cleanup_subcubes(base, 4)
