"""
Tests for pclean.utils.memory_estimate — RAM heuristic helpers.
"""
import math
import pytest
from pclean.utils.memory_estimate import (
    BYTES_PER_PIXEL_STANDARD,
    WORKER_BASE_OVERHEAD_GIB,
    estimate_worker_memory_gib,
    estimate_peak_ram_gib,
    recommend_nworkers,
)

GIB = 1024 ** 3


class TestEstimateWorkerMemory:
    """Unit tests for estimate_worker_memory_gib."""

    def test_irc10216_calibration_point(self):
        """Reproduce the empirical calibration from IRC+10216 imaging.

        8000x8000 image, 1 chan, standard gridder, hogbom →
        ~4.9 GiB C++ + 0.7 GiB overhead ≈ 5.2–5.6 GiB per worker.
        """
        mem = estimate_worker_memory_gib(imsize=8000, nchan_per_task=1)
        assert 4.5 < mem < 6.5, f"Expected ~5.2 GiB, got {mem:.2f} GiB"

    def test_square_imsize_scalar(self):
        """Scalar imsize treated as square."""
        mem_scalar = estimate_worker_memory_gib(imsize=4096)
        mem_pair = estimate_worker_memory_gib(imsize=[4096, 4096])
        assert mem_scalar == pytest.approx(mem_pair)

    def test_rectangular_imsize(self):
        """Rectangular images should use both dimensions."""
        mem = estimate_worker_memory_gib(imsize=[2048, 1024])
        npix = 2048 * 1024
        expected_image_gib = npix * BYTES_PER_PIXEL_STANDARD / GIB
        expected_total = WORKER_BASE_OVERHEAD_GIB + expected_image_gib
        assert mem == pytest.approx(expected_total, rel=1e-6)

    def test_multichannel_linear_scaling(self):
        """Memory should scale linearly with nchan_per_task."""
        mem1 = estimate_worker_memory_gib(imsize=4096, nchan_per_task=1)
        mem4 = estimate_worker_memory_gib(imsize=4096, nchan_per_task=4)
        image1 = mem1 - WORKER_BASE_OVERHEAD_GIB
        image4 = mem4 - WORKER_BASE_OVERHEAD_GIB
        assert image4 == pytest.approx(4.0 * image1, rel=1e-6)

    def test_mosaic_larger_than_standard(self):
        """Mosaic gridder should use more memory than standard."""
        mem_std = estimate_worker_memory_gib(imsize=4096, gridder="standard")
        mem_mos = estimate_worker_memory_gib(imsize=4096, gridder="mosaic")
        assert mem_mos > mem_std

    def test_mosaic_multifield_scaling(self):
        """More mosaic fields should increase memory."""
        mem1 = estimate_worker_memory_gib(
            imsize=4096, gridder="mosaic", nfields=1
        )
        mem50 = estimate_worker_memory_gib(
            imsize=4096, gridder="mosaic", nfields=50
        )
        assert mem50 > mem1

    def test_mtmfs_nterms_scaling(self):
        """MTMFS should scale as nterms^2."""
        mem1 = estimate_worker_memory_gib(
            imsize=4096, deconvolver="mtmfs", nterms=1
        )
        mem2 = estimate_worker_memory_gib(
            imsize=4096, deconvolver="mtmfs", nterms=2
        )
        image1 = mem1 - WORKER_BASE_OVERHEAD_GIB
        image2 = mem2 - WORKER_BASE_OVERHEAD_GIB
        assert image2 == pytest.approx(4.0 * image1, rel=1e-6)

    def test_mtmfs_nterms1_same_as_hogbom(self):
        """MTMFS with nterms=1 should match hogbom memory."""
        mem_hog = estimate_worker_memory_gib(imsize=4096, deconvolver="hogbom")
        mem_mt1 = estimate_worker_memory_gib(
            imsize=4096, deconvolver="mtmfs", nterms=1
        )
        assert mem_mt1 == pytest.approx(mem_hog, rel=1e-6)

    def test_unknown_gridder_treated_as_standard(self):
        """Unknown gridder defaults to factor 1.0."""
        mem_std = estimate_worker_memory_gib(imsize=4096, gridder="standard")
        mem_unk = estimate_worker_memory_gib(imsize=4096, gridder="novelgrid")
        assert mem_unk == pytest.approx(mem_std, rel=1e-6)

    def test_small_image_dominated_by_overhead(self):
        """Tiny image → memory dominated by base overhead."""
        mem = estimate_worker_memory_gib(imsize=64)
        assert mem == pytest.approx(WORKER_BASE_OVERHEAD_GIB, abs=0.01)

    def test_return_type_is_float(self):
        mem = estimate_worker_memory_gib(imsize=4096)
        assert isinstance(mem, float)


class TestEstimatePeakRam:
    """Tests for the total-system RAM estimator."""

    def test_12_workers_irc10216(self):
        """12 workers x IRC+10216 ≈ 12 × 5.2 + 0.5 ≈ 63 GiB."""
        total = estimate_peak_ram_gib(
            nworkers=12, imsize=8000, nchan_per_task=1
        )
        assert 50 < total < 80

    def test_single_worker(self):
        worker = estimate_worker_memory_gib(imsize=4096)
        total = estimate_peak_ram_gib(nworkers=1, imsize=4096)
        assert total == pytest.approx(worker + 0.5, rel=1e-6)

    def test_scales_with_workers(self):
        t4 = estimate_peak_ram_gib(nworkers=4, imsize=4096)
        t8 = estimate_peak_ram_gib(nworkers=8, imsize=4096)
        # Difference should be 4 * per_worker
        per_worker = estimate_worker_memory_gib(imsize=4096)
        assert (t8 - t4) == pytest.approx(4 * per_worker, rel=1e-6)


class TestRecommendNworkers:
    """Tests for the worker recommendation function."""

    def test_returns_at_least_1(self):
        """Even with tiny RAM, at least 1 worker is returned."""
        n = recommend_nworkers(available_ram_gib=2.0, imsize=8000)
        assert n >= 1

    def test_high_ram_many_workers(self):
        """256 GiB with small images should allow many workers."""
        n = recommend_nworkers(available_ram_gib=256.0, imsize=512)
        assert n > 10

    def test_respects_large_image(self):
        """Large images should reduce worker count."""
        n_small = recommend_nworkers(available_ram_gib=64.0, imsize=1024)
        n_large = recommend_nworkers(available_ram_gib=64.0, imsize=8000)
        assert n_large < n_small

    def test_auto_detect_ram(self):
        """With available_ram_gib=None, should auto-detect from OS."""
        n = recommend_nworkers(available_ram_gib=None, imsize=4096)
        assert isinstance(n, int)
        assert n >= 1

    def test_safety_factor(self):
        """Lower safety factor (less RAM usable per heuristic) → more conservative (fewer workers)."""
        n_high = recommend_nworkers(
            available_ram_gib=64.0, imsize=4096, ram_safety_factor=0.95
        )
        n_low = recommend_nworkers(
            available_ram_gib=64.0, imsize=4096, ram_safety_factor=0.50
        )
        assert n_low <= n_high
