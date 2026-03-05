"""Tests for the pure-numpy auto-multithresh masking module.

These tests exercise the algorithmic stages without requiring casatools,
using synthetic 2-D arrays to verify thresholding, pruning, smoothing,
growing, and the main ``automask_plane()`` entry point.
"""

from __future__ import annotations

import numpy as np
import pytest

from pclean.imaging.automask import (
    AutoMaskConfig,
    AutoMaskState,
    _grow_mask,
    _plane_stats,
    _prune_regions,
    _robust_rms,
    _smooth_and_cut,
    automask_plane,
)


# ======================================================================
# Helper: deterministic Gaussian source
# ======================================================================

def _make_residual(shape=(128, 128), peak=1.0, noise_std=0.01,
                   src_pos=(64, 64), src_sigma=3.0, seed=42):
    """Create a residual image with a Gaussian source + noise."""
    rng = np.random.default_rng(seed)
    data = rng.normal(0.0, noise_std, shape).astype(np.float32)
    y, x = np.mgrid[0:shape[0], 0:shape[1]]
    gauss = peak * np.exp(-(
        (y - src_pos[0])**2 + (x - src_pos[1])**2
    ) / (2.0 * src_sigma**2))
    data += gauss.astype(np.float32)
    return data


# ======================================================================
# _robust_rms
# ======================================================================

class TestRobustRMS:
    """Tests for MAD-based robust RMS estimator."""

    def test_gaussian_noise(self):
        rng = np.random.default_rng(0)
        data = rng.normal(0.0, 1.0, (1000, 1000)).astype(np.float32)
        rms = _robust_rms(data)
        assert 0.95 < rms < 1.05, f'Robust RMS should be ~1.0, got {rms}'

    def test_zero_array(self):
        data = np.zeros((10, 10), dtype=np.float32)
        assert _robust_rms(data) == 0.0


# ======================================================================
# _plane_stats
# ======================================================================

class TestPlaneStats:
    """Tests for statistics computation."""

    def test_pure_noise(self):
        rng = np.random.default_rng(1)
        data = rng.normal(0.0, 0.5, (256, 256)).astype(np.float32)
        absmax, median, rms, _ = _plane_stats(data, fastnoise=True)
        assert absmax > 0
        assert abs(median) < 0.1
        assert 0.4 < rms < 0.6

    def test_fastnoise_false(self):
        rng = np.random.default_rng(2)
        data = rng.normal(0.0, 1.0, (256, 256)).astype(np.float32)
        _, _, rms_fast, _ = _plane_stats(data, fastnoise=True)
        _, _, rms_slow, _ = _plane_stats(data, fastnoise=False)
        # Both should be close to 1.0
        assert abs(rms_fast - 1.0) < 0.1
        assert abs(rms_slow - 1.0) < 0.15

    def test_mad_robust_to_emission(self):
        """MAD-based RMS should be robust to emission contamination.

        With ~25% emission pixels (mimicking a bright CO channel),
        np.std would give an inflated RMS, but MAD stays close to the
        true noise because it has a 50% breakdown point.
        """
        rng = np.random.default_rng(42)
        noise_sigma = 0.5
        data = rng.normal(0.0, noise_sigma, (256, 256)).astype(np.float32)
        # inject emission into ~25% of pixels (top quartile)
        q75 = np.percentile(data, 75)
        data[data > q75] += 3.0
        _, _, rms, _ = _plane_stats(data, fastnoise=True)
        # MAD-based RMS should still be close to true noise
        assert rms < noise_sigma * 1.5, (
            f"rms={rms:.4f} too high — not robust to emission"
        )

    def test_fastnoise_false_with_prev_mask(self):
        """fastnoise=False should exclude previously-masked regions."""
        rng = np.random.default_rng(7)
        noise_sigma = 0.5
        data = rng.normal(0.0, noise_sigma, (256, 256)).astype(np.float32)
        # inject strong emission in a region
        data[100:150, 100:150] = 5.0
        # create a mask covering the emission
        prev_mask = np.zeros_like(data)
        prev_mask[100:150, 100:150] = 1.0
        _, _, rms, _ = _plane_stats(data, fastnoise=False,
                                    prev_mask=prev_mask)
        # with emission excluded, RMS should be close to true noise
        assert abs(rms - noise_sigma) < 0.15, (
            f"rms={rms:.4f} should be ~{noise_sigma} with emission masked"
        )


# ======================================================================
# _prune_regions
# ======================================================================

class TestPruneRegions:
    """Tests for small-region pruning."""

    def test_keeps_large_region(self):
        mask = np.zeros((64, 64), dtype=np.float32)
        mask[10:20, 10:20] = 1.0  # 100 pixels
        pruned = _prune_regions(mask, min_size=50)
        assert pruned.sum() == 100

    def test_removes_small_region(self):
        mask = np.zeros((64, 64), dtype=np.float32)
        mask[10:12, 10:12] = 1.0  # 4 pixels
        pruned = _prune_regions(mask, min_size=10)
        assert pruned.sum() == 0

    def test_mixed_regions(self):
        mask = np.zeros((64, 64), dtype=np.float32)
        mask[5:15, 5:15] = 1.0    # 100 pixels
        mask[40:42, 40:42] = 1.0  # 4 pixels
        pruned = _prune_regions(mask, min_size=10)
        assert pruned.sum() == 100

    def test_zero_min_size_noop(self):
        mask = np.ones((10, 10), dtype=np.float32)
        result = _prune_regions(mask, min_size=0)
        np.testing.assert_array_equal(result, mask)


# ======================================================================
# _smooth_and_cut
# ======================================================================

class TestSmoothAndCut:
    """Tests for Gaussian smoothing and binarisation."""

    def test_single_pixel_grows(self):
        mask = np.zeros((64, 64), dtype=np.float32)
        mask[32, 32] = 1.0
        smoothed = _smooth_and_cut(mask, beam_sigma_pix=(2.0, 2.0),
                                   smooth_factor=1.0, cut_threshold=0.01)
        # The smoothed mask should be larger than the original point
        assert smoothed.sum() > 1

    def test_zero_mask_unchanged(self):
        mask = np.zeros((32, 32), dtype=np.float32)
        result = _smooth_and_cut(mask, beam_sigma_pix=(2.0, 2.0),
                                 smooth_factor=1.0, cut_threshold=0.01)
        assert result.sum() == 0

    def test_high_cut_threshold(self):
        mask = np.zeros((64, 64), dtype=np.float32)
        mask[30:34, 30:34] = 1.0
        result = _smooth_and_cut(mask, beam_sigma_pix=(2.0, 2.0),
                                 smooth_factor=1.0, cut_threshold=0.99)
        # Very high cut threshold → only peak of smoothed survives
        assert result.sum() < mask.sum()


# ======================================================================
# _grow_mask
# ======================================================================

class TestGrowMask:
    """Tests for binary dilation growth."""

    def test_grows_within_constraint(self):
        prev = np.zeros((32, 32), dtype=np.float32)
        prev[16, 16] = 1.0
        constraint = np.ones((32, 32), dtype=np.float32)
        grown = _grow_mask(prev, constraint, iterations=3)
        assert grown.sum() > 1
        # Should be roughly diamond-shaped
        assert grown[16, 16] == 1.0
        assert grown[13, 16] == 1.0  # 3 pixels up

    def test_respects_constraint(self):
        prev = np.zeros((32, 32), dtype=np.float32)
        prev[16, 16] = 1.0
        constraint = np.zeros((32, 32), dtype=np.float32)
        constraint[15:18, 15:18] = 1.0  # small constraint region
        grown = _grow_mask(prev, constraint, iterations=100)
        assert grown.sum() <= constraint.sum()

    def test_zero_iterations_noop(self):
        prev = np.zeros((16, 16), dtype=np.float32)
        prev[8, 8] = 1.0
        constraint = np.ones((16, 16), dtype=np.float32)
        result = _grow_mask(prev, constraint, iterations=0)
        assert result.sum() == 1.0

    def test_empty_mask_noop(self):
        prev = np.zeros((16, 16), dtype=np.float32)
        constraint = np.ones((16, 16), dtype=np.float32)
        result = _grow_mask(prev, constraint, iterations=10)
        assert result.sum() == 0.0


# ======================================================================
# automask_plane — integration tests
# ======================================================================

class TestAutomaskPlane:
    """Integration tests for the full automasking pipeline."""

    @pytest.fixture()
    def bright_source(self):
        """Residual with a single bright Gaussian source."""
        return _make_residual(shape=(128, 128), peak=1.0, noise_std=0.01)

    @pytest.fixture()
    def default_cfg(self):
        return AutoMaskConfig(
            sidelobethreshold=2.0,
            noisethreshold=4.0,
            lownoisethreshold=1.5,
            negativethreshold=0.0,
            smoothfactor=1.0,
            minbeamfrac=0.3,
            cutthreshold=0.01,
            growiterations=75,
            dogrowprune=True,
            fastnoise=True,
        )

    def test_detects_bright_source(self, bright_source, default_cfg):
        state = AutoMaskState()
        mask = automask_plane(
            residual=bright_source,
            sidelobe_level=0.1,
            beam_area_pix=30.0,
            beam_sigma_pix=(3.0, 3.0),
            cfg=default_cfg,
            state=state,
        )
        assert mask.shape == bright_source.shape
        assert mask.dtype == np.float32
        # Mask should cover some area around the source
        assert mask.sum() > 0
        # Centre of source should be masked
        assert mask[64, 64] == 1.0

    def test_empty_residual_no_mask(self, default_cfg):
        residual = np.zeros((64, 64), dtype=np.float32)
        state = AutoMaskState()
        mask = automask_plane(
            residual=residual,
            sidelobe_level=0.1,
            beam_area_pix=20.0,
            beam_sigma_pix=(2.0, 2.0),
            cfg=default_cfg,
            state=state,
        )
        assert mask.sum() == 0

    def test_state_updates(self, bright_source, default_cfg):
        state = AutoMaskState()
        assert state.iteration == 0
        assert state.prevmask is None

        automask_plane(
            residual=bright_source,
            sidelobe_level=0.1,
            beam_area_pix=30.0,
            beam_sigma_pix=(3.0, 3.0),
            cfg=default_cfg,
            state=state,
        )
        assert state.iteration == 1
        assert state.prevmask is not None
        assert state.posmask is not None

    def test_grow_on_second_iteration(self, bright_source, default_cfg):
        state = AutoMaskState()
        # First iteration (no grow)
        mask1 = automask_plane(
            residual=bright_source,
            sidelobe_level=0.1,
            beam_area_pix=30.0,
            beam_sigma_pix=(3.0, 3.0),
            cfg=default_cfg,
            state=state,
        )
        # Second iteration (grow enabled)
        mask2 = automask_plane(
            residual=bright_source,
            sidelobe_level=0.1,
            beam_area_pix=30.0,
            beam_sigma_pix=(3.0, 3.0),
            cfg=default_cfg,
            state=state,
        )
        # Mask can only accumulate (grow + combine)
        assert mask2.sum() >= mask1.sum()

    def test_pb_mask_excludes_edges(self, bright_source, default_cfg):
        state = AutoMaskState()
        # PB that has low response at edges
        pb = np.ones_like(bright_source) * 0.5
        pb[:10, :] = 0.05
        pb[-10:, :] = 0.05
        mask = automask_plane(
            residual=bright_source,
            sidelobe_level=0.1,
            beam_area_pix=30.0,
            beam_sigma_pix=(3.0, 3.0),
            cfg=default_cfg,
            state=state,
            pb=pb,
            pblimit=0.2,
        )
        # No mask pixels in the low-PB edge rows
        assert mask[:10, :].sum() == 0
        assert mask[-10:, :].sum() == 0

    def test_negative_mask(self, default_cfg):
        """Negative features should be captured when negativethreshold > 0."""
        default_cfg.negativethreshold = 3.0
        # Make a residual with a negative source
        residual = _make_residual(shape=(128, 128), peak=-0.5,
                                  noise_std=0.01, src_pos=(64, 64))
        state = AutoMaskState()
        mask = automask_plane(
            residual=residual,
            sidelobe_level=0.1,
            beam_area_pix=30.0,
            beam_sigma_pix=(3.0, 3.0),
            cfg=default_cfg,
            state=state,
        )
        # The negative source should produce some mask
        assert mask.sum() > 0

    def test_noise_only_no_mask(self, default_cfg):
        """Pure noise should produce no (or very small) mask."""
        rng = np.random.default_rng(99)
        residual = rng.normal(0.0, 0.001, (128, 128)).astype(np.float32)
        state = AutoMaskState()
        mask = automask_plane(
            residual=residual,
            sidelobe_level=0.1,
            beam_area_pix=30.0,
            beam_sigma_pix=(3.0, 3.0),
            cfg=default_cfg,
            state=state,
        )
        # With noise only, mask should be very small or zero
        pct = 100.0 * mask.sum() / residual.size
        assert pct < 1.0, f'Noise-only mask too large: {pct:.1f}%'


# ======================================================================
# AutoMaskConfig.from_pclean_config
# ======================================================================

class TestAutoMaskConfig:
    """Test config creation from DeconvolutionConfig."""

    def test_from_pclean_config(self):
        from pclean.config import PcleanConfig

        cfg = PcleanConfig.from_flat_kwargs(
            vis='test.ms',
            imagename='test',
            usemask='auto-multithresh',
            sidelobethreshold=2.5,
            noisethreshold=4.5,
            growiterations=50,
        )
        am_cfg = AutoMaskConfig.from_pclean_config(cfg.deconvolution)
        assert am_cfg.sidelobethreshold == 2.5
        assert am_cfg.noisethreshold == 4.5
        assert am_cfg.growiterations == 50
