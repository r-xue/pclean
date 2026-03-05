"""Pure-numpy auto-multithresh masking.

Re-implements CASA's ``SDMaskHandler::autoMaskByMultiThreshold`` algorithm
using numpy/scipy, eliminating:

* Repeated full-cube copy to TempImage
* Per-plane TempImage allocations (~8 per chan per iteration)
* casacore TableCache interactions (mask0 subtable bug)
* Multiple statistics passes

The algorithm faithfully reproduces the six stages of the C++ implementation:
threshold → prune → smooth → grow → combine → negative mask.

References:
    SDMaskHandler.cc  ``autoMaskByMultiThreshold()``  (CASA 6.x)
    Kepley et al. 2020, PASP 132, 024505
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
from numpy.typing import NDArray
from scipy.ndimage import (
    binary_dilation,
    gaussian_filter,
    label,
    sum as ndi_sum,
)

log = logging.getLogger(__name__)

# Type alias for a 2-D float32 image plane.
Plane = NDArray[np.float32]


# ======================================================================
# Configuration
# ======================================================================

@dataclass
class AutoMaskConfig:
    """Parameters mirroring CASA's auto-multithresh knobs.

    Defaults match CASA's ``tclean`` defaults.
    """

    sidelobethreshold: float = 3.0
    noisethreshold: float = 5.0
    lownoisethreshold: float = 1.5
    negativethreshold: float = 0.0
    smoothfactor: float = 1.0
    minbeamfrac: float = 0.3
    cutthreshold: float = 0.01
    growiterations: int = 100
    dogrowprune: bool = True
    minpercentchange: float = 0.0
    fastnoise: bool = True

    @classmethod
    def from_pclean_config(cls, dec) -> AutoMaskConfig:
        """Build from a ``DeconvolutionConfig`` object."""
        return cls(
            sidelobethreshold=dec.sidelobethreshold,
            noisethreshold=dec.noisethreshold,
            lownoisethreshold=dec.lownoisethreshold,
            negativethreshold=dec.negativethreshold,
            smoothfactor=dec.smoothfactor,
            minbeamfrac=dec.minbeamfrac,
            cutthreshold=dec.cutthreshold,
            growiterations=dec.growiterations,
            dogrowprune=dec.dogrowprune,
            minpercentchange=dec.minpercentchange,
            fastnoise=dec.fastnoise,
        )


# ======================================================================
# Per-channel state
# ======================================================================

@dataclass
class AutoMaskState:
    """Mutable state carried across major-cycle iterations.

    The C++ implementation keeps ``posmask`` and ``prevmask`` inside
    ``SIImageStore``.  Here we keep lightweight numpy arrays per
    channel so the Python caller can stash them between cycles.
    """

    posmask: Plane | None = None  # accumulated positive mask
    prevmask: Plane | None = None  # mask from previous iteration
    iteration: int = 0  # how many times automask has been called
    skip: bool = False  # channel flagged as stable


# ======================================================================
# Statistics helpers
# ======================================================================

def _robust_rms(data: Plane) -> float:
    """MAD-based robust RMS estimate (σ ≈ 1.4826 × MAD)."""
    med = np.median(data)
    mad = np.median(np.abs(data - med))
    return float(mad * 1.4826)


def _plane_stats(
    residual: Plane,
    *,
    fastnoise: bool = True,
    prev_mask: Plane | None = None,
) -> tuple[float, float, float, float]:
    """Compute (absmax, median, robust_rms, classic_rms) for one plane.

    Both ``fastnoise`` paths use the MAD-based robust RMS estimator
    (σ ≈ 1.4826 × MAD), matching CASA C++ ``SDMaskHandler`` which
    always uses ``medabsdevmed * 1.4826`` from ``ImageStatsCalculator``
    with ``robust=true``.

    With ``fastnoise=True`` the MAD is computed on **all** pixels
    (source emission included); MAD is inherently robust to outliers.
    With ``fastnoise=False`` pixels inside *prev_mask* are excluded
    before computing the MAD, giving a source-free noise estimate.
    """
    absmax = float(np.max(np.abs(residual)))
    median = float(np.median(residual))
    if fastnoise:
        rms = _robust_rms(residual)
    else:
        if prev_mask is not None and np.any(prev_mask > 0.5):
            source_free = residual[prev_mask < 0.5]
            if source_free.size > 0:
                rms = _robust_rms(source_free)
            else:
                rms = _robust_rms(residual)
        else:
            rms = _robust_rms(residual)
    return absmax, median, rms, rms


# ======================================================================
# Core algorithm stages
# ======================================================================

def _prune_regions(
    mask: Plane,
    min_size: float,
) -> Plane:
    """Remove connected regions smaller than *min_size* pixels.

    Equivalent to ``SDMaskHandler::YAPruneRegions``.
    """
    if min_size <= 0:
        return mask
    labeled, n_features = label(mask)
    if n_features == 0:
        return mask
    component_sizes = ndi_sum(mask, labeled, range(1, n_features + 1))
    # Build a look-up table: label → keep (1) or discard (0)
    keep = np.zeros(n_features + 1, dtype=np.float32)
    n_kept = 0
    for i, size in enumerate(component_sizes, start=1):
        if size >= min_size:
            keep[i] = 1.0
            n_kept += 1
    log.debug('prune: %d / %d regions kept (min_size=%.1f pix)',
              n_kept, n_features, min_size)
    return keep[labeled]


def _smooth_and_cut(
    mask: Plane,
    beam_sigma_pix: tuple[float, float],
    smooth_factor: float,
    cut_threshold: float,
) -> Plane:
    """Gaussian-smooth the mask and binarise at *cut_threshold × max*.

    Equivalent to ``SDMaskHandler::convolveMask`` + ``makeMaskByPerChanThreshold``
    on the smoothed result.
    """
    if mask.max() == 0:
        return mask
    sigma = (beam_sigma_pix[0] * smooth_factor,
             beam_sigma_pix[1] * smooth_factor)
    smoothed = gaussian_filter(mask.astype(np.float32), sigma=sigma)
    peak = smoothed.max()
    if peak <= 0:
        return np.zeros_like(mask)
    cut = cut_threshold * peak
    return (smoothed >= cut).astype(np.float32)


def _grow_mask(
    prev_mask: Plane,
    constraint: Plane,
    iterations: int,
) -> Plane:
    """Binary-dilate *prev_mask* within *constraint* region.

    Uses a 3×3 cross structuring element (4-connected), matching
    ``SDMaskHandler::binaryDilation``.
    """
    if iterations <= 0 or prev_mask.max() == 0:
        return prev_mask
    cross = np.array([[0, 1, 0],
                      [1, 1, 1],
                      [0, 1, 0]], dtype=bool)
    grown = binary_dilation(
        prev_mask > 0,
        structure=cross,
        iterations=iterations,
        mask=constraint > 0,  # constrains where dilation can go
    )
    return grown.astype(np.float32)


# ======================================================================
# Main entry point
# ======================================================================

def automask_plane(
    residual: Plane,
    sidelobe_level: float,
    beam_area_pix: float,
    beam_sigma_pix: tuple[float, float],
    cfg: AutoMaskConfig,
    state: AutoMaskState,
    pb: Plane | None = None,
    pblimit: float = 0.2,
) -> Plane:
    """Compute the auto-multithresh mask for a single 2-D plane.

    This is the numpy equivalent of
    ``SDMaskHandler::autoMaskByMultiThreshold`` operating on one
    (pol, chan) slice.

    Args:
        residual: 2-D residual image (float32).
        sidelobe_level: PSF sidelobe level (scalar, from ``getPSFSidelobeLevel``).
        beam_area_pix: Restoring beam area in pixels.
        beam_sigma_pix: ``(sigma_y, sigma_x)`` of the restoring beam in pixels
            (for Gaussian smoothing).
        cfg: Automasking parameters.
        state: Mutable per-channel state (updated in-place).
        pb: Primary beam image (same shape as *residual*).  If given,
            regions with ``pb < pblimit`` are excluded.
        pblimit: PB cutoff for the mask region.

    Returns:
        The updated binary mask (float32, 0/1).
    """
    # ---- PB-mask --------------------------------------------------------
    if pb is not None:
        pb_mask = (pb >= pblimit)
    else:
        pb_mask = np.ones(residual.shape, dtype=bool)

    # Apply PB mask to residual for statistics
    masked_residual = np.where(pb_mask, residual, 0.0).astype(np.float32)

    # ---- Stage 0: statistics & thresholds --------------------------------
    absmax, median, rms, _ = _plane_stats(
        masked_residual[pb_mask] if pb_mask.any() else masked_residual,
        fastnoise=cfg.fastnoise,
        prev_mask=(state.prevmask[pb_mask] if (state.prevmask is not None
                   and pb_mask.any()) else state.prevmask),
    )

    # Nothing to mask if the residual is effectively zero
    if absmax == 0:
        log.info('automask iter %d: residual is zero — returning empty mask',
                 state.iteration)
        state.prevmask = np.zeros(residual.shape, dtype=np.float32)
        state.iteration += 1
        return state.prevmask

    sidelobe_thresh = sidelobe_level * cfg.sidelobethreshold * absmax + median
    noise_thresh = cfg.noisethreshold * rms + median
    low_noise_thresh = cfg.lownoisethreshold * rms + median
    mask_threshold = max(sidelobe_thresh, noise_thresh)
    threshold_source = ('sidelobe' if sidelobe_thresh >= noise_thresh
                        else 'noise')

    log.info(
        'automask iter %d: absmax=%.4g  median=%.4g  rms=%.4g  '
        'sidelobe_thresh=%.4g  noise_thresh=%.4g  '
        'mask_thresh=%.4g (%s-limited)',
        state.iteration, absmax, median, rms,
        sidelobe_thresh, noise_thresh, mask_threshold, threshold_source,
    )

    # ---- Stage 1: threshold mask + prune ---------------------------------
    threshold_mask = (masked_residual >= mask_threshold).astype(np.float32)
    n_thresh_raw = int(threshold_mask.sum())
    prune_size = cfg.minbeamfrac * beam_area_pix
    if prune_size > 0:
        threshold_mask = _prune_regions(threshold_mask, prune_size)
    n_thresh_pruned = int(threshold_mask.sum())
    log.info('automask iter %d: threshold → %d pix, after prune → %d pix '
             '(prune_size=%.1f)',
             state.iteration, n_thresh_raw, n_thresh_pruned, prune_size)

    # ---- Stage 2: smooth + binarise --------------------------------------
    smoothed_mask = _smooth_and_cut(
        threshold_mask, beam_sigma_pix, cfg.smoothfactor, cfg.cutthreshold,
    )
    log.info('automask iter %d: smooth+cut → %d pix',
             state.iteration, int(smoothed_mask.sum()))

    # ---- Stage 3: grow (binary dilation) — skip on first iteration -------
    grown_mask = np.zeros_like(residual)
    if state.iteration > 0 and state.prevmask is not None and cfg.growiterations > 0:
        # Constraint: residual above low-noise threshold
        low_mask_thresh = max(sidelobe_thresh, low_noise_thresh)
        constraint = (masked_residual >= low_mask_thresh).astype(np.float32)
        n_constraint = int(constraint.sum())

        grown_mask = _grow_mask(state.prevmask, constraint, cfg.growiterations)
        n_grown_raw = int(grown_mask.sum())

        # Prune the grown mask
        if cfg.dogrowprune and prune_size > 0:
            grown_mask = _prune_regions(grown_mask, prune_size)

        # Smooth the grown mask
        grown_mask = _smooth_and_cut(
            grown_mask, beam_sigma_pix, cfg.smoothfactor, cfg.cutthreshold,
        )
        log.info('automask iter %d: grow (%d iters, %d constraint pix) → '
                 '%d pix raw, %d pix after prune+smooth',
                 state.iteration, cfg.growiterations, n_constraint,
                 n_grown_raw, int(grown_mask.sum()))
    else:
        log.info('automask iter %d: grow skipped (first iteration)',
                 state.iteration)

    # ---- Stage 4: combine ------------------------------------------------
    if state.posmask is None:
        state.posmask = np.zeros_like(residual)

    posmask = np.maximum(state.posmask, np.maximum(smoothed_mask, grown_mask))

    # Apply PB constraint
    posmask = np.where(pb_mask, posmask, 0.0).astype(np.float32)
    state.posmask = posmask

    # ---- Stage 5: negative mask (optional) --------------------------------
    neg_mask = np.zeros_like(residual)
    if cfg.negativethreshold > 0:
        neg_thresh_val = cfg.negativethreshold * rms
        sidelobe_thresh_no_offset = sidelobe_level * cfg.sidelobethreshold * absmax
        neg_mask_threshold = -(max(sidelobe_thresh_no_offset, neg_thresh_val)) + median

        neg_threshold_mask = (masked_residual <= neg_mask_threshold).astype(np.float32)
        neg_mask = _smooth_and_cut(
            neg_threshold_mask, beam_sigma_pix, cfg.smoothfactor, cfg.cutthreshold,
        )
        log.info('automask iter %d: negative mask → %d pix '
                 '(neg_thresh=%.4g)',
                 state.iteration, int(neg_mask.sum()), neg_mask_threshold)

    # ---- Stage 6: final combination -------------------------------------
    final_mask: Plane
    if state.prevmask is not None:
        final_mask = np.maximum(state.prevmask, np.maximum(posmask, neg_mask))
    else:
        final_mask = np.maximum(posmask, neg_mask)

    # Enforce PB region
    final_mask = np.where(pb_mask, final_mask, 0.0).astype(np.float32)

    # ---- Update state ----------------------------------------------------
    # Check percent change for early-skip optimisation
    change_pct = 0.0
    if state.prevmask is not None:
        total_pix = float(pb_mask.sum()) if pb_mask.any() else float(residual.size)
        if total_pix > 0:
            change_pct = 100.0 * float(np.sum(np.abs(final_mask - state.prevmask))) / total_pix
            if cfg.minpercentchange > 0 and change_pct < cfg.minpercentchange:
                state.skip = True
                log.info('automask iter %d: mask change %.2f%% < '
                         'minpercentchange %.2f%% — flagging skip',
                         state.iteration, change_pct, cfg.minpercentchange)

    state.prevmask = final_mask.copy()
    state.iteration += 1

    npix = int(final_mask.sum())
    total = int(pb_mask.sum()) if pb_mask.any() else residual.size
    pct = 100.0 * npix / total if total > 0 else 0.0
    log.info('automask iter %d: final mask %d / %d pix (%.2f%%), '
             'change from prev %.2f%%',
             state.iteration - 1, npix, total, pct, change_pct)

    return final_mask


# ======================================================================
# Helper: extract beam info from CASA image
# ======================================================================

def beam_info_from_image(imagename: str) -> tuple[float, float, tuple[float, float]]:
    """Read beam area and sigma from a CASA image.

    Returns:
        ``(beam_area_pix, sidelobe_level, (sigma_y_pix, sigma_x_pix))``
    """
    import casatools as ct

    ia = ct.image()
    try:
        ia.open(imagename + '.psf')
        beam = ia.restoringbeam()
        cs = ia.coordsys()
        incr = cs.increment(type='direction', format='n')['numeric']
        cell_rad = abs(incr[0])  # radians per pixel
        cs.done()

        # Beam FWHM in pixels
        major_pix = beam['major']['value']
        minor_pix = beam['minor']['value']
        # Convert from beam units to pixel units
        if beam['major']['unit'] == 'arcsec':
            major_pix /= (cell_rad * 206264.80625)
            minor_pix /= (cell_rad * 206264.80625)
        elif beam['major']['unit'] == 'rad':
            major_pix /= cell_rad
            minor_pix /= cell_rad

        # FWHM → sigma: sigma = FWHM / (2 * sqrt(2 * ln(2)))
        fwhm_to_sigma = 1.0 / (2.0 * np.sqrt(2.0 * np.log(2.0)))
        sigma_y = major_pix * fwhm_to_sigma
        sigma_x = minor_pix * fwhm_to_sigma

        # Beam area in pixels = pi * major * minor / (4 * ln(2))
        beam_area_pix = np.pi * major_pix * minor_pix / (4.0 * np.log(2.0))

        ia.close()

        # Sidelobe level — matches SIImageStore::getPSFSidelobeLevel().
        #
        # C++ creates a Gaussian PSF model per channel, computes
        #   delobed = PSF - GaussianModel
        # and returns max(|min(PSF)|, max(delobed)).
        # A plain central-box exclusion misses sidelobes close to the main
        # lobe, systematically under-estimating the threshold and inflating
        # the mask compared to the C++ automask.
        ia.open(imagename + '.psf')
        psf_data = ia.getchunk()
        ia.close()
        psf_plane = psf_data[:, :, 0, 0].astype(np.float32)
        peak = psf_plane.max()
        if peak > 0:
            psf_norm = psf_plane / peak
        else:
            psf_norm = psf_plane
        cy, cx = psf_norm.shape[0] // 2, psf_norm.shape[1] // 2
        delta = np.zeros_like(psf_norm)
        delta[cy, cx] = 1.0
        gaussian_model = gaussian_filter(delta, sigma=(sigma_y, sigma_x))
        g_peak = float(gaussian_model.max())
        if g_peak > 0:
            gaussian_model /= g_peak
        delobed = psf_norm - gaussian_model
        sidelobe_level = float(max(
            abs(float(np.min(psf_norm))),
            float(np.max(delobed)),
        ))

        return beam_area_pix, sidelobe_level, (sigma_y, sigma_x)
    finally:
        ia.done()


def read_plane(imagename: str) -> Plane:
    """Read a single-channel CASA image into a 2-D numpy array."""
    import casatools as ct

    ia = ct.image()
    try:
        ia.open(imagename)
        data = ia.getchunk()
        ia.close()
        # Shape is (nx, ny, npol, nchan) — squeeze to 2D
        return data[:, :, 0, 0].astype(np.float32)
    finally:
        ia.done()


def write_plane(imagename: str, data: Plane) -> None:
    """Write a 2-D numpy array to an existing single-channel CASA image."""
    import casatools as ct

    ia = ct.image()
    try:
        ia.open(imagename)
        shape = ia.shape()
        # Reshape back to CASA's (nx, ny, npol, nchan) convention
        out = data.reshape(shape[0], shape[1], 1, 1).astype(np.float32)
        ia.putchunk(out)
        ia.close()
    finally:
        ia.done()
