"""Pure-numpy auto-multithresh masking.

Re-implements CASA's ``SDMaskHandler::autoMaskByMultiThreshold`` algorithm
using numpy/scipy, eliminating:

* Repeated full-cube copy to TempImage
* Per-plane TempImage allocations (~8 per chan per iteration)
* casacore TableCache interactions (mask0 subtable bug)
* Multiple statistics passes

The algorithm faithfully reproduces the six stages of the C++ implementation:
threshold → prune → smooth → grow → combine → negative mask.

Memory optimisation notes:
    All intermediate masks use ``np.bool_`` (1 byte/pixel) instead of
    ``float32`` (4 bytes/pixel), giving a **4× footprint reduction** on
    mask arrays.  Only the final mask returned to the caller is float32
    (CASA image format).  ``AutoMaskState.posmask`` and ``.prevmask``
    are stored as ``bool_`` between iterations.  When ``pb`` is *None*
    no PB-mask array is allocated at all (``pb_mask = None`` sentinel).

References:
    SDMaskHandler.cc  ``autoMaskByMultiThreshold()``  (CASA 6.x)
    Kepley et al. 2020, PASP 132, 024505
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TypeAlias

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
Plane: TypeAlias = NDArray[np.float32]


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

    posmask: NDArray[np.bool_] | None = None  # accumulated positive mask
    prevmask: NDArray[np.bool_] | None = None  # mask from previous iteration
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
    prev_mask: NDArray[np.bool_] | None = None,
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
    mask: NDArray,
    min_size: float,
) -> NDArray[np.bool_]:
    """Remove connected regions smaller than *min_size* pixels.

    Equivalent to ``SDMaskHandler::YAPruneRegions``.

    Returns:
        Boolean mask with small regions removed.
    """
    if min_size <= 0:
        return np.asarray(mask, dtype=np.bool_)
    bool_mask = np.asarray(mask > 0 if mask.dtype != np.bool_ else mask,
                           dtype=np.bool_)
    labeled, n_features = label(bool_mask)
    if n_features == 0:
        return bool_mask
    component_sizes = ndi_sum(bool_mask, labeled, range(1, n_features + 1))
    # Build a look-up table: label → keep?
    keep = np.zeros(n_features + 1, dtype=np.bool_)
    n_kept = 0
    for i, size in enumerate(component_sizes, start=1):
        if size >= min_size:
            keep[i] = True
            n_kept += 1
    log.debug('prune: %d / %d regions kept (min_size=%.1f pix)',
              n_kept, n_features, min_size)
    return keep[labeled]


def _beam_sigma_to_axis(
    sigma_major: float,
    sigma_minor: float,
    pa_rad: float = 0.0,
) -> tuple[float, float]:
    """Project beam (major, minor, PA) onto per-axis Gaussian sigmas.

    PA follows the CASA/IAU convention (measured East from North, i.e.
    counter-clockwise from the Dec axis).  ``getchunk()`` returns data
    with axis 0 = RA and axis 1 = Dec, so:

    * PA = 0   → major along Dec (axis 1), minor along RA (axis 0)
    * PA = 90° → major along RA  (axis 0), minor along Dec (axis 1)

    The projection uses the RMS width of the rotated ellipse along
    each image axis:

        σ_axis0² = σ_major² sin²(PA) + σ_minor² cos²(PA)
        σ_axis1² = σ_major² cos²(PA) + σ_minor² sin²(PA)

    Returns:
        ``(sigma_axis0, sigma_axis1)``
    """
    c = np.cos(pa_rad)
    s = np.sin(pa_rad)
    s2_maj = sigma_major ** 2
    s2_min = sigma_minor ** 2
    sigma_ax0 = np.sqrt(s2_maj * s * s + s2_min * c * c)
    sigma_ax1 = np.sqrt(s2_maj * c * c + s2_min * s * s)
    return float(sigma_ax0), float(sigma_ax1)


def _make_gaussian_psf(
    shape: tuple[int, int],
    sigma_major: float,
    sigma_minor: float,
    pa_rad: float = 0.0,
) -> NDArray[np.float32]:
    """Build a peak-normalised 2-D rotated Gaussian PSF model.

    This mirrors the C++ ``MakeGaussianPSF`` used by
    ``SIImageStore::getPSFSidelobeLevel()`` to compute the delobed
    sidelobe level.

    The Gaussian is centred at ``(shape[0]//2, shape[1]//2)`` using the
    CASA axis convention (axis 0 = RA, axis 1 = Dec, PA from North
    toward East).

    Memory-efficient: builds coordinate grids in-place and avoids
    intermediate full-size arrays.

    Returns:
        float32 array of *shape* with peak = 1.0.
    """
    cy, cx = shape[0] // 2, shape[1] // 2

    # Rotation: PA is from Dec (axis 1) toward RA (axis 0), CCW.
    cos_pa = np.cos(pa_rad)
    sin_pa = np.sin(pa_rad)

    # Row (axis-0) and column (axis-1) offsets
    rows = np.arange(shape[0], dtype=np.float32) - cy
    cols = np.arange(shape[1], dtype=np.float32) - cx

    # Rotated coordinates — broadcast without full meshgrid
    #   u =  d0 * sin(PA) + d1 * cos(PA)   → along major axis
    #   v = -d0 * cos(PA) + d1 * sin(PA)   → along minor axis
    # Compute (u/σ_major)² + (v/σ_minor)² in-place.
    inv_s2_maj = 1.0 / (2.0 * sigma_major ** 2) if sigma_major > 0 else 0.0
    inv_s2_min = 1.0 / (2.0 * sigma_minor ** 2) if sigma_minor > 0 else 0.0

    # Pre-compute row contribution terms (shape[0],)
    r_sin = rows * np.float32(sin_pa)
    r_cos = rows * np.float32(cos_pa)

    out = np.empty(shape, dtype=np.float32)
    for j, cj in enumerate(cols):
        u = r_sin + np.float32(cj * cos_pa)   # (shape[0],)
        v = np.float32(cj * sin_pa) - r_cos   # (shape[0],)
        np.multiply(u, u, out=u)
        u *= np.float32(inv_s2_maj)
        np.multiply(v, v, out=v)
        v *= np.float32(inv_s2_min)
        u += v
        np.negative(u, out=u)
        np.exp(u, out=out[:, j])

    peak = float(out[cy, cx])
    if peak > 0:
        out /= np.float32(peak)
    return out


def _smooth_and_cut(
    mask: NDArray,
    beam_sigma_pix: tuple[float, float],
    smooth_factor: float,
    cut_threshold: float,
    beam_pa_rad: float = 0.0,
) -> NDArray[np.bool_]:
    """Gaussian-smooth the mask and binarise at *cut_threshold × max*.

    Equivalent to ``SDMaskHandler::convolveMask`` + ``makeMaskByPerChanThreshold``
    on the smoothed result.

    When *beam_pa_rad* is non-zero the smoothing kernel sigmas are
    projected onto the image axes via :func:`_beam_sigma_to_axis`,
    correctly handling the beam position angle.

    Returns:
        Boolean mask.
    """
    # Fast path: entirely empty mask
    if not np.any(mask):
        return np.zeros(mask.shape, dtype=np.bool_)
    # Project beam sigmas onto image axes (handles PA)
    s0, s1 = _beam_sigma_to_axis(
        beam_sigma_pix[0], beam_sigma_pix[1], beam_pa_rad,
    )
    sigma = (s0 * smooth_factor, s1 * smooth_factor)
    smoothed = gaussian_filter(
        mask.astype(np.float32) if mask.dtype != np.float32 else mask,
        sigma=sigma,
    )
    peak = float(smoothed.max())
    if peak <= 0:
        return np.zeros(mask.shape, dtype=np.bool_)
    cut = cut_threshold * peak
    result = smoothed >= cut
    del smoothed
    return result


def _grow_mask(
    prev_mask: NDArray,
    constraint: NDArray,
    iterations: int,
) -> NDArray[np.bool_]:
    """Binary-dilate *prev_mask* within *constraint* region.

    Uses a 3×3 cross structuring element (4-connected), matching
    ``SDMaskHandler::binaryDilation``.

    Returns:
        Boolean mask.
    """
    if iterations <= 0 or not np.any(prev_mask):
        return np.asarray(prev_mask > 0 if prev_mask.dtype != np.bool_
                          else prev_mask, dtype=np.bool_)
    cross = np.array([[0, 1, 0],
                      [1, 1, 1],
                      [0, 1, 0]], dtype=bool)
    prev_bool = prev_mask if prev_mask.dtype == np.bool_ else prev_mask > 0
    con_bool = constraint if constraint.dtype == np.bool_ else constraint > 0
    return binary_dilation(
        prev_bool,
        structure=cross,
        iterations=iterations,
        mask=con_bool,
    )


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
    beam_pa_rad: float = 0.0,
) -> Plane:
    """Compute the auto-multithresh mask for a single 2-D plane.

    This is the numpy equivalent of
    ``SDMaskHandler::autoMaskByMultiThreshold`` operating on one
    (pol, chan) slice.

    Memory strategy:
        * All intermediate masks are ``np.bool_`` (1 byte/pixel).
        * ``pb_mask`` is ``None`` when no PB is provided (zero alloc).
        * ``state.posmask`` / ``state.prevmask`` stored as ``bool_``.
        * In-place ``|=`` replaces ``np.maximum`` chains.
        * Only the final return value is cast to ``float32`` for CASA I/O.

    Args:
        residual: 2-D residual image (float32).
        sidelobe_level: PSF sidelobe level (scalar, from ``getPSFSidelobeLevel``).
        beam_area_pix: Restoring beam area in pixels.
        beam_sigma_pix: ``(sigma_major, sigma_minor)`` of the restoring beam
            in pixels (for Gaussian smoothing).
        cfg: Automasking parameters.
        state: Mutable per-channel state (updated in-place).
        pb: Primary beam image (same shape as *residual*).  If given,
            regions with ``pb < pblimit`` are excluded.
        pblimit: PB cutoff for the mask region.
        beam_pa_rad: Beam position angle in radians (East from North).

    Returns:
        The updated binary mask (float32, 0/1).
    """
    # ---- PB-mask --------------------------------------------------------
    # When no PB is supplied, pb_mask stays None — zero allocation.
    pb_mask: NDArray[np.bool_] | None = None
    if pb is not None:
        pb_mask = pb >= pblimit

    # ---- Stage 0: statistics & thresholds --------------------------------
    if pb_mask is not None:
        stats_data = residual[pb_mask]
        stats_prev = (state.prevmask[pb_mask]
                      if state.prevmask is not None else None)
    else:
        stats_data = residual
        stats_prev = state.prevmask

    absmax, median, rms, _ = _plane_stats(
        stats_data,
        fastnoise=cfg.fastnoise,
        prev_mask=stats_prev,
    )
    del stats_data, stats_prev

    # Nothing to mask if the residual is effectively zero
    if absmax == 0:
        log.info('automask iter %d: residual is zero — returning empty mask',
                 state.iteration)
        state.prevmask = np.zeros(residual.shape, dtype=np.bool_)
        state.iteration += 1
        return np.zeros(residual.shape, dtype=np.float32)

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
    # Apply PB mask inline: only threshold where PB is valid
    threshold_mask: NDArray[np.bool_]
    if pb_mask is not None:
        threshold_mask = pb_mask & (residual >= mask_threshold)
    else:
        threshold_mask = residual >= mask_threshold
    n_thresh_raw = int(np.count_nonzero(threshold_mask))
    prune_size = cfg.minbeamfrac * beam_area_pix
    if prune_size > 0:
        threshold_mask = _prune_regions(threshold_mask, prune_size)
    n_thresh_pruned = int(np.count_nonzero(threshold_mask))
    log.info('automask iter %d: threshold → %d pix, after prune → %d pix '
             '(prune_size=%.1f)',
             state.iteration, n_thresh_raw, n_thresh_pruned, prune_size)

    # ---- Stage 2: smooth + binarise (returns bool) -----------------------
    smoothed_mask = _smooth_and_cut(
        threshold_mask, beam_sigma_pix, cfg.smoothfactor, cfg.cutthreshold,
        beam_pa_rad=beam_pa_rad,
    )
    del threshold_mask
    log.info('automask iter %d: smooth+cut → %d pix',
             state.iteration, int(np.count_nonzero(smoothed_mask)))

    # ---- Stage 3: grow (binary dilation) — skip on first iteration -------
    grown_mask: NDArray[np.bool_] | None = None
    if state.iteration > 0 and state.prevmask is not None and cfg.growiterations > 0:
        # Constraint: residual above low-noise threshold (bool)
        low_mask_thresh = max(sidelobe_thresh, low_noise_thresh)
        if pb_mask is not None:
            constraint = pb_mask & (residual >= low_mask_thresh)
        else:
            constraint = residual >= low_mask_thresh
        n_constraint = int(np.count_nonzero(constraint))

        grown_mask = _grow_mask(state.prevmask, constraint, cfg.growiterations)
        del constraint
        n_grown_raw = int(np.count_nonzero(grown_mask))

        # Prune the grown mask
        if cfg.dogrowprune and prune_size > 0:
            grown_mask = _prune_regions(grown_mask, prune_size)

        # Smooth the grown mask
        grown_mask = _smooth_and_cut(
            grown_mask, beam_sigma_pix, cfg.smoothfactor, cfg.cutthreshold,
            beam_pa_rad=beam_pa_rad,
        )
        log.info('automask iter %d: grow (%d iters, %d constraint pix) → '
                 '%d pix raw, %d pix after prune+smooth',
                 state.iteration, cfg.growiterations, n_constraint,
                 n_grown_raw, int(np.count_nonzero(grown_mask)))
    else:
        log.info('automask iter %d: grow skipped (first iteration)',
                 state.iteration)

    # ---- Stage 4: combine (in-place bool OR) -----------------------------
    if state.posmask is None:
        posmask = smoothed_mask.copy()
    else:
        posmask = state.posmask | smoothed_mask
    del smoothed_mask
    if grown_mask is not None:
        posmask |= grown_mask
        del grown_mask

    # Apply PB constraint
    if pb_mask is not None:
        posmask &= pb_mask
    state.posmask = posmask  # stored as bool

    # ---- Stage 5: negative mask (optional) --------------------------------
    neg_mask: NDArray[np.bool_] | None = None
    if cfg.negativethreshold > 0:
        neg_thresh_val = cfg.negativethreshold * rms
        sidelobe_thresh_no_offset = sidelobe_level * cfg.sidelobethreshold * absmax
        neg_mask_threshold = -(max(sidelobe_thresh_no_offset, neg_thresh_val)) + median

        if pb_mask is not None:
            neg_threshold_mask = pb_mask & (residual <= neg_mask_threshold)
        else:
            neg_threshold_mask = residual <= neg_mask_threshold
        neg_mask = _smooth_and_cut(
            neg_threshold_mask, beam_sigma_pix, cfg.smoothfactor,
            cfg.cutthreshold, beam_pa_rad=beam_pa_rad,
        )
        del neg_threshold_mask
        log.info('automask iter %d: negative mask → %d pix '
                 '(neg_thresh=%.4g)',
                 state.iteration, int(np.count_nonzero(neg_mask)),
                 neg_mask_threshold)

    # ---- Stage 6: final combination (in-place bool OR) -------------------
    if state.prevmask is not None:
        final_mask = state.prevmask | posmask
    else:
        final_mask = posmask.copy()
    if neg_mask is not None:
        final_mask |= neg_mask
        del neg_mask

    # Enforce PB region
    if pb_mask is not None:
        final_mask &= pb_mask

    # ---- Update state ----------------------------------------------------
    # Check percent change for early-skip optimisation
    change_pct = 0.0
    if state.prevmask is not None:
        n_changed = int(np.count_nonzero(final_mask != state.prevmask))
        total_pix = (int(np.count_nonzero(pb_mask))
                     if pb_mask is not None else residual.size)
        if total_pix > 0:
            change_pct = 100.0 * n_changed / total_pix
            if cfg.minpercentchange > 0 and change_pct < cfg.minpercentchange:
                state.skip = True
                log.info('automask iter %d: mask change %.2f%% < '
                         'minpercentchange %.2f%% — flagging skip',
                         state.iteration, change_pct, cfg.minpercentchange)

    state.prevmask = final_mask  # stored as bool (no copy needed — we own it)
    state.iteration += 1

    npix = int(np.count_nonzero(final_mask))
    total = (int(np.count_nonzero(pb_mask))
             if pb_mask is not None else residual.size)
    pct = 100.0 * npix / total if total > 0 else 0.0
    log.info('automask iter %d: final mask %d / %d pix (%.2f%%), '
             'change from prev %.2f%%',
             state.iteration - 1, npix, total, pct, change_pct)

    return final_mask.astype(np.float32)


# ======================================================================
# Helper: extract beam info from CASA image
# ======================================================================

def beam_info_from_image(
    imagename: str,
) -> tuple[float, float, tuple[float, float], float]:
    """Read beam area, sidelobe level, sigma, and PA from a CASA image.

    Returns:
        ``(beam_area_pix, sidelobe_level, (sigma_major_pix, sigma_minor_pix), pa_rad)``
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

        # Position angle
        pa_deg = beam.get('positionangle', {}).get('value', 0.0)
        pa_unit = beam.get('positionangle', {}).get('unit', 'deg')
        pa_rad = float(np.deg2rad(pa_deg) if pa_unit == 'deg' else pa_deg)

        # FWHM → sigma: sigma = FWHM / (2 * sqrt(2 * ln(2)))
        fwhm_to_sigma = 1.0 / (2.0 * np.sqrt(2.0 * np.log(2.0)))
        sigma_major = major_pix * fwhm_to_sigma
        sigma_minor = minor_pix * fwhm_to_sigma

        # Beam area in pixels = pi * major * minor / (4 * ln(2))
        beam_area_pix = float(
            np.pi * major_pix * minor_pix / (4.0 * np.log(2.0))
        )

        ia.close()

        # Sidelobe level — matches SIImageStore::getPSFSidelobeLevel().
        #
        # Build an analytic rotated-Gaussian PSF model (with PA), then
        #   delobed = PSF − Gaussian_model
        #   sidelobe = max(|min(PSF)|, max(delobed))
        ia.open(imagename + '.psf')
        psf_data = ia.getchunk()
        ia.close()
        psf_plane = psf_data[:, :, 0, 0].astype(np.float32)
        del psf_data
        peak = float(psf_plane.max())
        if peak > 0:
            psf_plane /= np.float32(peak)

        # Record |min(PSF)| before subtracting the model in-place.
        psf_min_abs = abs(float(np.min(psf_plane)))

        gaussian_model = _make_gaussian_psf(
            psf_plane.shape, sigma_major, sigma_minor, pa_rad,
        )
        psf_plane -= gaussian_model  # delobed in-place
        del gaussian_model
        sidelobe_level = float(max(
            psf_min_abs, float(np.max(psf_plane)),
        ))
        del psf_plane

        return beam_area_pix, sidelobe_level, (sigma_major, sigma_minor), pa_rad
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
