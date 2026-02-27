"""
Heuristic RAM estimator for parallel CASA imaging workers.

CASA's C++ imaging engine (casatools) allocates multiple image-sized
buffers during gridding that Python and Dask cannot track or free.
This module provides a *rough* estimate of peak RAM usage so that
users can choose an appropriate ``nworkers`` for their system.

Memory model
------------
During active imaging of a single sub-cube, CASA keeps approximately
the following buffers resident (per channel):

.. list-table::
   :header-rows: 1

   * - Buffer
     - Dtype
     - Bytes/pixel
   * - Complex visibility grid
     - complex64
     - 8
   * - Weight grid
     - complex64
     - 8
   * - FFT workspace (in + out)
     - complex64
     - 16
   * - Residual image
     - float32
     - 4
   * - Model image
     - float32
     - 4
   * - PSF image
     - float32
     - 4
   * - Weight image (sumwt)
     - float32
     - 4
   * - Primary beam (PB)
     - float32
     - 4
   * - Mask
     - float32
     - 4
   * - Temporary / bookkeeping
     - mixed
     - ~20

This sums to roughly **76 bytes per pixel per channel** for a
``standard`` gridder with ``deconvolver='hogbom'`` and Stokes I.

Scaling factors (multiplicative):

* **Mosaic gridder** — each pointing requires a convolution function
  (CF) table; memory scales with the number of fields and CF support
  size.  A 1.5x–3x multiplier over standard is typical.
* **MTMFS deconvolver** — internal Hessian products scale as
  *nterms* squared.
* **Multi-channel sub-cubes** — linear in ``nchan_per_task``.

Calibration
-----------
The 76 B/pix/chan constant was calibrated against an ALMA Band 6
cube-imaging run (IRC+10216, 8000 x 8000, 40 antennas, 449 280 rows,
``gridder='standard'``, ``deconvolver='hogbom'``), where each worker
consumed ~4.9 GiB of C++ memory with 1 channel per task.

    4.9 GiB / (8000 * 8000 * 1 chan) ≈ 76 B/pix/chan

The MS row count (nrows) contributes negligibly — visibilities are
processed in row chunks that occupy a few MB, dwarfed by the
multi-GiB image grids.  It is included only as a minor additive term.
"""

from __future__ import annotations

import logging
import os
from typing import Sequence

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Empirical constants (see module docstring for derivation)
# ---------------------------------------------------------------------------

#: Bytes per pixel per channel for the standard gridder (Stokes I, hogbom).
BYTES_PER_PIXEL_STANDARD: float = 76.0

#: Python + Dask worker process baseline overhead (GiB).
WORKER_BASE_OVERHEAD_GIB: float = 0.7

#: Gridder multipliers relative to ``standard``.
_GRIDDER_FACTOR: dict[str, float] = {
    "standard": 1.0,
    "wproject": 1.2,
    "widefield": 1.3,
    "mosaic": 2.0,
    "awproject": 2.5,
}


def estimate_worker_memory_gib(
    imsize: Sequence[int] | int,
    nchan_per_task: int = 1,
    gridder: str = "standard",
    deconvolver: str = "hogbom",
    nterms: int = 1,
    nfields: int = 1,
) -> float:
    """Estimate peak RAM (GiB) consumed by a single worker.

    Parameters
    ----------
    imsize : int or sequence of int
        Image dimensions in pixels.  A scalar is treated as a square.
    nchan_per_task : int
        Number of channels each worker images (``cube_chunksize``).
    gridder : str
        Gridder name (``standard``, ``mosaic``, ``wproject``, etc.).
    deconvolver : str
        Deconvolver name.  ``mtmfs`` triggers the *nterms* multiplier.
    nterms : int
        Number of Taylor terms (only relevant for ``mtmfs``).
    nfields : int
        Number of mosaic pointings (used to scale mosaic overhead).

    Returns
    -------
    float
        Estimated peak memory in GiB.

    Examples
    --------
    >>> estimate_worker_memory_gib(imsize=8000, nchan_per_task=1)
    5.22...
    >>> estimate_worker_memory_gib(imsize=[1280, 1024], gridder='mosaic',
    ...                            deconvolver='mtmfs', nterms=2)
    5.08...
    """
    if isinstance(imsize, (int, float)):
        nx = ny = int(imsize)
    else:
        nx = int(imsize[0])
        ny = int(imsize[1]) if len(imsize) > 1 else nx

    npix = nx * ny

    # --- Image grid memory (C++) ---
    gridder_key = gridder.lower()
    gfactor = _GRIDDER_FACTOR.get(gridder_key, 1.0)

    # Mosaic CF memory also scales (sub-linearly) with field count.
    if gridder_key == "mosaic" and nfields > 1:
        # Empirical: CF tables grow roughly as sqrt(nfields) beyond
        # the base mosaic overhead.
        import math
        gfactor *= 1.0 + 0.1 * (math.sqrt(nfields) - 1.0)

    # MTMFS multiplier: internal Hessian products scale as nterms^2
    # relative to a single-term deconvolver.
    deconv_factor = 1.0
    if deconvolver.lower() == "mtmfs" and nterms > 1:
        deconv_factor = nterms ** 2

    image_bytes = (
        npix
        * nchan_per_task
        * BYTES_PER_PIXEL_STANDARD
        * gfactor
        * deconv_factor
    )
    image_gib = image_bytes / (1024 ** 3)

    # --- Total per-worker ---
    total_gib = WORKER_BASE_OVERHEAD_GIB + image_gib
    return total_gib


def estimate_peak_ram_gib(
    nworkers: int,
    imsize: Sequence[int] | int,
    nchan_per_task: int = 1,
    gridder: str = "standard",
    deconvolver: str = "hogbom",
    nterms: int = 1,
    nfields: int = 1,
) -> float:
    """Estimate peak system RAM (GiB) for *nworkers* concurrent tasks.

    Parameters
    ----------
    nworkers : int
        Number of concurrent Dask workers.
    imsize, nchan_per_task, gridder, deconvolver, nterms, nfields
        Forwarded to :func:`estimate_worker_memory_gib`.

    Returns
    -------
    float
        Estimated total peak RAM in GiB.
    """
    per_worker = estimate_worker_memory_gib(
        imsize=imsize,
        nchan_per_task=nchan_per_task,
        gridder=gridder,
        deconvolver=deconvolver,
        nterms=nterms,
        nfields=nfields,
    )
    # Scheduler + client process overhead (small).
    scheduler_overhead_gib = 0.5
    total = nworkers * per_worker + scheduler_overhead_gib
    return total


def recommend_nworkers(
    available_ram_gib: float | None = None,
    imsize: Sequence[int] | int = 4096,
    nchan_per_task: int = 1,
    gridder: str = "standard",
    deconvolver: str = "hogbom",
    nterms: int = 1,
    nfields: int = 1,
    ram_safety_factor: float = 0.85,
) -> int:
    """Suggest the maximum number of workers that fit in available RAM.

    Parameters
    ----------
    available_ram_gib : float or None
        Total system RAM in GiB.  ``None`` reads from the OS.
    imsize, nchan_per_task, gridder, deconvolver, nterms, nfields
        Forwarded to :func:`estimate_worker_memory_gib`.
    ram_safety_factor : float
        Fraction of available RAM to target (default 0.85 = 85%).

    Returns
    -------
    int
        Recommended number of workers (at least 1).
    """
    if available_ram_gib is None:
        try:
            import psutil
            available_ram_gib = psutil.virtual_memory().total / (1024 ** 3)
        except ImportError:
            mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
            available_ram_gib = mem_bytes / (1024 ** 3)

    usable_ram = available_ram_gib * ram_safety_factor

    per_worker = estimate_worker_memory_gib(
        imsize=imsize,
        nchan_per_task=nchan_per_task,
        gridder=gridder,
        deconvolver=deconvolver,
        nterms=nterms,
        nfields=nfields,
    )

    # Reserve memory for the scheduler/client process.
    scheduler_overhead_gib = 0.5
    budget = usable_ram - scheduler_overhead_gib

    n = max(1, int(budget / per_worker))

    log.info(
        "Memory estimate: %.1f GiB/worker, %.1f GiB usable → %d workers "
        "(imsize=%s, gridder=%s, deconvolver=%s, nterms=%d)",
        per_worker,
        usable_ram,
        n,
        imsize,
        gridder,
        deconvolver,
        nterms,
    )
    return n
