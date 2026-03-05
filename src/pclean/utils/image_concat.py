"""Image concatenation utilities.

After parallel cube imaging each worker produces a sub-cube.  This
module concatenates them into the final output cube, mirroring the
``ia.imageconcat()`` call used in CASA's parallel cube imager.

Three concatenation modes are supported (via ``concat_mode`` in
:class:`~pclean.config.ClusterConfig`):

* **paged** (default): Pixel data are physically copied into a new
  self-contained CASA image.  Slower but fully independent of the
  subcubes after completion.
* **virtual** (``mode='nomovevirtual'``): The output image is a lightweight
  reference catalog that points at the original subcube files.  Near-instant
  but requires the subcubes to stay on disk (``keep_subcubes=True``).
* **movevirtual** (``mode='movevirtual'``): The subcube directories are
  renamed (moved) into the output image.  Near-instant on the same
  filesystem; the subcubes are consumed in the process.

When ``concat_mode='auto'`` (the default), the mode is derived from
``keep_subcubes``: ``True`` → virtual, ``False`` → paged.

When multiple extensions need concatenating (e.g. ``.image``, ``.residual``,
``.psf``, …), a ``ProcessPoolExecutor`` (``spawn`` start method) is used for
paged mode so that each subprocess gets its own casacore ``TableCache`` and
there is no shared C++ state between workers.  Virtual modes are run
sequentially because they are near-instant and write shared catalog metadata.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

log = logging.getLogger(__name__)

# casacore maintained a process-global `TableCache` (std::map) that is
# registered/looked-up on every table open.  A mutex was present from
# 2011 (commit 68d3f2d) until April 2021, when PR #1095 removed it as
# part of stripping all casacore-internal threading infrastructure
# (issue #896).  casacore >= 3.4.0 (CASA 6.x) therefore has NO mutex
# protecting the TableCache, making concurrent imageconcat() calls from
# threads a data race that causes a segfault (observed in CI with four
# threads in imageconcat() simultaneously on paged mode).
#
# ia.imageconcat() is a monolithic SWIG-wrapped C++ call with no
# internal thread guards: the table-cache registration and all pixel I/O
# are interleaved and cannot be split at the Python level.  We therefore
# serialise ALL modes through _tablelock, not just virtual modes.
_tablelock = threading.Lock()

_casatools = None


def _ct():
    global _casatools
    if _casatools is None:
        import casatools as ct

        _casatools = ct
    return _casatools


# ======================================================================
# Public
# ======================================================================


def concat_images(
    outimage: str,
    inimages: list[str],
    axis: int = -1,
    relax: bool = True,
    overwrite: bool = True,
    mode: str = 'paged',
) -> None:
    """Concatenate a list of CASA images along *axis*.

    Args:
        outimage: Path for the output concatenated image.
        inimages: Ordered list of input sub-images.
        axis: Axis to concatenate along (default -1 -> spectral).
        relax: Relax axis checks.
        overwrite: Overwrite *outimage* if it exists.
        mode: CASA imageconcat mode. ``'paged'`` (default) physically copies
            data. ``'nomovevirtual'`` creates a reference catalog (near-instant,
            but requires input images to stay on disk). ``'movevirtual'``
            creates a virtual concatenation by moving subcube directories into
            the output image.
    """
    ct = _ct()
    ia = ct.image()
    t0 = time.monotonic()
    try:
        # ia.imageconcat() is a monolithic SWIG-wrapped C++ call: the
        # casacore TableCache registration and all pixel I/O happen inside
        # a single function with no thread-safety guarantees.  Concurrent
        # calls from the ThreadPoolExecutor cause a segfault (observed in
        # CI: four threads all in imageconcat simultaneously).  Serialise
        # with _tablelock for all modes.
        with _tablelock:
            ia.imageconcat(
                outfile=outimage,
                infiles=inimages,
                axis=axis,
                relax=relax,
                overwrite=overwrite,
                tempclose=False,
                reorder=False,
                mode=mode,
            )
    finally:
        ia.done()
    elapsed = time.monotonic() - t0
    log.info(
        'Concatenated %d images → %s (mode=%s, %.1fs)',
        len(inimages),
        outimage,
        mode,
        elapsed,
    )


# Module-level worker function: must live at module scope to be picklable
# by ProcessPoolExecutor when using the 'spawn' start method.
def _concat_images_worker(args: tuple) -> str:
    """Subprocess entry-point for parallel extension concatenation."""
    outimage, inimages, mode = args
    concat_images(outimage, inimages, mode=mode)
    return outimage


def concat_subcubes(
    base_imagename: str,
    nparts: int,
    extensions: list[str] | None = None,
    mode: str = 'paged',
    max_workers: int = 4,
    # Deprecated — use *mode* instead.
    virtual: bool | None = None,
    # Private seam for tests: inject ThreadPoolExecutor to avoid subprocess
    # spawning while keeping mock-based assertions intact.
    _pool_cls=None,
) -> None:
    """Concatenate all standard image products from numbered sub-cubes.

    Products include ``.image``, ``.residual``, ``.psf``, etc.

    The *mode* parameter is forwarded directly to ``ia.imageconcat()``:

    * ``'paged'`` — pixel data are physically copied (default, always safe).
      Extensions are concatenated **in parallel** via ``ProcessPoolExecutor``
      (``spawn`` context) so each subprocess owns an independent casacore
      ``TableCache`` — true I/O parallelism, no shared C++ state.
    * ``'nomovevirtual'`` — lightweight reference catalog, near-instant but
      subcube files **must remain on disk**.  Run **sequentially** because
      virtual-catalog metadata is shared across calls.
    * ``'movevirtual'`` — renames subcubes into the output directory
      (near-instant, subcubes are consumed).  Also run sequentially.

    .. deprecated::
        The *virtual* parameter is deprecated.  Pass *mode* explicitly.

    Args:
        base_imagename: The original ``imagename`` (without ``.subcube.N``).
        nparts: Number of sub-cubes.
        extensions: Image extensions to concatenate.  Defaults to a standard set.
        mode: CASA ``imageconcat`` mode string.
        max_workers: Maximum parallel concatenation workers (paged mode only).
        virtual: **Deprecated.** ``True`` maps to ``mode='nomovevirtual'``.
    """
    # Backward compat: honour deprecated *virtual* flag if *mode* not overridden.
    if virtual is not None:
        import warnings

        warnings.warn(
            "concat_subcubes(virtual=...) is deprecated; use mode='nomovevirtual' "
            "or mode='paged' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if mode == 'paged':  # only override when caller did not set mode
            mode = 'nomovevirtual' if virtual else 'paged'
    if extensions is None:
        extensions = [
            '.image',
            '.residual',
            '.psf',
            '.model',
            '.pb',
            '.image.pbcor',
            '.mask',
            '.weight',
            '.sumwt',
        ]

    log.info(
        'Concatenating %d extensions (nparts=%d, mode=%s, max_workers=%d)',
        len(extensions),
        nparts,
        mode,
        max_workers,
    )
    t0 = time.monotonic()

    # Collect work items: (extension, list-of-input-files, output-file)
    work: list[tuple[str, list[str], str]] = []
    for ext in extensions:
        infiles = []
        for i in range(nparts):
            subname = f'{base_imagename}.subcube.{i}{ext}'
            if os.path.isdir(subname) or os.path.isfile(subname):
                infiles.append(subname)
        if not infiles:
            continue
        outfile = f'{base_imagename}{ext}'
        work.append((ext, infiles, outfile))

    if not work:
        log.warning('No subcube files found for concatenation')
        return

    failed: list[tuple[str, Exception]] = []
    worker_count = len(work)
    effective_workers = worker_count if max_workers <= 0 else min(max_workers, worker_count)

    _virtual = mode in ('nomovevirtual', 'movevirtual')
    if _virtual:
        # Virtual modes write shared casacore catalog metadata and are
        # near-instant — run sequentially to avoid catalog corruption.
        for ext, infiles, outfile in work:
            try:
                concat_images(outfile, infiles, mode=mode)
            except Exception as exc:  # pragma: no cover
                log.warning('Failed to concatenate %s: %s', ext, exc)
                failed.append((ext, exc))
    else:
        # Paged mode: spawn independent subprocesses so each gets its own
        # casacore TableCache — true parallelism with no shared C++ state.
        # ProcessPoolExecutor reuses workers across tasks so casatools is
        # imported once per worker, not once per extension.
        #
        # _pool_cls is a private seam for tests: pass ThreadPoolExecutor to
        # keep mock-based tests working without spawning real subprocesses.
        if _pool_cls is None:
            pool_factory = ProcessPoolExecutor
            pool_kwargs: dict = {
                'max_workers': effective_workers,
                'mp_context': multiprocessing.get_context('spawn'),
            }
        else:
            pool_factory = _pool_cls
            pool_kwargs = {'max_workers': effective_workers}

        with pool_factory(**pool_kwargs) as pool:
            future_to_ext = {
                pool.submit(_concat_images_worker, (outfile, infiles, mode)): ext
                for ext, infiles, outfile in work
            }
            for future in as_completed(future_to_ext):
                ext = future_to_ext[future]
                try:
                    future.result()
                except Exception as exc:
                    log.warning('Failed to concatenate %s: %s', ext, exc)
                    failed.append((ext, exc))

    elapsed = time.monotonic() - t0
    ok_count = len(work) - len(failed)
    log.info(
        'Concatenation complete: %d/%d extensions in %.1fs',
        ok_count,
        len(work),
        elapsed,
    )
    if failed:
        log.warning(
            'Failed extensions: %s',
            ', '.join(ext for ext, _ in failed),
        )
