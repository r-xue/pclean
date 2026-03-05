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
``.psf``, …), a ``ThreadPoolExecutor`` is used so that independent extensions
are processed in parallel, limited by ``max_workers``.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger(__name__)

# casacore maintained a process-global `TableCache` (std::map) that is
# registered/looked-up on every table open.  A mutex was present from
# 2011 (commit 68d3f2d) until April 2021, when PR #1095 removed it as
# part of stripping all casacore-internal threading infrastructure
# (issue #896).  casacore >= 3.4.0 (CASA 6.x) therefore has NO mutex
# protecting the TableCache, making concurrent imageconcat() calls from
# threads a data race.
#
# For *paged* mode we hold the lock only for the brief table-open phase
# (tempclose=False keeps handles alive, pixel I/O happens after release).
# For *virtual* modes the entire call must be serialised because the output
# reference-catalog metadata is written incrementally and shared.
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
        mode: CASA imageconcat mode.  ``'paged'`` (default) physically
            copies data.  ``'nomovevirtual'`` creates a reference catalog
            (near-instant, but requires input images to stay on disk).
    """
    ct = _ct()
    ia = ct.image()
    t0 = time.monotonic()
    _virtual = mode in ('nomovevirtual', 'movevirtual')
    try:
        # Virtual modes mutate shared casacore catalog metadata for the
        # entire call duration — must serialise fully.
        # Paged mode only needs the lock to cover the table-registration
        # phase; pixel I/O inside C++ is against independent files and
        # safe to run concurrently once the cache entry is established.
        if _virtual:
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
        else:
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


def concat_subcubes(
    base_imagename: str,
    nparts: int,
    extensions: list[str] | None = None,
    mode: str = 'paged',
    max_workers: int = 4,
    # Deprecated — use *mode* instead.
    virtual: bool | None = None,
) -> None:
    """Concatenate all standard image products from numbered sub-cubes.

    Products include ``.image``, ``.residual``, ``.psf``, etc.

    The *mode* parameter is forwarded directly to ``ia.imageconcat()``:

    * ``'paged'`` — pixel data are physically copied (default, always safe).
    * ``'nomovevirtual'`` — lightweight reference catalog, near-instant but
      subcube files **must remain on disk**.
    * ``'movevirtual'`` — renames subcubes into the output directory
      (near-instant, subcubes are consumed).

    .. deprecated::
        The *virtual* parameter is deprecated.  Pass *mode* explicitly.

    Multiple extensions are concatenated **in parallel** using up to
    *max_workers* threads.  CASA releases the GIL during I/O, so
    thread-level parallelism effectively overlaps disk reads/writes
    across independent image products.

    Args:
        base_imagename: The original ``imagename`` (without ``.subcube.N``).
        nparts: Number of sub-cubes.
        extensions: Image extensions to concatenate.  Defaults to a standard set.
        mode: CASA ``imageconcat`` mode string.
        max_workers: Maximum parallel concatenation threads.
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

    def _do_concat(ext: str, infiles: list[str], outfile: str) -> str:
        concat_images(outfile, infiles, mode=mode)
        return ext

    # Run extensions in parallel (threads — each creates its own ia tool).
    failed: list[tuple[str, Exception]] = []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(work))) as pool:
        future_to_ext = {
            pool.submit(_do_concat, ext, infiles, outfile): ext
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
