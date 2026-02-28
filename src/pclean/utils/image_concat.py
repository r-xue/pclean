"""Image concatenation utilities.

After parallel cube imaging each worker produces a sub-cube.  This
module concatenates them into the final output cube, mirroring the
``ia.imageconcat()`` call used in CASA's parallel cube imager.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

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
    virtual: bool = True,
) -> None:
    """Concatenate a list of CASA images along *axis*.

    Args:
        outimage: Path for the output concatenated image.
        inimages: Ordered list of input sub-images.
        axis: Axis to concatenate along (default -1 -> spectral).
        relax: Relax axis checks.
        overwrite: Overwrite *outimage* if it exists.
        virtual: If ``True`` create a virtual (reference) concatenation,
            which is fast but references the originals.
    """
    ct = _ct()
    ia = ct.image()
    try:
        ia.imageconcat(
            outfile=outimage,
            infiles=inimages,
            axis=axis,
            relax=relax,
            overwrite=overwrite,
            tempclose=False,
            reorder=False,
        )
    finally:
        ia.done()
    log.info('Concatenated %d images → %s', len(inimages), outimage)


def concat_subcubes(
    base_imagename: str,
    nparts: int,
    extensions: list[str] | None = None,
) -> None:
    """Concatenate all standard image products from numbered sub-cubes.

    Products include ``.image``, ``.residual``, ``.psf``, etc.

    Args:
        base_imagename: The original ``imagename`` (without ``.subcube.N``).
        nparts: Number of sub-cubes.
        extensions: Image extensions to concatenate.  Defaults to a standard set.
    """
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

    for ext in extensions:
        infiles = []
        for i in range(nparts):
            subname = f'{base_imagename}.subcube.{i}{ext}'
            if os.path.isdir(subname) or os.path.isfile(subname):
                infiles.append(subname)
        if not infiles:
            continue
        outfile = f'{base_imagename}{ext}'
        try:
            concat_images(outfile, infiles)
        except Exception as exc:
            log.warning('Failed to concatenate %s: %s', ext, exc)
