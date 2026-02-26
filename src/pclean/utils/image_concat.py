"""
Image concatenation utilities.

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
    """
    Concatenate a list of CASA images along *axis*.

    Parameters
    ----------
    outimage : str
        Path for the output concatenated image.
    inimages : list[str]
        Ordered list of input sub-images.
    axis : int
        Axis to concatenate along (default -1 → spectral).
    relax : bool
        Relax axis checks.
    overwrite : bool
        Overwrite *outimage* if it exists.
    virtual : bool
        If ``True`` create a virtual (reference) concatenation, which
        is fast but references the originals.
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
    log.info("Concatenated %d images → %s", len(inimages), outimage)


def concat_subcubes(
    base_imagename: str,
    nparts: int,
    extensions: list[str] | None = None,
) -> None:
    """
    Concatenate all standard image products (.image, .residual, .psf, …)
    from numbered sub-cubes into the final output.

    Parameters
    ----------
    base_imagename : str
        The original ``imagename`` (without ``.subcube.N``).
    nparts : int
        Number of sub-cubes.
    extensions : list[str], optional
        Image extensions to concatenate.  Defaults to a standard set.
    """
    if extensions is None:
        extensions = [
            ".image", ".residual", ".psf", ".model", ".pb",
            ".image.pbcor", ".mask", ".weight", ".sumwt",
        ]

    for ext in extensions:
        infiles = []
        for i in range(nparts):
            subname = f"{base_imagename}.subcube.{i}{ext}"
            if os.path.isdir(subname) or os.path.isfile(subname):
                infiles.append(subname)
        if not infiles:
            continue
        outfile = f"{base_imagename}{ext}"
        try:
            concat_images(outfile, infiles)
        except Exception as exc:
            log.warning("Failed to concatenate %s: %s", ext, exc)
