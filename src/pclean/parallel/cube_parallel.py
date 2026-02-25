"""
Parallel cube imaging engine.

Distributes channels across Dask workers.  Each worker runs a fully
independent ``SerialImager`` on its sub-cube (imaging + deconvolution).
After all workers finish, the coordinator concatenates the sub-cubes
into the final output cube.

This is *embarrassingly parallel* — there is no inter-worker
communication during imaging.
"""

from __future__ import annotations

import logging
import os
import shutil
from typing import Any, Dict, List, Optional

from pclean.params import PcleanParams
from pclean.parallel.cluster import DaskClusterManager
from pclean.parallel.worker_tasks import run_subcube
from pclean.utils.partition import partition_cube
from pclean.utils.image_concat import concat_subcubes

log = logging.getLogger(__name__)


class ParallelCubeImager:
    """
    Channel-parallel cube CLEAN imager.

    Parameters
    ----------
    params : PcleanParams
        Full parameter set (specmode must be cube/cubedata/cubesource).
    cluster : DaskClusterManager
        Running Dask cluster.
    """

    def __init__(self, params: PcleanParams, cluster: DaskClusterManager):
        self.params = params
        self.cluster = cluster
        self._subcube_params: List[PcleanParams] = []

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """
        Execute the full parallel cube pipeline.

        Returns
        -------
        dict
            Per-subcube summary list and the final concatenated image name.
        """
        client = self.cluster.client
        nworkers = self.cluster.worker_count

        # 1. Determine number of partitions from cube_chunksize
        nparts = self._compute_nparts(nworkers)

        # 2. Partition channels
        self._subcube_params = partition_cube(self.params, nparts)
        nparts = len(self._subcube_params)  # may differ slightly due to rounding
        log.info("Cube imaging: %d sub-cubes on %d workers",
                 nparts, nworkers)

        # 3. Submit each sub-cube as an independent task
        futures = []
        for sp in self._subcube_params:
            f = client.submit(run_subcube, sp.to_dict(), pure=False)
            futures.append(f)

        # 4. Gather results
        summaries = client.gather(futures)
        log.info("All %d sub-cubes completed", nparts)

        # 5. Concatenate sub-cube images into final output
        # Workers write images using absolute paths, so we must use
        # the same absolute base name when looking for sub-cube products.
        abs_imgname = os.path.abspath(self.params.imagename)
        concat_subcubes(abs_imgname, nparts)

        # 6. Clean up subcube artifacts unless keep_subcubes is set
        keep = self.params.parallelpars.get("keep_subcubes", False)
        if not keep:
            self._cleanup_subcubes(abs_imgname, nparts)

        return {
            "imagename": self.params.imagename,
            "subcube_summaries": summaries,
            "nparts": nparts,
        }

    @staticmethod
    def _cleanup_subcubes(abs_imgname: str, nparts: int) -> None:
        """Remove intermediate subcube images and per-worker temp dirs."""
        extensions = [
            ".image", ".residual", ".psf", ".model", ".pb",
            ".image.pbcor", ".mask", ".weight", ".sumwt",
        ]
        removed = 0
        for i in range(nparts):
            for ext in extensions:
                subdir = f"{abs_imgname}.subcube.{i}{ext}"
                if os.path.isdir(subdir):
                    shutil.rmtree(subdir, ignore_errors=True)
                    removed += 1
            # Per-subcube temp working directory
            tmpdir = os.path.join(
                os.path.dirname(abs_imgname) or os.getcwd(),
                f".{os.path.basename(abs_imgname)}.subcube.{i}.tmpdir",
            )
            if os.path.isdir(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
                removed += 1
        log.info("Cleaned up %d subcube artifacts", removed)

    def _compute_nparts(self, nworkers: int) -> int:
        """Compute the number of subcube partitions from *cube_chunksize*.

        * ``cube_chunksize = -1``  → one subcube per worker (default)
        * ``cube_chunksize = 1``   → one subcube per channel (max load balance)
        * ``cube_chunksize = N``   → ceil(nchan / N) subcubes
        """
        chunksize = self.params.parallelpars.get("cube_chunksize", -1)
        if chunksize <= 0:
            return nworkers

        nchan = self.params.allimpars["0"].get("nchan", -1)
        if nchan <= 0:
            nchan = 1

        import math
        nparts = math.ceil(nchan / chunksize)
        return max(nparts, 1)
