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

        # 1. Partition channels
        self._subcube_params = partition_cube(self.params, nworkers)
        nparts = len(self._subcube_params)
        log.info("Cube imaging: %d sub-cubes on %d workers",
                 nparts, nworkers)

        # 2. Submit each sub-cube as an independent task
        futures = []
        for sp in self._subcube_params:
            f = client.submit(run_subcube, sp.to_dict(), pure=False)
            futures.append(f)

        # 3. Gather results
        summaries = client.gather(futures)
        log.info("All %d sub-cubes completed", nparts)

        # 4. Concatenate sub-cube images into final output
        # Workers write images using absolute paths, so we must use
        # the same absolute base name when looking for sub-cube products.
        abs_imgname = os.path.abspath(self.params.imagename)
        concat_subcubes(abs_imgname, nparts)

        return {
            "imagename": self.params.imagename,
            "subcube_summaries": summaries,
            "nparts": nparts,
        }
