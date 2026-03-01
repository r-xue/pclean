"""Parallel cube imaging engine.

Distributes channels across Dask workers.  Each worker runs a fully
independent ``SerialImager`` on its sub-cube (imaging + deconvolution).
After all workers finish, the coordinator concatenates the sub-cubes
into the final output cube.

This is *embarrassingly parallel* -- there is no inter-worker
communication during imaging.
"""

from __future__ import annotations

import logging
import math
import os
import shutil
from typing import TYPE_CHECKING

from pclean.parallel.cluster import DaskClusterManager
from pclean.parallel.worker_tasks import run_subcube
from pclean.utils.image_concat import concat_subcubes
from pclean.utils.partition import partition_cube

if TYPE_CHECKING:
    from pclean.config import PcleanConfig

log = logging.getLogger(__name__)


class ParallelCubeImager:
    """Channel-parallel cube CLEAN imager.

    Args:
        config: Full imaging configuration (specmode must be cube/cubedata/cubesource).
        cluster: Running Dask cluster.
    """

    def __init__(self, config: PcleanConfig, cluster: DaskClusterManager):
        self.config = config
        self.cluster = cluster
        self._subcube_configs: list[PcleanConfig] = []

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """Execute the full parallel cube pipeline.

        Returns:
            Per-subcube summary list and the final concatenated image name.
        """
        client = self.cluster.client
        nworkers = self.cluster.worker_count

        # 1. Determine number of partitions from cube_chunksize
        nparts = self._compute_nparts(nworkers)

        # 2. Partition channels
        self._subcube_configs = partition_cube(self.config, nparts)
        nparts = len(self._subcube_configs)  # may differ slightly due to rounding
        log.info('Cube imaging: %d sub-cubes on %d workers', nparts, nworkers)

        # 3. Submit sub-cubes with bounded concurrency.
        #    Using ``as_completed`` keeps at most *nworkers* tasks
        #    in-flight, preventing the Dask scheduler from queuing
        #    all tasks upfront and reducing casacore table-cache
        #    pressure on reused worker processes.
        from dask.distributed import as_completed

        summaries: list[dict | None] = [None] * nparts
        # Map future -> index so we can preserve ordering.
        future_to_idx: dict = {}
        pending: list = []

        # Seed the initial batch (up to nworkers tasks).
        batch_end = min(nworkers, nparts)
        for i in range(batch_end):
            f = client.submit(
                run_subcube,
                self._subcube_configs[i].model_dump(mode='python'),
                pure=False,
            )
            future_to_idx[f] = i
            pending.append(f)

        next_idx = batch_end  # next subcube to submit

        ac = as_completed(pending)
        for completed_future in ac:
            idx = future_to_idx.pop(completed_future)
            summaries[idx] = completed_future.result()

            # Submit the next subcube (if any) to keep the pipeline full.
            if next_idx < nparts:
                f = client.submit(
                    run_subcube,
                    self._subcube_configs[next_idx].model_dump(mode='python'),
                    pure=False,
                )
                future_to_idx[f] = next_idx
                ac.add(f)
                next_idx += 1

        log.info('All %d sub-cubes completed', nparts)

        # 5. Concatenate sub-cube images into final output
        # Workers write images using absolute paths, so we must use
        # the same absolute base name when looking for sub-cube products.
        abs_imgname = os.path.abspath(self.config.imagename)
        concat_subcubes(abs_imgname, nparts)

        # 6. Clean up subcube artifacts unless keep_subcubes is set
        if not self.config.cluster.keep_subcubes:
            self._cleanup_subcubes(abs_imgname, nparts)

        return {
            'imagename': self.config.imagename,
            'subcube_summaries': summaries,
            'nparts': nparts,
        }

    @staticmethod
    def _cleanup_subcubes(abs_imgname: str, nparts: int) -> None:
        """Remove intermediate subcube images and per-worker temp dirs."""
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
        removed = 0
        for i in range(nparts):
            for ext in extensions:
                subdir = f'{abs_imgname}.subcube.{i}{ext}'
                if os.path.isdir(subdir):
                    shutil.rmtree(subdir, ignore_errors=True)
                    removed += 1
            # Per-subcube temp working directory
            tmpdir = os.path.join(
                os.path.dirname(abs_imgname) or os.getcwd(),
                f'.{os.path.basename(abs_imgname)}.subcube.{i}.tmpdir',
            )
            if os.path.isdir(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
                removed += 1
        log.info('Cleaned up %d subcube artifacts', removed)

    def _compute_nparts(self, nworkers: int) -> int:
        """Compute the number of subcube partitions from *cube_chunksize*.

        * ``cube_chunksize = -1``  -> one subcube per worker (default)
        * ``cube_chunksize = 1``   -> one subcube per channel (max load balance)
        * ``cube_chunksize = N``   -> ceil(nchan / N) subcubes
        """
        chunksize = self.config.cluster.cube_chunksize
        if chunksize <= 0:
            return nworkers

        nchan = self.config.image.nchan
        if nchan <= 0:
            nchan = 1

        nparts = math.ceil(nchan / chunksize)
        return max(nparts, 1)
