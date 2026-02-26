"""
Parallel continuum (MFS) imaging engine.

Distributes *visibility rows* across Dask workers.  Each worker runs
its own ``synthesisimager`` on a data chunk to produce a partial image.
The coordinator then uses ``synthesisnormalizer`` to **gather** partial
images, normalize, run the (serial) minor cycle, and **scatter** the
updated model back to workers for the next major cycle.

Parallelism pattern
-------------------
* **Major cycle** (gridding / degridding) — parallel across row chunks
* **Minor cycle** (deconvolution) — serial on the gathered full image
"""

from __future__ import annotations

import logging
import os
import shutil
from typing import Any, Dict, List, Optional

from pclean.params import PcleanParams
from pclean.parallel.cluster import DaskClusterManager
from pclean.parallel.worker_tasks import _WorkerGridder
from pclean.imaging.normalizer import Normalizer
from pclean.imaging.deconvolver import Deconvolver
from pclean.utils.partition import partition_continuum

log = logging.getLogger(__name__)

_casatools = None


def _ct():
    global _casatools
    if _casatools is None:
        import casatools as ct
        _casatools = ct
    return _casatools


class ParallelContinuumImager:
    """
    Row-parallel continuum (MFS) CLEAN imager.

    Parameters
    ----------
    params : PcleanParams
        Full parameter set (specmode should be ``"mfs"``).
    cluster : DaskClusterManager
        Running Dask cluster.
    """

    def __init__(self, params: PcleanParams, cluster: DaskClusterManager):
        self.params = params
        self.cluster = cluster

        self._part_params: List[PcleanParams] = []
        self._actors: list = []
        self._normalizer: Optional[Normalizer] = None
        self._deconvolver: Optional[Deconvolver] = None
        self._ib_tool = None
        self._major_count = 0

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """
        Execute the full parallel continuum pipeline.

        Returns
        -------
        dict
            Convergence summary.
        """
        try:
            self._partition_data()
            self._create_actors()
            self._setup_normalizer()
            self._setup_deconvolver()
            self._setup_iteration_control()

            # PSF
            self._parallel_make_psf()
            self._normalizer.normalize_psf()

            # PB
            self._parallel_make_pb()
            self._normalizer.normalize_pb()

            # Initial residual
            if self.params.miscpars.get("calcres", True):
                self._parallel_major_cycle(is_first=True)
                self._normalizer.post_major_mfs()

            # Major / minor loop
            if self.params.niter > 0:
                converged = self._check_convergence()
                while not converged:
                    self._deconvolver.setup_mask()
                    did = self._run_minor_cycle()
                    if did:
                        self._normalizer.pre_major_mfs()
                        self._parallel_major_cycle()
                        self._normalizer.post_major_mfs()
                    converged = self._check_convergence() or (not did)

                if self.params.alldecpars["0"].get("restoration", True):
                    self._deconvolver.restore()
                if self.params.alldecpars["0"].get("pbcor", False):
                    self._deconvolver.pbcor()

            # Clean up partial images unless keep_partimages is set
            keep = self.params.parallelpars.get('keep_partimages', False)
            if not keep:
                self._cleanup_partimages()

            return self._summary()
        finally:
            self._teardown()

    # ------------------------------------------------------------------
    # Private — partitioning & actor management
    # ------------------------------------------------------------------

    def _partition_data(self) -> None:
        nworkers = self.cluster.worker_count
        self._part_params = partition_continuum(self.params, nworkers)
        log.info("Continuum imaging: %d row-chunks on %d workers",
                 len(self._part_params), nworkers)

    def _create_actors(self) -> None:
        """Create persistent ``_WorkerGridder`` actors on each worker."""
        client = self.cluster.client
        self._actors = []
        for pp in self._part_params:
            actor_future = client.submit(
                _WorkerGridder, pp.to_dict(), actor=True,
            )
            self._actors.append(actor_future.result())  # blocks until ready

    def _teardown(self) -> None:
        for actor in self._actors:
            try:
                actor.done().result()
            except Exception:
                pass
        self._actors.clear()
        if self._normalizer is not None:
            self._normalizer.teardown()
        if self._deconvolver is not None:
            self._deconvolver.teardown()
        if self._ib_tool is not None:
            self._ib_tool.done()
            self._ib_tool = None

    # ------------------------------------------------------------------
    # Private — normalizer & deconvolver on coordinator
    # ------------------------------------------------------------------

    def _setup_normalizer(self) -> None:
        partimagenames = [pp.imagename for pp in self._part_params]
        normpars = dict(self.params.allnormpars["0"])
        normpars["partimagenames"] = partimagenames
        self._normalizer = Normalizer(normpars, partimagenames)
        self._normalizer.setup()

    def _setup_deconvolver(self) -> None:
        self._deconvolver = Deconvolver(
            imagename=self.params.imagename,
            decpars=dict(self.params.alldecpars["0"]),
        )
        self._deconvolver.setup()

    def _setup_iteration_control(self) -> None:
        ct = _ct()
        self._ib_tool = ct.iterbotsink()
        self._ib_tool.setupiteration(iterpars=dict(self.params.iterpars))

    # ------------------------------------------------------------------
    # Private — parallel major-cycle operations
    # ------------------------------------------------------------------

    def _parallel_make_psf(self) -> None:
        log.info("Computing PSF (parallel) …")
        futures = [a.make_psf() for a in self._actors]
        _wait_all(futures)

    def _parallel_make_pb(self) -> None:
        log.info("Computing PB (parallel) …")
        futures = [a.make_pb() for a in self._actors]
        _wait_all(futures)

    def _parallel_major_cycle(self, is_first: bool = False) -> None:
        log.info("Major cycle %d (parallel) …", self._major_count)
        last = False
        if self._ib_tool is not None and not is_first:
            last = self._ib_tool.cleanComplete(lastcyclecheck=True)
        controls = {"lastcycle": last}

        futures = [a.execute_major_cycle(controls) for a in self._actors]
        _wait_all(futures)
        self._major_count += 1

        if self._ib_tool is not None:
            self._ib_tool.endmajorcycle()

    # ------------------------------------------------------------------
    # Private — serial minor cycle on coordinator
    # ------------------------------------------------------------------

    def _run_minor_cycle(self) -> bool:
        iterbotrec = self._ib_tool.getminorcyclecontrols()
        exrec = self._deconvolver.execute_minor(iterbotrec)
        self._ib_tool.mergeexecrecord(exrec, 0)
        return exrec.get("iterdone", 0) > 0

    def _check_convergence(self) -> bool:
        self._ib_tool.resetminorcycleinfo()
        initrec = self._deconvolver.init_minor()
        self._ib_tool.mergeinitrecord(initrec)
        nmajor = self.params.iterpars.get("nmajor", -1)
        reached = nmajor > 0 and self._major_count >= nmajor
        return self._ib_tool.cleanComplete(reachedMajorLimit=reached)

    # ------------------------------------------------------------------
    # Private — partial-image cleanup
    # ------------------------------------------------------------------

    def _cleanup_partimages(self) -> None:
        """Remove intermediate per-worker partial images."""
        extensions = [
            '.image', '.residual', '.psf', '.model', '.pb',
            '.image.pbcor', '.mask', '.weight', '.sumwt',
        ]
        removed = 0
        for pp in self._part_params:
            abs_name = os.path.abspath(pp.imagename)
            for ext in extensions:
                path = f'{abs_name}{ext}'
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                    removed += 1
        if removed:
            log.info('Cleaned up %d partial-image artifacts', removed)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def _summary(self) -> dict:
        return {
            "imagename": self.params.imagename,
            "major_cycles": self._major_count,
            "nparts": len(self._part_params),
        }


# ======================================================================
# Internal helper
# ======================================================================


def _wait_all(actor_futures: list) -> list:
    """Block until all actor method futures complete."""
    return [f.result() for f in actor_futures]
