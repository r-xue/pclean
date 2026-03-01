"""Dask cluster lifecycle management.

Supports:

* Starting a ``LocalCluster`` (default)
* Submitting workers as SLURM batch jobs via ``dask_jobqueue.SLURMCluster``
* Connecting to an existing ``distributed.Client`` via scheduler address
* Graceful shutdown with image cleanup
"""

from __future__ import annotations

import logging
import os
from pprint import pformat

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy imports so the module is importable without dask installed.
# ---------------------------------------------------------------------------

_dask_distributed = None
QUEUE_WAIT = 60


def _dd():
    global _dask_distributed
    if _dask_distributed is None:
        import dask.distributed as dd  # type: ignore

        _dask_distributed = dd
    return _dask_distributed


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class DaskClusterManager:
    """Thin wrapper that owns a ``dask.distributed.Client``.

    Supports three cluster backends selected by *cluster_type*:

    * ``'local'`` — spin up a ``dask.distributed.LocalCluster`` (default).
    * ``'slurm'`` — submit workers as SLURM batch jobs via
      ``dask_jobqueue.SLURMCluster``.  Requires the optional
      ``dask-jobqueue`` package (``pip install dask-jobqueue``).
    * ``'address'`` — connect to a pre-existing scheduler at
      *scheduler_address*.

    For backward compatibility, if *scheduler_address* is set and
    *cluster_type* is left at ``'local'``, the manager silently
    switches to ``'address'`` mode.

    Args:
        nworkers: Number of workers.  ``None`` -> ``os.cpu_count()``.
        scheduler_address: Scheduler URL for ``'address'`` mode.
        threads_per_worker: Threads per Dask worker (default 1 -- CASA tools are
            not thread-safe).
        memory_limit: Per-worker memory limit.  Default ``'0'`` disables Dask's
            memory management, which is correct for CASA workloads because all
            heavy allocations happen inside C++ casatools (reported as
            "unmanaged memory").  Dask cannot free this memory, so its
            pause/spill heuristics only cause workers to stall.  Concurrency is
            bounded by ``as_completed`` instead.
        local_directory: Scratch directory for Dask spill-to-disk.
        cluster_type: ``'local'``, ``'slurm'``, or ``'address'``.
        slurm_queue: SLURM partition name (``--partition``).
        slurm_account: SLURM account string (``--account``).
        slurm_walltime: Per-job wall time (``--time``).
        slurm_job_mem: Per-job memory (``--mem``).
        slurm_cores_per_job: CPUs per SLURM job (``--cpus-per-task``).
        slurm_job_extra_directives: Extra ``#SBATCH`` lines.
        slurm_python: Path to the Python executable on compute nodes.
        slurm_local_directory: Worker scratch directory on compute nodes.
        slurm_log_directory: Directory for SLURM stdout/stderr logs.
        slurm_job_script_prologue: Shell commands injected before the
            worker process starts (e.g. ``module load`` or ``conda activate``).
    """

    def __init__(
        self,
        nworkers: int | None = None,
        scheduler_address: str | None = None,
        threads_per_worker: int = 1,
        memory_limit: str = '0',
        local_directory: str | None = None,
        cluster_type: str = 'local',
        slurm_queue: str | None = None,
        slurm_account: str | None = None,
        slurm_walltime: str = '04:00:00',
        slurm_job_mem: str = '20GB',
        slurm_cores_per_job: int = 1,
        slurm_job_extra_directives: list[str] | None = None,
        slurm_python: str | None = None,
        slurm_local_directory: str | None = None,
        slurm_log_directory: str = 'logs',
        slurm_job_script_prologue: list[str] | None = None,
    ):
        self.nworkers = nworkers or os.cpu_count() or 4
        self.scheduler_address = scheduler_address
        self.threads_per_worker = threads_per_worker
        self.memory_limit = memory_limit
        self.local_directory = local_directory

        # Backward compat: scheduler_address implies 'address' mode.
        if scheduler_address and cluster_type == 'local':
            cluster_type = 'address'
        self.cluster_type = cluster_type

        # SLURM-specific
        self.slurm_queue = slurm_queue
        self.slurm_account = slurm_account
        self.slurm_walltime = slurm_walltime
        self.slurm_job_mem = slurm_job_mem
        self.slurm_cores_per_job = slurm_cores_per_job
        self.slurm_job_extra_directives = slurm_job_extra_directives or []
        self.slurm_python = slurm_python
        self.slurm_local_directory = slurm_local_directory
        self.slurm_log_directory = slurm_log_directory
        self.slurm_job_script_prologue = slurm_job_script_prologue or []

        self._cluster = None
        self._client: object | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> 'DaskClusterManager':
        """Start (or connect to) the Dask cluster and return *self*."""
        dd = _dd()

        if self.threads_per_worker > 1:
            log.warning(
                'threads_per_worker=%d: casatools are NOT thread-safe; '
                'use threads_per_worker=1 (default) for correctness',
                self.threads_per_worker,
            )

        if self.cluster_type == 'address':
            log.info('Connecting to existing scheduler at %s', self.scheduler_address)
            self._client = dd.Client(self.scheduler_address)

        elif self.cluster_type == 'slurm':
            self._start_slurm(dd)

        else:  # 'local'
            log.info('Starting LocalCluster with %d workers', self.nworkers)
            self._cluster = dd.LocalCluster(
                n_workers=self.nworkers,
                processes=True,
                threads_per_worker=self.threads_per_worker,
                memory_limit=self.memory_limit,
                local_directory=self.local_directory,
            )
            self._client = dd.Client(self._cluster)

        # Block until all requested workers have registered with the
        # scheduler.  Without this, worker_count can return a smaller
        # number than nworkers due to a startup race condition.
        self._client.wait_for_workers(self.nworkers, timeout=QUEUE_WAIT)

        # Verify the cluster actually created the requested workers.
        # Use nthreads() for a fresh, synchronous query to the scheduler
        # (scheduler_info() can return a stale cached snapshot).
        actual = len(self._client.nthreads())
        if actual != self.nworkers:
            log.warning(
                'Requested %d workers but LocalCluster only created %d '
                '(system may lack resources). Adjusting nworkers.',
                self.nworkers,
                actual,
            )
            self.nworkers = actual

        log.info('Dask cluster ready: %d workers registered', self.worker_count)

        log.info('Dask dashboard: %s', self._client.dashboard_link)
        log.info('   client:  %s', self._client)
        log.info('   cluster: %s', self._client.cluster)

        def get_status(dask_worker) -> tuple[str, str]:
            return dask_worker.status, dask_worker.id

        status: dict[str, tuple[str, str]] = self._client.run(get_status)

        if status:
            log.info('worker status: \n %s', pformat(status))

        return self

    def _start_slurm(self, dd) -> None:
        """Create a ``dask_jobqueue.SLURMCluster`` and scale to *nworkers* jobs."""
        try:
            from dask_jobqueue import SLURMCluster
        except ImportError as exc:
            raise ImportError(
                "cluster_type='slurm' requires dask-jobqueue: "
                'pip install dask-jobqueue'
            ) from exc

        log.info(
            'Starting SLURMCluster (queue=%s, nworkers=%d, mem=%s, walltime=%s)',
            self.slurm_queue,
            self.nworkers,
            self.slurm_job_mem,
            self.slurm_walltime,
        )

        slurm_kwargs: dict = dict(
            queue=self.slurm_queue,
            account=self.slurm_account,
            walltime=self.slurm_walltime,
            cores=self.slurm_cores_per_job,
            memory=self.slurm_job_mem,
            processes=1,  # one Dask worker per SLURM job
            local_directory=self.slurm_local_directory or self.local_directory,
            log_directory=self.slurm_log_directory,
            job_extra_directives=self.slurm_job_extra_directives,
        )

        if self.slurm_python:
            slurm_kwargs['python'] = self.slurm_python

        if self.slurm_job_script_prologue:
            slurm_kwargs['job_script_prologue'] = self.slurm_job_script_prologue

        self._cluster = SLURMCluster(**slurm_kwargs)
        self._cluster.scale(jobs=self.nworkers)
        self._client = dd.Client(self._cluster)

    def shutdown(self) -> None:
        """Close client and cluster."""
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._cluster is not None:
            self._cluster.close()
            self._cluster = None

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def client(self):
        """Return the ``dask.distributed.Client``."""
        if self._client is None:
            raise RuntimeError('Cluster not started — call .start() first')
        return self._client

    @property
    def worker_count(self) -> int:
        """Number of workers currently registered with the scheduler.

        Uses ``client.nthreads()`` which is a direct synchronous query
        to the scheduler, avoiding stale cached snapshots from
        ``scheduler_info()``.

        Note that this can be less than the requested nworkers due to
        resource constraints or startup issues. The cluster manager will log
        a warning and adjust nworkers accordingly.
        """
        return len(self.client.nthreads())

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.shutdown()
        return False
