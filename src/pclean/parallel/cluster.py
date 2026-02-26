"""
Dask cluster lifecycle management.

Supports:
* Starting a ``LocalCluster`` (default)
* Connecting to an existing ``distributed.Client`` via scheduler address
* Graceful shutdown with image cleanup
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy imports so the module is importable without dask installed.
# ---------------------------------------------------------------------------

_dask_distributed = None


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
    """
    Thin wrapper that owns a ``dask.distributed.Client``.

    Parameters
    ----------
    nworkers : int or None
        Number of workers for a ``LocalCluster``.  ``None`` →
        ``os.cpu_count()``.
    scheduler_address : str or None
        If given, connect to an existing scheduler instead of
        creating a ``LocalCluster``.
    threads_per_worker : int
        Threads per Dask worker (default 1 — CASA tools are not
        thread-safe).
    memory_limit : str
        Per-worker memory limit (``"auto"`` or e.g. ``"8GiB"``).
    local_directory : str or None
        Scratch directory for Dask spill-to-disk.
    """

    def __init__(
        self,
        nworkers: int | None = None,
        scheduler_address: str | None = None,
        threads_per_worker: int = 1,
        memory_limit: str = 'auto',
        local_directory: str | None = None,
    ):
        self.nworkers = nworkers or os.cpu_count() or 4
        self.scheduler_address = scheduler_address
        self.threads_per_worker = threads_per_worker
        self.memory_limit = memory_limit
        self.local_directory = local_directory

        self._cluster = None
        self._client: object | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> "DaskClusterManager":
        """Start (or connect to) the Dask cluster and return *self*."""
        dd = _dd()

        if self.scheduler_address:
            log.info("Connecting to existing scheduler at %s",
                     self.scheduler_address)
            self._client = dd.Client(self.scheduler_address)
        else:
            log.info("Starting LocalCluster with %d workers", self.nworkers)
            self._cluster = dd.LocalCluster(
                n_workers=self.nworkers,
                threads_per_worker=self.threads_per_worker,
                memory_limit=self.memory_limit,
                local_directory=self.local_directory,
            )
            self._client = dd.Client(self._cluster)

        log.info("Dask dashboard: %s", self._client.dashboard_link)
        return self

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
            raise RuntimeError("Cluster not started — call .start() first")
        return self._client

    @property
    def worker_count(self) -> int:
        return len(self.client.scheduler_info()["workers"])

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.shutdown()
        return False
