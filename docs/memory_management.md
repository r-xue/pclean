# Memory Management in Parallel Workers

## Problem with `memory_limit='auto'`

With Dask's default `memory_limit='auto'`, total system RAM is divided equally
among workers (e.g. 83.79 GiB / 12 workers = 6.98 GiB each).

CASA's C++ imaging engine (casatools) allocates roughly 5 GiB per worker
during gridding in our initial test case (see the
[Reference Test Case](#reference-test-case) below).  Dask classifies these
allocations as "unmanaged memory" because they happen inside native code,
outside Python's heap.

When a worker's process memory reaches 80% of its limit, Dask **pauses** the
worker -- it stops accepting new tasks.  Because the C++ memory cannot be
freed by Dask (only completing the CASA task releases it), the pause/resume
cycle causes most workers to stall, leaving only a handful active at any
given time.

Under sustained memory pressure, the Dask TCP stream between workers and the
scheduler can also become corrupted, producing bogus frame-length headers:

```
Unable to allocate 7.27 EiB for an array with shape (8387229930220700999,)
```

This is not a real allocation request -- it is a deserialization of garbage
bytes from a corrupted TCP frame.

## Solution

Dask's per-worker memory management is disabled by setting `memory_limit='0'`
in `DaskClusterManager`.  This is appropriate for CASA workloads because:

1. All heavy memory comes from C++ casatools -- Dask cannot free it.
2. Pausing workers does not reduce memory -- the C++ allocation persists
   until the task finishes.
3. Concurrency is already bounded by the `as_completed` pattern in
   `ParallelCubeImager`, which keeps at most `nworkers` tasks in-flight.

## Relevant Code

| File | Role |
|------|------|
| `src/pclean/parallel/cluster.py` | `memory_limit='0'` default in `DaskClusterManager.__init__` |
| `src/pclean/parallel/cube_parallel.py` | `as_completed` bounded concurrency instead of bulk submit |
| `src/pclean/parallel/worker_tasks.py` | Post-task `gc.collect()` and `tb.clearlocks()` to release C++ resources |

## When to Re-enable Memory Limits

On a shared cluster (e.g. via `dask-jobqueue`) where other jobs compete for
RAM, a non-zero `memory_limit` can prevent worker processes from being
OOM-killed by the OS.  Pass it explicitly:

```python
cluster = DaskClusterManager(nworkers=8, memory_limit='16GiB')
```

For dedicated machines, `memory_limit='0'` (the default) gives the best
throughput.

## Reference Test Case

The memory behaviour described above was observed with the following ALMA
Band 6 cube-imaging job (`scripts/test1.py`):

| Parameter | Value |
|-----------|-------|
| Target | IRC+10216 |
| Measurement Set | `uid___A002_Xf0fd41_X5f5a_target.ms` |
| Spectral window | 25 |
| Antennas | 40 |
| Image size | 8000 x 8000 pixels |
| Cell size | 0.0046 arcsec |
| Spectral mode | cube |
| Channels | 7677 |
| Start frequency | 267.5866 GHz |
| Channel width | 0.2441 MHz |
| Deconvolver | Hogbom |
| Weighting | Robust 0.5 |
| Auto-masking | auto-multithresh |
| niter | 50 000 |
| Threshold | 2.0 mJy |
| Parallel workers | 12 |
| `cube_chunksize` | 1 (one channel per task) |

On a machine with 83.79 GiB total RAM and 12 workers, each worker consumed
approximately 5 GiB of unmanaged (C++) memory during gridding, exceeding
Dask's default 80% pause threshold of 5.6 GiB per worker (6.98 GiB limit).
Setting `memory_limit='0'` eliminated worker pausing and the associated TCP
corruption errors.
