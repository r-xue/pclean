# ADIOS2 as a MeasurementSet Storage Backend

## Overview

`Adios2StMan` is an alternative casacore storage manager that replaces
the default tile-based I/O (`TiledShapeStMan`) with the ADIOS2 BP5
engine.  Its design targets two bottlenecks common in parallel imaging
workflows: **lock contention** and **small random I/O**.

Whether these translate into meaningful speedups depends on the dataset
size, storage hardware, and parallelism level.  The claims below
describe the *mechanism*; actual gains should be validated with
representative benchmarks before drawing conclusions for a given
workflow.

## How the default storage manager works

With `TiledShapeStMan`, each table access involves:

1. A random seek to locate the tile on disk.
2. A small read or write of that tile.
3. A file-based table lock (acquired before, released after).

Under parallel workloads, multiple processes contend for the same lock
and generate many small random I/O operations — a pattern that scales
poorly on both spinning disks and networked filesystems.

## What ADIOS2 changes

| Aspect | Default (`TiledShapeStMan`) | `Adios2StMan` (BP5) |
|--------|----------------------------|---------------------|
| **Writes** | Small random writes per tile | Buffered in memory, flushed sequentially |
| **Reads** | Per-tile seek + read | Compact metadata index, fewer seeks |
| **Locking** | File-based table lock | No table locks |
| **Metadata** | Many small files (`table.info`, `table.f*`) | Single index file |
| **Concurrency** | Serialized by lock | Concurrent region-based access |

In principle, these properties reduce both latency and contention.

## Relevance to pclean

pclean's imaging loop is I/O-intensive: each major cycle reads the full
residual visibilities and writes back the model.  In parallel mode,
multiple Dask workers perform this simultaneously against the same
MeasurementSet.

The expected benefits of ADIOS2 in this context are:

- **Reduced lock contention** — workers no longer serialize on a shared
  table lock.
- **Fewer random IOPS** — buffered writes and aggregated metadata reduce
  the number of small disk operations.
- **Lower metadata overhead** — opening a table requires reading one
  index rather than probing many small files.

> **Caveat:** These are architectural advantages.  The degree of
> improvement is workload-dependent and should be quantified through
> controlled benchmarks on the target storage tier before assuming a
> particular speedup factor.

## Further reading

- [Converting an MS to ADIOS2](adios2_convert.md) — rewriting a
  MeasurementSet for the ADIOS2 backend.
- [Checking ADIOS2 support](check_adios2.md) — verifying that a
  `casatools` build includes `Adios2StMan`.
- [I/O benchmarks](benchmark_zfs_pools.md) — storage-tier performance
  measurements.
