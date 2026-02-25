# Per-Channel Convergence Optimization

## Problem

The current cube parallelism partitions channels into subcubes (e.g. 117 channels → 5 subcubes of ~24 channels). All channels within a subcube run the same number of major/minor cycles. If channel 5 has bright emission needing 10 major cycles but channels 6–23 are noise-dominated and converge after 1, those 18 channels still get dragged through 9 unnecessary major cycles (each re-reading the MS).

## Per-Channel Convergence

Each channel independently runs its own major-minor loop. Channels that converge early free up workers for still-iterating channels. This is essentially what CASA's MPI `parallel=True` cube mode does — it uses dynamic task allocation where each MPI rank grabs the next unconverged channel.

## Implementation Options

### Option A: Single-channel Dask tasks (simple, high throughput)

Submit 117 single-channel tasks instead of 5 subcubes of ~24 channels. Dask's dynamic scheduler naturally handles load balancing — when a worker finishes a fast-converging channel, it picks up the next pending one.

- **Pro:** Trivial to implement (just set `nparts=nchan`), maximum load balancing
- **Con:** More MS open/read overhead per channel, more image files to concatenate

### Option B: Subcube with internal per-channel convergence (complex, I/O efficient)

Keep subcubes but track convergence per-channel within each subcube. Major cycle reads the MS once for all channels (I/O efficient), but minor cycles only run on unconverged channels, and the subcube stops major-cycling once all its channels converge.

- **Pro:** Fewer MS reads, fewer image products
- **Con:** Needs custom per-channel convergence tracking; a single slow channel still blocks its subcube's major cycle

## Recommendation

Option A is the practical win. The MS read overhead per channel is small (the data is the same, just different gridding planes), and Dask's scheduler gives you automatic load balancing. The current code already supports this — you just need to control `nparts`:

```python
# Instead of nparts = nworkers (5), use nparts = nchan (117)
# Each task is 1 channel, Dask schedules dynamically across 5 workers
```

The throughput improvement depends on how unevenly emission is distributed across channels. For data with a few bright line channels and many continuum-only channels, the speedup can be significant (2–5x in wall time for high `niter`).

## Possible Interface

Add a `cube_chunksize` parameter:
- `cube_chunksize=1` — per-channel tasks (maximum load balancing)
- `cube_chunksize=-1` — current behavior (`nparts=nworkers`)
- `cube_chunksize=N` — user-specified grouping (N channels per task)
