# ALMA Cube Imaging — pclean Performance Report (v1)

## 1. Test Configuration

| Parameter | Value |
|---|---|
| Dataset | ALMA `uid___A002_Xf0fd41_X5f5a_target.ms`, SPW 25, IRC+10216 |
| Image size | 8000 × 8000 px, 0.0046 arcsec/px |
| Spectral | specmode=cube, 1000 channels |
| Weighting | briggsbwtaper, robust=0.5 |
| Deconvolver | hogbom, **niter=50000** — configured but 0 iterations executed (see note below) |
| Parallelism | Dask `LocalCluster`, 10 workers, `cube_chunksize=1` |
| Subcubes | 1000 (one per channel) |
| Log files | `test_alma_pclean_1.log` / `test_alma_pclean_1.rec` |

## 2. Result Summary

| Outcome | Value |
|---|---|
| **Completed** | **Yes** |
| Total wall time | **13h 47m 33s** |
| Imaging wall time | 10h 25m 17s |
| Concat wall time | 3h 20m (24% of total) |
| OOM killed | No |
| Peak RSS | 58.6 GB |

> **Note on deconvolution**: `niter=50000` was set in the run script but
> no Hogbom iterations were actually executed.  `SynthesisDeconvolver::setupDeconvolution`
> was called (hogbom configured), `initMinorCycle` ran, and `setupMask`
> (auto-multithresh) ran.  However, `executeminorcycle` never appears in the
> log.  The first convergence check logged *"Peak residual within mask : 0"*
> (empty mask), which likely caused `iterbotsink.cleanComplete()` to set an
> early-stop state before the mask was populated.  The run output is
> equivalent to PSF + residual + restore only.
> **Fixed (post-v1)**: `SerialImager.run()` now calls `update_mask()` before
> the first `has_converged()` check so that `initminorcycle()` sees a
> non-empty mask and `cleanComplete()` correctly returns `False`.

## 3. Phase Timings

| Phase | Start | End | Duration |
|---|---|---|---|
| Cluster start + partition | 06:49:54 | 06:49:56 | ~2 s |
| Parallel imaging (1000 subcubes, 10 workers) | 06:49:56 | 17:15:13 | **10h 25m 17s** |
| Pre-concat collect + handoff | 17:15:13 | 17:15:49 | ~36 s |
| `.image` concat (1000 inputs) | 17:15:49 | 18:20:20 | **64m 31s** |
| `.residual` concat (1000 inputs) | 18:20:41 | 19:07:31 | **46m 50s** |
| `.psf` concat (1000 inputs) | 19:07:51 | 19:42:50 | **34m 59s** |
| `.model` concat (1000 inputs) | 19:43:10 | 19:55:48 | **12m 38s** |
| `.pb` concat (1000 inputs) | 19:56:08 | 20:21:52 | **25m 44s** |
| `.mask` concat (1000 inputs) | 20:22:12 | 20:34:57 | **12m 45s** |
| `.sumwt` concat (1000 inputs) | 20:35:19 | 20:35:45 | **26s** |
| Cleanup | 20:35:45 | 20:37:27 | ~1m 42s |
| **Total** | **06:49:54** | **20:37:27** | **13h 47m 33s** |

Extensions concatenated: 7 (`.image.pbcor` and `.weight` not created at `niter=0`).

**Note**: `.model` (all-zero) and `.sumwt` (tiny: 1×1×1×1000) concat in <13 min
and 26 s respectively — pixel volume matters far more than file count.

## 4. Resource Usage

*Source: psrecord*

### 4.1 Processes

| Phase | NProc |
|---|---|
| Startup | 2 |
| Workers launched | 13 (main + 10 workers + Dask scheduler) |
| Post-imaging concat | 13 (workers idle but alive) |

### 4.2 Memory

| Metric | Value |
|---|---|
| Peak RSS | **58.6 GB** (during parallel imaging) |
| Peak virtual | **81.6 GB** |
| Peak MMap RSS | **52.4 GB** (CASA memory-mapped image files) |
| Swap used | ~5 MB (negligible — no thrashing) |
| RSS during concat | ~4–5 GB (workers idle, only coordinator active) |
| System page cache (start) | 122 GB |
| System page cache (end) | **15.4 GB** (exhausted by concat I/O) |

Peak RSS of 58.6 GB is spread across 10 workers simultaneously, each holding
one channel's worth of PSF + sensitivity store + gridder scratch (~5–6 GB each).
Memory drops after imaging completes because Dask reclaims worker process memory.

### 4.3 CPU

| Phase | CPU % (aggregate) |
|---|---|
| Worker ramp-up | 900–1000% (all 10 cores) |
| Sustained imaging | ~1000–1095% (near-linear 10-core scaling) |
| Concat (serial `imageconcat`) | ~160% (single-threaded I/O) |

Peak CPU of **1095%** indicates all 10 workers plus the coordinator were fully
active.  Concat drops to ~160% because `ia.imageconcat()` is single-threaded
and I/O-bound.

### 4.4 I/O

| Metric | Value |
|---|---|
| Total bytes read | **6.71 TB** |
| Total bytes written | **9.83 TB** |
| Final output dir size | **1.42 TB** (7 extensions) |
| Write amplification | **~6.9×** final output size |

**Write amplification breakdown (estimated):**

1. **Imaging phase** (~2.3 TB): 1000 workers × 9 subcube extensions.
   Per-channel single plane: 8000 × 8000 × 4 B = 256 MB × 9 extensions.
   Plus weight scratch files, gridder temp files, per-worker table caches.
2. **Concat phase**: reads all 1000 subcube inputs (~2.3 TB) per
   extension and writes merged output (physical copy doubles I/O).

## 5. Key Observations

### 5.1 Per-channel parallelism avoids OOM

Each Dask worker loads only a single channel at a time (~5–6 GB per worker).
This keeps peak RSS at 58.6 GB across 10 workers — **45% less** than tclean's
107.8 GB across 25 MPI ranks which hold all 1000 planes simultaneously.

### 5.2 Concat was the primary bottleneck

At 3h 20m (24% of total wall time), subcube concatenation was the single
largest post-imaging overhead.  Two root causes:

1. **Serial extension loop** — 7 independent extensions processed one at a
   time, making total time proportional to their sum.
2. **Physical-copy only** — `ia.imageconcat()` was called without forwarding
   the `mode` parameter, so every pixel was read and rewritten.

### 5.3 Page cache exhaustion during concat

System page cache collapsed from 122 GB → 15.4 GB as the coordinator
sequentially read 1000 subcubes per extension.  This is disk-throughput
bounded, not CPU bounded.

## 6. Bottleneck Analysis & Optimization Options

### Option A — Parallel extension concat (implemented)

Use `ProcessPoolExecutor` (spawn context) to run independent extensions in parallel worker processes.
CASA releases the GIL during I/O, so processes can overlap I/O and CPU work effectively.
With `max_workers=4`, estimated wall time drops from ~3h 20m to ~65 min.

### Option B — Virtual / reference concat (implemented)

Forward `mode='nomovevirtual'` to `ia.imageconcat()` to create a lightweight
reference catalog instead of copying pixels.  Near-instant (<1 min) but
requires subcubes to remain on disk (`keep_subcubes=True`).

### Option C — `movevirtual` mode (implemented)

`mode='movevirtual'` renames subcube directories into the output image.
Near-instant on the same filesystem.  Subcubes are consumed in the process.

### Option D — Larger `cube_chunksize`

With `cube_chunksize=10`, 1000 channels → 100 subcubes, reducing file-open
overhead by ~10×.

| `cube_chunksize` | Subcubes | Est. concat time (paged, serial) |
|---|---|---|
| 1 | 1000 | ~3h 20m |
| 5 | 200 | ~1.5–2 h |
| 10 | 100 | ~45–60 min |
| 50 | 20 | ~10 min |

### Resolution

Options A + B + C have been implemented via the `concat_mode` parameter
in `ClusterConfig` (values: `auto`, `paged`, `virtual`, `movevirtual`).
See `notes/concat_optimize.md` for implementation details.

## 7. Code References

- `src/pclean/utils/image_concat.py` — `concat_images()`, `concat_subcubes()`
- `src/pclean/parallel/cube_parallel.py` — `concat_subcubes()` call site + `_cleanup_subcubes()`
- `src/pclean/config.py` — `ClusterConfig.concat_mode`
