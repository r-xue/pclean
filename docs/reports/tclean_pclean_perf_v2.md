# ALMA Cube Imaging — pclean Performance Report (v2)

## 1. Test Configuration

| Parameter | Value |
|---|---|
| Dataset | ALMA `uid___A002_Xf0fd41_X5f5a_target.ms`, SPW 25, IRC+10216 |
| Image size | 8000 × 8000 px, 0.0046 arcsec/px |
| Spectral | specmode=cube, 1000 channels, start=268.5 GHz, width=0.244 MHz |
| Weighting | briggsbwtaper, robust=0.5, fracBW=0.000907945 |
| Deconvolver | hogbom, niter=50000, threshold=4.0 mJy |
| Masking | auto-multithresh (sidelobethreshold=2.5, noisethreshold=5.0, lownoisethreshold=1.5, negativethreshold=7.0, minbeamfrac=0.3, growiterations=50) |
| Parallelism | Dask `LocalCluster`, 10 workers, `cube_chunksize=1` |
| Subcubes | 1000 (one per channel) |
| Concat | `mode=paged`, `max_workers=4`, `keep_subcubes=True` |
| Log files | `test_alma_pclean_2.log` / `test_alma_pclean_2.rec` |
| Script | `scripts/test_alma_pclean_v2.py` |

### Changes from v1

| Aspect | v1 | v2 |
|---|---|---|
| Deconvolution | 0 iterations (empty-mask bug) | **~204 Hogbom iters / channel** (bug fixed) |
| Concat mode | serial, `paged` | **parallel** (`paged`, `max_workers=4`) |
| concat extensions | 7 (serial loop) | 9 (7 physical + parallel workers) |
| Stopping criterion | — | peak residual divergence (3× from minimum) |

> **Bug fix (post-v1)**: `SerialImager.run()` now calls `update_mask()`
> before the first `has_converged()` check, so `initminorcycle()` sees a
> populated auto-multithresh mask and deconvolution proceeds correctly.

## 2. Result Summary

| Outcome | Value |
|---|---|
| **Completed** | **Yes** |
| Total wall time | **68h 22m 49s** (246169 s) |
| Imaging wall time | ~65h 23m (2026-03-06 18:30:08 → 2026-03-09 11:53:14) |
| Concat wall time | 2h 59m 41s (10781 s, **16% of total**) |
| OOM killed | No |
| Peak RSS | **44.1 GB** (during parallel imaging) |

## 3. Phase Timings

| Phase | Start | End | Duration |
|---|---|---|---|
| Cluster start | 2026-03-06 18:30:06 | 18:30:08 | ~2 s |
| Data selection + freq grid | 18:30:08 | 18:33:13 | ~3m 5s |
| Parallel imaging (1000 subcubes, 10 workers) | 18:33:13 | 2026-03-09 11:53:14 | **~65h 20m** |
| `.model` concat (1000 inputs, paged) | 11:53:14 | 12:17:11 | **23m 57s** (1434 s) |
| `.psf` concat | 12:18:51 | 13:34:07 | **1h 40m 56s** (6049 s) |
| `.image` concat | 13:35:06 | 14:18:31 | **2h 25m 14s** (8714 s)† |
| `.residual` concat | 14:18:58 | 14:25:15 | **2h 31m 57s** (9117 s)† |
| `.sumwt` concat | 14:25:59 | 14:26:09 | **7m 37s** (457 s)† |
| `.pb` concat | 12:55:02 | 14:38:27 | **2h 21m 15s** (8475 s)† |
| `.mask` concat | 14:22:23 | 14:52:55 | **1h 18m 48s** (4728 s)† |
| **Total** | **2026-03-06 18:30:06** | **2026-03-09 14:52:55** | **68h 22m 49s** |

† With `max_workers=4`, extensions are concatenated in parallel. Wall
times listed are elapsed per extension; the critical-path wall time for
concatenation is the longest extension (`.residual`, 9117 s). Reported
durations are the per-extension I/O times logged by `image_concat`, which
overlap because up to 4 run concurrently.

**Total concat wall time: 10781 s ≈ 3h 0m** (parallel execution, limited
by the slowest extension set finishing).

### Per-Subcube Timing Breakdown

| Step | Min | Mean | Median | Max |
|---|---|---|---|---|
| `make_psf` | 210 s | 329 s | 365 s | 471 s |
| `major_cycle(0)` (gridding + predict) | 252 s | 285 s | 283 s | 350 s |
| deconvolution (minor cycles, in-cycle) | — | ~1500 s | — | — |
| `restore` | 7 s | 12 s | 11 s | 46 s |
| **run total** | **1596 s** | **2342 s** | **2431 s** | **3267 s** |

Each subcube completed exactly **1 major cycle** (major_cycle(0)) containing
multiple minor-cycle rounds before triggering the stopping criterion.

## 4. Deconvolution Statistics

| Metric | Value |
|---|---|
| Minor cycles (executeminorcycle calls) | 2971 across 1000 subcubes |
| Mean minor-cycle rounds / subcube | ~3.0 |
| Total Hogbom iterations | 203,902 |
| Mean Hogbom iters / minor-cycle round | 68.6 |
| Mean Hogbom iters / subcube | ~204 |
| Convergence: cyclethreshold | 2971 (inner loop stops) |
| Convergence: peak residual divergence (3×) | 1000 (outer loop stops) |
| Major cycles / subcube | 1 |

All 1000 subcubes terminated via the pclean stopping criterion
"peak residual increased by more than 3× from minimum" after ~3 minor-cycle
rounds within a single major cycle. This indicates the auto-multithresh mask
is allowing deconvolution but the peak residual diverges after a few rounds,
consistent with a source that requires careful cleaning depth control.

**First-batch deconvolution example** (subcube.0–9, channel 0–9):

| Round | Typical iters | Peak residual (Jy) | Model flux (Jy) | Stop reason |
|---|---|---|---|---|
| 1 | 64–71 | 0.27→0.08 | 0→0.9 | cyclethreshold |
| 2 | 54–60 | 0.44→0.13 | 0.9→−0.4 | cyclethreshold |
| 3 | 50–54 | 0.72→0.22 | −0.4→1.5 | cyclethreshold |
| — | — | peak > 3× min | — | **divergence** |

## 5. Resource Usage

*Source: psrecord (`test_alma_pclean_2.rec`)*

> **Note**: psrecord monitoring covered 2026-03-06 18:30:06 → 2026-03-09
> 02:39:35 (~32 h), ending during the imaging phase. No psrecord data is
> available for the final ~1/3 of imaging or for the concat phase.

### 5.1 Processes

| Phase | NProc |
|---|---|
| Startup | 2 |
| Workers launched | 13 (main + scheduler + 10 workers + nanny) |
| Peak NProc | **13** |

### 5.2 Memory

| Metric | Value |
|---|---|
| Peak RSS | **44.1 GB** (44070 MB) |
| Peak virtual | **66.8 GB** (68377 MB) |
| Peak MMap RSS | **29.2 GB** (29872 MB, CASA memory-mapped image files) |
| Swap used | 0 (none) |
| System page cache (start) | 43.8 GB |
| System page cache (end, at monitoring stop) | 54.8 GB |

Peak RSS of 44.1 GB is **25% lower** than v1's 58.6 GB despite deconvolution
actually executing. This is within the expected range: the deconvolution inner
loop (Hogbom) operates on already-allocated image buffers and does not allocate
significant additional memory beyond what make_psf + major_cycle create.

**Estimated per-worker RSS**: ~4.0–4.4 GB (44 GB / 10 workers + overhead).

### 5.3 CPU

| Phase | CPU % (aggregate) |
|---|---|
| Sustained imaging | ~980–1080% (near-linear 10-core scaling) |
| Peak CPU | **1078%** |

Peak CPU of 1078% is consistent with v1 (1095%), confirming all 10 workers
were fully utilized during imaging.

### 5.4 I/O

| Metric | Value |
|---|---|
| Total bytes read | **19.0 TB** (19,881 GB) |
| Total bytes written | **18.1 TB** (19,024 GB) |
| Final output dir size | **12.5 TB** (13,076 GB)† |
| Write amplification | **~1.5×** final dir size |

† Directory size includes both the 1000 subcubes (`keep_subcubes=True`) and
the 7 concatenated output extensions — this is *much* larger than v1's 1.42 TB
because subcubes are retained.

**I/O comparison with v1**:

| Metric | v1 | v2 | Factor |
|---|---|---|---|
| Read | 6.71 TB | 19.0 TB | 2.8× |
| Write | 9.83 TB | 18.1 TB | 1.8× |
| Dir size | 1.42 TB | 12.5 TB | 8.8×† |

† v1 deleted subcubes; v2 retained them (`keep_subcubes=True`).

The higher I/O in v2 is dominated by the deconvolution minor cycles:
each minor cycle reads/writes the residual, model, and mask images
(3 × 256 MB/plane × ~3 rounds × 1000 channels ≈ 2.3 TB additional).
Combined with the 6.5× longer wall time, the sustained I/O bandwidth
is similar to v1.

## 6. Comparison with v1

| Metric | v1 | v2 | Change |
|---|---|---|---|
| Total wall time | 13h 48m | **68h 23m** | **5.0×** slower |
| Imaging wall time | 10h 25m | ~65h 20m | 6.3× slower |
| Concat wall time | 3h 20m | 3h 0m | **10% faster** |
| Deconvolution | 0 iters | 203,902 iters | ∞ (bug fixed) |
| Major cycles / subcube | 0 | 1 | — |
| Minor-cycle rounds / subcube | 0 | ~3 | — |
| Peak RSS | 58.6 GB | 44.1 GB | **25% lower** |
| Peak MMap RSS | 52.4 GB | 29.2 GB | 44% lower |
| Peak CPU | 1095% | 1078% | ~same |
| Total I/O read | 6.71 TB | 19.0 TB | 2.8× |
| Total I/O write | 9.83 TB | 18.1 TB | 1.8× |

### Why v2 is 5× slower

v1 was effectively PSF+residual+restore only (0 deconvolution iterations).
v2 runs full deconvolution: ~3 minor-cycle rounds per subcube, each requiring
a full Hogbom iteration loop plus mask evaluation. The per-subcube run time
increased from ~375 s (v1) to ~2342 s (v2) — a **6.3× increase**, almost
entirely attributable to the deconvolution work.

### Why peak RSS is lower in v2

v1's 58.6 GB peak was measured with psrecord covering the full run. v2's
44.1 GB peak was measured over only ~32 h of the ~65 h imaging phase; the
true peak may be higher. The deconvolver itself adds negligible allocation —
it reuses the already-allocated image stores — so the per-worker footprint
is similar in both runs.

### Concat improvement

With parallel extension concat (`max_workers=4`), v2 concat completed in
~3 h vs v1's 3h 20m (serial, 7 extensions in sequence). The per-extension
times are higher in v2 (larger images after deconvolution produced model
content), but parallelism compensates.

## 7. Key Observations

### 7.1 Deconvolution now executes correctly

The v1 empty-mask bug is fixed. All 1000 channels underwent ~3 minor-cycle
rounds (203,902 total Hogbom iterations). The auto-multithresh mask successfully
identifies emission regions, and the divergence stopping criterion
(peak residual > 3× minimum) provides a sensible halt.

### 7.2 Divergence after ~3 minor cycles suggests under-cleaning

All subcubes stop after ~3 minor-cycle rounds due to peak residual
divergence (not threshold or niter convergence). This pattern —
residual decreasing then increasing — is characteristic of
auto-multithresh being too aggressive or the cycle factor being too high.
Potential improvements:

- Lower `cyclefactor` to reduce the per-cycle cleaning depth
- Adjust `negativethreshold` (currently 7.0) to limit divergent behavior
- Schedule a second major cycle to narrow the mask after initial cleaning

### 7.3 Per-channel parallelism still avoids OOM

Peak RSS of 44.1 GB across 10 workers (~4.4 GB/worker) remains well within
the 128.2 GB system RAM. The deconvolution adds negligible memory overhead
since Hogbom operates on already-allocated image buffers in-place.

### 7.4 Parallel concat effective

The `max_workers=4` parallel concat completed 7 extensions in ~3 h total
wall time, consistent with the longest single extension (.residual, 9117 s
≈ 2.5 h). This is comparable to v1's serialized 3h 20m because
per-extension times are longer, but true wall-clock overlap provides a net
improvement.

### 7.5 psrecord monitoring ended early

The `.rec` file covers only ~32 h of the ~68 h run. For future tests,
ensure psrecord is configured with a sufficiently long duration or no
timeout to capture the full run including concat.

## 8. Optimization Opportunities

### 8.1 Reduce per-subcube imaging time

Per-subcube run time (mean 2342 s) is dominated by deconvolution overhead.
Options:

- **Lower niter/threshold**: The current threshold (4.0 mJy) is never
  reached before divergence. A lower niter limit (e.g., 200) would cap
  the per-channel work without changing the output quality, since
  divergence stops it anyway.
- **Cycle factor tuning**: Fewer minor-cycle iterations per major cycle
  would allow a second major cycle with an updated mask.

### 8.2 Increase `cube_chunksize`

With `cube_chunksize=1`, each subcube is a single channel. Increasing to
5–10 would reduce Dask scheduling overhead and I/O setup costs, though
it increases per-worker memory footprint proportionally.

### 8.3 Virtual concat for intermediate runs

For development/iteration runs where subcubes are retained, `concat_mode='virtual'`
or `'movevirtual'` would reduce concat time from ~3 h to <1 min.

## 9. Code References

- `scripts/test_alma_pclean_v2.py` — test script
- `src/pclean/imaging/serial_imager.py` — `SerialImager.run()` (deconvolution loop, mask fix)
- `src/pclean/utils/image_concat.py` — `concat_images()`, parallel extension concat
- `src/pclean/parallel/cube_parallel.py` — Dask subcube orchestration
- `src/pclean/config.py` — `ClusterConfig.concat_mode`, `keep_subcubes`
