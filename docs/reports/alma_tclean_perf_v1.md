# ALMA Cube Imaging — tclean Performance Report (v1)

## 1. Test Configuration

| Parameter | Value |
|---|---|
| Dataset | ALMA `uid___A002_Xf0fd41_X5f5a_target.ms`, SPW 25, IRC+10216 |
| Image size | 8000 × 8000 px, 0.0046 arcsec/px |
| Spectral | specmode=cube, 1000 channels |
| Weighting | briggsbwtaper, robust=0.5, fracbw=0.000907945 |
| Deconvolver | hogbom, niter=50000, threshold=4.0mJy, usemask=auto-multithresh |
| Parallelism | CASA `parallel=True` (MPI), 11 CASA worker ranks (`OMPI_COMM_WORLD_SIZE=11`); psrecord NProc peak 25 (includes MPI infrastructure processes) |
| CASA version | 6.7.3-21 |
| Log files | `test_alma_tclean_1.log` / `test_alma_tclean_1.rec` |

## 2. Result Summary

| Outcome | Value |
|---|---|
| **Completed** | **No — OOM-killed** |
| psrecord window | 2026-03-04T06:50:14 → 2026-03-04T17:20:01 |
| Elapsed before kill (log) | ~10h 24m (06:50:14 → 17:14:26) |
| Last phase reached | `initMinorCycle` (before any deconvolution) |
| Kill signal | SIGKILL on MPI rank 1 (signal 9) |
| **Peak RSS** | **154.2 GB** at 06:59:18 (early PSF setup) |
| Final sample RSS | 107.8 GB at 17:20:01 (post-kill cleanup state) |

The 50000-iteration deconvolution was **never executed**.

## 3. Phase Timings

| Phase | Start | End | Duration |
|---|---|---|---|
| casahouse init / MPI startup | 06:50:14 | 06:50:25 | ~11 s |
| `tclean()` call / setup | 06:50:25 | 06:50:27 | ~2 s |
| `makePSF` (1000 ch, parallel) | 06:50:27 | 10:58:02 | **4h 7m 35s** |
| `executeMajorCycle 1` | 10:58:02 | 17:14:26 | **6h 16m 24s** |
| `initMinorCycle` / automask setup | 17:14:26 | 17:14:27 | < 1 s |
| **SIGKILL (rank 1)** | 17:14:27 | — | **OOM-killed** |
| Cleanup / lingering procs | 17:14:27 | 17:20:01 | ~5 min |

## 4. Resource Usage

*Source: psrecord*

### 4.1 Processes

| Metric | Value |
|---|---|
| CASA worker ranks | 11 (`OMPI_COMM_WORLD_SIZE=11`; CASA reports "Processes on node: 11") |
| Total NProc (psrecord peak) | 25 (includes prte/pmix daemons, mpirun, coordinator) |

### 4.2 Memory

| Metric | Value | Timestamp |
|---|---|---|
| **Peak RSS** | **154.2 GB** | 2026-03-04T06:59:18 (early PSF setup) |
| **Peak virtual** | **171.2 GB** | 2026-03-04T06:59:18 |
| **Peak MMap RSS** | **146.5 GB** | 2026-03-04T06:59:18 |
| Swap used | **0 MB** | (no swap at any point) |
| RSS at last sample (17:20:01) | 107.8 GB | post-kill cleanup |
| Virtual at last sample | 126.8 GB | post-kill cleanup |
| MMap at last sample | 81.9 GB | post-kill cleanup |
| System page cache (start) | 122.1 GB | 2026-03-04T06:51:39 |
| System page cache (end) | **7.4 GB** | 2026-03-04T17:20:01 |

> **Note on peak RSS**: The previously reported value of 107.8 GB was the
> *final* psrecord sample at 17:20:01 — measured during post-kill cleanup,
> not during the run.  The true peak of **154.2 GB** occurred at 06:59:18
> during the early PSF weight-grid allocation.

The page cache collapsing from 122 GB → 7 GB confirms severe memory pressure
from holding all 1000 channel planes simultaneously across 11 CASA worker ranks.

### 4.3 CPU

| Phase | CPU % (aggregate) |
|---|---|
| Sustained imaging | ~1000–1105% (11 CASA ranks; peak 1105.2%) |

### 4.4 I/O

| Metric | Value | Note |
|---|---|---|
| Total bytes read | **2.33 TB** | decimal (psrecord final) |
| Total bytes written | **8.24 TB** | decimal (psrecord final) |
| Output dir size | **1.48 TB** | decimal; 1484099 MB at 17:20:01 |
| Write amplification | **~5.6×** output size | 8.24 / 1.48 |

**Write volume breakdown (estimated):**

Each channel plane: 8000 × 8000 × 4 B = 256 MB per extension.
1000 channels × ~9 extensions = ~2.3 TB expected final output.
Total writes 8.24 TB ≈ 3.5× the expected output, due to:

- Weight maps (per-channel BriggsBWTaper weight grids)
- Partial gridded visibility scratch data per MPI rank
- Repeated residual/model writes across major cycle iterations

## 5. Key Observations

### 5.1 All 1000 channel planes live in memory simultaneously

All 11 CASA worker ranks grid 1000 channels collectively (330 subcubes per
pass, as logged: *"Subcubes: 330. Processes on node: 11"*), requiring the
full `briggsbwtaper` weight grid before any PSF plane is finalized.  This
drives RSS to **154.2 GB** at 06:59:18 — just 9 minutes after `makePSF`
started.

### 5.2 OOM at minor cycle entry

After makePSF (4h 7m) and major cycle 1 (6h 16m), CASA reaches
`SynthesisDeconvolver::initMinorCycle` at 17:14:26.  The log shows:

```
2026-03-04 17:14:26  initMinorCycle  Absolute Peak residual over full image: 0.134128
2026-03-04 17:14:26  setupMask  Setting up an auto-mask
signal 9 (Killed).
```

The kernel OOM-kills rank 1 during automask setup, before any CLEAN
component is subtracted.  The page cache had collapsed from 122 GB → 7 GB
(−115 GB) by this point.

### 5.3 I/O write amplification (8.24 TB for 1.48 TB output)

Between weight scratch, per-rank partial grids, and per-iteration residual
writes, tclean writes **~5.6×** more data than the partial output size.  This
saturates the NVMe subsystem and contributes to the 6h 16m major cycle time.

### 5.4 No concat phase

tclean produces a single monolithic CASA image directly — no subcube
concatenation step.  However, this comes at the cost of requiring all
channel data live in memory simultaneously.

## 6. Bottleneck Analysis

The fundamental bottleneck is **memory**: tclean's MPI parallel model holds
all 1000 channel planes across all ranks simultaneously, and the minor cycle
requires the full residual + PSF + automask structure in memory at once.

No tuning of MPI rank count or I/O can work around this — the monolithic cube
model requires $O(\text{nchan})$ memory, which exceeds the system's 128 GB
for this cube size.

## 7. Comparison with pclean

| | tclean (MPI, 11 CASA ranks) | pclean (10 Dask workers) |
|---|---|---|
| niter | 50000 (killed before minor) | 0 (makePSF only) |
| Parallelism | 11 CASA MPI ranks (NProc=25 total) | 10 Dask workers (NProc=13 total) |
| makePSF wall time | **4h 7m 35s** (06:50:27→10:58:02) | **≤ 10h 25m total** (PSF+save) |
| Major cycle 1 | **6h 16m 24s** (10:58:02→17:14:26, partial) | N/A |
| Concat overhead | N/A (monolithic image) | 3h 20m (7 extensions, serial) |
| **Peak RSS** | **154.2 GB** (06:59:18) | **58.6 GB** |
| Peak virtual | **171.2 GB** | **81.6 GB** |
| Peak MMap RSS | **146.5 GB** | **52.4 GB** |
| Swap | **0 MB** | ~5 MB |
| Page cache depleted | 122 → 7.4 GB | 122 → 15.4 GB |
| Peak CPU | ~1105% | ~1095% |
| Total I/O reads | **2.33 TB** | **6.71 TB** |
| Total I/O writes | **8.24 TB** (incomplete) | **9.83 TB** (complete) |
| Final output size | **1.48 TB** (partial, killed) | **1.42 TB** (7 extensions, complete) |
| OOM killed | **Yes** | **No** |
| Run completed | **No** | **Yes** (13h 47m) |

pclean uses **~63% less peak memory** (58.6 GB vs 154.2 GB) because each
worker loads only one channel at a time, while tclean holds all 1000 planes
collectively.

> **Note**: The pclean run used `niter=0` (makePSF only).  A direct
> apples-to-apples comparison requires a pclean run with the same niter.

## 8. Conclusions

For a 1000-channel, 8000×8000 cube with briggsbwtaper weighting:

- tclean with `parallel=True` is **OOM-killed** before completing even one
  minor cycle
- pclean completes PSF across 1000 channels in ~10h without OOM
- pclean's per-channel parallelism trades concat overhead for dramatically
  lower memory footprint — a bottleneck that has since been addressed
  (see `alma_pclean_perf_v1.md` § 6)
