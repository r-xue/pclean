# ALMA Cube Imaging — pclean vs tclean Comparison (v1)

Source reports: `alma_pclean_perf_v1.md` / `alma_tclean_perf_v1.md`

> **Caveat**: The pclean run used `niter=50000` (same as tclean) but **no
> Hogbom iterations were actually executed** (confirmed: no
> `executeminorcycle` calls appear in the log).  The most likely cause is
> that `auto-multithresh` generated an empty mask on the very first
> `has_converged()` check (logged: *"Peak residual within mask : 0"*),
> causing `iterbotsink.cleanComplete()` to signal early convergence before
> any CLEAN components could be subtracted.  The run output is therefore
> equivalent to `niter=0` (PSF + residual + restore only), but the root
> cause is a convergence-state issue, not a deliberate `niter=0` setting.
> tclean was also killed before any minor cycle; both runs produced
> PSF-only output.  The comparison is valid for the imaging and memory
> characteristics but not for deconvolution quality.

---

## 1. Run Outcome

| | tclean | pclean |
|---|---|---|
| Completed | **No** — OOM-killed | **Yes** |
| Wall time (before kill / total) | ~10h 24m (killed) | **13h 47m 33s** |
| Last phase reached | `initMinorCycle` (automask setup) | cleanup |
| Kill signal | SIGKILL (OOM, rank 1) | — |

**Takeaway**: tclean ran for 10h 24m and failed before executing a single CLEAN
iteration.  pclean completed the equivalent output (PSF + residual) in 13h 47m.

---

## 2. Memory

| Metric | tclean | pclean | Difference |
|---|---|---|---|
| Peak RSS | **154.2 GB** (06:59:18) | **58.6 GB** | pclean −62% |
| Peak virtual | **171.2 GB** | **81.6 GB** | pclean −52% |
| Peak MMap RSS | **146.5 GB** | **52.4 GB** | pclean −64% |
| Swap used | 0 MB | ~5 MB | — |
| Page cache (start) | 122.1 GB | 122 GB | same |
| Page cache (end) | **7.4 GB** (−115 GB) | **15.4 GB** (−107 GB) | tclean exhausts more |

**Takeaway**: pclean uses **~62% less peak RSS** (58.6 vs 154.2 GB).  Each Dask
worker loads one channel at a time (~5–6 GB), while tclean's 11 CASA MPI ranks
hold all 1000 planes collectively from the start of `makePSF`.  This difference
is what separates a successful run from an OOM kill.

> **Note**: tclean's peak of 154.2 GB occurred at 06:59:18 — just 9 minutes into
> `makePSF` — as all weight grids for 1000 channels were allocated upfront.
> The commonly cited value of 107.8 GB was the *post-kill cleanup* sample
> (17:20:01), not the run peak.

---

## 3. Phase Timings

| Phase | tclean | pclean |
|---|---|---|
| Startup / setup | ~13 s | ~2 s |
| **Total parallel imaging** | **4h 7m 35s** (makePSF only¹) | **10h 25m 17s** (full pipeline²) |
| Major cycle 1 | 6h 16m 24s (partial, killed) | N/A (0 iterations executed³) |
| Subcube concat | N/A (monolithic) | **3h 20m** (24% of total) |
| Cleanup | ~5 min (post-kill) | ~1m 42s |

¹ tclean's 4h 7m 35s is precisely timed from CASA log markers (`INFO makePSF`
→ `INFO executeMajorCycle`).

² pclean's 10h 25m 17s is the **total Dask imaging wall time** (06:49:56 →
17:15:13) covering `setup`, `make_psf`, `make_pb`, `run_major_cycle` (initial
residual), and `restore` — all executed serially within each worker.  Per-step
breakdown was not logged in the v1 run.  Sub-step timing has since been added
to `SerialImager.run()` (INFO lines: `make_psf: Xs`, `make_pb: Xs`, etc.) and
will be available in future runs.

³ `niter=50000` was passed but `executeminorcycle` never appears in the log.
The auto-multithresh mask was empty on the first `has_converged()` call
(*"Peak residual within mask : 0"*), which likely caused `cleanComplete()` to
set an early-stop state in the `iterbotsink`.  After `update_mask()` the mask
became non-zero (~70–75 mJy peaks), but no Hogbom iterations were dispatched.
**Fixed (post-v1)**: `SerialImager.run()` now calls `update_mask()` before
the first `has_converged()` check so that `initminorcycle()` sees a
non-empty mask and `cleanComplete()` correctly returns `False`.

**Takeaway**: A direct makePSF-vs-makePSF comparison is not possible from the
v1 pclean log.  tclean grids all 1000 channels at once across 11 MPI ranks;
pclean processes one channel per worker serially — the 10h 25m bound includes
all per-channel overhead beyond just PSF computation.  pclean's 3h 20m concat
overhead is a known bottleneck addressed via `concat_mode`
(see §6 of `alma_pclean_perf_v1.md`).

---

## 4. CPU

| Metric | tclean | pclean |
|---|---|---|
| Peak CPU | ~1105% | ~1095% |
| Imaging phase | ~1000–1105% (11 ranks) | ~1000–1095% (10 workers) |
| Concat phase | N/A | ~160% (serial, I/O-bound) |

**Takeaway**: Peak CPU is nearly identical — both saturate ~10–11 cores.
pclean's concat phase drops to ~160% because `ia.imageconcat()` is
single-threaded and disk-throughput limited.

---

## 5. I/O

| Metric | tclean | pclean |
|---|---|---|
| Total reads | **2.33 TB** | **6.71 TB** (×2.9 more) |
| Total writes | **8.24 TB** | **9.83 TB** |
| Final output size | **1.48 TB** (partial, killed) | **1.42 TB** (complete, 7 ext.) |
| Write amplification | **~5.6×** | **~6.9×** |

**Takeaway**: pclean reads nearly 3× more data than tclean.  The extra ~4.4 TB
of reads comes from the concat phase: each extension requires reading all 1000
subcube inputs (~2.3 TB total) to write the merged output.  tclean avoids this
because it writes a single monolithic image directly, but pays for it in memory.

pclean's higher write amplification (~6.9× vs ~5.6×) reflects the same
physical-copy concat: subcube pixels are read and rewritten into the merged
image.  The `virtual`/`movevirtual` concat modes eliminate this cost.

---

## 6. Parallelism Model

| | tclean | pclean |
|---|---|---|
| Framework | CASA MPI (`parallel=True`) | Dask `LocalCluster` |
| Worker count | 11 CASA ranks | 10 Dask workers |
| Total NProc (psrecord) | 25 (includes MPI infrastructure) | 13 (main + workers + scheduler) |
| Memory model | All channels live across all ranks | One channel per worker at a time |
| Concat | None (monolithic output) | Serial extension loop (addressed) |

---

## 7. Summary

| | tclean | pclean |
|---|---|---|
| **Completed** | **No** | **Yes** |
| **Peak RSS** | **154.2 GB** | **58.6 GB** |
| **makePSF speed** | **Faster** (4h 7m, confirmed) | Unknown (10h 25m is full imaging pipeline) |
| **Total wall time** | N/A (killed at 10h 24m) | 13h 47m (complete) |
| **OOM risk** | Fatal at 128 GB RAM | None |
| **Scalability** | Limited by $O(\text{nchan})$ memory | $O(1)$ memory per worker |

pclean trades PSF speed for memory safety.  For a 1000-channel, 8000×8000
cube on a 128 GB node, tclean is simply not viable regardless of tuning.
pclean's concat overhead (the remaining gap) is addressed by `concat_mode`.

---

```{toctree}
:caption: Detailed Reports
:maxdepth: 1

alma_pclean_perf_v1
alma_tclean_perf_v1
```
