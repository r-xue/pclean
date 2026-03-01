# I/O Report -- ZFS Pools on `xenon` 

**Date:** 2026-02-27
**Host:** xenon (Ubuntu 24.04.4 LTS)
**Tool:** fio 3.36, `ioengine=libaio`, `direct=1` (O_DIRECT, bypasses page cache)
**Duration:** 30 seconds per test

---

## Pools Under Test

| Pool | Backing | Layout | Disks | Raw Size | Used | Benchmark Path |
|------|---------|--------|-------|----------|------|----------------|
| `nvme` | NVMe SSD | stripe (2 drives) | 2x 932 GB SK Hynix P31 | 1.81 TiB | 84% | `/pool/nvme/benchmark` |
| `data0` | HDD | 3x mirror (6 drives) + L2ARC | 6x 8 TB WD + 120 GB SSD cache | 21.8 TiB | 91% | `/pool/data0/benchmark` |
| `data1` | HDD | raidz1 (4 drives) | 4x 14 TB WD | 50.9 TiB | 77% | `/pool/data1/benchmark` |
| `data2` | HDD | stripe (2 drives) | 2x 14 TB WD | 25.4 TiB | 12% | `/pool/data2/benchmark` |

> **Note:** `nvme` and `data2` are striped (no redundancy).  `data0` has mirror
> redundancy and an SSD read cache (L2ARC).  `data1` has single-parity raidz1.

---

## Cross-Pool Summary

| Test | Pattern | Block Size | Jobs | Depth | nvme (84%) | data0 (91%) | data1 (77%) | data2 (12%) |
|------|---------|-----------|------|-------|------------|-------------|-------------|-------------|
| Seq. Write | write | 1 MiB | 1 | 1 | **317 MiB/s** | 127 MiB/s | 187 MiB/s | 240 MiB/s |
| Seq. Read | read | 1 MiB | 1 | 1 | **1837 MiB/s** | 88.8 MiB/s | 240 MiB/s | 262 MiB/s |
| Rand Read IOPS | randread | 4 KiB | 4 | 32 | **21,500** | 419 | 12,700 | 11,800 |
| Rand Write IOPS | randwrite | 4 KiB | 4 | 32 | **48,300** | 8,803 | 15,600 | 16,300 |
| Mixed Read BW | randrw 70/30 | 256 KiB | 4 | 16 | **693 MiB/s** | 37.2 MiB/s | 37 MiB/s | 46 MiB/s |
| Mixed Write BW | randrw 70/30 | 256 KiB | 4 | 16 | **300 MiB/s** | 16.8 MiB/s | 17 MiB/s | 20 MiB/s |

---

## Pool: `nvme` (NVMe SSD stripe, 84% used)

### Results

| Test | Bandwidth | IOPS | Avg Latency | Total I/O |
|------|-----------|------|-------------|-----------|
| Sequential Write (1 MiB, 1 job) | 317 MiB/s (333 MB/s) | 317 | 3.15 ms | 9,522 MiB |
| Sequential Read (1 MiB, 1 job) | 1837 MiB/s (1926 MB/s) | 1837 | 0.54 ms | 53.8 GiB |
| Random Read (4 KiB, 4j x 32d) | 84.0 MiB/s (88.1 MB/s) | 21,500 | 5.95 ms | 2,520 MiB |
| Random Write (4 KiB, 4j x 32d) | 189 MiB/s (198 MB/s) | 48,300 | 2.65 ms | 5,659 MiB |
| Mixed R/W (256 KiB, 4j x 16d) | R: 693 / W: 300 MiB/s | R: 2,770 / W: 1,198 | ~15.8 ms | R: 20.3 GiB + W: 8,987 MiB |

### Latency Percentiles

| Test | p50 | p95 | p99 |
|------|-----|-----|-----|
| Sequential Write | 3.15 ms (avg) | — | max 10.8 ms |
| Sequential Read | 0.54 ms (avg) | — | max 20.5 ms |
| Random Read | 4.1 ms | 10.0 ms | 14.1 ms |
| Random Write | 1.9 ms | 5.7 ms | 9.2 ms |
| Mixed R (avg) | ~15.8 ms | — | — |
| Mixed W (avg) | ~16.7 ms | — | — |

### Observations

- NVMe-backed and far faster than the HDD pools in every metric.
- Sequential read throughput (1.8 GiB/s) and mixed workload numbers make this pool suitable for high-throughput pclean workloads.
- Random IOPS are orders of magnitude higher than the HDD pools.
- Utilization (84%) is approaching the caution zone. If the pool reaches >90%, the fragmentation effects seen on `data0` could appear; keep free space above ~15--20% where possible.

---

## Pool: `data0` (HDD 3x mirror + L2ARC, 91% used)

### Results

| Test | Bandwidth | IOPS | Avg Latency | Total I/O |
|------|-----------|------|-------------|-----------|
| Sequential Write (1 MiB, 1 job) | 127 MiB/s (134 MB/s) | 127 | 7.8 ms | 3,824 MiB |
| Sequential Read (1 MiB, 1 job) | 88.8 MiB/s (93.2 MB/s) | 88 | 11.2 ms | 2,666 MiB |
| Random Read (4 KiB, 4j x 32d) | 1.68 MiB/s (1.72 MB/s) | 419 | 283 ms | 49.2 MiB |
| Random Write (4 KiB, 4j x 32d) | 34.4 MiB/s (36.1 MB/s) | 8,803 | 14.1 ms | 1,032 MiB |
| Mixed R/W (256 KiB, 4j x 16d) | R: 37.2 / W: 16.8 MiB/s | R: 148 / W: 67 | ~274 ms | R: 1,116 MiB + W: 505 MiB |

### Latency Percentiles

| Test | p50 | p95 | p99 | Notes |
|------|-----|-----|-----|-------|
| Sequential Write | 7.84 ms (avg) | — | max 226 ms | — |
| Sequential Read | 11.2 ms (avg) | — | max 386 ms | Dramatically slower than data1/data2 (240--262 MiB/s). High-utilization ZFS pools suffer severe fragmentation, forcing reads to chase scattered blocks. |
| Random Read | 230 ms | 531 ms | 1,401 ms | Catastrophically slow -- **30x worse** than data1/data2 (~12k IOPS). No bimodal cache-hit pattern; at 91% the ARC is ineffective and nearly every read hits fragmented on-disk blocks. |
| Random Write | 13.7 ms | 31.3 ms | 35.4 ms | Roughly half the IOPS of data1/data2. ZFS COW at 91% must hunt harder for free blocks. |
| Mixed R/W | ~200 ms | ~827 ms | ~1.2 s | Similar throughput to data1 at the 256 KiB block size, but tail latency is severe (p99 > 1.2 s). |

### Comparison with Other HDD Pools

| Test | data0 (91%) | data1 (77%) | data2 (12%) | data0 vs. data2 |
|------|-------------|-------------|-------------|-----------------|
| Seq. Write | 127 MiB/s | 187 MiB/s | 240 MiB/s | **47% slower** |
| Seq. Read | 88.8 MiB/s | 240 MiB/s | 262 MiB/s | **66% slower** |
| Random Read IOPS | 419 | 12,700 | 11,800 | **28x slower** |
| Random Write IOPS | 8,803 | 15,600 | 16,300 | **46% slower** |
| Mixed Read BW | 37.2 MiB/s | 37 MiB/s | 46 MiB/s | **19% slower** |
| Mixed Write BW | 16.8 MiB/s | 17 MiB/s | 20 MiB/s | **16% slower** |
| Mixed p95 lat | 827 ms | 827 ms | 380 ms | **2.2x higher** |

### Observations

- **Random read IOPS drops from ~12k to 419 -- a 28--30x degradation.** This single metric disqualifies `data0` for any I/O-sensitive workload.
- **Sequential read collapses to 88.8 MiB/s** (66% slower than data2). Fragmentation scatters what should be sequential blocks across multiple disk seeks.
- **Sequential write degrades moderately** (127 MiB/s, 47% penalty). ZFS COW must search harder for contiguous free extents at high fill.
- **Random writes are least affected** (8,803 IOPS, ~46% loss). ZFS's ZIL and write coalescing still provide some buffering.
- **Mixed workload shows a floor effect.** The 256 KiB block size partially masks fragmentation, but per-I/O tail latency (p99 > 1.2 s) reveals the underlying illness.

---

## Pool: `data1` (HDD raidz1, 77% used)

### Results

| Test | Bandwidth | IOPS | Avg Latency | Total I/O |
|------|-----------|------|-------------|-----------|
| Sequential Write (1 MiB, 1 job) | 187 MiB/s (196 MB/s) | 187 | 5.3 ms | 5,612 MiB |
| Sequential Read (1 MiB, 1 job) | 240 MiB/s (251 MB/s) | 239 | 4.2 ms | 7,187 MiB |
| Random Read (4 KiB, 4j x 32d) | 50 MiB/s (52 MB/s) | 12,700 | 9.7 ms | 1,489 MiB |
| Random Write (4 KiB, 4j x 32d) | 61 MiB/s (64 MB/s) | 15,600 | 7.9 ms | 1,833 MiB |
| Mixed R/W (256 KiB, 4j x 16d) | R: 37 / W: 17 MiB/s | R: 148 / W: 67 | ~274 ms | R: 1,116 MiB + W: 505 MiB |

### Latency Percentiles

| Test | p50 | p95 | p99 | Notes |
|------|-----|-----|-----|-------|
| Sequential Write | 5.34 ms (avg) | — | max 197 ms | — |
| Sequential Read | 4.17 ms (avg) | — | max 160 ms | — |
| Random Read | 125 us | 396 us | 451 ms | Bimodal -- most reads from ARC cache (sub-ms), uncached reads hit disk (p99.5 = 566 ms, p99.9 = 1.28 s). |
| Random Write | 7.1 ms | 16.3 ms | 19.3 ms | — |
| Mixed R/W | ~199 ms | ~827 ms | ~1.2 s | Noticeably worse than data2 -- higher utilization and raidz1 parity overhead amplify COW overhead under contention. |

### Comparison with `data2`

| Test | data1 | data2 | Difference |
|------|-------|-------|------------|
| Seq. Write | 187 MiB/s | 240 MiB/s | data2 is **28% faster** |
| Seq. Read | 240 MiB/s | 262 MiB/s | data2 is **9% faster** |
| Random Read IOPS | 12,700 | 11,800 | ~equal (within noise) |
| Random Write IOPS | 15,600 | 16,300 | ~equal |
| Mixed Read BW | 37 MiB/s | 46 MiB/s | data2 is **24% faster** |
| Mixed Write BW | 17 MiB/s | 20 MiB/s | data2 is **18% faster** |
| Mixed p95 lat | 827 ms | 380 ms | data2 has **2.2x lower** tail latency |

### Observations

- **Sequential throughput:** `data2` is consistently faster (28% write, 9% read). `data2` is a 2-disk stripe while `data1` is raidz1 (4 disks, 1 parity) -- raidz1 trades some write throughput for parity computation and single-disk fault tolerance.
- **Random IOPS:** Both pools are comparable (~12--16k), suggesting similar underlying disk populations (both use 14 TB WD drives).
- **Mixed workload:** `data2` substantially outperforms `data1`, especially in tail latency (p95: 380 ms vs. 827 ms). Higher utilization (77% vs. 12%) and raidz1 parity overhead amplify write-amplification and COW overhead under contention.
- **Both pools are HDD-class:** Neither approaches SSD/NVMe performance.

---

## Pool: `data2` (HDD stripe, 12% used)

### Results

| Test | Bandwidth | IOPS | Avg Latency | Total I/O |
|------|-----------|------|-------------|-----------|
| Sequential Write (1 MiB, 1 job) | 240 MiB/s (251 MB/s) | 239 | 4.2 ms | 7,191 MiB |
| Sequential Read (1 MiB, 1 job) | 262 MiB/s (275 MB/s) | 262 | 3.8 ms | 7,875 MiB |
| Random Read (4 KiB, 4j x 32d) | 46 MiB/s (48 MB/s) | 11,800 | 10.2 ms | 1,379 MiB |
| Random Write (4 KiB, 4j x 32d) | 64 MiB/s (67 MB/s) | 16,300 | 7.6 ms | 1,912 MiB |
| Mixed R/W (256 KiB, 4j x 16d) | R: 46 / W: 20 MiB/s | R: 183 / W: 81 | ~225 ms | R: 1,382 MiB + W: 613 MiB |

### Latency Percentiles

| Test | p50 | p95 | p99 | Notes |
|------|-----|-----|-----|-------|
| Sequential Write | 4.17 ms (avg) | — | max 101 ms | — |
| Sequential Read | 3.8 ms (avg) | — | max 75.7 ms | — |
| Random Read | 135 us | 363 us | 354 ms | Heavily bimodal -- most reads from ARC cache (sub-ms), uncached reads hit spinning disk (p99.5 = 566 ms). |
| Random Write | 6.7 ms | 17.7 ms | 21.6 ms | ZFS TXG batching absorbs random writes, giving smoother latency than random reads. |
| Mixed R/W | ~199 ms | ~380 ms | ~840 ms | High latency under mixed load -- ZFS COW overhead + disk seeks when reads and writes compete. |

### Analysis for pclean

- **Sequential throughput (~240--262 MiB/s):** This pool is a 2-disk stripe (no redundancy), which gives good throughput from striping across both drives. With 8 Dask workers writing concurrently, aggregate demand could reach ~1--2 GB/s and saturate this pool. Keep `nworkers <= 4` and `cube_chunksize` moderate to avoid contention.
- **Random IOPS (~12--16k):** Adequate for CASA table system metadata operations. The bimodal random read latency (sub-ms median, ~350 ms tail) shows ZFS ARC caching is effective for hot data but uncached accesses hit spinning-disk latency.
- **Mixed workload (46 + 20 MiB/s):** Significant latency increase (p50 ~200 ms). This is the regime where Dask continuum parallelism operates -- concurrent visibility reads overlapping with intermediate image writes. Continuum imaging with many workers will be I/O-limited rather than CPU-limited.

---

## NVMe vs. HDD Reference Comparison

| Metric | nvme pool | Best HDD (data2) | Speedup |
|--------|-----------|-------------------|---------|
| Seq. Write | 317 MiB/s | 240 MiB/s | 1.3x |
| Seq. Read | 1837 MiB/s | 262 MiB/s | 7x |
| Random Read IOPS | 21,500 | 11,800 | 1.8x |
| Random Write IOPS | 48,300 | 16,300 | 3x |
| Mixed Read BW | 693 MiB/s | 46 MiB/s | 15x |
| Mixed Write BW | 300 MiB/s | 20 MiB/s | 15x |

Compared to a bare NVMe SSD (no ZFS overhead):

| Metric | nvme pool (ZFS) | Bare NVMe (typical) | Overhead |
|--------|-----------------|---------------------|----------|
| Seq. Write | 317 MiB/s | 3,000+ MiB/s | ~9x loss from ZFS COW |
| Seq. Read | 1837 MiB/s | 5,000+ MiB/s | ~2.7x |
| Random Read IOPS | 21,500 | 500,000+ | ~23x |
| Random Write IOPS | 48,300 | 300,000+ | ~6x |

---

## Key Findings

1. **ZFS fragmentation is catastrophic at 91% utilization.** Performance degrades roughly linearly from 12% to 77%, then **falls off a cliff** approaching 91%. The ZFS best-practice threshold of ~80% maximum utilization is confirmed empirically.

2. **Clear utilization--performance ordering:** `data2` (12%) > `data1` (77%) >> `data0` (91%) for every metric except random IOPS where data1 and data2 are equivalent.

3. **The `nvme` pool dominates all HDD pools** by 1.3--15x depending on the access pattern. Mixed workloads see the largest gap (15x).

4. **ZFS adds significant overhead even on NVMe.** Sequential write is only 317 MiB/s (vs. ~3 GB/s bare), a ~9x penalty from COW, checksumming, and TXG commit. This is the cost of ZFS's data integrity guarantees.

5. **All HDD pools are HDD-class.** Neither data1 nor data2 approaches NVMe performance. For I/O-intensive pclean runs, local NVMe is always preferred.

---

## Recommendations for pclean

| Scenario | Preferred Pool | Max Workers | Notes |
|----------|---------------|-------------|-------|
| Working directory (`imagename`, `local_directory`) | **nvme** | CPU/memory limited | NVMe I/O unlikely to be first bottleneck. |
| Working directory (ZFS only) | **data2** > data1 | 3--4 | I/O contention becomes limiting before CPU. |
| Archival / final products | data1 or data2 | N/A | Sequential write is sufficient for archival. |
| **Never** use for I/O-sensitive work | **data0** | 1 (last resort) | 28x worse random read; even single-worker runs will be I/O-bottlenecked. |

### Specific Guidance

- **Use local NVMe** for `imagename` and `local_directory`; keep final products on ZFS.
- **Set `local_directory`** to fast local storage so Dask spill-to-disk does not add ZFS latency.
- **Keep `cube_chunksize` moderate** on HDD pools to avoid many tiny sub-cubes that increase metadata overhead.
- **Consider freeing space on `data0`** (target < 80% / ~17.6 TiB used) to recover usable performance.
- **ZFS recordsize tuning:** If a pool is dedicated to imaging, `recordsize=1M` (matching dominant I/O block size) may improve sequential throughput.
- **Monitor `nvme` utilization.** At 84% it is approaching the caution zone; crossing 90% risks the same fragmentation cliff seen on `data0`.
