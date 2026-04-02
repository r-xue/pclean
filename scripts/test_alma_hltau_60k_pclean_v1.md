# ALMA HL-Tau 60k Cube Imaging — pclean v1

Config file: `scripts/test_alma_hltau_60k_pclean_v1.yaml`

## How to Run

### Prerequisites

- Access to `cvpost` (or equivalent SLURM cluster)
- Input MS: `/users/rxue/Workspace/naasc/datasets/alma_if/hltau_wsu/hltau_60k.0.ms`
- pclean installed via pixi (`forge` environment)

### Steps

**1. Enter the pclean project directory and activate the environment**

```bash
cd /users/rxue/Workspace/naasc/tickets/pclean/pclean
pixi shell -e forge
```

**2. Submit the imaging job**

```bash
pclean submit scripts/test_alma_hltau_60k_pclean_v1.yaml
```

This submits a two-layer SLURM stack:
- One **coordinator** job (`test_alma_hltau_60k_pclean_v1-coordinator`, 4 CPUs, 60 GB, up to 14 days) that hosts the Dask scheduler, partitions the 2200-channel cube into sub-cubes, dispatches tasks to workers, and concatenates results.
- Up to 16 **worker** jobs (`test_alma_hltau_60k_pclean_v1-worker`, 20 GB each) that each image one sub-cube independently.

Output images and logs are written to:

```
/users/rxue/Workspace/naasc/tests/misc/hltau_cube_imaging/alma_hltau_60k_pclean_v1/
```

**3. Monitor jobs**

```bash
squeue --me
```

The coordinator job appears first; worker jobs are spawned automatically by `dask-jobqueue` as the run proceeds.

**4. Override the output directory (optional)**

```bash
pclean submit scripts/test_alma_hltau_60k_pclean_v1.yaml \
    --workdir /path/to/custom/workdir
```

---

## Key Configuration Highlights

| Parameter | Value | Notes |
|-----------|-------|-------|
| `specmode` | `cube` | Per-channel imaging |
| `nchan` | 2200 | Subset of full 59 599-channel cube |
| `imsize` | 2250 × 2250 px | Full field |
| `cell` | 0.025 arcsec | |
| `weighting` | `briggsbwtaper`, robust 0.5 | |
| `deconvolver` | `hogbom` | |
| `usemask` | `auto-multithresh` | |
| `niter` | 0 | Dirty image only (no deconvolution) |
| `threshold` | 30 mJy | Per-plane RMS ≈ 14 mJy |
| `nworkers` | 16 | One SLURM job per sub-cube |
| `job_mem` | 20 GB | Per-worker SLURM memory |
| `psrecord` | true | CPU/memory profiling enabled |

To run a shallow clean instead of a dirty image, set `niter` to a
non-zero value and adjust `threshold` (e.g. `6.5mJy` for a moderate
clean, `2.0mJy` for an aggressive clean).

---

## Sizing CheatSheet

cvpost specs: 
core per memory: 490GiB / 24 cores = ~20 GiB/core
core per walltime: 14 days / 24 cores = ~14 days/core


```python
from pclean.utils.memory_estimate import (
    estimate_worker_memory_gib,
    estimate_peak_ram_gib,
    recommend_nworkers,
)

# Single worker: 8000×8000, standard gridder, 1 chan
mem = estimate_worker_memory_gib(imsize=2250, nchan_per_task=50)
# ≈ 19 GiB
```

## Potential Arrangement

### Option A: shorter walltime, more workers, one pass

Assume 50 channels per worker, then 1192 workers total for 59599 channels if done in one "pass"
nworkers: 59599 channels / 50 chan per worker = 1192 workers
Pass is defined as a work finished imaging a specific subcube chunk

### Option B: longer walltime, restricted by core slots availability, multiple passes 

Assuming 2 cvpost nodes: 
- 16 workers: 16 cores
- 1 coordinator 4 cores

The number of passes needed to do 59599 channels is 59599 / 50 / 16: 75 passes

