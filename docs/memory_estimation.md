# Memory Estimation Heuristics

## Module

`pclean.utils.memory_estimate` — heuristic RAM estimator for parallel CASA imaging workers.

## API

| Function | Purpose |
|---|---|
| `estimate_worker_memory_gib()` | Peak RAM for a single worker given `imsize`, `nchan_per_task`, `gridder`, `deconvolver`, `nterms`, `nfields` |
| `estimate_peak_ram_gib()` | Total system RAM for *N* concurrent workers (adds 0.5 GiB scheduler overhead) |
| `recommend_nworkers()` | Max workers that fit in available RAM (auto-detected or supplied), with a configurable safety factor |

## Core Model

Calibrated from IRC+10216 ALMA Band 6 cube-imaging logs (8000 × 8000, 40 antennas, 449 280 rows, `gridder='standard'`, `deconvolver='hogbom'`, 1 channel per task):

$$\text{mem\_per\_worker} = 0.7\;\text{GiB} + \frac{n_x \cdot n_y \cdot n_\text{chan} \cdot 76\;\text{B/pix} \cdot f_\text{gridder} \cdot n_\text{terms}^2}{2^{30}}$$

where:

- **0.7 GiB** — Python + Dask worker process baseline overhead (constant per worker).
- **76 B/pixel/channel** — empirical constant for the `standard` gridder, accounting for ~8–10 float/complex buffers CASA keeps simultaneously:

| Buffer | Dtype | Bytes/pixel |
|---|---|---|
| Complex visibility grid | complex64 | 8 |
| Weight grid | complex64 | 8 |
| FFT workspace (in + out) | complex64 | 16 |
| Residual image | float32 | 4 |
| Model image | float32 | 4 |
| PSF image | float32 | 4 |
| Weight image (sumwt) | float32 | 4 |
| Primary beam (PB) | float32 | 4 |
| Mask | float32 | 4 |
| Temporary / bookkeeping | mixed | ~20 |

## Scaling Factors

- **Mosaic gridder** (`f_gridder = 2.0`): each pointing requires a convolution function (CF) table; memory also grows sub-linearly with the number of fields as `1 + 0.1 × (√nfields − 1)`.
- **MTMFS deconvolver**: internal Hessian products scale as `nterms²`. With `nterms=1`, memory matches `hogbom`.
- **Multi-channel sub-cubes**: linear in `nchan_per_task`.
- **MS row count**: negligible — visibilities are processed in row chunks occupying a few MB, dwarfed by multi-GiB image grids.

### Gridder multipliers

| Gridder | Factor |
|---|---|
| `standard` | 1.0 |
| `wproject` | 1.2 |
| `widefield` | 1.3 |
| `mosaic` | 2.0 |
| `awproject` | 2.5 |

## Calibration Point

From the IRC+10216 reference run:

```
4.9 GiB / (8000 × 8000 × 1 chan) ≈ 76 B/pix/chan
```

Each worker consumed ~4.9 GiB of C++ (unmanaged) memory + ~0.7 GiB Python/Dask overhead = **~5.6 GiB total**.

## Example Usage

```python
from pclean.utils.memory_estimate import (
    estimate_worker_memory_gib,
    estimate_peak_ram_gib,
    recommend_nworkers,
)

# Single worker: 8000×8000, standard gridder, 1 chan
mem = estimate_worker_memory_gib(imsize=8000, nchan_per_task=1)
# ≈ 5.2 GiB

# 12 workers total system RAM
total = estimate_peak_ram_gib(nworkers=12, imsize=8000, nchan_per_task=1)
# ≈ 63 GiB

# How many workers fit in 64 GiB?
n = recommend_nworkers(available_ram_gib=64.0, imsize=8000)
# → 10
```

## Tests

19 unit tests in `tests/test_memory_estimate.py` covering:

- IRC+10216 calibration point validation (4.5–6.5 GiB range)
- Scalar vs rectangular `imsize` handling
- Linear `nchan_per_task` scaling
- Mosaic > standard memory, multi-field sub-linear growth
- MTMFS `nterms²` scaling, `nterms=1` ≡ `hogbom`
- Unknown gridder defaults to `standard` factor
- Small images dominated by base overhead
- `recommend_nworkers` with auto-detected RAM, safety factor control
