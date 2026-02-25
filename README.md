# pclean — Parallel CLEAN Imaging with Dask

`pclean` is a modular, Dask-accelerated radio-interferometric imaging package
that wraps CASA's synthesis imaging C++ tools (`casatools`) to provide
transparent parallelism for both **cube** and **continuum** imaging.

## Features

| Feature | Description |
|---------|-------------|
| **Cube parallelism** | Channels are distributed across Dask workers; each worker runs a full imaging + deconvolution pipeline on its sub-cube. |
| **Continuum parallelism** | Visibility rows are chunked across Dask workers for major-cycle gridding; minor cycles run on the gathered/normalized image. |
| **tclean-compatible API** | Drop-in `pclean()` function that accepts the same parameters as CASA `tclean`. |
| **Modular internals** | Every building block (imager, deconvolver, normalizer, partitioner, cluster manager) is importable and reusable. |

## Quick start

```python
from pclean import pclean

# Parallel cube imaging (channels distributed across workers)
pclean(
    vis="my.ms",
    imagename="cube_out",
    specmode="cube",
    imsize=[512, 512],
    cell="1arcsec",
    niter=1000,
    deconvolver="hogbom",
    parallel=True,
    nworkers=8,
)

# Parallel continuum imaging (visibility rows chunked)
pclean(
    vis="my.ms",
    imagename="cont_out",
    specmode="mfs",
    imsize=[2048, 2048],
    cell="0.5arcsec",
    niter=5000,
    deconvolver="mtmfs",
    nterms=2,
    parallel=True,
    nworkers=4,
)
```

## Architecture

```
pclean/
├── src/pclean/
│   ├── pclean.py                  # Top-level tclean-like interface
│   ├── params.py                  # Parameter container & validation
│   ├── imaging/
│   │   ├── serial_imager.py       # Single-process imager (base)
│   │   ├── deconvolver.py         # Deconvolution wrapper
│   │   └── normalizer.py          # Image normalization
│   ├── parallel/
│   │   ├── cluster.py             # Dask cluster lifecycle
│   │   ├── cube_parallel.py       # Channel-parallel cube imaging
│   │   ├── continuum_parallel.py  # Row-parallel continuum imaging
│   │   └── worker_tasks.py        # Pure functions submitted to workers
│   └── utils/
│       ├── partition.py           # Data / image partitioning helpers
│       └── image_concat.py        # Image concatenation
```

## Requirements

* Python ≥ 3.8
* `casatools`, `casatasks`
* `dask[distributed]`
* `numpy`

## License

GPL-3.0-or-later — see [LICENSE](LICENSE) for details.

## AI Disclosure

This project was developed with the assistance of an AI coding agent (GitHub Copilot, Claude). The AI contributed to code generation, debugging, architecture design, and documentation under human direction and review.
