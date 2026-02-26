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
| **CLI support** | Run imaging from the command line via `python -m pclean`. |
| **Modular internals** | Every building block (imager, deconvolver, normalizer, partitioner, cluster manager) is importable and reusable. |

## Quick start

```python
from pclean import pclean

# Parallel cube imaging (channels distributed across workers)
pclean(
    vis='my.ms',
    imagename='cube_out',
    specmode='cube',
    imsize=[512, 512],
    cell='1arcsec',
    niter=1000,
    deconvolver='hogbom',
    parallel=True,
    nworkers=8,
    cube_chunksize=1,       # one sub-cube per channel (max parallelism)
)

# Parallel continuum imaging (visibility rows chunked)
pclean(
    vis='my.ms',
    imagename='cont_out',
    specmode='mfs',
    imsize=[2048, 2048],
    cell='0.5arcsec',
    niter=5000,
    deconvolver='mtmfs',
    nterms=2,
    parallel=True,
    nworkers=4,
)
```

### Command-line interface

```bash
python -m pclean --vis my.ms --imagename out --specmode cube \
    --imsize 512 512 --cell 1arcsec --niter 1000 \
    --parallel --nworkers 8
```

### pclean-specific parameters

In addition to all standard `tclean` parameters, `pclean` accepts:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `parallel` | `False` | Enable Dask-distributed parallelism. |
| `nworkers` | `None` | Number of Dask workers (`None` → CPU count). |
| `scheduler_address` | `None` | Connect to an existing Dask scheduler instead of creating a local cluster. |
| `threads_per_worker` | `1` | Threads per Dask worker (CASA tools are not thread-safe). |
| `memory_limit` | `'auto'` | Per-worker memory cap. |
| `local_directory` | `None` | Dask scratch directory for spill-to-disk. |
| `cube_chunksize` | `-1` | Channels per sub-cube task. `-1` → one sub-cube per worker; `1` → one per channel. |
| `keep_subcubes` | `False` | Preserve intermediate sub-cube images after cube concatenation. |
| `keep_partimages` | `False` | Preserve partial images after continuum gather. |

## Architecture

```
pclean/
├── src/pclean/
│   ├── __init__.py                # Package init, exposes pclean()
│   ├── __main__.py                # CLI entry point (python -m pclean)
│   ├── pclean.py                  # Top-level tclean-like interface
│   ├── params.py                  # Parameter container & validation
│   ├── imaging/
│   │   ├── serial_imager.py       # Single-process imager (base engine)
│   │   ├── deconvolver.py         # Deconvolution wrapper
│   │   └── normalizer.py          # Image normalization (gather/scatter)
│   ├── parallel/
│   │   ├── cluster.py             # Dask cluster lifecycle management
│   │   ├── cube_parallel.py       # Channel-parallel cube imaging
│   │   ├── continuum_parallel.py  # Row-parallel continuum imaging
│   │   └── worker_tasks.py        # Serialisable functions for workers
│   └── utils/
│       ├── partition.py           # Data / image partitioning helpers
│       └── image_concat.py        # Sub-cube image concatenation
```

## Requirements

* Python ≥ 3.10
* `casatools`, `casatasks`
* `dask[distributed]`
* `numpy`

## License

GPL-3.0-or-later — see [LICENSE](LICENSE) for details.

## AI Disclosure

This project was developed with the assistance of an AI coding agent (GitHub Copilot, Claude). The AI contributed to code generation, debugging, and documentation under human direction, correction, and review.
