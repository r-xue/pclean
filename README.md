# pclean — Parallel CLEAN Imaging with Dask

[![tests](https://img.shields.io/github/actions/workflow/status/r-xue/pclean/test.yml?branch=main&logo=github&label=tests)](https://github.com/r-xue/pclean/actions/workflows/test.yml)
[![codecov](https://img.shields.io/codecov/c/github/r-xue/pclean?logo=codecov)](https://codecov.io/gh/r-xue/pclean)

`pclean` is a modular, Dask-accelerated radio-interferometric imaging package
that wraps CASA's synthesis imaging C++ tools (`casatools`) to provide
transparent parallelism for **cube** (channel-distributed) and **continuum**
(row-distributed) imaging workflows.

## Features

| Feature | Description |
|---------|-------------|
| **Cube parallelism** | Channels are distributed across Dask workers; each worker runs a complete imaging and deconvolution cycle on its sub-cube. |
| **Continuum parallelism** | Visibility rows are partitioned across Dask workers for major-cycle gridding; minor cycles run on the gathered, normalized image. |
| **tclean-compatible API** | Drop-in `pclean()` function accepting the same parameters as CASA `tclean`. |
| **CLI support** | Run imaging from the command line via `python -m pclean`. |
| **Modular internals** | Every building block — imager, deconvolver, normalizer, partitioner, cluster manager — is independently importable. |
| **ADIOS2 support** | Convert MeasurementSet columns to `Adios2StMan` with configurable engine type and buffer size for I/O benchmarking. Requires the `casatools` openmpi variant from conda-forge. |

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

### Additional parameters

Beyond the standard `tclean` parameters, `pclean` accepts:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `parallel` | `False` | Enable Dask-distributed parallelism. |
| `nworkers` | `None` | Number of Dask workers. `None` defaults to the available CPU count. |
| `scheduler_address` | `None` | Address of an existing Dask scheduler; when set, no local cluster is created. |
| `threads_per_worker` | `1` | Threads per Dask worker. Kept at 1 because CASA tools are not thread-safe. |
| `memory_limit` | `'0'` | Per-worker memory cap. `'0'` disables Dask memory management, preventing CASA C++ allocations from being paused or killed. |
| `local_directory` | `None` | Scratch directory for Dask spill-to-disk. |
| `cube_chunksize` | `-1` | Channels per sub-cube task. `-1` assigns one sub-cube per worker; `1` assigns one per channel. |
| `keep_subcubes` | `False` | Retain intermediate sub-cube images after concatenation. |
| `keep_partimages` | `False` | Retain partial images after continuum gather. |

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
│       ├── image_concat.py        # Sub-cube image concatenation
│       ├── memory_estimate.py     # Worker RAM estimation heuristics
│       ├── check_adios2.py        # Adios2StMan availability check
│       └── convert_adios2.py      # MS → ADIOS2 conversion utility
```

## Documentation

Design notes and technical references are maintained in [`docs/`](docs/):

### Design and parallelism

| Document | Description |
|----------|-------------|
| [code_structure.md](docs/code_structure.md) | Module-level code overview |
| [parallelization.md](docs/parallelization.md) | Cube vs. continuum parallelization architecture and diagrams |
| [per_channel_convergence.md](docs/per_channel_convergence.md) | Per-channel convergence strategy |
| [image_concatenation.md](docs/image_concatenation.md) | Sub-cube concatenation details |
| [briggsbwtaper.md](docs/briggsbwtaper.md) | `briggsbwtaper` weighting analysis and parallel-mode fix |
| [memory_estimation.md](docs/memory_estimation.md) | RAM estimation heuristics for worker sizing |
| [memory_management.md](docs/memory_management.md) | Why Dask memory management is disabled (`memory_limit='0'`) |

### ADIOS2 and I/O

| Document | Description |
|----------|-------------|
| [check_adios2.md](docs/check_adios2.md) | Verifying Adios2StMan availability |
| [adios2_convert.md](docs/adios2_convert.md) | Converting an MS to the ADIOS2 storage backend |
| [io_consideration.md](docs/io_consideration.md) | I/O bottleneck analysis and ZFS pool considerations |
| [runner_benchmark.md](docs/runner_benchmark.md) | Lightweight `fio` recipes for storage benchmarking |

### Development

| Document | Description |
|----------|-------------|
| [dev_guide.md](docs/dev_guide.md) | Development workflow: pixi environments, linting, testing |

## Requirements

* Python ≥ 3.10
* `casatools` ≥ 6.5
* `dask` + `distributed`
* `numpy`

### Pixi environments

The project uses [pixi](https://pixi.sh/) for reproducible environment
management.  Four environments are defined in `pyproject.toml`:

| Environment | Features | Description |
|-------------|----------|-------------|
| `default` | `casa` | Runtime with `casatools`/`casatasks` from PyPI. |
| `default-forge` | `casa-forge` | Runtime with `casatools`/`casatasks` from conda-forge (includes the openmpi variant required for `Adios2StMan`). |
| `dev` | `casa`, `dev` | Runtime plus pytest, pytest-cov, and ruff. |
| `test` | `dev` | Linting and testing only (no `casatools`). |

Common tasks are exposed as pixi scripts:

```bash
pixi run -e dev test          # pytest -v
pixi run -e dev test-cov      # pytest with coverage
pixi run -e dev lint          # ruff check
pixi run -e dev fmt           # ruff format
```

## References and acknowledgements

`pclean` builds on the imaging and calibration infrastructure developed by
the CASA team at NRAO / ESO / NAOJ.  The parallel imaging design draws on
ideas described in [CASA Memo 13](https://casadocs.readthedocs.io/en/stable/notebooks/memo-series.html).

If this package contributes to published research, please cite the CASA
software:

> CASA Team, Bean, B., Bhatnagar, S., et al. 2022,
> "CASA, the Common Astronomy Software Applications for Radio Astronomy,"
> *PASP*, 134, 114501.
> [doi:10.1088/1538-3873/ac9642](https://doi.org/10.1088/1538-3873/ac9642)

> McMullin, J. P., Waters, B., Schiebel, D., Young, W., & Golap, K. 2007,
> "CASA Architecture and Applications,"
> *ASP Conf. Ser.*, 376, 127.
> [ads:2007ASPC..376..127M](https://ui.adsabs.harvard.edu/abs/2007ASPC..376..127M)

Related memo:

> Sekhar, S., Rau, U., & Xue, R. 2024, "CASA Memo 13 — Cube Parallelization with CASA,"
> NRAO. [casadocs](https://casadocs.readthedocs.io/en/latest/notebooks/memo-series.html)

## License

Copyright 2025 the pclean authors.

GPL-3.0-or-later — see [LICENSE](LICENSE) for details.

## Disclaimer

This project is an independent, personal effort developed on the authors' own
time.  It is not affiliated with, endorsed by, or conducted as part of any
employer's projects or responsibilities.

## AI Disclosure

This project was developed with the assistance of AI coding agents
(GitHub Copilot, Claude).  The AI contributed to code generation, debugging,
and documentation under human direction and review.
