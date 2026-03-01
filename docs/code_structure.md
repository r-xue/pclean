# Code Structure

```text
pclean/
├── pyproject.toml
├── README.md
├── src/pclean/
│   ├── __init__.py / __main__.py      — package entry + CLI
│   ├── pclean.py                      — tclean-compatible top-level API
│   ├── params.py                      — parameter container & validation
│   ├── imaging/
│   │   ├── serial_imager.py           — single-process imager (base engine)
│   │   ├── deconvolver.py             — standalone deconvolution wrapper
│   │   └── normalizer.py              — image normalization wrapper
│   ├── parallel/
│   │   ├── cluster.py                 — Dask LocalCluster / Client management
│   │   ├── cube_parallel.py           — channel-parallel cube engine
│   │   ├── continuum_parallel.py      — row-parallel continuum engine
│   │   └── worker_tasks.py            — pure functions + actors for Dask workers
│   └── utils/
│       ├── partition.py               — data/image partitioning via synthesisutils
│       └── image_concat.py            — sub-cube concatenation
└── tests/
    ├── test_params.py                 — 12 tests (all pass)
    └── test_imager.py                 — 3 tests with mocked casatools (all pass)
```


## Key Design Decisions

| Aspect | Approach |
| --- | --- |
| **casatools direct** | Wraps `synthesisimager`, `synthesisdeconvolver`, `synthesisnormalizer`, and `iterbotsink` directly — no `casatasks` dependency at runtime. |
| **Cube parallelism** | Channels partitioned via `synthesisutils.cubedataimagepartition()`; each Dask worker runs a full independent `SerialImager` on its sub-cube; results concatenated with `imageconcat`. |
| **Continuum parallelism** | Visibility rows chunked via `synthesisutils.contdatapartition()`; workers run major cycles in parallel as Dask actors; coordinator gathers/normalizes, runs serial minor cycles, and scatters model back. |
| **Modularity** | Every component (imager, deconvolver, normalizer, cluster, partitioner) is independently importable and reusable. |
| **Serialization** | `PcleanParams.to_dict()`/`from_dict()` enables safe transfer to Dask workers — avoids C++ tool pickling issues. |
| **Interface** | `pclean()` function signature matches `tclean`'s 73 parameters + 5 Dask-specific extras; also works as `python -m pclean` CLI. |

---

See the individual module docstrings for detailed explanations of each parallelism strategy.