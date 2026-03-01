# Configuration Architecture

This document describes the current configuration system in pclean.

## Overview

pclean uses a **single-source-of-truth** configuration built on
[pydantic v2](https://docs.pydantic.dev/latest/) `BaseModel` classes.
The top-level `PcleanConfig` groups all parameters into nine logical
sub-models and provides:

- Built-in defaults matching CASA `tclean` behaviour.
- YAML file I/O for reproducible imaging runs.
- Layered composition (base + overlay merging).
- Backward-compatible flat-kwargs interface for the `pclean()` function.
- `to_casa_*()` bridge methods that translate user-facing parameters
  into the CASA-native dicts consumed by the C++ synthesis tools.

## Sub-model Hierarchy

```
PcleanConfig
├── selection:      SelectionConfig      # vis, field, spw, timerange, ...
├── image:          ImageConfig          # imagename, imsize, cell, specmode, nchan, ...
├── grid:           GridConfig           # gridder, facets, wprojplanes, pblimit, ...
├── weight:         WeightConfig         # weighting, robust, noise, uvtaper, ...
├── deconvolution:  DeconvolutionConfig  # deconvolver, scales, masking params, ...
├── iteration:      IterationConfig      # niter, gain, threshold, nmajor, ...
├── normalization:  NormConfig           # pblimit, normtype, psfcutoff
├── misc:           MiscConfig           # restart, savemodel, calcres, calcpsf
└── cluster:        ClusterConfig        # parallel, nworkers, type, ...
                    └── slurm: SlurmConfig   # queue, account, walltime, ...
```

All fields carry typed defaults.  Constructing `PcleanConfig()` with no
arguments produces a valid configuration equivalent to `tclean` defaults.

## Four Ways to Build a Config

### 1. Direct Python Construction

```python
from pclean.config import PcleanConfig, ImageConfig, IterationConfig

config = PcleanConfig(
    image=ImageConfig(imagename='test', imsize=[512, 512], cell='0.5arcsec'),
    iteration=IterationConfig(niter=500, threshold='1mJy'),
)
```

### 2. YAML File

```python
config = PcleanConfig.from_yaml('my_config.yaml')
```

A YAML file mirrors the sub-model hierarchy:

```yaml
selection:
  vis: my_data.ms
  field: '0'
image:
  imagename: output
  imsize: [1024, 1024]
  cell: 0.5arcsec
  specmode: cube
  nchan: 128
iteration:
  niter: 1000
cluster:
  parallel: true
  nworkers: 8
```

Only fields that differ from the defaults need to be specified.

### 3. Layered Composition (Merge)

```python
base = PcleanConfig.from_yaml('defaults.yaml')
overlay = PcleanConfig.from_yaml('my_overrides.yaml')
config = PcleanConfig.merge(base, overlay)
```

Later configs win.  Merging is deep — nested sub-model fields are
merged recursively rather than replaced wholesale.

### 4. Flat Keyword Arguments (Backward Compat)

The `pclean()` function still accepts the 80+ flat keywords familiar
from CASA `tclean`.  Internally it calls:

```python
config = PcleanConfig.from_flat_kwargs(vis='my.ms', imagename='test', ...)
```

which routes each keyword into the correct sub-model.  When a `--config`
YAML file is also provided, the flat kwargs override the file values.

## Merge Order

When multiple sources are provided the merge priority is (highest wins):

1. Explicit keyword arguments / CLI flags
2. `--config` YAML file
3. `--preset` (later `--preset` flags override earlier ones)
4. Built-in pydantic defaults

## Presets

Named presets live under `src/pclean/configs/presets/` as YAML files
(e.g. `vlass.yaml`) and are **bundled inside the wheel** via
`tool.setuptools.package-data`.  They can be loaded via:

```python
from pclean.config import load_preset
config = load_preset('vlass')
```

or from the CLI:

```bash
python -m pclean --preset vlass --selection.vis my.ms --image.imagename out
```

Presets set only the fields relevant to the observing programme;
everything else falls through to the built-in defaults.

## CLI Integration

The `python -m pclean` CLI supports three config-related flags:

| Flag | Purpose |
|------|---------|
| `--config FILE` | Load a YAML config as the base |
| `--preset NAME` | Load a named preset as the base |
| `--dump-config FILE` | Write the resolved config to YAML and exit |

Dot-notation overrides on the command line (e.g.
`--cluster.nworkers 16`) are merged on top.

## CASA Bridge Methods

CASA's C++ synthesis tools (`synthesisimager`, `synthesisdeconvolver`,
`synthesisnormalizer`, `iterbotsink`) expect parameter dicts with
CASA-internal field names and conventions that differ from the
user-facing API.  `PcleanConfig` provides bridge methods that perform
these translations:

| Method | Produces | Notable Translations |
|--------|----------|---------------------|
| `to_casa_selpars()` | `{'ms0': {...}, 'ms1': {...}}` | `timerange` → `timestr`, `uvrange` → `uvdist`, `observation` → `obs`, `intent` → `state` |
| `to_casa_impars()` | `{'0': {...}}` | Ensures `imsize`/`cell` are length-2 lists; injects `deconvolver` and `restart` cross-refs |
| `to_casa_gridpars()` | `{'0': {...}}` | Injects `imagename`, `deconvolver`, `interpolation` cross-refs |
| `to_casa_weightpars()` | `{...}` | `weighting` → `type` + `rmode` (briggs→norm, briggsabs→abs, briggsbwtaper→bwtaper+fracbw); `mosweight` → `multifield`; `perchanweightdensity` → `usecubebriggs` |
| `to_casa_decpars()` | `{'0': {...}}` | Injects `fullsummary` from iteration config |
| `to_casa_normpars()` | `{'0': {...}}` | `nterms` = `dec.nterms` if mtmfs else 1; injects `imagename`, `specmode` |
| `to_casa_iterpars()` | `{...}` | `gain` → `loopgain`; builds `allimages` sub-record with `multiterm` flag |
| `to_casa_miscpars()` | `{...}` | Passes `restart`, `calcres`, `calcpsf` |
| `to_casa_bundle()` | Full dict of all above | Serializable payload for continuum-parallel workers |

Bridge methods live on `PcleanConfig` (not on sub-models) because many
CASA translations cross sub-model boundaries — for example, `imagename`
appears in `impars`, `gridpars`, `normpars`, and `iterpars`.

## Convenience Properties

`PcleanConfig` exposes frequently accessed values as properties to avoid
repetitive sub-model traversal:

| Property | Returns |
|----------|---------|
| `specmode` | `image.specmode` |
| `imagename` | `image.imagename` |
| `parallel` | `cluster.parallel` |
| `niter` | `iteration.niter` |
| `is_cube` | `True` if specmode in `('cube', 'cubedata', 'cubesource')` |
| `is_mfs` | `True` if specmode is `'mfs'` |
| `nfields` | Always `1` (single-field imaging) |
| `nms` | Number of measurement sets in `selection.vis` |

## Data Flow Through Engines

```
pclean(vis=..., **kw)          # user entry point
  │
  ▼
PcleanConfig.from_flat_kwargs()  # build config
  │
  ├─ parallel=False ──► SerialImager(config)
  │                       └── calls to_casa_*() once in __init__
  │                       └── passes dicts to C++ tools
  │
  ├─ parallel=True, cube ──► ParallelCubeImager(config)
  │    │                       └── partition_cube(config) → list[PcleanConfig]
  │    │                       └── submit config.model_dump() to Dask workers
  │    ▼
  │    Workers: PcleanConfig.model_validate(dict) → SerialImager(config)
  │
  └─ parallel=True, mfs ──► ParallelContinuumImager(config)
       │                       └── partition_continuum(config) → list[dict]  (CASA bundles)
       │                       └── submit bundles to Dask actors
       ▼
       Workers: receive CASA bundle dicts, use directly with synthesisimager
       Coordinator: uses config.to_casa_normpars/decpars/iterpars for normalizer/deconvolver/iterbot
```

### Why Two Serialization Strategies?

- **Cube workers** run a full `SerialImager` (imaging + deconvolution),
  so they need the complete hierarchical config to call all
  `to_casa_*()` methods.  The config is serialized via `model_dump()`
  and reconstructed on the worker with `model_validate()`.

- **Continuum workers** only run `synthesisimager` (gridding).  They
  receive pre-translated CASA bundle dicts from
  `PcleanConfig.to_casa_bundle()`.  This avoids a round-trip through
  pydantic on the worker side and naturally handles the fact that
  `synthesisutils.contdatapartition()` returns CASA-native selpars that
  do not cleanly map back to user-facing config fields.

## Partitioning

| Function | Input | Output | Strategy |
|----------|-------|--------|----------|
| `partition_cube(config, nparts)` | `PcleanConfig` | `list[PcleanConfig]` | Uses `config.make_subcube_config()` to create per-subcube configs with adjusted `start`, `nchan`, and `imagename` |
| `partition_continuum(config, nparts)` | `PcleanConfig` | `list[dict]` | Calls `synthesisutils.contdatapartition()`, deep-copies `config.to_casa_bundle()`, overrides selpars and imagename per partition |

## File Inventory

| File | Role |
|------|------|
| `src/pclean/config.py` | Sub-model definitions, `PcleanConfig`, YAML I/O, merge, flat-kwargs bridge, all `to_casa_*()` methods, `make_subcube_config()` |
| `src/pclean/pclean.py` | `pclean()` entry point; builds `PcleanConfig`, dispatches to engines |
| `src/pclean/__main__.py` | CLI; `--config`, `--preset`, `--dump-config`, dot-notation overrides |
| `src/pclean/imaging/serial_imager.py` | Accepts `PcleanConfig`; pre-computes CASA dicts in `__init__` |
| `src/pclean/parallel/worker_tasks.py` | Dask worker functions; cube tasks accept config dicts, continuum tasks accept CASA bundles |
| `src/pclean/parallel/cube_parallel.py` | Cube engine; accepts `PcleanConfig`, serializes subcube configs |
| `src/pclean/parallel/continuum_parallel.py` | Continuum engine; accepts `PcleanConfig`, distributes CASA bundles |
| `src/pclean/utils/partition.py` | `partition_cube()` and `partition_continuum()` |
| `src/pclean/configs/defaults.yaml` | Auto-generated reference YAML with all built-in default values |
| `src/pclean/configs/presets/vlass.yaml` | VLASS continuum imaging preset |
| `src/pclean/params.py` | **Deprecated** legacy `PcleanParams` (retained for backward compat) |

## Defaults Reference

The canonical defaults are the pydantic `Field` defaults on each
sub-model class in `config.py`.  The file
`src/pclean/configs/defaults.yaml` is a machine-generated snapshot and
should be regenerated after changing any default:

```bash
pixi run -e dev gen-defaults
```
