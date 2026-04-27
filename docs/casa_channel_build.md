# Personal CASA Channel Build (`rxue`)

`pclean` depends on a personal conda channel (`rxue`) that ships a patched
CASA build based on the upstream modular release.  This channel provides
Python 3.12 support and several improvements that are not yet merged into
official NRAO releases.

## Channel and version

| Item | Value |
|------|-------|
| Conda channel | `rxue` (`https://conda.anaconda.org/rxue`) |
| CASA version | Modular **6.7.3.21** |
| Python | **3.12** |

## Applied patches

The build is assembled from four personal-fork feedstocks hosted under
[github.com/r-xue](https://github.com/r-xue).  Each repo is a fork of the
corresponding conda-forge feedstock with additional patches on a personal
branch.  The **Origin** column in each table indicates whether a patch
originated in the official [conda-forge](https://github.com/conda-forge)
feedstock or was added by r-xue.  Patches that apply only on macOS are
marked **[osx]**.

### casatools (v6.7.3.21)

Source: [r-xue/casatools-feedstock](https://github.com/r-xue/casatools-feedstock) — `recipe/`

| Patch file | Origin | Description |
|------------|--------|-------------|
| `macos-not-gcc.patch` **[osx]** | conda-forge | Disable the GCC library copy step in `setup.py` on macOS — handled by conda-forge instead. |
| `expose-par-cubegridding.patch` | **r-xue** | Add `setcubegridding` method to `SynthesisImager` and extend its XML documentation, enabling per-channel cube-gridding control from Python. |
| `expose-par-fracbw-cas-14520.patch` | **r-xue** | CAS-14520 — expose the `fracbw` (fractional-bandwidth) parameter in the `setweighting` Python binding of `SynthesisImager`. |
| `cas-14761.patch` | **r-xue** | CAS-14761 — three-part fix targeting cube+standard parallel imaging performance, bundling three upstream tickets: **(1) CAS-14759** *([@r-xue](https://github.com/r-xue))* — skip the redundant `normalizeprimarybeam()` call in `makePB` on the master process (`imager_base.py`); PB normalisation is already applied per sub-cube in `CubeMajorCycleAlgorithm::task()` in C++. **(2) CAS-14758** *([@r-xue](https://github.com/r-xue))* — cache the residual, mask and model `image()` tools across the inner `(chan × stokes)` loop in `fill_summary_minor` (`imager_return_dict.py`), removing O(nchan × nstokes) redundant open/close calls per minor-cycle summary. **(3) CAS-13898** *([@Kitchi](https://github.com/Kitchi))* — compute a per-image memory budget in `SDMaskHandler::autoMaskByMultiThreshold` that accounts for gridding overhead, so `TempImage` allocations stay in RAM and do not spill to disk during parallel cube cleaning. |

### casacpp (v6.7.3.21)

Source: [r-xue/casacpp-feedstock](https://github.com/r-xue/casacpp-feedstock) — `recipe/`

| Patch file | Origin | Description |
|------------|--------|-------------|
| `versioning.patch` | conda-forge | Remove the `scripts/version` CMake invocation that is not available during conda builds, using the version supplied by the recipe instead. |
| `protobuf-detection.patch` | conda-forge | Improve gRPC/Protobuf CMake detection so that both distro-packaged and source-compiled installations are found reliably. |
| `no-copy-unique_ptr.patch` **[osx]** | conda-forge | Fix the move constructor of `Vi2DataProvider` to use `std::move` on the `unique_ptr` member, silencing a clang error on macOS. |

### casacore (v3.8.0)

Source: [r-xue/casacore-feedstock](https://github.com/r-xue/casacore-feedstock) — `recipe/`

| Patch file | Origin | Description |
|------------|--------|-------------|
| `ncursesw.patch` | conda-forge | Link readline against `ncursesw` instead of `ncurses` to match the wide-character ncurses library name used in conda-forge. |
| `default-root.patch` | conda-forge | Inject `CONDA_CASA_ROOT` into the CMake definitions so casacore can locate the correct data directory inside the conda prefix. |
| `ignore-build-env-prefix.patch` | conda-forge | Filter the conda `build_env` path out of `CMAKE_SYSTEM_PREFIX_PATH`, preventing the build environment from polluting host-library discovery. |
| `boost-python-cmake.patch` | conda-forge | Fix Boost-Python CMake target detection for Boost ≥ 1.67, which changed the component naming scheme. |
| `clang-link.patch` **[osx]** | conda-forge | Remove the explicit `PYTHON2_LIBRARIES` entry from the `target_link_libraries` call for `casa_python` to avoid duplicate-symbol errors with clang. |
| `darwin-gettimeofday.patch` **[osx]** | conda-forge | Include `<sys/time.h>` on Darwin to resolve the `gettimeofday` declaration when the macOS SDK omits it from `<time.h>`. |
| `libadios2-macos.patch` **[osx]** | conda-forge (modified) | Require ADIOS2 ≥ 2.8.0 and request the `C` component on macOS, matching the version available in conda-forge; updated by r-xue for ADIOS2 2.8.0 compatibility. |
| `adios2stman_slicer_selection.patch` | **r-xue** | Fix a dimensionality mismatch in `Adios2StManColumn::columnSliceCellsVToSelection` for variable-shape (indirect) columns: resize `itsAdiosStart/Count` to `ndim(Slicer)+1` before calling `ADIOS2 SetSelection()`. |
| `adios2stman_readrandomaccess.patch` | **r-xue** | Open Adios2StMan tables with `ReadRandomAccess` mode; track a write-mode flag so `EndStep()` is only called when the engine was opened for writing, eliminating spurious write-lock acquisition. |

### casatasks (v6.7.3.21)

Source: [r-xue/casatasks-feedstock](https://github.com/r-xue/casatasks-feedstock) — `recipe/`

| Patch file | Origin | Description |
|------------|--------|-------------|
| `versioning.patch` | conda-forge | Same as in `casacpp` — remove the version-script CMake call that is absent during conda builds. |
| `cas-14761.patch` | **r-xue** | CAS-14761 — mirror of the `casatools` patch: apply the same three sub-ticket fixes — **CAS-14759** *([@r-xue](https://github.com/r-xue))*, **CAS-14758** *([@r-xue](https://github.com/r-xue))*, and **CAS-13898** *([@Kitchi](https://github.com/Kitchi))* — to the `casatasks` Python and C++ sources (see `casatools` entry above for full descriptions). |

## Environment setup

### Option 1: pixi (recommended)

**In `pyproject.toml`** (used by this project):

```toml
[tool.pixi.workspace]
channels = ["https://conda.anaconda.org/rxue", "conda-forge"]
```

**In a standalone `pixi.toml`** (for lightweight environments outside this project):

```toml
[workspace]
name = "micasa"
channels = ["rxue", "conda-forge"]
platforms = ["linux-64", "linux-aarch64", "osx-arm64"]

[dependencies]
conda-ecosystem-user-package-isolation = "*"
python = "3.12.*"
ipython = "*"
pip = ">=23.0"
casatasks = ">=6.7.3"
```

Install and activate the runtime environment:

```bash
pixi install
pixi shell
```

For development (includes pytest, ruff, etc.):

```bash
pixi install -e dev
pixi shell -e dev
```

### Option 2: conda / mamba

A standalone `environment.yml` is provided in the feedstock notes for
environments that do not use pixi:

```yaml
name: micasa
channels:
  - rxue
  - conda-forge
  - nodefaults
dependencies:
  - conda-ecosystem-user-package-isolation
  - python=3.12
  - ipython
  - pip>=23.0
  - casatasks>=6.7.3
```

Create and activate:

```bash
conda env create -f environment.yml
conda activate micasa
```

Update after changes:

```bash
conda env update -f environment.yml --prune
```

## Related resources

- Personal-fork feedstocks (all under [github.com/r-xue](https://github.com/r-xue)):
  - [r-xue/casatools-feedstock](https://github.com/r-xue/casatools-feedstock) — casatools + casacpp Python layer patches
  - [r-xue/casacpp-feedstock](https://github.com/r-xue/casacpp-feedstock) — casacpp C++ build patches
  - [r-xue/casacore-feedstock](https://github.com/r-xue/casacore-feedstock) — casacore library patches
  - [r-xue/casatasks-feedstock](https://github.com/r-xue/casatasks-feedstock) — casatasks Python layer patches
- Additional patches applied on top of the feedstocks: `casa6/` directory in the repository root
- Build configuration: `casa6/build.conf` and `casa6/Makefile`
- Channel packages: <https://anaconda.org/rxue>
