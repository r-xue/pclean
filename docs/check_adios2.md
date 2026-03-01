# Checking ADIOS2 Support

The `check_adios2_support` helper verifies whether the current `casatools`
build includes `Adios2StMan`.  It works by creating a temporary CASA table
with a column explicitly bound to `Adios2StMan`.  If the underlying
`casacore` was compiled without ADIOS2 (the `nompi` conda-forge variant),
the call fails with a `RuntimeError` about an unknown storage manager.

## Quick check from the command line

```bash
python -m pclean.utils.check_adios2
```

Example output when ADIOS2 **is** available (conda openmpi variant):

```
casatools version : 6.7.2.42
install origin    : conda
conda build string: mpi_openmpi_py312h...
  channel       : conda-forge
  subdir        : linux-64
  build_string  : mpi_openmpi_py312h...
Adios2StMan       : SUPPORTED
```

When it is **not** available (e.g. pip install or conda nompi variant):

```
casatools version : 6.7.2.42
install origin    : pip
Adios2StMan       : NOT SUPPORTED
```

## Using the helpers in Python

### Check ADIOS2 support

```python
from pclean.utils.check_adios2 import check_adios2_support

if check_adios2_support():
    print('ADIOS2 I/O path is ready')
else:
    print('Falling back to default storage managers')
```

### Inspect the casatools installation

```python
from pclean.utils.check_adios2 import get_casatools_info

info = get_casatools_info()
print(info.version)           # e.g. '6.7.2.42'
print(info.origin)            # 'conda', 'pip', or 'unknown'
print(info.conda_build_string)  # e.g. 'mpi_openmpi_py312h...' (empty for pip)
print(info.details)           # dict with channel, subdir, build_string (conda only)
```

### `CasatoolsInfo` fields

| Field                | Type            | Description                                         |
|----------------------|-----------------|-----------------------------------------------------|
| `version`            | `str`           | casatools version string.                           |
| `origin`             | `str`           | `'conda'`, `'pip'`, or `'unknown'`.                 |
| `conda_build_string` | `str`           | Conda build string (e.g. `mpi_openmpi_py312h...`). |
| `adios2_supported`   | `bool`          | Whether Adios2StMan is available.                   |
| `details`            | `dict[str,str]` | Extra metadata (channel, subdir) for conda installs.|

### `check_adios2_support` parameters

| Name      | Type   | Default | Description                                       |
|-----------|--------|---------|---------------------------------------------------|
| `cleanup` | `bool` | `True`  | Remove the temporary probe table after the check. |

Set `cleanup=False` to inspect the probe table (`_pclean_adios2_probe.tab`)
for debugging:

```python
check_adios2_support(cleanup=False)
```

## How it works

1. A minimal table descriptor with a single `float` column (`DATA`) is
   defined.
2. A `dminfo` dictionary requests `Adios2StMan` as the storage manager for
   that column.
3. `casatools.table().create()` is called.  If `casacore` recognises
   `Adios2StMan`, the table is created successfully; otherwise a
   `RuntimeError` is raised.
4. On success, the bound data-manager info is read back to confirm the
   column is indeed managed by `Adios2StMan`.

## Background

On conda-forge, ADIOS2 support is available only in the **`openmpi`**
variant of `casacore`
([PR #68](https://github.com/conda-forge/casacore-feedstock/pull/68),
merged August 2022).  The `casatools` package ships both `nompi` and
`openmpi` variants; only the latter links against the ADIOS2-enabled
`casacore`.

To install the `openmpi` variant explicitly:

```bash
conda install casatools 'casacore=*=*openmpi*' openmpi
```

> **Note:** Even with `Adios2StMan` compiled in, CASA will not use it
> automatically.  The input Measurement Set must be explicitly written
> with `Adios2StMan` (e.g. via `mstransform` or a table copy with the
> appropriate `dminfo`) before `pclean` can exercise the ADIOS2 I/O path.
