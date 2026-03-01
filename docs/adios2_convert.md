# Converting a MeasurementSet to ADIOS2

`convert_ms_to_adios2` rewrites the heavy data columns of a
MeasurementSet (MS) so they are stored through the `Adios2StMan` storage
manager instead of the default `StandardStMan` / `TiledShapeStMan`.

## Why a conversion step is needed

Even when `Adios2StMan` is compiled into the `casatools` build (see
[check_adios2.md](check_adios2.md)), CASA will not use it automatically.
The storage manager for each column is determined at table-creation time
and recorded in the table's `dminfo`.  To benchmark the ADIOS2 I/O path
one must explicitly rewrite the MS with the new manager **before**
running `pclean`.

## How it works

1. The source MS is opened and its `dminfo` dictionary is read.
2. For every data manager that handles one of the target columns, the
   column is moved out of that manager and consolidated into a single
   new `Adios2StMan` entry.
3. A deep copy with `valuecopy=True` is performed.  This forces the
   casacore Table Data System to physically read every cell through the
   old manager and rewrite it through the ADIOS2 C++ backend, rather
   than simply copying the underlying files.  The C++ `deepCopy`
   streams data row-by-row internally, so it does not load the full
   table into Python memory.
4. Sub-tables (`ANTENNA`, `FIELD`, `SPECTRAL_WINDOW`, etc.) are left
   untouched — their I/O footprint is negligible.

## Command-line usage

```bash
python -m pclean.utils.convert_adios2 input.ms output_adios2.ms
```

### Options

| Flag            | Description                                          |
|-----------------|------------------------------------------------------|
| `--overwrite`   | Remove `output_ms` if it already exists.             |
| `--columns COL [COL ...]` | Columns to rebind (default: see below).  |
| `--engine-type TYPE` | ADIOS2 engine type (default: `BP4`).            |
| `--max-buffer-size SIZE` | ADIOS2 write-buffer cap (BP4 only, e.g. `2Gb`). |
| `--adios2-xml PATH` | User-supplied ADIOS2 XML config (overrides `--engine-type` and `--max-buffer-size`; see [caveat](#xml-caveat)). |

Default target columns:

    DATA  CORRECTED_DATA  MODEL_DATA  FLAG  WEIGHT  SIGMA

### Examples

Rewrite all default columns:

```bash
python -m pclean.utils.convert_adios2 uid___A002.ms uid___A002_adios2.ms
```

Overwrite an existing output and rebind only `DATA` and `FLAG`:

```bash
python -m pclean.utils.convert_adios2 uid___A002.ms uid___A002_adios2.ms \
    --overwrite --columns DATA FLAG
```

## Python API

```python
from pclean.utils.convert_adios2 import convert_ms_to_adios2

convert_ms_to_adios2('input.ms', 'input_adios2.ms')
```

### Parameters

| Name             | Type                          | Default                        | Description                                      |
|------------------|-------------------------------|--------------------------------|--------------------------------------------------|
| `input_ms`       | `str`                         | *(required)*                   | Path to the source MeasurementSet.               |
| `output_ms`      | `str`                         | *(required)*                   | Destination path for the ADIOS2-backed copy.     |
| `target_columns` | `tuple[str, ...] \| list[str]` | See default list above        | Columns to rebind to Adios2StMan.                |
| `overwrite`      | `bool`                        | `False`                        | Remove `output_ms` if it already exists.         |
| `engine_type`    | `str`                         | `'BP4'`                        | ADIOS2 engine type (see buffer notes below).     |
| `engine_params`  | `dict[str, str] \| None`      | `None`                         | ADIOS2 engine parameters (see below).            |
| `adios2_xml`     | `str \| None`                  | `None`                         | Path to user-supplied ADIOS2 XML config file.    |

### Return value

The `output_ms` path on success.

### Exceptions

| Exception           | When                                                |
|---------------------|-----------------------------------------------------|
| `FileNotFoundError` | `input_ms` does not exist.                          |
| `FileExistsError`   | `output_ms` exists and `overwrite` is `False`.      |
| `RuntimeError`      | None of the `target_columns` were found in `input_ms`. |

## Prerequisites

The `openmpi` variant of `casatools` must be installed for `Adios2StMan`
to be available.  Verify with:

```bash
python -m pclean.utils.check_adios2
```

See [check_adios2.md](check_adios2.md) for details.

## Appendix: Adios2StMan copy constraints and memory

### Problem

For large MeasurementSets (multi-GB visibility columns), the conversion
can consume significant memory.  Reducing peak memory through row-level
chunking was investigated but is **not feasible** with Adios2StMan.

### Approaches tried and why they fail

| Approach | Result |
|---|---|
| `addrows(nrow)` + `putcol` per chunk | **SIGABRT** — `addrows` leaves variable-shape cells with null shape metadata; ADIOS2 aborts on `Variable<T>::SetShape` with a null pointer. |
| Incremental `addrows(chunk)` + `putcol` | Same `SetShape` null-pointer abort. |
| `selectrows` + `copy` for first chunk, then `copyrows` for remaining | First chunk succeeds; subsequent `copyrows` **SIGABRT** — `copyrows` reopens the table internally and ADIOS2 rejects the open mode for append (`Engine open mode not valid`). |

### Why `tb.copy()` is the only working path

Adios2StMan requires cell shapes to be established through casacore's
internal `Table::deepCopy` code path.  Manual row-level writes bypass
this path, and the ADIOS2 engine does not support reopening a table for
append after the initial write session is closed.

Casacore's C++ `deepCopy` with `valuecopy=True` already streams data
row-by-row internally — it does **not** load the entire table into
Python memory.  The peak memory footprint during conversion is dominated
by ADIOS2's internal write buffers rather than Python-side data.

### Controlling ADIOS2 write-buffer memory

ADIOS2 does **not** expose an environment variable for buffer control.

The ADIOS2 BP engine accumulates all `Put()` data within a single step
— `EndStep()` / `Close()` only run in the Adios2StMan destructor, after
the entire `deepCopy` completes.  Without explicit configuration the
write buffers grow proportionally to the table size.

**Critically, the default engine matters:**

| Engine | `MaxBufferSize` | `BufferChunkSize` | Notes |
|--------|-----------------|-------------------|-------|
| BP4    | **respected** — triggers intermediate flush to disk | N/A | Recommended for memory control. |
| BP5    | **ignored** | **respected** | Default in recent ADIOS2 builds. |

Because Adios2StMan's C++ constructor picks the ADIOS2 default engine
(usually BP5) when no engine type is specified, passing
`MaxBufferSize` via `ENGINEPARAMS` in the dminfo SPEC alone was
ineffective — BP5 ignored it entirely.

An XML config file approach was also attempted (writing a temporary
file and passing its path via the `XMLFILE` dminfo SPEC field), but
ADIOS2's XML parser crashed with `std::invalid_argument: stoul` in
some builds — so that path was abandoned.

`convert_ms_to_adios2` now:

1. **Defaults to `BP4`** (which respects `MaxBufferSize`).
2. **Sets `ENGINETYPE` and `ENGINEPARAMS`** directly in the dminfo
   `SPEC` record.  Casacore's `Adios2StMan::makeObject` reads these
   fields and calls `IO::SetEngine()` / `IO::SetParameters()` via the
   C++ API — bypassing the XML parser entirely.

```bash
# Cap write buffers at 2 GB with explicit BP4 engine
python -m pclean.utils.convert_adios2 input.ms output.ms \
    --max-buffer-size 2Gb
```

```python
# Python API
convert_ms_to_adios2(
    'input.ms', 'output.ms',
    engine_params={'MaxBufferSize': '2Gb'},
)
```

For full control, supply a custom XML:

<a id="xml-caveat"></a>

> **Caveat:** ADIOS2's XML parser crashed with `std::invalid_argument:
> stoul` in some builds.  Test the XML file on a small MS before
> running a large conversion.  The `ENGINETYPE` / `ENGINEPARAMS`
> approach (default) avoids the XML parser entirely and is preferred.

```xml
<?xml version="1.0"?>
<adios-config>
    <io name="Adios2StMan">
        <engine type="BP4">
            <parameter key="MaxBufferSize">2Gb</parameter>
            <parameter key="InitialBufferSize">256Mb</parameter>
        </engine>
    </io>
</adios-config>
```

```bash
python -m pclean.utils.convert_adios2 input.ms output.ms \
    --adios2-xml my_adios2.xml
```

Useful BP engine parameters for memory control:

| Parameter | Default (BP4 / BP5) | Description |
|---|---|---|
| `MaxBufferSize` | unlimited | Flush to disk when exceeded (BP4 only). |
| `InitialBufferSize` | 16 KB / 128 MB | Starting allocation size. |
| `BufferGrowthFactor` | 1.05 / — | Growth multiplier (BP4). |
| `BufferChunkSize` | — / 128 MB | Per-chunk allocation (BP5). |

### Workaround for very large datasets

If the ADIOS2 buffer memory is a concern for extremely large datasets,
split the MS into smaller partitions first (e.g. with
`casatasks.mstransform` or `casatasks.partition`), convert each
partition individually, then concatenate the results.
