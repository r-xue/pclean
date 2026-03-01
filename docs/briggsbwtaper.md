# `briggsbwtaper` Weighting and Parallel Cube Imaging

## Background

The `briggsbwtaper` weighting scheme (CAS-13021) applies a UV-distance-dependent
taper whose strength is controlled by the **fractional bandwidth** of the full
cube:

```
fracBW = 2 * (maxFreq - minFreq) / (maxFreq + minFreq)
```

The taper factor applied to each visibility in `BriggsCubeWeightor` is:

```
nCellsBW  = fracBW * sqrt(uscale*u² + vscale*v²)
uvDistFactor = nCellsBW + 0.5   (clamped for short baselines)
imweight /= gwt * f2 / uvDistFactor + d2
```

## Why It Fails in Parallel Cube Mode

In pclean's parallel cube mode, each Dask worker images an independent sub-cube
of 1–N channels (`cube_chunksize`). This breaks `briggsbwtaper` because:

1. **`fracBW` collapses to zero.** With `cube_chunksize=1`, each worker's
   sub-cube has a single channel, so `maxFreq == minFreq` → `fracBW = 0.0`.
   The C++ code in `BriggsCubeWeightor` throws:
   ```
   AipsError: BriggsCubeWeightor fractional bandwith is not a valid value, 0.0.
   ```

2. **Even with multi-channel chunks**, each worker computes `fracBW` from its
   own sub-cube, not the full bandwidth. The resulting taper would be physically
   incorrect.

## What `briggsbwtaper` Actually Needs

The algorithm has two independent components:

| Component | Scope | Parallelizable? |
|---|---|---|
| Per-channel Briggs weight density grid (`gwt`, `f2`, `d2`) | Single channel | Yes — `perchanweightdensity=True` makes each channel independent |
| `fracBW` scalar | Full cube bandwidth | No — requires knowledge of all channels |

Since per-channel density grids are already independent, the **only** cross-channel
quantity is the `fracBW` scalar. If this value were pre-computed from the full
cube parameters and passed to each worker, the results would be identical to
serial tclean.

## C++ Code Path

`SynthesisImagerVi2::weight()` already accepts `fracBW` as a parameter
(default `0.0`). When `fracBW != 0.0`, it skips auto-computation and uses the
passed value directly:

```cpp
// SynthesisImagerVi2.cc line 879
if (rmode == "bwtaper") {
    if (fracBW == 0.0) {
        // auto-compute from sub-cube spectral axis — FAILS for 1-chan
        minFreq = SpectralImageUtil::worldFreq(itsMaxCoordSys, 0.0);
        maxFreq = SpectralImageUtil::worldFreq(itsMaxCoordSys, itsMaxShape(3)-1);
        fracBW = 2 * (maxFreq - minFreq) / (maxFreq + minFreq);
    }
    // if fracBW was already nonzero, used as-is ✓
}
```

The `fracBW` value propagates through:

```
SynthesisImagerVi2::weight()
  → fillWeightRecord() stores fracBW in Record
    → BriggsCubeWeightor reads fracBW_p
      → used in weightUniform() / getWeightUniform() taper formula
```

## Fix: Exposing `fracBW` Through the Python Binding (Option A — Implemented)

The Python `setweighting` binding originally did not expose the `fracBW`
parameter. This has been fixed with three coordinated changes:

### 1. casatools XML binding (`synthesisimager.xml`)

Added `<param name="fracbw" type="double">` to the `setweighting` method
definition, with a default of `0.0` (preserving backward compatibility — existing
callers that omit it get the auto-compute behavior).

### 2. casatools C++ wrapper (`synthesisimager_cmpt.cc`)

Added `const double fracbw` to the `setweighting()` function signature and
passed it as the 13th argument to `itsImager->weight()`:

```cpp
itsImager->weight(type, rmode, cnoise, robust, cfov, npixels,
                  multifield, usecubebriggs, filtertype, bmaj, bmin, bpa, fracbw);
```

### 3. pclean `params.py`

- Added `fracbw=0.0` to `_DEFAULT_WEIGHT`.
- When `weighting="briggsbwtaper"`, pre-computes `fracBW` from the full cube's
  `start`, `width`, and `nchan` parameters:

```python
fracBW = 2.0 * (maxFreq - minFreq) / (maxFreq + minFreq)
```

This value is stored in `weightpars["fracbw"]` and passed through to each Dask
worker via `si.setweighting(**params.weightpars)`. Since the binding now accepts
`fracbw`, the C++ layer receives the correct full-cube fractional bandwidth
and skips the broken auto-computation.

**Note:** Requires rebuilding casatools with the modified XML/C++ files.

## Fallback Workaround

Use `weighting='briggs'` with `perchanweightdensity=True` (the default):

```python
pclean(
    ...,
    weighting="briggs",      # not "briggsbwtaper"
    robust=0.5,
    perchanweightdensity=True,
    parallel=True,
    cube_chunksize=1,
)
```

This computes per-channel Briggs weights independently and is compatible with
per-channel parallelization.

## tclean Reference Constraints

tclean itself also restricts `briggsbwtaper`:

- Requires `perchanweightdensity=True`
- Requires `specmode='cube'` (not `'mfs'` or `'cont'`)
- Requires `npixels=0`

See `task_tclean.py` lines 218–236 in casa6.
