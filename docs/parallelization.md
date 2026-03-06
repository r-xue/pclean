# Parallelization Modes

## Cube Mode (`specmode='cube'`)

```mermaid
flowchart LR
    subgraph Coord["Coordinator"]
        direction TB
        A[pclean] --> B[Partition<br/>channels]
        B --> C[Submit]
        G[Gather] --> H[Concat<br/>subcubes]
        H --> I[Final cube]
    end

    subgraph Workers["Dask Workers (embarrassingly parallel)"]
        direction TB

        subgraph W0["Worker 0 · ch 0-23"]
            direction LR
            W0a[setup] --> W0b[PSF] --> W0c[PB] --> W0d[Major 1] --> W0e[Converge?] --> W0f[Mask] --> W0g[Minor] --> W0h[Major 2] --> W0i[Done]
        end

        subgraph W1["Worker 1 · ch 24-47"]
            direction LR
            W1a[setup] --> W1b[PSF → PB → Major → Minor → Done]
        end

        subgraph W2["Worker 2 · ch 48-70"]
            direction LR
            W2a[setup] --> W2b[PSF → PB → Major → Minor → Done]
        end

        subgraph W3["Worker 3 · ch 71-93"]
            direction LR
            W3a[setup] --> W3b[PSF → PB → Major → Minor → Done]
        end

        subgraph W4["Worker 4 · ch 94-116"]
            direction LR
            W4a[setup] --> W4b[PSF → PB → Major → Minor → Done]
        end
    end

    C --> W0a
    C --> W1a
    C --> W2a
    C --> W3a
    C --> W4a
    W0i --> G
    W1b --> G
    W2b --> G
    W3b --> G
    W4b --> G

    style Coord fill:#e1f5fe
    style Workers fill:#c8e6c9
    style W0 fill:#a5d6a7
    style W1 fill:#a5d6a7
    style W2 fill:#a5d6a7
    style W3 fill:#a5d6a7
    style W4 fill:#a5d6a7
```

## Continuum Mode (`specmode='mfs'`)

```mermaid
flowchart LR
    subgraph Init["Setup"]
        direction TB
        A[pclean] --> B[Partition<br/>rows] --> C[Create<br/>actors]
    end

    subgraph PSF["PSF (parallel)"]
        direction TB
        P0[Worker 0] ~~~ P1[Worker 1] ~~~ P2[Worker N]
    end

    subgraph PB["PB (parallel)"]
        direction TB
        PB0[Worker 0] ~~~ PB1[Worker 1] ~~~ PB2[Worker N]
    end

    subgraph Maj1["Major Cycle (parallel)"]
        direction TB
        M0[Worker 0<br/>grid] ~~~ M1[Worker 1<br/>grid] ~~~ M2[Worker N<br/>grid]
    end

    subgraph Loop["Iteration Loop (coordinator)"]
        direction TB
        POST[Gather +<br/>Normalize] --> MASK[setupMask]
        MASK --> CONV{Converged?}
        CONV -->|No| MINOR[Minor cycle<br/>serial deconv]
        MINOR --> PRE[Scatter<br/>model]
        PRE --> MAJ2
        CONV -->|Yes| RESTORE[Restore +<br/>PBcor]
    end

    subgraph MAJ2["Next Major (parallel)"]
        direction TB
        M20[Worker 0] ~~~ M21[Worker 1] ~~~ M22[Worker N]
    end

    C --> PSF
    PSF --> NORM1[Normalize<br/>PSF]
    NORM1 --> PB
    PB --> NORM2[Normalize<br/>PB]
    NORM2 --> Maj1
    Maj1 --> POST
    MAJ2 --> POST

    style Init fill:#e1f5fe
    style PSF fill:#c8e6c9
    style PB fill:#c8e6c9
    style Maj1 fill:#c8e6c9
    style MAJ2 fill:#c8e6c9
    style Loop fill:#fff9c4
    style MINOR fill:#ffecb3
```

## Key Differences

| Aspect | Cube | Continuum (MFS) |
|--------|------|-----------------|
| **What's parallel** | Entire pipeline per subcube | Only gridding/degridding (major cycle) |
| **Minor cycle** | Parallel (per subcube) | Serial on coordinator |
| **Communication** | None (embarrassingly parallel) | Gather/scatter each major cycle |
| **Partition axis** | Frequency channels | Visibility rows |
| **Final assembly** | `imageconcat` of subcubes | Normalizer gathers partial images |

## Known Limitations

### `weighting='briggsbwtaper'` in Parallel Cube Mode

The `briggsbwtaper` weighting scheme (CAS-13021) requires the **fractional
bandwidth** of the full cube:

```
fracBW = 2 * (maxFreq - minFreq) / (maxFreq + minFreq)
```

In parallel cube mode each Dask worker images an independent sub-cube (often a
single channel), so the C++ auto-computation of `fracBW` from the sub-cube's
spectral axis would produce `0.0` and fail.

#### Fix

The `fracBW` parameter needs to be exposed through the casatools Python binding
(`synthesisimager.setweighting(fracbw=...)`), then pclean can pre-computes it from
the full cube `start`/`width`/`nchan` before dispatching to workers. Each
worker receives the correct full-bandwidth `fracBW` scalar alongside its
independent per-channel Briggs density grid.

**Requirements:**
- casatools must be rebuilt from the patched XML and C++ sources
- `start` and `width` must be specified as frequency quantities (e.g. `"100GHz"`)
  so the pre-computation can resolve them. If they are not parseable, `fracBW`
  falls back to `0.0` (auto-compute), which will still fail for single-channel
  sub-cubes.

#### Fallback workaround

Use `weighting='briggs'` (with `perchanweightdensity=True`, the default),
which computes per-channel Briggs weights independently — this is compatible
with per-channel parallelization but does not offer the improved imaging fidelity of
off-axis sources for wide-bandwidth cubes.

```python
pclean(
    ...
    weighting="briggs",   # not "briggsbwtaper"
    robust=0.5,
    perchanweightdensity=True,
    parallel=True,
    cube_chunksize=1,
)
```

#### CASA `tclean` reference

`tclean` itself also restricts `briggsbwtaper`:
- Requires `perchanweightdensity=True`
- Requires `specmode='cube'` (not `'mfs'` or `'cont'`)
- Requires `npixels=0`

See `task_tclean.py` lines 218–236 in casa6.
