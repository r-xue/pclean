# Image Concatenation: Sequential Step in the Parallel Pipeline (as of v0.1)

## Where It Happens

| Step | Description | Execution |
|------|-------------|-----------|
| 1 | Partition channels into subcubes | sequential (fast) |
| 2 | Submit subcube tasks to Dask workers | parallel |
| 3 | Wait for all subcubes to complete | as_completed |
| 4 | Concatenate subcube images into final cube | sequential (*) |

Image concatenation is sequential and runs on the main process after all parallel subcube tasks complete.

## Is It a Bottleneck?

**Usually no.** Imaging dominates runtime by 10–100×.

Step | Typical Time | Bound By |
------|-------------|----------|
| Subcube imaging (gridding + FFT + deconvolution) | Minutes to hours | CPU + I/O |
| Image concatenation (`ia.imageconcat`) | Seconds to ~1 min | Disk I/O only |

For a test case with 117 channels at 90&times;90 pixels, concatenation is trivial. Even for large cubes (e.g., 4096&times;4096 &times; 1000 channels), concatenation is mostly sequential disk I/O that takes far less time than imaging.

## When It Could Be a Bottleneck

- **Very large cubes** on slow storage (spinning disks, NFS)
- **Virtual vs. physical concatenation**: CASA `imageconcat` supports virtual concatenation (nearly instant) vs. physical copy (slower)
- **Multiple image products**: residual, model, psf, pb — each needs concatenation

## Potential Optimizations

### 1. Use Virtual Concatenation

```python
ia.imageconcat(outfile=outfile, infiles=infiles, relax=True,
               tempclose=True, overwrite=True)  # virtual by default
```

### 2. Parallelize Across Image Products

Concatenate residual, model, psf, pb simultaneously instead of sequentially:

```python
# Instead of sequential:
for product in ['residual', 'model', 'psf', 'pb']:
    concatenate(product)

# Could do:
futures = [client.submit(concatenate, p) for p in ['residual', 'model', 'psf', 'pb']]
client.gather(futures)
```

### 3. Stream Concatenation

Start concatenating the first subcube as soon as it finishes, rather than waiting for all to complete. The `as_completed` pattern in `cube_parallel.py` already provides ordering — it could be extended:

```python
# Pseudocode: incremental concatenation
for future in as_completed(futures):
    idx, result = future.result()
    append_to_output_cube(idx, result)  # concat as they arrive
```

## Verdict

Concatenation is sequential but is **not a meaningful bottleneck** for typical use cases. The imaging itself (step 2) dominates runtime. Optimizing concatenation would be premature unless profiling shows otherwise for a specific workload.
