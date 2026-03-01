# Developer Guide

This guide covers how to set up a local development environment, run tests,
lint and format code, and contribute changes back to the project.

## Prerequisites

- [Pixi](https://pixi.sh) (conda-based environment manager)
- Git

## Setting Up the Development Environment

Clone the repository and let Pixi resolve all dependencies (Python, CASA tools,
test and dev tooling):

```bash
git clone https://github.com/r-xue/pclean.git
cd pclean
pixi install -e dev
```

The project defines three Pixi environments:

| Environment | Features            | Purpose |
|-------------|---------------------|---------|
| `default`   | `casa`              | Runtime only -- has `casatools` / `casatasks`. |
| `dev`       | `casa` + `dev`      | Full development -- runtime deps plus pytest, pytest-cov, and ruff. |
| `test`      | `dev`               | CI-only -- dev tooling *without* the conda `casa` packages (casatools is installed via PyPI). |

For day-to-day development, use the **`dev`** environment:

```bash
pixi shell -e dev
```

## Pixi Tasks

All tasks are defined under `[tool.pixi.feature.dev.tasks]` in `pyproject.toml`
and belong to the `dev` (and `test`) environments.

### `test` -- Run the test suite

```bash
pixi run -e dev test
```

Executes `pytest tests/ -v`. Runs the full test suite with verbose output.

### `test-cov` -- Run tests with coverage

```bash
pixi run -e dev test-cov
```

Executes `pytest tests/ -v --cov=pclean --cov-report=term-missing`. Runs the
full suite and prints a line-by-line coverage report to the terminal, showing
which lines are not exercised. The minimum coverage threshold is configured at
60 % in `[tool.coverage.report]`.

### `lint` -- Lint with ruff

```bash
pixi run -e dev lint
```

Executes `ruff check src tests`. Runs ruff's linter over all source and test
files. Violations are printed but **not** auto-fixed -- review each finding
change. The enabled rule sets (configured in `[tool.ruff.lint]`) include:

| Rule set | Description |
|----------|-------------|
| `E4/E7/E9` | pycodestyle errors (imports, statements, runtime) |
| `W`      | pycodestyle warnings (trailing whitespace, etc.) |
| `F`      | pyflakes (unused imports/variables, undefined names) |
| `D`      | pydocstyle (Google convention) |
| `I`      | isort (import ordering) |
| `UP`     | pyupgrade (modern Python 3.10+ syntax) |
| `B`      | flake8-bugbear (common bugs and design issues) |
| `SIM`    | flake8-simplify (simplifiable patterns) |
| `C4`     | flake8-comprehensions (unnecessary wrappers) |
| `LOG`    | flake8-logging (lazy format checks) |
| `RUF`    | ruff-specific rules (unused noqa, mutable defaults, etc.) |

To auto-fix safe violations locally:

```bash
ruff check --fix src tests
```

### `fmt` -- Format code in-place

```bash
pixi run -e dev fmt
```

Executes `ruff format src tests`. Rewrites files to match the project's
formatting rules (single quotes, 120-char line length, Google-style docstrings).
Run this before committing.

### `fmt-check` -- Verify formatting (dry-run)

```bash
pixi run -e dev fmt-check
```

Executes `ruff format src tests --check`. Exits with a non-zero status if any
file would be reformatted, but **does not modify** any files. This is the
variant used in CI to enforce consistent style.

## Typical Development Workflow

```bash
# 1. Create a feature branch
git checkout -b my-feature

# 2. Activate the dev environment
pixi shell -e dev

# 3. Make changes ...

# 4. Format and lint
pixi run fmt
pixi run lint

# 5. Run tests
pixi run test

# 6. Commit
git add -A
git commit -m 'Add my feature'
```

## Continuous Integration

Two GitHub Actions workflows run on every push and pull request:

### Ruff (`.github/workflows/ruff.yml`)

- **Lint step** -- `ruff check src tests` (no `--fix`; the workflow only
  detects violations).
- **Format step** -- `ruff format src tests --check` (verifies formatting
  without modifying files).

Both steps are pinned to ruff **0.11.6**.

### Tests (`.github/workflows/test.yml`)

- Runs on `ubuntu-latest` and `macos-latest`.
- Uses the `test` Pixi environment.
- Fetches CASA runtime data and pipeline test data (cached).
- Runs `pytest` with coverage and uploads results to
  [Codecov](https://codecov.io/gh/r-xue/pclean).

## Code Style

The project follows the conventions documented in
[`.github/copilot-instructions.md`](../.github/copilot-instructions.md).
Key points:

- **Python >= 3.10** -- use `X | Y` unions, built-in generics, etc.
- **Line length** -- 120 characters.
- **Quotes** -- single quotes for string literals.
- **Docstrings** -- Google style (PEP-257). No types in `Args`/`Returns`
  descriptions; rely on signature annotations.
- **Logging** -- lazy formatting (`logger.info('Msg: %s', var)`), never
  f-strings.

## Project Layout

```
src/pclean/
├── __init__.py              # Package init, exposes pclean()
├── __main__.py              # CLI entry point (python -m pclean)
├── pclean.py                # Top-level tclean-like interface
├── params.py                # Parameter container & validation
├── imaging/
│   ├── serial_imager.py     # Single-process imager (base engine)
│   ├── deconvolver.py       # Deconvolution wrapper
│   └── normalizer.py        # Image normalization (gather/scatter)
├── parallel/
│   ├── cluster.py           # Dask cluster lifecycle management
│   ├── cube_parallel.py     # Channel-parallel cube imaging
│   ├── continuum_parallel.py# Row-parallel continuum imaging
│   └── worker_tasks.py      # Serialisable functions for workers
└── utils/
    ├── partition.py         # Data/image partitioning helpers
    ├── image_concat.py      # Sub-cube image concatenation
    └── memory_estimate.py   # Worker RAM estimation heuristics
```

## Further Reading

| Document | Description |
|----------|-------------|
| [parallelization.md](parallelization.md) | Cube vs. continuum parallelization architecture |
| [memory_estimation.md](memory_estimation.md) | RAM estimation heuristics for worker sizing |
| [memory_management.md](memory_management.md) | Why Dask memory management is disabled |
| [code_structure.md](code_structure.md) | Module-level code overview |
| [briggsbwtaper.md](briggsbwtaper.md) | `briggsbwtaper` weighting analysis |
| [per_channel_convergence.md](per_channel_convergence.md) | Per-channel convergence strategy |
| [image_concatenation.md](image_concatenation.md) | Sub-cube concatenation details |
