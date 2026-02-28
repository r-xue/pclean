"""Convert a MeasurementSet to use the Adios2StMan storage manager."""

import logging
import os

logger = logging.getLogger(__name__)

# Heavy visibility columns whose I/O dominates runtime.
_DEFAULT_TARGET_COLUMNS = ('DATA', 'CORRECTED_DATA', 'MODEL_DATA', 'FLAG', 'WEIGHT', 'SIGMA')


def convert_ms_to_adios2(
    input_ms: str,
    output_ms: str,
    *,
    target_columns: tuple[str, ...] | list[str] = _DEFAULT_TARGET_COLUMNS,
    overwrite: bool = False,
) -> str:
    """Copy a MeasurementSet, rebinding heavy columns to Adios2StMan.

    The function reads the existing ``dminfo`` from *input_ms*, replaces
    the storage-manager type for every manager that handles one of the
    *target_columns*, and performs a deep ``valuecopy`` so that the bulk
    data is physically rewritten through the ADIOS2 C++ backend.

    Sub-tables (``ANTENNA``, ``FIELD``, ``SPECTRAL_WINDOW``, etc.) are
    left on their default storage managers because their I/O footprint is
    negligible.

    Args:
        input_ms: Path to the source MeasurementSet.
        output_ms: Destination path for the ADIOS2-backed copy.
        target_columns: Column names to rebind to Adios2StMan.
        overwrite: If ``True``, remove *output_ms* if it already exists.

    Returns:
        The *output_ms* path on success.

    Raises:
        FileNotFoundError: If *input_ms* does not exist.
        FileExistsError: If *output_ms* exists and *overwrite* is ``False``.
        RuntimeError: If Adios2StMan is not available in the current build.
    """
    import casatools  # lazy — casatools is heavy

    if not os.path.exists(input_ms):
        raise FileNotFoundError(f'Input MS not found: {input_ms}')
    if os.path.exists(output_ms):
        if overwrite:
            import shutil

            shutil.rmtree(output_ms)
            logger.info('Removed existing output: %s', output_ms)
        else:
            raise FileExistsError(f'Output MS already exists: {output_ms}')

    target_set = set(target_columns)

    tb = casatools.table()
    tb.open(input_ms)

    dminfo = tb.getdminfo()

    # Collect target columns found in the table and strip them from
    # their original managers.  Adios2StMan only supports a single
    # instance per table, so all rebound columns must be grouped into
    # one manager entry.
    adios2_cols: list[str] = []
    keys_to_drop: list[str] = []

    for _key, manager in list(dminfo.items()):  # iterate over a snapshot; we mutate dminfo below
        managed_cols = manager.get('COLUMNS', [])
        overlap = [c for c in managed_cols if c in target_set]  # columns to migrate
        if not overlap:
            continue
        remaining = [c for c in managed_cols if c not in target_set]  # columns staying put
        old_type = manager['TYPE']
        if remaining:
            # Keep this manager for the non-target columns only.
            manager['COLUMNS'] = remaining
        else:
            # All columns in this manager are targets — drop it entirely.
            keys_to_drop.append(_key)
        adios2_cols.extend(overlap)
        logger.info('Moving %s out of %s (%s)', overlap, manager['NAME'], old_type)

    for k in keys_to_drop:  # prune now-empty managers from the dminfo dict
        del dminfo[k]

    if not adios2_cols:
        tb.close()
        raise RuntimeError(f'None of the target columns {list(target_set)} were found in {input_ms}')

    # Assign a new dminfo key that won't collide with the existing ones.
    max_key = max(int(k.strip('*')) for k in dminfo) if dminfo else 0
    new_key = f'*{max_key + 1}'
    # Single manager entry: Adios2StMan allows only one instance per table.
    dminfo[new_key] = {
        'NAME': 'Adios2StMan',
        'TYPE': 'Adios2StMan',
        'COLUMNS': adios2_cols,  # all target columns consolidated here
    }
    logger.info('Created single Adios2StMan manager for columns: %s', adios2_cols)

    logger.info('Copying %s -> %s (valuecopy, this may take a while)...', input_ms, output_ms)
    tb.copy(newtablename=output_ms, deep=True, valuecopy=True, dminfo=dminfo)
    tb.close()

    logger.info('Conversion complete: %s', output_ms)
    return output_ms


if __name__ == '__main__':
    import argparse

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    parser = argparse.ArgumentParser(
        description='Rewrite a MeasurementSet with Adios2StMan for benchmarking.',
    )
    parser.add_argument('input_ms', help='Path to the source MeasurementSet')
    parser.add_argument('output_ms', help='Destination path for the ADIOS2-backed MS')
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Remove output_ms if it already exists',
    )
    parser.add_argument(
        '--columns',
        nargs='+',
        default=list(_DEFAULT_TARGET_COLUMNS),
        help='Columns to rebind (default: %(default)s)',
    )
    args = parser.parse_args()

    convert_ms_to_adios2(
        args.input_ms,
        args.output_ms,
        target_columns=args.columns,
        overwrite=args.overwrite,
    )
