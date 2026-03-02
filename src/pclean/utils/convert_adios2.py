"""Convert a MeasurementSet to use the Adios2StMan storage manager."""

import logging
import os

logger = logging.getLogger(__name__)

# Heavy visibility columns whose I/O dominates runtime.
_DEFAULT_TARGET_COLUMNS = ('DATA', 'CORRECTED_DATA', 'MODEL_DATA', 'FLAG', 'WEIGHT', 'SIGMA')


def _normalize_adios2_size(value: str) -> str:
    """Normalise a human-friendly size string to ADIOS2's expected format.

    ADIOS2 accepts ``Kb``, ``Mb``, ``Gb``, ``Tb`` (note lowercase ``b``).
    Common variants like ``2GB``, ``512mb``, ``4gb`` are rewritten here.
    """
    import re

    m = re.fullmatch(r'(\d+)\s*([KMGT])(?:i?[Bb])?', value.strip(), re.IGNORECASE)
    if m:
        return f'{m.group(1)}{m.group(2).upper()}b'
    return value  # pass through as-is (may be a plain byte count)


def convert_ms_to_adios2(
    input_ms: str,
    output_ms: str,
    *,
    target_columns: tuple[str, ...] | list[str] = _DEFAULT_TARGET_COLUMNS,
    overwrite: bool = False,
    engine_type: str = 'BP4',
    engine_params: dict[str, str] | None = None,
    adios2_xml: str | None = None,
    taql: str | None = None,
) -> str:
    """Copy a MeasurementSet, rebinding heavy columns to Adios2StMan.

    The function reads the existing ``dminfo`` from *input_ms*, replaces
    the storage-manager type for every manager that handles one of the
    *target_columns*, and performs a deep ``valuecopy`` so that the bulk
    data is physically rewritten through the ADIOS2 C++ backend.

    Sub-tables (``ANTENNA``, ``FIELD``, ``SPECTRAL_WINDOW``, etc.) are
    left on their default storage managers because their I/O footprint is
    negligible.

    Note:
        Adios2StMan requires the copy to happen in a single
        ``Table::deepCopy`` pass.  Manual row-level approaches
        (``addrows`` + ``putcol``, or ``copyrows``) are not supported
        because the ADIOS2 engine needs cell shapes established through
        casacore's internal copy path and does not allow reopening a
        table for append.

        Casacore's C++ ``deepCopy`` streams data row-by-row, but the
        ADIOS2 BP engine accumulates all ``Put()`` data within a single
        step — ``EndStep()`` / ``Close()`` run only in the Adios2StMan
        destructor.  Use *engine_params* to control buffer sizing.

        The default ADIOS2 engine (usually BP5) **ignores**
        ``MaxBufferSize``; it only honours ``BufferChunkSize``.  This
        function defaults to ``BP4`` and passes the engine type via the
        ``ENGINETYPE`` dminfo SPEC field so casacore's
        ``Adios2StMan::makeObject`` sets the correct engine before
        opening.

    Args:
        input_ms: Path to the source MeasurementSet.
        output_ms: Destination path for the ADIOS2-backed copy.
        target_columns: Column names to rebind to Adios2StMan.
        overwrite: If ``True``, remove *output_ms* if it already exists.
        engine_type: ADIOS2 engine type.  ``'BP4'`` is recommended
            because BP4 respects ``MaxBufferSize`` and flushes to disk
            when the buffer exceeds that cap.  BP5 uses a different
            allocation model (see ``BufferChunkSize``).
        engine_params: ADIOS2 engine parameters.  Useful keys:

            * ``MaxBufferSize`` — triggers flush when exceeded
              (BP4 only, e.g. ``'2Gb'``).
            * ``InitialBufferSize`` — starting allocation (BP4).
            * ``BufferGrowthFactor`` — growth multiplier (BP4).
            * ``BufferChunkSize`` — per-chunk size (BP5).
        adios2_xml: Path to a user-supplied ADIOS2 XML config file.
            If provided, *engine_type* and *engine_params* are ignored.
        taql: Optional TaQL ``WHERE`` clause to select a subset of
            rows before copying (e.g.
            ``'DATA_DESC_ID IN [0]'``).  When set, ``tb.query(taql)``
            is used as the copy source, so only matching rows are
            written.  Sub-tables are copied as-is.

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
    adios2_spec: dict = {
        'NAME': 'Adios2StMan',
        'TYPE': 'Adios2StMan',
        'COLUMNS': adios2_cols,  # all target columns consolidated here
    }

    # Configure engine type and params via the SPEC record fields that
    # casacore's Adios2StMan::makeObject reads (ENGINETYPE / ENGINEPARAMS).
    # This avoids generating an XML file (which triggered a stoul crash
    # in some ADIOS2 builds) and ensures BP4 is used instead of BP5
    # (BP5 ignores MaxBufferSize).
    spec: dict[str, str | dict[str, str]] = {}
    if adios2_xml:
        # User-supplied XML: pass it directly — engine_type/engine_params ignored.
        spec['XMLFILE'] = adios2_xml
        logger.info('Using user-supplied ADIOS2 XML config: %s', adios2_xml)
    else:
        spec['ENGINETYPE'] = engine_type
        merged_params = dict(engine_params or {})
        # Normalise size strings so ADIOS2's parser doesn't choke on
        # common variants like '2GB' (it expects '2Gb').
        for size_key in ('MaxBufferSize', 'InitialBufferSize', 'BufferChunkSize'):
            if size_key in merged_params:
                merged_params[size_key] = _normalize_adios2_size(merged_params[size_key])
        if merged_params:
            spec['ENGINEPARAMS'] = merged_params
            logger.info('Adios2StMan engine: %s, params: %s', engine_type, merged_params)
        else:
            logger.info('Adios2StMan engine: %s (default params)', engine_type)

    adios2_spec['SPEC'] = spec
    dminfo[new_key] = adios2_spec
    logger.info('Created single Adios2StMan manager for columns: %s', adios2_cols)

    # Adios2StMan requires a single-pass deepCopy — manual row-level
    # approaches (addrows+putcol, copyrows) abort because:
    #   - addrows leaves variable-shape cells with null metadata
    #     (ADIOS2 SetShape null-pointer SIGABRT)
    #   - copyrows reopens the table internally; ADIOS2 rejects the
    #     open mode for append ("Engine open mode not valid")
    # Casacore's C++ deepCopy streams data row-by-row so it does not
    # load the full table into Python; peak memory is from ADIOS2's
    # internal write buffers.
    # Optionally restrict to a row subset via TaQL.
    if taql:
        src = tb.query(query=taql)
        logger.info('TaQL selection: %s (%d -> %d rows)', taql, tb.nrows(), src.nrows())
    else:
        src = tb

    nrow = src.nrows()
    logger.info('Copying %s -> %s (%d rows, valuecopy, this may take a while)...', input_ms, output_ms, nrow)
    src.copy(newtablename=output_ms, deep=True, valuecopy=True, dminfo=dminfo)

    if taql:
        src.close()
    tb.close()

    logger.info('Conversion complete: %s', output_ms)
    return output_ms


def _get_spw_ids(vis: str) -> list[int]:
    """Return sorted list of SPW IDs present in *vis*."""
    import casatools

    ms = casatools.ms()
    ms.open(vis)
    spw_info = ms.getspectralwindowinfo()
    ms.close()
    return sorted(int(k) for k in spw_info.keys())


def _get_ddids_for_spw(vis: str, spw_id: int) -> list[int]:
    """Return DATA_DESC_IDs that map to *spw_id* via the DATA_DESCRIPTION sub-table."""
    import casatools

    tb = casatools.table()
    tb.open(os.path.join(vis, 'DATA_DESCRIPTION'))
    spw_col = tb.getcol('SPECTRAL_WINDOW_ID')
    tb.close()
    return [int(i) for i, s in enumerate(spw_col) if s == spw_id]


def split_and_convert_ms_to_adios2(
    input_ms: str,
    output_dir: str,
    *,
    target_columns: tuple[str, ...] | list[str] = _DEFAULT_TARGET_COLUMNS,
    overwrite: bool = False,
    engine_type: str = 'BP4',
    engine_params: dict[str, str] | None = None,
    adios2_xml: str | None = None,
) -> list[str]:
    """Select rows by SPW and convert each subset to Adios2StMan in one pass.

    This implements *Workaround 3* from the Adios2StMan debug notes.
    For each SPW the function builds a TaQL ``DATA_DESC_ID IN [...]``
    clause and passes it to `convert_ms_to_adios2` via the *taql*
    parameter.  The row selection and ADIOS2 rebinding happen in a
    single ``deepCopy`` — no intermediate MS is written.

    Sub-tables (``SPECTRAL_WINDOW``, ``DATA_DESCRIPTION``, etc.) are
    copied as-is and therefore still contain entries for all SPWs.
    This is cosmetic; the imager only accesses rows present in the
    main table.

    Args:
        input_ms: Path to the source MeasurementSet.
        output_dir: Directory under which per-SPW ADIOS2 datasets are
            written (``<output_dir>/<basename>_spw<N>.ms``).
        target_columns: Columns to rebind to Adios2StMan.
        overwrite: If ``True``, remove existing outputs.
        engine_type: ADIOS2 engine type (forwarded to
            `convert_ms_to_adios2`).
        engine_params: ADIOS2 engine parameters.
        adios2_xml: Path to a user-supplied ADIOS2 XML config.

    Returns:
        List of output ADIOS2-backed MS paths.
    """
    if not os.path.exists(input_ms):
        raise FileNotFoundError(f'Input MS not found: {input_ms}')

    os.makedirs(output_dir, exist_ok=True)
    spws = _get_spw_ids(input_ms)
    logger.info('Converting %s per-SPW (%d SPWs) to ADIOS2', input_ms, len(spws))

    outputs: list[str] = []
    for spw_id in spws:
        out_ms = os.path.join(output_dir, f'spw{spw_id}.ms')

        # Build TaQL to select rows for this SPW.
        ddids = _get_ddids_for_spw(input_ms, spw_id)
        ddid_csv = ','.join(str(d) for d in ddids)
        taql = f'DATA_DESC_ID IN [{ddid_csv}]'

        convert_ms_to_adios2(
            input_ms,
            out_ms,
            target_columns=target_columns,
            overwrite=overwrite,
            engine_type=engine_type,
            engine_params=engine_params,
            adios2_xml=adios2_xml,
            taql=taql,
        )
        outputs.append(out_ms)

    logger.info('Done. %d ADIOS2 datasets in %s', len(outputs), output_dir)
    return outputs


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
    parser.add_argument(
        '--engine-type',
        default='BP4',
        help='ADIOS2 engine type (default: %(default)s). BP4 respects MaxBufferSize.',
    )
    parser.add_argument(
        '--max-buffer-size',
        default=None,
        help='ADIOS2 MaxBufferSize engine param, triggers flush when exceeded (BP4 only, e.g. 2Gb)',
    )
    parser.add_argument(
        '--adios2-xml',
        default=None,
        help='Path to a user-supplied ADIOS2 XML config file (overrides --engine-type and --max-buffer-size)',
    )
    parser.add_argument(
        '--split-spw',
        action='store_true',
        help='Convert each SPW separately via TaQL row selection '
             '(workaround for the getSliceV dimension bug). '
             'output_ms is treated as a directory.',
    )
    parser.add_argument(
        '--taql',
        default=None,
        help='TaQL WHERE clause to select a row subset (e.g. "DATA_DESC_ID IN [0]")',
    )
    args = parser.parse_args()

    ep: dict[str, str] | None = None
    if args.max_buffer_size:
        ep = {'MaxBufferSize': _normalize_adios2_size(args.max_buffer_size)}

    if args.split_spw:
        split_and_convert_ms_to_adios2(
            args.input_ms,
            args.output_ms,
            target_columns=args.columns,
            overwrite=args.overwrite,
            engine_type=args.engine_type,
            engine_params=ep,
            adios2_xml=args.adios2_xml,
        )
    else:
        convert_ms_to_adios2(
            args.input_ms,
            args.output_ms,
            target_columns=args.columns,
            overwrite=args.overwrite,
            engine_type=args.engine_type,
            engine_params=ep,
            adios2_xml=args.adios2_xml,
            taql=args.taql,
        )
