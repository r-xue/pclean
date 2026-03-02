"""Quick diagnostic to verify Adios2StMan availability in the current casatools build."""

import json
import logging
import os
import shutil
import sys
from dataclasses import dataclass, field
from glob import glob
from importlib.metadata import PackageNotFoundError, version

logger = logging.getLogger(__name__)


@dataclass
class CasatoolsInfo:
    """Summary of the casatools installation."""

    version: str = 'unknown'
    origin: str = 'unknown'
    conda_build_string: str = ''
    adios2_supported: bool = False
    details: dict[str, str] = field(default_factory=dict)


def get_casatools_info() -> CasatoolsInfo:
    """Detect casatools version and whether it was installed via conda or pip.

    Inspects conda-meta records first (definitive for conda installs), then
    falls back to ``importlib.metadata`` / ``pip`` provenance checks.

    Returns:
        A populated ``CasatoolsInfo`` dataclass.
    """
    info = CasatoolsInfo()

    # --- version -----------------------------------------------------------
    try:
        info.version = version('casatools')
    except PackageNotFoundError:
        try:
            import casatools
            info.version = getattr(casatools, '__version__', 'unknown')
        except ImportError:
            logger.warning('casatools is not installed')
            return info

    # --- origin (conda vs pip) ---------------------------------------------
    # 1. Check for a conda-meta record in the current prefix.
    conda_prefix = os.environ.get('CONDA_PREFIX', sys.prefix)
    conda_meta = os.path.join(conda_prefix, 'conda-meta')
    matches = glob(os.path.join(conda_meta, 'casatools-*.json'))

    if matches:
        info.origin = 'conda'
        try:
            with open(matches[0], encoding='utf-8') as fh:
                meta = json.load(fh)
            info.conda_build_string = meta.get('build', '')
            info.details['channel'] = meta.get('channel', '')
            info.details['subdir'] = meta.get('subdir', '')
            info.details['build_string'] = info.conda_build_string
        except (OSError, json.JSONDecodeError) as exc:
            logger.debug('Failed to read conda-meta record: %s', exc)
    else:
        # 2. If not conda, check whether pip metadata exists.
        from importlib.util import find_spec
        if find_spec('casatools') is not None:
            info.origin = 'pip'

    logger.info(
        'casatools %s installed via %s (build: %s)',
        info.version, info.origin, info.conda_build_string or 'n/a',
    )
    return info


def check_adios2_support(*, cleanup: bool = True) -> bool:
    """Create a throwaway CASA table with Adios2StMan and report whether it succeeds.

    This attempts to bind a single float column to the ``Adios2StMan``
    storage manager.  If the underlying ``casacore`` was not compiled with
    ADIOS2 support (i.e. the ``nompi`` variant), a ``RuntimeError`` about
    an unknown storage manager is raised.

    Args:
        cleanup: Remove the temporary table directory after the check.

    Returns:
        ``True`` if Adios2StMan is available, ``False`` otherwise.
    """
    import casatools  # lazy import — casatools is heavy

    tb = casatools.table()
    table_name = '_pclean_adios2_probe.tab'

    if os.path.exists(table_name):
        shutil.rmtree(table_name)

    tabledesc = {
        'DATA': {
            'desc': 'Probe column for ADIOS2 support',
            'name': 'DATA',
            'valueType': 'float',
            'ndim': 1,
        }
    }

    dminfo = {
        '*1': {
            'NAME': 'Adios2Probe',
            'TYPE': 'Adios2StMan',
            'COLUMNS': ['DATA'],
        }
    }

    try:
        tb.create(tablename=table_name, tabledesc=tabledesc, dminfo=dminfo)
        bound = tb.getdminfo()
        ok = bound.get('*1', {}).get('TYPE') == 'Adios2StMan'
        tb.close()
        if ok:
            logger.info('Adios2StMan is available in this casatools build')
        else:
            logger.warning('Table created but Adios2StMan binding could not be verified')
        return ok
    except RuntimeError as exc:
        logger.warning('Adios2StMan is NOT available: %s', exc)
        return False
    finally:
        if cleanup and os.path.exists(table_name):
            shutil.rmtree(table_name)


def ms_uses_adios2(ms_path: str) -> bool:
    """Check whether any column in the given MS is managed by Adios2StMan.

    Opens the table read-only, inspects ``getdminfo()``, and returns
    ``True`` if at least one data-manager entry has ``TYPE == 'Adios2StMan'``.

    Args:
        ms_path: Path to a MeasurementSet directory.

    Returns:
        ``True`` if the MS contains ADIOS2-managed columns.
    """
    import casatools

    tb = casatools.table()
    try:
        tb.open(ms_path, nomodify=True)
        dminfo = tb.getdminfo()
        tb.close()
    except Exception as exc:
        logger.debug('Could not inspect dminfo for %s: %s', ms_path, exc)
        return False

    for dm in dminfo.values():
        if isinstance(dm, dict) and dm.get('TYPE') == 'Adios2StMan':
            return True
    return False


def force_omp_single_thread() -> None:
    """Force the OpenMP runtime to use exactly 1 thread.

    General thread-safety precaution for ADIOS2-backed storage managers.
    CASA gridding internals can launch OpenMP tasks that concurrently
    access the MS; limiting to a single thread avoids potential data
    races in the ADIOS2 engine.

    ``os.environ['OMP_NUM_THREADS']`` alone is insufficient because
    ``libgomp`` reads the variable only once (at the first OpenMP
    call, typically during ``import casatools``).  This helper
    therefore also calls ``omp_set_num_threads(1)`` via ``ctypes``
    to override the cached value immediately.
    """
    import ctypes
    import ctypes.util

    os.environ['OMP_NUM_THREADS'] = '1'

    for lib_name in ('gomp', 'omp', 'iomp5'):
        path = ctypes.util.find_library(lib_name)
        if path:
            try:
                lib = ctypes.CDLL(path)
                lib.omp_set_num_threads(ctypes.c_int(1))
                logger.info(
                    'Forced OMP threads to 1 via %s (omp_set_num_threads)',
                    lib_name,
                )
                return
            except (OSError, AttributeError):
                continue

    logger.debug(
        'Could not locate OpenMP runtime library; '
        'relying on OMP_NUM_THREADS env var only',
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    info = get_casatools_info()
    logger.info('casatools version : %s', info.version)
    logger.info('install origin    : %s', info.origin)
    if info.conda_build_string:
        logger.info('conda build string: %s', info.conda_build_string)
    for key, val in info.details.items():
        logger.info('  %14s: %s', key, val)

    available = check_adios2_support()
    status = 'SUPPORTED' if available else 'NOT SUPPORTED'
    logger.info('Adios2StMan       : %s', status)
