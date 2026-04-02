"""pclean.utils — partitioning, concatenation, and miscellaneous helpers."""

from contextlib import contextmanager

from pclean.utils.memory_estimate import (
    estimate_peak_ram_gib,
    estimate_worker_memory_gib,
    recommend_nworkers,
)

_casalog = None


def _get_casalog():
    """Return a process-wide ``logsink`` singleton (lazy-initialised)."""
    global _casalog
    if _casalog is None:
        from casatools import logsink as _logsink_cls

        _casalog = _logsink_cls()
    return _casalog


@contextmanager
def suppress_casalog_msgs(messages: list[str]):
    """Context manager / decorator that suppresses specific CASA log messages.

    Uses ``casalog.filterMsg`` / ``clearFilterMsgList`` with the
    required ``setglobal(True)`` call so the filter actually takes
    effect.  The filter is always cleared on exit.

    Can be used as a ``with`` block::

        with suppress_casalog_msgs(['No table opened']):
            ...

    or as a decorator::

        @suppress_casalog_msgs(['No table opened'])
        def teardown(self):
            ...
    """
    casalog = _get_casalog()
    casalog.setglobal(True)
    casalog.filterMsg(messages)
    try:
        yield
    finally:
        casalog.clearFilterMsgList()
