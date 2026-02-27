"""
CLI entry point for pclean.

Usage::

    python -m pclean --vis my.ms --imagename out --specmode cube \\
        --imsize 512 512 --cell 1arcsec --niter 1000 \\
        --parallel --nworkers 8

All tclean parameters are supported as ``--<name> <value>`` flags.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog='pclean',
        description='Parallel CLEAN imaging with Dask and CASA tools',
    )
    # Data selection
    p.add_argument('--vis', nargs='+', default=[''])
    p.add_argument('--field', default='')
    p.add_argument('--spw', default='')
    p.add_argument('--timerange', default='')
    p.add_argument('--uvrange', default='')
    p.add_argument('--antenna', default='')
    p.add_argument('--scan', default='')
    p.add_argument('--observation', default='')
    p.add_argument('--intent', default='')
    p.add_argument('--datacolumn', default='corrected')
    # Image
    p.add_argument('--imagename', default='')
    p.add_argument('--imsize', nargs='+', type=int, default=[100])
    p.add_argument('--cell', default='1arcsec')
    p.add_argument('--phasecenter', default='')
    p.add_argument('--stokes', default='I')
    p.add_argument('--projection', default='SIN')
    # Spectral
    p.add_argument('--specmode', default='mfs')
    p.add_argument('--nchan', type=int, default=-1)
    p.add_argument('--start', default='')
    p.add_argument('--width', default='')
    p.add_argument('--outframe', default='LSRK')
    p.add_argument('--restfreq', nargs='*', default=[])
    p.add_argument('--interpolation', default='linear')
    # Gridder
    p.add_argument('--gridder', default='standard')
    p.add_argument('--wprojplanes', type=int, default=1)
    p.add_argument('--pblimit', type=float, default=0.2)
    # Deconvolver
    p.add_argument('--deconvolver', default='hogbom')
    p.add_argument('--scales', nargs='*', type=int, default=[])
    p.add_argument('--nterms', type=int, default=2)
    # Weighting
    p.add_argument('--weighting', default='natural')
    p.add_argument('--robust', type=float, default=0.5)
    p.add_argument('--uvtaper', nargs='*', default=[])
    # Iteration
    p.add_argument('--niter', type=int, default=0)
    p.add_argument('--gain', type=float, default=0.1)
    p.add_argument('--threshold', default='0.0mJy')
    p.add_argument('--nsigma', type=float, default=0.0)
    p.add_argument('--cycleniter', type=int, default=-1)
    p.add_argument('--cyclefactor', type=float, default=1.0)
    p.add_argument('--nmajor', type=int, default=-1)
    # Masking
    p.add_argument('--usemask', default='user')
    p.add_argument('--mask', default='')
    p.add_argument('--pbmask', type=float, default=0.0)
    # Restoration
    p.add_argument('--restoration', action='store_true', default=True)
    p.add_argument('--no-restoration', dest='restoration',
                   action='store_false')
    p.add_argument('--pbcor', action='store_true', default=False)
    # Misc
    p.add_argument('--savemodel', default='none')
    p.add_argument('--restart', action='store_true', default=True)
    p.add_argument('--no-restart', dest='restart', action='store_false')
    # Dask parallel
    p.add_argument('--parallel', action='store_true', default=False)
    p.add_argument('--nworkers', type=int, default=None)
    p.add_argument('--scheduler-address', default=None)
    p.add_argument('--threads-per-worker', type=int, default=1)
    p.add_argument('--memory-limit', default='auto')
    p.add_argument('--local-directory', default=None)
    # Logging
    p.add_argument('--log-level', default='INFO',
                   choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    return p


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s %(name)-20s %(levelname)-8s %(message)s',
    )

    from pclean.pclean import pclean

    kwargs = vars(args)
    kwargs.pop('log_level', None)
    # Normalise CLI names to Python names
    kwargs['scheduler_address'] = kwargs.pop('scheduler_address', None)
    kwargs['threads_per_worker'] = kwargs.pop('threads_per_worker', 1)
    kwargs['memory_limit'] = kwargs.pop('memory_limit', '0')
    kwargs['local_directory'] = kwargs.pop('local_directory', None)

    vis = kwargs.pop('vis')
    result = pclean(vis=vis, **kwargs)

    print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
