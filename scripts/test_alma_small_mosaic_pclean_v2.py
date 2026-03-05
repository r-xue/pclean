"""pclean verification script — ALMA W43-MM1 cube imaging (spw24).

Run from the working directory with psrecord:

    cd /zfs/nvme/Workspace/nrao/casa_dist/pclean/pclean/working/pclean_small_mosaic_v2
    pixi run -e forge psrecord \
        --log /home/rxue/Workspace/nvme/nrao/casa_dist/pclean/pclean/logs/test_alma_small_mosaic_pclean_2.rec \
        --include-children --include-io --include-cache --use-timestamp \
        --include-dir /zfs/nvme/Workspace/nrao/casa_dist/pclean/pclean/working/pclean_small_mosaic_v2 \
        "python /home/rxue/Workspace/nvme/nrao/casa_dist/pclean/pclean/scripts/test_alma_small_mosaic_pclean_v2.py \
         > /home/rxue/Workspace/nvme/nrao/casa_dist/pclean/pclean/logs/test_alma_small_mosaic_pclean_2.log 2>&1"
"""

import shutil
import time

from pclean import pclean

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

MS = '/zfs/data0/Workspace/nrao/tests/projs/csv-3899/main_eb2/working/uid___A002_X1181695_X1c6a4_targets_line.ms'

IMAGENAME = 'oussid.s39_0.W43-MM1_sci.spw24.repBW.regcal.I.iter1'

# ---------------------------------------------------------------------------

if __name__ == '__main__':
    # Remove any previous output images
    for suffix in (
        '', '.residual', '.image', '.model', '.psf', '.pb',
        '.sumwt', '.weight', '.mask',
    ):
        shutil.rmtree(IMAGENAME + suffix, ignore_errors=True)

    t0 = time.monotonic()
    pclean(
        # ---- Data selection ----
        vis=[MS],
        field='W43-MM1',
        spw=['24'],
        antenna=[
            '0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,'
            '20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42&'
        ],
        scan=['8'],
        intent='OBSERVE_TARGET#ON_SOURCE',
        datacolumn='data',

        # ---- Image geometry ----
        imagename=IMAGENAME,
        imsize=[900, 864],
        cell=['0.14arcsec'],
        phasecenter='ICRS 18:47:46.7005 -001.54.16.747',
        stokes='I',

        # ---- Spectral setup ----
        specmode='cube',
        nchan=28,
        start='115.0066527461GHz',
        width='15.6234856MHz',
        outframe='LSRK',

        # ---- Gridder ----
        perchanweightdensity=True,
        gridder='mosaic',
        mosweight=True,
        usepointing=False,
        pblimit=0.2,

        # ---- Deconvolution ----
        deconvolver='hogbom',
        restoration=True,
        restoringbeam='common',
        pbcor=True,

        # ---- Weighting ----
        weighting='briggsbwtaper',
        robust=2.0,
        npixels=0,

        # ---- Iteration control ----
        niter=6000000,
        threshold='0.00682Jy',
        nsigma=0.0,
        interactive=False,
        fullsummary=False,
        restart=True,
        calcres=True,
        calcpsf=True,

        # ---- Auto-multithresh masking ----
        usemask='auto-multithresh',
        sidelobethreshold=2.5,
        noisethreshold=5.0,
        lownoisethreshold=1.5,
        negativethreshold=7.0,
        minbeamfrac=0.3,
        growiterations=50,
        dogrowprune=True,
        minpercentchange=1.0,
        fastnoise=True,
        savemodel='none',

        # ---- Parallelization ----
        parallel=True,
        concat_mode='paged',
        keep_subcubes=True,
    )
    elapsed = time.monotonic() - t0
    print(f'pclean elapsed time: {elapsed:.2f}s')

