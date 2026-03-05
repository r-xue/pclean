"""pclean verification script — ALMA IRC+10216 cube imaging (spw25, 1000-ch subset).

Run from the working directory with psrecord:

    cd /zfs/data2/Workspace/nrao/tests/projs/cubeimaging/pclean_v2
    pixi run -e forge psrecord \
        --log /home/rxue/Workspace/nvme/nrao/casa_dist/pclean/pclean/logs/test_alma_pclean_2.rec \
        --include-children --include-io --include-cache --use-timestamp \
        --include-dir /zfs/data2/Workspace/nrao/tests/projs/cubeimaging/pclean_v2 \
        "python /home/rxue/Workspace/nvme/nrao/casa_dist/pclean/pclean/scripts/test_alma_pclean_v2.py \
         > /home/rxue/Workspace/nvme/nrao/casa_dist/pclean/pclean/logs/test_alma_pclean_2.log 2>&1"
"""

import shutil
import time

from pclean import pclean

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DATA_PATH = '/zfs/data0/Workspace/nrao/datasets/alma_if/cubeimaging/'
# DATA_PATH = '/zfs/data2/Workspace/nrao/tests/projs/cubeimaging-rawdata/'

MS = DATA_PATH + 'uid___A002_Xf0fd41_X5f5a_target.ms'
# MS = DATA_PATH + 'uid___A002_Xf0fd41_X5f5a_target_adios2.ms'

IMAGENAME = 'pclean.IRC+10216_sci.spw25.cube.regcal.I'

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
        field='IRC+10216',
        spw='25',
        antenna=(
            '0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,'
            '20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39&'
        ),
        scan=(
            '7,9,11,13,15,17,19,20,24,26,28,30,32,34,36,37,'
            '41,43,45,47,49,51,53,54,58,60,62,64,66,68,70,71,75,77,79'
        ),
        intent='OBSERVE_TARGET#ON_SOURCE',
        datacolumn='data',

        # ---- Image geometry ----
        imagename=IMAGENAME,
        imsize=[8000, 8000],
        cell=['0.0046arcsec'],
        phasecenter='ICRS 09:47:57.4981 +013.16.44.121',
        stokes='I',

        # ---- Spectral setup (1000-channel subset for testing) ----
        specmode='cube',
        nchan=1000,
        start='268.5GHz',
        width='0.2441382MHz',
        outframe='LSRK',
        # Full-cube alternatives:
        # nchan=7677,
        # start='267.5866469025GHz',

        # ---- Gridder ----
        perchanweightdensity=True,
        gridder='standard',
        mosweight=False,
        usepointing=False,
        pblimit=0.2,

        # ---- Deconvolution ----
        deconvolver='hogbom',
        restoration=True,
        pbcor=False,
        # restoringbeam='common',

        # ---- Weighting (briggsbwtaper requires patched casatools) ----
        weighting='briggsbwtaper',
        robust=0.5,
        npixels=0,

        # ---- Iteration control ----
        niter=50000,
        threshold='4.0mJy',
        # threshold='2.0mJy',
        nsigma=0.0,
        interactive=False,
        fullsummary=False,

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
        nworkers=10,
        cube_chunksize=1,
        concat_mode='paged',
        keep_subcubes=True,
        # cluster_type='slurm',
        # slurm_job_mem='10GB',
        # slurm_python='/zfs/nvme/Workspace/nrao/casa_dist/pclean/pclean/.pixi/envs/default/bin/python',
    )
    elapsed = time.monotonic() - t0
    print(f'pclean elapsed time: {elapsed:.2f}s')
