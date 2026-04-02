"""tclean baseline for IRC+10216 SPW 25 full-cube imaging (v4).

Companion to pclean's test_alma_pclean_v4.yaml — same data selection,
image geometry, and weighting so the two can be compared directly.

Usage:
  # Serial (monolithic CASA):
  casa -c scripts/test_alma_tclean_v4.py

  # MPI parallel (9 processes):
  xvfb-run -a mpicasa -n 9 casa -c scripts/test_alma_tclean_v4.py

  # With psrecord profiling:
  psrecord --log /home/rxue/Workspace/nvme/nrao/casa_dist/pclean/pclean/logs/test_alma_tclean_v4.rec \
      --include-children --include-io --include-cache --use-timestamp \
      --include-dir /zfs/data2/Workspace/nrao/tests/projs/cubeimaging/tclean_v4 \
      "${casampi_envs} xvfb-run -a ${CASA_BIN}/mpicasa ${MPICASA_OPTS} -n 17 ${CASA_BIN}/casa ${CASA_OPTS} -c /home/rxue/Workspace/nvme/nrao/casa_dist/pclean/pclean/scripts/test_alma_tclean_v4.py" \
      > /home/rxue/Workspace/nvme/nrao/casa_dist/pclean/pclean/logs/test_alma_tclean_v4.log 2>&1
"""

import os
import time

from casatasks import tclean

if __name__ == "__main__":

    DATA_DIR = "/zfs/data0/Workspace/nrao/datasets/alma_if/cubeimaging/"
    IMAGE_PREFIX = "tclean.IRC+10216_sci.spw25.cube.regcal.I"

    os.system(f"rm -rf {IMAGE_PREFIX}*")

    start = time.time()
    tclean(
        # --- Data selection ------------------------------------------------
        vis=[os.path.join(DATA_DIR, "uid___A002_Xf0fd41_X5f5a_target.ms")],
        # vis=[os.path.join(DATA_DIR, "uid___A002_Xf0fd41_X5f5a_target_adios2.ms")],
        field="IRC+10216",
        spw="25",
        antenna=(
            "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,"
            "20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39&"
        ),
        scan=(
            "7,9,11,13,15,17,19,20,24,26,28,30,32,34,36,37,"
            "41,43,45,47,49,51,53,54,58,60,62,64,66,68,70,71,75,77,79"
        ),
        intent="OBSERVE_TARGET#ON_SOURCE",
        datacolumn="data",

        # --- Image geometry ------------------------------------------------
        imagename=IMAGE_PREFIX,
        imsize=[128, 128],              # full-field: [8000, 8000]
        cell=["0.0046arcsec"],
        phasecenter="ICRS 09:47:57.460 +013.16.43.94",
        #              full-field: "ICRS 09:47:57.4981 +013.16.44.121"
        stokes="I",

        # --- Spectral axis ------------------------------------------------
        specmode="cube",
        nchan=7677,                     # full SPW
        start="267.5866469025GHz",
        width="0.2441382MHz",
        outframe="LSRK",
        perchanweightdensity=True,

        # --- Gridding ------------------------------------------------------
        gridder="standard",
        mosweight=False,
        usepointing=False,
        pblimit=0.2,

        # --- Deconvolution -------------------------------------------------
        deconvolver="hogbom",
        restoration=False,              # PSF-only run; enable for cleaning
        # restoringbeam="common",
        pbcor=False,

        # --- Weighting -----------------------------------------------------
        weighting="briggsbwtaper",      # requires custom casatools build
        robust=0.5,
        npixels=0,

        # --- Iteration control ---------------------------------------------
        niter=0,                        # dirty image only
        threshold="4.0mJy",             # alt: "2.0mJy"
        nsigma=0.0,
        interactive=False,
        fullsummary=False,

        # --- Auto-multithresh masking --------------------------------------
        usemask="auto-multithresh",
        sidelobethreshold=2.5,
        noisethreshold=5.0,
        lownoisethreshold=1.5,
        negativethreshold=7.0,
        minbeamfrac=0.3,
        growiterations=50,
        dogrowprune=True,
        minpercentchange=1.0,
        fastnoise=True,

        # --- Misc ----------------------------------------------------------
        savemodel="none",
        parallel=True,
    )
