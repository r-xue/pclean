import os

import pytest
from pclean import pclean
from casatools import ctsys

MS_PATH = ctsys.resolve("pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms")


@pytest.mark.skipif(not os.path.exists(MS_PATH), reason=f"Test data not found at {MS_PATH}")
def test_pclean_integration_cube(tmp_path, monkeypatch):
    """
    Run pclean with real data locally if the dataset exists.
    This test replicates the script provided by the user.
    """
    # Change to the temporary directory so outputs are generated there
    monkeypatch.chdir(tmp_path)
    
    # Define parameters from the user's script
    vis_path = MS_PATH
    imagename = 'oussid.s31_0.helms30_sci.spw0.mfs.regcal.I.findcont'
    
    pclean(
        vis=[vis_path],
        field='helms30', 
        spw='0', 
        antenna='0,1,2,3,4,5,6,7,8,9,10&',
        scan='10', 
        intent='OBSERVE_TARGET#ON_SOURCE', 
        datacolumn='data',
        imagename=imagename,
        imsize=[90, 90], 
        cell=['0.91arcsec'], 
        phasecenter='ICRS 01:03:01.3200 -000.32.59.640', 
        stokes='I', 
        specmode='cube', 
        nchan=117,
        start='214.4501854310GHz', 
        width='15.6245970MHz', 
        outframe='LSRK',
        perchanweightdensity=True, 
        gridder='standard', 
        mosweight=False,
        usepointing=False, 
        pblimit=0.2, 
        deconvolver='hogbom', 
        restoration=False,
        restoringbeam=[], 
        pbcor=False, 
        weighting='robust', 
        robust=1.0,
        npixels=0, 
        niter=0, 
        threshold='0mJy', 
        interactive=False,
        fullsummary=False, 
        savemodel='none', 
        parallel=True, 
        cube_chunksize=1
    )
    
    # Basic assertions to check if output files were created
    # Since niter=0, we expect residual image but maybe not restored depending on restoration=False
    # For cleanup, we rely on tmp_path automated cleanup
    
    # Expected outputs from a cube run without restoration (niter=0, restoration=False)
    # usually include .psf, .residual, .sumwt, .pb (if pbcor or implicit/standard gridder which makes .pb)
    
    # Check for .psf
    assert os.path.exists(f"{imagename}.psf") or os.path.exists(f"{imagename}.psf.tt0"), "PSF image not found"
    
    # Check for .residual
    assert os.path.exists(f"{imagename}.residual") or os.path.exists(f"{imagename}.residual.tt0"), "Residual image not found"

    # Check for .pb (primary beam) - 'standard' gridder usually produces this
    assert os.path.exists(f"{imagename}.pb") or os.path.exists(f"{imagename}.pb.tt0"), "PB image not found"
    
    # Check for .sumwt
    assert os.path.exists(f"{imagename}.sumwt") or os.path.exists(f"{imagename}.sumwt.tt0"), "Sumwt image not found"


@pytest.mark.skipif(not os.path.exists(MS_PATH), reason=f"Test data not found at {MS_PATH}")
def test_pclean_integration_mfs(tmp_path, monkeypatch):
    """
    Run pclean in MFS mode with auto-multithresh masking (replicating user tclean call).
    """
    monkeypatch.chdir(tmp_path)

    vis_path = MS_PATH
    imagename = 'oussid.s36_0.helms30_sci.spw0.cont.regcal.I.iter1'

    # Note: Using pclean instead of tclean
    pclean(
        vis=[vis_path],
        field='helms30',
        spw='0:214.4792446490~216.2448696490GHz',  # Can be list or string in pclean? standard is string usually, tclean accepts list
        antenna='0,1,2,3,4,5,6,7,8,9,10&',
        scan='10',
        intent='OBSERVE_TARGET#ON_SOURCE',
        datacolumn='data',
        imagename=imagename,
        imsize=[90, 90],
        cell=['0.91arcsec'],
        phasecenter='ICRS 01:03:01.3200 -000.32.59.640',
        stokes='I',
        specmode='mfs',
        nchan=-1,
        outframe='LSRK',
        perchanweightdensity=False,
        gridder='standard',
        mosweight=False,
        usepointing=False,
        pblimit=0.2,
        deconvolver='hogbom',
        restoration=True,
        restoringbeam='common',
        pbcor=True,
        weighting='briggs',
        robust=0.5,
        npixels=0,
        niter=6000,
        threshold='0.00515Jy',
        nsigma=0.0,
        interactive=False,
        fullsummary=True,
        savemodel='none',
        # Masking parameters (automultithresh)
        usemask='auto-multithresh',
        sidelobethreshold=1.25,
        noisethreshold=5.0,
        lownoisethreshold=2.0,
        negativethreshold=0.0,
        minbeamfrac=0.1,
        growiterations=75,
        dogrowprune=True,
        minpercentchange=1.0,
        fastnoise=False,
        restart=True,
        calcres=False,
        calcpsf=False,
        parallel=True
    )

    # Assertions for MFS run with restoration and pbcor
    
    # 1. Main image (restored)
    # in mfs mode with nterms=1 (default for hogbom), extension is usually .image.tt0 or just .image depending on implementation/CASA version
    # Since specmode='mfs', we look for .image.tt0 widely used
    assert os.path.exists(f"{imagename}.image.tt0") or os.path.exists(f"{imagename}.image"), "Restored image not found"

    # 2. Residual
    assert os.path.exists(f"{imagename}.residual.tt0") or os.path.exists(f"{imagename}.residual"), "Residual image not found"

    # 3. Model
    assert os.path.exists(f"{imagename}.model.tt0") or os.path.exists(f"{imagename}.model"), "Model image not found"

    # 4. Mask (since auto-multithresh was used)
    assert os.path.exists(f"{imagename}.mask.tt0") or os.path.exists(f"{imagename}.mask"), "Mask image not found"
    
    # 5. PB (primary beam)
    assert os.path.exists(f"{imagename}.pb.tt0") or os.path.exists(f"{imagename}.pb"), "PB image not found"

    # 6. PB Corrected Image (since pbcor=True)
    assert os.path.exists(f"{imagename}.image.pbcor.tt0") or os.path.exists(f"{imagename}.image.pbcor"), "PB corrected image not found"


