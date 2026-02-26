"""
Integration test: VLASS quicklook mtmfs mosaic (parallel).

Exercises the parallel-continuum (row-chunk) engine with
deconvolver='mtmfs', gridder='mosaic', nterms=2, niter=0.
Requires the VLASS quicklook regression dataset.
"""

import os

import pytest
from casatools import ctsys

from pclean import pclean

VLASS_QL_MS = ctsys.resolve(
    "pl-regressiontest/vlass_quicklook/"
    "TSKY0001.sb32295801.eb32296475.57549.31722762731_split_withcorrectdata.ms"
)


@pytest.mark.skipif(
    not os.path.exists(VLASS_QL_MS),
    reason=f"VLASS quicklook MS not found at {VLASS_QL_MS}",
)
def test_vlass_mtmfs_mosaic_parallel(tmp_path, monkeypatch):
    """Parallel mtmfs mosaic imaging (niter=0, no restoration)."""
    monkeypatch.chdir(tmp_path)

    imagename = "quicklook.I.iter0"

    pclean(
        vis=[VLASS_QL_MS],
        field=(
            "128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,"
            "144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,"
            "250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,"
            "266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,"
            "532,533,534,535,536,537,538,539,540,541,542,543,544,545,546,547,"
            "548,549,550,551,552,553,554,555,556,557,558,559,560,561,562,563,"
            "654,655,656,657,658,659,660,661,662,663,664,665,666,667,668,669,"
            "670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,"
            "936,937,938,939,940,941,942,943,944,945,946,947,948,949,950,951,"
            "952,953,954,955,956,957,958,959,960,961,962,963,964,965,966,967,"
            "1058,1059,1060,1061,1062,1063,1064,1065,1066,1067,1068,1069,"
            "1070,1071,1072,1073,1074,1075,1076,1077,1078,1079,1080,1081,"
            "1082,1083,1084,1085,1086,1087,1088,1089,"
            "1340,1341,1342,1343,1344,1345,1346,1347,1348,1349,1350,1351,"
            "1352,1353,1354,1355,1356,1357,1358,1359,1360,1361,1362,1363,"
            "1364,1365,1366,1367,1368,1369,1370,1371,"
            "1462,1463,1464,1465,1466,1467,1468,1469,1470,1471,1472,1473,"
            "1474,1475,1476,1477,1478,1479,1480,1481,1482,1483,1484,1485,"
            "1486,1487,1488,1489,1490,1491,1492,1493,"
            "1744,1745,1746,1747,1748,1749,1750,1751,1752,1753,1754,1755,"
            "1756,1757,1758,1759,1760,1761,1762,1763,1764,1765,1766,1767,"
            "1768,1769,1770,1771,1772,1773,1774,1775,"
            "1866,1867,1868,1869,1870,1871,1872,1873,1874,1875,1876,1877,"
            "1878,1879,1880,1881,1882,1883,1884,1885,1886,1887,1888,1889,"
            "1890,1891,1892,1893,1894,1895,1896,1897,"
            "2148,2149,2150,2151,2152,2153,2154,2155,2156,2157,2158,2159,"
            "2160,2161,2162,2163,2164,2165,2166,2167,2168,2169,2170,2171,"
            "2172,2173,2174,2175,2176,2177,2178,2179"
        ),
        spw="2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17",
        antenna="0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26&",
        scan="15,18,21,24,29",
        intent="OBSERVE_TARGET#UNSPECIFIED",
        datacolumn="corrected",
        imagename=imagename,
        imsize=[512, 512],
        cell="1.0arcsec",
        phasecenter="J2000 20:11:59.992 -000.42.36.0000",
        stokes="I",
        specmode="mfs",
        reffreq="3.0GHz",
        nchan=-1,
        outframe="LSRK",
        perchanweightdensity=False,
        gridder="mosaic",
        wprojplanes=1,
        mosweight=False,
        conjbeams=False,
        usepointing=False,
        rotatepastep=360.0,
        pblimit=0.2,
        deconvolver="mtmfs",
        scales=[0],
        nterms=2,
        restoration=False,
        restoringbeam="common",
        pbcor=False,
        weighting="briggs",
        robust=1.0,
        npixels=0,
        niter=0,
        threshold="0.0mJy",
        nsigma=0,
        cycleniter=-1,
        cyclefactor=1.0,
        interactive=0,
        fastnoise=True,
        savemodel="none",
        parallel=True,
    )

    # -- mtmfs with nterms=2 produces .tt0 / .tt1 extensions ----------
    for tt in range(2):
        assert os.path.isdir(f"{imagename}.psf.tt{tt}"), \
            f"PSF tt{tt} not found"
        assert os.path.isdir(f"{imagename}.residual.tt{tt}"), \
            f"Residual tt{tt} not found"

    # Cross-term PSF: .psf.tt2 (2*nterms - 1 = 3 terms total)
    assert os.path.isdir(f"{imagename}.psf.tt2"), "PSF tt2 not found"

    # Weight / sumwt images
    assert os.path.isdir(f"{imagename}.weight.tt0"), "Weight tt0 not found"
    assert os.path.isdir(f"{imagename}.sumwt.tt0"), "Sumwt tt0 not found"

    # PB (mosaic gridder)
    assert os.path.isdir(f"{imagename}.pb.tt0"), "PB tt0 not found"

    # Model images (niter=0 → zeros, but should still be created)
    assert os.path.isdir(f"{imagename}.model.tt0"), "Model tt0 not found"

    # Partial images should have been cleaned up (keep_partimages defaults False)
    import glob
    leftover = glob.glob(f"{imagename}.part.*")
    assert leftover == [], f"Partial images not cleaned up: {leftover}"
