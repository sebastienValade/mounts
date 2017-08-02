import os
import numpy as np
import matplotlib.pyplot as plt
# import snappy
from snappy import GPF
from snappy import ProductIO
from snappy import HashMap
# from snappy import jpy
import logging


# --- load all available gpf operators
GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

# --- import hashmap
# NB: http://forum.step.esa.int/t/example-script-for-multiple-operations/2636/2
# HashMap is a dictionary. It maps parameter names (strings) to values (objects).
# It depends on the operator and parameter if a default value exists.
# Even if all parameters have a default value, an empty HashMap needs to be used.

# import snappy
# HashMap = snappy.jpy.get_type('java.util.HashMap')


# --- various (not fully understood)
# ProductIOPlugInManager = snappy.jpy.get_type('org.esa.snap.core.dataio.ProductIOPlugInManager')
# Logger = jpy.get_type('java.util.logging.Logger')
# Level = jpy.get_type('java.util.logging.Level')
# Arrays = jpy.get_type('java.util.Arrays')
# File = jpy.get_type('java.io.File')
# String = jpy.get_type('java.lang.String')
# HashMap = jpy.get_type('java.util.HashMap')
# Logger.getLogger('').setLevel(Level.OFF)
# snappy.SystemUtils.LOG.setLevel(Level.OFF)
# reader_spi_it = ProductIOPlugInManager.getInstance().getAllReaderPlugIns()
# writer_spi_it = ProductIOPlugInManager.getInstance().getAllWriterPlugIns()


def read_product(obj):
    """Open product.

    Arguments:
        obj {string} -- accepts .zip file (S1), or unzipped .SAFE directory (S1+S2)

    Returns:
        product
    """

    logging.info('reading product')

    # file_manifest = obj.path_and_file + '/manifest.safe'
    path_and_file = obj.path_and_file
    p = ProductIO.readProduct(path_and_file)

    return p


def write_product(obj, fileout=None, pathout=None, formatout=None):
    """TO DO: allow other types here: BEAM-DIMAP, GeoTIFF-BigTIFF, HDF5"""

    if fileout is None:
        fileout = get_name(obj)

    if pathout is None:
        pathout = os.getcwd()
        pathout = os.path.join(pathout, '')  # add trailing slash if missing (os independent)

    if formatout is None:
        ext = '.dim'
        formatout = 'BEAM-DIMAP'

    logging.info('saving product')
    ProductIO.writeProduct(obj, pathout + fileout + ext, formatout)


def resample(obj):
    """
    Parameter Options: (sh gpt -h Resample)
        - downsampling=<string>                The method used for aggregation (downsampling to a coarser resolution).
                                                    Value must be one of 'First', 'Min', 'Max', 'Mean', 'Median'.
                                                    Default value is 'First'.
        - flagDownsampling=<string>            The method used for aggregation (downsampling to a coarser resolution) of flags.
                                                    Value must be one of 'First', 'FlagAnd', 'FlagOr', 'FlagMedianAnd', 'FlagMedianOr'.
                                                    Default value is 'First'.
        - referenceBand=<string>               The name of the reference band. All other bands will be re-sampled to match its size and resolution. Either this or targetResolutionor targetWidth and targetHeight must be set.
        - resampleOnPyramidLevels=<boolean>    This setting will increase performance when viewing the image, but accurate resamplings are only retrieved when zooming in on a pixel.
                                                    Default value is 'true'.
        - targetHeight=<integer>               The height that all bands of the target product shall have. If this is set, targetWidth must be set, too. Either this and targetWidth or referenceBand or targetResolution must be set.
        - targetResolution=<integer>           The resolution that all bands of the target product shall have. The same value will be applied to scale image widths and heights. Either this or referenceBand or targetwidth and targetHeight must be set.
        - targetWidth=<integer>                The width that all bands of the target product shall have. If this is set, targetHeight must be set, too. Either this and targetHeight or referenceBand or targetResolution must be set.
        - upsampling=<string>                  The method used for interpolation (upsampling to a finer resolution).
                                                    Value must be one of 'Nearest', 'Bilinear', 'Bicubic'.
                                                    Default value is 'Nearest'.
    """

    logging.info('gpt operator = Resample')

    # --- set operator parameters and apply
    parameters = HashMap()
    parameters.put('referenceBand', 'B1')
    result = GPF.createProduct('Resample', parameters, obj)

    return result


def subset(obj, polygon_wkt=None, north_bound=None, west_bound=None, south_bound=None, east_bound=None):
    """GPT subset

    Optional input argument:

    Arguments:
        obj {[type]} -- [description]

    Keyword Arguments:
        polygon_wkt {string} -- [description] (default: {None})
        north_bound {int} -- [description] (default: {None})
        west_bound {int} -- [description] (default: {None})
        south_bound {int} -- [description] (default: {None})
        east_bound {int} -- [description] (default: {None})

    Returns:
        Product subset

    Parameter Options:
        - copyMetadata=<boolean>                    Whether to copy the metadata of the source product.
                                                        Default value is 'false'.
        - fullSwath=<boolean>                       Forces the operator to extend the subset region to the full swath.
                                                        Default value is 'false'.
        - geoRegion=<geometry>                      The subset region in geographical coordinates using WKT-format,
                                                        e.g. POLYGON((<lon1> <lat1>, <lon2> <lat2>, ..., <lon1> <lat1>))
                                                        (make sure to quote the option due to spaces in <geometry>).
                                                        If not given, the entire scene is used.
        - region=<rectangle>                        The subset region in pixel coordinates.
                                                        Use the following format: <x>,<y>,<width>,<height>
                                                        If not given, the entire scene is used. The 'geoRegion' parameter has precedence over this parameter.
        - sourceBands=<string,string,string,...>    The list of source bands.
        - subSamplingX=<int>                        The pixel sub-sampling step in X (horizontal image direction)
                                                        Default value is '1'.
        - subSamplingY=<int>                        The pixel sub-sampling step in Y (vertical image direction)
                                                        Default value is '1'.
        - tiePointGridNames=<string,string,...>     The comma-separated list of names of tie-point grids to be copied.
                                                        If not given, all bands are copied.
    """

    logging.info('gpt operator = Subset')

    # --- set operator parameters (see: gpt -h operator)
    parameters = HashMap()
    parameters.put('copyMetadata', 'true')
    if polygon_wkt is None:
        if all(v is not None for v in [north_bound, west_bound, south_bound, east_bound]):
            polygon_wkt = "POLYGON((" + str(west_bound) + ' ' + str(south_bound) + ', ' + str(east_bound) + ' ' + str(south_bound) + ', ' + str(east_bound) + ' ' + str(north_bound) + ', ' + str(west_bound) + ' ' + str(north_bound) + ', ' + str(west_bound) + ' ' + str(south_bound) + '))'
        else:
            print('enter valid polygon bounds')
            quit()
    parameters.put('geoRegion', polygon_wkt)

    # --- apply operator
    result = GPF.createProduct('Subset', parameters, obj)

    return result


def topsar_split(obj, subswath_name, polarisation):
    """
    Parameter Options: (gpt -h TOPSAR-Split)
  -PfirstBurstIndex=<integer>                           The first burst index
                                                        Valid interval is [1, *).
                                                        Default value is '1'.
  -PlastBurstIndex=<integer>                            The last burst index
                                                        Valid interval is [1, *).
                                                        Default value is '9999'.
  -PselectedPolarisations=<string,string,string,...>    The list of polarisations
  -Psubswath=<string>                                   The list of source bands.
  -PwktAoi=<string>                                     WKT polygon to be used for selecting bursts
    """

    logging.info('gpt operator = TOPSAR-Split')

    parameters = HashMap()
    parameters.put('subswath', subswath_name)
    parameters.put('selectedPolarisations', polarisation)
    result = GPF.createProduct('TOPSAR-Split', parameters, obj)

    return result


def apply_orbit_file(obj):
    """
    Parameter Options: (gpt -h Apply-Orbit-File)
        - continueOnFail=<boolean>    Sets parameter 'continueOnFail' to <boolean>.
                                        Default value is 'false'.
        - orbitType=<string>          Sets parameter 'orbitType' to <string>.
                                        Value must be one of 'Sentinel Precise (Auto Download)', 'Sentinel Restituted (Auto Download)', 'DORIS Preliminary POR (ENVISAT)', 'DORIS Precise VOR (ENVISAT) (Auto Download)', 'DELFT Precise (ENVISAT, ERS1&2) (Auto Download)', 'PRARE Precise (ERS1&2) (Auto Download)', 'Kompsat5 Precise'.
                                        Default value is 'Sentinel Precise (Auto Download)'.
        - polyDegree=<int>            Sets parameter 'polyDegree' to <int>.
                                        Default value is '3'.
    """

    # NB: aux files automatically downloaded are stored in ".snap/auxdata/Orbits/Sentinel-1/POEORB/S1A/"

    logging.info('gpt operator = Apply-Orbit-File')

    parameters = HashMap()
    # parameters.put('orbitType', "Sentinel Precise (Auto Download)")
    # parameters.put("Orbit State Vectors", "Sentinel Precise (Auto Download)")
    # parameters.put("Polynomial Degree", 3)
    result = GPF.createProduct('Apply-Orbit-File', parameters, obj)

    return result


def back_geocoding(obj_master, obj_slave):
    """Product back-geocoding.

    NB: master image = oldest, slave image = newest

    Parameter Options: (gpt -h Back-Geocoding)
        - demName=<string>                         The digital elevation model.
                                                        Default value is 'SRTM 3Sec'.
        - demResamplingMethod=<string>             Sets parameter 'demResamplingMethod' to <string>.
                                                        Default value is 'BICUBIC_INTERPOLATION'.
        - disableReramp=<boolean>                  Sets parameter 'disableReramp' to <boolean>.
                                                     Default value is 'false'.
        - externalDEMFile=<file>                   Sets parameter 'externalDEMFile' to <file>.
        - externalDEMNoDataValue=<double>          Sets parameter 'externalDEMNoDataValue' to <double>.
                                                        Default value is '0'.
        - maskOutAreaWithoutElevation=<boolean>    Sets parameter 'maskOutAreaWithoutElevation' to <boolean>.
                                                        Default value is 'true'.
        - outputDerampDemodPhase=<boolean>         Sets parameter 'outputDerampDemodPhase' to <boolean>.
                                                        Default value is 'false'.
        - outputRangeAzimuthOffset=<boolean>       Sets parameter 'outputRangeAzimuthOffset' to <boolean>.
                                                        Default value is 'false'.
        - resamplingType=<string>                  The method to be used when resampling the slave grid onto the master grid.
                                                        Default value is 'BISINC_5_POINT_INTERPOLATION'.
    """

    logging.info('gpt operator = Back-Geocoding')

    parameters = HashMap()
    parameters.put("Digital Elevation Model", "SRTM 3Sec (Auto Download)")
    parameters.put("DEM Resampling Method", "BICUBIC_INTERPOLATION")
    parameters.put("Resampling Type", "BISINC_5_POINT_INTERPOLATION")
    parameters.put("Mask out areas with no elevation", True)
    parameters.put("Output Deramp and Demod Phase", False)

    # NB: list of products in reverse order: [slave=newest, master=oldest]
    prods = []
    prods.append(obj_slave)
    prods.append(obj_master)
    result = GPF.createProduct('Back-Geocoding', parameters, prods)

    return result


def deburst(obj):
    """"
    Parameter Options: (gpt -h TOPSAR-Deburst)
        - selectedPolarisations=<string,string,string,...>    The list of polarisations
    """

    logging.info('gpt operator = TOPSAR-Deburst')

    parameters = HashMap()
    result = GPF.createProduct('TOPSAR-Deburst', parameters, obj)

    return result


def interferogram(obj):
    """"
    Parameter Options: (gpt -h Interferogram)
        - cohWinAz=<int>                      Size of coherence estimation window in Azimuth direction
                                                    Default value is '10'.
        - cohWinRg=<int>                      Size of coherence estimation window in Range direction
                                                    Default value is '10'.
        - includeCoherence=<boolean>          Sets parameter 'includeCoherence' to <boolean>.
                                                    Default value is 'true'.
        - orbitDegree=<int>                   Degree of orbit (polynomial) interpolator
                                                    Value must be one of '1', '2', '3', '4', '5'.
                                                    Default value is '3'.
        - squarePixel=<boolean>               Use ground square pixel
                                                    Default value is 'true'.
        - srpNumberPoints=<int>               Number of points for the 'flat earth phase' polynomial estimation
                                                    Value must be one of '301', '401', '501', '601', '701', '801', '901', '1001'.
                                                    Default value is '501'.
        - srpPolynomialDegree=<int>           Order of 'Flat earth phase' polynomial
                                                    Value must be one of '1', '2', '3', '4', '5', '6', '7', '8'.
                                                    Default value is '5'.
        - subtractFlatEarthPhase=<boolean>    Sets parameter 'subtractFlatEarthPhase' to <boolean>.
                                                    Default value is 'true'.
    """

    logging.info('gpt operator = Interferogram')

    parameters = HashMap()
    parameters.put('Subtract flat-earth phase', True)
    parameters.put('Degree of "Flat Earth" polynomial', 5)
    parameters.put('Number of "Flat Earth" estimation points', 501)
    parameters.put('Orbit interpolation degree', 3)
    parameters.put('Include coherence estimation', True)
    parameters.put('Square Pixel', False)
    parameters.put('Independent Window Sizes', False)
    parameters.put('Coherence Azimuth Window Size', 10)
    parameters.put('Coherence Range Window Size', 10)

    result = GPF.createProduct('Interferogram', parameters, obj)

    return result


def topo_phase_removal(obj):
    """"
    Parameter Options: (gpt -h TopoPhaseRemoval)
        - demName=<string>                   The digital elevation model.
                                                    Default value is 'SRTM 3Sec'.
        - externalDEMFile=<file>             Sets parameter 'externalDEMFile' to <file>.
        - externalDEMNoDataValue=<double>    Sets parameter 'externalDEMNoDataValue' to <double>.
                                                    Default value is '0'.
        - orbitDegree=<int>                  Degree of orbit interpolation polynomial
                                                    Valid interval is (1, 10].
                                                    Default value is '3'.
        - outputElevationBand=<boolean>      Output elevation band.
                                                    Default value is 'false'.
        - outputTopoPhaseBand=<boolean>      Output topographic phase band.
                                                    Default value is 'false'.
        - tileExtensionPercent=<string>      Define extension of tile for DEM simulation (optimization parameter).
                                                    Default value is '100'.
    """

    logging.info('gpt operator = TopoPhaseRemoval')

    parameters = HashMap()
    result = GPF.createProduct('TopoPhaseRemoval', parameters, obj)

    return result


def goldstein_phase_filtering(obj):
    """"
    Parameter Options: (gpt -h GoldsteinPhaseFiltering)
        - alpha=<double>                 adaptive filter exponent
                                                    Valid interval is (0, 1].
                                                    Default value is '1.0'.
        - coherenceThreshold=<double>    The coherence threshold
                                                    Valid interval is [0, 1].
                                                    Default value is '0.2'.
        - FFTSizeString=<string>         Sets parameter 'FFTSizeString' to <string>.
                                                    Value must be one of '32', '64', '128', '256'.
                                                    Default value is '64'.
        - useCoherenceMask=<boolean>     Use coherence mask
                                                    Default value is 'false'.
        - windowSizeString=<string>      Sets parameter 'windowSizeString' to <string>.
                                                    Value must be one of '3', '5', '7'.
                                                    Default value is '3'.
    """

    logging.info('gpt operator = GoldsteinPhaseFiltering')

    parameters = HashMap()
    result = GPF.createProduct('GoldsteinPhaseFiltering', parameters, obj)

    return result


def terrain_correction(obj):
    """"Range Doppler method for orthorectification.

    Parameter Options: (gpt -h Terrain-Correction)
        - alignToStandardGrid=<boolean>                 Force the image grid to be aligned with a specific point
                                                            Default value is 'false'.
        - applyRadiometricNormalization=<boolean>       Sets parameter 'applyRadiometricNormalization' to <boolean>.
                                                            Default value is 'false'.
        - auxFile=<string>                              The auxiliary file
                                                            Value must be one of 'Latest Auxiliary File', 'Product Auxiliary File', 'External Auxiliary File'.
                                                            Default value is 'Latest Auxiliary File'.
        - demName=<string>                              The digital elevation model.
                                                            Default value is 'SRTM 3Sec'.
        - demResamplingMethod=<string>                  Sets parameter 'demResamplingMethod' to <string>.
                                                            Value must be one of 'NEAREST_NEIGHBOUR', 'BILINEAR_INTERPOLATION', 'CUBIC_CONVOLUTION', 'BISINC_5_POINT_INTERPOLATION', 'BISINC_11_POINT_INTERPOLATION', 'BISINC_21_POINT_INTERPOLATION', 'BICUBIC_INTERPOLATION', 'DELAUNAY_INTERPOLATION'.
                                                            Default value is 'BILINEAR_INTERPOLATION'.
        - externalAuxFile=<file>                        The antenne elevation pattern gain auxiliary data file.
        - externalDEMApplyEGM=<boolean>                 Sets parameter 'externalDEMApplyEGM' to <boolean>.
                                                            Default value is 'true'.
        - externalDEMFile=<file>                        Sets parameter 'externalDEMFile' to <file>.
        - externalDEMNoDataValue=<double>               Sets parameter 'externalDEMNoDataValue' to <double>.
                                                            Default value is '0'.
        - imgResamplingMethod=<string>                  Sets parameter 'imgResamplingMethod' to <string>.
                                                            Value must be one of 'NEAREST_NEIGHBOUR', 'BILINEAR_INTERPOLATION', 'CUBIC_CONVOLUTION', 'BISINC_5_POINT_INTERPOLATION', 'BISINC_11_POINT_INTERPOLATION', 'BISINC_21_POINT_INTERPOLATION', 'BICUBIC_INTERPOLATION'.
                                                            Default value is 'BILINEAR_INTERPOLATION'.
        - incidenceAngleForGamma0=<string>              Sets parameter 'incidenceAngleForGamma0' to <string>.
                                                            Value must be one of 'Use incidence angle from Ellipsoid', 'Use local incidence angle from DEM', 'Use projected local incidence angle from DEM'.
                                                            Default value is 'Use projected local incidence angle from DEM'.
        - incidenceAngleForSigma0=<string>              Sets parameter 'incidenceAngleForSigma0' to <string>.
                                                            Value must be one of 'Use incidence angle from Ellipsoid', 'Use local incidence angle from DEM', 'Use projected local incidence angle from DEM'.
                                                            Default value is 'Use projected local incidence angle from DEM'.
        - mapProjection=<string>                        The coordinate reference system in well known text format
                                                            Default value is 'WGS84(DD)'.
        - nodataValueAtSea=<boolean>                    Mask the sea with no data value (faster)
                                                            Default value is 'true'.
        - outputComplex=<boolean>                       Sets parameter 'outputComplex' to <boolean>.
                                                            Default value is 'false'.
        - pixelSpacingInDegree=<double>                 The pixel spacing in degrees
                                                            Default value is '0'.
        - pixelSpacingInMeter=<double>                  The pixel spacing in meters
                                                            Default value is '0'.
        - saveBetaNought=<boolean>                      Sets parameter 'saveBetaNought' to <boolean>.
                                                            Default value is 'false'.
        - saveDEM=<boolean>                             Sets parameter 'saveDEM' to <boolean>.
                                                            Default value is 'false'.
        - saveGammaNought=<boolean>                     Sets parameter 'saveGammaNought' to <boolean>.
                                                            Default value is 'false'.
        - saveIncidenceAngleFromEllipsoid=<boolean>     Sets parameter 'saveIncidenceAngleFromEllipsoid' to <boolean>.
                                                            Default value is 'false'.
        - saveLatLon=<boolean>                          Sets parameter 'saveLatLon' to <boolean>.
                                                            Default value is 'false'.
        - saveLocalIncidenceAngle=<boolean>             Sets parameter 'saveLocalIncidenceAngle' to <boolean>.
                                                            Default value is 'false'.
        - saveProjectedLocalIncidenceAngle=<boolean>    Sets parameter 'saveProjectedLocalIncidenceAngle' to <boolean>.
                                                            Default value is 'false'.
        - saveSelectedSourceBand=<boolean>              Sets parameter 'saveSelectedSourceBand' to <boolean>.
                                                            Default value is 'true'.
        - saveSigmaNought=<boolean>                     Sets parameter 'saveSigmaNought' to <boolean>.
                                                            Default value is 'false'.
        - sourceBands=<string,string,string,...>        The list of source bands.
        - standardGridOriginX=<double>                  x-coordinate of the standard grid's origin point
                                                            Default value is '0'.
        - standardGridOriginY=<double>                  y-coordinate of the standard grid's origin point
                                                            Default value is '0'.
    """

    logging.info('gpt operator = Terrain-Correction')

    parameters = HashMap()

    print('---> warning: check if selected source band valid')
    print('---> selected band = ' + 'phase')
    print('---> available bands = ')
    get_bandnames(obj)

    parameters.put('sourceBands', 'phase')
    result = GPF.createProduct('Terrain-Correction', parameters, obj)

    return result


def band_maths():
    # marpet code: https://github.com/senbox-org/snap-engine/blob/master/snap-python/src/main/resources/snappy/examples/snappy_bmaths.py
    pass


####################################################################


def get_rasterDim(obj, band_name):

    band = obj.getBand(band_name)
    w = band.getRasterWidth()
    h = band.getRasterHeight()

    return w, h


def print_rasterDim(obj, band_name):
    w, h = get_rasterDim(obj, band_name)
    print(str(w) + ' x ' + str(h))


def print_engineConfig():

    # => update parameters used in:
    # >> /home/khola/SOFTWARES/ESA/snap/etc/snap.properties

    # http://forum.step.esa.int/t/basic-error-with-snappy-python-beginner/5220/5
    from snappy import EngineConfig
    print('tileCachSize', EngineConfig.instance().preferences().get('snap.jai.tileCacheSize', None))
    print('parallelism', EngineConfig.instance().preferences().get('snap.parallelism', None))


def get_bandnames(obj):

    r = obj.getBandNames()
    r = list(r)
    print(r)

    return r


def get_name(obj):
    r = obj.getName()
    return r


def read_xmlnodes(obj):

    path_and_file = '/home/khola/DATA/data_satellite/tmp/S2A_MSIL1C_20170202T090201_N0204_R007_T35SNA_20170202T090155.SAFE'

    fxml = path_and_file + '/MTD_MSIL1C.xml'

    product = ProductIO.readProduct(fxml)
    maskGroup = product.getMaskGroup()
    names = maskGroup.getNodeNames()
    for name in names:
        print(name)

    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()


####################################################################


def plotBand(obj, band_name, fout=None):

    logging.info('plotting band')

    band = obj.getBand(band_name)

    # --- get band dimensions
    w, h = get_rasterDim(obj, band_name)

    # band_data = np.zeros(w * h, np.float32)
    # band.readPixels(0, 0, w, h, band_data)
    band_data = np.zeros(w * h, np.float32)
    band.readPixels(0, 0, w, h, band_data)

    obj.dispose()
    band_data.shape = h, w
    imgplot = plt.imshow(band_data)

    if fout is None:
        fout = 'band_' + band_name + '.png'

    imgplot.write_png(fout)