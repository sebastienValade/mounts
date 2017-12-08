import os
import numpy as np
from snappy import GPF
from snappy import ProductIO
from snappy import HashMap
import logging
import utilityme as utils
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


# --- load all available gpf operators
# NB: not necessary anymore since version 5.0 (http://forum.step.esa.int/t/a-fatal-java-error-occurs-when-running-snappy-scripts/2093/10)
GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

# --- import hashmap
# NB: http://forum.step.esa.int/t/example-script-for-multiple-operations/2636/2
# HashMap is a dictionary. It maps parameter names (strings) to values (objects).
# It depends on the operator and parameter if a default value exists.
# Even if all parameters have a default value, an empty HashMap needs to be used.

# from snappy import jpy
# HashMap = jpy.get_type('java.util.HashMap')


# --- various
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


def read_product(*args, **kwargs):
    """Open product.

    Arguments:
    Accepts: .zip, .dim, unzipped .SAFE directory

    *args
        object from class fetchme.Product() with attribute 'path_and_file'

    **kwargs:
        path_and_file <string>

    Returns:
        product

    Examples:
        # EX1: case where argument a keyword argument
        import snapme as gpt
        p = gpt.read_product(path_and_file = '/path/file.zip')

        # EX2: case where argument is an object with attribute 'path_and_file'
        import fetchme
        import snapme as gpt
        obj = fetchme.Product()
        obj.path_and_file = '/home/sebastien/DATA/data_satellite/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE.zip'
        p = gpt.read_product(obj)

        # See native funtions available:
        p.<tab>
        EX: p.getName()

    """

    logging.info('reading product')

    # case if args agument passed
    if len(args) > 0 and hasattr(args[0], 'path_and_file'):
        pnf = args[0].path_and_file

    # case if keyword agument passed
    if len(kwargs) > 0 and 'path_and_file' in kwargs:
        pnf = kwargs['path_and_file']

    p = ProductIO.readProduct(pnf)

    return p


def write_product(obj, f_out=None, p_out=None, fmt_out=None):
    """Writes a product with the specified format to the given file.

    The method also writes all band data to the file. Therefore the band data must either
        be completely loaded (Band.rasterData is not null)
        or the product must be associated with a product reader (Product.productReader is not null) so that unloaded data can be reloaded.

    Available format: BEAM-DIMAP, GeoTIFF, GeoTIFF-BigTIFF, HDF5, ...
    """

    if f_out is None:
        f_out = get_name(obj)

    if p_out is None:
        p_out = '../data/'
    else:
        p_out = os.path.join(p_out, '')  # add trailing slash if missing (os independent)

    if fmt_out is None:
        ext = '.dim'
        fmt_out = 'BEAM-DIMAP'
    else:
        ext = ''

    logging.info('saving product')

    # print p_out + f_out + ext
    # print fmt_out
    # return
    ProductIO.writeProduct(obj, p_out + f_out + ext, fmt_out)


# def write(obj, f_out=None, p_out=None, fmt_out=None):
#     """Writes a data product to a file.
#         Source Options:
#         - source=<file>    The source product to be written.
#                      This is a mandatory source.

#         Parameter Options: (sh gpt -h Write)
#         - clearCacheAfterRowWrite=<boolean>    If true, the internal tile cache is cleared after a tile row has been written. Ignored if writeEntireTileRows=false.
#                                          Default value is 'false'.
#         - deleteOutputOnFailure=<boolean>      If true, all output files are deleted after a failed write operation.
#                                          Default value is 'true'.
#         - file=<file>                          The output file to which the data product is written.
#         - formatName=<string>                  The name of the output file format.
#                                          Default value is 'BEAM-DIMAP'.
#         - writeEntireTileRows=<boolean>        If true, the write operation waits until an entire tile row is computed.
#                                          Default value is 'true'.
#     """

#     if f_out is None:
#         f_out = get_name(obj)

#     if p_out is None:
#         p_out = '../data/'
#     else:
#         p_out = os.path.join(p_out, '')  # add trailing slash if missing (os independent)

#     if fmt_out is None:
#         ext = '.dim'
#         fmt_out = 'BEAM-DIMAP'
#     else:
#         ext = ''

#     logging.info('gpt operator = Write')

#     # --- set operator parameters and apply
#     parameters = HashMap()
#     result = GPF.createProduct('Write', parameters, obj)


def band_select(obj, sourceBands=None):
    """Creates a new product with only selected bands. (gpt -h BandSelect)

    Source Options:
        - source=<file>    Sets source 'source' to <filepath>.
                            This is a mandatory source.

    Parameter Options:
        - bandNamePattern=<string>                            Band name regular expression pattern
        - selectedPolarisations=<string,string,string,...>    The list of polarisations
        - sourceBands=<string,string,string,...>              The list of source bands.
    """

    logging.info('gpt operator = BandSelect')

    # --- check if source bands valid & format python list to string
    r, sourceBands_valid = is_bandinproduct(obj, sourceBands)
    sourceBands_valid_str = ','.join(sourceBands_valid)

    parameters = HashMap()
    parameters.put('sourceBands', sourceBands_valid_str)
    result = GPF.createProduct('BandSelect', parameters, obj)

    return result


def resample(obj, referenceBand=None):
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
    parameters.put('referenceBand', referenceBand)
    result = GPF.createProduct('Resample', parameters, obj)

    return result


def subset(obj, geoRegion=None, region=None, north_bound=None, west_bound=None, south_bound=None, east_bound=None):
    """GPT subset

    Optional input argument:

    Arguments:
        obj {[type]} -- [description]

    Keyword Arguments:
        geoRegion {string} -- [description] (default: {None})
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

    Examples:
        Get region of interest:
        NB: select region at http://boundingbox.klokantech.com/, and copy/paste output in desired format

        Using FGDC format (rectangle bounds)
            subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale
            p = gpt.subset(p, **subset_bounds)
        Using WTK format
            NB: select region at http://boundingbox.klokantech.com/, and copy/paste 'OGC WKT' format
            polygon_wkt = 'POLYGON((14.890766 37.417891, 15.117702 37.417891, 15.117702 37.289897, 14.890766 37.289897, 14.890766 37.417891))'
            p = gpt.subset(p, geoRegion=polygon_wkt)
    """

    logging.info('gpt operator = Subset')

    # --- set operator parameters (see: gpt -h operator)
    parameters = HashMap()
    parameters.put('copyMetadata', 'true')

    if geoRegion is not None:
        parameters.put('geoRegion', geoRegion)
    elif region is not None:
        parameters.put('region', region)
    elif all(v is not None for v in [north_bound, west_bound, south_bound, east_bound]):
        geoRegion = "POLYGON((" + str(west_bound) + ' ' + str(south_bound) + ', ' + str(east_bound) + ' ' + str(south_bound) + ', ' + str(east_bound) + ' ' + str(north_bound) + ', ' + str(west_bound) + ' ' + str(north_bound) + ', ' + str(west_bound) + ' ' + str(south_bound) + '))'
        parameters.put('geoRegion', geoRegion)

    # --- apply operator
    result = GPF.createProduct('Subset', parameters, obj)

    return result


def topsar_split(obj,
                 subswath='IW1',
                 polarisation='VV'):
    """
    Parameter Options: (gpt -h TOPSAR-Split)
    - firstBurstIndex=<integer>                         The first burst index
                                                            Valid interval is [1, *).
                                                            Default value is '1'.
    - lastBurstIndex=<integer>                          The last burst index
                                                            Valid interval is [1, *).
                                                        Default value is '9999'.
    - selectedPolarisations=<string,string,string,...>  The list of polarisations
    - subswath=<string>                                     The list of source bands.
    - wktAoi=<string>                                       WKT polygon to be used for selecting bursts
    """

    logging.info('gpt operator = TOPSAR-Split')

    parameters = HashMap()
    parameters.put('subswath', subswath)
    parameters.put('selectedPolarisations', polarisation)
    result = GPF.createProduct('TOPSAR-Split', parameters, obj)

    return result


def apply_orbit_file(obj,
                     continueOnFail=False,
                     orbitType='Sentinel Precise (Auto Download)',
                     polyDegree=3):
    """
    NB: aux files automatically downloaded are stored in ".snap/auxdata/Orbits/Sentinel-1/POEORB/S1A/"

    Parameter Options: (gpt -h Apply-Orbit-File)
        - continueOnFail=<boolean>    Sets parameter 'continueOnFail' to <boolean>.
                                        Default value is 'false'.
        - orbitType=<string>          Sets parameter 'orbitType' to <string>.
                                        Value must be one of 'Sentinel Precise (Auto Download)', 'Sentinel Restituted (Auto Download)', 'DORIS Preliminary POR (ENVISAT)', 'DORIS Precise VOR (ENVISAT) (Auto Download)', 'DELFT Precise (ENVISAT, ERS1&2) (Auto Download)', 'PRARE Precise (ERS1&2) (Auto Download)', 'Kompsat5 Precise'.
                                        Default value is 'Sentinel Precise (Auto Download)'.
        - polyDegree=<int>            Sets parameter 'polyDegree' to <int>.
                                        Default value is '3'.
    """

    logging.info('gpt operator = Apply-Orbit-File')

    parameters = HashMap()
    parameters.put('orbitType', orbitType)
    parameters.put('polyDegree', polyDegree)
    parameters.put('continueOnFail', continueOnFail)

    result = GPF.createProduct('Apply-Orbit-File', parameters, obj)

    return result


def back_geocoding(obj_master, obj_slave,
                   demName='SRTM 3Sec',
                   demResamplingMethod='BICUBIC_INTERPOLATION',
                   resamplingType='BISINC_5_POINT_INTERPOLATION',
                   disableReramp=False,
                   maskOutAreaWithoutElevation=True,
                   outputRangeAzimuthOffset=False,
                   outputDerampDemodPhase=False):
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
    parameters.put('demName', demName)
    parameters.put('demResamplingMethod', demResamplingMethod)
    parameters.put('resamplingType', resamplingType)
    parameters.put('disableReramp', disableReramp)
    parameters.put('maskOutAreaWithoutElevation', maskOutAreaWithoutElevation)
    parameters.put('outputRangeAzimuthOffset', outputRangeAzimuthOffset)
    parameters.put('outputDerampDemodPhase', outputDerampDemodPhase)

    # parameters.put("Digital Elevation Model", "SRTM 3Sec (Auto Download)")
    # parameters.put("DEM Resampling Method", "BICUBIC_INTERPOLATION")
    # parameters.put("Resampling Type", "BISINC_5_POINT_INTERPOLATION")
    # parameters.put("Mask out areas with no elevation", True)
    # parameters.put("Output Deramp and Demod Phase", False)

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


def interferogram(obj,
                  cohWinAz=10,
                  cohWinRg=10,
                  includeCoherence=True,
                  orbitDegree=3,
                  squarePixel=True,
                  srpNumberPoints=501,
                  srpPolynomialDegree=5,
                  subtractFlatEarthPhase=True):
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
    parameters.put('subtractFlatEarthPhase', subtractFlatEarthPhase)
    parameters.put('srpPolynomialDegree', srpPolynomialDegree)
    parameters.put('srpNumberPoints', srpNumberPoints)
    parameters.put('orbitDegree', orbitDegree)
    parameters.put('includeCoherence', includeCoherence)
    parameters.put('squarePixel', squarePixel)
    parameters.put('cohWinAz', cohWinAz)
    parameters.put('cohWinRg', cohWinRg)

    # parameters.put('Subtract flat-earth phase', True)
    # parameters.put('Degree of "Flat Earth" polynomial', 5)
    # parameters.put('Number of "Flat Earth" estimation points', 501)
    # parameters.put('Orbit interpolation degree', 3)
    # parameters.put('Include coherence estimation', True)
    # parameters.put('Square Pixel', False)
    # parameters.put('Independent Window Sizes', False)
    # parameters.put('Coherence Azimuth Window Size', 10)
    # parameters.put('Coherence Range Window Size', 10)

    result = GPF.createProduct('Interferogram', parameters, obj)

    return result


def topo_phase_removal(obj,
                       demName='SRTM 3Sec',
                       orbitDegree=3,
                       tileExtensionPercent='100',
                       topoPhaseBandName='topo_phase'):
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
        - topoPhaseBandName=<string>         The topographic phase band name.
                                                    Default value is 'topo_phase'.
    """

    logging.info('gpt operator = TopoPhaseRemoval')

    parameters = HashMap()
    parameters.put('demName', demName)
    parameters.put('orbitDegree', orbitDegree)
    parameters.put('tileExtensionPercent', tileExtensionPercent)
    parameters.put('topoPhaseBandName', topoPhaseBandName)  # root name for new created band

    result = GPF.createProduct('TopoPhaseRemoval', parameters, obj)

    # # - print created band
    # band_list = get_bandnames(result)
    # matching = [b for b in band_list if topoPhaseBandName in b]
    # logging.info('new band created: "' + matching[0] + '"')

    return result


def goldstein_phase_filtering(obj,
                              alpha=1.0,
                              coherenceThreshold=0.2,
                              FFTSizeString='64',
                              useCoherenceMask=False,
                              windowSizeString='3'):
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
    parameters.put('alpha', alpha)
    parameters.put('coherenceThreshold', coherenceThreshold)
    parameters.put('FFTSizeString', FFTSizeString)
    parameters.put('useCoherenceMask', useCoherenceMask)
    parameters.put('windowSizeString', windowSizeString)

    result = GPF.createProduct('GoldsteinPhaseFiltering', parameters, obj)

    return result


def terrain_correction(obj, sourceBands):
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

    # --- check if source bands valid
    r, sourceBands_valid = is_bandinproduct(obj, sourceBands)

    # --- format python list to string
    sourceBands_valid_str = ','.join(sourceBands_valid)

    parameters = HashMap()
    parameters.put('sourceBands', sourceBands_valid_str)
    result = GPF.createProduct('Terrain-Correction', parameters, obj)

    return result


def collocate(obj_master, obj_slave):
    """Collocates two products based on their geo-codings.

        Source Options:
          - master=<file>    The source product which serves as master.
                             This is a mandatory source.
          - slave=<file>     The source product which serves as slave.
                             This is a mandatory source.

        Parameter Options:
          - masterComponentPattern=<string>     The text pattern to be used when renaming master components.
                                                Default value is '${ORIGINAL_NAME}_M'.
          - renameMasterComponents=<boolean>    Whether or not components of the master product shall be renamed in the target product.
                                                Default value is 'true'.
          - renameSlaveComponents=<boolean>     Whether or not components of the slave product shall be renamed in the target product.
                                                Default value is 'true'.
          - resamplingType=<resamplingType>     The method to be used when resampling the slave grid onto the master grid.
                                                Default value is 'NEAREST_NEIGHBOUR'.
          - slaveComponentPattern=<string>      The text pattern to be used when renaming slave components.
                                                Default value is '${ORIGINAL_NAME}_S'.
          - targetProductType=<string>          The product type string for the target product (informal)
                                                Default value is 'COLLOCATED'.
    """

    logging.info('gpt operator = Collocate')

    parameters = HashMap()
    parameters.put('resamplingType', 'NEAREST_NEIGHBOUR')

    sources = HashMap()
    sources.put('master', obj_master)
    sources.put('slave', obj_slave)

    result = GPF.createProduct('Collocate', parameters, sources)

    return result


def speckle_filter(obj, sourceBands):
    """Speckle Reduction. (gpt -h Speckle-Filter)

        Parameter Options:
            - anSize=<int>                              The Adaptive Neighbourhood size
                                                            Valid interval is (1, 200].
                                                            Default value is '50'.
            - dampingFactor=<int>                       The damping factor (Frost filter only)
                                                            Valid interval is (0, 100].
                                                            Default value is '2'.
            - enl=<double>                              The number of looks
                                                            Valid interval is (0, *).
                                                            Default value is '1.0'.
            - estimateENL=<boolean>                     Sets parameter 'estimateENL' to <boolean>.
                                                            Default value is 'false'.
            - filter=<string>                           Sets parameter 'filter' to <string>.
                                                            Value must be one of 'None', 'Boxcar', 'Median', 'Frost', 'Gamma Map', 'Lee', 'Refined Lee', 'Lee Sigma', 'IDAN'.
                                                            Default value is 'Lee Sigma'.
            - filterSizeX=<int>                         The kernel x dimension
                                                            Valid interval is (1, 100].
                                                            Default value is '3'.
            - filterSizeY=<int>                         The kernel y dimension
                                                            Valid interval is (1, 100].
                                                            Default value is '3'.
            - numLooksStr=<string>                      Sets parameter 'numLooksStr' to <string>.
                                                            Value must be one of '1', '2', '3', '4'.
                                                            Default value is '1'.
            - sigmaStr=<string>                         Sets parameter 'sigmaStr' to <string>.
                                                            Value must be one of '0.5', '0.6', '0.7', '0.8', '0.9'.
                                                            Default value is '0.9'.
            - sourceBands=<string,string,string,...>    The list of source bands.
            - targetWindowSizeStr=<string>              Sets parameter 'targetWindowSizeStr' to <string>.
                                                            Value must be one of '3x3', '5x5'.
                                                            Default value is '3x3'.
            - windowSize=<string>                       Sets parameter 'windowSize' to <string>.
                                                            Value must be one of '5x5', '7x7', '9x9', '11x11', '13x13', '15x15', '17x17'.
                                                            Default value is '7x7'.
    """
    logging.info('gpt operator = Speckle-Filter')

    # --- check if source bands valid
    r, sourceBands_valid = is_bandinproduct(obj, sourceBands)

    # --- format python list to string
    sourceBands_valid_str = ','.join(sourceBands_valid)

    parameters = HashMap()
    parameters.put('sourceBands', sourceBands_valid_str)

    result = GPF.createProduct('Speckle-Filter', parameters, obj)

    return result


def polarimetric_matrix(obj, matrix='C2'):
    """Generates covariance or coherency matrix for given product.
    (gpt -h Polarimetric-Matrices)

    Source Options:
        - source=<file>    Sets source 'source' to <filepath>.
                                This is a mandatory source.

    Parameter Options:
        - matrix=<string>    The covariance or coherency matrix
                                Value must be one of 'C2', 'C3', 'C4', 'T3', 'T4'.
                                Default value is 'T3'.
    """

    logging.info('gpt operator = Polarimetric-Matrices')

    parameters = HashMap()
    parameters.put('matrix', matrix)

    result = GPF.createProduct('Polarimetric-Matrices', parameters, obj)

    return result


def merge(obj_master, obj_slave):
    """Allows merging of several source products by using specified 'master' as reference product.
    (gpt -h Merge)

    Source Options:
        - masterProduct=<file>      The master, which serves as the reference, e.g. providing the geo-information.
                                        This is a mandatory source.

    Parameter Options:
        - geographicError=<float>   Defines the maximum lat/lon error in degree between the products. If set to NaN no check for compatible geographic boundary is performed
                                        Default value is '1.0E-5f'.
    """

    # WARNING: if error "Product [sourceProducts] is not compatible to master product."
    # http://forum.step.esa.int/t/product-sourceproduct-is-not-compatible-to-master-product/1761/6

    logging.info('gpt operator = Merge')

    parameters = HashMap()

    sources = HashMap()
    sources.put('masterProduct', obj_master)
    sources.put('sourceProducts', obj_slave)

    result = GPF.createProduct('Merge', parameters, sources)

    return result


def band_maths(product, expression=None, targetband_name='band_new'):
    # marpet code: https://github.com/senbox-org/snap-engine/blob/master/snap-python/src/main/resources/snappy/examples/snappy_bmaths.py
    # == ./.snap/snap-python/snappy/examples/snappy_bmaths.py
    #
    # TODO: implement masking and export in kmz:
    # http://www.un-spider.org/advisory-support/recommended-practices/recommended-practice-flood-mapping/step-by-step
    #
    # WARNING: All the methods in the Band Maths are pixel-based. They don't compute values for a whole band.
    # http://forum.step.esa.int/t/mean-average-and-standard-deviation-band-math/1879

    from snappy import jpy

    width = product.getSceneRasterWidth()
    height = product.getSceneRasterHeight()
    name = product.getName()
    description = product.getDescription()
    band_names = product.getBandNames()

    print list(band_names)

    print("Product: %s, %d x %d pixels, %s" % (name, width, height, description))
    print("Bands:   %s" % (list(band_names)))

    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

    BandDescriptor = jpy.get_type('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor')

    targetBand1 = BandDescriptor()
    targetBand1.name = targetband_name
    targetBand1.type = 'float32'
    # targetBand1.expression = '(radiance_10 - radiance_7) / (radiance_10 + radiance_7)'
    targetBand1.expression = expression

    # targetBand2 = BandDescriptor()
    # targetBand2.name = 'band_2'
    # targetBand2.type = 'float32'
    # targetBand2.expression = '(radiance_9 - radiance_6) / (radiance_9 + radiance_6)'

    # targetBands = jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', 2)
    # targetBands[0] = targetBand1
    # targetBands[1] = targetBand2

    targetBands = jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', 1)
    targetBands[0] = targetBand1

    parameters = HashMap()
    parameters.put('targetBands', targetBands)

    result = GPF.createProduct('BandMaths', parameters, product)

    return result

    """
       Please note: the next major version of snappy/jpy will be more pythonic in the sense that implicit data type
       conversions are performed. The 'parameters' from above variable could then be given as a Python dict object:

        parameters = {
            'targetBands': [
                {
                    'name': 'band_1',
                    'type': 'float32',
                    'expression': '(radiance_10 - radiance_7) / (radiance_10 + radiance_7)'
                },
                {
                    'name': 'band_2',
                    'type': 'float32',
                    'expression': '(radiance_9 - radiance_6) / (radiance_9 + radiance_6)'
                }
            ]
        }
    """


####################################################################

def sar(cfg_productselection,
        cfg_sar,
        cfg_plot,
        store_result2db=None,
        print_sqlQuery=None,
        print_sqlResult=None,
        file_credentials_mysql=None):

    import fnmatch

    print('=== SAR PROCESSING')

    # --- get database credentials
    if file_credentials_mysql is None:
        file_credentials_mysql = './conf/credentials_mysql.txt'
    f = file(file_credentials_mysql)
    (db_usr, db_pwd) = f.readline().split(' ')

    # --- connect to database
    import utilityme as utils
    dbo = utils.Database(db_host='127.0.0.1', db_usr=db_usr, db_pwd=db_pwd, db_type='mysql')

    # --- add mission in cfg_productselection if not specified (NB: '%'=wild card)
    if 'mission' not in cfg_productselection:
        cfg_productselection['mission'] = 'SENTINEL-1%'

    # --- query archive with selected options
    stmt = dbo.dbmounts_archive_querystmt(**cfg_productselection)
    rows = dbo.execute_query(stmt)
    dat = rows.all()
    if print_sqlQuery is True:
        print(stmt)

    if print_sqlResult is True:
        print('--- Selected products (ordered by orbit direction / acqstarttime):')
        for k, r in enumerate(dat):
            print(str(k), r.title, r.orbitdirection)

    # === PROCESS
    subswath = cfg_sar['subswath']
    bands2plot = cfg_sar['bands2plot']
    subset_wkt = cfg_plot['subset_wkt']
    pathout_root = cfg_plot['pathout_root']
    thumbnail = cfg_plot['thumbnail']
    target_name = cfg_productselection['target_name']
    target_id = dbo.dbmounts_target_nameid(target_name=target_name)

    start_idx = 0
    for k, r in enumerate(dat, start=start_idx):

        # TODO: wtf, why r.title always start from idx = 0
        print str(k) + ' ' + r.title
        print str(k) + ' ' + dat[k].title

        # if (k < start_idx):  # or (k >= 2):
        #     continue

        print('  | ' + str(k) + ' - ' + dat[k].title)

        extra_optn = []
        p = read_product(path_and_file=dat[k].abspath)
        # p = topsar_split(p, subswath=subswath, polarisation=polarization)
        p = apply_orbit_file(p)
        p = deburst(p)
        # p = subset(p, geoRegion=subset_wkt) # NB: if subset geocode, image "cropped" then "orientated", leaving white spaces in jpg

        # TODO: adapt to query of which polarization to plot
        bdnames = get_bandnames(p, print_bands=None)
        bandname_int = fnmatch.filter(bdnames, 'Intensity_*')
        bandpolar = [i.split('Intensity_', 1)[1] for i in bandname_int]
        sourceBands = bandname_int  # add here bands to analyze

        p = terrain_correction(p, sourceBands)

        if 'speckle_filter' in cfg_sar:
            p = speckle_filter(p, sourceBands)
            extra_optn.append('spkle')

        # if 'polar' in cfg_sar:
        #     # --- get polarimetric covariance matrix
        #     s1_bis = read_product(path_and_file=s1_abspath)
        #     s1_bis = deburst(s1_bis)
        #     polmat = polarimetric_matrix(s1_bis)
        #     polmat = apply_orbit_file(polmat)
        #     gpt.get_bandnames(polmat, print_bands=1)
        #     polmat_bands = ['C11', 'C12_real', 'C12_imag', 'C22']
        #     polmat = gpt.terrain_correction(polmat, polmat_bands)
        #     polmat = gpt.subset(polmat, **subset_bounds)

        #     # --- compute C12_ampl (band math + merge)
        #     bandmath_expression = 'ampl(C12_real, C12_imag)'
        #     targetband_name = 'C12_ampl'
        #     p_new = gpt.band_maths(polmat, expression=bandmath_expression, targetband_name=targetband_name)
        #     polmat = gpt.merge(polmat, p_new)

        p = subset(p, geoRegion=subset_wkt)

        # --- set output file name based on metadata
        metadata = get_metadata_S1(p)
        f_out = []
        for pol in bandpolar:
            fnameout_band = '_'.join([metadata['acqstarttime_str'], subswath, pol, 'int'] + extra_optn)
            f_out.append(fnameout_band)

        # --- plot
        p_out = pathout_root + target_name + '/'
        imgs_fullpath = plotBands(p, band_name=sourceBands, f_out=f_out, p_out=p_out, thumbnail=thumbnail)
        # plotBands_np(p, band_name=sourceBands, f_out=f_out)

        # --- export
        # fmt_out = 'GeoTIFF'
        # f_out = '_'.join([metadata['acqstarttime_str'], subswath, polarization, 'int'])
        # write_product(p, f_out=f_out, fmt_out=fmt_out)

        # --- store image file to database
        if store_result2db is True:
            path_ln = ['data_mounts' + i.split('/data_mounts')[1] for i in imgs_fullpath]  # = abspath from data_mounts folder, linked to mountsweb static folder
            imgs_type = ['int_' + s for s in bandpolar]
            id_master = [str(dat[k].id)] * len(sourceBands)
            id_slave = id_master
            id_target = [str(target_id)] * len(sourceBands)

            print('Store to DB_MOUNTS.results_img')
            dict_val = {'title': f_out,
                        'abspath': path_ln,  # [path_ln + fnameout_ifg, path_ln + fnameout_coh],
                        'type': imgs_type,
                        'id_master': id_master,
                        'id_slave': id_slave,
                        'target_id': id_target}
            dbo.insert('DB_MOUNTS', 'results_img', dict_val)

        p.dispose()

        # print 'pausing'
        # import time
        # time.sleep(5)


def dinsar(cfg_productselection,
           cfg_dinsar,
           cfg_plot,
           store_result2db=None,
           print_sqlQuery=None,
           print_sqlResult=None,
           file_credentials_mysql=None):

    import fnmatch

    print('=== DINSAR PROCESSING')

    # --- get database credentials
    if file_credentials_mysql is None:
        file_credentials_mysql = './conf/credentials_mysql.txt'
    f = file(file_credentials_mysql)
    (db_usr, db_pwd) = f.readline().split(' ')

    # --- connect to database
    import utilityme as utils
    dbo = utils.Database(db_host='127.0.0.1', db_usr=db_usr, db_pwd=db_pwd, db_type='mysql')

    # --- add mission in cfg_productselection if not specified (NB: '%'=wild card)
    if 'mission' not in cfg_productselection:
        cfg_productselection['mission'] = 'SENTINEL-1%'

    # --- query archive with selected options
    stmt = dbo.dbmounts_archive_querystmt(**cfg_productselection)
    rows = dbo.execute_query(stmt)
    dat = rows.all()
    if print_sqlQuery is True:
        print(stmt)

    # --- create master slave pairs (msp)
    msp_ASC = [(x, y) for x, y in zip(dat[0::], dat[1::]) if x.orbitdirection == 'ASCENDING' and y.orbitdirection == 'ASCENDING']
    msp_DSC = [(x, y) for x, y in zip(dat[0::], dat[1::]) if x.orbitdirection == 'DESCENDING' and y.orbitdirection == 'DESCENDING']
    msp = msp_ASC + msp_DSC

    if print_sqlResult is True:
        print('--- Selected products (ordered by orbitdirection/acqstarttime):')
        for r in dat:
            print(r.title, r.orbitdirection)

        print('--- Created master/slave pairs:')
        for k, val in enumerate(msp):
            print('- pair ' + str(k + 1) + '/' + str(len(msp)))
            print '  master = ' + msp[k][0].title + ' ' + msp[k][0].orbitdirection
            print '  slave = ' + msp[k][1].title + ' ' + msp[k][1].orbitdirection

    # === PROCESS
    subswath = cfg_dinsar['subswath']
    polarization = cfg_dinsar['polarization']
    bands2plot = cfg_dinsar['bands2plot']
    subset_wkt = cfg_plot['subset_wkt']
    pathout_root = cfg_plot['pathout_root']
    thumbnail = cfg_plot['thumbnail']
    target_name = cfg_productselection['target_name']
    target_id = dbo.dbmounts_target_nameid(target_name=target_name)

    start_idx = 0
    for k, r in enumerate(msp, start=start_idx):

        master_title = msp[k][0].title
        slave_title = msp[k][1].title
        master_id = str(msp[k][0].id)
        slave_id = str(msp[k][1].id)
        print('---')
        print('- pair ' + str(k + 1) + '/' + str(len(msp)))
        print('MASTER = ' + master_title + ' (' + msp[k][0].orbitdirection + ')')
        print('SLAVE = ' + slave_title + ' (' + msp[k][1].orbitdirection + ')')

        # --- read master product
        master_abspath = msp[k][0].abspath
        m = read_product(path_and_file=master_abspath)

        # --- read slave product
        slave_abspath = msp[k][1].abspath
        s = read_product(path_and_file=slave_abspath)

        # --- split product
        m = topsar_split(m, subswath=subswath, polarisation=polarization)
        s = topsar_split(s, subswath=subswath, polarisation=polarization)

        # --- apply orbit file
        m = apply_orbit_file(m)
        s = apply_orbit_file(s)

        # -- subset giving pixel-coordinates to avoid DEM-assisted back-geocoding on full swath???
        # TODO: try!

        # --- back-geocoding (TOPS coregistration)
        p = back_geocoding(m, s)

        # --- interferogram
        p = interferogram(p)

        # --- deburst
        p = deburst(p)

        # --- topographic phase removal
        p = topo_phase_removal(p)

        # --- phase filtering
        p = goldstein_phase_filtering(p)

        # --- define bands to analyze
        # TODO: select based on 'bands2plot'
        bdnames = get_bandnames(p, print_bands=None)

        band_ifg = fnmatch.filter(bdnames, 'Phase_*')
        band_coh = fnmatch.filter(bdnames, 'coh_*')
        # band_int = fnmatch.filter(bdnames, 'Intensity_*') # >> combination of  slave/master intensity?
        sourceBands = [band_ifg[0], band_coh[0]]

        # --- terrain correction (geocoding)
        p = terrain_correction(p, sourceBands)

        # --- subset
        # p = subset(p, **subset_bounds)
        p = subset(p, geoRegion=subset_wkt)

        # --- set output file name based on metadata
        metadata_master = get_metadata_S1(m)
        metadata_slave = get_metadata_S1(s)
        fnameout_ifg = '_'.join([metadata_master['acqstarttime_str'], metadata_slave['acqstarttime_str'], subswath, polarization, 'ifg'])
        fnameout_coh = '_'.join([metadata_master['acqstarttime_str'], metadata_slave['acqstarttime_str'], subswath, polarization, 'coh'])

        # --- write result product
        save_product = 1
        if save_product:
            print('  writing product')
            fout_product = '_'.join([metadata_master['acqstarttime_str'], metadata_slave['acqstarttime_str'], subswath, polarization])
            write_product(p, f_out=fout_product, p_out='/home/sebastien/DATA/data_snap/')

        # --- plot
        # if bands2plot:
        #     # TODO: plot only what has been requested in list, e.g. ['ifg', 'coh', 'amp'] => when defining source bands

        #     p_out = pathout_root + target_name + '/'
        #     imgs_fullpath = plotBands(p, sourceBands, f_out=[fnameout_ifg, fnameout_coh], p_out=p_out, thumbnail=thumbnail)

        #     # --- store image file to database
        #     if store_result2db is True:
        #         path_ln = ['data_mounts' + i.split('/data_mounts')[1] for i in imgs_fullpath]  # = abspath from data_mounts folder, linked to mountsweb static folder

        #         print('Store to DB_MOUNTS.results_img')
        #         dict_val = {'title': [fnameout_ifg, fnameout_coh],
        #                     'abspath': path_ln,  # [path_ln + fnameout_ifg, path_ln + fnameout_coh],
        #                     'type': ['ifg', 'coh'],
        #                     'id_master': [master_id, master_id],
        #                     'id_slave': [slave_id, slave_id],
        #                     'target_id': [str(target_id), str(target_id)]}
        #         dbo.insert('DB_MOUNTS', 'results_img', dict_val)

        # # --- analyze
        # bands2analyze = ['coh']
        # if bands2analyze:
        #     print 'TODO'

            # --- dispose => Releases all of the resources used by this object instance and all of its owned children.
        print('Product dispose (release all resources used by object)')
        p.dispose()


def nir(cfg_productselection,
        cfg_nir,
        cfg_plot,
        store_result2db=None,
        print_sqlQuery=None,
        print_sqlResult=None,
        file_credentials_mysql=None):

    print('=== NIR PROCESSING')

    # --- get database credentials
    if file_credentials_mysql is None:
        file_credentials_mysql = './conf/credentials_mysql.txt'
    f = file(file_credentials_mysql)
    (db_usr, db_pwd) = f.readline().split(' ')

    # --- connect to database
    import utilityme as utils
    dbo = utils.Database(db_host='127.0.0.1', db_usr=db_usr, db_pwd=db_pwd, db_type='mysql')

    # --- add mission in cfg_productselection if not specified (NB: '%'=wild card)
    if 'mission' not in cfg_productselection:
        cfg_productselection['mission'] = 'SENTINEL-2%'

    # --- get processing parameters
    subset_wkt = cfg_plot['subset_wkt']
    pathout_root = cfg_plot['pathout_root']
    thumbnail = cfg_plot['thumbnail']
    bname_red = cfg_nir['bname_red']
    bname_green = cfg_nir['bname_green']
    bname_blue = cfg_nir['bname_blue']
    target_name = cfg_productselection['target_name']
    target_id = dbo.dbmounts_target_nameid(target_name=target_name)

    # --- query archive with selected options
    stmt = dbo.dbmounts_archive_querystmt(**cfg_productselection)
    rows = dbo.execute_query(stmt)
    dat = rows.all()
    if print_sqlQuery is True:
        print(stmt)

    for r in dat:
        print('  | ' + r.title)

        p = read_product(path_and_file=r.abspath)
        p = resample(p, referenceBand='B2')
        p = subset(p, geoRegion=subset_wkt)

        # --- write result product
        save_product = 0
        if save_product:
            print('  writing product')
            metadata_master = get_metadata_S2(p)
            fout_product = '_'.join(['S2', metadata_master['acqstarttime_str']])
            write_product(p, f_out=fout_product, p_out='/home/sebastien/DATA/data_snap/ertaale/')
            
        plot_nir = 0
        if plot_nir:
            f_out = r.acqstarttime_str + '_' + bname_red + bname_green + bname_blue + '_nir'
            p_out = pathout_root + target_name + '/'
            img_fullpath = plotBands_rgb(p, bname_red=bname_red, bname_green=bname_green, bname_blue=bname_blue, p_out=p_out, f_out=f_out, thumbnail=thumbnail)

            # --- store image file to database
            if store_result2db is True:
                path_ln = 'data_mounts' + img_fullpath.split('/data_mounts')[1]  # = abspath from data_mounts folder, linked to mountsweb static folder

                print('Store to DB_MOUNTS.results_img')
                dict_val = {'title': f_out,
                            'abspath': path_ln,
                            'type': 'nir',
                            'id_master': str(r.id),
                            'id_slave': str(r.id),
                            'target_id': str(target_id)}
                dbo.insert('DB_MOUNTS', 'results_img', dict_val)

        analyze_nir = 1
        if analyze_nir:

            R_rad = p.getBand(bname_red)
            width = R_rad.getRasterWidth()
            height = R_rad.getRasterHeight()
            R_rad_data = np.zeros(width * height, dtype=np.float32)
            R_rad.readPixels(0, 0, height, width, R_rad_data)
            R_rad_data.shape = width, height

            thresh = 0.7
            mask = np.where(R_rad_data > thresh, 1, 0)
            hot_nbpix = np.count_nonzero(mask)
            
            # tmp: when pb in image it returns the total nb of pixels in image... set to 0 instead
            #if hot_nbpix > 730000:
	      #hot_nbpix = 0

            # --- store data to database
            if store_result2db is True:
                id_image = 1  # TODO: link to plotted image or process

                from dateutil.parser import parse
                time_datetime = parse(r.acqstarttime_str).strftime('%Y-%m-%d %H:%M:%S.%f')

                print('Store to DB_MOUNTS.results_dat')
                dict_val = {'time': time_datetime,
                            'type': 'nir',
                            'data': str(hot_nbpix),
                            'id_image': str(id_image),
                            'target_id': str(target_id)}
                dbo.insert('DB_MOUNTS', 'results_dat', dict_val)


def graph_processing(config_file):
    """Runs snapme operators sequentially from yaml configuration file.

    Arguments:
        config_file {str} -- full path to yaml configuration file

    Example: read, split, applyOrbit, subset, and plot.
        The yaml file content should be structured as follows:

        optn_processing:
            - operator: read_product
              path_and_file: '/home/sebastien/DATA/data_satellite/ertaale/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE.zip'
            - operator: topsar_split
              subswath: IW2
            - operator: apply_orbit_file
            - operator: subset
              north_bound: 13.55
              west_bound: 40.64
              south_bound: 13.62
              east_bound: 40.715
            - operator: plotBand
              cmap: gist_rainbow
              band_name: Intensity_IW2_VV

        Operators are read sequentially. The operator name is the name of the funtion in snapme. The operator options are passed as keys, name/values identical to those used in snapme functions.
    """

    # --- read config file
    cfg = utils.read_configfile(config_file)

    p = []

    # --- loop through operators sequentially
    for i, operator in enumerate(cfg['optn_processing']):

        # --- get operator name and options
        operator_name = operator['operator']
        operator_options = dict(operator)
        del operator_options['operator']

        # --- call method (must be defined within this module)
        p = globals()[operator_name](p, **operator_options)

        if p is None:
            continue
        else:
            print('    | ' + str(p))
            bds = get_bandnames(p)
            print('    | ' + str(bds))


def get_rasterDim(obj, band_name):

    band = obj.getBand(band_name)
    w = band.getRasterWidth()
    h = band.getRasterHeight()

    return w, h


def print_rasterDim(obj, band_name):
    w, h = get_rasterDim(obj, band_name)
    print(str(w) + ' x ' + str(h))


def print_engineConfig():

    # NB: engine configuration set in file 'snap.properties'. Read in order of priority:
    # 1) user setting: $HOME/.snap/etc/snap.properties
    # 2) default setting: $SNAP/etc/snap.properties

    # http://forum.step.esa.int/t/basic-error-with-snappy-python-beginner/5220/5
    from snappy import EngineConfig
    print('tileCachSize', EngineConfig.instance().preferences().get('snap.jai.tileCacheSize', None))
    print('parallelism', EngineConfig.instance().preferences().get('snap.parallelism', None))


def get_bandnames(obj, print_bands=None):

    r = obj.getBandNames()
    r = list(r)

    if print_bands:
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


def plotBands_np(obj, band_name=None, cmap=None, f_out=None, p_out=None):
    """Plot band (or list of bands), using numpy."""

    if band_name is None:
        # get bands available in product if none specified
        band_name_valid = get_bandnames(obj)
    else:
        # check if band_name valid
        r, band_name_valid = is_bandinproduct(obj, band_name)

    for k, bname in enumerate(band_name_valid):

        logging.info('plotting band "' + bname + '"')

        # --- get band data
        band = obj.getBand(bname)

        if band is None:
            logging.info('warning: band "' + bname + '" is None')
            continue

        # --- initialize empty matrix
        w, h = get_rasterDim(obj, bname)
        band_data = np.zeros(w * h, np.float32)

        # --- set geocoding if it has been calculated already for another band of this product
        # see also: obj.transferGeoCodingTo = Transfers the geo-coding of this product instance to the destProduct with respect to the given subsetDef.
        if 'geocoding' in locals():
            logging.info('copying geocoding')
            band.setGeoCoding(geocoding)

        # --- read band pixels
        # NB: operation will be "Converting DEM to radar system for this tile."
        logging.info('reading band pixels')
        band.readPixels(0, 0, w, h, band_data)
        band_data.shape = h, w

        # --- get geocoding of the band (will be copied to other bands in product)
        if 'geocoding' not in locals():
            geocoding = band.getGeoCoding()

        # --- get colormap
        if type(cmap) is list and len(cmap) == len(band_name_valid):
            colormap = cmap[k]
        elif type(cmap) is list and len(cmap) == 1:
            colormap = cmap[0]
        else:
            colormap = None

        imgplot = plt.imshow(band_data, cmap=colormap)

        # --- save png
        if f_out is None:
            # # - set file name based on metadata
            # metadata_master = get_metadata_S1(obj)
            # metadata_slave = get_metadata_slave(obj, slave_idx=0)
            # fname_out = metadata_master['acqstarttime_str'] + '_' + metadata_slave['acqstarttime_str'] + '_' + '_'.join(bname.split('_')[0:3]) + '.png'
            fname_out = 'band_%s.png' % bname

        else:
            fname_out = f_out[k]

        if p_out is None:
            p_out = '../data/'
        imgplot.write_png(p_out + fname_out)

        # --- check output file size
        if os.path.getsize(p_out + fname_out) < 10000:
            print 'WARNING: file size <10KB, abnormal'
            # pdb.set_trace()

    # return band


def plotBands(obj, band_name=None, f_out=None, p_out=None, fmt_out=None, thumbnail=None):
    """Plot band (or list of bands), using jpy.

    https://github.com/senbox-org/snap-engine/blob/b8c9e5c1c657bb8c022bb41439ffd59ec019fcc4/snap-python/src/main/resources/snappy/examples/snappy_write_image.py

    Arguments:
        obj {[type]} -- [description]

    Keyword Arguments:
        band_name {[type]} -- [description] (default: {None})
        f_out {[type]} -- [description] (default: {None})
        p_out {[type]} -- [description] (default: {None})
    """

    from snappy import jpy
    JAI = jpy.get_type('javax.media.jai.JAI')
    ImageManager = jpy.get_type('org.esa.snap.core.image.ImageManager')

    # More Java type definitions required for image generation
    Color = jpy.get_type('java.awt.Color')
    ColorPoint = jpy.get_type('org.esa.snap.core.datamodel.ColorPaletteDef$Point')
    ColorPaletteDef = jpy.get_type('org.esa.snap.core.datamodel.ColorPaletteDef')
    ImageInfo = jpy.get_type('org.esa.snap.core.datamodel.ImageInfo')
    ImageLegend = jpy.get_type('org.esa.snap.core.datamodel.ImageLegend')
    # Disable JAI native MediaLib extensions
    System = jpy.get_type('java.lang.System')
    System.setProperty('com.sun.media.jai.disableMediaLib', 'true')

    if band_name is None:
        # get bands available in product if none specified
        band_name_valid = get_bandnames(obj)
    else:
        # check if band_name valid
        r, band_name_valid = is_bandinproduct(obj, band_name)

    imgs_fullpath = []
    for k, bname in enumerate(band_name_valid):

        logging.info('plotting band "' + bname + '"')

        # --- get band data
        band = obj.getBand(bname)

        if band is None:
            logging.info('warning: band "' + bname + '" is None')
            continue

        # --- set geocoding if it has been calculated already for another band of this product
        # see also: obj.transferGeoCodingTo = Transfers the geo-coding of this product instance to the destProduct with respect to the given subsetDef.
        if 'geocoding' in locals():
            logging.info('copying geocoding')
            band.setGeoCoding(geocoding)

        im = ImageManager.getInstance().createColoredBandImage([band], band.getImageInfo(), 0)

        # --- get geocoding of the band (will be copied to other bands in product)
        if 'geocoding' not in locals():
            geocoding = band.getGeoCoding()

        # --- save png
        if f_out is None:
            # # - set file name based on metadata
            # metadata_master = get_metadata_S1(obj)
            # metadata_slave = get_metadata_slave(obj, slave_idx=0)
            # fname_out = metadata_master['acqstarttime_str'] + '_' + metadata_slave['acqstarttime_str'] + '_' + '_'.join(bname.split('_')[0:3]) + '.png'
            fname_out = 'band_%s.png' % bname

        else:
            if isinstance(f_out, str):
                fname_out = f_out
            else:
                fname_out = f_out[k]

        if p_out is None:
            p_out = '../data/'
        else:
            p_out = os.path.join(p_out, '')  # add trailing slash if missing (os independent)

        if fmt_out is None:
            fmt_out = 'png'

        file_fullpath = p_out + fname_out + '.' + fmt_out
        imgs_fullpath.append(file_fullpath)
        JAI.create("filestore", im, file_fullpath, fmt_out)

        if thumbnail is True:
            file_fullpath_thumb = p_out + fname_out + '_thumb.' + fmt_out
            utils.create_thumbnail(file_fullpath, file_fullpath_thumb)

    return imgs_fullpath


def plotBands_rgb(self, bname_red='B4', bname_green='B3', bname_blue='B2', f_out=None, p_out=None, fmt_out=None, thumbnail=None):
    """Plot RGB bands in optical data (e.g., S2, Envisat), using jpy.

    https://github.com/senbox-org/snap-engine/blob/b8c9e5c1c657bb8c022bb41439ffd59ec019fcc4/snap-python/src/main/resources/snappy/examples/snappy_write_image.py

    Keyword Arguments:
        bname_red {str} -- [description] (default: {'B4'})
        bname_green {str} -- [description] (default: {'B3'})
        bname_blue {str} -- [description] (default: {'B2'})
        f_out {[type]} -- [description] (default: {None})
        p_out {[type]} -- [description] (default: {None})
    """

    from snappy import ProductUtils, ProgressMonitor
    from snappy import jpy
    JAI = jpy.get_type('javax.media.jai.JAI')
    ImageManager = jpy.get_type('org.esa.snap.core.image.ImageManager')

    red = self.getBand(bname_red)
    green = self.getBand(bname_green)
    blue = self.getBand(bname_blue)
    bands = [red, green, blue]

    image_info = ProductUtils.createImageInfo(bands, True, ProgressMonitor.NULL)
    im = ImageManager.getInstance().createColoredBandImage(bands, image_info, 0)

    # --- save png
    if f_out is None:
        f_out = self.getName() + '_' + bname_red + bname_green + bname_blue + '.png'
    if p_out is None:
        p_out = '../data/'
    else:
        p_out = os.path.join(p_out, '')  # add trailing slash if missing (os independent)

    if fmt_out is None:
        fmt_out = 'png'

    file_fullpath = p_out + f_out + '.' + fmt_out
    JAI.create("filestore", im, file_fullpath, fmt_out)

    if thumbnail is True:
        file_fullpath_thumb = p_out + f_out + '_thumb.' + fmt_out
        utils.create_thumbnail(file_fullpath, file_fullpath_thumb)

    return file_fullpath


def plotBands_rgb_np(self, bname_red='B4', bname_green='B3', bname_blue='B2', f_out=None, p_out=None):
    # Plot RGB bands in optical data (S2, Envisat), using numpy.
    # NB: see https://github.com/techforspace/sentinel
    #
    # RGB band names:
    #   - Sentinel 2 -> MSIL1C
    #       => bname_red='B4', bname_green='B3', bname_blue='B2'
    #   - ENVISAT -> MERIS (Imaging multi-spectral radiometers (vis/IR))
    #       =>  bname_red='radiance_4', bname_green='radiance_3', bname_blue='radiance_2'

    from skimage import exposure

    logging.info('reading RGB bands (' + bname_red + ', ' + bname_green + ', ' + bname_blue + ')')

    # !!! look at:
    # http://forum.step.esa.int/t/sentinel-2-product-to-rgb-png-patches-in-python-using-snappy/5354/2
    # https://github.com/senbox-org/snap-engine/blob/b8c9e5c1c657bb8c022bb41439ffd59ec019fcc4/snap-python/src/main/resources/snappy/examples/snappy_write_image.py

    # ===> original solution => correct RGB rendereing BUT different image size (black band on right hand side) then produced by plotBand ...
    #   => image is like shifted towards North, upermost is lost, width narrowed
    # --- get R,G,B bands
    B_rad = self.getBand(bname_blue)
    width = B_rad.getRasterWidth()
    height = B_rad.getRasterHeight()
    B_rad_data = np.zeros(width * height, dtype=np.float32)
    B_rad.readPixels(0, 0, height, width, B_rad_data)
    B_rad_data.shape = width, height

    G_rad = self.getBand(bname_green)
    G_rad_data = np.zeros(width * height, dtype=np.float32)
    G_rad.readPixels(0, 0, height, width, G_rad_data)
    G_rad_data.shape = width, height

    R_rad = self.getBand(bname_red)
    R_rad_data = np.zeros(width * height, dtype=np.float32)
    R_rad.readPixels(0, 0, height, width, R_rad_data)
    R_rad_data.shape = width, height

    # --- contrast enhancement
    # => rescale pixel intensities for each channel according to a low and a high saturation thresholds (expressed as % of the total number of pixels)
    cube = np.zeros((width, height, 3), dtype=np.float32)
    print(cube.shape)

    saturation_threshold = 1
    if saturation_threshold is not None:
        logging.info('applying contrast enhancement')

        val1, val2 = np.percentile(B_rad_data, (4, 95))
        sat_B_rad_data = exposure.rescale_intensity(B_rad_data, in_range=(val1, val2))

        val1, val2 = np.percentile(G_rad_data, (4, 95))
        sat_G_rad_data = exposure.rescale_intensity(G_rad_data, in_range=(val1, val2))

        val1, val2 = np.percentile(R_rad_data, (4, 95))
        sat_R_rad_data = exposure.rescale_intensity(R_rad_data, in_range=(val1, val2))

        cube[:, :, 0] = sat_R_rad_data
        cube[:, :, 1] = sat_G_rad_data
        cube[:, :, 2] = sat_B_rad_data

    else:
        cube[:, :, 0] = R_rad_data
        cube[:, :, 1] = G_rad_data
        cube[:, :, 2] = B_rad_data

    # ===> solution identical to plotBand => identical image size BUT uncorrect RGB rendereing ...
    # # --- get R,G,B bands
    # B_rad = self.getBand(bname_blue)
    # width = B_rad.getRasterWidth()
    # height = B_rad.getRasterHeight()
    # B_rad_data = np.zeros(width * height, np.float32)
    # B_rad.readPixels(0, 0, width, height, B_rad_data)
    # B_rad_data.shape = height, width

    # G_rad = self.getBand(bname_green)
    # G_rad_data = np.zeros(width * height, np.float32)
    # G_rad.readPixels(0, 0, width, height, B_rad_data)
    # G_rad_data.shape = height, width

    # R_rad = self.getBand(bname_red)
    # R_rad_data = np.zeros(width * height, np.float32)
    # R_rad.readPixels(0, 0, width, height, B_rad_data)
    # R_rad_data.shape = height, width

    # # --- contrast enhancement
    # # => rescale pixel intensities for each channel according to a low and a high saturation thresholds (expressed as % of the total number of pixels)
    # cube = np.zeros((height, width, 3), dtype=np.float32)
    # print(cube.shape)
    #
    # saturation_threshold = None
    # if saturation_threshold is not None:
    #     logging.info('applying contrast enhancement')

    #     val1, val2 = np.percentile(B_rad_data, (4, 95))
    #     sat_B_rad_data = exposure.rescale_intensity(B_rad_data, in_range=(val1, val2))

    #     val1, val2 = np.percentile(G_rad_data, (4, 95))
    #     sat_G_rad_data = exposure.rescale_intensity(G_rad_data, in_range=(val1, val2))

    #     val1, val2 = np.percentile(R_rad_data, (4, 95))
    #     sat_R_rad_data = exposure.rescale_intensity(R_rad_data, in_range=(val1, val2))

    #     cube[:, :, 0] = sat_R_rad_data
    #     cube[:, :, 1] = sat_G_rad_data
    #     cube[:, :, 2] = sat_B_rad_data

    # else:
    #     cube[:, :, 0] = R_rad_data
    #     cube[:, :, 1] = G_rad_data
    #     cube[:, :, 2] = B_rad_data

    fig = plt.imshow(cube)

    # --- save png
    if f_out is None:
        f_out = self.getName() + '_RGB.png'
    if p_out is None:
        p_out = '../data/'

    fig.write_png(p_out + f_out)


def is_bandinproduct(obj, band_name):
    """Check if band name is in product.

    Arguments:
        obj       <product obj>
        band_name <string, or list of string>

    Returns:
        is_band = list of 0 1 values whether band name in product (1) or not (0)
        band_name_valid = list of queried bands with only valid names
    """

    # --- check input
    if isinstance(band_name, str):
        band_name = [band_name]

    # --- get bands available in product
    product_bands = get_bandnames(obj)

    is_band = []
    band_name_valid = []

    # --- loop through bands to check
    for bname in band_name:
        if bname in product_bands:
            is_band.append(1)
            band_name_valid.append(bname)
        else:
            logging.info('warning: band "' + bname + '" not in product')
            is_band.append(0)

    return is_band, band_name_valid


def get_metadata_S1(self):
    '''Get metadata for S1 products (Abstracted_Metadata node).'''

    # --- get list of metadata categories
    # print list(self.getMetadataRoot().getElementNames())

    # --- get list of attributes (in this node):
    # print list(self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeNames())

    # --- get list of other nodes
    # print list(self.getMetadataRoot().getElement('Abstracted_Metadata').getElementNames())

    product_title = self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeString('PRODUCT')
    product_type = self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeString('PRODUCT_TYPE')
    mission = self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeString('MISSION')
    acquisition_mode = self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeString('ACQUISITION_MODE')
    acqstart_str = self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeString('first_line_time')
    orbit_relativenb = self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeString('REL_ORBIT')
    orbit_direction = self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeString('PASS')
    polarization_1 = self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeString('mds1_tx_rx_polar')
    polarization_2 = self.getMetadataRoot().getElement('Abstracted_Metadata').getAttributeString('mds2_tx_rx_polar')
    if polarization_2 == '-':
        polarization = polarization_1
    else:
        polarization = ' '.join([polarization_1, polarization_2])

    from dateutil.parser import parse
    acqstart_datetime = parse(acqstart_str).strftime('%Y-%m-%d %H:%M:%S.%f')
    acqstart_iso = parse(acqstart_str).strftime('%Y%m%dT%H%M%S')

    # NB: I'm not using datetime because of abbreviated month format
    # datetime.datetime.strptime(date_string, format1).strftime(format2)

    # NB: keys should be identical to columns of DB_MOUNTS.archive table
    metadata = {'title': product_title,
                'producttype': product_type,
                'mission': mission,
                'acquisitionmode': acquisition_mode,
                'acqstarttime': acqstart_datetime,
                'acqstarttime_str': acqstart_iso,
                'relativeorbitnumber': orbit_relativenb,
                'orbitdirection': orbit_direction,
                'polarization': polarization}

    return metadata


def get_metadata_S1_slave(self, slave_idx=0):
    # --- get list of metadata categories
    # print list(self.getMetadataRoot().getElementNames())

    # --- get list of attributes (in this node):
    # print list(self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeNames())

    # --- get list of other nodes
    # print list(self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getElementNames())

    product_title = self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeString('PRODUCT')
    product_type = self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeString('PRODUCT_TYPE')
    mission = self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeString('MISSION')
    acquisition_mode = self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeString('ACQUISITION_MODE')
    acqstart_str = self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeString('first_line_time')
    orbit_relativenb = self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeString('REL_ORBIT')
    orbit_direction = self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeString('PASS')
    polarization_1 = self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeString('mds1_tx_rx_polar')
    polarization_2 = self.getMetadataRoot().getElement('Slave_Metadata').getElementAt(slave_idx).getAttributeString('mds2_tx_rx_polar')
    polarization = ' '.join([polarization_1, polarization_2])

    from dateutil.parser import parse
    acqstart_datetime = parse(acqstart_str).strftime('%Y-%m-%d %H:%M:%S.%f')
    acqstart_iso = parse(acqstart_str).strftime('%Y%m%dT%H%M%S')

    # NB: I'm not using datetime because of abbreviated month format
    # datetime.datetime.strptime(date_string, format1).strftime(format2)

    # NB: keys should be identical to columns of DB_ARCHIVE's tables
    metadata_slave = {'title': product_title,
                      'producttype': product_type,
                      'mission': mission,
                      'acquisitionmode': acquisition_mode,
                      'acqstarttime': acqstart_datetime,
                      'acqstarttime_str': acqstart_iso,
                      'relativeorbitnumber': orbit_relativenb,
                      'orbitdirection': orbit_direction,
                      'polarization': polarization}

    return metadata_slave


def metadata_naming_convention():
    metadata_names = []

    # --- metadata providers
    metadata_names.append(['opensearch', 'Abstracted_Metadata', 'Original_Product_Metadata', 'example'])

    # --- metadata naming convention
    metadata_names.append(['title', 'PRODUCT', '', 'S1A_IW_SL1__1_DV_20170101T165556_20170101T165629_014641_017CE5_D905'])
    metadata_names.append(['filename', '', '', 'S1A_IW_SL1__1_DV_20170101T165556_20170101T165629_014641_017CE5_D905.SAFE'])
    metadata_names.append(['beginposition', 'first_line_time', 'acquisitionPeriod/startTime', '01-JAN-2017 16:55:59.196569 | 2017-01-01T16:55:59.196Z'])
    metadata_names.append(['endposition', 'last_line_time', 'acquisitionPeriod/stopTime', '01-JAN-2017 16:56:26.144913 | 2017-01-01T16:56:26.144Z'])
    metadata_names.append(['ingestiondate', '', '', '2017-01-01T20:44:29.821Z'])
    metadata_names.append(['producttype', 'PRODUCT_TYPE', 'standAloneProductInformation/productType', 'SLC'])
    metadata_names.append(['', 'MISSION', '', 'SENTINEL-1A'])
    metadata_names.append(['platformname', '', 'platform/familyName', 'Sentinel-1'])
    metadata_names.append(['', '', 'platform/number', 'A'])
    metadata_names.append(['platformidentifier', '', 'platform/nssdcIdentifier', '2014-016A'])
    metadata_names.append(['sensoroperationalmode', 'ACQUISITION_MODE', '', 'IW'])
    metadata_names.append(['orbitnumber', 'ABS_ORBIT', 'orbitNumber', '14'])
    metadata_names.append(['relativeorbitnumber', 'REL_ORBIT', 'relativeOrbitNumber', '14'])
    metadata_names.append(['orbitdirection', 'PASS', 'pass', 'ASCENDING'])
    metadata_names.append(['polarisationmode', '', '', 'VV VH'])
    metadata_names.append(['', 'mds1_tx_rx_polar', '', 'VV'])
    metadata_names.append(['', 'mds2_tx_rx_polar', '', 'VH'])


def get_metadata_S2(self):
    # This is intended to extract the main metadata of the product, similar to those obtained for s1 products with "get_metadata_S1"
    # Although a field "Abstracted_Metadata" appears in SNAP Desktop Metadata section for S2 products, it cannot be accessed by command line
    # Below is a way to gather them...

    product_title = self.getMetadataRoot().getElement('Level-1C_User_Product').getElement('General_Info').getElement('Product_Info').getAttributeString('PRODUCT_URI')
    product_title = product_title.split('.SAFE')[0]

    product_type = self.getMetadataRoot().getElement('Level-1C_User_Product').getElement('General_Info').getElement('Product_Info').getAttributeString('PRODUCT_TYPE')

    mission = self.getMetadataRoot().getElement('Level-1C_User_Product').getElement('General_Info').getElement('Product_Info').getElement('Datatake').getAttributeString('SPACECRAFT_NAME')
    mission = mission.upper()  # >> 'Sentinel-2A' to 'SENTINEL-2A'

    acquisition_mode = '-'

    acqstart_str = self.getMetadataRoot().getElement('Level-1C_DataStrip_ID').getElement('General_Info').getElement('Datastrip_Time_Info').getAttributeString('DATASTRIP_SENSING_START')

    orbit_relativenb = self.getMetadataRoot().getElement('Level-1C_User_Product').getElement('General_Info').getElement('Product_Info').getElement('Datatake').getAttributeString('SENSING_ORBIT_NUMBER')
    orbit_direction = self.getMetadataRoot().getElement('Level-1C_User_Product').getElement('General_Info').getElement('Product_Info').getElement('Datatake').getAttributeString('SENSING_ORBIT_DIRECTION')

    polarization = '-'

    from dateutil.parser import parse
    acqstart_datetime = parse(acqstart_str).strftime('%Y-%m-%d %H:%M:%S.%f')
    acqstart_iso = parse(acqstart_str).strftime('%Y%m%dT%H%M%S')

    # NB: I'm not using datetime because of abbreviated month format
    # datetime.datetime.strptime(date_string, format1).strftime(format2)

    # NB: keys should be identical to columns of DB_MOUNTS.archive table
    metadata = {'title': product_title,
                'producttype': product_type,
                'mission': mission,
                'acquisitionmode': acquisition_mode,
                'acqstarttime': acqstart_datetime,
                'acqstarttime_str': acqstart_iso,
                'relativeorbitnumber': orbit_relativenb,
                'orbitdirection': orbit_direction,
                'polarization': polarization}

    return metadata
