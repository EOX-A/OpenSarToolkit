import datetime
import logging
from shapely.wkt import loads


logger = logging.getLogger(__name__)


def create_s1_product_specs(product_type='*', polarisation='*', beam='*'):
    '''A helper function to create a scihub API compliant product specs string

    Args:
        product_type (str): the product type to look at
        polarisation (str):

    Returns:
        str: Copernicus' scihub compliant product specs string

    Notes:
        Default values for all product specifications is the *-character,
        meaning to look for all kinds of data by default.

    '''

    # bring product type, polarisation and beam to query format
    if product_type == 'GRD':
        product_type = 'GRD_HD,GRD_MD,GRD_MS,GRD_HS'
    if polarisation == 'VV,VH':
        polarisation = 'VV%2BVH'
    if polarisation == 'HH,HV':
        polarisation = 'HH%2BHV'
    if polarisation == '*':
        polarisation = 'VV%2BVH'
    product_type = "processinglevel={}".format(product_type)
    polarisation = "polarization={}".format(polarisation)
    beam = "beamSwath={}".format(beam)

    return '{}&{}&{}'.format(
        product_type, polarisation, beam
    )


def create_query(aoi, toi, product_specs):
    '''A helper function to create a scihub API compliant query

    Args:
        satellite (str): the satellite (e.g. Sentinel-1)
        aoi (str): a Copernicus scihub compliant AOI string
        toi (str): a Copernicus scihub compliant TOI string
        product_specs (str): a Copernicus scihub compliant product specs string

    Returns:
        str: Copernicus' scihub compliant query string (i.e. OpenSearch query)
             formattted with urllib

    '''
    # construct the final query
    satellite = 'platform=SENTINEL-1'
    aoi = aoi.replace(' ', '%20')
    output = 'output=jsonlite'
    query = '{}&{}&{}&{}&{}'.format(satellite, product_specs, aoi, toi, output)

    return query


def create_aoi_str(aoi_wkt):
    '''A helper function to create a scihub API compliant AOI string

    Args:
        aoi (str): is WKT representation of the Area Of Interest

    Returns:
        str: Copernicus' scihub compliant AOI string

    '''

    geom = loads(aoi_wkt)
    if geom.geom_type == 'Point':
        aoi_str = "intersectsWith=({}, {})".format(geom.y, geom.x)

    else:
        # simplify geometry
        aoi_convex = geom.convex_hull

        # create scihub-confrom aoi string
        aoi_str = 'intersectsWith={}'.format(aoi_convex)

    return aoi_str


def create_toi_str(start='2014-10-01',
                   end=datetime.datetime.now().strftime("%Y-%m-%d")
                   ):
    '''A helper function to create a scihub API compliant TOI string

    Args:
        start (str): the start date of the Time Of Interest represented by
                     a string of a YYYY-MM-DD format string
        end (str): the end date of the Time Of Interest represented by
                   a string of a YYYY-MM-DD format string

    Returns:
        str: Copernicus' scihub compliant TOI string

    '''

    # bring start and end date to query format
    start = '{}T00:00:01Z'.format(start)
    end = '{}T23:59:00Z'.format(end)
    toi = ('start={}&end={}'.format(start, end,))
    return toi