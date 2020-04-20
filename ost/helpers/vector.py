import os
import math
from os.path import join as opj
import numpy as np
import sys
import json
import glob
from functools import partial
import rasterio
import numpy.ma as ma
from affine import Affine

import osr
import ogr
import geopandas as gpd
import logging

from shapely.ops import transform
from shapely.wkt import loads
from shapely.geometry import Point, Polygon, mapping, shape
from fiona import collection
from fiona.crs import from_epsg

from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


def ls_to_vector(infile, out_path=None, driver="GPKG", buffer=2):
    prefix = glob.glob(os.path.abspath(infile[:-4]) + '*data')[0]
    if out_path is None:
        out_path = prefix.replace('.data', '.gpkg')
    if len(glob.glob(opj(prefix, '*layover_shadow*.img'))) == 1:
        ls_mask = glob.glob(opj(prefix, '*layover_shadow*.img'))[0]
    else:
        ls_mask = None

    if not ls_mask:
        return None

    features_gdf = gpd.GeoDataFrame()
    features_gdf['geometry'] = None

    with rasterio.open(ls_mask) as src:
        ls_arr = ma.masked_array(
            data=np.expand_dims(
                buffer_array(np.where(src.read(1) > 0, 1, 0), buffer=buffer), axis=0
            ).astype(np.uint8, copy=False),
            mask=np.expand_dims(
                buffer_array(np.where(src.read(1) > 0, 1, 0), buffer=buffer), axis=0
            ).astype(np.bool, copy=False)
        )
        shapes = rasterio.features.shapes(ls_arr.data,
                                          connectivity=4,
                                          mask=ls_arr.mask,
                                          transform=src.transform
                                          )
    geom = [shape(i) for i, v in shapes]
    features_gdf = gpd.GeoDataFrame({'geometry': geom})
    if features_gdf.empty:
        return None
    features_gdf.to_file(out_path, driver=driver)
    h.delete_dimap(dimap_prefix=prefix.replace('.data', ''))
    return out_path


def buffer_array(arr, buffer=0):
    """
    Buffer True values of array.

    Parameters
    ----------
    arr : np.ndarray
        Bool array.
    buffer : int
        Buffer in pixels around masked patches.

    Returns
    -------
    np.ndarray
    """
    if not isinstance(arr, np.ndarray):
        raise TypeError("not a NumPy array")
    elif arr.ndim != 2:
        raise TypeError("array not 2-dimensional")
    elif arr.dtype != np.bool:
        arr = arr.astype(np.bool)

    if buffer == 0 or not arr.any():
        return arr.astype(np.bool, copy=False)
    else:
        return rasterio.features.geometry_mask(
            (
                shape(p).buffer(buffer)
                for p, v in
            rasterio.features.shapes(arr.astype(np.uint8, copy=False), mask=arr)
                if v
            ),
            arr.shape,
            Affine(1.0, 0.0, 0.0, 0.0, 1.0, 0.0),
            invert=True
        ).astype(np.bool, copy=False)


def get_epsg(prjfile):
    '''Get the epsg code from a projection file of a shapefile

    Args:
        prjfile: a .prj file of a shapefile

    Returns:
        str: EPSG code

    '''

    prj_file = open(prjfile, 'r')
    prj_txt = prj_file.read()
    srs = osr.SpatialReference()
    srs.ImportFromESRI([prj_txt])
    srs.AutoIdentifyEPSG()
    # return EPSG code
    return srs.GetAuthorityCode(None)


def get_proj4(prjfile):
    '''Get the proj4 string from a projection file of a shapefile

    Args:
        prjfile: a .prj file of a shapefile

    Returns:
        str: PROJ4 code

    '''

    prj_file = open(prjfile, 'r')
    prj_string = prj_file.read()

    # Lambert error
    if '\"Lambert_Conformal_Conic\"' in prj_string:

        print(' ERROR: It seems you used an ESRI generated shapefile'
              ' with Lambert Conformal Conic projection. ')
        print(' This one is not compatible with Open Standard OGR/GDAL'
              ' tools used here. ')
        print(' Reproject your shapefile to a standard Lat/Long projection'
              ' and try again')
        exit(1)

    srs = osr.SpatialReference()
    srs.ImportFromESRI([prj_string])
    return srs.ExportToProj4()


def epsg_to_wkt_projection(epsg_code):
    
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(epsg_code)  
            
    return spatial_ref.ExpotToWkt()


def reproject_geometry(geom, inproj4, out_epsg):
    '''Reproject a wkt geometry based on EPSG code

    Args:
        geom (ogr-geom): an ogr geom objecct
        inproj4 (str): a proj4 string
        out_epsg (str): the EPSG code to which the geometry should transformed

    Returns
        geom (ogr-geometry object): the transformed geometry

    '''

    geom = ogr.CreateGeometryFromWkt(geom)
    # input SpatialReference
    spatial_ref_in = osr.SpatialReference()
    spatial_ref_in.ImportFromProj4(inproj4)

    # output SpatialReference
    spatial_ref_out = osr.SpatialReference()
    spatial_ref_out.ImportFromEPSG(int(out_epsg))

    # create the CoordinateTransformation
    coord_transform = osr.CoordinateTransformation(spatial_ref_in,
                                                   spatial_ref_out)
    try:
        geom.Transform(coord_transform)
    except:
        print(' ERROR: Not able to transform the geometry')
        sys.exit()

    return geom


def geodesic_point_buffer(lat, lon, meters, envelope=False):
    round_area = Point(lon, lat).buffer(meters_to_degrees(latitude=lat, meters=meters))
    if envelope is True:
        geom = round_area.envelope
    else:
        geom = round_area
    return geom.wkt


def meters_to_degrees(latitude, meters):
    '''Convert resolution in meters to degree based on Latitude

    '''
    earth_radius = 6378137
    degrees_to_radians = math.pi/180.0
    radians_to_degrees = 180.0/math.pi
    # "Given a latitude and a distance west, return the change in longitude."
    # Find the radius of a circle around the earth at given latitude.
    if isinstance(latitude, list):
        latitude = latitude[1]
    if latitude > 90 or latitude < -90:
        raise ValueError
    r = earth_radius*math.cos(latitude*degrees_to_radians)
    return (meters/r)*radians_to_degrees


def latlon_to_wkt(lat, lon, buffer_degree=None, buffer_meter=None, envelope=False):
    '''A helper function to create a WKT representation of Lat/Lon pair

    This function takes lat and lon vale and returns the WKT Point
    representation by default.

    A buffer can be set in metres, which returns a WKT POLYGON. If envelope
    is set to True, the buffer will be squared by the extent buffer radius.

    Args:
        lat (str): Latitude (deg) of a point
        lon (str): Longitude (deg) of a point
        buffer (float): optional buffer around the point
        envelope (bool): gives a square instead of a circular buffer
                         (only applies if bufferis set)

    Returns:
        wkt (str): WKT string

    '''

    if buffer_degree is None and buffer_meter is None:
        aoi_wkt = 'POINT ({} {})'.format(lon, lat)

    elif buffer_degree:
        aoi_geom = loads('POINT ({} {})'.format(lon, lat)).buffer(buffer_degree)
        if envelope:
            aoi_geom = aoi_geom.envelope

        aoi_wkt = aoi_geom.to_wkt()

    elif buffer_meter:
        aoi_wkt = geodesic_point_buffer(lat, lon, buffer_meter, envelope)

    return aoi_wkt


def wkt_manipulations(wkt, buffer=None, convex=False, envelope=False):

    geom = ogr.CreateGeometryFromWkt(wkt)

    if buffer:
        geom = geom.Buffer(buffer)

    if convex:
        geom = geom.ConvexHull()

    if envelope:
        geom = geom.GetEnvelope()
        geom = ogr.CreateGeometryFromWkt(
            'POLYGON (({} {}, {} {}, {} {}, {} {}, {} {}, {} {}))'.format(
                geom[1], geom[3], geom[0], geom[3], geom[0], geom[2],
                geom[1], geom[2], geom[1], geom[3], geom[1], geom[3]))

    return geom.ExportToWkt()


def shp_to_wkt(shapefile, buffer=None, convex=False, envelope=False):
    '''A helper function to translate a shapefile into WKT


    '''

    # get filepaths and proj4 string
    shpfile = os.path.abspath(shapefile)
    prjfile = shpfile[:-4] + '.prj'
    proj4 = get_proj4(prjfile)

    lyr_name = os.path.basename(shapefile)[:-4]
    shp = ogr.Open(os.path.abspath(shapefile))
    lyr = shp.GetLayerByName(lyr_name)
    geom = ogr.Geometry(ogr.wkbGeometryCollection)

    for feat in lyr:
        geom.AddGeometry(feat.GetGeometryRef())
        wkt = geom.ExportToWkt()

    if proj4 != '+proj=longlat +datum=WGS84 +no_defs':
        logger.info('Reprojecting AOI file to Lat/Long (WGS84)')
        wkt = reproject_geometry(wkt, proj4, 4326).ExportToWkt()

    # do manipulations if needed
    wkt = wkt_manipulations(wkt, buffer=buffer, convex=convex,
                            envelope=envelope)

    return wkt


def kml_to_wkt(kmlfile):

    shp = ogr.Open(os.path.abspath(kmlfile))
    lyr = shp.GetLayerByName()
    for feat in lyr:
        geom = feat.GetGeometryRef()
    wkt = str(geom)

    return wkt


def latlon_to_shp(lon, lat, shapefile):

    shapefile = str(shapefile)

    schema = {'geometry': 'Point',
              'properties': {'id': 'str'}}

    wkt = loads('POINT ({} {})'.format(lon, lat))

    with collection(shapefile, "w",
                    crs=from_epsg(4326),
                    driver="ESRI Shapefile",
                    schema=schema) as output:

        output.write({'geometry': mapping(wkt),
                      'properties': {'id': '1'}})


def shp_to_gdf(shapefile):

    gdf = gpd.GeoDataFrame.from_file(shapefile)

    prjfile = shapefile[:-4] + '.prj'
    proj4 = get_proj4(prjfile)

    if proj4 != '+proj=longlat +datum=WGS84 +no_defs':
        logger.info('reprojecting AOI layer to WGS84.')
        # reproject
        gdf.crs = (proj4)
        gdf = gdf.to_crs({'init': 'epsg:4326'})

    return gdf


def wkt_to_gdf(wkt):

    # load wkt
    geometry = loads(wkt)

    # point wkt
    if geometry.geom_type == 'Point':
        data = {'id': ['1'],
                'geometry': loads(wkt).buffer(0.05).envelope}
        gdf = gpd.GeoDataFrame(data)
    
    # polygon wkt
    elif geometry.geom_type == 'Polygon':
        data = {'id': ['1'],
                'geometry': loads(wkt)}
        gdf = gpd.GeoDataFrame(
            data, crs = {'init': 'epsg:4326',  'no_defs': True}
        )

    # geometry collection of single multiploygon
    elif (
            geometry.geom_type == 'GeometryCollection' and
            len(geometry) == 1 and 'MULTIPOLYGON' in str(geometry)
    ):

        data = {'id': ['1'],
                'geometry': geometry}
        gdf = gpd.GeoDataFrame(data, crs = {'init': 'epsg:4326',  'no_defs': True})
        
        ids, feats =[], []
        for i, feat in enumerate(gdf.geometry.values[0]):
            ids.append(i)
            feats.append(feat)

        gdf = gpd.GeoDataFrame({'id': ids,
                                'geometry': feats}, 
                                 geometry='geometry', 
                                 crs = gdf.crs
        )
    
    # geometry collection of single polygon
    elif geometry.geom_type == 'GeometryCollection' and len(geometry) == 1:
        
        data = {'id': ['1'],
                'geometry': geometry}
        gdf = gpd.GeoDataFrame(
            data, crs = {'init': 'epsg:4326',  'no_defs': True}
        )

    # everything else (hopefully)
    else:

        i, ids, geoms = 1, [], []
        for geom in geometry:
            ids.append(i)
            geoms.append(geom)
            i += 1

        gdf = gpd.GeoDataFrame({'id': ids,
                                'geometry': geoms},
                                crs = {'init': 'epsg:4326',  'no_defs': True}
              )
    
    return gdf


def gdf_to_json_geometry(gdf):
    """Function to parse features from GeoDataFrame in such a manner 
       that rasterio wants them"""
#    
#    try:
#        gdf.geometry.values[0].type
#        features = [json.loads(gdf.to_json())['features'][0]['geometry']]
#    except AttributeError:
#        ids, feats =[], []
#        for i, feat in enumerate(gdf.geometry.values[0]):
#            ids.append(i)
#            feats.append(feat)
#
#        gdf = gpd.GeoDataFrame({'id': ids,
#                                'geometry': feats}, 
#                                    geometry='geometry', 
#                                    crs = gdf.crs
#                                    )
    geojson = json.loads(gdf.to_json())
    return [feature['geometry'] for feature in geojson['features'] 
            if feature['geometry']]


def exterior(infile, outfile, buffer=None):

    gdf = gpd.read_file(infile, crs={'init': 'EPSG:4326'})
    gdf.geometry = gdf.geometry.apply(lambda row: Polygon(row.exterior))
    gdf_clean = gdf[gdf.geometry.area >= 1.0e-6]

    if buffer:
        gdf_clean.geometry = gdf_clean.geometry.buffer(buffer)

    gdf_clean.to_file(outfile, driver='GPKG')


def difference(infile1, infile2, outfile):

    gdf1 = gpd.read_file(infile1)
    gdf2 = gpd.read_file(infile2)

    gdf3 = gpd.overlay(gdf1, gdf2, how='symmetric_difference')

    gdf3.to_file(outfile, driver='GPKG')


def buffer_shape(infile, outfile, buffer=None):

    with collection(infile, "r") as in_shape:
        # schema = in_shape.schema.copy()
        schema = {'geometry': 'Polygon', 'properties': {'id': 'int'}}
        crs = in_shape.crs
        with collection(
                outfile, "w", "ESRI Shapefile", schema, crs=crs) as output:

            for i, point in enumerate(in_shape):
                output.write({
                    'properties': {
                        'id': i
                    },
                    'geometry': mapping(
                        shape(point['geometry']).buffer(buffer))
                })


def plot_inventory(aoi, inventory_df, transparency=0.05, annotate=False):

    import matplotlib.pyplot as plt

    # load world borders for background
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

    # import aoi as gdf
    aoi_gdf = wkt_to_gdf(aoi)

    # get bounds of AOI
    bounds = inventory_df.geometry.bounds

    # get world map as base
    base = world.plot(color='lightgrey', edgecolor='white')

    # plot aoi
    aoi_gdf.plot(ax=base, color='None', edgecolor='black')

    # plot footprints
    inventory_df.plot(ax=base, alpha=transparency)

    # set bounds
    plt.xlim([bounds.minx.min()-2, bounds.maxx.max()+2])
    plt.ylim([bounds.miny.min()-2, bounds.maxy.max()+2])
    plt.grid(color='grey', linestyle='-', linewidth=0.2)
    if annotate:
        import math
        for idx, row in inventory_df.iterrows():
            # print([row['geometry'].bounds[0],row['geometry'].bounds[3]])
            coord = [row['geometry'].centroid.x, row['geometry'].centroid.y]
            x1, y2, x2, y1 = row['geometry'].bounds
            angle = math.degrees(math.atan2((y2 - y1), (x2 - x1)))
            # rint(angle)
            plt.annotate(s=row['bid'], xy=coord, rotation=angle + 5, size=10, color='red', horizontalalignment='center')
