import os
from os.path import join as opj
import importlib
import json
import glob
import logging
import urllib
from urllib.error import URLError
import zipfile
import fnmatch
import rasterio
import xml.dom.minidom
import xml.etree.ElementTree as eTree
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads
from shapely.geometry import Polygon, Point

from ost.helpers import scihub, raster as ras
from ost.s1.grd_to_ard import grd_to_ard
from ost.s1.burst_batch import bursts_to_ards
from ost.helpers.settings import APIHUB_BASEURL, APIHUB_QUERY_BASEURL, \
    check_ard_parameters
from ost.s1 import burst_inventory
from ost.s1.ard_to_rgb import ard_to_rgb, ard_to_thumbnail, \
    ard_slc_to_rgb, ard_slc_to_thumbnail

logger = logging.getLogger(__name__)


class Sentinel1Scene:

    def __init__(self, scene_id, ard_type='OST-GTC'):
        self.scene_id = scene_id
        self.mission_id = scene_id[0:3]
        self.mode_beam = scene_id[4:6]
        self.product_type = scene_id[7:10]
        self.resolution_class = scene_id[10]
        self.proc_level = scene_id[12]
        self.pol_mode = scene_id[14:16]
        self.start_date = scene_id[17:25]
        self.start_time = scene_id[26:32]
        self.stop_date = scene_id[33:41]
        self.stop_time = scene_id[42:48]
        self.abs_orbit = scene_id[49:55]
        self.data_take_id = scene_id[57:62]
        self.unique_id = scene_id[63:]
        self.year = scene_id[17:21]
        self.month = scene_id[21:23]
        self.day = scene_id[23:25]
        self.onda_class = scene_id[4:14]

        # Calculate the relative orbit out of absolute orbit
        # (from Peter Meadows (ESA) @
        # http://forum.step.esa.int/t/sentinel-1-relative-orbit-from-filename/7042)
        if self.mission_id == 'S1A':
            self.orbit_offset = 73
            self.satellite = "Sentinel-1A"
        elif self.mission_id == 'S1B':
            self.orbit_offset = 27
            self.satellite = "Sentinel-1B"

        self.rel_orbit = (((int(self.abs_orbit)
                            - int(self.orbit_offset)) % 175) + 1)

        # Get polarisations
        self.polarisation_list = []
        if self.pol_mode == 'SH':
            self.polarisation_list = ['HH']
        elif self.pol_mode == 'SV':
            self.polarisation_list = ['VV']
        elif self.pol_mode == 'DH':
            self.polarisation_list = ['HH', 'HV']
        elif self.pol_mode == 'DV':
            self.polarisation_list = ['VV', 'VH']
        elif self.pol_mode == 'HH':
            self.polarisation_list = ['HH']
        elif self.pol_mode == 'HV':
            self.polarisation_list = ['HV']
        elif self.pol_mode == 'VV':
            self.polarisation_list = ['VV']
        elif self.pol_mode == 'VH':
            self.polarisation_list = ['VH']
        else:
            raise ValueError(
                'Incompatible polarization mode in %s', self.scene_id
            )

        # get acquisition mode
        if self.mode_beam == 'IW':
            self.acq_mode = "Interferometric Wide Swath"
        elif self.mode_beam == 'SM':
            self.acq_mode = "Stripmap"
        elif self.mode_beam == 'EW':
            self.acq_mode = "Extra-Wide swath"
        elif self.mode_beam == 'WV':
            self.acq_mode = "Wave"

        # get acquisition mode
        if self.product_type == 'GRD':
            self.p_type = "Ground Range Detected (GRD)"
        elif self.product_type == 'SLC':
            self.p_type = "Single-Look Complex (SLC)"
        elif self.product_type == 'OCN':
            self.p_type = "Ocean"
        elif self.product_type == 'RAW':
            self.p_type = "Raw Data (RAW)"

        # set initial product paths to None
        self.ard_dimap = None
        self.ard_rgb = None
        self.rgb_thumbnail = None

        # set initial ARD parameters to ard_type
        self.ard_parameters = {}
        self.get_ard_parameters(ard_type)
        check_ard_parameters(self.ard_parameters)

        self.data_mount = '/eodata'

    def info(self):

        # create info dictionary necessary for tests
        inf_dict = {}
        inf_dict.update(
            Scene_Identifier=str(self.scene_id),
            Satellite=str(self.satellite),
            Acquisition_Mode=str(self.acq_mode),
            Processing_Level=str(self.proc_level),
            Product_Type=str(self.p_type),
            Acquisition_Date=str(self.start_date),
            Start_Time=str(self.start_time),
            Stop_Time=str(self.stop_time),
            Absolute_Orbit=str(self.abs_orbit),
            Relative_Orbit=str(self.rel_orbit),
        )

        # actual print function
        print(" -------------------------------------------------")
        print(" Scene Information:")
        print(" Scene Identifier:        " + str(self.scene_id))
        print(" Satellite:               " + str(self.satellite))
        print(" Acquisition Mode:        " + str(self.acq_mode))
        print(" Processing Level:        " + str(self.proc_level))
        print(" Product Type:            " + str(self.p_type))
        print(" Acquisition Date:        " + str(self.start_date))
        print(" Start Time:              " + str(self.start_time))
        print(" Stop Time:               " + str(self.stop_time))
        print(" Absolute Orbit:          " + str(self.abs_orbit))
        print(" Relative Orbit:          " + str(self.rel_orbit))
        print(" -------------------------------------------------")

        return inf_dict

    def download(self, download_dir, mirror=None):

        if not mirror:
            logger.info('One or more of your scenes need to be downloaded.')
            print(' Select the server from where you want to download:')
            print(' (1) Copernicus Apihub (ESA, rolling archive)')
            print(' (2) Alaska Satellite Facility (NASA, full archive)')
            mirror = input(' Type 1 or 2')

        from ost.s1 import download
        if mirror == '1':
            uname, pword = scihub.ask_credentials()
            opener = scihub.connect(uname=uname, pword=pword)
            df = pd.DataFrame(
                {'identifier': [self.scene_id],
                 'uuid': [self.scihub_uuid(opener)]
                 }
            )
        else:  # ASF
            df = pd.DataFrame({'identifier': [self.scene_id]})
            download.download_sentinel1(df, download_dir, mirror)
            return

        download.download_sentinel1(df,
                                    download_dir,
                                    mirror,
                                    uname=uname,
                                    pword=pword
                                    )
        # delete credentials
        del uname, pword

    # location of file (including diases)
    def _download_path(self, download_dir, mkdir=False):
        download_path = Path(download_dir).joinpath(
            f'SAR/{self.product_type}/{self.year}/{self.month}/{self.day}'
        )
        # make dir if not existent
        if mkdir:
            download_path.mkdir(parents=True, exist_ok=True)
        # get filepath
        filepath = download_path.joinpath(f'{self.scene_id}.zip')
        return filepath

    def get_path(self, download_dir=None, data_mount=None):
        path = None
        product_dl_path = None
        if download_dir is not None:
            if isinstance(download_dir, str):
                download_dir = Path(download_dir)
            product_dl_path = self._download_path(
                download_dir=download_dir, mkdir=False
            )
            if product_dl_path.with_suffix('.downloaded').exists():
                path = product_dl_path

        if path is None:
            raise FileNotFoundError(
                'No product path found for: {}, {}'.format(
                    self.scene_id,
                    product_dl_path,
                )
            )
        return path


    # scihub related
    def scihub_uuid(self, opener):
        base_url = (
            f'{APIHUB_QUERY_BASEURL}?$filter='
        )
        action = urllib.request.quote(f'Name eq \'{self.scene_id}\'')

        # construct the download url
        url = base_url + action

        # DEBUG log
        # logger.info(f"Quering {url}")
        try:
            req = opener.open(url)
        except URLError as error:
            raise error
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode('utf-8')

            # return uuid from response
            return response.split("Products('")[1].split("')")[0]

    def scihub_url(self, opener):

        # return the full url
        return f'{APIHUB_BASEURL}(\'{self.scihub_uuid(opener)}\')/$value'

    def scihub_md5(self, opener):

        # return the md5 checksum
        return (
            f'{APIHUB_BASEURL}(\'{self.scihub_uuid(opener)}\')'
            f'/Checksum/Value/$value'
        )

    def scihub_online_status(self, opener):

        # get url for product
        url = f'{APIHUB_BASEURL}(\'{self.scihub_uuid(opener)}\')/Online/$value'

        # check if something is coming back from our request
        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            raise error
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode('utf-8')

            if response == 'true':
                response = True
            elif response == 'false':
                response = False

        return response

    def scihub_trigger_production(self, opener):

        # get uuid and construct url for scihub's apihub
        uuid = self.scihub_uuid(opener)
        url = f'{APIHUB_BASEURL}(\'{uuid}\')/$value'

        try:
            # get the request
            req = opener.open(url)

        except URLError as error:
            if hasattr(error, 'reason'):
                raise ConnectionError(
                    'We failed to connect to the server. Reason: {}'.format(
                        error.reason
                    )
                )
            elif hasattr(error, 'code'):
                raise ConnectionError(
                    'The server couldnt fulfill the request. '
                    'Error code: {}'.format(error.code)
                )

        # write the request to to the response variable
        # (i.e. the xml coming back from scihub)
        code = req.getcode()
        if code == 202:
            print(' Production of {} successfully requested.'
                  .format(self.scene_id))

        return code

    # burst part
    def _scihub_annotation_url(self, opener):

        uuid = self.scihub_uuid(opener)

        logger.info('Getting URLS of annotation files'
                    ' for S1 product: {}.'.format(self.scene_id))
        anno_path = ('(\'{}\')/Nodes(\'{}.SAFE\')/Nodes(\'annotation\')/'
                     'Nodes'.format(uuid, self.scene_id))
        url = APIHUB_QUERY_BASEURL + anno_path
        # DEBUG log
        # logger.info(f"Getting annotation via: {url}")
        try:
            req = opener.open(url)
        except URLError as error:
            raise error
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode('utf-8')

            # parse the xml page from the response
            dom = xml.dom.minidom.parseString(response)
            url_list = []
            # loop thorugh each entry (with all metadata)
            for node in dom.getElementsByTagName('entry'):
                download_url = node.getElementsByTagName(
                    'id')[0].firstChild.nodeValue

                if download_url[-6:-2] == '.xml':
                    url_list.append('{}/$value'.format(download_url))

        return url_list

    def _burst_database(self, et_root):
        '''
        This functions expects an xml string from a Sentinel-1 SLC
        annotation file and extracts relevant information for burst
        identification as a GeoPandas GeoDataFrame.

        Much of the code is taken from RapidSAR
        package (once upon a time on github).
        '''

        column_names = ['SceneID', 'Track', 'Date', 'SwathID', 'AnxTime',
                        'BurstNr', 'geometry']
        gdf = gpd.GeoDataFrame(columns=column_names)

        track = self.rel_orbit
        acq_date = self.start_date

        # pol = root.find('adsHeader').find('polarisation').text
        swath = et_root.find('adsHeader').find('swath').text
        lines_per_burst = int(et_root.find('swathTiming').find(
            'linesPerBurst').text)
        pixels_per_burst = int(et_root.find('swathTiming').find(
            'samplesPerBurst').text)
        burstlist = et_root.find('swathTiming').find('burstList')
        geolocation_grid = et_root.find('geolocationGrid')[0]
        first = {}
        last = {}
        # Get burst corner geolocation info
        for geo_point in geolocation_grid:
            if geo_point.find('pixel').text == '0':
                first[geo_point.find('line').text] = np.float32(
                    [geo_point.find('latitude').text,
                     geo_point.find('longitude').text])
            elif geo_point.find('pixel').text == str(pixels_per_burst - 1):
                last[geo_point.find('line').text] = np.float32(
                    [geo_point.find('latitude').text,
                     geo_point.find('longitude').text])

        for i, b in enumerate(burstlist):
            firstline = str(i * lines_per_burst)
            lastline = str((i + 1) * lines_per_burst)
            azi_anx_time = np.float32(b.find('azimuthAnxTime').text)
            orbit_time = 12 * 24 * 60 * 60 / 175

            if azi_anx_time > orbit_time:
                azi_anx_time = np.mod(azi_anx_time, orbit_time)

            azi_anx_time = int(np.round(azi_anx_time * 10))
            try:
                firstthis = first[firstline]
            except:
                firstline = str(int(firstline) - 1)
                try:
                    firstthis = first[firstline]
                except:
                    logger.info('First line not found in annotation file')
                    firstthis = []
            try:
                lastthis = last[lastline]
            except:
                lastline = str(int(lastline) - 1)
                try:
                    lastthis = last[lastline]
                except:
                    logger.info('Last line not found in annotation file')
                    lastthis = []
            corners = np.zeros([4, 2], dtype=np.float32)

            # Had missing info for 1 burst in a file, hence the check
            if len(firstthis) > 0 and len(lastthis) > 0:
                corners[0] = first[firstline]
                corners[1] = last[firstline]
                corners[3] = first[lastline]
                corners[2] = last[lastline]

            wkt = 'POLYGON (({} {},{} {},{} {},{} {},{} {}))'.format(
                np.around(float(corners[0, 1]), 3),
                np.around(float(corners[0, 0]), 3),
                np.around(float(corners[3, 1]), 3),
                np.around(float(corners[3, 0]), 3),
                np.around(float(corners[2, 1]), 3),
                np.around(float(corners[2, 0]), 3),
                np.around(float(corners[1, 1]), 3),
                np.around(float(corners[1, 0]), 3),
                np.around(float(corners[0, 1]), 3),
                np.around(float(corners[0, 0]), 3))

            geo_dict = {'SceneID': self.scene_id, 'Track': track,
                        'Date': acq_date, 'SwathID': swath,
                        'AnxTime': azi_anx_time, 'BurstNr': i + 1,
                        'geometry': loads(wkt)
                        }
            gdf = gdf.append(geo_dict, ignore_index=True)
        return gdf

    def scihub_annotation_get(self, uname=None, pword=None):

        # define column names fro BUrst DF
        column_names = ['SceneID', 'Track', 'Date', 'SwathID',
                        'AnxTime', 'BurstNr', 'geometry']

        gdf_final = gpd.GeoDataFrame(columns=column_names)

        # get connected to scihub
        opener = scihub.connect(APIHUB_BASEURL, uname, pword)
        anno_list = self._scihub_annotation_url(opener)

        for url in anno_list:
            try:
                # get the request
                req = opener.open(url)
            except URLError as error:
                if hasattr(error, 'reason'):
                    raise ConnectionError(
                        'We failed to connect to the server. Reason: {}'.format(
                            error.reason
                        )
                    )
                elif hasattr(error, 'code'):
                    raise ConnectionError(
                        'The server couldnt fulfill the request. '
                        'Error code: {}'.format(error.code)
                    )
            else:
                # write the request to to the response variable
                # (i.e. the xml coming back from scihub)
                response = req.read().decode('utf-8')

                et_root = eTree.fromstring(response)

                # parse the xml page from the response
                gdf = self._burst_database(et_root)
                gdf_final = gdf_final.append(gdf)
        gdf_final.reset_index(drop=False, inplace=True)
        gdf_final.rename(columns={"index": "bid"}, inplace=True)
        return gdf_final.drop_duplicates(['AnxTime'], keep='first')

    def zip_annotation_get(self, download_dir, data_mount='/eodata'):

        column_names = ['SceneID', 'Track', 'Date', 'SwathID', 'AnxTime',
                        'BurstNr', 'geometry']

        # crs for empty dataframe
        crs = {'init': 'epsg:4326'}
        gdf_final = gpd.GeoDataFrame(columns=column_names, crs=crs)
        file = self.get_path(download_dir, data_mount)

        # extract info from archive
        archive = zipfile.ZipFile(file, 'r')
        namelist = archive.namelist()
        xml_files = fnmatch.filter(namelist, "*/annotation/s*.xml")

        # loop through xml annotation files
        for xml_file in xml_files:
            xml_string = archive.open(xml_file)
            gdf = self._burst_database(eTree.parse(xml_string))
            gdf_final = gdf_final.append(gdf)
        gdf_final.reset_index(drop=False, inplace=True)
        gdf_final.rename(columns={"index": "bid"}, inplace=True)
        return gdf_final.drop_duplicates(['AnxTime'], keep='first')

    def safe_annotation_get(self, download_dir, data_mount='/eodata'):

        column_names = ['SceneID', 'Track', 'Date', 'SwathID',
                        'AnxTime', 'BurstNr', 'geometry']
        gdf_final = gpd.GeoDataFrame(columns=column_names)

        for anno_file in glob.glob(
                '{}/annotation/*xml'.format(
                    self.get_path(download_dir=download_dir,
                                  data_mount=data_mount))):
            # parse the xml page from the response
            gdf = self._burst_database(eTree.parse(anno_file))
            gdf_final = gdf_final.append(gdf)
        gdf_final.reset_index(drop=False, inplace=True)
        gdf_final.rename(columns={"index": "bid"}, inplace=True)

        return gdf_final.drop_duplicates(['AnxTime'], keep='first')

    # other data providers
    def asf_url(self):

        asf_url = 'https://datapool.asf.alaska.edu'

        if self.mission_id == 'S1A':
            mission = 'SA'
        elif self.mission_id == 'S1B':
            mission = 'SB'

        if self.product_type == 'SLC':
            pType = self.product_type
        elif self.product_type == 'GRD':
            pType = 'GRD_{}{}'.format(self.resolution_class, self.pol_mode[0])

        productURL = '{}/{}/{}/{}.zip'.format(asf_url, pType,
                                              mission, self.scene_id)
        return productURL

    # processing related functions
    def get_ard_parameters(self, ard_type='OST-GTC'):

        # get path to ost package
        rootpath = importlib.util.find_spec('ost').submodule_search_locations[
            0]
        rootpath = opj(rootpath, 'graphs', 'ard_json')

        template_file = opj(rootpath, '{}.{}.json'.format(
            self.product_type.lower(),
            ard_type.replace('-', '_').lower()))

        with open(template_file, 'r') as ard_file:
            self.ard_parameters = json.load(ard_file)['processing']

    def set_external_dem(self, dem_file):
        # check if file exists
        if not os.path.isfile(dem_file):
            raise FileNotFoundError(
                'No dem file found at location {}.'.format(dem_file)
            )
        # get no data value
        with rasterio.open(dem_file) as file:
            dem_nodata = file.nodata

        # get resapmpling
        img_res = self.ard_parameters['single ARD']['dem']['image_resampling']
        dem_res = self.ard_parameters['single ARD']['dem']['dem_resampling']

        # update ard parameters
        dem_dict = dict({'dem_name': 'External DEM',
                         'dem_file': dem_file,
                         'dem_nodata': dem_nodata,
                         'dem_resampling': dem_res,
                         'image_resampling': img_res})
        self.ard_parameters['single_ARD']['dem'] = dem_dict

    def create_ard(
            self,
            download_dir,
            out_dir,
            filelist=None,
            out_prefix=None,
            temp_dir=None,
            subset=None,
            max_workers=os.cpu_count(),
            executor_type='concurrent_processes',
            overwrite=False
    ):
        if filelist is None:
            filelist = [self.get_path(download_dir=download_dir)]

        if overwrite is True:
            logger.info('Overwrite flag here is a placeholder')

        self.ard_dimap = None
        if self.product_type == 'SLC':
            out_files_dict = {'bs': [], 'ls': [], 'coh': [], 'pol': []}
        elif self.product_type == 'GRD':
            out_files_dict = {'bs': '', 'ls': ''}
        else:
            raise TypeError('Only GRD or SLC S1 product types are supported!!')

        if subset is not None:
            p_poly = loads(subset)
            self.processing_poly = p_poly
            self.center_lat = p_poly.bounds[3]-p_poly.bounds[1]
        else:
            self.processing_poly = self.get_product_polygon(download_dir=download_dir)
            try:
                if self.product_type == 'GRD':
                    self.center_lat = self._get_center_lat(filelist[0])
                else:
                    self.center_lat = self._get_center_lat(filelist)
            except Exception as e:
                raise e
        if float(self.center_lat) > 59 or float(self.center_lat) < -59:
            logger.info(
                'INFO: Scene is outside SRTM coverage. Will use 30m ASTER'
                ' DEM instead.'
            )
            self.ard_parameters['dem'] = 'ASTER 1sec GDEM'

        if self.product_type == 'GRD':
            if out_prefix is None:
                out_prefix = '{}_{}'.format(
                        self.scene_id, self.ard_parameters['single_ARD']['product_type']
                )
            out_prefix = out_prefix.replace(' ', '_')
            with TemporaryDirectory(dir=temp_dir) as temp:
                if isinstance(filelist, str):
                    filelist = [filelist]
                # run the processing
                return_code, out_bs, out_ls = grd_to_ard(
                    filelist=filelist,
                    output_dir=out_dir,
                    file_id=out_prefix,
                    temp_dir=temp,
                    ard_params=self.ard_parameters,
                    subset=subset,
                    gpt_max_workers=os.cpu_count()
                    )
                out_files_dict['bs'] = out_bs
                out_files_dict['ls'] = out_ls
                # write to class attribute
                self.ard_dimap = out_files_dict
                if not os.path.isfile(str(self.ard_dimap['bs'])):
                    raise RuntimeError

        elif self.product_type == 'SLC':
            """
            Works for only one product at a time, all products are handled as 
            master products in this condition, returning an ARD with 
            the provided ARD parameters!
            """
            # we need to convert the infile t a list for the grd_to_ard routine
            if subset is not None:
                try:
                    processing_poly = loads(subset)
                    self.processing_poly = processing_poly
                except Exception as e:
                    raise e
            else:
                processing_poly = None
            # get bursts
            master_bursts = self.zip_annotation_get(download_dir=download_dir)
            bursts = burst_inventory.get_bursts_by_polygon(
                burst_inv=master_bursts,
                out_poly=processing_poly
            )
            with TemporaryDirectory(dir=temp_dir) as temp:
                self.config_dict = {}
                self.config_dict['processing'] = self.ard_parameters
                self.config_dict['processing_dir'] = Path(out_dir)
                self.config_dict['download_dir'] = Path(download_dir)
                self.config_dict['temp_dir'] = Path(temp)
                self.config_dict['data_mount'] = self.data_mount
                self.config_dict['godale_max_workers'] = max_workers
                self.config_dict['gpt_max_workers'] = 1

                exception_flag = True
                exception_counter = 0
                while exception_flag is True:
                    if exception_counter > 3 or exception_flag is False:
                        break
                    try:
                        out_files_dict = bursts_to_ards(
                            burst_gdf=bursts,
                            config_dict=self.config_dict,
                            executor_type=executor_type,
                            max_workers=max_workers
                        )
                    except Exception as e:
                        logger.info(e)
                        max_workers = int(max_workers/2)
                        exception_counter += 1
                    else:
                        exception_flag = False
            self.ard_dimap = out_files_dict
        else:
            raise TypeError('Create_ard needs S1 SLC or GRD!!!')
        return self.ard_dimap

    def create_rgb(self, outfile, process_bounds=None, driver='GTiff'):
        # invert ot db from create_ard workflow for rgb creation
        # (otherwise we do it double)
        logger.info('Creating RGB Geotiff for scene: %s', self.scene_id)
        if self.ard_parameters['single_ARD']['to_db']:
            to_db = False
        else:
            to_db = True
        if self.product_type == 'GRD':
            self.processing_poly = None
            ard_to_rgb(infile=self.ard_dimap['bs'],
                       outfiles=[outfile, None],
                       driver=driver,
                       to_db=to_db
                       )
        elif self.product_type == 'SLC':
            if process_bounds is None:
                process_bounds = self.processing_poly.bounds
            bs_list = self.ard_dimap['bs']
            ard_slc_to_rgb(bs_list, outfile, process_bounds, driver, to_db)
        self.ard_rgb = outfile
        logger.info('RGB Geotiff done for scene: %s', self.scene_id)
        return outfile

    def create_rgb_thumbnail(self, outfile, driver='JPEG', shrink_factor=25):
        # invert to db from create_ard workflow for rgb creation
        # (otherwise we do it double)
        if self.product_type == 'GRD':
            if self.ard_parameters['to_db']:
                to_db = False
            else:
                to_db = True
            self.rgb_thumbnail = outfile
            ard_to_thumbnail(
                self.ard_dimap,
                self.rgb_thumbnail,
                driver,
                shrink_factor,
                to_db
            )
        elif self.product_type == 'SLC':
            to_db = False
            self.rgb_thumbnail = outfile
            ard_slc_to_thumbnail(
                self.ard_rgb,
                self.rgb_thumbnail,
                driver,
                shrink_factor
            )
        return outfile

    def visualise_rgb(self, shrink_factor=25):

        ras.visualise_rgb(self.ard_rgb, shrink_factor)

    # other functions
    def _get_center_lat(self, scene_path=None):
        scene_path = str(scene_path)
        if scene_path[-4:] == '.zip':
            zip_archive = zipfile.ZipFile(scene_path)
            manifest = zip_archive.read('{}.SAFE/manifest.safe'
                                        .format(self.scene_id))
        elif scene_path[-5:] == '.SAFE':
            with open(opj(scene_path, 'manifest.safe'), 'rb') as file:
                manifest = file.read()

        root = eTree.fromstring(manifest)
        for child in root:
            metadata = child.findall('metadataObject')
            for meta in metadata:
                for wrap in meta.findall('metadataWrap'):
                    for data in wrap.findall('xmlData'):
                        for frameSet in data.findall(
                                '{http://www.esa.int/safe/sentinel-1.0}frameSet'):
                            for frame in frameSet.findall(
                                    '{http://www.esa.int/safe/sentinel-1.0}frame'):
                                for footprint in frame.findall(
                                        '{http://www.esa.int/'
                                        'safe/sentinel-1.0}footPrint'):
                                    for coords in footprint.findall(
                                            '{http://www.opengis.net/gml}'
                                            'coordinates'):
                                        coordinates = coords.text.split(' ')

        sums = 0
        for i, coords in enumerate(coordinates):
            sums = sums + float(coords.split(',')[0])

        return sums / (i + 1)

    def get_product_polygon(self, download_dir, data_mount=None):
        scene_path = str(self.get_path(download_dir=download_dir, data_mount=data_mount))
        if scene_path[-4:] == '.zip':
            zip_archive = zipfile.ZipFile(scene_path)
            manifest = zip_archive.read('{}.SAFE/manifest.safe'
                                        .format(self.scene_id))
        elif scene_path[-5:] == '.SAFE':
            with open(opj(scene_path, 'manifest.safe'), 'rb') as file:
                manifest = file.read()

        root = eTree.fromstring(manifest)
        for child in root:
            metadata = child.findall('metadataObject')
            for meta in metadata:
                for wrap in meta.findall('metadataWrap'):
                    for data in wrap.findall('xmlData'):
                        for frameSet in data.findall(
                                '{http://www.esa.int/safe/sentinel-1.0}frameSet'):
                            for frame in frameSet.findall(
                                    '{http://www.esa.int/safe/sentinel-1.0}frame'):
                                for footprint in frame.findall(
                                        '{http://www.esa.int/'
                                        'safe/sentinel-1.0}footPrint'):
                                    for coords in footprint.findall(
                                            '{http://www.opengis.net/gml}'
                                            'coordinates'):
                                        coordinates = coords.text.split(' ')
        for i, p in enumerate(coordinates):
            coordinates[i] = Point(float(p.split(',')[1]), float(p.split(',')[0]))
        poly = Polygon([[p.x, p.y] for p in coordinates])
        return poly
