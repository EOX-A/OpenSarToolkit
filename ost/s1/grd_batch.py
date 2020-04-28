import os
from os.path import join as opj
import numpy as np
import json
import glob
import itertools
import logging
import gdal
from retry import retry

from godale import Executor

from shapely.wkt import loads
from shapely.ops import unary_union
from shapely.geometry import box

from ost.s1.s1scene import Sentinel1Scene
from ost.helpers import raster as ras
from ost.generic import ts_extent
from ost.generic import ts_ls_mask
from ost.generic import ard_to_ts
from ost.generic import timescan
from ost.generic import mosaic
from ost.s1.grd_to_ard import grd_to_ard
from ost.s1.ard_to_rgb import ard_to_rgb

logger = logging.getLogger(__name__)


def _create_processing_dict(inventory_df):
    ''' This function might be obsolete?

    '''

    # initialize empty dictionary
    dict_scenes = {}

    # get relative orbits and loop through each
    tracklist = inventory_df['relativeorbit'].unique()
    for track in tracklist:
        # initialize an empty list that will be filled by
        # list of scenes per acq. date
        all_ids = []

        # get acquisition dates and loop through each
        acquisition_dates = inventory_df['acquisitiondate'][
            inventory_df['relativeorbit'] == track].unique()

        # loop through dates
        for acquisition_date in acquisition_dates:
            # get the scene ids per acquisition_date and write into a list
            single_id = []
            single_id.append(inventory_df['identifier'][
                (inventory_df['relativeorbit'] == track) &
                (inventory_df['acquisitiondate'] == acquisition_date)].tolist())

            # append the list of scenes to the list of scenes per track
            all_ids.append(single_id[0])

        # add this list to the dctionary and associate the track number
        # as dict key
        dict_scenes[track] = all_ids

    return dict_scenes


@retry(tries=4, delay=5, logger=logger)
def _execute_grd_batch(
        list_of_scenes,
        inventory_df,
        download_dir,
        processing_dir,
        temp_dir,
        config_dict,
        subset=None,
        to_tif=False,
        ):
    track, list_of_scenes = list_of_scenes
    if subset is not None:
        acq_poly = None
        sub_boudns = loads(subset).bounds
        sub_poly = box(sub_boudns[0], sub_boudns[1], sub_boudns[2], sub_boudns[3])
        for scene in list_of_scenes:
            if acq_poly is None:
                acq_poly = Sentinel1Scene(scene).get_product_polygon(
                    download_dir=download_dir
                )
            else:
                acq_poly = unary_union([
                    acq_poly,
                    Sentinel1Scene(scene).get_product_polygon(
                        download_dir=download_dir
                    )
                ])
        if acq_poly is None:
            subset = sub_poly.envelope.wkt
        elif not acq_poly.intersects(sub_poly):
            for i, row in inventory_df.iterrows():
                if row.identifier == Sentinel1Scene(scene).scene_id:
                    logger.debug(
                        'Scene does not intersect the subset %s',
                        Sentinel1Scene(scene).scene_id
                    )
                    inventory_df.at[i, 'out_dimap'] = None
                    inventory_df.at[i, 'out_ls_mask'] = None
                    if to_tif:
                        inventory_df.at[i, 'out_tif'] = None
        elif (acq_poly.intersection(sub_poly).area / acq_poly.area) * 100 > 85 or \
                acq_poly.within(sub_poly):
            subset = None
        else:
            subset = acq_poly.intersection(sub_poly).envelope.wkt

    # get acquisition date
    acquisition_date = Sentinel1Scene(list_of_scenes[0]).start_date
    # create a subdirectory baed on acq. date
    out_dir = opj(processing_dir, track, acquisition_date)
    os.makedirs(out_dir, exist_ok=True)

    file_id = '{}_{}'.format(acquisition_date, track)
    out_file = opj(out_dir, '{}_BS.dim'.format(file_id))
    out_ls_mask = opj(out_dir, '{}_LS.gpkg'.format(file_id))
    tif_file = out_file.replace('.dim', '.tif')

    # check if already processed
    if os.path.isfile(opj(out_dir, '.processed')):
        logger.info(
            'Acquisition from {} of track {}'
            ' already processed'.format(acquisition_date, track)
        )
        for i, row in inventory_df.iterrows():
            for s in list_of_scenes:
                if row.identifier == Sentinel1Scene(s).scene_id:
                    if os.path.isfile(out_file):
                        inventory_df.at[i, 'out_dimap'] = out_file
                    else:
                        inventory_df.at[i, 'out_dimap'] = None

                    if os.path.isfile(out_ls_mask):
                        inventory_df.at[i, 'out_ls_mask'] = out_ls_mask
                    else:
                        inventory_df.at[i, 'out_ls_mask'] = None

                    if to_tif and not os.path.isfile(tif_file) and \
                            os.path.isfile(out_file):
                        ard_to_rgb(
                            infile=out_file,
                            outfile=tif_file,
                            driver='GTiff',
                            to_db=True,
                            executor_type=config_dict['executor_type'],
                            max_workers=os.cpu_count()
                        )
                        inventory_df.at[i, 'out_tif'] = tif_file
                    elif to_tif and os.path.isfile(tif_file):
                        inventory_df.at[i, 'out_tif'] = tif_file
                    else:
                        inventory_df.at[i, 'out_tif'] = None
    else:
        # get the paths to the file
        scene_paths = ([
            Sentinel1Scene(i).get_path(download_dir=download_dir)
            for i in list_of_scenes
        ])

        # apply the grd_to_ard function
        return_code, out_file, out_ls_mask = grd_to_ard(
            scene_paths,
            out_dir,
            file_id,
            temp_dir,
            ard_params=config_dict['processing'],
            subset=subset,
            gpt_max_workers=config_dict['gpt_max_workers']
        )
        for i, row in inventory_df.iterrows():
            for s in list_of_scenes:
                if row.identifier == Sentinel1Scene(s).scene_id:
                    inventory_df.at[i, 'out_dimap'] = out_file
                    inventory_df.at[i, 'out_ls_mask'] = out_ls_mask
                    if to_tif and out_file is not None:
                        if not os.path.exists(tif_file):
                            ard_to_rgb(
                                infile=out_file,
                                outfile=tif_file,
                                driver='GTiff',
                                to_db=True
                            )
                        inventory_df.at[i, 'out_tif'] = tif_file
    return inventory_df, list_of_scenes


def grd_to_ard_batch(
        inventory_df,
        download_dir,
        processing_dir,
        temp_dir,
        config_dict,
        subset=None,
        to_tif=False,
):
    # Where all combinations are stored for parallel processing
    lists_to_process = []
    # where all frames are grouped into acquisitions
    processing_dict = _create_processing_dict(inventory_df)

    for track, allScenes in processing_dict.items():
        for list_of_scenes in processing_dict[track]:
            lists_to_process.append((track, list_of_scenes))
    executor = Executor(max_workers=int(os.cpu_count()/4),
                        executor=config_dict['executor_type']
                        )
    for task in executor.as_completed(
        func=_execute_grd_batch,
        iterable=lists_to_process,
        fargs=[
            inventory_df,
            download_dir,
            processing_dir,
            temp_dir,
            config_dict,
            subset,
            to_tif
        ],
    ):
        try:
            temp_inv, list_of_scenes = task.result()
            for i, row in inventory_df.iterrows():
                for scene in list_of_scenes:
                    if row.identifier.lower() in scene.lower():
                        inventory_df.at[i, 'out_dimap'] = temp_inv.at[i, 'out_dimap']
                        inventory_df.at[i, 'out_ls_mask'] = temp_inv.at[i, 'out_ls_mask']
                        inventory_df.at[i, 'out_tif'] = temp_inv.at[i, 'out_tif']
        except Exception as e:
            logger.info(e)
    return inventory_df


def ards_to_timeseries(
        inventory_df,
        processing_dir,
        temp_dir,
        config_dict,
):
    ard = config_dict['processing']['single_ARD']
    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = opj(processing_dir, track)
        # get common burst extent
        list_of_scenes = glob.glob(opj(track_dir, '20*', '*data*', '*img'))
        list_of_scenes = [x for x in list_of_scenes if 'layover' not in x]
        extent = opj(track_dir, '{}.extent.gpkg'.format(track))
        logger.info('Creating common extent mask for track {}'.format(track))
        ts_extent.mt_extent(list_of_scenes=list_of_scenes,
                            out_file=extent,
                            temp_dir=temp_dir,
                            buffer=-0.0018
                            )

    if ard['create_ls_mask']:
        for track in inventory_df.relativeorbit.unique():

            # get the burst directory
            track_dir = opj(processing_dir, track)
            list_of_layover = inventory_df['out_ls_mask'].to_list()
            counter = 0
            for e in list_of_layover:
                if np.isnan(e):
                    counter += 1

            if counter == len(list_of_layover):
                logger.debug('No layerover masks found skipping!')
            else:
                # layover/shadow mask
                out_ls = opj(track_dir, '{}.ls_mask.tif'.format(track))

                logger.info('Creating common Layover/Shadow mask for track {}'.format(track))
                ts_ls_mask.mt_layover(filelist=list_of_layover,
                                      outfile=out_ls,
                                      temp_dir=temp_dir,
                                      extent=extent,
                                      )

    for track in inventory_df.relativeorbit.unique():
        # get the burst directory
        track_dir = opj(processing_dir, track)

        for pol in ['VV', 'VH', 'HH', 'HV']:

            # see if there is actually any imagery in thi polarisation
            list_of_files = sorted(glob.glob(
                opj(track_dir, '20*', '*data*', '*ma0*{}*img'.format(pol))))
            # create list of dims if polarisation is present
            list_of_dims = sorted(glob.glob(
                opj(track_dir, '20*', '*BS*dim'))
            )
            if len(list_of_dims) == 0:
                continue
            ard_to_ts.ard_to_ts(
                list_of_files=list_of_dims,
                product='BS'.lower(),
                pol=pol,
                config_dict=config_dict,
                track=track
            )


def timeseries_to_timescan(
        inventory_df,
        processing_dir,
        confic_dict,
):
    # load ard parameters
    ard = confic_dict['processing']['single_ARD']
    ard_mt = confic_dict['processing']['time-series_ARD']
    ard_tscan = confic_dict['processing']['time-scan_ARD']

    # get the db scaling right
    to_db = ard['to_db']
    if ard['to_db'] or ard_mt['to_db']:
        to_db = True

    dtype_conversion = True if ard_mt['dtype_output'] != 'float32' else False

    for track in inventory_df.relativeorbit.unique():

        logger.info('Entering track {}.'.format(track))
        # get track directory
        track_dir = opj(processing_dir, track)
        # define and create Timescan directory
        timescan_dir = opj(track_dir, 'Timescan')
        os.makedirs(timescan_dir, exist_ok=True)

        # loop thorugh each polarization
        for polar in ['VV', 'VH', 'HH', 'HV']:
            if os.path.isfile(opj(timescan_dir, '.{}.processed'.format(polar))):
                logger.info(
                    'Timescans for track {} already processed.'.format(track))
                continue

            # get timeseries vrt
            timeseries = opj(track_dir,
                             'Timeseries',
                             'Timeseries_bs_{}.vrt'.format(polar)
            )

            if not os.path.isfile(timeseries):
                continue

            logger.info('Processing Timescans of {} for track {}.'.format(polar, track))
            # create a datelist for harmonics
            scenelist = glob.glob(
                opj(track_dir, '*bs.{}.tif'.format(polar))
            )

            # create a datelist for harmonics calculation
            datelist = []
            for file in sorted(scenelist):
                datelist.append(os.path.basename(file).split('.')[1])

            # define timescan prefix
            timescan_prefix = opj(timescan_dir, 'bs.{}'.format(polar))

            # run timescan
            timescan.mt_metrics(
                stack=timeseries,
                out_prefix=timescan_prefix,
                metrics=ard_tscan['metrics'],
                rescale_to_datatype=dtype_conversion,
                to_power=False,
                outlier_removal=ard_tscan['remove_outliers'],
                datelist=datelist
            )


def mosaic_timeseries(
        inventory_df,
        processing_dir,
        config_dict,
):
    logger.info('Mosaicking Time-series layers')

    # create output folder
    ts_dir = opj(processing_dir, 'Mosaic', 'Timeseries')
    os.makedirs(ts_dir, exist_ok=True)

    # loop through polarisations
    for p in ['VV', 'VH', 'HH', 'HV']:
        tracks = inventory_df.relativeorbit.unique()
        nr_of_ts = len(glob.glob(opj(
            processing_dir, tracks[0], 'Timeseries', '*_{}.tif'.format(p)))
        )
        if nr_of_ts == 0:
            continue

        outfiles = []
        for i in range(1, nr_of_ts + 1):
            filelist = glob.glob(opj(
                processing_dir, '*', 'Timeseries',
                '{:02d}_*_{}.tif'.format(i, p)
            ))
            filelist = [file for file in filelist if 'Mosaic' not in file]

            logger.info(opj(
                processing_dir, '*', 'Timeseries',
                '{}_*_{}.tif'.format(i, p)
            ))
            # create
            datelist = []
            for file in filelist:
                datelist.append(os.path.basename(file).split('_')[1])

            filelist = ' '.join(filelist)
            if nr_of_ts > 1:
                start, end = sorted(datelist)[0], sorted(datelist)[-1]
            elif nr_of_ts == 1:
                start, end = sorted(datelist)[0], sorted(datelist)[0]
            else:
                break

            if start == end:
                outfile = opj(ts_dir, '{}_{}_bs_{}.tif'.format(i, start, p))
            else:
                outfile = opj(ts_dir, '{}.{}-{}_bs_{}.tif'.format(i, start, end, p))

            check_file = opj(
                os.path.dirname(outfile),
                '.{}.processed'.format(os.path.basename(outfile)[:-4])
            )

            outfiles.append(outfile)

            if os.path.isfile(check_file):
                logger.info(
                    'Mosaic layer {} already processed.'.format(os.path.basename(outfile))
                )
                continue

            logger.info('Mosaicking layer {}.'.format(os.path.basename(outfile)))
            mosaic.mosaic(filelist, outfile, config_dict)

        # create vrt
        vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
        gdal.BuildVRT(
            opj(ts_dir, 'Timeseries_{}.vrt'.format(p)),
            outfiles,
            options=vrt_options
        )


def mosaic_timescan(inventory_df, processing_dir, temp_dir, proc_file,
                    cut_to_aoi=False, exec_file=None):

    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        metrics = ard_params['time-scan ARD']['metrics']

    if 'harmonics' in metrics:
        metrics.remove('harmonics')
        metrics.extent(['amplitude', 'phase', 'residuals'])

    if 'percentiles' in metrics:
            metrics.remove('percentiles')
            metrics.extent(['p95', 'p5'])

    # create out directory of not existent
    tscan_dir = opj(processing_dir, 'Mosaic', 'Timescan')
    os.makedirs(tscan_dir, exist_ok=True)
    outfiles = []

    # loop through all pontial proucts
    for polar, metric in itertools.product(['VV', 'HH', 'VH', 'HV'], metrics):

        # create a list of files based on polarisation and metric
        filelist = glob.glob(opj(processing_dir, '*', 'Timescan',
                                 '*bs.{}.{}.tif'.format(polar, metric)
                            )
                   )

        # break loop if there are no files
        if not len(filelist) >= 2:
            continue

        # get number
        filelist = ' '.join(filelist)
        outfile = opj(tscan_dir, 'bs.{}.{}.tif'.format(polar, metric))
        check_file = opj(
                os.path.dirname(outfile),
                '.{}.processed'.format(os.path.basename(outfile)[:-4])
        )

        if os.path.isfile(check_file):
            logger.info('Mosaic layer {} already '
                  ' processed.'.format(os.path.basename(outfile)))
            continue

        logger.info('Mosaicking layer {}.'.format(os.path.basename(outfile)))
        mosaic.mosaic(filelist, outfile, temp_dir, cut_to_aoi)
        outfiles.append(outfile)

    if exec_file:
        print(' gdalbuildvrt ....command, outfiles')
    else:
        ras.create_tscan_vrt(tscan_dir, proc_file)
