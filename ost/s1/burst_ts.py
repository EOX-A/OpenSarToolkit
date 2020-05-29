import os
import json
import itertools
import logging
from pathlib import Path

from ost.helpers import raster as ras
from ost.generic import ard_to_ts, ts_extent, ts_ls_mask, timescan, mosaic

logger = logging.getLogger(__name__)

# ---------------------------------------------------
# Global variable
PRODUCT_LIST = [
    'bs_HH', 'bs_VV', 'bs_HV', 'bs_VH',
    'coh_VV', 'coh_VH', 'coh_HH', 'coh_HV',
    'pol_Entropy', 'pol_Anisotropy', 'pol_Alpha'
]


def _create_extents(burst_gdf, config_dict):
    processing_dir = Path(config_dict['processing_dir'])
    temp_dir = Path(config_dict['temp_dir'])

    # create extent iterable
    for burst in burst_gdf.bid.unique():  # ***

        # get the burst directory
        burst_dir = processing_dir.joinpath(burst)
        if _burstdir_is_empty(burst_dir):
            continue

        # get common burst extent
        list_of_bursts = list(burst_dir.glob('**/*img'))
        list_of_bursts = [
            str(x) for x in list_of_bursts if 'layover' not in str(x)
        ]
        extent = burst_dir.joinpath(f'{burst}.extent.gpkg')

        ts_extent.mt_extent(
            list_of_scenes=list_of_bursts,
            out_file=extent,
            temp_dir=temp_dir,
            buffer=-0.0018
        )


def _create_mt_ls_mask(burst_gdf, config_dict):
    processing_dir = config_dict['processing_dir']
    temp_dir = config_dict['temp_dir']

    # create layover
    for burst in burst_gdf.bid.unique():  # ***

        # get the burst directory
        burst_dir = Path(processing_dir).joinpath(burst)
        if _burstdir_is_empty(burst_dir):
            continue

        # get layover scenes
        list_of_scenes = list(burst_dir.glob('20*/*data*/*img'))
        list_of_layover = [
            str(x) for x in list_of_scenes if 'layover' in str(x)
            ]

        # we need to redefine the namespace of the already created extents
        extent = burst_dir.joinpath(f'{burst}.extent.gpkg')
        if not extent.exists():
            raise FileNotFoundError(
                f'extent file for burst {burst} not found.'
            )

        # layover/shadow mask
        out_ls = burst_dir.joinpath(f'{burst}.ls_mask.tif')

        ts_ls_mask.mt_layover(
            filelist=list_of_layover,
            outfile=out_ls,
            temp_dir=temp_dir,
            extent=str(extent),
            )


def _create_timeseries(burst_gdf, config_dict):

    # we need a
    dict_of_product_types = {'bs': 'Gamma0', 'coh': 'coh', 'pol': 'pol'}
    pols = ['VV', 'VH', 'HH', 'HV', 'Alpha', 'Entropy', 'Anisotropy']

    processing_dir = config_dict['processing_dir']

    for burst in burst_gdf.bid.unique():
        burst_dir = Path(processing_dir).joinpath(burst)
        if _burstdir_is_empty(burst_dir):
            continue

        for pr, pol in itertools.product(dict_of_product_types.items(), pols):
            # unpack items
            product, product_name = list(pr)

            # take care of H-A-Alpha naming for file search
            if pol in ['Alpha', 'Entropy', 'Anisotropy'] and product is 'pol':
                list_of_files = sorted(
                    list(burst_dir.glob(f'20*/*data*/*{pol}*img')))
            else:
                # see if there is actually any imagery for this
                # combination of product and polarisation
                list_of_files = sorted(
                    list(burst_dir.glob(
                        f'20*/*data*/*{product_name}*{pol}*img')
                    )
                )

            if len(list_of_files) == 0:
                continue

            # create list of dims if polarisation is present
            list_of_dims = sorted(list(burst_dir.glob(f'20*/*{product}*dim')))

            ard_to_ts.ard_to_ts(
                list_of_files=list_of_dims,
                product=product,
                pol=pol,
                config_dict=config_dict,
                burst=burst,
                track=None,
            )


def ards_to_timeseries(burst_gdf, config_dict):

    logger.info('Processing all burst ARDs time-series')

    ard = config_dict['processing']['single_ARD']
    ard_mt = config_dict['processing']['time-series_ARD']

    # create all extents
    _create_extents(burst_gdf, config_dict)

    # update extents in case of ls_mask
    if ard['create_ls_mask']:
        logger.warning('LS mask in Timeseries currently under cosntruction!!')
        # _create_mt_ls_mask(burst_gdf, config_dict)

    # finally create time-series
    _create_timeseries(burst_gdf, config_dict)


# --------------------
# timescan part
# --------------------
def timeseries_to_timescan(burst_gdf, config_dict):
    """Function to create a timescan out of a OST timeseries.

    """
    logger.info('Processing all burst ARDs time-series to ARD timescans')

    # -------------------------------------
    # 1 load project config
    processing_dir = config_dict['processing_dir']
    ard = config_dict['processing']['single_ARD']
    ard_mt = config_dict['processing']['time-series_ARD']
    ard_tscan = config_dict['processing']['time-scan_ARD']

    # get the db scaling right
    if ard['to_db'] or ard_mt['to_db']:
        to_db = True

    # get datatype right
    dtype_conversion = True if ard_mt['dtype_output'] != 'float32' else False

    # -------------------------------------
    # 2 create iterable for parallel processing
    for burst in burst_gdf.bid.unique():
        logger.info(burst)

        # get relevant directories
        burst_dir = Path(processing_dir).joinpath(burst)
        if _burstdir_is_empty(burst_dir):
            continue
        timescan_dir = burst_dir.joinpath('Timescan')
        timescan_dir.mkdir(parents=True, exist_ok=True)
        
        for product in PRODUCT_LIST:
            # check if already processed
            if timescan_dir.joinpath(f'.{product}.processed').exists():
                logger.info(f'Timescans for burst {burst} already processed.')
                continue

            # get respective timeseries
            timeseries = burst_dir.joinpath(
                f'Timeseries/Timeseries_{product}.vrt'
            )
            
            # che if this timsereis exists ( since we go through all products
            if not timeseries.exists():
                continue

            # datelist for harmonics
            scenelist = list(burst_dir.glob(f'Timeseries/*{product}*tif'))
            datelist = [
                file.name.split('.')[1][:6] for file in sorted(scenelist)
            ]

            # define timescan prefix
            timescan_prefix = timescan_dir.joinpath(product)

            # get rescaling and db right (backscatter vs. coh/pol)
            if 'bs.' in str(timescan_prefix):
                to_power, rescale = to_db, dtype_conversion
            else:
                to_power, rescale = False, False

            timescan.mt_metrics(
                stack=timeseries,
                out_prefix=timescan_prefix,
                metrics=ard_tscan['metrics'],
                datelist=datelist,
                rescale_to_datatype=rescale,
                to_power=to_power,
                outlier_removal=ard_tscan['remove_outliers'],
            )

        ras.create_tscan_vrt(timescan_dir=timescan_dir,
                             config_dict=config_dict
                             )


def mosaic_timeseries(burst_inventory, project_file):

    logger.info('Mosaicking time-series layers.')
    # -------------------------------------
    # 1 load project config
    with open(project_file, 'r') as ard_file:
        project_params = json.load(ard_file)
        processing_dir = project_params['project']['processing_dir']

    # create output folder
    ts_dir = Path(processing_dir).joinpath('Mosaic/Timeseries')
    ts_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------------------
    # 2 create iterable
    # loop through each product
    iter_list, vrt_iter_list = [], []
    for product in PRODUCT_LIST:

        #
        bursts = burst_inventory.bid.unique()
        nr_of_ts = len(list(
            Path(processing_dir).glob(
                f'{bursts[0]}/Timeseries/*.{product}.tif'
            )
        ))

        # in case we only have one layer
        if not nr_of_ts > 1:
            continue

        outfiles = []
        for i in range(1, nr_of_ts + 1):

            # create the file list for files to mosaic
            filelist = list(Path(processing_dir).glob(
                f'*/Timeseries/{i:02d}.*{product}.tif'
            ))

            # assure that we do not inlcude potential Mosaics
            # from anterior runs
            filelist = [file for file in filelist if 'Mosaic' not in str(file)]

            logger.info(f'Creating timeseries mosaic {i} for {product}.')

            # create dates for timseries naming
            datelist = []
            for file in filelist:
                if '.coh.' in str(file):
                    datelist.append(
                        f"{file.name.split('.')[2]}_{file.name.split('.')[1]}"
                    )
                else:
                    datelist.append(file.name.split('.')[1])

            # get start and endate of mosaic
            start, end = sorted(datelist)[0], sorted(datelist)[-1]
            filelist = ' '.join([str(file) for file in filelist])

            # create namespace for output file
            if start == end:
                outfile = ts_dir.joinpath(
                              f'{i:02d}.{start}.{product}.tif'
                )

            else:
                outfile = ts_dir.joinpath(
                              f'{i:02d}.{start}-{end}.{product}.tif'
                )

            # create nmespace for check_file
            check_file = outfile.parent.joinpath(
                f'.{outfile.name[:-4]}.processed'
            )

            if os.path.isfile(check_file):
                print('INFO: Mosaic layer {} already'
                      ' processed.'.format(outfile))
                continue

            # append to list of outfile for vrt creation
            outfiles.append(outfile)
            iter_list.append([filelist, outfile, project_file])

        vrt_iter_list.append([ts_dir, product, outfiles])

    mosaic.mosaic(iter_list)
    mosaic.create_timeseries_mosaic_vrt(vrt_iter_list)


def mosaic_timescan(burst_inventory, config_dict):

    logger.info('Mosaicking time-scan layers.')

    processing_dir = config_dict['processing_dir']
    metrics = config_dict['processing']['time-scan_ARD']['metrics']

    if 'harmonics' in metrics:
        metrics.remove('harmonics')
        metrics.extent(['amplitude', 'phase', 'residuals'])

    if 'percentiles' in metrics:
        metrics.remove('percentiles')
        metrics.extent(['p95', 'p5'])

    tscan_dir = Path(processing_dir).joinpath('Mosaic/Timescan')
    tscan_dir.mkdir(parents=True, exist_ok=True)

    iter_list, outfiles = [], []
    for product, metric in itertools.product(PRODUCT_LIST, metrics):

        filelist = list(Path(processing_dir).glob(
            f'*/Timescan/*{product}.{metric}.tif'
        ))

        if not len(filelist) >= 1:
            continue

        filelist = ' '.join([str(file) for file in filelist])

        outfile = tscan_dir.joinpath(f'{product}.{metric}.tif')
        check_file = outfile.parent.joinpath(
            f'.{outfile.name[:-4]}.processed'
        )

        if check_file.exists():
            logger.info(f'Mosaic layer {outfile.name} already processed.')
            continue

        logger.info(f'Mosaicking layer {outfile.name}.')
        outfiles.append(outfile)
        iter_list.append([filelist, outfile, config_dict])

    mosaic.mosaic(iter_list)
    ras.create_tscan_vrt([tscan_dir, config_dict])


def _burstdir_is_empty(burst_dir):
    is_empty = False
    paths = list(Path(burst_dir).rglob('*.processed'))
    counter = 0
    for f in paths:
        if str(f).endswith('.processed'):
            with open(str(f), 'r') as pro_f:
                for line in pro_f.readlines():
                    if 'empty' in line:
                        counter += 1
    if counter == len(paths):
        is_empty = True
    return is_empty
