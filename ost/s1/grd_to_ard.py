import os
import logging
import glob
import shutil
import time
import rasterio
import numpy as np
import gdal
from retry import retry

from os.path import join as opj

from ost.generic import common_wrappers as common
from ost.helpers import helpers as h, raster as ras
from ost.helpers.vector import ls_to_vector
from ost.helpers.errors import GPTRuntimeError
from ost.helpers.settings import GPT_FILE, OST_ROOT

logger = logging.getLogger(__name__)


def grd_to_ard(filelist,
               output_dir, 
               file_id, 
               temp_dir, 
               ard_params,
               subset=None,
               gpt_max_workers=os.cpu_count()
               ):
    '''The main function for the grd to ard generation

    This function represents the full workflow for the generation of an
    Analysis-Ready-Data product. The standard parameters reflect the CEOS
    ARD defintion for Sentinel-1 backcsatter products.

    By changing the parameters, taking care of all parameters
    that can be given. The function can handle multiple inputs of the same
    acquisition, given that there are consecutive data takes.

    Args:
        filelist (list): must be a list with one or more absolute
                  paths to GRD scene(s)
        output_dir: os.path object or string for the folder
                    where the output file should be written#
        file_id (str): prefix of the final output file
        temp_dir:
        resolution: the resolution of the output product in meters
        ls_mask: layover/shadow mask generation (Boolean)
        speckle_filter: speckle filtering (Boolean)

    Returns:
        nothing

    Notes:
        no explicit return value, since output file is our actual return
    '''

    ard = ard_params['single_ARD']
    polars = ard['polarisation'].replace(' ', '')

    # ---------------------------------------------------------------------
    # 1 Import
    
    # slice assembly if more than one scene
    if len(filelist) > 1:
        for file in filelist:
            grd_import = opj(temp_dir, '{}_imported'.format(
                os.path.basename(file)[:-5]))
            logfile = opj(output_dir, '{}.Import.errLog'.format(
                os.path.basename(file)[:-5]))
            
            return_code = _grd_frame_import(file, grd_import, logfile, polars)
            if return_code != 0:
                h.delete_dimap(grd_import)
                raise GPTRuntimeError(
                    'Something went wrong with slice assembly/importing'
                )

        # create list of scenes for full acquisition in
        # preparation of slice assembly
        pre_slice_imports = glob.glob(opj(temp_dir, '*imported.dim'))
        scenelist = ' '.join(glob.glob(opj(temp_dir, '*imported.dim')))

        # create file strings
        grd_import = opj(temp_dir, '{}_imported'.format(file_id))
        logfile = opj(output_dir, '{}._slice_assembly.errLog'.format(file_id))
        return_code = _slice_assembly(scenelist, grd_import, logfile)
        
        # delete inputs
        for file in pre_slice_imports:
            h.delete_dimap(file.replace('.dim', ''))
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(grd_import)
            raise GPTRuntimeError(
                'Something went wrong with slice assembly/importing'
            )

        # subset mode after slice assembly
        if subset:
            grd_subset = opj(temp_dir, '{}_imported_subset'.format(file_id))
            return_code = _grd_subset_georegion('{}.dim'.format(grd_import), 
                                                grd_subset, logfile, subset)

            # delete slice assembly input to subset
            h.delete_dimap(grd_import)
            
            # delete output if command failed for some reason and return
            if return_code != 0:
                h.delete_dimap(grd_subset)
                raise GPTRuntimeError('Something went wrong when subsetting')
            
    # single scene case
    else:
        grd_import = opj(temp_dir, '{}_imported'.format(file_id))
        logfile = opj(output_dir, '{}.Import.errLog'.format(file_id))

        if subset is None:
            return_code = _grd_frame_import(filelist[0], grd_import, logfile, 
                                            polars)
        else:
            return_code = _grd_frame_import_subset(filelist[0], grd_import, 
                                                   subset, logfile, 
                                                   polars)
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(grd_import)
            raise GPTRuntimeError('Something went wrong when importing')
    
    # ---------------------------------------------------------------------
    # 2 GRD Border Noise
    if ard['remove_border_noise'] and not subset:
        for polarisation in ['VV', 'VH', 'HH', 'HV']:
            infile = glob.glob(opj(
                    temp_dir, '{}_imported*data'.format(file_id),
                    'Intensity_{}.img'.format(polarisation)))

            if len(infile) == 1:
                # run grd Border Remove
                logger.info('Remove border noise for {} band.'.format(
                    polarisation))
                _grd_remove_border(infile[0])

    # set input for next step
    if os.path.isfile(opj(temp_dir, '{}_imported.dim'.format(file_id))) or \
            os.path.isfile(opj(temp_dir, '{}_imported_subset.dim'.format(file_id))):
        infile = glob.glob(opj(temp_dir, '{}_imported*dim'.format(file_id)))[0]
    else:
        logger.info('%s is an empty product', file_id)
        return return_code, None, None
    
    # ---------------------------------------------------------------------
    # 3 Calibration
    if ard['product_type'] == 'GTC-sigma0':
        calibrate_to = 'sigma0'
    elif ard['product_type'] == 'GTC-gamma0':
        calibrate_to = 'gamma0'
    elif ard['product_type'] == 'RTC-gamma0':
        calibrate_to = 'beta0'
    else:
        raise TypeError('Wrong ARD product Type for GRD processing')
       
    calibrated = opj(temp_dir, '{}_cal'.format(file_id))
    logfile = opj(output_dir, '{}.Calibration.errLog'.format(file_id))
    return_code = common.calibration(
        infile,
        calibrated,
        logfile,
        calibrate_to,
        gpt_max_workers=gpt_max_workers
    )
    
    # delete input
    h.delete_dimap(infile[:-4])
    
    # delete output if command failed for some reason and return
    if return_code != 0:
        h.delete_dimap(calibrated)
        raise GPTRuntimeError('Something went wrong when calibrating')
    
    # input for next step
    infile = '{}.dim'.format(calibrated)
    
    # ---------------------------------------------------------------------
    # 4 Multi-looking
    if int(ard['resolution']) >= 20:
        # calculate the multi-look factor
        ml_factor = int(int(ard['resolution']) / 10)
        
        multi_looked = opj(temp_dir, '{}_ml'.format(file_id))
        logfile = opj(output_dir, '{}.multilook.errLog'.format(file_id))
        return_code = common.multi_look(
            infile,
            multi_looked,
            logfile,
            ml_factor,
            ml_factor,
            gpt_max_workers
        )

        # delete input
        h.delete_dimap(infile[:-4])
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(multi_looked)
            raise GPTRuntimeError(
                'Something went wrong with multilooking {}'.format(return_code)
            )
            
        # define input for next step
        infile = '{}.dim'.format(multi_looked)
    
    # ---------------------------------------------------------------------
    # 5 Layover shadow mask
    if ard['create_ls_mask']:
        ls_mask = opj(temp_dir, '{}.ls_mask'.format(file_id))
        logfile = opj(output_dir, '{}.ls_mask.errLog'.format(file_id))
        return_code = common.ls_mask(
            infile,
            ls_mask,
            logfile,
            ard,
            gpt_max_workers
        )
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(ls_mask)
            raise GPTRuntimeError(
                'Something went wrong with ls_mask {}'.format(return_code)
            )

        # move to final destination
        out_ls_mask = opj(output_dir, '{}_LS'.format(file_id))

        # delete original file in case they exist
        if os.path.exists(str(out_ls_mask) + '.dim'):
            h.delete_dimap(out_ls_mask)

        # move out of temp
        shutil.move('{}.dim'.format(ls_mask), '{}.dim'.format(out_ls_mask))
        shutil.move('{}.data'.format(ls_mask), '{}.data'.format(out_ls_mask))
        
    # ---------------------------------------------------------------------
    # 6 Speckle filtering
    if ard['remove_speckle']:
        logfile = opj(output_dir, '{}.Speckle.errLog'.format(file_id))
        filtered = opj(temp_dir, '{}_spk'.format(file_id))

        # run processing
        return_code = common.speckle_filter(
            infile,
            filtered,
            logfile,
            ard['speckle filter'],
            gpt_max_workers
        )

        # delete input
        h.delete_dimap(infile[:-4])
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(filtered)
            raise GPTRuntimeError(
                'Something went wrong when applying single scene speckle filter'
            )
       
        # define input for next step
        infile = '{}.dim'.format(filtered)
        
    # ---------------------------------------------------------------------
    # 7 Terrain flattening
    if ard['product_type'] == 'RTC-gamma0':
        flattened = opj(temp_dir, '{}_flat'.format(file_id))
        logfile = opj(output_dir, '{}.tf.errLog'.format(file_id))
        return_code = common.terrain_flattening(
            infile,
            flattened,
            logfile,
            ard['dem'],
            gpt_max_workers
        )

        # delete input file
        h.delete_dimap(infile[:-4])
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(flattened)
            raise GPTRuntimeError('Something went wrong with TF')
        
        # define input for next step
        infile = '{}.dim'.format(flattened)

    # ---------------------------------------------------------------------
    # 8 Linear to db
    if ard['to_db']:
        db_scaled = opj(temp_dir, '{}_db'.format(file_id))
        logfile = opj(output_dir, '{}.db.errLog'.format(file_id))
        return_code = common.linear_to_db(infile, db_scaled, logfile)
        
        # delete input file
        h.delete_dimap(infile[:-4])
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(db_scaled)
            raise GPTRuntimeError('Something went wrong when converting to DB')
        
        # set input for next step
        infile = '{}.dim'.format(db_scaled)

    # ---------------------------------------------------------------------
    # 9 Geocoding
    geocoded = opj(temp_dir, '{}_bs'.format(file_id))
    logfile = opj(output_dir, '{}_bs.errLog'.format(file_id))
    return_code = common.terrain_correction(
        infile=infile,
        outfile=geocoded,
        logfile=logfile,
        resolution=ard['resolution'],
        dem_dict=ard['dem'],
        gpt_max_workers=gpt_max_workers
    )
    
    # delete input file
    h.delete_dimap(infile[:-4])
    
    # delete output if command failed for some reason and return
    if return_code != 0:
        h.delete_dimap(geocoded)
        raise GPTRuntimeError('Something went wrong when geocoding')

    # define final destination
    out_final = opj(output_dir, '{}_BS'.format(file_id))

    # ---------------------------------------------------------------------
    # 10 Checks and move to output directory
    # remove output file if exists
    if os.path.exists(out_final + '.dim'):
        h.delete_dimap(out_final)   
    
    # check final output
    # return_code = h.check_out_dimap(geocoded)
    # if return_code != 0:
    #     h.delete_dimap(geocoded)
    #     raise GPTRuntimeError('Something wrong with the GPT output')
    
    # move to final destination
    shutil.move('{}.dim'.format(geocoded), '{}.dim'.format(out_final))
    shutil.move('{}.data'.format(geocoded), '{}.data'.format(out_final))

    # write processed file to keep track of files already processed
    with open(opj(output_dir, '.processed'), 'w') as file:
        file.write('passed all tests \n')

    # Return colected files that have been processed
    if ard['create_ls_mask']:
        out_final_ls_mask = out_ls_mask + '.dim'
        out_ls_mask = ls_to_vector(infile=out_final_ls_mask, driver='GPKG')
        if out_ls_mask is None:
            h.delete_dimap(out_final_ls_mask.replace('.dim', ''))
    else:
        out_ls_mask = None

    return return_code, out_final + '.dim', out_ls_mask


@retry(tries=3, delay=1, logger=logger)
def _grd_frame_import(infile, outfile, logfile, polarisation='VV,VH,HH,HV'):
    '''A wrapper of SNAP import of a single Sentinel-1 GRD product

    This function takes an original Sentinel-1 scene (either zip or
    SAFE format), updates the orbit information (does not fail if not
    available), removes the thermal noise and stores it as a SNAP
    compatible BEAM-Dimap format.

    Args:
        infile: string or os.path object for
                an original Sentinel-1 GRD product in zip or SAFE format
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        polarisation (str): a string consisiting of the polarisation (comma separated)
                     e.g. 'VV,VH',
                     default value: 'VV,VH,HH,HV'
    '''

    logger.info('Importing {} by applying precise orbit file and'
                ' removing thermal noise'.format(
        os.path.basename(infile)))  # get path to ost graph
    graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '1_AO_TNR.xml')

    # construct command
    command = '{} {} -x -q {} -Pinput=\'{}\' -Ppolarisation={} \
               -Poutput=\'{}\''.format(
        GPT_FILE, graph, 2 * os.cpu_count(), infile, polarisation, outfile)

    # run command
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully imported product')
        return return_code
    else:
        # read logfile
        raise GPTRuntimeError('ERROR: Frame import exited with an error {}. \
                See {} for Snap Error output'.format(return_code, logfile)
                              )


@retry(tries=3, delay=1, logger=logger)
def _grd_frame_import_subset(infile,
                             outfile,
                             georegion,
                             logfile,
                             polarisation='VV,VH,HH,HV'
                             ):
    '''A wrapper of SNAP import of a subset of single Sentinel-1 GRD product

    This function takes an original Sentinel-1 scene (either zip or
    SAFE format), updates the orbit information (does not fail if not
    available), removes the thermal noise, subsets it to the given georegion
    and stores it as a SNAP
    compatible EAM-Dimap format.


    Args:
        infile: string or os.path object for
                an original Sentinel-1 GRD product in zip or SAFE format
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        polarisation (str): a string consisiting of the polarisation (comma separated)
                     e.g. 'VV,VH',
                     default value: 'VV,VH,HH,HV'
        georegion (str): a WKT style formatted POLYGON that bounds the
                         subset region
    '''

    logger.info('Importing {} by applying precise orbit file and'
                ' removing thermal noise, as well as subsetting.'.format(
        os.path.basename(infile)))

    # get path to graph
    graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '1_AO_TNR_SUB.xml')

    # construct command
    command = '{} {} -x -q {} -Pinput=\'{}\' -Pregion=\'{}\' -Ppolarisation={} \
                      -Poutput=\'{}\''.format(
        GPT_FILE, graph, 2 * os.cpu_count(),
        infile, georegion, polarisation, outfile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully imported product')
        return return_code
    else:
        raise GPTRuntimeError('ERROR: Frame import exited with an error {}. \
                See {} for Snap Error output'.format(return_code, logfile)
                              )


@retry(tries=3, delay=1, logger=logger)
def _slice_assembly(filelist, outfile, logfile, polarisation='VV,VH,HH,HV'):
    '''A wrapper of SNAP's slice assembly routine

    This function assembles consecutive frames acquired at the same date.
    Can be either GRD or SLC products

    Args:
        filelist (str): a string of a space separated list of OST imported
                        Sentinel-1 product slices to be assembled
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
    '''

    logger.info('Assembling consecutive frames:')

    # construct command
    command = '{} SliceAssembly -x -q {} -PselectedPolarisations={} \
               -t \'{}\' {}'.format(
        GPT_FILE, 2 * os.cpu_count(), polarisation, outfile, filelist)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully assembled products')
        return return_code
    else:
        raise GPTRuntimeError(
            'ERROR: Slice Assembly exited with an error {}. '
            'See {} for Snap Error output'.format(return_code, logfile)
        )


@retry(tries=3, delay=1, logger=logger)
def _grd_subset(infile, outfile, logfile, region):
    '''A wrapper around SNAP's subset routine

    This function takes an OST imported frame and subsets it according to
    the coordinates given in the region

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        region (str): a list of image coordinates that bound the subset region
    '''

    # format region string
    region = ','.join([str(int(x)) for x in region])

    # construct command
    command = '{} Subset -x -q {} -Pregion={} -t \'{}\' \'{}\''.format(
        GPT_FILE, 2 * os.cpu_count(), region, outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully subsetted product')
        return return_code
    else:
        raise GPTRuntimeError('ERROR: Subsetting exited with an error {}.  \
                See {} for Snap Error output'.format(return_code, logfile)
                              )


@retry(tries=3, delay=1, logger=logger)
def _grd_subset_georegion(infile, outfile, logfile, georegion):
    '''A wrapper around SNAP's subset routine

    This function takes an OST imported frame and subsets it according to
    the coordinates given in the region

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        georegion (str): a WKT style formatted POLYGON that bounds the
                   subset region
    '''

    logger.info('Subsetting imported imagery.')

    # extract window from scene
    command = '{} Subset -x -q {} -Ssource=\'{}\' -t \'{}\' \
                 -PcopyMetadata=true -PgeoRegion=\'{}\''.format(
        GPT_FILE, 2 * os.cpu_count(), infile, outfile, georegion)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully subsetted product.')
        return return_code
    else:
        raise GPTRuntimeError(
            'ERROR: Subsetting exited with an error {}.  See {} for Snap '
            'Error output'.format(return_code, logfile)
        )


@retry(tries=3, delay=1, logger=logger)
def _grd_remove_border(infile):
    '''An OST function to remove GRD border noise from Sentinel-1 data

    This is a custom routine to remove GRD border noise
    from Sentinel-1 GRD products. It works on the original intensity
    images.

    NOTE: For the common dimap format, the infile needs to be the
    ENVI style file inside the *data folder.

    The routine checks the outer 3000 columns for its mean value.
    If the mean value is below 100, all values will be set to 0,
    otherwise the routine will continue fpr another 150 columns setting
    the value to 0. All further columns towards the inner image are
    considered valid.

    Args:
        infile: string or os.path object for a
                gdal compatible intensity file of Sentinel-1

    Notes:
        The file will be manipulated inplace, meaning,
        no new outfile is created.
    '''

    # logger.info('Removing the GRD Border Noise.')
    currtime = time.time()

    # read raster file and get number of columns adn rows
    raster = gdal.Open(infile, gdal.GA_Update)
    cols = raster.RasterXSize
    rows = raster.RasterYSize

    # create 3000xrows array for the left part of the image
    array_left = np.array(raster.GetRasterBand(1).ReadAsArray(0,
                                                              0, 3000, rows))

    for x in range(3000):
        # condition if more than 50 pixels within the line have values
        # less than 500, delete the line
        # if np.sum(np.where((array_left[:,x] < 200)
        # & (array_left[:,x] > 0) , 1, 0)) <= 50:
        if np.mean(array_left[:, x]) <= 100:
            array_left[:, x].fill(0)
        else:
            z = x + 150
            if z > 3000:
                z = 3000
            for y in range(x, z, 1):
                array_left[:, y].fill(0)

            cols_left = y
            break

    try:
        cols_left
    except NameError:
        cols_left = 3000

    # write array_left to disk
    # logger.info('Total amount of columns: {}'.format(cols_left))
    # logger.info('Number of colums set to 0 on the left side: '
    #     ' {}'.format(cols_left))
    # raster.GetRasterBand(1).WriteArray(array_left[:, :+cols_left], 0, 0, 1)
    raster.GetRasterBand(1).WriteArray(array_left[:, :+cols_left], 0, 0)

    array_left = None

    # create 2d array for the right part of the image (3000 columns and rows)
    cols_last = cols - 3000
    array_right = np.array(raster.GetRasterBand(1).ReadAsArray(cols_last,
                                                               0, 3000, rows))

    # loop through the array_right columns in opposite direction
    for x in range(2999, 0, -1):

        if np.mean(array_right[:, x]) <= 100:
            array_right[:, x].fill(0)
        else:
            z = x - 150
            if z < 0:
                z = 0
            for y in range(x, z, -1):
                array_right[:, y].fill(0)

            cols_right = y
            break

    try:
        cols_right
    except NameError:
        cols_right = 0

    #
    col_right_start = cols - 3000 + cols_right
    # logger.info('Number of columns set to 0 on the'
    #     ' right side: {}'.format(3000 - cols_right))
    # logger.info('Amount of columns kept: {}'.format(col_right_start))
    raster.GetRasterBand(1).WriteArray(array_right[:, cols_right:],
                                       col_right_start, 0)
    array_right = None
    h.timer(currtime)