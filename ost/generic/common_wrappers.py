import os
import logging
from retry import retry
from os.path import join as opj
from pathlib import Path

from ost.helpers import helpers as h
from ost.helpers.settings import GPT_FILE, OST_ROOT
from ost.helpers.errors import GPTRuntimeError

logger = logging.getLogger(__name__)


@retry(tries=3, delay=1, logger=logger)
def calibration(infile, outfile, logfile, calibrate_to, gpt_max_workers=os.cpu_count()):
    # transform calibration parameter to snap readable
    sigma0, beta0, gamma0 = 'false', 'false', 'false'

    if calibrate_to is 'gamma0':
        gamma0 = 'true'
    elif calibrate_to is 'beta0':
        beta0 = 'true'
    elif calibrate_to is 'sigma0':
        sigma0 = 'true'

    logger.info('Calibrating the product to {}.'.format(calibrate_to))
    # contrcut command string

    command = ('{} Calibration -x -q {}'
               ' -PoutputBetaBand=\'{}\''
               ' -PoutputGammaBand=\'{}\''
               ' -PoutputSigmaBand=\'{}\''
               ' -t \'{}\' \'{}\''.format(
        GPT_FILE, 2 * gpt_max_workers,
        beta0, gamma0, sigma0,
        outfile, infile
    )
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # hadle errors and logs
    if return_code == 0:
        logger.info('Calibration to {} successful.'.format(calibrate_to))
    else:
        print(' ERROR: Calibration exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


@retry(tries=3, delay=1, logger=logger)
def multi_look(infile, outfile, logfile, rg_looks, az_looks, gpt_max_workers=os.cpu_count()):
    logger.info('Multi-looking the image with {} looks in'
                ' azimuth and {} looks in range.'.format(az_looks, rg_looks)
                )
    # construct command string
    command = ('{} Multilook -x -q {}'
               ' -PnAzLooks={}'
               ' -PnRgLooks={}'
               ' -t \'{}\' {}'.format(
        GPT_FILE, 2 * gpt_max_workers,
        az_looks, rg_looks,
        outfile, infile
    )
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully multi-looked product.')
    else:
        print(' ERROR: Multi-look exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


@retry(tries=3, delay=1, logger=logger)
def speckle_filter(infile, outfile, logfile, speckle_dict,
                   gpt_max_workers=os.cpu_count()):
    """ Wrapper around SNAP's peckle Filter function

    This function takes OST imported Sentinel-1 product and applies
    a standardised version of the Lee-Sigma Speckle Filter with
    SNAP's defaut values.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        gpt_max_workers (int): number of cpus used - useful for parallel processing
    """

    logger.info('Applying speckle filtering.')
    # contrcut command string
    command = (
        '{} Speckle-Filter -x -q {}'
        ' -PestimateENL=\'{}\''
        ' -PanSize=\'{}\''
        ' -PdampingFactor=\'{}\''
        ' -Penl=\'{}\''
        ' -Pfilter=\'{}\''
        ' -PfilterSizeX=\'{}\''
        ' -PfilterSizeY=\'{}\''
        ' -PnumLooksStr=\'{}\''
        ' -PsigmaStr=\'{}\''
        ' -PtargetWindowSizeStr=\"{}\"'
        ' -PwindowSize=\"{}\"'
        ' -t \'{}\' \'{}\''.format(
              GPT_FILE, 2*gpt_max_workers,
              speckle_dict['estimate_ENL'],
              speckle_dict['pan_size'],
              speckle_dict['damping'],
              speckle_dict['ENL'],
              speckle_dict['filter'],
              speckle_dict['filter_x_size'],
              speckle_dict['filter_y_size'],
              speckle_dict['num_of_looks'],
              speckle_dict['sigma'],
              speckle_dict['target_window_size'],
              speckle_dict['window_size'],
              outfile, infile)
              )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # hadle errors and logs
    if return_code == 0:
        logger.info('Successfully applied speckle filtering.')
        return return_code
    else:
        raise GPTRuntimeError(
            'ERROR: Speckle filtering exited with an error {}. See {} for '
            'Snap Error output'.format(return_code, logfile)
        )


@retry(tries=3, delay=1, logger=logger)
def linear_to_db(infile, outfile, logfile, gpt_max_workers=os.cpu_count()):
    """A wrapper around SNAP's linear to db routine

    This function takes an OST calibrated Sentinel-1 product
    and converts it to dB.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        gpt_max_workers (int): number of cpus used - useful for parallel processing
    """

    logger.info('Converting the image to dB-scale.')
    # construct command string
    command = (
        f'{GPT_FILE} LinearToFromdB -x -q {2*gpt_max_workers} -t \'{outfile}\' {infile}'
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully converted product to dB-scale.')
        return return_code
    else:
        raise GPTRuntimeError(
            'ERROR: dB Scaling exited with an error {}. See {} for '
            'Snap Error output'.format(return_code, logfile)
        )


def terrain_flattening(
            infile,
            outfile,
            logfile,
            dem_dict,
            gpt_max_workers=os.cpu_count()
):
    command = (
        '{} Terrain-Flattening -c 256M -q {}'
        ' -PdemName=\'{}\''
        ' -PdemResamplingMethod=\'{}\''
        ' -PexternalDEMFile=\'{}\''
        ' -PexternalDEMNoDataValue={}'
        ' -PadditionalOverlap=0.1'
        ' -PoversamplingMultiple=1.5'
        ' -t \'{}\' \'{}\''.format(
            GPT_FILE,
            2 * gpt_max_workers,
            dem_dict['dem_name'],
            dem_dict['dem_resampling'],
            dem_dict['dem_file'],
            dem_dict['dem_nodata'],
            outfile,
            infile
        )
    )
    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully terrain flattened product')
        return return_code
    else:
        raise GPTRuntimeError(
            'ERROR: Terrain Flattening exited with an error {}. See {} for '
            'Snap Error output'.format(return_code, logfile)
        )


@retry(tries=3, delay=1, logger=logger)
def terrain_correction(infile,
                       outfile,
                       logfile,
                       resolution,
                       dem_dict,
                       gpt_max_workers=os.cpu_count()
                       ):
    """
    A wrapper around SNAP's Terrain Correction routine
    This function takes an OST calibrated Sentinel-1 product and
    does the geocodification.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        resolution (int): the resolution of the output product in meters
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'
        gpt_max_workers (int): number of cpus used - useful for parallel processing

    """

    command = (
        '{} Terrain-Correction -x -q {}'
        ' -PdemName=\'{}\''
        ' -PdemResamplingMethod=\'{}\''
        ' -PexternalDEMFile=\'{}\''
        ' -PexternalDEMNoDataValue={}'
        ' -PexternalDEMApplyEGM=\'{}\''
        ' -PimgResamplingMethod=\'{}\''
        ' -PpixelSpacingInMeter={}'
        ' -t \'{}\' \'{}\''.format(
            GPT_FILE,
            2 * gpt_max_workers,
            dem_dict['dem_name'],
            dem_dict['dem_resampling'],
            dem_dict['dem_file'],
            dem_dict['dem_nodata'],
            str(dem_dict['egm_correction']).lower(),
            dem_dict['image_resampling'],
            resolution,
            outfile,
            infile
        )
    )
    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully terrain corrected product')
        return return_code
    else:
        raise GPTRuntimeError(
            'ERROR: Terrain Correction exited with an error {}. See {} for '
            'Snap Error output'.format(return_code, logfile)
        )


@retry(tries=3, delay=1, logger=logger)
def ls_mask(infile, outfile, logfile, ard, gpt_max_workers=os.cpu_count()):
    """

    :param infile:
    :param outfile:
    :param logfile:
    :param ard:
    :param gpt_max_workers:
    :return:
    """
    logger.info('Creating the Layover/Shadow mask')
    # get path to workflow xml
    graph = OST_ROOT.joinpath('graphs/S1_GRD2ARD/3_LSmap.xml')
    dem_dict = ard['dem']

    # ls_mask acceleration, image resampling nearest and 2x resolution
    image_resampling = 'NEAREST_NEIGHBOUR'
    image_resolution = ard["resolution"]*2

    command = (
        f'{GPT_FILE} {graph} -x -q {2 * gpt_max_workers} '
        f'-Pinput=\'{infile}\' '
        f'-Presol={image_resolution} '
        f'-Pdem=\'{dem_dict["dem_name"]}\' '
        f'-Pdem_file=\'{dem_dict["dem_file"]}\' '
        f'-Pdem_nodata=\'{dem_dict["dem_nodata"]}\' '
        f'-Pdem_resampling=\'{dem_dict["dem_resampling"]}\' '
        f'-Pimage_resampling=\'{image_resampling}\' '
        f'-Pegm_correction=\'{str(dem_dict["egm_correction"]).lower()}\' '
        f'-Poutput=\'{outfile}\''
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.info('Succesfully created a Layover/Shadow mask')
        return return_code
    else:
        raise RuntimeError(
            f'Layover/Shadow mask creation exited with an error {return_code}.'
            f' See {logfile} for Snap Error output'
        )


@retry(tries=3, delay=1, logger=logger)
def create_stack(filelist, out_stack, logfile,
                 polarisation=None, pattern=None, gpt_max_workers=os.cpu_count()):
    '''

    :param filelist: list of single Files (space separated)
    :param outfile: the stack that is generated
    :return:
    '''

    if pattern:
        graph = opj(OST_ROOT, 'graphs', 'S1_TS', '1_BS_Stacking_HAalpha.xml')
        command = '{} {} -x -q {} -Pfilelist={} -PbandPattern=\'{}.*\' \
               -Poutput={}'.format(GPT_FILE, graph, 2*gpt_max_workers,
                                   filelist, pattern, out_stack)
    else:
        graph = opj(OST_ROOT, 'graphs', 'S1_TS', '1_BS_Stacking.xml')
        command = '{} {} -x -q {} -Pfilelist={} -Ppol={} \
               -Poutput={}'.format(GPT_FILE, graph, 2*gpt_max_workers,
                                   filelist, polarisation, out_stack)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Successfully created multi-temporal stack')
    else:
        raise GPTRuntimeError(
            'Multi-temporal Spackle Filter exited with an error {}. '
            'See {} for Snap Error output'.format(return_code, logfile)
        )

    return return_code


@retry(tries=3, delay=1, logger=logger)
def mt_speckle_filter(
        in_stack,
        out_stack,
        logfile,
        speckle_dict,
        gpt_max_workers=os.cpu_count()
):
    logger.info('Applying multi-temporal speckle filtering.')
    command = (
        '{} Multi-Temporal-Speckle-Filter -x -q {}'
        ' -PestimateENL={}'
        ' -PanSize={}'
        ' -PdampingFactor={}'
        ' -Penl={}'
        ' -Pfilter=\'{}\''
        ' -PfilterSizeX={}'
        ' -PfilterSizeY={}'
        ' -PnumLooksStr={}'
        ' -PsigmaStr={}'
        ' -PtargetWindowSizeStr={}'
        ' -PwindowSize={}'
        ' -t \'{}\' \'{}\''.format(
            GPT_FILE, 2 * gpt_max_workers,
            speckle_dict['estimate_ENL'],
            speckle_dict['pan_size'],
            speckle_dict['damping'],
            speckle_dict['ENL'],
            speckle_dict['filter'],
            speckle_dict['filter_x_size'],
            speckle_dict['filter_y_size'],
            speckle_dict['num_of_looks'],
            speckle_dict['sigma'],
            speckle_dict['target_window_size'],
            speckle_dict['window_size'],
            out_stack, in_stack
        )
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Successfully applied multi-temporal speckle filtering')
        return return_code
    else:
        raise GPTRuntimeError(
            'Multi-temporal Spackle Filter exited with an error {}. '
            'See {} for Snap Error output'.format(return_code, logfile)
        )
