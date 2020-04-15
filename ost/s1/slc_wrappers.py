import os
from os.path import join as opj
import logging

from retry import retry

from ost.helpers.settings import GPT_FILE, OST_ROOT
from ost.helpers.errors import GPTRuntimeError
from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


@retry(tries=3, delay=1, logger=logger)
def burst_import(infile, outfile, logfile, swath, burst, polar='VV,VH,HH,HV',
                 gpt_max_workers=os.cpu_count()):
    """A wrapper of SNAP import of a single Sentinel-1 SLC burst

    This function takes an original Sentinel-1 scene (either zip or
    SAFE format), updates the orbit information (does not fail if not
    available), and extracts a single burst based on the
    given input parameters.

    Args:
        infile: string or os.path object for
                an original Sentinel-1 GRD product in zip or SAFE format
        outfile string or os.path object for the output
                    file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        swath (str): the corresponding IW subswath of the burst
        burst (str): the burst number as in the Sentinel-1 annotation file
        polar (str): a string consisiting of the polarisation (comma separated)
                     e.g. 'VV,VH',
                     default: 'VV,VH,HH,HV'
        gpt_max_workers(int): the number of cpu cores to allocate to the gpt job,
                default: os.cpu_count()
    """

    # get path to graph
    graph = OST_ROOT.joinpath('graphs/S1_SLC2ARD/S1_SLC_BurstSplit_AO.xml')

    logger.info(
        f'Importing Burst {burst} from Swath {swath} '
        f'from scene {os.path.basename(infile)}'
    )

    command = '{} {} -x -q {} -Pinput={} -Ppolar={} -Pswath={}\
                      -Pburst={} -Poutput={}' \
        .format(GPT_FILE, graph, 2*gpt_max_workers, infile, polar, swath,
                burst, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully imported product')
        return return_code
    else:
        raise GPTRuntimeError(
            'ERROR: Frame import exited with an error {}. See {} for '
            'Snap Error output'.format(return_code, logfile)
        )


@retry(tries=3, delay=1, logger=logger)
def ha_alpha(infile,
             outfile,
             logfile,
             pol_speckle_filter=False,
             pol_speckle_dict=None,
             gpt_max_workers=os.cpu_count()
             ):
    """A wrapper of SNAP H-A-alpha polarimetric decomposition

    This function takes an OST imported Sentinel-1 scene/burst
    and calulates the polarimetric decomposition parameters for
    the H-A-alpha decomposition.

    Args:
        infile: string or os.path object for
                an original Sentinel-1 GRD product in zip or SAFE format
        out_prefix: string or os.path object for the output
                    file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        pol_speckle_filter (bool): wether or not to apply the
                                   polarimetric speckle filter
        gpt_max_workers(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    """

    if pol_speckle_filter:
        graph = OST_ROOT.joinpath(
            'graphs/S1_SLC2ARD/S1_SLC_Deb_Spk_Halpha.xml'
        )
        logger.info('Applying the polarimetric speckle filter and'
                    ' calculating the H-alpha dual-pol decomposition')
        command = (
            f'{GPT_FILE} {graph} -x -q {2*gpt_max_workers} '
            f'-Pinput={infile} -Poutput={outfile} '
            f'-Pfilter=\'{pol_speckle_dict["polarimetric_filter"]}\' '
            f'-Pfilter_size=\'{pol_speckle_dict["filter_size"]}\' '
            f'-Pnr_looks={pol_speckle_dict["num_of_looks"]} '
            f'-Pwindow_size={pol_speckle_dict["window_size"]} '
            f'-Ptarget_window_size={pol_speckle_dict["target_window_size"]} '
            f'-Ppan_size={pol_speckle_dict["pan_size"]} '
            f'-Psigma={pol_speckle_dict["sigma"]}'
        )
    else:
        graph = OST_ROOT.joinpath(
            'graphs/S1_SLC2ARD/S1_SLC_Deb_Halpha.xml'
        )

        logger.info('Calculating the H-alpha dual polarisation')
        command = (
            f'{GPT_FILE} {graph} -x -q {2*gpt_max_workers} ' 
            f'-Pinput={infile} -Poutput={outfile}'
        )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully created H/A/Alpha product')
    else:
        raise GPTRuntimeError('ERROR: H/Alpha exited with an error {}. \
                See {} for Snap Error output'.format(return_code, logfile)
                              )


@retry(tries=3, delay=1, logger=logger)
def calibration(
        infile,
        outfile,
        logfile,
        ard,
        region='',
        gpt_max_workers=os.cpu_count()
):
    '''A wrapper around SNAP's radiometric calibration
    This function takes OST imported Sentinel-1 product and generates
    it to calibrated backscatter.
    3 different calibration modes are supported.
        - Radiometrically terrain corrected Gamma nought (RTC)
          NOTE: that the routine actually calibrates to bet0 and needs to
          be used together with _terrain_flattening routine
        - ellipsoid based Gamma nought (GTCgamma)
        - Sigma nought (GTCsigma).
    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        resolution (int): the resolution of the output product in meters
        product_type (str): the product type of the output product
                            i.e. RTC, GTCgamma or GTCsigma
    '''

    # load ards
    dem_dict = ard['dem']

    # calculate Multi-Look factors
    azimuth_looks = 1   # int(np.floor(ard['resolution'] / 10 ))
    range_looks = 5   # int(azimuth_looks * 5)

    # construct command dependent on selected product type
    if ard['product_type'] == 'RTC-gamma0':
        logger.info('Calibrating the product to a RTC product.')

        # get graph for RTC generation
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalBeta_Deb_ML_TF_Sub.xml')
        # construct command
        command = '{} {} -x -q {} ' \
                  '-Prange_looks={} -Pazimuth_looks={} ' \
                  '-Pdem=\'{}\' -Pdem_file="{}" -Pdem_nodata={} ' \
                  '-Pdem_resampling={} -Pregion="{}" ' \
                  '-Pinput="{}" -Poutput="{}"'.format(
            GPT_FILE, graph, 2*gpt_max_workers,
            range_looks, azimuth_looks,
            dem_dict['dem_name'], dem_dict['dem_file'],
            dem_dict['dem_nodata'], dem_dict['dem_resampling'],
            region, infile, outfile)

    elif ard['product_type'] == 'GTC-gamma0':

        logger.info('Calibrating the product to a GTC product (Gamma0).')

        # get graph for GTC gammao0 generation
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalGamma_Deb_ML_Sub.xml')

        # construct command
        command = '{} {} -x -q {} ' \
                  '-Prange_looks={} -Pazimuth_looks={} ' \
                  '-Pregion="{}" -Pinput="{}" -Poutput="{}"' \
            .format(GPT_FILE, graph, 2*gpt_max_workers,
                    range_looks, azimuth_looks,
                    region, infile, outfile)

    elif ard['product_type'] == 'GTC-sigma0':
        logger.info(
            'Calibrating the product to a GTC product (Sigma0).'
        )

        # get graph for GTC-gamma0 generation
        graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD',
                    'S1_SLC_TNR_CalSigma_Deb_ML_Sub.xml')

        # construct command
        command = '{} {} -x -q {} ' \
                  '-Prange_looks={} -Pazimuth_looks={} ' \
                  '-Pregion="{}" -Pinput="{}" -Poutput="{}"' \
            .format(GPT_FILE, graph, 2*gpt_max_workers,
                    range_looks, azimuth_looks,
                    region, infile, outfile)
    else:
        raise TypeError('Wrong product type selected.')

    logger.info("Removing thermal noise, calibrating and debursting")
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully calibrated product')
        return return_code
    else:
        raise GPTRuntimeError(
            'Calibration exited with an error {}. '
            'See {} for Snap Error output'.format(return_code, logfile)
        )


@retry(tries=3, delay=1, logger=logger)
def coreg(master, slave, outfile, logfile, dem_dict, gpt_max_workers=os.cpu_count()):
    '''A wrapper around SNAP's back-geocoding co-registration routine

    This function takes a list of 2 OST imported Sentinel-1 SLC products
    and co-registers them properly. This routine is sufficient for coherence
    estimation, but not for InSAR, since the ESD refinement is not applied.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'
        gpt_max_workers(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    '''

    logger.info('Co-registering {} and {}'.format(master, slave))
    command = (
        f'{GPT_FILE} Back-Geocoding -x -q {2*gpt_max_workers} '
        f'-PdemName=\'{dem_dict["dem_name"]}\' '
        f'-PdemResamplingMethod=\'{dem_dict["dem_resampling"]}\' '
        f'-PexternalDEMFile=\'{dem_dict["dem_file"]}\' '
        f'-PexternalDEMNoDataValue=\'{dem_dict["dem_nodata"]}\' '
        f'-PmaskOutAreaWithoutElevation=false '
        f'-t \'{outfile}\''
        f' {master} {slave}'
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully coregistered product.')
    else:
        raise GPTRuntimeError('ERROR: Co-registration exited with '
                              'an error {}. See {} for Snap '
                              'Error output'.format(return_code, logfile)
                              )


@retry(tries=3, delay=1, logger=logger)
def coreg2(master, slave, outfile, logfile, dem_dict, gpt_max_workers=os.cpu_count()):
    '''A wrapper around SNAP's back-geocoding co-registration routine

    This function takes a list of 2 OST imported Sentinel-1 SLC products
    and co-registers them properly. This routine is sufficient for coherence
    estimation, but not for InSAR, since the ESD refinement is not applied.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'
        gpt_max_workers(int): the number of cpu cores to allocate to the gpt job - defaults to cpu count


    '''

    # get path to graph
    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_Coreg.xml')

    logger.info('Co-registering {} and {}'.format(master, slave))
    command = ('{} {} -x -q {} '
               ' -Pmaster={}'
               ' -Pslave={}'
               ' -Pdem=\'{}\''
               ' -Pdem_file=\'{}\''
               ' -Pdem_nodata=\'{}\''
               ' -Pdem_resampling=\'{}\''
               ' -Poutput={} '.format(
        GPT_FILE, graph, 2*gpt_max_workers,
        master, slave,
        dem_dict['dem_name'], dem_dict['dem_file'],
        dem_dict['dem_nodata'], dem_dict['dem_resampling'],
        outfile)
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully coregistered product.')
    else:
        raise GPTRuntimeError('ERROR: Co-registration exited with '
                              'an error {}. See {} for Snap '
                              'Error output'.format(return_code, logfile)
                              )


@retry(tries=3, delay=1, logger=logger)
def coherence(infile, outfile, logfile, ard,
              gpt_max_workers=os.cpu_count()):
    '''A wrapper around SNAP's coherence routine

    This function takes a co-registered stack of 2 Sentinel-1 SLC products
    and calculates the coherence.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        gpt_max_workers(int): the number of cpu cores to allocate to the gpt job,
                default: os.cpu_count()
    '''

    # get path to graph
    graph = opj(OST_ROOT, 'graphs', 'S1_SLC2ARD', 'S1_SLC_Coh_Deb.xml')
    polar = ard['coherence_bands'].replace(' ', '')
    logger.info('Coherence estimation')
    command = '{} {} -x -q {} ' \
              '-Pazimuth_window={} -Prange_window={} ' \
              '-Ppolar=\'{}\' -Pinput={} -Poutput={}' \
        .format(GPT_FILE, graph, 2*gpt_max_workers,
                ard['coherence_azimuth'], ard['coherence_range'],
                polar, infile, outfile)

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully created coherence product.')
        return return_code
    else:
        raise GPTRuntimeError('ERROR: Coherence exited with an error {}. \
                See {} for Snap Error output'.format(return_code, logfile))
