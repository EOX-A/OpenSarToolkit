import numpy as np
import logging

from retry import retry

from ost.helpers.settings import GPT_FILE, OST_ROOT
from ost.helpers.errors import GPTRuntimeError, InvalidFileError
from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


@retry(tries=3, delay=1, logger=logger)
def burst_import(
        infile,
        outfile,
        logfile,
        swath,
        burst,
        config_dict
):
    """A wrapper of SNAP import of a single Sentinel-1 SLC burst
    This function takes an original Sentinel-1 scene (either zip or
    SAFE format), updates the orbit information (does not fail if not
    available), and extracts a single burst based on the
    given input parameters.
    :param infile:
    :param outfile:
    :param logfile:
    :param swath:
    :param burst:
    :param config_dict:
    :return:
    """

    # get polarisations to import
    ard = config_dict['processing']['single_ARD']
    bs_polar = ard['polarisation'].replace(' ', '')
    coh_polar = ard['coherence_bands'].replace(' ', '')

    if ard['coherence']:
        polars = bs_polar if len(bs_polar) >= len(coh_polar) else coh_polar
    else:
        polars = bs_polar

    # get cpus
    cpus = config_dict['gpt_max_workers']

    # get path to graph
    graph = OST_ROOT.joinpath('graphs/S1_SLC2ARD/S1_SLC_BurstSplit_AO.xml')

    logger.info(
        f'Importing Burst {burst} from Swath {swath} from scene {infile.name}'
    )

    command = (
        f'{GPT_FILE} {graph} -x -q {2 * cpus} '
        f'-Pinput={str(infile)} '
        f'-Ppolar={polars} '
        f'-Pswath={swath} '
        f'-Pburst={burst} '
        f'-Poutput={str(outfile)}'
    )

    logger.info(f'Executing command: {command}')
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully imported burst.')
    else:
        raise GPTRuntimeError(
            f'Frame import exited with error {return_code}. '
            f'See {logfile} for Snap\'s error message.'
        )

    # do not do the check routine
    # return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix('.dim'))
    else:
        raise InvalidFileError(
            f'Product did not pass file check: {return_code}'
        )


@retry(tries=3, delay=1, logger=logger)
def ha_alpha(infile, outfile, logfile, config_dict):
    """A wrapper of SNAP H-A-alpha polarimetric decomposition
    This function takes an OST imported Sentinel-1 scene/burst
    and calulates the polarimetric decomposition parameters for
    the H-A-alpha decomposition.
    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    ard = config_dict['processing']['single_ARD']
    remove_pol_speckle = ard['remove_pol_speckle']
    pol_speckle_dict = ard['pol_speckle_filter']
    cpus = config_dict['gpt_max_workers']

    if remove_pol_speckle:
        graph = OST_ROOT.joinpath(
            'graphs/S1_SLC2ARD/S1_SLC_Deb_Spk_Halpha.xml'
        )
        logger.info(
            'Applying the polarimetric speckle filter and'
            ' calculating the H-alpha dual-pol decomposition'
        )

        command = (
            f'{GPT_FILE} {graph} -x -q {2 * cpus} '
            f'-Pinput={str(infile)} '
            f'-Poutput={str(outfile)} '
            f"-Pfilter=\'{pol_speckle_dict['polarimetric_filter']}\' "
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
            f'{GPT_FILE} {graph} -x -q {2 * cpus} '
            f'-Pinput="{str(infile)}" '
            f'-Poutput="{str(outfile)}"'
        )

    logger.info(f'Executing command: {command}')
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully created H/A/Alpha product')
    else:
        raise GPTRuntimeError(
            f'H/Alpha exited with an error {return_code}. '
            f'See {logfile} for Snap\'s error message.'
        )

    # do check routine
    # return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix('.dim'))
    else:
        raise InvalidFileError(
            f'Product did not pass file check: {return_code}'
        )


@retry(tries=3, delay=1, logger=logger)
def calibration(
        infile,
        outfile,
        logfile,
        config_dict,
):
    """A wrapper around SNAP's radiometric calibration
    This function takes OST imported Sentinel-1 product and generates
    it to calibrated backscatter.
    3 different calibration modes are supported.
        - Radiometrically terrain corrected Gamma nought (RTC)
          NOTE: that the routine actually calibrates to bet0 and needs to
          be used together with _terrain_flattening routine
        - ellipsoid based Gamma nought (GTCgamma)
        - Sigma nought (GTCsigma).
    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :param region:
    :return:
    """

    # get relevant config parameters
    ard = config_dict['processing']['single_ARD']
    cpus = config_dict['gpt_max_workers']
    dem_dict = ard['dem']
    region = ''
    # calculate Multi-Look factors
    azimuth_looks = min(1, int(np.floor(ard['resolution'] / 10)))
    range_looks = min(5, int(azimuth_looks * 5))

    # construct command dependent on selected product type
    if ard['product_type'] == 'RTC-gamma0':
        logger.info('Calibrating the product to a RTC product.')

        # get graph for RTC generation
        graph = OST_ROOT.joinpath(
            'graphs/S1_SLC2ARD/S1_SLC_TNR_CalBeta_Deb_ML_TF_Sub.xml'
        )

        # construct command
        command = (
            f"{GPT_FILE} {graph} -x -q {2 * cpus} "
            f"-Prange_looks={range_looks} "
            f"-Pazimuth_looks={azimuth_looks} "
            f"-Pdem=\'{dem_dict['dem_name']}\' "
            f"-Pdem_file=\'{dem_dict['dem_file']}\' "
            f"-Pdem_nodata={dem_dict['dem_nodata']} "
            f"-Pdem_resampling={dem_dict['dem_resampling']} "
            f"-Pregion=\'{region}\' "
            f"-Pinput={str(infile)} "
            f"-Poutput={str(outfile)}"
        )

    elif ard['product_type'] == 'GTC-gamma0':
        logger.info('Calibrating the product to a GTC product (Gamma0).')

        # get graph for GTC-gammao0 generation
        graph = OST_ROOT.joinpath(
            'graphs/S1_SLC2ARD/S1_SLC_TNR_CalGamma_Deb_ML_Sub.xml'
        )

        # construct command
        command = (
            f'{GPT_FILE} {graph} -x -q {2 * cpus} '
            f'-Prange_looks={range_looks} '
            f'-Pazimuth_looks={azimuth_looks} '
            f'-Pregion="{region}" '
            f'-Pinput="{str(infile)}" '
            f'-Poutput="{str(outfile)}"'
        )

    elif ard['product_type'] == 'GTC-sigma0':
        logger.info('Calibrating the product to a GTC product (Sigma0).')

        # get graph for GTC-sigma0 generation
        graph = OST_ROOT.joinpath(
            'graphs/S1_SLC2ARD/S1_SLC_TNR_CalSigma_Deb_ML_Sub.xml'
        )

        # construct command
        command = (
            f'{GPT_FILE} {graph} -x -q {2 * cpus} '
            f'-Prange_looks={range_looks} '
            f'-Pazimuth_looks={azimuth_looks} '
            f'-Pregion="{region}" '
            f'-Pinput="{str(infile)}" '
            f'-Poutput="{str(outfile)}"'
        )
    else:
        raise TypeError('Wrong product type selected.')

    logger.info(f'Command: {command}')
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully calibrated product')
    else:
        raise GPTRuntimeError(
            f'Calibration exited with an error {return_code}. '
            f'See {logfile} for Snap\'s error output.'
        )

    # do check routine
    # return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix('.dim'))
    else:
        raise InvalidFileError(
            f'Product did not pass file check: {return_code}'
        )


@retry(tries=3, delay=1, logger=logger)
def coreg(master, slave, outfile, logfile, config_dict):
    """A wrapper around SNAP's back-geocoding co-registration routine
    This function takes 2 OST imported Sentinel-1 SLC products
    (master and slave) and co-registers them properly.
    This routine is sufficient for coherence estimation,
    but not for InSAR, since the ESD refinement is not applied.
    :param master:
    :param slave:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    cpus = config_dict['gpt_max_workers']
    dem_dict = config_dict['processing']['single_ARD']['dem']

    logger.info(f'Co-registering {master} and {slave}')

    # construct command
    command = (
        f'{GPT_FILE} Back-Geocoding -x -q {2 * cpus} '
        f'-PdemName=\'{dem_dict["dem_name"]}\' '
        f'-PdemResamplingMethod=\'{dem_dict["dem_resampling"]}\' '
        f'-PexternalDEMFile=\'{dem_dict["dem_file"]}\' '
        f'-PexternalDEMNoDataValue=\'{dem_dict["dem_nodata"]}\' '
        f'-PmaskOutAreaWithoutElevation=false '
        f'-t \'{str(outfile)}\''
        f' "{master}" "{slave}"'
    )

    logger.info(f'Executing command: {command}')
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully coregistered product.')
    else:
        raise GPTRuntimeError(
            f'Co-registration exited with an error {return_code}. '
            f'See {logfile} for Snap error output.'
        )

    # do check routine
    # return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix('.dim'))
    else:
        raise InvalidFileError(
            f'Product did not pass file check: {return_code}'
        )


@retry(tries=3, delay=1, logger=logger)
def coreg2(master, slave, outfile, logfile, config_dict):
    """A wrapper around SNAP's back-geocoding co-registration routine
    This function takes 2 OST imported Sentinel-1 SLC products
    (master and slave) and co-registers them properly.
    This routine is sufficient for coherence estimation,
    but not for InSAR, since the ESD refinement is not applied.
    :param master:
    :param slave:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    cpus = config_dict['gpt_max_workers']
    dem_dict = config_dict['processing']['single_ARD']['dem']

    # get path to graph
    graph = OST_ROOT.joinpath('graphs/S1_SLC2ARD/S1_SLC_Coreg.xml')

    logger.info(f'Co-registering {master} and {slave}')
    command = (
        f"{GPT_FILE} {graph} -x -q {2*cpus} "
        f" -Pmaster={master} "
        f" -Pslave={slave} "
        f" -Pdem=\'{dem_dict['dem_name']}\' "
        f" -Pdem_file=\'{dem_dict['dem_file']}\' "
        f" -Pdem_nodata=\'{dem_dict['dem_nodata']}\' "
        f" -Pdem_resampling=\'{dem_dict['dem_resampling']}\' "
        f" -Poutput={str(outfile)}"
    )

    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Successfully co-registered product.')
    else:
        raise GPTRuntimeError(
            f'Co-registration exited with an error {return_code}. '
            f'See {logfile} for Snap\'s error message.'
        )

    # do check routine
    # return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix('.dim'))
    else:
        raise InvalidFileError(
            f'Product did not pass file check: {return_code}'
        )


@retry(tries=3, delay=1, logger=logger)
def coherence(infile, outfile, logfile, config_dict):
    """A wrapper around SNAP's coherence routine
    This function takes a co-registered stack of 2 Sentinel-1 SLC products
    and calculates the coherence.
    :param infile:
    :param outfile:
    :param logfile:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    ard = config_dict['processing']['single_ARD']
    polars = ard['coherence_bands'].replace(' ', '')
    cpus = config_dict['gpt_max_workers']

    # get path to graph
    graph = OST_ROOT.joinpath('graphs/S1_SLC2ARD/S1_SLC_Coh_Deb.xml')

    logger.info('Coherence estimation')

    command = (
        f"{GPT_FILE} {graph} -x -q {2 * cpus} "
        f"-Pazimuth_window={ard['coherence_azimuth']} "
        f"-Prange_window={ard['coherence_range']} "
        f'-Ppolar=\'{polars}\' '
        f'-Pinput="{str(infile)}" '
        f'-Poutput="{str(outfile)}"'
    )

    logger.info(f'Executing command: {command}')
    return_code = h.run_command(command, logfile)

    if return_code == 0:
        logger.info('Succesfully created coherence product.')
    else:
        raise GPTRuntimeError(
            f'Coherence exited with an error {return_code}. '
            f'See {logfile} for Snap\'s error message.'
        )

    # do check routine
    # return_code = h.check_out_dimap(outfile)
    if return_code == 0:
        return str(outfile.with_suffix('.dim'))
    else:
        raise InvalidFileError(
            f'Product did not pass file check: {return_code}'
        )
