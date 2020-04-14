import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from ost.helpers import helpers as h
from ost.helpers.vector import ls_to_vector
from ost.helpers.errors import GPTRuntimeError
from ost.generic import common_wrappers as common
from ost.s1 import slc_wrappers as slc


logger = logging.getLogger(__name__)


def create_polarimetric_layers(import_file, out_dir, burst_prefix,
                               config_dict):
    """ Pipeline for Dual-polarimetric decomposition

    Args:
        import_file:
        ard:
        temp_dir:
        out_dir:
        burst_id:
        ncores:

    Returns:

    """

    # get relevant config parameters
    ard = config_dict['processing']['single_ARD']
    gpt_cpus = config_dict['gpt_max_workers']

    # temp dir for intermediate files
    with TemporaryDirectory(prefix=f"{config_dict['temp_dir']}/") as temp:
        temp = Path(temp)
        # -------------------------------------------------------
        # 1 Polarimetric Decomposition

        # create namespace for temporary decomposed product
        out_haa = temp.joinpath(f'{burst_prefix}_h')

        # create namespace for decompose log
        haa_log = out_dir.joinpath(f'{burst_prefix}_haa.err_log')

        # run polarimetric decomposition
        slc.ha_alpha(
            import_file, out_haa, haa_log, ard['remove_pol_speckle'],
            ard['pol_speckle_filter'], gpt_cpus
        )

        # -------------------------------------------------------
        # 2 Geocoding

        # create namespace for temporary geocoded product
        out_htc = temp.joinpath(f'{burst_prefix}_pol')

        # create namespace for geocoding log
        haa_tc_log = out_dir.joinpath(f'{burst_prefix}_haa_tc.err_log')

        # run geocoding
        common.terrain_correction(
            '{}.dim'.format(out_haa), out_htc, haa_tc_log,
            ard['resolution'], ard['dem'], gpt_cpus
        )

        # last check on the output files
        try:
            h.check_out_dimap(out_htc)
        except ValueError:
            pass

        # move to final destination
        h.move_dimap(out_htc, out_dir.joinpath(f'{burst_prefix}_pol'))

        # write out check file for tracking that it is processed
        with open(out_dir.joinpath('.pol.processed'), 'w+') as file:
            file.write('passed all tests \n')


def create_backscatter_layers(import_file, out_dir, burst_prefix,
                              config_dict):
    """

    :param import_file:
    :param out_dir:
    :param burst_prefix:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    ard = config_dict['processing']['single_ARD']
    gpt_cpus = config_dict['gpt_max_workers']

    # temp dir for intermediate files
    with TemporaryDirectory(prefix=f"{config_dict['temp_dir']}/") as temp:

        temp = Path(temp)
        # ---------------------------------------------------------------------
        # 1 Calibration

        # create namespace for temporary calibrated product
        out_cal = temp.joinpath(f'{burst_prefix}_cal')

        # create namespace for calibrate log
        cal_log = out_dir.joinpath(f'{burst_prefix}_cal.err_log')

        # run calibration on imported scene
        slc.calibration(
            import_file, out_cal, cal_log, ard, region='', ncores=gpt_cpus
        )

        # ---------------------------------------------------------------------
        # 2 Speckle filtering
        if ard['remove_speckle']:

            # create namespace for temporary speckle filtered product
            speckle_import = temp.joinpath(f'{burst_prefix}_speckle_import')

            # create namespace for speckle filter log
            speckle_log = out_dir.joinpath(f'{burst_prefix}_speckle.err_log')

            # run speckle filter on calibrated input
            common.speckle_filter(
                f'{out_cal}.dim', speckle_import, speckle_log,
                ard['speckle_filter'], gpt_cpus
            )

            # remove input
            h.delete_dimap(out_cal)

            # reset master_import for following routine
            out_cal = speckle_import

        # ---------------------------------------------------------------------
        # 3 dB scaling
        if ard['to_db']:

            # create namespace for temporary db scaled product
            out_db = temp.joinpath(f'{burst_prefix}_cal_db')

            # create namespace for db scaling log
            db_log = out_dir.joinpath(f'{burst_prefix}_cal_db.err_log')

            # run db scaling on calibrated/speckle filtered input
            common.linear_to_db(f'{out_cal}.dim', out_db, db_log, gpt_cpus)

            # remove tmp files
            h.delete_dimap(out_cal)

            # set out_cal to out_db for further processing
            out_cal = out_db

        # ---------------------------------------------------------------------
        # 4 Geocoding

        # create namespace for temporary geocoded product
        out_tc = temp.joinpath(f'{burst_prefix}_bs')

        # create namespace for geocoding log
        tc_log = out_dir.joinpath(f'{burst_prefix}_bs_tc.err_log')

        # run terrain correction on calibrated/speckle filtered/db  input
        common.terrain_correction(
            f'{out_cal}.dim', out_tc, tc_log,
            ard['resolution'], ard['dem'], gpt_cpus
        )

        # check for validity of final backscatter product
        try:
            h.check_out_dimap(out_tc)
        except ValueError:
            pass

        # move final backscatter product to actual output directory
        h.move_dimap(out_tc, out_dir.joinpath(f'{burst_prefix}_bs'))

        # ---------------------------------------------------------------------
        # 9 Layover/Shadow mask
        if ard['create_ls_mask']:

            # create namespace for temporary LS map product
            out_ls = temp.joinpath(f'{burst_prefix}_LS')

            # create namespace for LS map log
            ls_log = out_dir.joinpath(f'{burst_prefix}_LS.err_log')

            # run ls mask generation on calibration
            common.ls_mask(f'{out_cal}.dim', out_ls, ls_log, ard, gpt_cpus)

            # check for validity of final backscatter product
            try:
                h.check_out_dimap(out_ls)
            except ValueError:
                pass

            # move ls data to final destination
            h.move_dimap(out_ls, out_dir.joinpath(f'{burst_prefix}_LS'))

        # write out check file for tracking that it is processed
        with open(out_dir.joinpath('.bs.processed'), 'w+') as file:
            file.write('passed all tests \n')

    # Return colected files that have been processed
    if ard['create_ls_mask'] is True:
        out_ls_mask = out_ls + '.dim'
        out_ls_mask = ls_to_vector(infile=out_ls_mask, driver='GPKG')
    else:
        out_ls_mask = None

    return out_tc + '.dim', out_ls_mask


def create_coherence_layers(
        master_import, slave_import, out_dir,
        master_prefix, config_dict
):
    """

    :param master_import:
    :param slave_import:
    :param out_dir:
    :param master_prefix:
    :param config_dict:
    :return:
    """

    # get relevant config parameters
    ard = config_dict['processing']['single_ARD']
    gpt_cpus = config_dict['gpt_max_workers']

    with TemporaryDirectory(prefix=f"{config_dict['temp_dir']}/") as temp:

        temp = Path(temp)
        # ---------------------------------------------------------------
        # 1 Co-registration
        # create namespace for temporary co-registered stack
        out_coreg = temp.joinpath(f'{master_prefix}_coreg')

        # create namespace for co-registration log
        coreg_log = out_dir.joinpath(f'{master_prefix}_coreg.err_log')

        # run co-registration
        slc.coreg(
            master_import, slave_import, out_coreg, coreg_log,
            ard['dem'], gpt_cpus
        )

        # remove imports
        h.delete_dimap(master_import)

        # if remove_slave_import is True:
        #    h.delete_dimap(slave_import)

        # ---------------------------------------------------------------
        # 2 Coherence calculation

        # create namespace for temporary coherence product
        out_coh = temp.joinpath(f'{master_prefix}_coherence')

        # create namespace for coherence log
        coh_log = out_dir.joinpath(f'{master_prefix}_coh.err_log')

        # run coherence estimation
        slc.coherence(f'{out_coreg}.dim', out_coh, coh_log, ard, gpt_cpus)

        # remove coreg tmp files
        h.delete_dimap(out_coreg)

        # ---------------------------------------------------------------
        # 3 Geocoding

        # create namespace for temporary geocoded roduct
        out_tc = temp.joinpath(f'{master_prefix}_coh')

        # create namespace for geocoded log
        tc_log = out_dir.joinpath(f'{master_prefix}_coh_tc.err_log')

        # run geocoding
        common.terrain_correction(
            f'{out_coh}.dim', out_tc, tc_log,
            ard['resolution'], ard['dem'], gpt_cpus
        )

        # ---------------------------------------------------------------
        # 4 Checks and Clean-up

        # remove tmp files
        h.delete_dimap(out_coh)

        # check on coherence data
        try:
            h.check_out_dimap(out_tc)
        except ValueError:
            pass

        # move to final destination
        h.move_dimap(out_tc, out_dir.joinpath(f'{master_prefix}_coh'))

        # write out check file for tracking that it is processed
        with open(out_dir.joinpath('.coh.processed'), 'w+') as file:
            file.write('passed all tests \n')


def burst_to_ard(burst, config_dict):
    # no this is gdf thing (id, gdf_row)
    if isinstance(burst, tuple):
        i, burst = burst

    ard = config_dict['processing']['single_ARD']
    temp_dir = Path(config_dict['temp_dir'])
    gpt_cpus = config_dict['gpt_max_workers']

    # creation of out_directory
    out_dir = Path(burst.out_directory)
    out_dir.mkdir(parents=True, exist_ok=True)

    # get info on master from GeoSeries
    master_prefix = burst['master_prefix']
    master_file = burst['file_location']
    master_burst_nr = burst['BurstNr']
    swath = burst['SwathID']

    # existence of processed files
    pol_file = out_dir.joinpath('.pol.processed').exists()
    bs_file = out_dir.joinpath('.bs.processed').exists()
    coh_file = out_dir.joinpath('.coh.processed').exists()

    # check if we need to produce coherence
    if ard['coherence']:
        # we check if there is actually a slave file or
        # if it is the end of the time-series
        coherence = True if burst.slave_file else False
    else:
        coherence = False

    # check if somethings already processed
    if (
            (ard['H-A-Alpha'] and not pol_file) or
            (ard['backscatter'] and not bs_file) or
            (coherence and not coh_file)
    ):

        # ---------------------------------------------------------------------
        # 1 Import
        # import master
        # create namespace for master import
        master_import = temp_dir.joinpath(f'{master_prefix}_import')

        if not Path(f'{str(master_import)}.dim').exists():
            # create namesapce for log file
            import_log = out_dir.joinpath(f'{master_prefix}_import.err_log')

            # get polarisations to import
            polars = ard['polarisation'].replace(' ', '')

            # run import
            return_code = slc.burst_import(
                master_file, master_import, import_log, swath,
                master_burst_nr, polars, gpt_cpus
            )
            if return_code != 0:
                h.delete_dimap(master_import)
                raise GPTRuntimeError('Something with importing went wrong!')

        # ---------------------------------------------------------------------
        # 2 Product Generation
        if ard['H-A-Alpha'] and not pol_file:
            create_polarimetric_layers(
                f'{master_import}.dim', out_dir, master_prefix, config_dict
            )

        if ard['backscatter'] and not bs_file:
            create_backscatter_layers(
                f'{master_import}.dim', out_dir, master_prefix, config_dict
            )

        if coherence and not coh_file:
            # get info on master from GeoSeries
            slave_prefix = burst['slave_prefix']
            slave_file = burst['slave_file']
            slave_burst_nr = burst['slave_burst_nr']

            # import slave
            slave_import = temp_dir.joinpath(f'{slave_prefix}_import')
            import_log = out_dir.joinpath(f'{slave_prefix}_import.err_log')
            polars = ard['polarisation'].replace(' ', '')
            return_code = slc.burst_import(
                slave_file, slave_import, import_log, swath, slave_burst_nr,
                polars, gpt_cpus
            )

            if return_code != 0:
                h.remove_folder_content(temp_dir)
                raise GPTRuntimeError('Something with coherence generation went wrong!')

            create_coherence_layers(
                f'{master_import}.dim', f'{slave_import}.dim', out_dir,
                master_prefix, config_dict
            )
        else:
            # remove master import
            h.delete_dimap(master_import)

    # Get out files after the processing if any
    if ard['H-A-Alpha']:
        out_pol = out_dir.joinpath(master_prefix + '_pol.dim')
    else:
        out_pol = None
    if ard['backscatter']:
        out_bs = out_dir.joinpath(master_prefix + '_bs.dim')
        out_ls = out_dir.joinpath(master_prefix + '_LS.dim')
    else:
        out_bs, out_ls = None, None
    if coherence:
        out_coh = out_dir.joinpath(master_prefix + '_coh.dim')
    else:
        out_coh = None

    return out_bs, out_ls, out_coh, out_pol
