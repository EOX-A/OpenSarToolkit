import os
import pytest
import logging
from pathlib import Path

from ost.s1.slc_wrappers import burst_import, calibration
from ost.s1.burst_to_ard import create_coherence_layers, \
    create_polarimetric_layers

logger = logging.getLogger(__name__)


def test_burst_import(s1_slc_master,
                      s1_slc_slave,
                      s1_slc_ost_slave,
                      s1_slc_ost_master,
                      slc_project_class
                      ):
    slc_project_class.update_ard_parameters()
    scene_id, master = s1_slc_ost_master
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2 or burst.SwathID != 'IW1':
            continue
        out_path = burst_import(
            infile=Path(s1_slc_master),
            outfile=Path(os.path.join(
                slc_project_class.temp_dir,
                scene_id+'_'+burst.bid+'_import'
            )),
            logfile=logger,
            swath=burst.SwathID,
            burst=burst.BurstNr,
            config_dict=slc_project_class.config_dict
        )
        assert os.path.isfile(out_path)

    scene_id, master = s1_slc_ost_slave
    out_path = burst_import(
        infile=Path(s1_slc_slave),
        outfile=Path(os.path.join(
            slc_project_class.temp_dir,
            scene_id+'_'+'A15_IW1_7542'+'_import'
        )),
        logfile=logger,
        swath="IW1",
        burst=3,
        config_dict=slc_project_class.config_dict
    )
    assert os.path.isfile(out_path)


def test_burst_calibration(s1_slc_ost_master,
                           slc_project_class,
                           ):
    slc_project_class.ard_parameters.update(
        product_type='RTC-gamma0',
        resolution=60
    )
    slc_project_class.update_ard_parameters()
    scene_id, master = s1_slc_ost_master
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2 or burst.SwathID != 'IW1':
            continue
        out_path = calibration(
            infile=Path(os.path.join(
                slc_project_class.temp_dir,
                scene_id+'_'+burst.bid+'_import.dim'
            )),
            outfile=Path(os.path.join(
                slc_project_class.temp_dir, scene_id+'_BS'
            )),
            logfile=logger,
            config_dict=slc_project_class.config_dict
        )
        assert os.path.isfile(out_path)

    slc_project_class.ard_parameters.update(
        product_type='GTC-gamma0',
        resolution=60
    )
    slc_project_class.update_ard_parameters()
    scene_id, master = s1_slc_ost_master
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2 or burst.SwathID != 'IW1':
            continue
        out_path = calibration(
            infile=Path(os.path.join(
                slc_project_class.temp_dir,
                scene_id+'_'+burst.bid+'_import.dim'
            )),
            outfile=Path(os.path.join(
                slc_project_class.temp_dir, scene_id+'_BS'
            )),
            logfile=logger,
            config_dict=slc_project_class.config_dict
        )
        assert os.path.isfile(out_path)


def test_coherence(s1_slc_ost_master,
                   s1_slc_ost_slave,
                   slc_project_class
                   ):
    slc_project_class.update_ard_parameters()
    master_id, master = s1_slc_ost_master
    slave_id, master = s1_slc_ost_slave
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2 or burst.SwathID != 'IW1':
            continue
        out_path = create_coherence_layers(
            master_import=Path(os.path.join(
                slc_project_class.temp_dir,
                master_id+'_'+burst.bid+'_import.dim'
            )),
            slave_import=Path(os.path.join(
                slc_project_class.temp_dir,
                slave_id+'_'+'A15_IW1_7542'+'_import.dim'
            )),
            out_dir=Path(os.path.join(
                slc_project_class.temp_dir,
            )),
            master_prefix=master_id,
            config_dict=slc_project_class.config_dict
        )
        assert os.path.isfile(out_path[0])


# @pytest.mark.skip(reason="Takes too long skip for now!")
def test_burst_ha_alpha(
        s1_slc_master,
        s1_slc_ost_master,
        slc_project_class,
):
    slc_project_class.ard_parameters.update(
        resolution=60
    )
    slc_project_class.update_ard_parameters()
    master_id, master = s1_slc_ost_master
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2:
            continue
        out_path = create_polarimetric_layers(
            import_file=Path(os.path.join(
                slc_project_class.temp_dir,
                master_id+'_'+burst.bid+'_import.dim'
            )),
            out_dir=Path(os.path.join(
                slc_project_class.temp_dir,
            )),
            burst_prefix=master_id,
            config_dict=slc_project_class.config_dict
            )
        assert os.path.isfile(out_path[0])
