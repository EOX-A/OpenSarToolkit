import os
import rasterio
from ost.helpers.settings import CONFIG_CHECK


def test_update_ard_param(grd_project_class):
    grd_project_class.ard_parameters["single_ARD"]["type"] = 'OST-GTC'
    grd_project_class.update_ard_parameters()
    assert grd_project_class.config_dict["processing"]["single_ARD"]["type"] == 'OST-GTC'


# Test GRDs to ARD kind of batch
def test_grds_to_ards(grd_project_class):
    for ard_type in CONFIG_CHECK['type']['choices']:
        if ard_type in CONFIG_CHECK['ard_types_grd']['choices']:
            grd_project_class.ard_parameters["single_ARD"]["type"] = ard_type
            grd_project_class.update_ard_parameters()
            grd_project_class.ard_parameters['single_ARD']['resolution'] = 20
            grd_project_class.update_ard_parameters()
            # Test ard, Timeseries and Timescan with just one product
            grd_project_class.grds_to_ard(
                timeseries=True,
                timescan=True,
                mosaic=False,
                overwrite=True,
                to_tif=True,
            )
            for i, row in grd_project_class.inventory.iterrows():
                assert os.path.isfile(row.out_dimap)
                assert os.path.isfile(row.out_tif)
                with rasterio.open(row.out_tif, "r") as test_tif:
                    assert test_tif.read(1).any()


# Test GRDs to ARD kind of batch
def test_grds_to_single_band_tifs(grd_project_class):
    for ard_type in CONFIG_CHECK['type']['choices']:
        if ard_type in CONFIG_CHECK['ard_types_grd']['choices'] and 'OST-GTC' in ard_type:
            grd_project_class.ard_parameters["single_ARD"]["type"] = ard_type
            grd_project_class.update_ard_parameters()
            grd_project_class.ard_parameters['single_ARD']['resolution'] = 20
            grd_project_class.update_ard_parameters()
            # Test ard, Timeseries and Timescan with just one product
            grd_project_class.grds_to_ard(
                timeseries=False,
                timescan=False,
                mosaic=False,
                overwrite=True,
                to_tif=True,
                single_band_tifs=True,
            )
            for i, row in grd_project_class.inventory.iterrows():
                assert os.path.isfile(row.out_dimap)
                assert os.path.isfile(row.out_vv)
                assert os.path.isfile(row.out_vh)
                with rasterio.open(row.out_vv, "r") as test_tif:
                    assert test_tif.read(1).any()


# Test Bursts to ARD kind of batch
def test_bursts_to_ards(slc_project_class):
    for ard_type in CONFIG_CHECK['type']['choices']:
        if ard_type in CONFIG_CHECK['ard_types_slc']['choices'] and 'OST-GTC' in ard_type:
            slc_project_class.ard_parameters["single_ARD"]["type"] = ard_type
            slc_project_class.update_ard_parameters()
            slc_project_class.ard_parameters['single_ARD']['resolution'] = 20
            slc_project_class.update_ard_parameters()
            slc_project_class.bursts_to_ards(
                timeseries=True,
                timescan=True,
                mosaic=False,
                overwrite=True,
                max_workers=2
            )
