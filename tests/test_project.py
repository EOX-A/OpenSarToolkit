from ost.helpers.settings import CONFIG_CHECK


def test_update_ard_param(grd_project_class):
    grd_project_class.ard_parameters["single_ARD"]["type"] = 'OST-GTC'
    grd_project_class.update_ard_parameters()
    assert grd_project_class.config_dict["processing"]["single_ARD"]["type"] == 'OST-GTC'


# Test GRDs to ARD kind of batch
def test_grds_to_ard(grd_project_class):
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
                cut_to_aoi=False
            )
