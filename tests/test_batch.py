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
            grd_project_class.grds_to_ard(
                timeseries=False,
                timescan=False,
                mosaic=False,
                overwrite=True,
                cut_to_aoi=False
            )

# TODO: Runs endlessly on travis for whatever reason
# # Test burst batch for all slc ARD types,
# split into more so that TRAVIS, wont get crazy
# # [OST-GTC, OST-RTC, OST-minimal]

# def test_burst_batch_ost_gtc(slc_project_class):
#     slc_project_class.ard_parameters["single_ARD"]["type"] = 'OST-GTC'
#     slc_project_class.update_ard_parameters()
#     slc_project_class.ard_parameters['single_ARD']['resolution'] = 50
#     slc_project_class.bursts_to_ard(
#         timeseries=False,
#         timescan=False,
#         mosaic=False,
#         overwrite=True,
#         cut_to_aoi=False,
#         ncores=2
#     )
# def test_burst_batch_ost_rtc(slc_project_class):
#     slc_project_class.ard_parameters["single_ARD"]["type"] = 'OST-RTC'
#     slc_project_class.update_ard_parameters()
#     slc_project_class.ard_parameters['single_ARD']['resolution'] = 50
#     slc_project_class.bursts_to_ard(
#         timeseries=False,
#         timescan=False,
#         mosaic=False,
#         overwrite=True,
#         cut_to_aoi=False,
#         ncores=2
#     )
