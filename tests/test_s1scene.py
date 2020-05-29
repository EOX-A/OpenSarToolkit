import os
from shapely.geometry import box

from ost.s1.s1scene import Sentinel1Scene
from ost.helpers.settings import HERBERT_USER


def test_s1scene_metadata(s1_id, s1_mai_2020_id, grd_project_class):
    s1 = Sentinel1Scene(s1_id)
    control_id = 'S1B_IW_GRDH_1SDV_20180813T054020_20180813T054045_012240_0168D6_B775'
    control_dict = {'Scene_Identifier':
                        'S1B_IW_GRDH_1SDV_20180813T054020_20180813T054045_012240_0168D6_B775',
                    'Satellite': 'Sentinel-1B',
                    'Acquisition_Mode': 'Interferometric Wide Swath',
                    'Processing_Level': '1',
                    'Product_Type': 'Ground Range Detected (GRD)',
                    'Acquisition_Date': '20180813',
                    'Start_Time': '054020',
                    'Stop_Time': '054045',
                    'Absolute_Orbit': '012240',
                    'Relative_Orbit': '139'
                    }
    control_poly = 'POLYGON ((10.623921 53.968655, 6.593705 54.389683, 6.990701 55.883099, 11.171576 55.458015, 10.623921 53.968655))'
    assert control_dict == s1.info()
    assert s1.scene_id == control_id
    s1.zip_annotation_get(download_dir=grd_project_class.download_dir)
    assert control_poly == s1.get_product_polygon(

        download_dir=grd_project_class.download_dir
    ).wkt
    s1._get_center_lat(
        scene_path=s1.get_path(
            download_dir=grd_project_class.download_dir
        )
    )
    s1 = Sentinel1Scene(s1_mai_2020_id)
    s1.scihub_annotation_get(
        uname=HERBERT_USER['uname'],
        pword=HERBERT_USER['pword']
    )
    s1.asf_url()
    s1.get_ard_parameters(ard_type='OST-GTC')


# def test_s1scene_slc_processing(s1_slc_ost_master,
#                                 slc_project_class,
#                                 some_bounds_slc,
#                                 ):
#     s1scene = s1_slc_ost_master[1]
#     aoi = box(some_bounds_slc[0], some_bounds_slc[1],
#               some_bounds_slc[2], some_bounds_slc[3]
#               ).wkt
#     out_files_dict = s1scene.create_ard(
#         download_dir=slc_project_class.download_dir,
#         out_dir=slc_project_class.project_dir,
#         overwrite=True,
#         subset=aoi
#         )
#     out_tif = s1scene.create_rgb(
#         outfile=os.path.join(str(slc_project_class.project_dir), s1scene.scene_id+'.tif')
#     )
#     assert os.path.exists(out_files_dict['bs'][0][1])
#     assert os.path.exists(out_files_dict['ls'][0])
#     assert os.path.exists(out_tif)
#
#
# def test_s1scene_grd_processing(s1_grd_notnr_ost_product,
#                                 grd_project_class,
#                                 some_bounds_grd
#                                 ):
#     s1scene = s1_grd_notnr_ost_product[1]
#     aoi = box(some_bounds_grd[0], some_bounds_grd[1],
#               some_bounds_grd[2], some_bounds_grd[3]
#               ).wkt
#     out_dict = s1scene.create_ard(
#         download_dir=grd_project_class.download_dir,
#         out_dir=grd_project_class.project_dir,
#         overwrite=True,
#         subset=aoi
#         )
#     out_tif = s1scene.create_rgb(outfile=out_dict['bs'].replace('.dim', '.tif'))
#     assert os.path.exists(out_dict['bs'])
#     assert out_dict['ls'] is None
#     assert os.path.exists(out_tif)
#     s1scene.visualise_rgb()
