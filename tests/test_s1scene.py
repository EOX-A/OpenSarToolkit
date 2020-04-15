import os
from shapely.geometry import box
from ost.s1.s1scene import Sentinel1Scene


def test_s1scene_metadata(s1_id):
    s1 = Sentinel1Scene(s1_id)
    control_id = 'S1A_IW_GRDH_1SDV_20141003T040550_20141003T040619_002660_002F64_EC04'
    control_dict = {'Scene_Identifier':
                        'S1A_IW_GRDH_1SDV_20141003T040550_20141003T040619_002660_002F64_EC04',
                    'Satellite': 'Sentinel-1A',
                    'Acquisition_Mode': 'Interferometric Wide Swath',
                    'Processing_Level': '1',
                    'Product_Type': 'Ground Range Detected (GRD)',
                    'Acquisition_Date': '20141003',
                    'Start_Time': '040550',
                    'Stop_Time': '040619',
                    'Absolute_Orbit': '002660',
                    'Relative_Orbit': '138'
                    }
    assert control_dict == s1.info()
    assert s1.scene_id == control_id


def test_s1scene_slc_processing(s1_slc_ost_master,
                                slc_project_class,
                                some_bounds_slc,
                                ):
    s1scene = s1_slc_ost_master[1]
    aoi = box(some_bounds_slc[0], some_bounds_slc[1],
              some_bounds_slc[2], some_bounds_slc[3]
              ).wkt
    out_files_dict = s1scene.create_ard(
        download_dir=slc_project_class.download_dir,
        out_dir=slc_project_class.project_dir,
        overwrite=True,
        subset=aoi
        )
    out_tif = s1scene.create_rgb(
        outfile=os.path.join(str(slc_project_class.project_dir), s1scene.scene_id+'.tif')
    )
    assert os.path.exists(out_files_dict['bs'][0][1])
    assert os.path.exists(out_files_dict['ls'][0])
    assert os.path.exists(out_tif)


def test_s1scene_grd_processing(s1_grd_notnr_ost_product,
                                grd_project_class,
                                some_bounds_grd
                                ):
    s1scene = s1_grd_notnr_ost_product[1]
    aoi = box(some_bounds_grd[0], some_bounds_grd[1],
              some_bounds_grd[2], some_bounds_grd[3]
              ).wkt
    out_dict = s1scene.create_ard(
        download_dir=grd_project_class.download_dir,
        out_dir=grd_project_class.project_dir,
        overwrite=True,
        subset=aoi
        )
    out_tif = s1scene.create_rgb(outfile=out_dict['bs'].replace('.dim', '.tif'))
    assert os.path.exists(out_dict['bs'])
    assert out_dict['ls'] is None
    assert os.path.exists(out_tif)
