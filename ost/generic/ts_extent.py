try:
    import gdal
except:
    try:
        from osgeo import gdal
    except Exception as e:
        raise e
from pathlib import Path
from tempfile import TemporaryDirectory

from ost.helpers import raster as ras, vector as vec


def mt_extent(list_of_scenes,
              out_file,
              temp_dir,
              buffer
              ):

    # get out directory
    out_dir = Path(out_file).parent
    # build vrt stack from all scenes
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(
        str(out_dir.joinpath('extent.vrt')), list_of_scenes,
        options=vrt_options
    )
    with TemporaryDirectory(prefix=f'{temp_dir}/') as temp:

        # create namespace for temp file
        outline_file = Path(temp).joinpath(Path(out_file).name)

        # create outline
        ras.outline(out_dir.joinpath('extent.vrt'), outline_file, 0, False)

        # create exterior ring and write out
        vec.exterior(outline_file, out_file, buffer)
