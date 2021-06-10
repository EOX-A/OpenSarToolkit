import os
import pytest
import pandas as pd
from tempfile import TemporaryDirectory

from ost.s1.s1scene import Sentinel1Scene as S1Scene
from ost.helpers.helpers import check_zipfile
from ost.helpers.asf import check_connection as check_connection_asf
from ost.helpers.scihub import check_connection as check_connection_scihub, \
    connect
from ost.s1.download import download_sentinel1

from ost.helpers.settings import HERBERT_USER, APIHUB_BASEURL 


def test_asf_connection():
    herbert_uname = HERBERT_USER['uname']
    herbert_password = HERBERT_USER['asf_pword']
    response_code = check_connection_asf(uname=herbert_uname,
                                         pword=herbert_password
                                         )
    control_code = 200
    assert response_code == control_code


# @pytest.mark.xfail(reason="Sometimes down for maintanance, allow to fail!!")
def test_esa_scihub_connection(s1_grd_notnr_ost_product):
    herbert_uname = HERBERT_USER['uname']
    herbert_password = HERBERT_USER['pword']
    response_code = check_connection_scihub(uname=herbert_uname,
                                            pword=herbert_password
                                            )
    control_code = 200
    assert response_code == control_code
    opener = connect(
        base_url=APIHUB_BASEURL,
        uname=herbert_uname,
        pword=herbert_password
    )
    control_uuid = '1b43fb7d-bd2c-41cd-86a1-3442b1fbd5bb'
    uuid = s1_grd_notnr_ost_product[1].scihub_uuid(opener)
    assert uuid == control_uuid


# @pytest.mark.skipif("TRAVIS" in os.environ and os.environ["TRAVIS"] == "true",
#                     reason="Skipping this test on Travis CI."
#                     )
# def test_asf_download(s1_grd_notnr_ost_product, mirror=2):
#     herbert_uname = HERBERT_USER['uname']
#     herbert_password = HERBERT_USER['asf_pword']
#     df = pd.DataFrame({'identifier': [s1_grd_notnr_ost_product[1].scene_id]})
#     with TemporaryDirectory(dir=os.getcwd()) as temp:
#         download_sentinel1(
#             inventory_df=df,
#             download_dir=temp,
#             mirror=mirror,
#             concurrent=1,
#             uname=herbert_uname,
#             pword=herbert_password
#         )
#         from ost.helpers.helpers import check_zipfile
#         product_path = s1_grd_notnr_ost_product[1].get_path(
#             download_dir=temp,
#         )
#         return_code = check_zipfile(product_path)
#         assert return_code is None
#
#
# @pytest.mark.xfail(reason="This is currently not usually working, so allow to fail!!")
# def test_esa_scihub_download(s1_mai_2021_id,
#                              mirror=1
#                              ):
#     herbert_uname = HERBERT_USER['uname']
#     herbert_password = HERBERT_USER['pword']
#     product = S1Scene(s1_mai_2021_id)
#     df = pd.DataFrame({'identifier': [product.scene_id]})
#     with TemporaryDirectory(dir=os.getcwd()) as temp:
#         download_sentinel1(
#             inventory_df=df,
#             download_dir=temp,
#             mirror=mirror,
#             concurrent=1,
#             uname=herbert_uname,
#             pword=herbert_password
#         )
#
#         product_path = product.get_path(
#             download_dir=temp,
#             data_mount='/eodata'
#         )
#         return_code = check_zipfile(product_path)
#         assert return_code is None
