import os
import logging
import requests
from http.cookiejar import CookieJar
import urllib.error
import urllib.request as urlreq
import tqdm.auto as tqdm
from pathlib import Path
from retry import retry

from godale import Executor

from ost.helpers import helpers as h
from ost.helpers.errors import DownloadError
from ost.s1.s1scene import Sentinel1Scene as S1Scene

logger = logging.getLogger(__name__)


def check_connection(uname, pword):
    '''A helper function to check if a connection can be established
    Args:
        uname: username of ASF Vertex server
        pword: password of ASF Vertex server
    Returns
        int: status code of the get request
    '''
    password_manager = urlreq.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(
        None, "https://urs.earthdata.nasa.gov", uname, pword
    )

    cookie_jar = CookieJar()

    opener = urlreq.build_opener(
        urlreq.HTTPBasicAuthHandler(password_manager),
        urlreq.HTTPCookieProcessor(cookie_jar)
    )
    urlreq.install_opener(opener)

    url = ('https://datapool.asf.alaska.edu/SLC/SA/S1A_IW_SLC__1SSV_'
           '20160801T234454_20160801T234520_012413_0135F9_B926.zip'
           )

    try:
        urlreq.urlopen(url=url)
    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        # ...
        response_code = e.reason
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        # ...
        response_code = e.reason
    else:
        response_code = 200
    return response_code


@retry(tries=5)
def s1_download(argument_list):
    """
    This function will download S1 products from ASF mirror.

    :param url: the url to the file you want to download
    :param filename: the absolute path to where the downloaded file should
                    be written to
    :param uname: ESA's scihub username
    :param pword: ESA's scihub password
    :return:
    """

    url = argument_list[0]
    filename = Path(argument_list[1])
    uname = argument_list[2]
    pword = argument_list[3]

    password_manager = urlreq.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(
        None, "https://urs.earthdata.nasa.gov", uname, pword
    )

    cookie_jar = CookieJar()

    opener = urlreq.build_opener(
        urlreq.HTTPBasicAuthHandler(password_manager),
        urlreq.HTTPCookieProcessor(cookie_jar)
    )
    urlreq.install_opener(opener)

    logger.info('INFO: Downloading scene to: {}'.format(filename))
    # submit the request using the session
    try:
        response = urlreq.urlopen(url=url)
        # raise an exception in case of http errors
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.info(
                'Product %s missing from the archive, continuing.',
                filename.split('/')[-1]
            )
            return filename.split('/')[-1]
        else:
            raise e
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        # ...
        raise e

    # get download size
    total_length = int(response.headers.get('content-length', 0))

    # define chunk_size
    chunk_size = 1024

    # check if file is partially downloaded
    if filename.exists():
        first_byte = os.path.getsize(filename)
    else:
        first_byte = 0

    # actual download
    with requests.Session() as s, open(filename, "ab") as file:
        s.auth = (uname, pword)
        response_1 = s.request('get', url)
        response = s.get(response_1.url, auth=(uname, pword), stream=True)

        if total_length is None:
            file.write(response.content)
        else:
            try:
                pbar = tqdm.tqdm(total=total_length, initial=first_byte,
                                 unit='B', unit_scale=True,
                                 desc='INFO: Downloading ')

                for chunk in response.iter_content(chunk_size):
                    if chunk:
                        file.write(chunk)
                        pbar.update(chunk_size)
            finally:
                pbar.close()

    logger.info(
        f'Checking the zip archive of {filename.name} for inconsistency'
    )
    zip_test = h.check_zipfile(filename)

    # if it did not pass the test, remove the file
    # in the while loop it will be downloaded again
    if zip_test is not None:
        if os.path.exists(filename):
            os.remove(filename)
        raise DownloadError(f'{filename.name} did not pass the zip test. \
              Re-downloading the full scene.')
    else:
        logger.info(f'{filename.name} passed the zip test.')
        with open(str(Path(filename).with_suffix('.downloaded')), 'w+') as file:
            file.write('successfully downloaded \n')


def batch_download(inventory_df,
                   download_dir,
                   uname,
                   pword,
                   max_workers=10,
                   executor_type='concurrent_threads'
                   ):

    # create list with scene ids to download
    scenes = inventory_df['identifier'].tolist()

    # initialize check variables and loop until fulfilled
    asf_list = []
    check_counter = 0
    for scene_id in scenes:

        # initialize scene instance and get destination filepath
        scene = S1Scene(scene_id)
        filepath = scene._download_path(download_dir, True)

        # check if already downloaded
        if Path(f'{filepath}.downloaded').exists():
            check_counter += 1
            logger.info(f'{scene.scene_id} has been already downloaded.')
            continue

        # append to list
        asf_list.append([scene.asf_url(), filepath, uname, pword])

    # if list is not empty, do parallel download
    if asf_list:
        executor = Executor(max_workers=max_workers, executor=executor_type)
        for task in executor.as_completed(
                func=s1_download,
                iterable=asf_list,
                fargs=[]
        ):
            task.result()
            check_counter += 1

    # if all have been downloaded then we are through
    if len(inventory_df['identifier'].tolist()) == check_counter:
        logger.info('All products are downloaded.')
    # else we
    else:
        for scene in scenes:
            # we check if outputfile exists...
            scene = S1Scene(scene)
            filepath = scene._download_path(download_dir)
            if Path(f'{str(filepath)}.downloaded').exists():
                # ...and remove from list
                scenes.remove(scene.scene_id)
        raise DownloadError('ASF download is incomplete or has failed.')
