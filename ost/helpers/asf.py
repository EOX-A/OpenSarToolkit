import os
import logging
import requests
import tqdm
from pathlib import Path
from retrying import retry

from godale import Executor

from ost.helpers import helpers as h
from ost.helpers.errors import DownloadError
from ost import Sentinel1Scene as S1Scene

logger = logging.getLogger(__name__)


# we need this class for earthdata access
class SessionWithHeaderRedirection(requests.Session):
    """ A class that helps connect to NASA's Earthdata

    """

    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.

    def rebuild_auth(self, prepared_request, response):

        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:

            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)

            if (original_parsed.hostname != redirect_parsed.hostname) and \
                redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:

                del headers['Authorization']

        return


def check_connection(uname, pword):
    '''A helper function to check if a connection can be established

    Args:
        uname: username of ASF Vertex server
        pword: password of ASF Vertex server

    Returns
        int: status code of the get request
    '''

    # random url to check
    url = (
        'https://datapool.asf.alaska.edu/SLC/SB/''S1B_IW_SLC__1SDV_'
        '20191119T053342_20191119T053410_018992_023D59_F309.zip'
    )

    # connect and get response
    session = SessionWithHeaderRedirection(uname, pword)
    response = session.get(url, stream=True)

    return response.status_code


@retry(stop_max_attempt_number=5)
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

    session = SessionWithHeaderRedirection(uname, pword)

    logger.info('Downloading scene to: {}'.format(filename))
    # submit the request using the session
    response = session.get(url, stream=True)

    # raise an exception in case of http errors
    response.raise_for_status()

    # get download size
    total_length = int(response.headers.get('content-length', 0))

    # define chunk_size
    chunk_size = 1024

    # check if file is partially downloaded
    if filename.exists():
        first_byte = os.path.getsize(filename)
    else:
        first_byte = 0

    # get byte offset for already downloaded file
    header = {"Range": f"bytes={first_byte}-{total_length}"}
    response = session.get(url, headers=header, stream=True)

    # actual download
    with open(filename, "ab") as file:
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
    for scene_id in scenes:

        # initialize scene instance and get destination filepath
        scene = S1Scene(scene_id)
        filepath = scene._download_path(download_dir, True)

        # check if already downloaded
        if Path(f'{filepath}.downloaded').exists():
            logger.info(f'{scene.scene_id} has been already downloaded.')
            continue

        # append to list
        asf_list.append([scene.asf_url(), filepath, uname, pword])

    check_counter = 0
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
