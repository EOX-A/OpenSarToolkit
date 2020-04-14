import os
import json
import itertools
import logging
import multiprocessing as mp
from pathlib import Path

from godale._concurrent import Executor

from ost.helpers import raster as ras
from ost.s1.burst_inventory import prepare_burst_inventory
from ost.s1.burst_to_ard import burst_to_ard
from ost.generic import ard_to_ts, ts_extent, ts_ls_mask, timescan, mosaic

logger = logging.getLogger(__name__)

# ---------------------------------------------------
# Global variable
PRODUCT_LIST = [
    'bs.HH', 'bs.VV', 'bs.HV', 'bs.VH',
    'coh.VV', 'coh.VH', 'coh.HH', 'coh.HV',
    'pol.Entropy', 'pol.Anisotropy', 'pol.Alpha'
]


def bursts_to_ards(
        burst_gdf,
        config_file,
        executor_type='concurrent_processes',
        max_workers=1
):

    print('--------------------------------------------------------------')
    logger.info('Processing all single bursts to ARD')
    print('--------------------------------------------------------------')

    logger.info('Preparing the processing pipeline. This may take a moment.')
    proc_inventory = prepare_burst_inventory(burst_gdf, config_file)

    with open(config_file, 'r') as file:
        config_dict = json.load(file)
    # we update max_workers in case we have less cpus_per_process
    # then cpus available
    if max_workers == 1 and config_dict['gpt_max_workers'] < os.cpu_count():
        max_workers = int(os.cpu_count() / config_dict['gpt_max_workers'])

    # now we run with godale, which works also with 1 worker
    executor = Executor(executor=executor_type, max_workers=max_workers)
    for task in executor.as_completed(
            func=burst_to_ard,
            iterable=proc_inventory.iterrows(),
            fargs=[str(config_file), ]
    ):
        task.result()
