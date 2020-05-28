import os
import logging

from godale._concurrent import Executor

from ost.s1.burst_to_ard import burst_to_ard
from ost.s1 import burst_inventory


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
        config_dict,
        executor_type='concurrent_processes',
        max_workers=1
):
    logger.info('Processing all single bursts to ARD')
    proc_inventory = burst_inventory.prepare_burst_inventory(burst_gdf, config_dict)

    # we update max_workers in case we have less gpt_max_workers
    # then cpus available
    if max_workers > os.cpu_count():
        max_workers = os.cpu_count()
    if max_workers == 1 or len(burst_gdf) == 1:
        config_dict['gpt_max_workers'] = os.cpu_count()
    elif max_workers <= os.cpu_count():
        config_dict['gpt_max_workers'] = int(os.cpu_count() / len(burst_gdf))
    elif len(burst_gdf) <= max_workers:
        config_dict['gpt_max_workers'] = int(max_workers / len(burst_gdf))
        max_workers = int(len(burst_gdf))

    out_files = {'bs': [], 'ls': [], 'coh': [], 'pol': []}
    # now we run with godale, which works also with 1 worker
    if max_workers == 1 or len(burst_gdf) == 1:
        for burst in proc_inventory.iterrows():
            burst_id, burst_date, out_bs, \
                out_ls, out_pol, out_coh, error = burst_to_ard(
                    burst=burst,
                    config_dict=config_dict
                )
            out_files['bs'].append(out_bs)
            out_files['ls'].append(out_ls)
            out_files['coh'].append(out_coh)
            out_files['pol'].append(out_pol)
    else:
        executor = Executor(executor=executor_type, max_workers=max_workers)
        for task in executor.as_completed(
                func=burst_to_ard,
                iterable=proc_inventory.iterrows(),
                fargs=[config_dict]
        ):
            burst_id, burst_date, out_bs, \
                out_ls, out_pol, out_coh, error = task.result()
            out_files['bs'].append(out_bs)
            out_files['ls'].append(out_ls)
            out_files['coh'].append(out_coh)
            out_files['pol'].append(out_pol)

    return out_files
