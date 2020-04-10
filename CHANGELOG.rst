#########
Changelog
#########


-----
0.9.5
-----
* renamed the number of cores to be used for GPT and regular concurency to:
    * config_dict['gpt_max_workers']
    * config_dict['godale_max_workers']
* Layover shadow masks converted to gpkg vector file
* GRD batch processing now returns updated inventory with paths to ard products
* concurency with godale instead of multiprocessing
* add silent mode to ost.helpers.helpers run_command (no gpt output in stdout)
* burst_ts.py added it should contain all timeseries and timescan related functions
    * batch/cuncurency of these functions will be handled by burst_batch.py

-----
0.9.4
-----
* SLC burst processing there but in developement
* GRD processing there
* no tests
* pre 2020 version of the OST