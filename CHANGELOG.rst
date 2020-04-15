#########
Changelog
#########


-----
0.9.5
-----
* godale for batch Download
* added get_bursts_by_polygon in s1.burst_inventory function
* added np_binary_erosion in helpers.raster function
* added Depre. Warning to PEPS and ONDA, scihub and ASF as default search and dl
* added a DownloadError as general custom DL error
* added SLC processing to the Sentinel1Scene class
    * also added a test for it
    * burst batch processing now returns dict with processed out_files
* GRD s1 scene now returns also "bs" and "ls" paths in dictionary
* also conversion of GRD and SLC to RGB GeoTiffs (core functions now in ost.s1.ard_to_rgb.py)
* defined a default OST Geotiff profile for rasterio in settings/py
* Project class now gets HERBERT for search and download as default
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