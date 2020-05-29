#########
Changelog
#########

-----
0.9.7
-----
* added asf search
* set asf search as primary search
* remove get_paths from creo and onda DIAS from Sentinel1Scene class
* remove peps from s1scene
* remove onda and peps download fuctions as they are not tested
* updated SLC routine to current PhiLab version
* adjusted burst processing to EOX-A release
* adjusted ard_json for slc to match PhiLab
* added Coherence test and updateted polarimetric test

-----
0.9.6
-----
* added single geotiff output for GRD processing
* GRD batch returns updated inventory with processed products

-----
0.9.5
-----
* 2020-04-16
* temporary EOX version, core functions should work for both SLC and GRD
* Project GRD and burst processing goes up to (including) Timescans
    * Mosaic creation currently under construction and will raise Warning if triggered in the project
* changed retrying module to retry
    * reason: could find how to get log/error output of retrying module
    * Conda installation is far away
* updated Timeseries and Timescan for the current GRD processing
    * tests run only on 1 Product, TODO test on multiple
    * TODO test on burst(s)
* Timeseries extent.shp to extent.gpkg
* godale for batch Download
* added get_bursts_by_polygon in s1.burst_inventory function
* added np_binary_erosion in helpers.raster function
* added Depre. Warning to PEPS and ONDA, curently not availible
* scihub and ASF as default search and dl
* added a DownloadError as general custom DL error
* added SLC processing to the Sentinel1Scene class
    * also added a test for it
    * burst batch processing now returns dict with processed out_files
* GRD s1 scene now returns also "bs" and "ls" paths in dictionary
* also conversion of GRD and SLC to RGB GeoTiffs (core functions now in ost.s1.ard_to_rgb.py)
* defined a default OST Geotiff profile for rasterio in settings/py
* Project class now gets HERBERT for search and download as default
* renamed the number of cores to be used for GPT and regular concurency to:
    * config_dict['gpt_max_workers'] (down to the wrappers!)
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