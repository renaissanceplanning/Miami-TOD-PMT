Introduction and Setup
======================

Transit Oriented Communities (TOC) are a major focus of Miami-Dade
County's regional growth management and transportation strategy. TOCs
focus new residential, commercial, office and institutional investments
in areas served by premium transit. The site created by these tools
provide helpful information on TOC development in the county across four
key topics: economic development, urban form, regional travel, and local
access.

The scripts download data resources and generate new data to monitor
development, transportation infrastructure, and travel patterns over
time. The following project documents the environment, scripts and usage
for the Miami-Dade TPO's TOD Performance Monitoring Toolkit.

Installation and Automated Downloads
====================================

1.  [Environment Setup](#environment-setup)
    1.  [Conda Envs](#building-python-conda-environment)
        1.  [Download Environment - pmt\_download](#pmt_download)
        2.  [Processing Environment - pmt\_tools](#pmt_tools)
        3.  [Documentation Environment - pmt\_docs](#pmt_docs)

2.  [Using the tools](#tool-usage)

Environment Setup
=================

Assumptions
-----------

-   ArcGIS Pro is installed in a standard location
    :   ex: C:\Program Files\ArcGISPro

-   ArcGIS Pro version \> 2.7
-   Familiarity with command line interactions

Building python CONDA environment
---------------------------------

### `pmt_download`

`used for download procedures only`

> **Manual Install**
>
> 1.  Select Windows Start
> 2.  Navigate to 'Python Command Prompt' under ArcGIS folder --\> Open
> 3.  In the command window run the below commands:
>     :   1.  Create a new environment
>
>             >  
>             > conda create --name %LocalAppData%\\ESRI\\conda\\envs\\pmt_download
>             > 
>
>         2.  Activate the newly created environment
>
>             >  
>             > activate pmt_download
>             > 
>
>         3.  Install conda packages
>
>             >  
>             > conda install geopandas pandas numpy scipy rtree=0.9.4
>             > * enter 'y/yes' and return when asked
>             > 
>
>         4.  Install pip packages
>
>             >  
>             > pip install censusdata osmnx
>             > 
>
> **Semi-Automated Install**
>
> `Assumes Anaconda or Miniconda is installed`
>
> 1.  Select Windows Start
> 2.  Navigate to or search for Anaconda Prompt
> 3.  Change directory to pmt\_code project
>
>     >  
>     > cd /path/to/code
>     > ex: cd "C:\github\project\Miami-TOD-PMT"
>     > 
>
> 4.  Run the following commands
>
>     >  
>     > conda install -c conda-forge mamba
>     > mamba env create -f environment_download.yml
>     > 
>
### `pmt_tools`

> **Manual Install**
>
> 1.  Select Windows Start
> 2.  Navigate to 'Python Command Prompt' under ArcGIS folder --\> Open
> 3.  In the command window run the below commands:
> 
>         1.  Clone the existing ArcGIS python default environment
>             (*arcgispro-py3*)
>
>             >  
>             > conda create --clone arcgispro-py3 --name %LocalAppData%\\ESRI\\conda\\envs\\pmt_tools
>             > 
>
>         2.  Activate the newly created environment
>
>             >  
>             > activate pmt_tools
>             > 
>
>         3.  Install conda packages using **conda-forge** channel
>
>             >  
>             > conda install -c conda-forge momepy sphinx dask
>             > * enter 'y/yes' and return when asked
>             > * spyder is optional if you want to install a Data Science focused IDE
>             > 
>
>         4.  Install pip packages
>
>             >  
>             > pip install simpledbf
>             > 
>
> **Semi-Automated Install**
>
> `Assumes Anaconda or Miniconda is installed`
>
> 1.  Select Windows Start
> 2.  Navigate to or search for Anaconda Prompt
> 3.  Change directory to pmt_code project
>
>     >  
>     > cd /path/to/code_dir
>     > 
>
> 4.  Run the following commands
>
>     >  
>     > conda install -c conda-forge mamba

>     > mamba env create -f environment_processing.yml
>     > 
>
*WARNING:*
----------

*If you have recently updated ArcGIS Pro to a new Major Version, you
will need to remove the existing environment and recreate it using steps
4-7 again if using the manual install method. If using the
semi-automated method, check that the python version in
environment\_process.yml matches the installation of Pro*

-   Remove env

    > conda env remove -n pmt_tools

-   Follow the above steps to recreate the environment

### `pmt_docs`

> `used for generating documentation updates due to process enhancements or code changes`
>
> > **Semi-Automated Install**
> >
> > `Assumes Anaconda or Miniconda is installed`
> >
> > 1.  Select Windows Start
> > 2.  Navigate to or search for Anaconda Prompt
> > 3.  Change directory to pmt\_code project
> >
> >     >  
> >     > cd /path/to/code_dir
> >     > 
> >
> > 4.  Run the following commands
> >
> >     >  
> >     > conda install -c conda-forge mamba

> >     > mamba env create -f environment_docs.yml
> >     > 
> >
Tool Usage
==========

Download Tools
--------------

> 1.  follow steps 1-3 of manual environment setup processes to open
>     correct command prompt
> 2.  open PMT\_toolsconfigdownload\_config.py
>
>     > -   verify all existing configuration variables are ready to use
>
> 3.  open PMT\_toolsutils.py
>     :   -   verify DATA\_ROOT variable is set correctly (todo: allow
>             setting DATA\_ROOT in executable)
>
> 4.  activate pmt\_download environment
>
>     >  
>     > conda activate pmt_download
>     > 
>
> 5.  run downloader script
>
>     >  
>     > Usage: python downloader.py
>     >
>     >  download all available datasources automagically and place them in the RAW folder by data category
>     >
>     > If flags are provided, individual download procedures will be run
>     >  -s: setup_download_folder is run, building the base folder structure (--setup)
>     >  -u: download_urls is run, grabbing all data available directly from a URL endpoint (--urls)
>     >  -o: download_osm_data is run, pulling osm_networks and osm_builidng_footprints (--osm)
>     >  -g: download_census_geo is run, pulling census geography data used in the tool (--census_geo)
>     >  -c: download_commutes_data is run, pulling commute data for the tool (--commutes)
>     >  -r: download_race_data is run, pulling race data for the tool (--race)
>     >  -l: download_lodes_data is run, pulling jobs data for the tool (--lodes)
>     >
>     > Example Usage:
>     >
>     >     python downloader.py -s -u [will build the download folder structure and download url endpoints]****
>     > 
>

