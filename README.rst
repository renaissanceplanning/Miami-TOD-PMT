Introduction and Setup
-----------------------
Transit Oriented Communities (TOC) are a major focus of Miami-Dade County's regional growth management and
transportation strategy. TOCs focus new residential, commercial, office and institutional investments in areas
served by premium transit. The site created by these tools provide helpful information on TOC development in
the county across four key topics: economic development, urban form, regional travel, and local access.

The scripts download data resources and generate new data to monitor development, transportation infrastructure,
and travel patterns over time. The following project documents the environment, scripts and usage for the
Miami-Dade TPO's TOD Performance Monitoring Toolkit.

Installation and Automated Downloads
------------------------------------
#. `Environment Setup <#environment-setup>`_

   #. `Conda Envs <#building-python-conda-environment>`_

      #. `Download Environment - pmt_download <#pmt_download>`_
      #. `Processing Environment - pmt_tools <#pmt_tools>`_
      #. `Documentation Environment - pmt_docs <#pmt_docs>`_

#. `Using the tools <#tool-usage>`_

Environment Setup
-----------------
Assumptions
^^^^^^^^^^^
* ArcGIS Pro is installed in a standard location
    ex: C:\Program Files\ArcGIS\Pro
* ArcGIS Pro version > 2.7
* Familiarity with command line interactions

Building python CONDA environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``pmt_download``
"""""""""""""""""""""""""""""""""""""""""
``used for download procedures only``

    **Manual Install**

    #. Select Windows Start
    #. Navigate to 'Python Command Prompt' under ArcGIS folder --> Open
    #. In the command window run the below commands:
        #. Create a new environment

            .. code-block::

               conda create --name %LocalAppData%\\ESRI\\conda\\envs\\pmt_download
        #. Activate the newly created environment

            .. code-block::

               activate pmt_download
        #. Install conda packages

            .. code-block::

               conda install geopandas pandas numpy scipy rtree=0.9.4
               * enter 'y/yes' and return when asked
        #. Install pip packages

            .. code-block::

                pip install censusdata osmnx

    **Semi-Automated Install**

    ``Assumes Anaconda or Miniconda is installed``

    #. Select Windows Start
    #. Navigate to or search for `Anaconda Prompt`
    #. Change directory to pmt_code project

            .. code-block::

                cd /path/to/code
    #. Run the following commands

            .. code-block::

                conda install -c conda-forge mamba
                mamba env create -f environment_download.yml


``pmt_tools``
"""""""""""""""""""""""""""""""""""""""""

    **Manual Install**

    #. Select Windows Start
    #. Navigate to 'Python Command Prompt' under ArcGIS folder --> Open
    #. In the command window run the below commands:
        #. Clone the existing ArcGIS python default environment (\ *arcgispro-py3*\ )

            .. code-block::

                conda create --clone arcgispro-py3 --name %LocalAppData%\\ESRI\\conda\\envs\\pmt_tools
        #. Activate the newly created environment

            .. code-block::

                activate pmt_tools
        #. Install conda packages using **conda-forge** channel

            .. code-block::

               conda install -c conda-forge momepy sphinx dask
               * enter 'y/yes' and return when asked
               * spyder is optional if you want to install a Data Science focused IDE
        #. Install pip packages

            .. code-block::

                pip install simpledbf


    **Semi-Automated Install**

    ``Assumes Anaconda or Miniconda is installed``

    #. Select Windows Start
    #. Navigate to or search for `Anaconda Prompt`
    #. Change directory to pmt_code project

            .. code-block::

                cd /path/to/code_dir
    #. Run the following commands

            .. code-block::

                conda install -c conda-forge mamba
                mamba env create -f environment_processing.yml

*WARNING:*
^^^^^^^^^^^^^^

*If you have recently updated ArcGIS Pro to a new Major Version, you will need to remove the existing environment and recreate it using
steps 4-7 again if using the manual install method. If using the semi-automated method, check that the python version
in environment_process.yml matches the installation of Pro*


* Remove env

       conda env remove -n pmt_tools
* Follow the above steps to recreate the environment

``pmt_docs``
"""""""""""""""""""""""""""""""""""""""""
    ``used for generating documentation updates due to process enhancements or code changes``

        **Semi-Automated Install**

        ``Assumes Anaconda or Miniconda is installed``

        #. Select Windows Start
        #. Navigate to or search for `Anaconda Prompt`
        #. Change directory to pmt_code project

            .. code-block::

                cd /path/to/code_dir
        #. Run the following commands

            .. code-block::

                conda install -c conda-forge mamba
                mamba env create -f environment_docs.yml


Tool Usage
----------

Download Tools
^^^^^^^^^^^^^^

    #. follow steps 1-3 of manual environment setup processes to open correct command prompt
    #. open PMT_tools\config\download_config.py

        * verify all existing configuration variables are ready to use
    #. open PMT_tools\utils.py
        * verify DATA_ROOT variable is set correctly    (todo: allow setting DATA_ROOT in executable)
    #. activate pmt_download environment

        .. code-block::

         conda activate pmt_download

    #. run downloader script

        .. code-block::

         Usage: python downloader.py

          download all available datasources automagically and place them in the RAW folder by data category

         If flags are provided, individual download procedures will be run
          -s: setup_download_folder is run, building the base folder structure (--setup)
          -u: download_urls is run, grabbing all data available directly from a URL endpoint (--urls)
          -o: download_osm_data is run, pulling osm_networks and osm_builidng_footprints (--osm)
          -g: download_census_geo is run, pulling census geography data used in the tool (--census_geo)
          -c: download_commutes_data is run, pulling commute data for the tool (--commutes)
          -r: download_race_data is run, pulling race data for the tool (--race)
          -l: download_lodes_data is run, pulling jobs data for the tool (--lodes)

         Example Usage:

             python downloader.py -s -u [setup download folder and download url endpoints]****

