==============
Miami-TOD-PMT
==============
Scripts and docs for the Miami-Dade TPO's TOD Performance Monitoring Toolkit

Environment Setup
-----------------

Assumptions
------------
- ArcGIS Pro is installed in a standard location
    ex: C:\Program Files\ArcGIS\Pro
- Familiarity with command line interactions

Building python CONDA environment
----------------------------------
1) Select Windows Start 
2) Navigate to 'Python Command Prompt' under ArcGIS folder --> Open
3) In the command window run the below commands:
4) Clone the existing ArcGIS python environment

..

    conda create --clone arcgispro-py3 --name %LocalAppData%\ESRI\conda\envs\pmt_tools

    * (this may take some time, so be patient)

4) Activate the newly created environment

..

    activate pmt_tools

5) Update python to ensure its in sync with the arcgis pro need

..

    conda update python

6) Install conda packages using **conda-forge** channel

..

    conda install -c conda-forge momepy osmnx geopandas rasterio spyder sphinx rasterstats fiona 

    * enter 'y/yes' and return when asked
    * spyder is optional if you want to install a Data Science focused IDE

7) Install pip packages

..

    pip install esridump censusdata

-------------------------------------------------------------------------------------------------

=======
WARNING
=======
If you have recently updated ArcGIS Pro, you will need to remove the existing environment and recreate it.
    - Remove env
        conda env remove -n pmt_tools

    - Follow the above steps to recreate the environment

        
        