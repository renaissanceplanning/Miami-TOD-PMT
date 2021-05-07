# Miami-TOD-PMT
Scripts and docs for the Miami-Dade TPO's TOD Performance Monitoring Toolkit
## Table of Contents
1. [Environment Setup](#environment-setup)
   2. [Conda Envs](#building-python-conda-environment)
2. 
## Environment Setup
##### Assumptions
- ArcGIS Pro is installed in a standard location
    ex: C:\Program Files\ArcGIS\Pro
- ArcGIS Pro version > 2.7
- Familiarity with command line interactions

##### Building python CONDA environment
###### Env: pmt_download
1. Select Windows Start
2. Navigate to 'Python Command Prompt' under ArcGIS folder --> Open
3. In the command window run the below commands:
4. _Create a new environment_
    ```
    conda create --name %LocalAppData%\ESRI\conda\envs\pmt_download
    ```
4) _Activate the newly created environment_
    ```
    activate pmt_download
    ```
5) _Install conda packages using **conda-forge** channel_
    ```
    conda install -c conda-forge osmnx geopandas pandas numpy scipy
    * enter 'y/yes' and return when asked
    ```
6) _Install pip packages_
    ```
    pip install censusdata
    ```
###### Env: pmt_tools
repeat steps 1-3 from above

4) _Clone the existing ArcGIS python environment_
    ```
    conda create --clone arcgispro-py3 --name %LocalAppData%\ESRI\conda\envs\pmt_tools

    * (this may take some time, so be patient)
    ```
4) _Activate the newly created environment_
    ```
    activate pmt_tools
    ```
5) _Install conda packages using **conda-forge** channel_
    ```
    conda install -c conda-forge momepy osmnx sphinx dask
    * enter 'y/yes' and return when asked
    * spyder is optional if you want to install a Data Science focused IDE
    ```
6) _Install pip packages_
    ```
    pip install esridump censusdata simpledbf
    ```
_WARNING_
If you have recently updated ArcGIS Pro to a new Major Version, you will need to remove the existing environment and recreate it using
steps 4-7 again.
- Remove env
    ```
    conda env remove -n pmt_tools
    ```
- Follow the above steps to recreate the environment
