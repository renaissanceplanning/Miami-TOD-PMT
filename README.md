# Miami-TOD-PMT
Scripts and docs for the Miami-Dade TPO's TOD Performance Monitoring Toolkit
## Table of Contents
1. [Environment Setup](#environment-setup)
   1. [Conda Envs](#building-python-conda-environment)
      1. [Download Environment - pmt_download](#env-pmt_download)
      2. [Processing Environment - pmt_tools](#env-pmt_tools)
3. [Using the tools](#tool-usage)

## Environment Setup
### Assumptions
- ArcGIS Pro is installed in a standard location
    ex: C:\Program Files\ArcGIS\Pro
- ArcGIS Pro version > 2.7
- Familiarity with command line interactions

### Building python CONDA environment

##### **Environment:** `pmt_download`
```used for download procedures only```
1. Select Windows Start
2. Navigate to 'Python Command Prompt' under ArcGIS folder --> Open
3. In the command window run the below commands:
4. Create a new environment
    ```
    conda create --name %LocalAppData%\ESRI\conda\envs\pmt_download
    ```
4) Activate the newly created environment
    ```
    activate pmt_download
    ```
5) Install conda packages
    ```
    conda install geopandas pandas numpy scipy rtree=0.9.4
    * enter 'y/yes' and return when asked
    ```
6) Install pip packages
    ```
    pip install censusdata osmnx
    ```

#### **Environment:** `pmt_tools`
...repeat steps 1-3 from above

4) Clone the existing ArcGIS python default environment (_arcgispro-py3_)
    ```
    conda create --clone arcgispro-py3 --name %LocalAppData%\ESRI\conda\envs\pmt_tools
    * (this may take some time, so be patient)
    ```
4) Activate the newly created environment
    ```
    activate pmt_tools
    ```
5) Install conda packages using **conda-forge** channel
    ```
    conda install -c conda-forge momepy sphinx dask
    * enter 'y/yes' and return when asked
    * spyder is optional if you want to install a Data Science focused IDE
    ```
6) Install pip packages
    ```
    pip install simpledbf
    ```

### _WARNING:_
_If you have recently updated ArcGIS Pro to a new Major Version, you will need to remove the existing environment and recreate it using
steps 4-7 again._
- Remove env
    ```
    conda env remove -n pmt_tools
    ```
- Follow the above steps to recreate the environment

## Tool Usage
### Download Tools
1) 
2) activate pmt_download environment
```
conda activate pmt_download
```