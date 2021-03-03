# Miami-TOD-PMT
Scripts and docs for the Miami-Dade TPO's TOD Performance Monitoring Toolkit

## Environment Setup
##### Assumptions
- ArcGIS Pro is installed in a standard location
    ex: C:\Program Files\ArcGIS\Pro
- Familiarity with command line interactions

##### Building python CONDA environment
1) Select Windows Start 
2) Navigate to 'Python Command Prompt' under ArcGIS folder --> Open
3) In the command window run the below commands:
4) _Clone the existing ArcGIS python environment_
    ``` 
    conda create --clone arcgispro-py3 --name %LocalAppData%\ESRI\conda\envs\pmt_tools
    
    * (this may take some time, so be patient)
    ```
4) _Activate the newly created environment_
    ```
    activate pmt_tools
    ```
5) _Update python to ensure its in sync with the arcgis pro need_
    ```
    conda update python
    ```
6) _Install conda packages using **conda-forge** channel_
    ```
    conda install -c conda-forge momepy osmnx sphinx dask
    * enter 'y/yes' and return when asked
    * spyder is optional if you want to install a Data Science focused IDE
    ```
7) _Install pip packages_
    ```
    pip install esridump censusdata
    ```

#### _WARNING_
If you have recently updated ArcGIS Pro, you will need to remove the existing environment and recreate it.
- Remove env
    ```
    conda env remove -n pmt_tools
    ```
- Follow the above steps to recreate the environment

        
        
