# Miami-TOD-PMT
Scripts and docs for the Miami-Dade TPO's TOD Performance Monitoring Toolkit

## Environment Setup
##### - Assumptions -
    1) ArcGIS Pro is installed in a standard location
        ex: C:\Program Files\ArcGIS\Pro
    2) Familiarity with command line interactions
    
##### - Building python CONDA environment - 
    1) Select Windows Start 
    2) Navigate to 'Python Command Prompt' under ArcGIS folder --> Open
    3) In the command window run the below commands:
        _Clone the existing ArcGIS python environment_
        ``` 
        conda create --clone arcgispro-py3 --name %LocalAppData%\ESRI\conda\envs\pmt_tools
        ```
        _Activate the newly created environemnt_
        ```
        activate urban_morph
        ```
        _Install conda packages_
        ```
        conda install momepy osmnx geopandas 
        ```
            - enter 'y/yes' and return when asked
        _Install pip packages_
        ```
        pip install esridump
        ```
        
        