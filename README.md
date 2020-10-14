# Miami-TOD-PMT
Scripts and docs for the Miami-Dade TPO's TOD Performance Monitoring Toolkit

## Environment Setup
##### Assumptions
1) ArcGIS Pro is installed in a standard location
    ex: C:\Program Files\ArcGIS\Pro
2) Familiarity with command line interactions
    
##### Building python CONDA environment
- Select Windows Start 
- Navigate to 'Python Command Prompt' under ArcGIS folder --> Open
- In the command window run the below commands:
_Clone the existing ArcGIS python environment_
``` 
conda create --clone arcgispro-py3 --name %LocalAppData%\ESRI\conda\envs\pmt_tools
* (this may take some time, so be patient)
```
- _Activate the newly created environemnt_
```
activate pmt_tools
```
- _Update python to ensure its in sync with the arcgis pro need_
```
conda update python
```
- _Install conda packages using **conda-forge** channel_
```
conda install -c conda-forge momepy osmnx geopandas rasterio spyder sphinx rasterstats fiona 
* enter 'y/yes' and return when asked
* spyder is optional if you want to install a Data Science focused IDE
```
- _Install pip packages_
```
pip install esridump censusdata
```
        
        