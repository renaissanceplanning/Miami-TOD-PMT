"""
Author: Alex Bell

Download building permit data from local data sources and standardize
attributes.

Sources inlcude:
  - Miami-Dade County Building permits
      - Application Type
      - Property use
  - City of Miami
"""

# %% IMPORTS
import urllib3
import json
import pandas as pd
import geopandas as gpd
import os

# %% GLOBALS
# TODO: Replace globals with a config file or something?
permit_url = r"https://opendata.arcgis.com/datasets/31cd319f45544648b59f0418aea60091_0.geojson"
app_type_url = r"https://opendata.arcgis.com/datasets/cf047c70557a49e1a6848a62081bf93c_0.geojson"
prop_use_url = r"https://opendata.arcgis.com/datasets/5c1986f08a5a40b4be8a55418c833da8_0.geojson"
permit_a_code = "APPTYPE"
permit_p_code = "PROPUSE"
app_type_code = "CODE"
prop_use_code = "CODE"

permit_url_mia = r"https://opendata.arcgis.com/datasets/1d6fc60b087c4bcaa22345f429a2ec5a_0.geojson"

app_type_rename = {
    app_type_code : permit_a_code,
    "CODEDESC" : "app_type_desc"
}
prop_use_rename = {
    prop_use_code : permit_p_code,
    "CODEDESC" : "prop_use_desc"
}

# %% FUNCTIONS
def fetchJsonUrl(url, encoding="utf-8", is_spatial=False, crs="epsg:4326"):
    """
    Retrieve a json/geojson file at the given url and convert to a
    data frame or geodataframe.

    PARAMETERS
    -----------
    url: String
    encoding: String, default="uft-8"
    is_spatial: Boolean, default=False
        If True, dump json to geodataframe
    crs: String
    """
    http = urllib3.PoolManager()
    req = http.request("GET", url)
    req_json = json.loads(req.data.decode(encoding))
    gdf = gpd.GeoDataFrame.from_features(
            req_json["features"], crs=crs)
    if is_spatial:
        return gdf
    else:
        return pd.DataFrame(gdf.drop(columns="geometry"))

def enrichPermits_MD(permits, enrich_df, permit_code_field,
                     enrich_code_field):
    """
    Adds columns to a data frame of permits features based on a 
    related table provided as a data frame.

    Based on the construction of table from the Miami-Dade data portal
    at the time of development, this function leans on several assumptions:
      1. The field on which to join data in the permits data frame
        (`permit_code_field`) is stored as a string with leading zeros.
      2. The field on which to join data in the enrichment data frame
        (`enrich_code_field`) is stored as an integer.
      3. To succesfully merge these data frames, `permit_code_field` is cast
        to integer type in a new column.
      #1. The "application types" and "property uses" listed in the tables
      #   bearing these names are consistent and exhaustive. 

    PARAMETERS:
    ------------
    permits: GeoDataFrame
    enrich_df: DataFrame
    permit_code_field: String
        The name of the column in `permits` that relates its features to
        rows in `enrich_df`
    enrich_code_field: String
        The name of the column in `enrich_df` that relates its rows to
        features in `permits`.
    """
    # Calc int join field    
    permits[permit_code_field] = permits[permit_code_field].astype(int)
    # Embellish
    permits = permits.merge(
        enrich_df, 
        how="left", 
        left_on=permit_code_field,
        right_on=enrich_code_field
        )
    return permits

def fetchPermits_MD(permit_url, 
                    app_type_url=None,
                    prop_use_url=None, 
                    ):
    """
    Searches for permit point data at a given url and joins application
    type and property use attributes, if specified.

    PARAMETERS
    -------------
    permit_url: String
    app_type_url: String, default=None    
    prop_use_url: String, default=None
    """
    permits = fetchJsonUrl(permit_url, is_spatial=True)

    if app_type_url is not None:
        app_types = fetchJsonUrl(app_type_url)
    else:
        app_types = None
    if prop_use_url is not None:
        prop_use = fetchJsonUrl(prop_use_url)
    else:
        prop_use = None
    
    return permits, app_types, prop_use

# %% FETCH PERMITS
permits, app_types, prop_use = fetchPermits_MD(permit_url, app_type_url,
                                               prop_use_url=prop_use_url)

# %% STORE RAW DATA
permit_shape = r"..\Data\Raw\Permits\BuildingPermits_MD.shp"
app_type_table = r"..\Data\Raw\Permits\BuildingPermits_MD_AppType.csv"
prop_use_table = r"..\Data\Raw\Permits\BuildingPermits_MD_PropUse.csv"

permits.to_file(permit_shape)
app_types.to_csv(app_type_table)
prop_use.to_csv(prop_use_table)

# %% ENRICH PERMITS
# Clean up enrichment tables
app_types.rename(app_type_rename, axis=1, inplace=True)
prop_use.rename(prop_use_rename, axis=1, inplace=True)

app_type_cols = list(app_type_rename.values())
prop_use_cols = list(prop_use_rename.values())

permits_e = enrichPermits_MD(permits, app_types, permit_a_code, permit_a_code)
permits_e = enrichPermits_MD(permits_e, prop_use, permit_p_code, permit_p_code)

# %% CLEAN THE FINAL TABLE
#print(permits.dtypes)


# SUGGESTED CRITERIA:
#  TYPE == "BLDG"
#  ISSUDATE > 1/1/BaseYear
#  
#  (maybe find out what values in BPSTATUS mean?)



#  Convert ESTVALUE to integer?
