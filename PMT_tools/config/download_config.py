from collections import OrderedDict

'''Crash Configuration'''
# download configuration
CRASHES_SERVICE = r'https://gis.fdot.gov/arcgis/rest/services/Crashes_All/FeatureServer/0/'
PED_BIKE_QUERY = {"where": "COUNTY_TXT = 'MIAMI-DADE' AND PEDESTRIAN_BICYCLIST_IND = 'Y'"}



''' Census Configuration'''
CENSUS_FTP_HOME = 'ftp://ftp2.census.gov/geo/tiger/TIGER2012/'
CENSUS_SCALE = "block group"
CENSUS_GEO_TYPES = ["tabblock", "bg"]
CENSUS_STATE = "FL"
CENSUS_COUNTY = "086"
ACS_RACE_TABLE = "B03002"
ACS_RACE_COLUMNS = {
    "002E": "Total_Non_Hisp",
    "012E": "Total_Hispanic",
    "003E": "White_Non_Hisp",
    "004E": "Black_Non_Hisp",
    "006E": "Asian_Non_Hisp",
    "009E": "Multi_Non_Hisp",
    "013E": "White_Hispanic",
    "014E": "Black_Hispanic",
    "016E": "Asian_Hispanic",
    "019E": "Multi_Hispanic"
}
ACS_MODE_TABLE = "B08301"
ACS_MODE_COLUMNS = {
    "001E": "Total_Commutes",
    "003E": "Drove_alone",
    "004E": "Carpool",
    "010E": "Transit",
    "016E": "Taxi",
    "017E": "Motorcycle",
    "018E": "Bicycle",
    "019E": "Walk",
    "020E": "Other",
    "021E": "Work_From_Home"
}

''' All downloadable URLs 
    - Impervious Surface data NLCD
    - Urban Growth Boundary
    - County Boundary
    - Various Parks data'''
DOWNLOAD_URL_DICT = {
    "Imperviousness": r"https://s3-us-west-2.amazonaws.com/mrlc/NLCD_2016_Impervious_L48_20190405.zip",
    "MD_Urban_Growth_Boundary": r"https://opendata.arcgis.com/datasets/a468dc11c02f4467ade836947627554b_0.geojson",
    "Miami-Dade_County_Boundary": r"https://opendata.arcgis.com/datasets/cec575982ea64ef7a11e587e532c6b6a_0.geojson",
    "Municipal_Parks": r"https://opendata.arcgis.com/datasets/16fe02a1defa45b28bf14a29fb5f0428_0.geojson",
    "County_Parks": r"https://opendata.arcgis.com/datasets/aca1e6ff0f634be282d50cc2d534a832_0.geojson",
    "Federal_State_Parks": r"https://opendata.arcgis.com/datasets/fa11a4c0a3554467b0fd5bc54edde4f9_0.geojson",
    "Park_Facilities": r"https://opendata.arcgis.com/datasets/8c9528d3e1824db3b14ed53188a46291_0.geojson"
}

''' Energy Consumption Table Download '''
RESIDENTIAL_ENERGY_CONSUMPTION_URL = r"https://www.eia.gov/consumption/residential/data/2015/c&e/ce4.9.xlsx"
COMMERCIAL_ENERGY_CONSUMPTION_URL = r"https://www.eia.gov/consumption/commercial/data/2012/c&e/xls/pba3.xlsx"

''' LODES Download '''
LODES_URL = "https://lehd.ces.census.gov/data/lodes/LODES7"
LODES_YEARS = [2014, 2015, 2016, 2017, 2018]
LODES_FILE_TYPES = ["od", "rac", "wac"]
LODES_STATES = ["al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl",
                "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
                "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne",
                "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
                "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt",
                "va", "wa", "wv", "wi", "wy", "dc"]
LODES_WORKFORCE_SEGMENTS = ["S000", "SA01", "SA02", "SA03", "SE01", "SE02", "SE03", "SI01", "SI02", "SI03", ""]
LODES_PART = ["main", "aux", ""]
LODES_JOB_TYPES = ["JT00", "JT01", "JT02", "JT03", "JT04", "JT05"]
LODES_AGG_GEOS = ["st", "cty", "trct", "bgrp", "cbsa", "zcta"]
