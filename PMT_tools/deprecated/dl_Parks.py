"""
Created: October 2020
@Author: Brian Froeb

Download park features
------------------------
This script defines a basic function for downloading parks features
including municipal, county, state, and national parks polygons and
park facility point features to the "raw" data folder.

If run as the "main" function, default parks URL's are downloaded.
"""

import geopandas as gpd
import PMT

def dl_parks(parks_url, out_path, out_name):
    """
    Read a url where park features are stored and save a shape file
    to the specified output location.

    Parameters
    ----------
    parks_url: String
    out_path: Path
    out_name: String

    Returns
    -------
    None
       A shape file of parks features is saved at `{out_path}/{out_name}`
    """
    parks = gpd.read_file(parks_url)
    out_file = PMT.make_path(out_path, out_name)
    parks.to_file(out_file)

if __name__ == "__main__":
    out_path = PMT.make_path(PMT.RAW, "Parks")
    # URLs
    muni_url = "https://opendata.arcgis.com/datasets/16fe02a1defa45b28bf14a29fb5f0428_0.geojson"
    county_url = "https://opendata.arcgis.com/datasets/aca1e6ff0f634be282d50cc2d534a832_0.geojson"
    state_fed_url = "https://opendata.arcgis.com/datasets/fa11a4c0a3554467b0fd5bc54edde4f9_0.geojson"
    park_fac_url = "https://opendata.arcgis.com/datasets/8c9528d3e1824db3b14ed53188a46291_0.geojson"
    urls = [muni_url, county_url, state_fed_url, park_fac_url]
    # Names
    muni_name="Municipal_Park.shp"
    county_name="County_Park.shp"
    state_fed_name="Nat_St_Park.shp"
    park_fac_name = "Park_Facility.shp"
    names = [muni_name, county_name, state_fed_name, park_fac_name]
    # Fetch
    for url, name in zip(urls, names):
           dl_parks(url, out_path, name)
