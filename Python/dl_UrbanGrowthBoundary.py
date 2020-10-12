"""
Created: October 2020
@Author: Alex Bell

If run as "main", the urban development boundary feature 
for Miami-Dade county is downloaded from a default URL
and saved to the "raw" data directory.
"""

import PMT
if __name__ == "__main__":
    url = r"https://opendata.arcgis.com/datasets/a468dc11c02f4467ade836947627554b_0.geojson"
    out_file = PMT.makePath(PMT.RAW, "UrbanDevelopmentBoundary.shp")
    ugb_gdf = PMT.fetchJsonUrl(url, is_spatial=True)
    ugb_gdf.to_file(out_file)
