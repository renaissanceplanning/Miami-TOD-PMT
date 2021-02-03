"""
Created: October 2020
@Author: Alex Bell

If run as "main", the urban development boundary feature 
for Miami-Dade county is downloaded from a default URL
and saved to the "raw" data directory.
"""
# %% MAIN
import PMT_tools.PMT as PMT
from PMT_tools.PMT import arcpy
import sys
import traceback

# %%
if __name__ == "__main__":
    try:
        url = r"https://opendata.arcgis.com/datasets/a468dc11c02f4467ade836947627554b_0.geojson"
        out_file = PMT.makePath(PMT.RAW, "UrbanDevelopmentBoundary4.shp")
        ugb_gdf = PMT.fetchJsonUrl(url, out_file, is_spatial=True, overwrite=True)
        print("Download successful")
    except:
        # Get the traceback object
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]

        # Concatenate information together concerning the error into a message string
        pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
        msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"

        # Return python error messages for use in script tool or Python window
        arcpy.AddError(pymsg)
        arcpy.AddError(msgs)

        # Print Python error messages for use in Python / Python window
        print(pymsg)
        print(msgs)
