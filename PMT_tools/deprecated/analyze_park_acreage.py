"""
Created: OCtober 2020
@Author: Alex Bell

....
We need summarize of parks in each station area per 1000 pop.
We need features that sum to park acreage in an arbitrary geography for spatial selection
We need features that sum to population (in thousands) for arbitrary geo for sp. selection
We need to summarize park acrage in station areas per 1000 pop in selected areas

... parks feature dataset
    - park_polygons (show on map)
    - park_points (summarize in indicators)
        :: total acrage
        :: acreage per 1K
    - (implies presence of socioeconomic_demographic parcel summaries)
    - for station rankings, reporting - parks and parcels are needed.
"""

# %% IMPORTS
import PMT
import arcpy


# %% GLOBALS


# %% FUNCTIONS

# %% MAIN
if __name__ == "__main__":
    station_areas = PMT.make_path(
        PMT.BASIC_FEATURES, "SMART_Plan_Station_Areas")
    for year in PMT.YEARS:
        print(year)
        out_fc = PMT.make_path(
            PMT.ROOT, f"PMT_{year}.gdb", "station_areas", "park_acreage")
        

