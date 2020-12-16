"""
Created: December 2020
@Author: Charles Rudder

Defines a series of setup functions to build databases from scratch

The GLOBAL dictionaries are also a tool to validate the expected final
datasets have been created
"""

import arcpy
import os, sys

# import PMT

# %% Globals
FL_STATE_PLANE_FT_EPSG = 2881

GDB_CONFIG = {
    "geodatabases": {
        "PMT_BasicFeatures": {
            "feature_datasets": {
                "BasicFeatures": {
                    "feature_classes": [
                        "Corridors",
                        "MiamiDadeCountyBoundary",
                        "SMARTplanAlignments",
                        "SMARTplanStations",
                        "StationAreas",
                        "StationsLong",
                        "SummaryAreas",
                        "UrbanGrowthBoundary",
                    ]
                }
            },
            "tables": [],
        },
        "PMT_Year": {
            "feature_datasets": {
                "CensusBlockGroups": {
                    "feature_classes": ["BgEnrichment", "BgAllocation"]
                },
                "Buildings": {"feature_classes": []},
                "Corridors": {"feature_classes": []},
                "Networks": {
                    "feature_classes": [
                        "BikeToParks_Merge",
                        "BikeToParks_NoMerge",
                        "BikeToParks_NonOverlap",
                        "BikeToParks_Overlap",
                        "BikeToStn_Merge",
                        "BikeToStn_NoMerge",
                        "BikeToStn_NonOverlap",
                        "BikeToStn_Overlap",
                        "WalkToParks_Merge",
                        "WalkToParks_NoMerge",
                        "WalkToParks_NonOverlap",
                        "WalkToParks_Overlap",
                        "WalkToStn_Merge",
                        "WalkToStn_NoMerge",
                        "WalkToStn_NonOverlap",
                        "WalkToStn_Overlap",
                    ]
                },
                "Parcels": {
                    "feature_classes": [
                        "ContiguityDevelopableArea",
                        "LandUseAndValue",
                        "SocioeconomicDemographics",
                        "WalkTime",
                    ]
                },
                "SafetySecurity": {"feature_classes": ["BikePedCrashes"]},
                "SERPM": {"feature_classes": []},
                "StationAreas": {"feature_classes": ["DiversityMetrics"]},
                "Transport": {"feature_classes": ["TransitRidership"]},
            },
            "tables": ["BlocksByFloorAreaByUse", "RegionalDiversity"],
        },
        "PMT_Snapshot": {
            "feature_datasets": {"CensusBlocks": {"feature_classes": ["EachBlock"]}},
            "tables": ["BlocksByFloorAreaByUse", "BlocksByModeChoice"],
        },
        "PMT_Trend": {
            "feature_datasets": {"CensusBlocks": {"feature_classes": ["EachBlock"]}},
            "tables": ["BlocksByYear", "BlockFloorAreaByUseByYear"],
        },
        "PMT_NearTerm": {
            "feature_datasets": {"Permit": {"feature_classes": ["EachPermit"]}},
            "tables": ["BlockByYear"],
        },
        "PMT_LongTerm": {
            "feature_datasets": {"CensusBlocks": {"feature_classes": []}},
            "tables": [],
        },
    },
    "spatial_reference": arcpy.SpatialReference(FL_STATE_PLANE_FT_EPSG),
}


def build_year_gdb(folder_path):
    sr = GDB_CONFIG["spatial_reference"]
    gdbs = GDB_CONFIG["geodatabases"]

    # build year gdbs
    for year in range(2014, 2020):
        arcpy.AddMessage(f'...creating PMT yearly GDB for {year}')
        gdb = f"{list(gdbs.keys())[1]}".replace("Year", str(year))
        fds = gdbs["PMT_Year"]["feature_datasets"].keys()
        gdb_path = arcpy.CreateFileGDB_management(
            out_folder_path=folder_path, out_name=gdb
        )
        for fd in fds:
            arcpy.AddMessage(f'\t- adding FeatureDataset: {fd}')
            arcpy.CreateFeatureDataset_management(
                out_dataset_path=gdb_path, out_name=fd, spatial_reference=sr
            )


def build_time_period_gdbs(folder_path):
    sr = GDB_CONFIG["spatial_reference"]
    gdbs = GDB_CONFIG["geodatabases"]

    # build time period gdb
    for gdb in list(gdbs.keys())[2:]:
        fds = gdbs[gdb]['feature_datasets'].keys()
        gdb_path = arcpy.CreateFileGDB_management(
            out_folder_path=folder_path, out_name=gdb
        )
        for fd in fds:
            arcpy.CreateFeatureDataset_management(
                out_dataset_path=gdb_path, out_name=fd, spatial_reference=sr
            )


if __name__ == "__main__":
    arcpy.env.overwriteOutput = True
    out_path = r"C:\Users\V_RPG\OneDrive - Renaissance Planning Group\SHARE\PMT\Data\Temp"

    # build_year_gdb(folder_path=out_path)
    build_time_period_gdbs(folder_path=out_path)
