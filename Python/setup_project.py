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


# class Field(object):
#     def __init__(self, attribute, data_type):
#         self.attribute = attribute
#         self.data_type = data_type
#
#
# class FeatureClass(Field):
#     def __init__(self, feature_class, attributes, attribute, data_type):
#         super().__init__(attribute, data_type)
#         self.feature_class = feature_class
#         self.attributes = attributes
#
#
# class FeatureDataset(FeatureClass):
#     def __init__(self, feature_dataset, feature_classes, sr, feature_class, attributes, attribute, data_type):
#         super().__init__(feature_class, attributes, attribute, data_type)
#         self.feature_dataset = feature_dataset
#         self.feature_classes = feature_classes
#         self.spatial_ref = sr
#
#
# class GeoDB(object):
#     def __init__(self, gdb_name, feature_datasets, feature_classes):
#         self.gdb_name = gdb_name
#         self.feature_datasets = feature_datasets
#         self.feature_classes = feature_classes
#

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
                "Networks": {
                    "feature_classes": {
                        "edges": {"attribute_map": [()]},
                        "osm_ND_Junctions": {"attribute_map": [()]},
                        "walk_to_parks_MERGE": {
                            "attribute_map": [("To_Break", "TEXT")]
                        },
                        "walk_to_parks_NON_OVERLAP": {
                            "attribute_map": [("ToCumul_Minutes", "TEXT")]
                        },
                        "walk_to_stn_MERGE": {"attribute_map": [("To_Break", "TEXT")]},
                        "walk_to_stn_NON_OVERLAP": {
                            "attribute_map": [("ToCumul_Minutes", "TEXT")]
                        },
                    }
                },
                "Points": {
                    "feature_classes": {
                        "BikePedCrashes": {
                            "attribute_map": [
                                ("WEEK_DAY", "TEXT"),
                                ("INJSEVER", "TEXT"),
                                ("TRANS_TYPE", "TEXT"),
                            ]
                        },
                        "BuildingPermits": {
                            "attribute_map": [
                                ("FOLIO", "TEXT"),
                                ("STATUS", "TEXT"),
                                ("UNITS_VAL", "DOUBLE"),
                                ("COST", "DOUBLE"),
                                ("PED_ORIENT", "DOUBLE"),
                            ]
                        },
                        "TransitRidership": {
                            "attribute_map": [
                                ("DAY_OF_WEEK", "TEXT"),
                                ("ROUTE", "LONG"),
                                ("DIRECTION", "TEXT"),
                                ("ON", "DOUBLE"),
                                ("OFF", "DOUBLE"),
                            ]
                        },
                    }
                },
                "Polygons": {
                    "feature_classes": {
                        "BlockGroups": {
                            "attribute_map": [("GEOID10", "TEXT"), ("YEAR", "LONG")]
                        },
                        "Blocks": {
                            "attribute_map": [("GEOID10", "TEXT"), ("YEAR", "LONG")]
                        },
                        "MAZ": {"attribute_map": [("MAZ", "LONG")]},
                        "Parcels": {
                            "attribute_map": [
                                ("FOLIO", "TEXT"),
                                ("DOR_UC", "LONG"),
                                ("NO_RES_UNTS", "DOUBLE"),
                                ("TOT_LVG_AR", "DOUBLE"),
                                ("JV", "DOUBLE"),
                                ("TV_NSD", "DOUBLE"),
                                ("LND_SQFOOT", "DOUBLE"),
                            ]
                        },
                        "SummaryAreas": {
                            "attribute_map": [
                                ("Name", "TEXT"),
                                ("Corridor", "TEXT"),
                                ("RowID", "LONG"),
                                ("YEAR", "LONG"),
                            ]
                        },
                        "TAZ": {"attribute_map": [("TAZ", "TEXT"),]},
                    }
                },
            },
            "tables": {
                "access_maz": {"attribute_map": [()]},
                "EconDemo_parcels": {
                    "attribute_map": [
                        ("FOLIO", "TEXT"),
                        ("TotalEmployment", "DOUBLE"),
                        (),
                    ]  # CNS{Sector}, JobsBySector
                },
                "Enrichment_blockgroups": {"attribute_map": [("GEOID10", "TEXT")]},
                "LandUseCodes_parcels": {
                    "attribute_map": [
                        ("FOLIO", "TEXT"),
                        ("GN_VA_LU", "TEXT"),
                        ("RES_NRES", "TEXT"),
                    ]
                },
                "WalkTime_parcels": {
                    "attribute_map": [
                        ("FOLIO", "TEXT"),
                        ("MinTimeStn_walk", "DOUBLE"),
                        ("MinTimePark_walk", "DOUBLE"),
                        ("MinTimeStn_ideal", "DOUBLE"),
                        ("MinTimePark_ideal", "DOUBLE"),
                        ("BinStn_walk", "TEXT"),
                        ("BinPark_walk", "TEXT"),
                        ("BinStn_ideal", "TEXT"),
                        ("BinPark_ideal", "TEXT"),
                    ]
                },
            },
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
        arcpy.AddMessage(f"...creating PMT yearly GDB for {year}")
        gdb = f"{list(gdbs.keys())[1]}".replace("Year", str(year))
        fds = gdbs["PMT_Year"]["feature_datasets"].keys()
        gdb_path = arcpy.CreateFileGDB_management(
            out_folder_path=folder_path, out_name=gdb
        )
        for fd in fds:
            arcpy.AddMessage(f"\t- adding FeatureDataset: {fd}")
            arcpy.CreateFeatureDataset_management(
                out_dataset_path=gdb_path, out_name=fd, spatial_reference=sr
            )


def build_time_period_gdbs(folder_path):
    sr = GDB_CONFIG["spatial_reference"]
    gdbs = GDB_CONFIG["geodatabases"]

    # build time period gdb
    for gdb in list(gdbs.keys())[2:]:
        fds = gdbs[gdb]["feature_datasets"].keys()
        gdb_path = arcpy.CreateFileGDB_management(
            out_folder_path=folder_path, out_name=gdb
        )
        for fd in fds:
            arcpy.CreateFeatureDataset_management(
                out_dataset_path=gdb_path, out_name=fd, spatial_reference=sr
            )


# def validate_year_gdb()
if __name__ == "__main__":
    arcpy.env.overwriteOutput = True
    out_path = (
        r"C:\Users\V_RPG\OneDrive - Renaissance Planning Group\SHARE\PMT\Data\Temp"
    )

    # build_year_gdb(folder_path=out_path)
    build_time_period_gdbs(folder_path=out_path)
