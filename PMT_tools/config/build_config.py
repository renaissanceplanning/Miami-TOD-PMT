"""
The `build_config` module defines how feature classes are overlaid, summarized, and reported
in the TOC tool. All analysis and reporting executed by `builder` is driven by the settings
defined here. Most variables defined in this module focus on one of the following objectives:
    
    - Providing table or feature class specifications (path, key fields, etc.)
    - Defining feature intersection and summarization parameters using supporting classes defined in `PMT`
    - Defining field calculation formulas and code blocks
"""
import numpy as np

try:
    from PMT_tools.PMT import Column, DomainColumn, AggColumn, Consolidation, MeltColumn
    from PMT_tools.PMT import _make_access_col_specs
    from PMT_tools.config import prepare_config as pconfig
except:
    from pathlib import Path
    import sys
    root_path = Path(__file__).parents[1]
    print(root_path)
    sys.path.append(root_path)
    from PMT import(
        Column,
        DomainColumn,
        AggColumn,
        Consolidation,
        MeltColumn,
        _make_access_col_specs,
    )
    from . import prepare_config as pconfig

# GLOBALS
# SNAPSHOT_YEAR = PMT.YEARS[-1] # TODO: not a config item? always take a snapshot of each year in PMT.YEARS?
SUMMARY_AREAS_FINAL_KEY = "RowID"
MODES = ["Auto", "Transit", "Walk", "Bike"]
NM_MODES = ["W", "B"]
ACTIVITIES = ["TotalJobs", "ConsJobs", "HCJobs", "EnrollAdlt", "EnrollK12", "HH"]
TIME_BREAKS = [15, 30, 45, 60]
PAR_SUM_FIELDS = [
    "NO_RES_UNTS",
    "Total_Employment",
    "TOT_LVG_AREA",
    "JV",
    "TV_NSD",
    "LND_VAL",
    "LND_SQFOOT",
]


"""
#######################
Specifications for feature classes
Feature Class Name | Unique ID attribute | Feature Dataset Name
#######################
"""
# fc_name, id, FDS data resides
BLOCK_FC_SPECS = ("Census_Blocks", pconfig.BLOCK_COMMON_KEY, "Polygons")
PAR_FC_SPECS = ("Parcels", pconfig.PARCEL_COMMON_KEY, "Polygons")
MAZ_FC_SPECS = ("MAZ", pconfig.MAZ_COMMON_KEY, "Polygons")
TAZ_FC_SPECS = ("TAZ", pconfig.TAZ_COMMON_KEY, "Polygons")
SUM_AREA_FC_SPECS = ("SummaryAreas", pconfig.SUMMARY_AREAS_COMMON_KEY, "Polygons")
# SUM_AREA_FC_SPECS = ("SummaryAreas", SUMMARY_AREAS_FINAL_KEY, "Polygons")

NODES_FC_SPECS = ("nodes_bike", "NODE_ID", "Networks")  # TODO: define common key
TRANSIT_FC_SPECS = ("TransitRidership", pconfig.TRANSIT_COMMON_KEY, "Points",)
PARKS_FC_SPECS = ("Park_points", pconfig.PARK_POINTS_COMMON_KEY, "Points",)
# EDGES_FC_SPECS = ("edges_bike", "OBJECTID", "Networks")  # removed to utilize MD bike facility data
EDGES_FC_SPECS = ("bike_facilities", pconfig.BIKE_FAC_COMMON_KEY, "Networks")

FC_SPECS = [
    BLOCK_FC_SPECS,
    PAR_FC_SPECS,
    MAZ_FC_SPECS,
    TAZ_FC_SPECS,
    SUM_AREA_FC_SPECS,
    NODES_FC_SPECS,
    TRANSIT_FC_SPECS,
    PARKS_FC_SPECS,
    EDGES_FC_SPECS,
]

# fields and rename fields for MAZ/TAZ
MAZ_WALK_FIELDS, MAZ_WALK_RENAMES = _make_access_col_specs(
    activities=ACTIVITIES, time_breaks=TIME_BREAKS, mode="Walk"
)
MAZ_BIKE_FIELDS, MAZ_BIKE_RENAMES = _make_access_col_specs(
    activities=ACTIVITIES, time_breaks=TIME_BREAKS, mode="Bike"
)
TAZ_AUTO_FIELDS, TAZ_AUTO_RENAMES = _make_access_col_specs(
    activities=ACTIVITIES, time_breaks=TIME_BREAKS, mode="Auto"
)
TAZ_TRANSIT_FIELDS, TAZ_TRANSIT_RENAMES = _make_access_col_specs(
    activities=ACTIVITIES, time_breaks=TIME_BREAKS, mode="Transit"
)

"""
#######################
Specifications for tables
Table Name | Unique ID attribute | Fields Used | Rename Mapping
#######################
"""
TABLE_SPECS = [
    ("Access_maz_Walk", pconfig.MAZ_COMMON_KEY, MAZ_WALK_FIELDS, MAZ_WALK_RENAMES),
    ("Access_maz_Bike", pconfig.MAZ_COMMON_KEY, MAZ_BIKE_FIELDS, MAZ_BIKE_RENAMES),
    ("Access_taz_Auto", pconfig.TAZ_COMMON_KEY, TAZ_AUTO_FIELDS, TAZ_AUTO_RENAMES),
    ("Access_taz_Transit", pconfig.TAZ_COMMON_KEY, TAZ_TRANSIT_FIELDS, TAZ_TRANSIT_RENAMES,),
    ("Centrality_parcels", pconfig.PARCEL_COMMON_KEY, "*", {"CentIdx": "CentIdx_PAR"}),
    ("Contiguity_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
    ("Diversity_summaryareas", pconfig.SUMMARY_AREAS_COMMON_KEY, "*", {}),
    ("BikeFac_summaryareas", pconfig.SUMMARY_AREAS_COMMON_KEY, "*", {},),
    ("EconDemog_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
    # ("EnergyCons_parcels", pconfig.PARCEL_COMMON_KEY, "*"),   # dropped from project but left in config to allow calc
    ("Imperviousness_census_blocks", pconfig.BLOCK_COMMON_KEY, "*", {}),
    ("LandUseCodes_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
    ("TripStats_TAZ", pconfig.TAZ_COMMON_KEY, "*", {}),
    ("WalkTime_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
    ("WalkTimeIdeal_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
]

"""
#######################
enrichment var dicts
- Assumes all table results have been joined to corresponding feature classes
- (e.g., parcel tables in CLEANED gdbs are now joined to parcels fc in snapshot gdb)
sources: Summary geometry and disaggregation geometry pair ordered Summ, Disag
grouping: Attribute used to group the disaggregated data up to the summary geometry
agg_cols: columns to be aggregate up, including any renaming, optional aggregation method available (default=sum)
consolidate: new columns generated by consolidating a list of provided attributes, optional consolidation method (default=sum)
melt_cols: unused in the enrichment context, but used to elongate/unpivot tables across categorical columns provided
disagg_full_geometries: use all geometries or just those that overlap when performing intersection
#######################
"""
BLOCK_PAR_ENRICH = {
    "sources": (BLOCK_FC_SPECS, PAR_FC_SPECS),
    "grouping": Column(name=BLOCK_FC_SPECS[1]),
    "agg_cols": [
        AggColumn("Total_Population"),
        AggColumn("NO_RES_UNTS"),
        AggColumn("JV"),  # AggColumn("TOT_LVG_AREA")
        AggColumn("TV_NSD"),
        AggColumn("LND_VAL"),
        AggColumn("LND_SQFOOT"),
        AggColumn("Total_Commutes"),
        AggColumn("Drove_PAR", rename="Drove"),
        AggColumn("Carpool_PAR", rename="Carpool"),
        AggColumn("Transit_PAR", rename="Transit"),
        AggColumn("NonMotor_PAR", rename="NonMotor"),
        AggColumn("Work_From_Home_PAR", rename="Work_From_Home"),
        AggColumn("AllOther_PAR", rename="AllOther"),
        # AggColumn("BTU_RES"), AggColumn("NRES_BTU"),
        AggColumn(name="Developable_Area"),
        AggColumn(name="VAC_AREA"),
        AggColumn(name="RES_AREA"),
        AggColumn(name="NRES_AREA"),
        AggColumn(
            name="Max_Contiguity", agg_method=np.nanmedian, rename="Median_Contiguity"
        ),
        AggColumn(name="Max_Scaled_Area", rename="Scaled_Area"),
        AggColumn(name="Total_Employment"),
        AggColumn(name="CNS16_PAR", rename="HCJobs"),
        AggColumn(name="CNS15_PAR", rename="EdJobs"),
    ],
    "consolidate": [
        Consolidation(name="RsrcJobs", input_cols=["CNS01_PAR", "CNS02_PAR"]),
        Consolidation(
            name="IndJobs", input_cols=["CNS05_PAR", "CNS06_PAR", "CNS08_PAR"]
        ),
        Consolidation(
            name="ConsJobs", input_cols=["CNS07_PAR", "CNS17_PAR", "CNS18_PAR"]
        ),
        Consolidation(
            name="OffJobs",
            input_cols=[
                "CNS09_PAR",
                "CNS10_PAR",
                "CNS11_PAR",
                "CNS12_PAR",
                "CNS13_PAR",
                "CNS20_PAR",
            ],
        ),
        Consolidation(
            name="OthJobs",
            input_cols=["CNS03_PAR", "CNS04_PAR", "CNS14_PAR", "CNS19_PAR"],
        ),
    ],
    "melt_cols": [],
    "disag_full_geometries": False,
}
SA_PAR_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, PAR_FC_SPECS),
    "grouping": Column(name=SUM_AREA_FC_SPECS[1]),
    "agg_cols": [
        AggColumn(name=pconfig.PARCEL_COMMON_KEY, agg_method="size", rename="NParcels"),
        AggColumn(name="Total_Population"),
        AggColumn(name="NO_RES_UNTS"),
        AggColumn(name="TOT_LVG_AREA"),
        AggColumn(name="JV"),
        AggColumn(name="TV_NSD"),
        AggColumn(name="LND_VAL"),
        AggColumn(name="LND_SQFOOT"),
        AggColumn(name="Total_Commutes"),
        AggColumn(name="Drove_PAR", rename="Drove"),
        AggColumn(name="Carpool_PAR", rename="Carpool"),
        AggColumn(name="Transit_PAR", rename="Transit"),
        AggColumn(name="NonMotor_PAR", rename="NonMotor"),
        AggColumn(name="Work_From_Home_PAR", rename="Work_From_Home"),
        AggColumn(name="AllOther_PAR", rename="AllOther"),
        # AggColumn("BTU_RES"), AggColumn("NRES_BTU"),  # dropped from project but left in config to allow calc
        AggColumn(name="Developable_Area"),
        AggColumn(name="VAC_AREA"),
        AggColumn(name="RES_AREA"),
        AggColumn(name="NRES_AREA"),
        AggColumn(
            name="Max_Contiguity", agg_method=np.nanmedian, rename="Median_Contiguity"
        ),
        AggColumn(name="Max_Scaled_Area", rename="Scaled_Area"),
        AggColumn(name="min_time_stn_walk", agg_method="mean"),
        AggColumn(name="min_time_park_walk", agg_method="mean"),
        AggColumn(name="DirIdx_stn", agg_method=np.nanmedian),
        AggColumn(name="DirIdx_park", agg_method=np.nanmedian),
        AggColumn(name="stn_in_15"),
        AggColumn(name="park_in_15"),
        AggColumn(name="Total_Employment"),
        AggColumn(name="CNS16_PAR", rename="HCJobs"),
        AggColumn(name="CNS15_PAR", rename="EdJobs"),
    ],
    "consolidate": [
        Consolidation(name="RsrcJobs", input_cols=["CNS01_PAR", "CNS02_PAR"]),
        Consolidation(
            name="IndJobs", input_cols=["CNS05_PAR", "CNS06_PAR", "CNS08_PAR"]
        ),
        Consolidation(
            name="ConsJobs", input_cols=["CNS07_PAR", "CNS17_PAR", "CNS18_PAR"]
        ),
        Consolidation(
            name="OffJobs",
            input_cols=[
                "CNS09_PAR",
                "CNS10_PAR",
                "CNS11_PAR",
                "CNS12_PAR",
                "CNS13_PAR",
                "CNS20_PAR",
            ],
        ),
        Consolidation(
            name="OthJobs",
            input_cols=["CNS03_PAR", "CNS04_PAR", "CNS14_PAR", "CNS19_PAR"],
        ),
        Consolidation(
            name="CentIdxFA",
            input_cols=["TOT_LVG_AREA", "CentIdx_PAR"],
            cons_method=np.product,
        ),
    ],
    "melt_cols": [],
    "disag_full_geometries": False,
}
SA_BLOCK_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, BLOCK_FC_SPECS),
    "grouping": Column(name=SUM_AREA_FC_SPECS[1]),
    "agg_cols": [
        AggColumn(name=BLOCK_FC_SPECS[1], agg_method="size", rename="NBlocks"),
        AggColumn(name="TotalArea", rename="BlockArea"),
        AggColumn(name="NonDevArea"),
        AggColumn(name="DevOSArea"),
        AggColumn(name="DevLowArea"),
        AggColumn(name="DevMedArea"),
        AggColumn(name="DevHighArea"),
        AggColumn(name="IMP_PCT", agg_method="mean"),
    ],
    "consolidate": [
        Consolidation(
            name="BlockFlrAr",
            input_cols=["TotalArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="NonDevFlrAr",
            input_cols=["NonDevArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="DevOSFlrAr",
            input_cols=["DevOSArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="DevLowFlrAr",
            input_cols=["DevLowArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="DevMedFlrAr",
            input_cols=["DevMedArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="DevHiFlrAr",
            input_cols=["DevHighArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
    ],
    "melt_cols": [],
    "disag_full_geometries": False,
}
SA_MAZ_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, MAZ_FC_SPECS),
    "grouping": Column(name=SUM_AREA_FC_SPECS[1]),
    "agg_cols": [
        AggColumn(name=maz_col, agg_method="mean")
        for maz_col in MAZ_WALK_RENAMES.values()
    ]
    + [
        AggColumn(name=maz_col, agg_method="mean")
        for maz_col in MAZ_BIKE_RENAMES.values()
    ],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False,
}
SA_TAZ_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, TAZ_FC_SPECS),
    "grouping": Column(name=SUM_AREA_FC_SPECS[1]),
    "agg_cols": [
        AggColumn(name=taz_col, agg_method="mean")
        for taz_col in TAZ_AUTO_RENAMES.values()
    ]
    + [
        AggColumn(name=taz_col, agg_method="mean")
        for taz_col in TAZ_TRANSIT_RENAMES.values()
    ]
    + [
        AggColumn("VMT_ALL"),
        AggColumn("AVG_TIME_FROM", agg_method="mean"),
        AggColumn("AVG_DIST_FROM", agg_method="mean"),
        AggColumn("TRIPS_PER_ACT_FROM", agg_method="mean"),
        AggColumn("VMT_PER_ACT_FROM", agg_method="mean"),
        AggColumn("AVG_TIME_TO", agg_method="mean"),
        AggColumn("AVG_DIST_TO", agg_method="mean"),
        AggColumn("TRIPS_PER_ACT_TO", agg_method="mean"),
        AggColumn("VMT_PER_ACT_TO", agg_method="mean"),
    ],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False,
}
SA_NODES_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, NODES_FC_SPECS),
    "grouping": Column(name=SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn(name="CentIdx", agg_method="mean", rename="CentIdx_raw")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False,
}
SA_TRANSIT_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, TRANSIT_FC_SPECS),
    "grouping": Column(name=SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn(name="ON"), AggColumn(name="OFF"), AggColumn(name="TOTAL")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False,
}

BLOCK_TRANSIT_ENRICH = {
    "sources": (BLOCK_FC_SPECS, TRANSIT_FC_SPECS),
    "grouping": Column(name=BLOCK_FC_SPECS[1]),
    "agg_cols": [AggColumn(name="ON"), AggColumn(name="OFF"), AggColumn(name="TOTAL")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False,
}
SA_PARKS_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, PARKS_FC_SPECS),
    "grouping": Column(name=SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn(name="TOTACRE", rename="Park_Acres")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False,
}

ENRICH_INTS = [
    BLOCK_PAR_ENRICH,
    SA_PAR_ENRICH,
    SA_BLOCK_ENRICH,
    SA_MAZ_ENRICH,
    SA_TAZ_ENRICH,
    SA_NODES_ENRICH,
    SA_TRANSIT_ENRICH,
    BLOCK_TRANSIT_ENRICH,
    SA_PARKS_ENRICH,
]

"""
#######################
ELONGATE var dicts 
sources: Summary geometry and disaggregation geometry pair ordered Summ, Disag
grouping: Attribute used to group the disaggregated data up to the summary geometry
agg_cols: columns to be aggregate up, including any renaming, optional aggregation method available (default=sum)
consolidate: new columns generated by consolidating a list of provided attributes, optional consolidation method (default=sum)
melt_cols: used to elongate/unpivot tables across categorical columns provided
out_table: name of the table generated from elongation
#######################
"""
YEAR_COL = AggColumn(name="Year", agg_method="mean", default=-9999)
SA_GROUP_COLS = [
    Column(name=SUM_AREA_FC_SPECS[1]),
    Column(name="Name"),
    Column(name="Corridor"),
]

# ---------- DOMAIN DEFS
# --- these columns are added to allow us to sort categorical data within dashboards
LU_CAT_DOM = DomainColumn(
    name="LU_CAT_DOM",
    default=-1,
    domain_map={
        "Vacant/Undeveloped": 1,
        "Single-family": 2,
        "Multifamily": 3,
        "Industrial/Manufacturing": 4,
        "Commercial/Retail": 5,
        "Office": 6,
        "Other": 7,
    },
)
DEV_ST_DOM = DomainColumn(
    name="DEV_ST_DOM",
    default=-1,
    domain_map={
        "NonDevArea": 1,
        "DevOSArea": 2,
        "DevLowArea": 3,
        "DevMedArea": 4,
        "DevHighArea": 5,
    },
)
TRANSIT_DOM = DomainColumn(
    name="TRANSIT_DOM",
    default=-1,
    domain_map={
        "EARLY AM 02:45AM-05:59AM": 1,
        "AM PEAK 06:00AM-08:30AM": 2,
        "MIDDAY 08:30AM-02:59PM": 3,
        "PM PEAK 03:00PM-05:59PM": 4,
        "EVENING 06:00PM-07:59PM": 5,
        "LATE NIGHT 08:00PM-03:00AM": 6,
    },
)
WALK_DOM = DomainColumn(
    name="WALK_DOM",
    default=-1,
    domain_map={
        "0 to 5 minutes": 1,
        "5 to 10 minutes": 2,
        "10 to 15 minutes": 3,
        "15 to 20 minutes": 4,
        "20 to 25 minutes": 5,
        "25 to 30 minutes": 6,
    },
)

# ------------ Based on intersects
SA_PARCELS_LU_LONG = {
    "sources": (SUM_AREA_FC_SPECS, PAR_FC_SPECS),
    "grouping": SA_GROUP_COLS
    + [Column(name="GN_VA_LU", default="Unknown", domain=LU_CAT_DOM)],
    "agg_cols": [
        YEAR_COL,
        AggColumn(name="NO_RES_UNTS"),
        AggColumn(name="TOT_LVG_AREA"),
        AggColumn(name="JV"),
        AggColumn(name="TV_NSD"),
        AggColumn(name="LND_SQFOOT"),
        AggColumn(name="JV_SF", agg_method=np.nanmedian),
        AggColumn(name="TV_SF", agg_method=np.nanmedian),
        AggColumn(name="LV_SF", agg_method=np.nanmedian),
        # AggColumn("BTU_RES"), AggColumn("NRES_BTU")   # dropped from project but left in config to allow calc
    ],
    "consolidate": [
        Consolidation(
            name="CentIdxFA",
            input_cols=["TOT_LVG_AREA", "CentIdx_PAR"],
            cons_method=np.product,
        )
    ],
    "melt_cols": None,
    "out_table": "AttrByLU",
}
SA_PARCELS_COMMUTE_LONG = {
    "sources": (SUM_AREA_FC_SPECS, SUM_AREA_FC_SPECS),
    "grouping": SA_GROUP_COLS,
    "agg_cols": [YEAR_COL, AggColumn(name="Total_Commutes")],
    "consolidate": [],
    "melt_cols": MeltColumn(
        label_col="CommMode",
        val_col="Commutes",
        input_cols=[
            "Drove",
            "Carpool",
            "Transit",
            "NonMotor",
            "Work_From_Home",
            "AllOther",
        ],
    ),
    "out_table": "CommutesByMode",
}
SA_PARCELS_JSECTOR_LONG = {
    "sources": (SUM_AREA_FC_SPECS, SUM_AREA_FC_SPECS),
    "grouping": SA_GROUP_COLS,
    "agg_cols": [YEAR_COL, AggColumn(name="Total_Employment")],
    "consolidate": [],
    "melt_cols": MeltColumn(
        label_col="Sector",
        val_col="Jobs",
        input_cols=[
            "RsrcJobs",
            "IndJobs",
            "ConsJobs",
            "OffJobs",
            "EdJobs",
            "HCJobs",
            "OthJobs",
        ],
    ),
    "out_table": "JobsBySector",
}
SA_PARCELS_WALK_STA_LONG = {
    "sources": (SUM_AREA_FC_SPECS, PAR_FC_SPECS),
    "grouping": SA_GROUP_COLS
    + [
        Column(name="GN_VA_LU", default="Unknown", domain=LU_CAT_DOM),
        Column(name="bin_stn_walk", domain=WALK_DOM),
    ],
    "agg_cols": [
        YEAR_COL,
        AggColumn(name="TOT_LVG_AREA"),
        AggColumn(name="NO_RES_UNTS"),
        AggColumn(name=pconfig.PARCEL_COMMON_KEY, agg_method="size", rename="NParcels"),
        AggColumn(name="stn_in_15"),
    ],
    "consolidate": [],
    "melt_cols": None,
    "out_table": "WalkTimeToStations",
}
SA_PARCELS_WALK_PARK_LONG = {
    "sources": (SUM_AREA_FC_SPECS, PAR_FC_SPECS),
    "grouping": SA_GROUP_COLS + [Column(name="bin_park_walk", domain=WALK_DOM)],
    "agg_cols": [
        YEAR_COL,
        AggColumn(name="TOT_LVG_AREA"),
        AggColumn(name="NO_RES_UNTS"),
        AggColumn(name=pconfig.PARCEL_COMMON_KEY, agg_method="size", rename="NParcels"),
        AggColumn(name="park_in_15"),
    ],
    "consolidate": [],
    "melt_cols": None,
    "out_table": "WalkTimeToParks",
}
SA_BLOCK_DEV_STATUS_LONG = {
    "sources": (SUM_AREA_FC_SPECS, BLOCK_FC_SPECS),
    "grouping": SA_GROUP_COLS,
    "agg_cols": [YEAR_COL, AggColumn(name="TotalArea")],
    "consolidate": [
        Consolidation(
            name="BlockFlrAr",
            input_cols=["TotalArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="NonDevFlrAr",
            input_cols=["NonDevArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="DevOSFlrAr",
            input_cols=["DevOSArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="DevLowFlrAr",
            input_cols=["DevLowArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="DevMedFlrAr",
            input_cols=["DevMedArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
        Consolidation(
            name="DevHiFlrAr",
            input_cols=["DevHighArea", "TOT_LVG_AREA"],
            cons_method=np.product,
        ),
    ],
    "melt_cols": MeltColumn(
        label_col="DevStatus",
        val_col="Area",
        input_cols=[
            "NonDevArea",
            "DevOSArea",
            "DevLowArea",
            "DevMedArea",
            "DevHighArea",
        ],
        domain=DEV_ST_DOM,
    ),
    "out_table": "AreaByDevStatus",
}
SA_TRANSIT_LONG = {
    "sources": (SUM_AREA_FC_SPECS, TRANSIT_FC_SPECS),
    "grouping": SA_GROUP_COLS + [Column(name="TIME_PERIOD", domain=TRANSIT_DOM)],
    "agg_cols": [YEAR_COL, AggColumn(name="TOTAL")],
    "consolidate": [],
    "melt_cols": MeltColumn(
        label_col="ON_OFF", val_col="Value", input_cols=["ON", "OFF"]
    ),
    "out_table": "TransitByTimeOfDay",
}

BIKE_FAC_TYPE_COLS = [
    "Bike_Lane",
    "Paved_Path",
    "Sidewalk",
    "Wide_Curb_Lane",
    "Paved_Shoulder",
    "Sidepath",
]
SA_BIKE_LONG = {
    "sources": (SUM_AREA_FC_SPECS, SUM_AREA_FC_SPECS),
    "grouping": SA_GROUP_COLS,
    "agg_cols": [],  # [YEAR_COL, AggColumn(name="Bike_Miles")],
    "consolidate": [Consolidation(name="Total_Miles", input_cols=BIKE_FAC_TYPE_COLS)],
    "melt_cols": [
        MeltColumn(
            label_col="Bike_Fac",
            val_col="Bike_Miles",
            input_cols=BIKE_FAC_TYPE_COLS,
            agg_method=sum,
            default=0.0,
            domain=None,
        )
    ],
    "out_table": "BikeFacilityMilesByTier",
}
ELONGATE_SPECS = [
    SA_PARCELS_LU_LONG,
    SA_PARCELS_COMMUTE_LONG,
    SA_PARCELS_JSECTOR_LONG,  # SA_PARCELS_JTYPE_LONG
    SA_PARCELS_WALK_STA_LONG,
    SA_PARCELS_WALK_PARK_LONG,
    SA_BLOCK_DEV_STATUS_LONG,
    SA_TRANSIT_LONG,
    SA_BIKE_LONG,
]

"""
#######################
CALC FIELD SPECS 
- some data are calculated for dashboards due to limitations in the ArcGIS dashboard application not 
    being fully capable of on the fly calculations efficiently
    * includes CODE BLOCKS to functionalize commonly used calcs and handle NULL data
    * Calc Field Specs:
        tables: list of tables that will get the calculated field (different summarizations need the same value)
        new_field: field name for calulated value
        field_type: data type of the calculated value
        code_block: this can be explicit (string representation of a python function) or reference a reused code block
#######################
"""
# reused code blocks
DIVIDE_CODE_BLOCK = """
def divide(numerator, denominator):
    if None in [numerator, denominator]:
        return None
    elif denominator == 0:
        return None
    else:
        return numerator / denominator
    """
DIVIDE_TO_PERCENT_CODE_BLOCK = """
def divide(numerator, denominator):
    if None in [numerator, denominator]:
        return None
    elif denominator == 0:
        return None
    else:
        return (numerator / denominator) * 100
    """
DENSITY_CODE_BLOCK = """
def density(numerator, denominator, scalar):
    if None in [numerator, denominator]:
        return None
    elif denominator == 0:
        return None
    else:
        return numerator / (denominator / scalar)
    """
PER_SQFT_IDX_CODE_BLOCK = """
def val_per_sqft_idx(value, land_sqft):
    if None in [value, land_sqft]:
        return None
    else:
        return value / land_sqft
    """

NA_MODE_SHARE_CODE_BLOCK = """
def shr_na(transit, nonmotor, other, commutes):
    if None in [transit, nonmotor, other, commutes]:
        return 0
    elif commutes == 0:
        return 0
    else:
        return 100 * (transit + nonmotor + other) / commutes
    """
SUM_CODE_BLOCK = """
def sum(field_list):
    total = 0
    if None in field_list:
        return total
    else:
        for field in field_list:
            total += field
        return total
    """

# calc variables
RES_AREA = {
    "tables": [PAR_FC_SPECS],
    "new_field": "RES_AREA",
    "field_type": "FLOAT",
    "expr": "calc_area(!LND_SQFOOT!, !NO_RES_UNTS!)",
    "code_block": """
def calc_area(sq_ft, activity):
    if activity is None:
        return 0
    elif activity <= 0:
        return 0
    else:
        return sq_ft
    """,
}
NRES_AREA = {
    "tables": [PAR_FC_SPECS],
    "new_field": "NRES_AREA",
    "field_type": "FLOAT",
    "expr": "calc_area(!LND_SQFOOT!, !Total_Employment!)",
    "code_block": """
def calc_area(sq_ft, activity):
    if activity is None:
        return 0
    elif activity <= 0:
        return 0
    else:
        return sq_ft
    """,
}
VAC_AREA = {
    "tables": [PAR_FC_SPECS],
    "new_field": "VAC_AREA",
    "field_type": "FLOAT",
    "expr": "calc_area(!LND_SQFOOT!, !GN_VA_LU!)",  # Alternatively, define as all parcels with no building area?
    "code_block": """
def calc_area(sq_ft, lu):
    if lu is None:
        return 0
    elif lu == 'Vacant/Undeveloped':
        return sq_ft
    else:
        return 0
    """,
}
RES_DENS = {
    "tables": [PAR_FC_SPECS, BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "RES_DENS",
    "field_type": "FLOAT",
    "expr": "density(!NO_RES_UNTS!, !RES_AREA!, 43560.0)",
    "code_block": DENSITY_CODE_BLOCK,
}
NRES_DENS = {
    "tables": [PAR_FC_SPECS, BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "NRES_DENS",
    "field_type": "FLOAT",
    "expr": "density(!Total_Employment!, !RES_AREA!, 43560.0)",
    "code_block": DENSITY_CODE_BLOCK,
}
FAR_DENS = {
    "tables": [PAR_FC_SPECS, BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "FAR",
    "field_type": "FLOAT",
    "expr": "divide(!TOT_LVG_AREA!, !LND_SQFOOT!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
JH_RATIO = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "JHRatio",
    "field_type": "FLOAT",
    "expr": "divide(!Total_Employment!, !NO_RES_UNTS!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
GRID_DENS = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "GRID_DENS",
    "field_type": "FLOAT",
    "expr": "density(!NBlocks!, !LND_SQFOOT!, (43560.0 * 640.0))",  # Convert sq feet to sq miles via acres to sq miles
    "code_block": DENSITY_CODE_BLOCK,
}
NA_MODE_SHARE = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "SHR_NONAUTO",
    "field_type": "FLOAT",
    "expr": "shr_na(!Transit!, !NonMotor!, !AllOther!, !Total_Commutes!)",
    "code_block": NA_MODE_SHARE_CODE_BLOCK,
}
ACCESS_IN30 = {
    "params": [ACTIVITIES, [M[0] for M in MODES]],
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "{0}in30{1}",
    "field_type": "FLOAT",
    "expr": "sum(field_list=[!{0}15Min{1}!, !{0}30Min{1}!])",
    "code_block": SUM_CODE_BLOCK,
}
ACCESS_IN30_MAZ = {
    "params": [ACTIVITIES, NM_MODES],
    "tables": [MAZ_FC_SPECS],
    "new_field": "{0}in30{1}",
    "field_type": "FLOAT",
    "expr": "sum(field_list=[!{0}15Min{1}!, !{0}30Min{1}!])",
    "code_block": SUM_CODE_BLOCK,
}
NM_JH_BAL = {
    "params": [NM_MODES],
    "tables": [SUM_AREA_FC_SPECS, MAZ_FC_SPECS],
    "new_field": "{0}_JHBal",
    "field_type": "FLOAT",
    "expr": "divide(!TotalJobsin30{0}!, !HHin30{0}!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
DIRECT_IDX = {
    "params": [["stn", "park"]],
    "tables": [PAR_FC_SPECS],
    "new_field": "DirIdx_{0}",
    "field_type": "FLOAT",
    "expr": "dir_idx(!min_time_{0}_walk!, !min_time_{0}!)",
    "code_block": """
def dir_idx(walk_time, ideal_time):
    if ideal_time is None:
        return -1
    elif ideal_time == 0:
        return 1
    if walk_time is None:
        return -2
    elif walk_time == 0:
        return 1
    if walk_time < ideal_time:
        return 1
    else:
        return walk_time / ideal_time
    """,
}
SHR_RES_UNTS = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "SHR_RES_UNTS",
    "field_type": "FLOAT",
    "expr": "!NO_RES_UNTS! / {0}",
    "code_block": "",
}
SHR_JOBS = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "SHR_JOBS",
    "field_type": "FLOAT",
    "expr": "!Total_Employment! / {0}",
    "code_block": "",
}
SHR_LVG_AREA = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "SHR_LVG_AREA",
    "field_type": "FLOAT",
    "expr": "!TOT_LVG_AREA! / {0}",
    "code_block": "",
}
TV_SF = {
    "tables": [PAR_FC_SPECS],
    "new_field": "TV_SF",
    "field_type": "FLOAT",
    "expr": "divide(!TV_NSD!, !LND_SQFOOT!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
JV_SF = {
    "tables": [PAR_FC_SPECS],
    "new_field": "JV_SF",
    "field_type": "FLOAT",
    "expr": "divide(!JV!, !LND_SQFOOT!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
LV_SF = {
    "tables": [PAR_FC_SPECS],
    "new_field": "LV_SF",
    "field_type": "FLOAT",
    "expr": "divide(!LND_VAL!, !LND_SQFOOT!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
TV_IDX = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "TV_IDX",
    "field_type": "FLOAT",
    "expr": "(!TV_NSD! / !LND_SQFOOT!) / ({0}/{1})",
    "code_block": "",
}
JV_IDX = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "JV_IDX",
    "field_type": "FLOAT",
    "expr": "(!JV! / !LND_SQFOOT!) / ({0}/{1})",
    "code_block": "",
}
LV_IDX = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "LV_IDX",
    "field_type": "FLOAT",
    "expr": "(!LND_VAL! / !LND_SQFOOT!) / ({0}/{1})",
    "code_block": "",
}
IS_IN_15 = {
    "params": [["stn", "park"]],
    "tables": [PAR_FC_SPECS],
    "new_field": "{0}_in_15",
    "field_type": "FLOAT",
    "expr": "tag_in_15(!min_time_{0}_walk!)",
    "code_block": """
def tag_in_15(walk_time):
    if walk_time is None:
        return 0
    elif walk_time <= 15.0:
        return 1
    else:
        return 0
    """,
}
PROP_IN15 = {
    "params": [["stn", "park"]],
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "Prop_{0}15",
    "field_type": "FLOAT",
    "expr": "100 * (!{0}_in_15! / !NParcels!)",
    "code_block": "",
}
# BIKE_FAC = {
#     "tables": [EDGES_FC_SPECS],
#     "new_field": "Bike_Fac",
#     "field_type": "LONG",
#     "expr": "sum(field_list=[!bikability!, !cycleway!])",
#     "code_block": SUM_CODE_BLOCK
# }
# BIKE_MILES = {
#     "tables": [EDGES_FC_SPECS],
#     "new_field": "Bike_Miles",
#     "field_type": "FLOAT",
#     "expr": "!Length!/1609.344",  # meters to miles conversion
#     "code_block": ""
# }
TV_SF_AGG = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "TV_SF",
    "field_type": "FLOAT",
    "expr": "divide(!TV_NSD!, !LND_SQFOOT!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
JV_SF_AGG = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "JV_SF",
    "field_type": "FLOAT",
    "expr": "divide(!JV!, !LND_SQFOOT!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
LV_SF_AGG = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "LV_SF",
    "field_type": "FLOAT",
    "expr": "divide(!LND_VAL!, !LND_SQFOOT!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
CENT_IDX = {
    "tables": [SUM_AREA_FC_SPECS, (SA_PARCELS_LU_LONG["out_table"], "", "")],
    "new_field": "CentIdx",
    "field_type": "FLOAT",
    "expr": "divide(!CentIdxFA!, !TOT_LVG_AREA!)",
    "code_block": DIVIDE_CODE_BLOCK,
}
DEV_STAT_USE_FA_SHR = {
    "tables": [(SA_BLOCK_DEV_STATUS_LONG["out_table"], "", "")],
    "new_field": "FA_SHR",
    "field_type": "FLOAT",
    "expr": "fa_shr(!DevStatus!, !BlockFlrAr!, !NonDevFlrAr!, !DevOSFlrAr!, !DevLowFlrAr!, !DevMedFlrAr!, !DevHiFlrAr!)",
    "code_block": """
def fa_shr(dev_status, tot_fl_area, nd_area, os_area, lo_area, md_area, hi_area):
    if None in [dev_status, tot_fl_area, nd_area, os_area, lo_area, md_area, hi_area]:
        return 0
    if tot_fl_area == 0:
        return 0
    if dev_status == "NonDevArea":
        num = nd_area
    if dev_status == "DevOSArea":
        num = os_area
    if dev_status == "DevLowArea":
        num = lo_area
    if dev_status == "DevMedArea":
        num = md_area
    if dev_status == "DevHighArea":
        num = hi_area
    return num/tot_fl_area
""",
}
NONDEV_FA_SHR = {
    "tables": [SUM_AREA_FC_SPECS, (SA_BLOCK_DEV_STATUS_LONG["out_table"], "", "")],
    "new_field": "NONDEV_FA_SHR",
    "field_type": "FLOAT",
    "expr": "divide(!NonDevFlrAr!, !BlockFlrAr!)",
    "code_block": DIVIDE_TO_PERCENT_CODE_BLOCK,
}
DEVOS_FA_SHR = {
    "tables": [SUM_AREA_FC_SPECS, (SA_BLOCK_DEV_STATUS_LONG["out_table"], "", "")],
    "new_field": "DEVOS_FA_SHR",
    "field_type": "FLOAT",
    "expr": "divide(!DevOSFlrAr!, !BlockFlrAr!)",
    "code_block": DIVIDE_TO_PERCENT_CODE_BLOCK,
}
DEVLOW_FA_SHR = {
    "tables": [SUM_AREA_FC_SPECS, (SA_BLOCK_DEV_STATUS_LONG["out_table"], "", "")],
    "new_field": "DEVLOW_FA_SHR",
    "field_type": "FLOAT",
    "expr": "divide(!DevLowFlrAr!, !BlockFlrAr!)",
    "code_block": DIVIDE_TO_PERCENT_CODE_BLOCK,
}
DEVMED_FA_SHR = {
    "tables": [SUM_AREA_FC_SPECS, (SA_BLOCK_DEV_STATUS_LONG["out_table"], "", "")],
    "new_field": "DEVMED_FA_SHR",
    "field_type": "FLOAT",
    "expr": "divide(!DevMedFlrAr!, !BlockFlrAr!)",
    "code_block": DIVIDE_TO_PERCENT_CODE_BLOCK,
}
DEVHI_FA_SHR = {
    "tables": [SUM_AREA_FC_SPECS, (SA_BLOCK_DEV_STATUS_LONG["out_table"], "", "")],
    "new_field": "DEVHI_FA_SHR",
    "field_type": "FLOAT",
    "expr": "divide(!DevHiFlrAr!, !BlockFlrAr!)",
    "code_block": DIVIDE_TO_PERCENT_CODE_BLOCK,
}
PARKS_PER_CAP = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "PARK_AC_PER1000",
    "field_type": "FLOAT",
    "expr": "density(!Park_Acres!, !Total_Population!, 1000)",
    "code_block": DENSITY_CODE_BLOCK,
}
# region wide calculation specs
REG_REF_CALCS = [
    (SHR_RES_UNTS, "NO_RES_UNTS"),
    (SHR_JOBS, "Total_Employment"),
    (SHR_LVG_AREA, "TOT_LVG_AREA"),
    (TV_IDX, ["TV_NSD", "LND_SQFOOT"]),
    (JV_IDX, ["JV", "LND_SQFOOT"]),
    (LV_IDX, ["LND_VAL", "LND_SQFOOT"]),
]

PRECALCS = [
    VAC_AREA,
    RES_AREA,
    NRES_AREA,
    DIRECT_IDX,
    TV_SF,
    JV_SF,
    LV_SF,
    IS_IN_15,
    # BIKE_FAC, BIKE_MILES
]
CALCS = [
    RES_DENS,
    NRES_DENS,
    FAR_DENS,
    JH_RATIO,
    CENT_IDX,
    GRID_DENS,
    NA_MODE_SHARE,
    ACCESS_IN30,
    ACCESS_IN30_MAZ,
    NM_JH_BAL,
    PROP_IN15,
    TV_SF_AGG,
    JV_SF_AGG,
    LV_SF_AGG,
    DEV_STAT_USE_FA_SHR
    # NONDEV_FA_SHR,
    # DEVOS_FA_SHR,
    # DEVLOW_FA_SHR,
    # DEVMED_FA_SHR,
    # DEVHI_FA_SHR,
    # PARKS_PER_CAP,
]

""" TREND PARAMS """
STD_IDX_COLS = [pconfig.SUMMARY_AREAS_COMMON_KEY, "Name", "Corridor"]
# STD_IDX_COLS = [SUMMARY_AREAS_FINAL_KEY, "Name", "Corridor"]

ACC_IDX_COLS = ["Activity", "TimeBin", "from_time"]
AUTO_ACC_DIFF = {
    "table": "ActivityByTime_Auto",
    "index_cols": STD_IDX_COLS + ACC_IDX_COLS,
}
TRAN_ACC_DIFF = {
    "table": "ActivityByTime_Transit",
    "index_cols": STD_IDX_COLS + ACC_IDX_COLS,
}
BIKE_ACC_DIFF = {
    "table": "ActivityByTime_Bike",
    "index_cols": STD_IDX_COLS + ACC_IDX_COLS,
}
WALK_ACC_DIFF = {
    "table": "ActivityByTime_Walk",
    "index_cols": STD_IDX_COLS + ACC_IDX_COLS,
}
DEV_STATUS_DIFF = {
    "table": "AreaByDevStatus",
    "index_cols": STD_IDX_COLS + ["DevStatus", "DEV_ST_DOM"],
}
ATTR_LU_DIFF = {
    "table": "AttrByLU",
    "index_cols": STD_IDX_COLS + ["GN_VA_LU", "LU_CAT_DOM"],
}
BIKE_FAC_DIFF = {
    "table": "BikeFacilityMilesByTier",
    "index_cols": STD_IDX_COLS + ["Bike_Fac"],
}
COMMUTE_DIFF = {"table": "CommutesByMode", "index_cols": STD_IDX_COLS + ["CommMode"]}
JOBS_DIFF = {"table": "JobsBySector", "index_cols": STD_IDX_COLS + ["Sector"]}
TRAN_DIFF = {
    "table": "TransitByTimeOfDay",
    "index_cols": STD_IDX_COLS + ["TIME_PERIOD", "TRANSIT_DOM", "ON_OFF"],
}
WALK_PARK_DIFF = {
    "table": "WalkTimeToParks",
    "index_cols": STD_IDX_COLS + ["bin_park_walk", "WALK_DOM"],
}
WALK_STN_DIFF = {
    "table": "WalkTimeToStations",
    "index_cols": STD_IDX_COLS + ["GN_VA_LU", "bin_stn_walk", "LU_CAT_DOM", "WALK_DOM"],
}
DIFF_TABLES = [
    AUTO_ACC_DIFF,
    TRAN_ACC_DIFF,
    BIKE_ACC_DIFF,
    WALK_ACC_DIFF,
    DEV_STATUS_DIFF,
    ATTR_LU_DIFF,
    BIKE_FAC_DIFF,
    COMMUTE_DIFF,
    JOBS_DIFF,
    TRAN_DIFF,
    WALK_PARK_DIFF,
    WALK_STN_DIFF,
]

# Feature diffs
SUM_AREA_DIFF = {"table": SUM_AREA_FC_SPECS, "index_cols": STD_IDX_COLS}
BLOCK_DIFF = {"table": BLOCK_FC_SPECS, "index_cols": BLOCK_FC_SPECS[1]}
MAZ_DIFF = {"table": MAZ_FC_SPECS, "index_cols": pconfig.MAZ_COMMON_KEY}
TAZ_DIFF = {"table": TAZ_FC_SPECS, "index_cols": pconfig.TAZ_COMMON_KEY}
DIFF_FEATURES = [SUM_AREA_DIFF, BLOCK_DIFF, MAZ_DIFF, TAZ_DIFF]

# Long features
SUM_AREA_LONG = {"table": SUM_AREA_FC_SPECS, "index_cols": STD_IDX_COLS}
LONG_FEATURES = [SUM_AREA_LONG]
