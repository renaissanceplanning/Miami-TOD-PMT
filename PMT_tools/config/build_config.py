from PMT_tools.PMT import Column, DomainColumn, AggColumn, Consolidation, MeltColumn
from PMT_tools.config import prepare_config as pconfig
import numpy as np

# GLOBALS
# SNAPSHOT_YEAR = PMT.YEARS[-1] not a config item? always take a snapshot of each year in PMT.YEARS?
MODES = ["Auto", "Transit", "Walk", "Bike"]
NM_MODES = ["W", "B"]
ACTIVITIES = ["TotalJobs", "ConsJobs", "HCJobs", "EnrollAdlt", "EnrollK12", "HH"]
TIME_BREAKS = [15, 30, 45, 60]
PAR_SUM_FIELDS = ["NO_RES_UNTS", "Total_Employment", "TOT_LVG_AREA", "JV", "TV_NSD", "LND_VAL", "LND_SQFOOT"]


def _makeAccessColSpecs(activities, time_breaks, mode, include_average=True):
    cols = []
    new_names = []
    for a in activities:
        for tb in time_breaks:
            col = f"{a}{tb}Min"
            cols.append(col)
            new_names.append(f"{col}{mode[0]}")
        if include_average:
            col = f"AvgMin{a}"
            cols.append(col)
            new_names.append(f"{col}{mode[0]}")
    renames = dict(zip(cols, new_names))
    return cols, renames

# TODO: check for misreferences to Blocks, or BlockGroups
# fc_name, id, FDS data resides
BLOCK_FC_SPECS = ("Census_Blocks", "GEOID10", "Polygons")  # TODO: define common key
PAR_FC_SPECS = ("Parcels", pconfig.PARCEL_COMMON_KEY, "Polygons")
MAZ_FC_SPECS = ("MAZ", pconfig.MAZ_COMMON_KEY, "Polygons")
TAZ_FC_SPECS = ("TAZ", pconfig.TAZ_COMMON_KEY, "Polygons")
SUM_AREA_FC_SPECS = ("SummaryAreas", pconfig.SUMMARY_AREAS_COMMON_KEY, "Polygons")
NODES_FC_SPECS = ("nodes_bike", "NODE_ID", "Networks")  # TODO: define common key
TRANSIT_FC_SPECS = ("TransitRidership", "OBJECTID", "Points")  # TODO: define common key
PARKS_FC_SPECS = ("Park_points", "OBJECTID_1", "Points")  # TODO: assign/define common key
EDGES_FC_SPECS = ("edges_bike", "OBJECTID", "Networks")  # TODO: define common key

FC_SPECS = [
    BLOCK_FC_SPECS, PAR_FC_SPECS, MAZ_FC_SPECS,
    TAZ_FC_SPECS, SUM_AREA_FC_SPECS, NODES_FC_SPECS,
    TRANSIT_FC_SPECS, PARKS_FC_SPECS, EDGES_FC_SPECS
]

# fields and rename fields for MAZ/TAZ
MAZ_WALK_FIELDS, MAZ_WALK_RENAMES = _makeAccessColSpecs(activities=ACTIVITIES, time_breaks=TIME_BREAKS, mode="Walk")
MAZ_BIKE_FIELDS, MAZ_BIKE_RENAMES = _makeAccessColSpecs(activities=ACTIVITIES, time_breaks=TIME_BREAKS, mode="Bike")
TAZ_AUTO_FIELDS, TAZ_AUTO_RENAMES = _makeAccessColSpecs(activities=ACTIVITIES, time_breaks=TIME_BREAKS, mode="Auto")
TAZ_TRANSIT_FIELDS, TAZ_TRANSIT_RENAMES = _makeAccessColSpecs(activities=ACTIVITIES, time_breaks=TIME_BREAKS,
                                                              mode="Transit")
# table_name, id, fields
TABLE_SPECS = [
    ("Access_maz_Walk", pconfig.MAZ_COMMON_KEY, MAZ_WALK_FIELDS, MAZ_WALK_RENAMES),
    ("Access_maz_Bike", pconfig.MAZ_COMMON_KEY, MAZ_BIKE_FIELDS, MAZ_BIKE_RENAMES),
    ("Access_taz_Auto", pconfig.TAZ_COMMON_KEY, TAZ_AUTO_FIELDS, TAZ_AUTO_RENAMES),
    ("Access_taz_Transit", pconfig.TAZ_COMMON_KEY, TAZ_TRANSIT_FIELDS, TAZ_TRANSIT_RENAMES),
    ("Contiguity_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
    ("Diversity_summaryareas", pconfig.SUMMARY_AREAS_COMMON_KEY, "*", {}),
    ("EconDemog_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
    # ("EnergyCons_parcels", pconfig.PARCEL_COMMON_KEY, "*"),
    ("Imperviousness_census_blocks", "GEOID10", "*", {}),  # TODO: BLOCK COMMON KEY?
    ("LandUseCodes_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
    ("TripStats_TAZ", pconfig.TAZ_COMMON_KEY, "*", {}),
    ("WalkTime_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
    ("WalkTimeIdeal_parcels", pconfig.PARCEL_COMMON_KEY, "*", {}),
]

# enrichment var dicts
# Assumes all table results have been joined to corresponding feature classes
# (e.g., parcel tables in CLEANED gdbs are now joined to parcels fc in snapshot gdb)
BLOCK_PAR_ENRICH = {
    "sources": (BLOCK_FC_SPECS, PAR_FC_SPECS),
    "grouping": Column(BLOCK_FC_SPECS[1]),
    "agg_cols":
        [AggColumn("Total_Population"), AggColumn("NO_RES_UNTS"), AggColumn("TOT_LVG_AREA"), AggColumn("JV"),
         AggColumn("TV_NSD"), AggColumn("LND_SQFOOT"), AggColumn("Total_Commutes"),
         AggColumn("Drove_PAR", rename="Drove"), AggColumn("Carpool_PAR", rename="Carpool"),
         AggColumn("Transit_PAR", rename="Transit"), AggColumn("NonMotor_PAR", rename="NonMotor"),
         AggColumn("Work_From_Home_PAR", rename="Work_From_Home"), AggColumn("AllOther_PAR", rename="AllOther"),
         # AggColumn("BTU_RES"), AggColumn("NRES_BTU"),
         AggColumn("Developable_Area"), AggColumn("VAC_AREA"), AggColumn("RES_AREA"), AggColumn("NRES_AREA"),
         AggColumn("Max_Contiguity", agg_method=np.nanmedian, rename="Median_Contiguity"),
         AggColumn("Max_Scaled_Area", rename="Scaled_Area"),
         AggColumn("Total_Employment"),
         AggColumn("CNS16_PAR", rename="HCJobs"), AggColumn("CNS15_PAR", rename="EdJobs")],
    "consolidate":
        [Consolidation("RsrcJobs", ["CNS01_PAR", "CNS02_PAR"]),
         Consolidation("IndJobs", ["CNS05_PAR", "CNS06_PAR", "CNS08_PAR"]),
         Consolidation("ConsJobs", ["CNS07_PAR", "CNS17_PAR", "CNS18_PAR"]),
         Consolidation("OffJobs", ["CNS09_PAR", "CNS10_PAR", "CNS11_PAR", "CNS12_PAR", "CNS13_PAR", "CNS20_PAR"]),
         Consolidation("OthJobs", ["CNS03_PAR", "CNS04_PAR", "CNS14_PAR", "CNS19_PAR"])],
    "melt_cols": [],
    "disag_full_geometries": False
}
SA_PAR_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, PAR_FC_SPECS),
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols":
        [AggColumn(pconfig.PARCEL_COMMON_KEY, agg_method="size", rename="NParcels"),
         AggColumn("Total_Population"), AggColumn("NO_RES_UNTS"), AggColumn("TOT_LVG_AREA"), AggColumn("JV"),
         AggColumn("TV_NSD"), AggColumn("LND_VAL"), AggColumn("LND_SQFOOT"),
         AggColumn("Total_Commutes"),
         AggColumn("Drove_PAR", rename="Drove"), AggColumn("Carpool_PAR", rename="Carpool"),
         AggColumn("Transit_PAR", rename="Transit"), AggColumn("NonMotor_PAR", rename="NonMotor"),
         AggColumn("Work_From_Home_PAR", rename="Work_From_Home"), AggColumn("AllOther_PAR", rename="AllOther"),
         # AggColumn("BTU_RES"), AggColumn("NRES_BTU"),
         AggColumn("Developable_Area"), AggColumn("VAC_AREA"), AggColumn("RES_AREA"), AggColumn("NRES_AREA"),
         AggColumn("Max_Contiguity", agg_method=np.nanmedian, rename="Median_Contiguity"),
         AggColumn("Max_Scaled_Area", rename="Scaled_Area"),
         AggColumn("min_time_stn_walk", agg_method="mean"), AggColumn("min_time_park_walk", agg_method="mean"),
         AggColumn("DirIdx_stn", agg_method=np.nanmedian), AggColumn("DirIdx_park", agg_method=np.nanmedian),
         AggColumn("stn_in_15"), AggColumn("park_in_15"),
         AggColumn("Total_Employment"), AggColumn("CNS16_PAR", rename="HCJobs"), AggColumn("CNS15_PAR", rename="EdJobs")],
    "consolidate":
        [Consolidation("RsrcJobs", ["CNS01_PAR", "CNS02_PAR"]),
         Consolidation("IndJobs", ["CNS05_PAR", "CNS06_PAR", "CNS08_PAR"]),
         Consolidation("ConsJobs", ["CNS07_PAR", "CNS17_PAR", "CNS18_PAR"]),
         Consolidation("OffJobs", ["CNS09_PAR", "CNS10_PAR", "CNS11_PAR", "CNS12_PAR", "CNS13_PAR", "CNS20_PAR"]),
         Consolidation("OthJobs", ["CNS03_PAR", "CNS04_PAR", "CNS14_PAR", "CNS19_PAR"])],
    "melt_cols": [],
    "disag_full_geometries": False
}
SA_BLOCK_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, BLOCK_FC_SPECS),
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn(BLOCK_FC_SPECS[1], agg_method="size", rename="NBlocks")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False
}
SA_MAZ_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, MAZ_FC_SPECS),
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn(maz_col, agg_method="mean") for maz_col in MAZ_WALK_RENAMES.values()] +
                [AggColumn(maz_col, agg_method="mean") for maz_col in MAZ_BIKE_RENAMES.values()],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False
}
SA_TAZ_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, TAZ_FC_SPECS),
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn(taz_col, agg_method="mean") for taz_col in TAZ_AUTO_RENAMES.values()] +
                [AggColumn(taz_col, agg_method="mean") for taz_col in TAZ_TRANSIT_RENAMES.values()] +
                [AggColumn("VMT"), AggColumn("TRAN_PMT"),
                 AggColumn("AVG_TIME_AU", agg_method="mean"),
                 AggColumn("AVG_DIST_AU", agg_method="mean"),
                 AggColumn("AVG_TIME_TR", agg_method="mean"),
                 AggColumn("AVG_DIST_TR", agg_method="mean")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False
}
SA_NODES_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, NODES_FC_SPECS),
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn("CentIdx", agg_method="mean")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False
}
SA_TRANSIT_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, TRANSIT_FC_SPECS),
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn("ON"), AggColumn("OFF"), AggColumn("TOTAL")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False
}
BLOCK_TRANSIT_ENRICH = {
    "sources": (BLOCK_FC_SPECS, TRANSIT_FC_SPECS),
    "grouping": Column(BLOCK_FC_SPECS[1]),
    "agg_cols": [AggColumn("ON"), AggColumn("OFF"), AggColumn("TOTAL")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False
}
SA_PARKS_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, PARKS_FC_SPECS),
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn("TOTACRE", rename="Park_Acres")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": False
}
SA_EDGES_ENRICH = {
    "sources": (SUM_AREA_FC_SPECS, EDGES_FC_SPECS),
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn("Bike_Miles")],
    "consolidate": [],
    "melt_cols": [],
    "disag_full_geometries": True
}
ENRICH_INTS = [BLOCK_PAR_ENRICH, SA_PAR_ENRICH, SA_BLOCK_ENRICH, SA_MAZ_ENRICH, SA_TAZ_ENRICH,
               SA_NODES_ENRICH, SA_TRANSIT_ENRICH, BLOCK_TRANSIT_ENRICH, SA_PARKS_ENRICH, SA_EDGES_ENRICH]

# elongate var dicts
YEAR_COL = AggColumn("Year", agg_method="mean", default=-9999)
SA_GROUP_COLS = [Column(SUM_AREA_FC_SPECS[1]), Column("Name"), Column("Corridor")]
# ---------- DOMAIN DEFS
LU_CAT_DOM = DomainColumn(
    name="LU_CAT_DOM",
    default=-1,
    domain_map={
        'Vacant/Undeveloped': 1,
        'Single-family': 2,
        'Multifamily': 3,
        'Industrial/Manufacturing': 4,
        'Commercial/Retail': 5,
        'Office': 6,
        'Other': 7
    }
)
DEV_ST_DOM = DomainColumn(
    name="DEV_ST_DOM",
    default=-1,
    domain_map={
        'NonDevArea': 1,
        'DevOSArea': 2,
        'DevLowArea': 3,
        'DevMedArea': 4,
        'DevHighArea': 5
    }
)
TRANSIT_DOM = DomainColumn(
    name="TRANSIT_DOM",
    default=-1,
    domain_map={
        'EARLY AM 02:45AM-05:59AM': 1,
        'AM PEAK 06:00AM-08:30AM': 2,
        'MIDDAY 08:30AM-02:59PM': 3,
        'PM PEAK 03:00PM-05:59PM': 4,
        'EVENING 06:00PM-07:59PM': 5,
        'LATE NIGHT 08:00PM-03:00AM': 6
    }
)
WALK_DOM = DomainColumn(
    name="WALK_DOM",
    default=-1,
    domain_map={
        '0 to 5 minutes': 1,
        '5 to 10 minutes': 2,
        '10 to 15 minutes': 3,
        '15 to 20 minutes': 4,
        '20 to 25 minutes': 5,
        '25 to 30 minutes': 6
    }
)

# ------------ Based on intersects
SA_PARCELS_LU_LONG = {
    "sources": (SUM_AREA_FC_SPECS, PAR_FC_SPECS),
    "grouping":
        SA_GROUP_COLS + [Column("GN_VA_LU", default="Unknown", domain=LU_CAT_DOM)],
    "agg_cols":
        [YEAR_COL, AggColumn("NO_RES_UNTS"), AggColumn("TOT_LVG_AREA"), AggColumn("JV"),
         AggColumn("TV_NSD"), AggColumn("LND_SQFOOT"), AggColumn("JV_SF", agg_method=np.nanmedian),
         AggColumn("TV_SF", agg_method=np.nanmedian), AggColumn("LV_SF", agg_method=np.nanmedian),
         # AggColumn("BTU_RES"), AggColumn("NRES_BTU")
         ],
    "consolidate": [],
    "melt_cols": None,
    "out_table": "AttrByLU"
}
SA_PARCELS_COMMUTE_LONG = {
    "sources": (SUM_AREA_FC_SPECS, SUM_AREA_FC_SPECS),
    "grouping": SA_GROUP_COLS,
    "agg_cols": [YEAR_COL, AggColumn("Total_Commutes")],
    "consolidate": [],
    "melt_cols": MeltColumn(label_col="CommMode", val_col="Commutes",
                           input_cols=["Drove", "Carpool", "Transit",
                                       "NonMotor", "Work_From_Home", "AllOther"]),
    "out_table": "CommutesByMode"
}
SA_PARCELS_JSECTOR_LONG = {
    "sources": (SUM_AREA_FC_SPECS, SUM_AREA_FC_SPECS),
    "grouping": SA_GROUP_COLS,
    "agg_cols": [YEAR_COL, AggColumn("Total_Employment")],
    "consolidate": [],
    "melt_cols": MeltColumn(label_col="Sector", val_col="Jobs",
                           input_cols=["RsrcJobs", "IndJobs", "ConsJobs", "OffJobs", "EdJobs", "HCJobs", "OthJobs"]),
    "out_table": "JobsBySector"
}
SA_PARCELS_WALK_STA_LONG = {
    "sources": (SUM_AREA_FC_SPECS, PAR_FC_SPECS),
    "grouping": SA_GROUP_COLS + [
        Column("GN_VA_LU", default="Unknown", domain=LU_CAT_DOM),
        Column("bin_stn_walk", domain=WALK_DOM)
    ],
    "agg_cols": [YEAR_COL, AggColumn("TOT_LVG_AREA"), AggColumn("NO_RES_UNTS"),
                    AggColumn(pconfig.PARCEL_COMMON_KEY, agg_method="size", rename="NParcels"),
                    AggColumn("stn_in_15")],
    "consolidate": [],
    "melt_cols": None,
    "out_table": "WalkTimeToStations"
}
SA_PARCELS_WALK_PARK_LONG = {
    "sources": (SUM_AREA_FC_SPECS, PAR_FC_SPECS),
    "grouping": SA_GROUP_COLS + [Column("bin_park_walk", domain=WALK_DOM)],
    "agg_cols": [YEAR_COL, AggColumn("TOT_LVG_AREA"), AggColumn("NO_RES_UNTS"),
                    AggColumn(pconfig.PARCEL_COMMON_KEY, agg_method="size", rename="NParcels"),
                    AggColumn("park_in_15")],
    "consolidate": [],
    "melt_cols": None,
    "out_table": "WalkTimeToParks"
}
SA_BLOCK_DEV_STATUS_LONG = {
    "sources": (SUM_AREA_FC_SPECS, BLOCK_FC_SPECS),
    "grouping": SA_GROUP_COLS,
    "agg_cols": [YEAR_COL, AggColumn("TotalArea")],
    "consolidate": [],
    "melt_cols": MeltColumn(label_col="DevStatus", val_col="Area",
                           input_cols=["NonDevArea", "DevOSArea", "DevLowArea", "DevMedArea", "DevHighArea"],
                           domain=DEV_ST_DOM),
    "out_table": "AreaByDevStatus"
}
SA_TRANSIT_LONG = {
    "sources": (SUM_AREA_FC_SPECS, TRANSIT_FC_SPECS),
    "grouping": SA_GROUP_COLS + [Column("TIME_PERIOD", domain=TRANSIT_DOM)],
    "agg_cols": [YEAR_COL, AggColumn("TOTAL")],
    "consolidate": [],
    "melt_cols": MeltColumn(label_col="ON_OFF", val_col="Value", input_cols=["ON", "OFF"]),
    "out_table": "TransitByTimeOfDay"
}
SA_BIKE_LONG = {
    "sources": (SUM_AREA_FC_SPECS, EDGES_FC_SPECS),
    "grouping": SA_GROUP_COLS + [Column("Bike_Fac")],
    "agg_cols": [YEAR_COL, AggColumn("Bike_Miles")],
    "consolidate": [],
    "melt_cols": None,
    "out_table": "BikeFacilityMilesByTier"
}
ELONGATE_SPECS = [SA_PARCELS_LU_LONG, SA_PARCELS_COMMUTE_LONG,
                  SA_PARCELS_JSECTOR_LONG,  # SA_PARCELS_JTYPE_LONG
                  SA_PARCELS_WALK_STA_LONG, SA_PARCELS_WALK_PARK_LONG,
                  SA_BLOCK_DEV_STATUS_LONG, SA_TRANSIT_LONG, SA_BIKE_LONG]

# CALC FIELD SPECS
RES_DENS = {
    "tables": [PAR_FC_SPECS, BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "RES_DENS",
    "field_type": "FLOAT",
    "expr": "!NO_RES_UNTS!/(!RES_AREA! / 43560.0)",
    "code_block": ""
}
NRES_DENS = {
    "tables": [PAR_FC_SPECS, BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "NRES_DENS",
    "field_type": "FLOAT",
    "expr": "!Total_Employment!/(!RES_AREA! / 43560.0)",
    "code_block": ""
}
FAR_DENS = {
    "tables": [PAR_FC_SPECS, BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "FAR",
    "field_type": "FLOAT",
    "expr": "!TOT_LVG_AREA!/!LND_SQFOOT!",
    "code_block": ""
}
JH_RATIO = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "JHRatio",
    "field_type": "FLOAT",
    "expr": "!Total_Employment!/!NO_RES_UNTS!",
    "code_block": ""
}
GRID_DENS = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "GRID_DENS",
    "field_type": "FLOAT",
    "expr": "!NBlocks!/(!LND_SQFOOT! / (43560.0 * 640.0))",  # Convert sq feet to sq miles via acres to sq miles
    "code_block": ""
}
NA_MODE_SHARE = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "SHR_NONAUTO",
    "field_type": "FLOAT",
    "expr": "100 * (!Transit! + !NonMotor! + !AllOther!)/!Total_Commutes!",
    "code_block": ""
}
ACCESS_IN30 = {
    "params": [ACTIVITIES, [M[0] for M in MODES]],
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "{0}in30{1}",
    "field_type": "FLOAT",
    "expr": "!{0}15Min{1}! + !{0}30Min{1}!",
    "code_block": ""
}
ACCESS_IN30_MAZ = {
    "params": [ACTIVITIES, NM_MODES],
    "tables": [MAZ_FC_SPECS],
    "new_field": "{0}in30{1}",
    "field_type": "FLOAT",
    "expr": "!{0}15Min{1}! + !{0}30Min{1}!",
    "code_block": ""
}
NM_JH_BAL = {
    "params": [NM_MODES],
    "tables": [SUM_AREA_FC_SPECS, MAZ_FC_SPECS],
    "new_field": "{0}_JHBal",
    "field_type": "FLOAT",
    "expr": "!TotalJobsin30{0}! / !HHin30{0}!",
    "code_block": ""
}
DIRECT_IDX = {
    "params": [["stn", "park"]],
    "tables": [PAR_FC_SPECS],
    "new_field": "DirIdx_{0}",
    "field_type": "FLOAT",
    "expr": "dir_idx(!min_time_{0}_walk!, !min_time_{0}!)",
    "code_block":
        """
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
    """
}
SHR_RES_UNTS = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "SHR_RES_UNTS",
    "field_type": "FLOAT",
    "expr": "!NO_RES_UNTS! / {0}",
    "code_block": ""
}
SHR_JOBS = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "SHR_JOBS",
    "field_type": "FLOAT",
    "expr": "!Total_Employment! / {0}",
    "code_block": ""
}
SHR_LVG_AREA = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "SHR_LVG_AREA",
    "field_type": "FLOAT",
    "expr": "!TOT_LVG_AREA! / {0}",
    "code_block": ""
}
TV_SF = {
    "tables": [PAR_FC_SPECS],
    "new_field": "TV_SF",
    "field_type": "FLOAT",
    "expr": "!TV_NSD! / !LND_SQFOOT!",
    "code_block": ""
}
JV_SF = {
    "tables": [PAR_FC_SPECS],
    "new_field": "JV_SF",
    "field_type": "FLOAT",
    "expr": "!JV! / !LND_SQFOOT!",
    "code_block": ""
}
LV_SF = {
    "tables": [PAR_FC_SPECS],
    "new_field": "LV_SF",
    "field_type": "FLOAT",
    "expr": "!LND_VAL! / !LND_SQFOOT!",
    "code_block": ""
}
TV_IDX = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "TV_IDX",
    "field_type": "FLOAT",
    "expr": "(!TV_NSD! / !LND_SQFOOT!) / ({0}/{1})",
    "code_block": ""
}
JV_IDX = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "JV_IDX",
    "field_type": "FLOAT",
    "expr": "(!JV! / !LND_SQFOOT!) / ({0}/{1})",
    "code_block": ""
}
LV_IDX = {
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "LV_IDX",
    "field_type": "FLOAT",
    "expr": "(!LND_VAL! / !LND_SQFOOT!) / ({0}/{1})",
    "code_block": ""
}
IS_IN_15 = {
    "params": [["stn", "park"]],
    "tables": [PAR_FC_SPECS],
    "new_field": "{0}_in_15",
    "field_type": "FLOAT",
    "expr": "tag_in_15(!min_time_{0}_walk!)",
    "code_block":
        """
def tag_in_15(walk_time):
    if walk_time is None:
        return 0
    elif walk_time <= 15.0:
        return 1
    else:
        return 0
    """
}
PROP_IN15 = {
    "params": [["stn", "park"]],
    "tables": [SUM_AREA_FC_SPECS],
    "new_field": "Prop_{0}15",
    "field_type": "FLOAT",
    "expr": "!{0}_in_15! / !NParcels!",
    "code_block": ""
}
BIKE_FAC = {
    "tables": [EDGES_FC_SPECS],
    "new_field": "Bike_Fac",
    "field_type": "LONG",
    "expr": "!bikability! + !cycleway!",
    "code_block": ""
}
BIKE_MILES = {
    "tables": [EDGES_FC_SPECS],
    "new_field": "Bike_Miles",
    "field_type": "FLOAT",
    "expr": "!Length!/1609.344",  # meters to miles conversion
    "code_block": ""
}
TV_SF_AGG = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "TV_SF",
    "field_type": "FLOAT",
    "expr": "!TV_NSD! / !LND_SQFOOT!",
    "code_block": ""
}
JV_SF_AGG = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "JV_SF",
    "field_type": "FLOAT",
    "expr": "!JV! / !LND_SQFOOT!",
    "code_block": ""
}
LV_SF_AGG = {
    "tables": [BLOCK_FC_SPECS, SUM_AREA_FC_SPECS],
    "new_field": "LV_SF",
    "field_type": "FLOAT",
    "expr": "!LND_VAL! / !LND_SQFOOT!",
    "code_block": ""
}

REG_REF_CALCS = [
    (SHR_RES_UNTS, "NO_RES_UNTS"),
    (SHR_JOBS, "Total_Employment"),
    (SHR_LVG_AREA, "TOT_LVG_AREA"),
    (TV_IDX, ["TV_NSD", "LND_SQFOOT"]),
    (JV_IDX, ["JV", "LND_SQFOOT"]),
    (LV_IDX, ["LND_VAL", "LND_SQFOOT"])
]

PRECALCS = [DIRECT_IDX, TV_SF, JV_SF, LV_SF, IS_IN_15, BIKE_FAC, BIKE_MILES]
CALCS = [RES_DENS, NRES_DENS, FAR_DENS, JH_RATIO, GRID_DENS, NA_MODE_SHARE,
         ACCESS_IN30, ACCESS_IN30_MAZ, NM_JH_BAL, PROP_IN15, TV_SF_AGG, JV_SF_AGG, LV_SF_AGG
         ]


## TREND PARAMS
STD_IDX_COLS = [pconfig.SUMMARY_AREAS_COMMON_KEY, "Name", "Corridor"]
ACC_IDX_COLS = ["Activity", "TimeBin"]
AUTO_ACC_DIFF = {
    "table": "ActivityByTime_Auto",
    "index_cols": STD_IDX_COLS + ACC_IDX_COLS
}
TRAN_ACC_DIFF = {
    "table": "ActivityByTime_Transit",
    "index_cols": STD_IDX_COLS + ACC_IDX_COLS
}
BIKE_ACC_DIFF = {
    "table": "ActivityByTime_Bike",
    "index_cols": STD_IDX_COLS + ACC_IDX_COLS
}
WALK_ACC_DIFF = {
    "table": "ActivityByTime_Walk",
    "index_cols": STD_IDX_COLS + ACC_IDX_COLS
}
DEV_STATUS_DIFF = {
    "table": "AreaByDevStatus",
    "index_cols": STD_IDX_COLS + ["DevStatus", "DEV_ST_DOM"]
}
ATTR_LU_DIFF = {
    "table": "AttrByLU",
    "index_cols": STD_IDX_COLS + ["GN_VA_LU", "LU_CAT_DOM"]
}
BIKE_FAC_DIFF = {
    "table": "BikeFacilityMilesByTier",
    "index_cols": STD_IDX_COLS + ["Bike_Fac"]
}
COMMUTE_DIFF = {
    "table": "CommutesByMode",
    "index_cols": STD_IDX_COLS + ["CommMode"]
}
JOBS_DIFF = {
    "table": "JobsBySector",
    "index_cols": STD_IDX_COLS + ["Sector"]
}
TRAN_DIFF = {
    "table": "TransitByTimeOfDay",
    "index_cols": STD_IDX_COLS + ["TIME_PERIOD", "TRANSIT_DOM", "ON_OFF"]
}
WALK_PARK_DIFF = {
    "table": "WalkTimeToParks",
    "index_cols": STD_IDX_COLS + ["bin_park_walk", "WALK_DOM"]
}
WALK_STN_DIFF = {
    "table": "WalkTimeToStations",
    "index_cols": STD_IDX_COLS + ["GN_VA_LU", "bin_stn_walk", "LU_CAT_DOM", "WALK_DOM"]
}
DIFF_TABLES = [AUTO_ACC_DIFF, TRAN_ACC_DIFF, BIKE_ACC_DIFF, WALK_ACC_DIFF,
                DEV_STATUS_DIFF, ATTR_LU_DIFF, COMMUTE_DIFF, JOBS_DIFF,
                TRAN_DIFF, WALK_PARK_DIFF, WALK_STN_DIFF]

# Feature diffs
SUM_AREA_DIFF = {
    "table": SUM_AREA_FC_SPECS,
    "index_cols": STD_IDX_COLS
}
BLOCK_DIFF = {
    "table": BLOCK_FC_SPECS,
    "index_cols": BLOCK_FC_SPECS[1]
}
MAZ_DIFF ={
    "table": MAZ_FC_SPECS,
    "index_cols": pconfig.MAZ_COMMON_KEY
}
TAZ_DIFF ={
    "table": TAZ_FC_SPECS,
    "index_cols": pconfig.TAZ_COMMON_KEY
}
DIFF_FEATURES = [SUM_AREA_DIFF, BLOCK_DIFF, MAZ_DIFF, TAZ_DIFF]

# Long features
SUM_AREA_LONG ={
    "table": SUM_AREA_FC_SPECS,
    "index_cols": STD_IDX_COLS
}
LONG_FEATURES = [SUM_AREA_LONG]