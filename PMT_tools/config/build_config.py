from PMT_tools.PMT import Column, AggColumn, Consolidation, MeltColumn
import numpy as np
from datetime import datetime

# GLOBALS

SNAPSHOT_YEAR = (datetime.now().year - 2)  # programatically getting year of most recent data
MODES = ["Auto", "Transit", "Walk", "Bike"]
ACTIVITIES = ["TotalJobs", "ConsJobs", "HCJobs", "EnrollAdlt", "EnrollK12", "HH"]
TIME_BREAKS = [15, 30, 45, 60]


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


# fc_name, id, FDS data resides
FC_SPECS = [("Blocks", "GEOID10", "Polygon"),
            ("Parcels", "FOLIO", "Polygon"),
            ("MAZ", "MAZ", "Polygon"),
            ("TAZ", "TAZ", "Polygon"),
            ("SummaryAreas", "RowID", "Polygon"),
            ("nodes_bike", "NODEID", "Network")]
BLOCK_FC_SPECS, PARCEL_FC_SPECS, MAZ_FC_SPECS, SUM_AREA_FC_SPECS, NODES_FC_SPECS = FC_SPECS

# fields and rename fields for MAZ/TAZ
MAZ_WALK_FIELDS, MAZ_WALK_RENAMES = _makeAccessColSpecs(ACTIVITIES, TIME_BREAKS, "Walk")
MAZ_BIKE_FIELDS, MAZ_BIKE_RENAMES = _makeAccessColSpecs(ACTIVITIES, TIME_BREAKS, "Bike")
TAZ_AUTO_FIELDS, TAZ_AUTO_RENAMES = _makeAccessColSpecs(ACTIVITIES, TIME_BREAKS, "Auto")
TAZ_TRANSIT_FIELDS, TAZ_TRANSIT_RENAMES = _makeAccessColSpecs(ACTIVITIES, TIME_BREAKS, "Transit")
# table_name, id, fields
TABLE_SPECS = [("Imperviousness_blocks", "GEIOID10", "*"),
               ("EnergyCons_parcels", "FOLIO", "*"),
               ("LandUseCodes_parcels", "FOLIO", "*"),
               ("WalkTime_parcels", "FOLIO", "*"),
               ("Contiguity_parcels", "FOLIO", "*"),
               ("access_maz_Walk", "MAZ", MAZ_WALK_FIELDS),
               ("access_maz_Bike", "MAZ", MAZ_BIKE_FIELDS),
               ("access_taz_Walk", "TAZ", TAZ_AUTO_FIELDS),
               ("access_taz_Bike", "TAZ", TAZ_TRANSIT_FIELDS),
               ("TripStats_TAZ", "TAZ"),
               ("NetworkCentrality_nodes_bike", "Node", "*"),
               ("Diversity_summaryareas", "RowID_", "*")]

# enrichment var dicts
BLOCK_PAR_ENRICH = {
    "grouping": Column(BLOCK_FC_SPECS[1]),
    "agg_cols":
        [AggColumn("NO_RES_UNTS"), AggColumn("TOT_LVG_AREA"), AggColumn("JV"),
         AggColumn("TV_NSD"), AggColumn("LND_SQFOOT"), AggColumn("Total_Commutes"),
         AggColumn("Drove"), AggColumn("Carpool"), AggColumn("Transit"),
         AggColumn("NonMotor"), AggColumn("Work_From_Home"), AggColumn("AllOther"),
         AggColumn("BTU_RES"), AggColumn("NRES_BTU"), AggColumn("Developable_Area"),
         AggColumn("Mean_Contiguity", agg_method="mean"), AggColumn("Mean_Scaled_Area", agg_method="mean"),
         AggColumn("Median_Scaled_Area", agg_method=np.median), AggColumn("Total_Employment"),
         AggColumn("CNS16", rename="HCJobs"), AggColumn("CNS15", rename="EdJobs")],
    "consolidate":
        [Consolidation("RsrcJobs", ["CNS01", "CNS02"]),
         Consolidation("IndJobs", ["CNS05", "CNS06", "CNS08"]),
         Consolidation("ConsJobs", ["CNS07", "CNS17", "CNS18"]),
         Consolidation("OffJobs", ["CNS09", "CNS10", "CNS11", "CNS12", "CNS13", "CNS20"]),
         Consolidation("OthJobs", ["CNS03", "CNS04", "CNS14", "CNS19"])],
    "melt_cols": []
}
SA_PAR_ENRICH = {
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols":
        [AggColumn("NO_RES_UNTS"), AggColumn("TOT_LVG_AREA"), AggColumn("JV"),
         AggColumn("TV_NSD"), AggColumn("LND_SQFOOT"), AggColumn("Total_Commutes"),
         AggColumn("Drove"), AggColumn("Carpool"), AggColumn("Transit"),
         AggColumn("NonMotor"), AggColumn("Work_From_Home"), AggColumn("AllOther"),
         AggColumn("BTU_RES"), AggColumn("NRES_BTU"), AggColumn("Developable_Area"),
         AggColumn("Mean_Contiguity", agg_method="mean"), AggColumn("Mean_Scaled_Area", agg_method="mean"),
         AggColumn("Median_Scaled_Area", agg_method=np.median), AggColumn("Max_Scaled_Area", agg_method="max"),
         AggColumn("MinTimeStn_walk", agg_method="mean"), AggColumn("MinTimePark_walk", agg_method="mean"),
         AggColumn("MinTimeStn_ideal", agg_method="mean"), AggColumn("MinTimeParks_ideal", agg_method="mean"),
         AggColumn("NStn_walk", agg_method="mean"), AggColumn("NPark_walk", agg_method="mean"),
         AggColumn("Total_Employment"), AggColumn("CNS16", rename="HCJobs"), AggColumn("CNS15", rename="EdJobs")],
    "consolidate":
        [Consolidation("RsrcJobs", ["CNS01", "CNS02"]),
         Consolidation("IndJobs", ["CNS05", "CNS06", "CNS08"]),
         Consolidation("ConsJobs", ["CNS07", "CNS17", "CNS18"]),
         Consolidation("OffJobs", ["CNS09", "CNS10", "CNS11", "CNS12", "CNS13", "CNS20"]),
         Consolidation("OthJobs", ["CNS03", "CNS04", "CNS14", "CNS19"])],
    "melt_cols": [],
}
SA_MAZ_ENRICH = {
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn(maz_col, agg_method="mean") for maz_col in MAZ_WALK_RENAMES.values()] +
                [AggColumn(maz_col, agg_method="mean") for maz_col in MAZ_BIKE_RENAMES.values()],
    "consolidation": [],
    "melt_cols": []
}
SA_TAZ_ENRICH = {
    "grouping": Column(SUM_AREA_FC_SPECS[1]),
    "agg_cols": [AggColumn(taz_col, agg_method="mean") for taz_col in TAZ_AUTO_RENAMES] +
                [AggColumn(taz_col, agg_method="mean") for taz_col in TAZ_TRANSIT_RENAMES] +
                [AggColumn("VMT"), AggColumn("TRAN_PMT"),
                 AggColumn("AVG_TIME_AU", agg_method="mean"),
                 AggColumn("AVG_DIST_AU", agg_method="mean"),
                 AggColumn("AVG_TIME_TR", agg_method="mean"),
                 AggColumn("AVG_DIST_TR", agg_method="mean")],
    "consolidate": [],
    "melt_cols": []
}

# elongate var dicts
#TODO: manage YEAR_COL better to allow for processing multiple years, add year as input?
YEAR_COL = AggColumn("Year", agg_method="mean", default=SNAPSHOT_YEAR)
SA_GROUP_COLS = [Column(SUM_AREA_FC_SPECS[1]), Column("Name"), Column("Corridor")]
SA_PARCELS_LU_LONG = {
    "grouping":
        SA_GROUP_COLS + [Column("GN_VA_LU", default="Unknown")],
    "agg_cols":
        [YEAR_COL, AggColumn("NO_RES_UNTS"), AggColumn("TOT_LVG_AREA"), AggColumn("JV"),
         AggColumn("TV_NSD"), AggColumn("LND_SQFOOT"), AggColumn("BTU_RES"), AggColumn("NRES_BTU")],
    "consolidate": [],
    "melt_cols": None
}
SA_PARCELS_COMMUTE_LONG = {
    "grouping":
        SA_GROUP_COLS,
    "agg_cols": [YEAR_COL, AggColumn("Total_Commutes")],
    "consolidate": [],
    "melt_col": MeltColumn(label_col="CommMode", val_col="Commutes",
                           input_cols=["Drove", "Carpool", "Transit",
                                       "NonMotor", "Work_From_Home", "AllOther"])
}
SA_PARCELS_JTYPE_LONG = {
    "grouping": Column(PARCEL_FC_SPECS[1]),
    "aggregation": [AggColumn("CNS16", rename="HCJobs"), AggColumn("CNS15", rename="EdJobs")],
    "consolidate":
        [Consolidation("RsrcJobs", ["CNS01", "CNS02"]), Consolidation("IndJobs", ["CNS05", "CNS06", "CNS08"]),
         Consolidation("ConsJobs", ["CNS07", "CNS17", "CNS18"]),
         Consolidation("OffJobs", ["CNS09", "CNS10", "CNS11", "CNS12", "CNS13", "CNS20"]),
         Consolidation("OthJobs", ["CNS03", "CNS04", "CNS14", "CNS19"])],
    "melt_col": None
}
SA_PARCELS_JSECTOR_LONG = {
    "grouping": SA_GROUP_COLS,
    "aggregation": [YEAR_COL, AggColumn("Total_Employment")],
    "consolidate": [],
    "melt_col": MeltColumn(label_col="Sector", val_col="Jobs",
                           input_cols=["RsrcJobs", "IndJobs", "ConsJobs", "OffJobs", "EdJobs", "HCJobs", "OthJobs"])
}
SA_PARCELS_WALK_STA_LONG = {
    "grouping": SA_GROUP_COLS + [Column("GN_VA_LU", default="Unknown"), Column("BinStn_walk")],
    "aggregation": [YEAR_COL, AggColumn("TOT_LVG_AREA"), AggColumn("NO_RES_UNTS"),
                    AggColumn("FOLIO", agg_method="size", rename="NParcels")],
    "consolidate": [],
    "melt_col": None
}
SA_PARCELS_WALK_PARK_LONG = {
    "grouping": SA_GROUP_COLS + [Column("BinStn_walk")],
    "aggregation": [YEAR_COL, AggColumn("TOT_LVG_AREA"), AggColumn("NO_RES_UNTS"),
                    AggColumn("FOLIO", agg_method="size", rename="NParcels")],
    "consolidate": [],
    "melt_col": None
}
SA_BLOCK_DEV_STATUS_LONG = {
    "grouping": SA_GROUP_COLS,
    "aggregation": [YEAR_COL, AggColumn("TotalArea")],
    "consolidate": [],
    "melt_col": MeltColumn(label_col="DevStatus", val_col="Area",
                           input_cols=["NonDevArea", "DevOSArea", "DevLowArea", "DevMedArea", "DevHighArea"])
}
ELONGATE_SPEC_DICTS = [SA_PARCELS_LU_LONG, SA_PARCELS_COMMUTE_LONG,
                      SA_PARCELS_JTYPE_LONG, SA_PARCELS_JSECTOR_LONG,
                      SA_PARCELS_WALK_STA_LONG, SA_PARCELS_WALK_PARK_LONG,
                      SA_BLOCK_DEV_STATUS_LONG]