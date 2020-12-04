"""
Created: November 2020
@Author: Alex Bell

Provides a simple function to read transit ridership data from an Excel file
and generate a shape file version of the records with only key fields
included.

If run as "main", ridership records for stop locations in Miami-Dade County for
2019 are read, cleaned, and exported.
"""


# %% IMPORTS
import PMT
import pandas as pd
import arcpy

# %% GLOBALS
TABLES = {
    2019: ["MDTA_APC_WEEKDAY_APRIL2019_resave.xlsx", "MDTA_AVG_BUS_STOP_RIDERSHIP_BY_"]
}

GROUP_FIELDS = ["DIRECTION", "TIME_PERIOD"]
SUM_FIELS = ["ON", "OFF", "LOAD"]
LAT = "LAT"
LONG = "LONG"

# %% FUNCTIONS
def cleanTransitAPCData(df, group_fields, sum_fields, out_fc,
                        x_field="LONG", y_field="LAT"):
    """
    A simple function to take a data frame of transit automated passenger
    counter (APC) data, group by key fields, summarize key quantities,
    and save to a shape file. Input data are assumed to use the WGS 84
    (EPSG: 4326) spatial reference.

    Parameters
    ------------
    df: DataFrame
    group_fields: [String,...]
        A list of fields in `df` to use in grouping records for summarization.
    sum_fields: [String,...]
        A list of fields in `df` to summarize
    out_fc: Path
    x_field: String, default="LONG"
    y_field: String, default="LAT"

    Returns
    --------
    out_fc: Path
    """
    sr = arcpy.SpatialReference(4326)
    gb_fields = [x_field, y_field] + group_fields
    all_fields = gb_fields + sum_fields
    # Select key fields
    sel_df = df[all_fields].copy()
    # Group by and summarize
    print("... summarizing")
    sum_df = sel_df.groupby(gb_fields).sum().reset_index()
    # save
    print("... saving")
    PMT.dfToPoints(sum_df, out_fc, [x_field, y_field], sr)
    return out_fc


# %% MAIN
if __name__ == "__main__":
    for year in PMT.YEARS:
        print(year)
        out_fc = PMT.makePath(
            PMT.CLEANED, "Transp", f"Transit_ridership_{year}.shp")
        try:
            wb, sheet = TABLES[year]
            workbook = PMT.makePath(PMT.RAW, "Transit", wb)
            df = pd.read_excel(workbook, sheet)
            cleanTransitAPCData(df, GROUP_FIELDS, SUM_FIELS, out_fc,
                                x_field=LONG, y_field=LAT)
        except KeyError:
            print("-- No data found")
            continue
