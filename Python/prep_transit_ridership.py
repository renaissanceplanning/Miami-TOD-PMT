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
from PMT import RAW, CLEANED, YEARS
import pandas as pd
import arcpy

# %% GLOBALS
TABLES = {
    2014: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1411_2015_APR_standard_format.XLS",
    2015: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1512_2016_APR_standard_format.XLS",
    2016: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1611_2017_APR_standard_format.XLS",
    2017: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1803_2018_APR_standard_format.XLS",
    2018: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_1811_2019_APR_standard_format.XLS",
    2019: "AVERAGE_RIDERSHIP_PER_STOP_PER_TRIP_WEEKDAY_2003_2020_APR_standard_format.XLS",
}
SR = arcpy.SpatialReference(4326)

FIELDS_DICT = {
    "ADAY": "DAY_OF_WEEK",
    "ATIMEPER": "TIME_PERIOD",
    "AROUTE": "ROUTE",
    "ATRIP": "TRIP_TIME",
    "ABLOCK": "BLOCK_OPERATED",
    "ADIR": "DIRECTION",
    "ASTOP": "SEQUENTIAL_STOP_NO",
    "AQSTOP": "UNIQUE_STOP_NO",
    "ANAMSTP": "STOP_NAME",
    "ALAT": "LAT",
    "ALONG": "LONG",
    "ADWELL_TIME": "DWELL_TIME",
}

GROUP_FIELDS = ["DIRECTION", "TIME_PERIOD"]
SUM_FIELDS = ["ON", "OFF", "LOAD"]
LAT = "LAT"
LONG = "LONG"


# %% FUNCTIONS


def read_transit_xls(xls_path, sheet=None, head_row=None, rename_dict=None):
    """
    XLS File Desc: Sheet 1 contains header and max rows for a sheet (65536),
                    data continue on subsequent sheets without header
    reads in the provided xls file and due to odd formatting concatenates
    the sheets into a single data frame. The
    Parameters
    ----------
    xls_path: str
        String path to xls file
    rename_dict: dict
        dictionary to map existing column names to more readable names
    Returns: pd.Dataframe
        pandas dataframe of data from xls
    -------
    """
    # TODO: add logic to handle files that dont match existing format
    # read the xls into dict
    xls_dict = pd.read_excel(xls_path, sheet_name=sheet, header=head_row)
    # concatenate all sheets and set columns from sheet1:row1
    df = pd.concat(xls_dict.values())
    df.columns = df.iloc[0]
    df = df[df.index > 0]
    df = df.infer_objects()
    # rename columns
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True)

        drop_cols = [col for col in df.columns if col not in rename_dict.values()]
        df.drop(columns=drop_cols, axis=1, inplace=True)
    return df


# %% MAIN
if __name__ == "__main__":
    arcpy.env.overwriteOutput = True
    GITHUB = False
    if GITHUB:
        DATA = r"C:\Users\V_RPG\OneDrive - Renaissance Planning Group\SHARE\PMT_PROJECT\Data"
        RAW = PMT.makePath(DATA, "Raw")
        CLEANED = PMT.makePath(DATA, "Cleaned")
    data_path = PMT.makePath(RAW, "Transit", "TransitRidership_byStop")
    for year in YEARS:
        print(year)
        # setup loop vars
        out_clean_table = PMT.makePath(PMT.CLEANED, "Transp", f"transit_ridership_{year}.csv")
        out_fc = PMT.makePath(PMT.DATA, f"PMT_{year}.gdb", "Transport", "transit_ridership")
        xls_file = PMT.makePath(data_path, TABLES[year])
        # define on/off attribute per file
        file_tag = "_".join(TABLES[year].split("_")[7:10])
        FIELDS_DICT[f"ON_{file_tag}"] = "ON"
        FIELDS_DICT[f"OFF_{file_tag}"] = "OFF"
        try:
            arcpy.AddMessage((f'...reformatting ridership data'))
            cleaned_df = read_transit_xls(xls_path=xls_file, rename_dict=FIELDS_DICT)
            arcpy.AddMessage(f'...writing formatted data to {PMT.CLEANED}')
            cleaned_df.to_csv(out_clean_table)
            arcpy.AddMessage(f'...converting coordinates to point feature class')
            PMT.dfToPoints(df=cleaned_df, out_fc=out_fc, shape_fields=[LONG, LAT], spatial_reference=SR)
            arcpy.AddMessage(f'...{year} done')
            del FIELDS_DICT[f"ON_{file_tag}"], FIELDS_DICT[f"OFF_{file_tag}"]
        except KeyError:
            print("-- No data found")
            continue
