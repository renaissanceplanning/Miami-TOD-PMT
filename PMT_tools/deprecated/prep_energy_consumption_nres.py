"""
Created: October 2020
@Author: Alex Bell

Provides a simple function to ingest and clean EIA energy consumption
data for non-residential buildings. 
"""
# %% IMPORTS
import PMT
import pandas as pd
import numpy as np

# %% GLOBALS
COLUMNS = ["BldgType", "NumBldgs", "TotFloorArea", "FAPerBldg",
            "TotBTU_trillion", "BTUPerBldg_Million", "BTUperSqFt_Thous",
            "LowQuartile", "Median", "HighQuartile"]
SKIPROWS=6

# %% FUNCTION
def cleanEnergyConsumption(in_excel, sheet_name, skiprows, columns,
                           out_table, remove_na_from="NumBldgs",
                           fill_na=0.0):
    """
    A simple function to read energy consumption rates for non-residential
    buildings from an Excel file, provide clean column names, and store a
    csv version of the content.

    Parameters
    -------------
    in_excel: Path
    sheet_name: String
    skiprows: Int
    columns: [String,...]
    out_table: Path
    remove_na_from: String, default="NumBldgs"
        A column in `columns` on which to filter the excel records,
        excluding any `NaN` or `Q` values from the clean output.
    fill_na: Numeric, default=0.0

    Returns
    --------
    out_table
    """
    nres_cons = pd.read_excel(in_excel, sheet_name, skiprows=skiprows)
    nres_cons.columns = columns
    fltr = np.logical_or(
        nres_cons[remove_na_from].isna(),
        nres_cons[remove_na_from] == "Q")
    nres_cons = nres_cons[~fltr]
    nres_cons.fillna(0, inplace=True)
    nres_cons.to_csv(out_table, index=False)


# %% MAIN
if __name__ == "__main__":
    nres_cons_f = PMT.makePath(PMT.RAW, "EIA_ComEnergy2015_pba3.xlsx")
    in_sheet = "data"
    out_table = PMT.makePath(
        PMT.CLEANED, "Energy_Consumption", "NRes_Energy_Consumption.csv")
    cleanEnergyConsumption(nres_cons_f, in_sheet, SKIPROWS, COLUMNS,
                           out_table, remove_na_from="NumBldgs",
                           fill_na=0.0)

