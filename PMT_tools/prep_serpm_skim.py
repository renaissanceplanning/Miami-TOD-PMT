"""
Created: December 2020
@Author: Alex Bell


"""


# %% IMPORTS
import PMT
import pandas as pd
from six import string_types

# %% GLOBALS
MODEL_YEARS = [2045] #2015


# %% FUNCTIONS
def cleanSkim(in_csv, o_field, d_field, imp_fields, out_csv, 
              chunksize=100000, rename={}, **kwargs):
    """
    A simple function to read rows from a skim table (csv file), select
    key columns, and save to an ouptut csv file. Keyword arguments can be
    given to set column types, etc.

    Parameters
    -------------
    in_csv: Path
    o_field: String
    d_field: String
    imp_fields: [String, ...]
    out_csv: Path
    chunksize: Integer, default=1000000
    rename: Dict, default={}
        A dictionary to rename columns with keys reflecting existing column
        names and values new column names.
    kwargs:
        Keyword arguments parsed by the pandas `read_csv` method.
    """
    # Manage vars
    if isinstance(imp_fields, string_types):
        imp_fields = [imp_fields]
    # Read chunks
    mode = "w"
    header = True
    usecols = [o_field, d_field] + imp_fields
    for chunk in pd.read_csv(
        in_csv, usecols=usecols, chunksize=chunksize, **kwargs):
        if rename:
            chunk.rename(columns=rename, inplace=True)
        # write output
        chunk.to_csv(out_csv, header=header, mode=mode, index=False)
        header = False
        mode = "a"

# %% MAIN
if __name__ == "__main__":
    for year in MODEL_YEARS:
        print(year)
        # Auto skims
        print("... auto")
        auto_csv = PMT.makePath(PMT.RAW, "SERPM", f"GP_Skims_AM_{year}.csv")
        auto_out = PMT.makePath(
            PMT.CLEANED, "SERPM", f"Auto_Skim_{year}.csv")
        auto_o = "F_TAZ"
        auto_d = "T_TAZ"
        auto_imp = "TIME"
        auto_rename = {
            auto_o: "OName",
            auto_d: "DName",
            auto_imp: "Minutes"
        }
        auto_dt = {
            auto_o: int,
            auto_d: int,
            auto_imp: float
        }
        cleanSkim(auto_csv, auto_o, auto_d, auto_imp, auto_out,
                  rename=auto_rename, chunksize=100000, thousands=",",
                  dtype=auto_dt)

        # Transit skims
        print(".. transit")
        transit_csv = PMT.makePath(
            PMT.RAW, "SERPM", f"Tran_Skims_AM_{year}.csv")
        transit_out = PMT.makePath(
            PMT.CLEANED, "SERPM", f"Transit_Skim_{year}.csv")
        transit_o = "F_TAZ"
        transit_d = "T_TAZ"
        transit_imp = "TIME"
        transit_rename = {
            transit_o: "OName",
            transit_d: "DName",
            transit_imp: "Minutes"
        }
        transit_dt = {
            transit_o: int,
            transit_d: int,
            transit_imp: float
        }
        cleanSkim(transit_csv, transit_o, transit_d, transit_imp, transit_out,
                  rename=transit_rename, chunksize=100000, thousands=",",
                  dtype=transit_dt)

