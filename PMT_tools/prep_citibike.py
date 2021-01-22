"""
Created: December 2020
@Author: Alex Bell


"""

# %% IMPORTS
import arcpy
import PMT
import pandas as pd
import os
import glob

# %% GLOBALS
MONTHS = ["January", "February", "March", "April", "May", "June", "July",
          "August", "September", "October", "November", "December"]


# %% FUNCTIONS
def findFiles(folder, *substrings):
    f_lists = [
        glob.glob(PMT.makePath(folder, s)) for s in substrings
    ]
    return set.intersection(*map(set, f_lists))

def cleanTransactionTable(workbook, sheet_name=0, header=9):
    pass


# %% MAIN
if __name__ == "__main__":
    cb_path = PMT.makePath(PMT.RAW, "SharedMobility", "Citi Bike Data")
    for year in PMT.YEARS:
        print (year)
        for month in MONTHS:
            folder = PMT.makePath(cb_path, f"{month} {year}")
            print(folder)
            if arcpy.Exists(folder):
                # Open and summarize sheets
                mem_list = findFiles(folder, "*member*", "*transaction*")
                print(len(mem_list))
                #cleanTransactionTable()
            else:
                print(f"-- data not found for {month}")



