"""
created October 2020
@author: Alex Bell

Download commercial and residential energy consumption tables available
from the Energy Information Administration. EIA conducts surveys routinely.
At the time this script was written, the latest commercial building data
were available from the 2012 survey; the latest residential building data
were available from the 2015 survey.

Future updates to this script may require a review of latest survey products
and appropriate url structures to obtain more recent rates.
"""
# %%
import PMT
import urllib3
import os

# %% FUNCTIONS
def fetchEnergyRates(url, out_file):
    """
    Makes a GET request to obtain an excel file at the given url and 
    saves the data to a specified output location.
    """
    http = urllib3.PoolManager()
    req = http.request("GET", url)
    with open(out_file, "wb") as f:
        f.write(req.data)

# %% MAIN
if __name__ == "__main__":
    res_url = r"https://www.eia.gov/consumption/residential/data/2015/c&e/ce4.9.xlsx"
    com_url = r"https://www.eia.gov/consumption/commercial/data/2012/c&e/xls/pba3.xlsx"
    # Navigate up two directories to be in the main PMT folder
    os.chdir('..')
    os.chdir('..')
    # Specify output files
    res_file = PMT.makePath(PMT.RAW, "EIA_ResEnergy2015_ce4_9.xlsx")
    com_file = PMT.makePath(PMT.RAW, "EIA_ComEnergy2015_pba3.xlsx")
    # Download and save
    fetchEnergyRates(res_url, res_file)
    fetchEnergyRates(com_url, com_file)
