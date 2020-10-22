"""
Created: October 2020
@Author: Brian Froeb & Alex Bell

Downloads basic demographic (race, ethnicity) and transportation data
(journey to work) from standard American Community Survey (ACS) tables.
Race groupings always include White, Black, Asian, 2 or More, and Other
by ethnicity (Hispanic/Non-Hispanic). Transportation groupings always
include Drove Alone, Carpool (any number), Transit (any submode), Taxi,
Motorcycle, Bicycle, Walk, Other, and Work From Home. These are generalized
into Drove (Drove Alone + Motorcycle), Carpool, Transit, NonMotorized
(Bicyle + Walk), Work From Home, and All Other (everything else). Commute 
mode shares for generalized modes are then calculated before the table is
stored on disk.

If run as "main", default years are downloaded using ACS 5-year estimates
for Miami-Dade County.
"""


# %% IMPORTS
import PMT
import censusdata as census
import pandas as pd

# %% FUNCTIONS
def censusGeoIndexToColumns(index, gen_geoid=True, geoid="GEOID10"):
    """
    Given an index of `censusgeo` objects, return a dataframe with
    columns reflecting the geographical hierarchy and identifying
    discrete features.

    Parameters
    -----------
    index: Index
        A pandas Index of `censusgeo` objects.
    gen_geoid: Boolean, default=True
        If True, the geographical hierarchy will be concatenated into a
        geoid field. If False, only the geographicl hierarchy fields are
        returned.
    geoid: String, default="GEOID10"
        The name to assign the geoid column if `gen_geoid` is True.

    Returns
    --------
    geo_cols: DataFrame
        A data frame with columns reflecting the geographical hierachy of
        `index`, identifying discrete geographic features. This data
        frame has `index` as its index.
    """
    idx_stack = []
    for i in index.to_list():
        columns = i.hierarchy().split("> ")
        params = i.params()
        _df_ = pd.DataFrame(params)
        _df_ = pd.DataFrame(_df_[1].to_frame().T)
        _df_.columns=columns
        idx_stack.append(_df_)
    geo_cols = pd.concat(idx_stack)
    
    if gen_geoid:
        geo_cols[geoid] = geo_cols.values.sum(axis=1)

    return geo_cols.set_index(index)


def _fetchAcs(year, acs_dataset, state, county, table, columns):
    scale = "block group"
    variables = [f"{table}_{c}" for c in list(columns.keys())]
    # Reconstruct dictionary with explicit ordering
    values = [columns[c.split("_")[1]] for c in variables]
    rename = dict(zip(variables, values))
    # Set the geography object
    geo = census.censusgeo(
        [('state', state), ('county', county), (scale, '*')])
    # Fetch data
    data = census.download(
        acs_dataset, year, geo, var=variables)
    # Rename columns
    data.rename(columns=rename, inplace=True)
    return data


def dlRaceVars(year, acs_dataset="acs5", state="12",county="086"):
    """
    Downloads population race and ethnicity variables from available ACS data
    in table B03002.
    
    Parameters
    -------------
    year: Int
    acs_dataset: String, default="acs5"
        Which ACS dataset to download (3-year, 5-year, e.g.)
    state: String, default="12"
        Which state FIPS code to download data for (`12` is Florida)
    county: String, defult="086"
        Which county FIPS code to download data for (`086` is Miami-Dade)

    Returns
    --------
    race_data: DataFrame
        A data frame with columns showing population by race (white, black,
        Asian, 2 or more, or other) and ethnicity (Hispanic, non-Hispanic)
        for block groups in the specified state-county.

    Raises
    -------
    ValueError
        If the table is not found (i.e. the requested year's data are not
        available)
    """
    # Set table and columns to fetch (with renaming specs for legibility)
    scale="block_group"
    table="B03002"
    columns = {
        "002E": "Total_Non_Hisp",
        "012E": "Total_Hispanic",
        "003E": "White_Non_Hisp",
        "004E": "Black_Non_Hisp",
        "006E": "Asian_Non_Hisp",
        "009E": "Multi_Non_Hisp",
        "013E": "White_Hispanic",
        "014E": "Black_Hispanic",
        "016E": "Asian_Hispanic",
        "019E": "Multi_Hispanic"
    }
    race_data = _fetchAcs(year, acs_dataset, state, county, table, columns)
    race_variables = [f"{table}_{c}" for c in list(columns.keys())]
    # Calculate "other" race totals (those not in the specified categories)
    race_data["Other_Non_Hisp"] = (race_data.Total_Non_Hisp -
                                   race_data.White_Non_Hisp -
                                   race_data.Black_Non_Hisp -
                                   race_data.Asian_Non_Hisp -
                                   race_data.Multi_Non_Hisp)
    race_data["Other_Hispanic"] = (race_data.Total_Hispanic -
                                   race_data.White_Hispanic -
                                   race_data.Black_Hispanic -
                                   race_data.Asian_Hispanic -
                                   race_data.Multi_Hispanic)
    # Use the census geo index to make geo tag cols
    geo_cols = censusGeoIndexToColumns(race_data.index, gen_geoid=True,
                                       geoid="GEOID10")
    race_data = pd.concat([geo_cols, race_data], axis=1)
    
    return race_data.reset_index(drop=True)


def dlCommuteVars(year, acs_dataset="acs5", state="12",county="086"):
    """
    Downloads commute (journey to work) data from available ACS data
    in table B08301.
    
    Parameters
    -------------
    year: Int
    acs_dataset: String, default="acs5"
        Which ACS dataset to download (3-year, 5-year, e.g.)
    state: String, default="12"
        Which state FIPS code to download data for (`12` is Florida)
    county: String, defult="086"
        Which county FIPS code to download data for (`086` is Miami-Dade)

    Returns
    --------
    commute_data: DataFrame
        A data frame with columns showing ....

    Raises
    -------
    ValueError
        If the table is not found (i.e. the requested year's data are not
        available)
    """
    # Set table and columns to fetch (with renaming specs for legibility)
    scale="block_group"
    table="B08301"
    columns = {
        "001E": "Total_Commutes",
        "003E": "Drove_alone",
        "004E": "Carpool",
        "010E": "Transit",
        "016E": "Taxi",
        "017E": "Motorcycle",
        "018E": "Bicycle",
        "019E": "Walk",
        "020E": "Other",
        "021E": "Work_From_Home"
    }
    # Fetch data
    mode_data = _fetchAcs(year, acs_dataset, state, county, table, columns)
    # Create Subtotals
    mode_data["Drove"] = mode_data.Drove_alone + mode_data.Motorcycle
    mode_data["NonMotor"] = mode_data.Bicycle + mode_data.Walk
    mode_data["AllOther"] = mode_data.Taxi + mode_data.Other
    # Calc shares
    mode_data["SOV_Share"] = mode_data.Drove/mode_data.Total_Commutes
    mode_data["HOV_Share"] = mode_data.Carpool/mode_data.Total_Commutes
    mode_data["PT_Share"] = mode_data.Transit/mode_data.Total_Commutes
    mode_data["NM_Share"] = mode_data.NonMotor/mode_data.Total_Commutes
    mode_data["Oth_Share"] = mode_data.AllOther/mode_data.Total_Commutes
    mode_data["WFH_Share"] = mode_data.Work_From_Home/mode_data.Total_Commutes
    
    # Use the census geo index to make geo tag cols
    geo_cols = censusGeoIndexToColumns(mode_data.index, gen_geoid=True,
                                       geoid="GEOID10")
    mode_data = pd.concat([geo_cols, mode_data], axis=1)
    
    return mode_data.reset_index(drop=True)

# %%
if __name__ == "__main__":
    bg_path = PMT.makePath(PMT.RAW, "BlockGroups")
    for year in PMT.YEARS:
        # setup folders
        race_out = PMT.makePath(bg_path, f"ACS_{year}_race.csv")
        commute_out = PMT.makePath(bg_path, f"ACS_{year}_commute.csv")
        print(f"Fetching race data ({race_out})")
        try:
            race = dlRaceVars(year, acs_dataset="acs5", state="12",
                              county="086")
            race.to_csv(race_out, index=False)
        except:
            print(f"ERROR DOWNLOADING RACE DATA ({year})")
        
        print(f"Fetching commute data ({commute_out})")
        try:
            commute = dlCommuteVars(year, acs_dataset="acs5", state="12",
                                    county="086")
            commute.to_csv(commute_out, index=False)
        except:
            print(f"ERROR DOWNLOADING COMMUTE DATA ({year})")
