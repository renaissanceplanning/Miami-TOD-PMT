# -*- coding: utf-8 -*-
"""
Created on Tue Feb  9 09:48:34 2021

@author: Aaron Weinstock
"""

# Most recent edits include
# --> splitting into 2 functions: model and apply
# --> saving model results as intermediate to Reference folder
# --> applying models one year at a time rather than "all at once"

# %% Imports

import pandas as pd
import re
import arcpy
from sklearn import linear_model
import scipy
import numpy as np
import os

import PMT_tools.PMT as PMT

# %% Function

def analyze_blockgroup_model(bg_enrich_path,
                             acs_years,
                             lodes_years,
                             save_directory):
    '''
    fit linear models to block group-level total employment, population, and
    commutes at the block group level, and save the model coefficients for 
    future prediction

    Parameters
    ----------
    bg_enrich_path : str
        path to enriched block group data, with a fixed-string wild card for
        year (see Notes)
    acs_years : list of int
        years for which ACS variables (population, commutes) are present in 
        the data
    lodes_years : list of int
        years for which LODES variables (employment) are present in the data
    save_directory : str
        directory to which to save the model coefficients (results will be 
        saved as a csv)
        
    Notes
    -----
    in `bg_enrich_path`, replace the presence of a year with the string 
    "{year}". For example, if your enriched block group data for 2010-2015 is 
    stored at "Data_2010.gdb/enriched", "Data_2010.gdb/enriched", ..., then 
    `bg_enrich_path = "Data_{year}.gdb/enriched"`.

    Returns
    -------
    save_path : str
        path to a table of model coefficients

    '''
    
    # 1. Read 
    # -------
    print("1. Reading input data (block group)")
    
    # Initialize with constants
    df = []
    years = np.unique(np.concatenate([acs_years, lodes_years]))
    
    # Loop through years to read, edit, and store
    for y in years:
        print("----> Loading", str(y))
        
        # Read
        load_path = re.sub("{year}", str(y), bg_enrich_path,
                           flags = re.IGNORECASE)
        fields = [f.name for f in arcpy.ListFields(load_path)]
        tab = arcpy.da.FeatureClassToNumPyArray(in_table = load_path,
                                                field_names = fields,
                                                null_value = 0)
        tab = pd.DataFrame(tab)
        
        # Edit
        tab["Year"] = y
        tab["Since_2013"] = y - 2013
        tab["Total_Emp_Area"] = (
            tab["CNS_01_par"] + tab["CNS_02_par"] + tab["CNS_03_par"] + 
            tab["CNS_04_par"] + tab["CNS_05_par"] + tab["CNS_06_par"] + 
            tab["CNS_07_par"] + tab["CNS_08_par"] + tab["CNS_09_par"] + 
            tab["CNS_10_par"] + tab["CNS_11_par"] + tab["CNS_12_par"] + 
            tab["CNS_13_par"] + tab["CNS_14_par"] + tab["CNS_15_par"] + 
            tab["CNS_16_par"] + tab["CNS_17_par"] + tab["CNS_18_par"] + 
            tab["CNS_19_par"] + tab["CNS_20_par"]
        )
        if y in lodes_years:
            tab["Total_Employment"] = (
                tab["CNS01"] + tab["CNS02"] + tab["CNS03"] + tab["CNS04"] + 
                tab["CNS05"] + tab["CNS06"] + tab["CNS07"] + tab["CNS08"] + 
                tab["CNS09"] + tab["CNS10"] + tab["CNS11"] + tab["CNS12"] + 
                tab["CNS13"] + tab["CNS14"] + tab["CNS15"] + tab["CNS16"] + 
                tab["CNS17"] + tab["CNS18"] + tab["CNS19"] + tab["CNS20"]
            )
        if y in acs_years:
            tab["Total_Population"] = (
                tab["Total_Non_Hisp"] + tab["Total_Hispanic"]
            )
        
        # Store
        df.append(tab)
    
    # Bind up the table, filling empty rows
    df = pd.concat(df, ignore_index=True)
    
    # 2. Model
    # --------
    print("2. Modeling total employment, population, and commutes")
    
    # Variable setup: defines our variables of interest for modeling
    independent_variables = ["LND_VAL", "LND_SQFOOT", "JV", "TOT_LVG_AREA" ,
                             "NO_BULDNG", "NO_RES_UNTS", "RES_par",
                             "CNS_01_par", "CNS_02_par", "CNS_03_par",
                             "CNS_04_par", "CNS_05_par", "CNS_06_par",
                             "CNS_07_par", "CNS_08_par", "CNS_09_par",
                             "CNS_10_par", "CNS_11_par", "CNS_12_par",
                             "CNS_13_par", "CNS_14_par", "CNS_15_par",
                             "CNS_16_par", "CNS_17_par", "CNS_18_par",
                             "CNS_19_par", "CNS_20_par", "Total_Emp_Area",
                             "Since_2013"]
    response = {"Total_Employment": lodes_years,
                "Total_Population": acs_years,
                "Total_Commutes": acs_years}
    
    # Step 1: Overwrite NA values with 0 (where we should have data but don't)
    # -- parcel-based variables should be there every time: fill all with 0
    # -- job variables should be there for `lodes_years`: fill these with 0
    # -- dem variables should be there for `acs_years`: fill these with 0
    print("2.1 replacing missing values")
    
    parcel_na_dict = {iv: 0 for iv in independent_variables}
    df.fillna(parcel_na_dict,
              inplace=True)
    
    df.loc[(df.Total_Employment.isna()) & df.Year.isin(lodes_years), "Total_Employment"] = 0
    df.loc[(df.Total_Population.isna()) & df.Year.isin(acs_years), "Total_Population"] = 0
    df.loc[(df.Total_Commutes.isna()) & df.Year.isin(acs_years), "Total_Commutes"] = 0
    
    # Now, subset our table for ease of interpretation
    keep_cols = ["GEOID10", "Year"] + independent_variables + list(response.keys())
    df = df[keep_cols]
    
    # Step 2: conduct modeling by extracting a correlation matrix between candidate
    # explanatories and our responses, identifying explanatories with significant
    # correlations to our response, and fitting a MLR using these explanatories
    print("2.2 fitting and applying models")
    
    fits = []
    for key, value in response.items():
        print("---->", key)
        
        # Subset to relevant years for relevant years
        mdf = df[df.Year.isin(value)][independent_variables + [key]]
        n = len(mdf.index)
        
        # Correlation of all explanatories with response
        corr_mat = mdf.corr()
        cwr = corr_mat[key]
        cwr.drop(key, inplace=True)
        cwr = cwr[~cwr.isna()]
        
        # Calculate t statistic and p-value for correlation test
        t_stat = cwr * np.sqrt((n-2) / (1-cwr**2))
        p_values = pd.Series(scipy.stats.t.sf(t_stat, n-2) * 2,
                             index = t_stat.index)
        
        # Variables for the model
        mod_vars = []
        cutoff = 0.05
        while len(mod_vars) == 0:
            mod_vars = p_values[p_values.le(cutoff)].index.tolist()
            cutoff += 0.05
        
        # Fit a multiple linear regression
        regr = linear_model.LinearRegression()
        regr.fit(X = mdf[mod_vars],
                 y = mdf[[key]])
        
        # Save the model coefficients
        fits.append(pd.Series(regr.coef_[0],
                              index = mod_vars,
                              name = key))
        
    # Step 3: combine results into a single df
    print("2.3 formatting model coefficients into a single table")
    
    coefs = pd.concat(fits, axis=1).reset_index()
    coefs.rename(columns = {"index": "Variable"},
                 inplace=True)
    coefs.fillna(0,
                 inplace=True)
    
    # 3. Write
    # --------
    print("3. Writing results")
    
    save_path = os.path.join(save_directory, 
                             "block_group_model_coefficients.csv")
    coefs.to_csv(save_path,
                 index = False)
    
    # Done
    # ----
    print("Done!")
    print("")
    return save_path    
    
# ----------------------------------------------------------------------------
    

def analyze_blockgroup_apply(year,
                             bg_enrich_path,
                             bg_geometry_path,
                             model_coefficients_path,
                             save_gdb_location,
                             shares_from = None):
    '''
    predict block group-level total employment, population, and commutes using
    pre-fit linear models, and apply a shares-based approach to subdivide
    totals into relevant subgroups

    Parameters
    ----------
    year : int
        year of the `bg_enrich` data
    bg_enrich_path : str
        path to enriched block group data; this is the data to which the
        models will be applied
    bg_geometry_path : str
        path to geometry of block groups underlying the data
    model_coefficients_path : str
        path to table of model coefficients
    save_gdb_location : str
        gdb location to which to save the results (results will be saved as a
        table)
    shares_from : dict, optional
        if the year of interest does not have observed data for either LODES
        or ACS, provide other files from which subgroup shares can be
        calculated (with the keys "LODES" and "ACS", respectively). 
        For example, imagine that you are applying the models to a year where
        ACS data was available but LODES data was not. Then,
        `shares_from = {"LODES": "path_to_most_recent_bg_enrich_file_with_LODES"}.
        A separate file does not need to be referenced for ACS because the
        data to which the models are being applied already reflects shares for
        ACS variables.
        The default is None, which assumes LODES and ACS data are available
        for the year of interest in the provided `bg_enrich` file

    Returns
    -------
    save_path : str
        path to a table of model application results

    '''
    
    # 1. Read 
    # -------
    print("1. Reading input data (block group)")
    
    # Load
    fields = [f.name for f in arcpy.ListFields(bg_enrich_path)]
    df = arcpy.da.FeatureClassToNumPyArray(in_table = bg_enrich_path,
                                            field_names = fields,
                                            null_value = 0)
    df = pd.DataFrame(df)
    
    # Edit
    df["Since_2013"] = year - 2013
    df["Total_Emp_Area"] = (
        df["CNS_01_par"] + df["CNS_02_par"] + df["CNS_03_par"] + 
        df["CNS_04_par"] + df["CNS_05_par"] + df["CNS_06_par"] + 
        df["CNS_07_par"] + df["CNS_08_par"] + df["CNS_09_par"] + 
        df["CNS_10_par"] + df["CNS_11_par"] + df["CNS_12_par"] + 
        df["CNS_13_par"] + df["CNS_14_par"] + df["CNS_15_par"] + 
        df["CNS_16_par"] + df["CNS_17_par"] + df["CNS_18_par"] + 
        df["CNS_19_par"] + df["CNS_20_par"]
    )
    
    # Fill na
    independent_variables = ["LND_VAL", "LND_SQFOOT", "JV", "TOT_LVG_AREA" ,
                             "NO_BULDNG", "NO_RES_UNTS", "RES_par",
                             "CNS_01_par", "CNS_02_par", "CNS_03_par",
                             "CNS_04_par", "CNS_05_par", "CNS_06_par",
                             "CNS_07_par", "CNS_08_par", "CNS_09_par",
                             "CNS_10_par", "CNS_11_par", "CNS_12_par",
                             "CNS_13_par", "CNS_14_par", "CNS_15_par",
                             "CNS_16_par", "CNS_17_par", "CNS_18_par",
                             "CNS_19_par", "CNS_20_par", "Total_Emp_Area",
                             "Since_2013"]
    parcel_na_dict = {iv: 0 for iv in independent_variables}
    df.fillna(parcel_na_dict,
              inplace=True)
    
    # 2. Apply models
    # ---------------
    print("2. Applying models to predict totals")
    
    # Load the coefficients
    coefs = pd.read_csv(model_coefficients_path)
    
    # Predict using matrix multiplication
    mod_inputs = df[coefs["Variable"]]
    coef_values = coefs.drop(columns = "Variable")
    preds = np.matmul(mod_inputs.to_numpy(), coef_values.to_numpy())
    preds = pd.DataFrame(preds)
    preds.columns = coef_values.columns.tolist()
    pwrite = pd.concat([df[["GEOID10"]], preds], axis=1)
    
    # If any prediction is below 0, turn it to 0
    pwrite.loc[pwrite.Total_Employment < 0, "Total_Employment"] = 0
    pwrite.loc[pwrite.Total_Population < 0, "Total_Population"] = 0
    pwrite.loc[pwrite.Total_Commutes < 0, "Total_Commutes"] = 0
    
    
    # 3. Shares
    # ---------
    print("3. Identifying shares of subgroups")
    
    # Variable setup: defines our variables of interest for modeling
    dependent_variables_emp = ["CNS01", "CNS02", "CNS03", "CNS04", "CNS05", 
                               "CNS06", "CNS07", "CNS08", "CNS09", "CNS10",
                               "CNS11", "CNS12", "CNS13", "CNS14", "CNS15", 
                               "CNS16", "CNS17", "CNS18", "CNS19", "CNS20"]
    dependent_variables_pop_tot = ["Total_Hispanic", "Total_Non_Hisp"]
    dependent_variables_pop_sub = ["White_Hispanic", "Black_Hispanic", 
                                   "Asian_Hispanic", "Multi_Hispanic",
                                   "Other_Hispanic", "White_Non_Hisp", 
                                   "Black_Non_Hisp", "Asian_Non_Hisp", 
                                   "Multi_Non_Hisp", "Other_Non_Hisp"]
    dependent_variables_trn = ["Drove", "Carpool", "Transit",
                               "NonMotor", "Work_From_Home", "AllOther"]
    acs_vars = dependent_variables_pop_tot + dependent_variables_pop_sub + dependent_variables_trn
    
    # Pull shares variables from appropriate sources, recognizing that they
    # may not all be the same!
    print("3.1 formatting shares data")
    
    # Format
    if shares_from is not None:
        if "LODES" in shares_from.keys():
            lodes = arcpy.da.FeatureClassToNumPyArray(in_table = shares_from["LODES"],
                                                      field_names = ["GEOID10"] + dependent_variables_emp,
                                                      null_value = 0)
            lodes = pd.DataFrame(lodes)
        else:
            lodes = df[["GEOID10"] + dependent_variables_emp]
        if "ACS" in shares_from.keys():
            acs = arcpy.da.FeatureClassToNumPyArray(in_table = shares_from["ACS"],
                                                    field_names = ["GEOID10"] + acs_vars,
                                                    null_value = 0)
            acs = pd.DataFrame(acs)
        else:
            acs = df[["GEOID10"] + acs_vars]
    
    # Merge and replace NA
    shares_df = pd.merge(lodes, acs, on="GEOID10", how="left")
    shares_df.fillna(0,
                     inplace=True)
    
    # Step 2: Calculate shares relative to total
    # This is done relative to the "Total" variable for each group 
    print("3.2 calculating shares")
    
    shares_dict = {}
    for name, vrs in zip(["Emp", "Pop_Tot", "Pop_Sub", "Comm"],
                         [dependent_variables_emp, 
                          dependent_variables_pop_tot, 
                          dependent_variables_pop_sub,
                          dependent_variables_trn]):
        sdf = shares_df[vrs]
        sdf["TOTAL"] = sdf.sum(axis=1)
        for d in vrs:
            sdf[d] = sdf[d] / sdf["TOTAL"]
        sdf["GEOID10"] = shares_df["GEOID10"]
        sdf.drop(columns = "TOTAL", 
                 inplace=True)
        shares_dict[name] = sdf
    
    # Step 3: some rows have NA shares because the total for that class of
    # variables was 0. For these block groups, take the average share of all
    # block groups that touch that one
    print("3.3 estimating missing shares")
    
    # What touches what?
    print("----> determining block group relationships")
    
    arcpy.PolygonNeighbors_analysis(in_features = bg_geometry_path,
                                    out_table = "in_memory\\neighbors", 
                                    in_fields = "GEOID10")
    touch = arcpy.da.FeatureClassToNumPyArray(in_table = "in_memory\\neighbors",
                                              field_names = ["src_GEOID10","nbr_GEOID10"])
    touch = pd.DataFrame(touch)
    touch.rename(columns = {"src_GEOID10": "GEOID10",
                            "nbr_GEOID10": "Neighbor"},
                 inplace=True)
    
    # Loop filling of NA by mean of adjacent non-NAs
    ctf = 1
    i = 1
    while(ctf > 0):
    
        print("Filling NA values by mean of adjacent non-NA values: iteration", str(i))    
    
        # First, identify cases where we need to fill NA        
        to_fill = []
        for key, value in shares_dict.items():
            f = value[value.isna().any(axis=1)]
            f = f[["GEOID10"]]
            f["Fill"] = key
            to_fill.append(f)
        to_fill = pd.concat(to_fill, ignore_index=True)
        
        # Create a neighbors table
        nt = pd.merge(to_fill,
                      touch,
                      how = "left",
                      on = "GEOID10")
        nt.rename(columns = {"GEOID10": "Source",
                             "Neighbor": "GEOID10"},
                  inplace=True)
        
        # Now, merge in the shares data for appropriate rows
        fill_by_touching = {}
        nrem = []
        for key, value in shares_dict.items():
            fill_df = pd.merge(nt[nt.Fill == key],
                               value,
                               how = "left",
                               on = "GEOID10")
            nv = fill_df.groupby("Source").mean()
            nv["RS"] = nv.sum(axis=1)
            data_cols = [c for c in nv.columns.tolist() if c != "GEOID10"]
            for d in data_cols:
                nv[d] = nv[d] / nv["RS"]
            nv.drop(columns = "RS",
                    inplace=True)
            nv = nv.reset_index()
            nv.rename(columns = {"Source":"GEOID10"},
                      inplace=True)
            not_replaced = value[~value.GEOID10.isin(nv.GEOID10)]
            replaced = pd.concat([not_replaced, nv])
            fill_by_touching[key] = replaced
            nrem.append(len(replaced[replaced.isna().any(axis=1)].index))
            
        # Now, it's possible that some block group/year combos to be filled had
        # 0 block groups in that year touching them that had data. If this happened,
        # we're goisadfsdfsdfng to repeat the process. Check by summing nrem
        # and initialize by resetting the shares dict
        ctf = sum(nrem)
        i += 1
        shares_dict = fill_by_touching
      
    # Step 4: merge and format the shares
    print("3.4 merging and formatting shares")
    
    filled_shares = [df.set_index("GEOID10") for df in shares_dict.values()]
    cs_shares = pd.concat(filled_shares, axis=1).reset_index()
    cs_shares.rename(columns = {"index":"GEOID10"},
                     inplace=True)
    
    # 4. Block group estimation
    # -------------------------
    print("4. Estimating variable levels using model estimates and shares")
    
    # Now, our allocations are simple multiplication problems! Hooray!
    # So, all we have to do is multiply the shares by the appropriate column
    # First, we'll merge our estimates and shares
    alloc = pd.merge(pwrite, 
                     cs_shares, 
                     on = "GEOID10")
        
    # We'll do employment first
    print("----> total employment")
    for d in dependent_variables_emp:
        alloc[d] = alloc[d] * alloc.Total_Employment
    
    # Now population
    print("----> total population")
    for d in dependent_variables_pop_tot:
        alloc[d] = alloc[d] * alloc.Total_Population
    for d in dependent_variables_pop_sub:
        alloc[d] = alloc[d] * alloc.Total_Population
        
    # Finally commutes
    print("----> total commutes")
    for d in dependent_variables_trn:
        alloc[d] = alloc[d] * alloc.Total_Commutes
        
    # 5. Writing
    # ----------
    print("5. Writing outputs")
    
    # Here we write block group for allocation
    save_path = os.path.join(save_gdb_location,
                             "Modeled_blockgroups")
    PMT.dfToTable(df = alloc, 
                  out_table = save_path)
        
    
    # Done
    # ----
    print("Done!")
    print("")
    return save_path

# %% Main

if __name__ == "__main__":
    # Inputs for modeling
    bg_enrich_path = r"K:\Projects\MiamiDade\PMT\Data\IDEAL_PMT_{year}.gdb\Enrichment_blockgroups"
    acs_years = [2014, 2015, 2016, 2017, 2018]
    lodes_years = [2014, 2015, 2016, 2017]
    save_directory = r"K:\Projects\MiamiDade\PMT\Data\Reference"
    
    # Run model fitting
    model_coefficients_path = analyze_blockgroup_model(bg_enrich_path = bg_enrich_path,
                                                       acs_years = acs_years,
                                                       lodes_years = lodes_years,
                                                       save_directory = save_directory)
    
    # Set up application step with seed for constant shares
    # (and a constant for geometry since it never changes)
    bg_geometry_path = r"K:\Projects\MiamiDade\PMT\Data\IDEAL_PMT_2019.gdb\Polygons\BlockGroups"
    m_lodes = str(max(lodes_years))
    m_acs = str(max(acs_years))
    
    # Loop apply for all our years of interest
    # This will work for all historical years, for near term and trend we
    # need unique application (because they don't follow the year naming 
    # structure)
    for year in [2014, 2015, 2016, 2017, 2018, 2019]:
        # Inputs
        data_path = re.sub("{year}", 
                           str(year), 
                           bg_enrich_path)
        save_gdb_location = re.sub("{year}", 
                                   str(year), 
                                   r"K:\Projects\MiamiDade\PMT\Data\IDEAL_PMT_{year}.gdb")
        shares_from = {}
        if year not in lodes_years:
            shares_from["LODES"] = re.sub("{year}", 
                                          m_lodes, 
                                          bg_enrich_path)
        if year not in acs_years:
            shares_from["ACS"] = re.sub("{year}", 
                                        m_acs, 
                                        bg_enrich_path)
        if shares_from == {}:
            shares_from = None
            
        # Apply
        analyze_blockgroup_apply(year = year,
                                 bg_enrich_path = data_path,
                                 model_coefficients_path = model_coefficients_path,
                                 save_gdb_location = save_gdb_location,
                                 shares_from = shares_from)
        
    
              
              
          
                  
          
      