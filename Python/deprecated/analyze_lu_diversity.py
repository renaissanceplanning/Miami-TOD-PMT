# -*- coding: utf-8 -*-

import geopandas as gpd
import numpy as np
import pandas as pd
import itertools
import math
from scipy import stats
import os
import sys
import arcpy #I need to install arcpy

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

"""
Function name:
land_use_diversity
    
Description:
Calculates various land use diversity metrics within buffered areas of
provided geometries 

Inputs:
parcel_polygons_path: string, path to shapefile of parcel polygons (the
                      level at which diversity will be calculated)
parcel_attributes_path: string, path to .csv of parcel attributes
within_radius: int or float, buffer radius (in units of parcel polygons CRS)
               inside of which land use will be assessed for each parcel
id_field: string, field in parcel attributes defining a unique ID for parcels
on_field: string, field in parcel attributes defining the variable over which 
          diversity will be assessed
          See notes for elaboration
land_use_field: string of field in parcel attributes defining parcel land use
relevant_land_uses: list, strings of land uses deemed relevant for assessment
                    of land use diversity (allows for filtering of non-active
                    land uses, if desired)
                    All elements in this list must be present in the land use
                    field
                    Default None, use all land uses in the land use field
how: list, strings containing any or all of "simpson", "shannon",
     "berger-parker", "enp" ,"chi-squared", defining the types of diversity
     that will be calculated
     Default is a list containing all options
     See notes for metric descriptions
chisq_props: dictionary, with keys being all relevant land uses, and values
             being desired proportions of each land use. Values must sum to 1
             Default None, all relevant land uses get equal proportions
chunks: int, number of chunks to analyze the data in. Must be >= 1
        Default 1, no chunking
regional_adjustment: bool, should the parcel-level diversity measures be
                     adjusted according to the regional measures?
                     Default is True, each metric will be divided by the
                     regional value for that metric (both un-adjusted and
                     adjusted measures will be returned)
save_directory: string, path to desired save directory
                Default None, no save completed
gdb_location: string, path to desired gdb save location
              Default None, no save to gdb completed
              If provided, save_directory must also be provided (to make
              shp to gdb conversion possible)
    
Notes:
1. on_field determines how a parcel will be weighted in a diversity measure.
   For example, if we wanted parcels to be weighted on their land area, we'd
   provided a field in which land area is held. If we want each parcel to carry
   the same weight (i.e. a 1-to-1 correspondence between parcel and land use,
   on_field would be a column of ones)
2. The diversity measures are defined as followed:
    1. Simpson index: mathematically, the probability that a random draw of
       one unit of land use A would be followed by a random draw of one unit
       of land use B. Ranges from 0 (all land uses present in equal abundance)
       to 1 (only one land use present)
    2. Shannon index: borrowing from information theory, Shannon quantifies
       the uncertainty in predicting the land use of a random one unit draw.
       The higher the uncertainty, the higher the diversity. Ranges from 0
       (only one land use present) to -log(1/|land uses|) (all land uses
       present in equal abundance)
    3. Berger-Parker index: the maximum proportional abundance, giving a
       measure of dominance. Ranges from 1/|land uses| (all land uses present
       in equal abundance) to 1 (only one land use present). Lower values
       indicate a more even spread, while high values indicate the dominance
       of one land use.
    4. Effective number of parties (ENP): a count of land uses, as weighted
       by their proportional abundance. A land use contributes less to ENP if
       it is relatively rare, and more if it is relatively common. Ranges from
       1 (only one land use present) to |land uses| (all land uses present in
       equal abunance)
    5. Chi-squared goodness of fit: 1 - the p-value from a chi-squared goodness
       of fit test, assuming an "optimal" land use distribution ("optimal" is
       assumed to be equal abundance of all land uses, but can be specified by
       the user). By using 1-p, if areas A and B have the same proportions of
       land uses but area A has a larger total, area A will be considered
       more diverse (i.e. this metric assumes diversity is more difficult to
       achieve in more built-up areas). Ranges from 0 (all land uses present
       in equal abundance) to 1 (only one land use present)

    
Returns:
GeoDataFrame of the parcel polygons, attributed with:
    1. Parcel ID
    2. A column for each metric requested
    3. A column for each adjusted metric, if regional adjustment is True

@author: Aaron Weinstock
"""

# Testing specs
parcel_polygons_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\Parcels\Parcel_Geometry\Miami_2019.shp"
parcel_attributes_path = r"K:\Projects\MiamiDade\PMT\Data\Cleaned\Parcels\Parcel_Attributes\Miami_2019_DOR.csv"
within_radius = 1320
id_field = "PARCELNO"
on_field = "TOT_LVG_AREA"
land_use_field = "DIV_CLASS"
relevant_land_uses = ["sf",
                      "mf",
                      "industrial",
                      "shopping",
                      "auto",
                      "office",
                      "civic",
                      "misc",
                      "education",
                      "restaurant",
                      "lodging",
                      "entertainment",
                      "grocery",
                      "healthcare"]
how = ["simpson","shannon","berger-parker","enp","chi-squared"] # Lieberson? Gini?
chisq_props = None
chunks = 19
regional_adjustment = True
save_directory = None
gdb_location = None

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

def land_use_diversity(parcel_polygons_path,
                       parcel_attributes_path,
                       within_radius,
                       id_field,
                       on_field,
                       land_use_field,
                       relevant_land_uses = None,
                       how = ["simpson","shannon","berger-parker","enp","chi-squared"],
                       chisq_props = None,
                       chunks = 1,
                       regional_adjustment = True,
                       save_directory = None,
                       gdb_location = None):
    
    # Validation of inputs (if not related to parcels) -----------------------
    
    # parcel_polygons_path: must be a string, valid path, and be a .shp
    if type(parcel_polygons_path) is not str:
        sys.exit("'parcel_polygons_path' must be a string pointing to a shapefile of parcel polygons")
    if not os.path.exists(parcel_polygons_path):
        sys.exit("'parcel_polygons_path' is not a valid path")
    if parcel_polygons_path.split(".")[-1] != "shp":
        sys.exit("'parcel_polygons_path' does not point to a shapefile")
        
    # parcel_attributes_path: must be a string, valid path, and be a .cssv
    if type(parcel_attributes_path) is not str:
        sys.exit("'parcel_attributes_path' must be a string pointing to a .csv of parcel attributes")
    if not os.path.exists(parcel_attributes_path):
        sys.exit("'parcel_attributes_path' is not a valid path")
    if parcel_attributes_path.split(".")[-1] != "csv":
        sys.exit("'parcel_attributes_path' does not point to a .csv")
    
    # within_radius: must be a float or an int
    if type(within_radius) not in [int, float]:
        sys.exit("'within_radius' must be a number")
        
    # how: must be a string with specific entries
    if type(how) is not list:
        sys.exit("'how' must be a list of strings, including any or all of 'simpson', 'shannon', 'berger-parker', 'enp', and 'chi-squared'")
    else:
        vh = [h not in ["simpson","shannon","berger-parker","enp","chi-squared"] for h in how]
        if any(vh) == True:
            sys.exit("at least one entry in 'how' is not among 'simpson', 'shannon', 'berger-parker', 'enp', and 'chi-squared'")
    
    # chunks: must be a float or an int
    if type(chunks) is not int:
        sys.exit("'chunks' must be a integer")
    if chunks < 1:
        sys.exit("'chunks' must be greater than 1")
        
    # regional_adjustment: must be a boolean
    if type(regional_adjustment) is not bool:
        sys.exit("'regional_comparison' must be either 'True' or 'False'")
    
    # save_directory
    if save_directory is not None:
        if type(save_directory) is not str:
            sys.exit("'save_directory' must be a string of the desired save location")
        if not os.path.exists(save_directory):
            try: 
                os.mkdir(save_directory)
            except:
                sys.exit("'save_directory' is not a valid directory (or otherwise cannot be created)")  
    
    # gdb_location
    if gdb_location is not None and save_directory is None:
        sys.exit("if 'gdb_location' is provided, 'save_directory' must also be provided (so shp to gdb conversion can be performed)")
    
    # Chunking setup ---------------------------------------------------------
    
    print("Reading parcel geometries...")
    # Read parcels geometry
    parcels = gpd.read_file(parcel_polygons_path)
    n = len(parcels.index)
    parcels["BID"] = np.arange(n)
    parcels = parcels[["BID",id_field,"geometry"]]
    pgeo = parcels.geometry
    parcels["geometry"] = parcels.geometry.centroid
    
    print("Setting up spatial index for parcels...")
    # Set up a spatial index for the points
    # (This will make intersecting a LOT faster)
    spatial_index = parcels.sindex
    
    print("Buffering parcels to provided radius...")
    # Buffer the centroids according to provided radius
    pbuff = parcels
    pbuff["geometry"] = parcels.geometry.buffer(within_radius)
    
    print("Reading and formatting parcel attributes...")
    # Read parcels attributes and filter according to inputs
    attr = pd.read_csv(parcel_attributes_path)
    # This is only needed because DIV_CLASS isn't in our parcel attributes
    # In general practice, we won't need this (i.e. this is a hard code)
    lu_rc = pd.read_csv(r"K:\Projects\MiamiDade\PMT\Data\Reference\Land_Use_Recode.csv")
    lu_rc = lu_rc[["DOR_UC","DIV_CLASS"]]
    attr = attr.merge(lu_rc, how="left")
    
    # Validate inputs related to attributes
    attr_cols = attr.columns.to_list()
    if id_field not in attr_cols:
        sys.exit("the provided 'id_field' is not in the parcel attributes")
    if on_field not in attr_cols:
        sys.exit("the provided 'on_field' is not in the parcel attributes")
    if land_use_field not in attr_cols:
        sys.exit("the provided 'land_use_field' is not in the parcel attributes")
    if relevant_land_uses is not None:
        vlu = [lu not in attr[land_use_field].to_list() for lu in relevant_land_uses]
        if any(vlu) == True:
            sys.exit("at least one entry in 'relevant_land_uses' is not in the land use field")
    else:
        relevant_land_uses = np.unique(attr[land_use_field]).tolist()
    if chisq_props is not None:
        if type(chisq_props) is not dict:
            sys.exit("if 'chisq_probs' is provided, it must be a dictionary")
        if sorted(chisq_props.keys()) != sorted(relevant_land_uses):
            sys.exit("if 'chisq_probs' is provided, its keys must match the elements of 'relevant_land_uses' exactly")
        if sum(chisq_props.values()) != 1:
            sys.exit("the values in 'chisq_props' must sum to 1")
        props = pd.DataFrame({"LU": chisq_props.keys(),
                              "ChiP": chisq_props.values()})
    else:
        chisq_props = dict()
        nlu = len(relevant_land_uses)
        for lu in relevant_land_uses:
            chisq_props[lu] = 1/nlu                
        props = pd.DataFrame({"LU": chisq_props.keys(),
                              "ChiP": chisq_props.values()})       
    
    # Format
    attr["PIDX"] = np.arange(n)
    attr = attr[["PIDX",land_use_field, on_field]]
    attr.columns = ["PIDX","LU","ON"]
    attr = attr.fillna(0)
    attr = attr[attr.LU.isin(relevant_land_uses)]
    attr = attr[attr.ON > 0]

    print("Initializing chunking...\n")
    # Set up chunking
    chunk_ids = np.arange(chunks) 
    each = math.ceil(n / chunks)
    chk = np.repeat(chunk_ids, each)
    chk = chk[:n]
    pbuff["Chunk"] = chk
    
    # Loop through chunks and calculate diversity ----------------------------
        
    chunked_diversity = []
    
    for i in chunk_ids:
        print(''.join(["Chunk ", str(i+1), "/", str(chunks)]))
        
        print("-- Performing intersection...")
        # Subset to appropriate chunk
        pbuff_chunked = pbuff[pbuff.Chunk == i]
        bids = pbuff_chunked["BID"]
        pbuff_chunked = pbuff_chunked.geometry
        
        # Perform the intersection
        pidx = []
        # j = 1
        for pb in pbuff_chunked:
            # print(j)
            # j += 1
            pidx.append(list(spatial_index.intersection(pb.bounds)))
        
        print("-- Formatting data and merging with attributes...")
        # DataFrame of parcel centroids (PIDX) within parcel buffers (BID)
        # (We merge on PIDX, summarize on BID)
        lens = [len(x) for x in pidx]
        pd_bid = [np.repeat(b, l) for b,l in zip(bids, lens)]
        pd_bid = np.concatenate(pd_bid)
        pd_idx = list(itertools.chain.from_iterable(pidx))
        pidx = pd.DataFrame({"PIDX":pd_idx, "BID":pd_bid})
        
        # Merging
        pidx = pidx.merge(attr, how="inner")
        pidx = pidx.drop(columns="PIDX")    
        
        # Getting counts
        by_lu_and_bid = pidx.groupby(["BID","LU"]).sum().reset_index()
        by_bid = pidx.drop(columns="LU").groupby("BID").sum().reset_index().rename(columns={"ON":"TOTAL"})
        pidx = by_lu_and_bid.merge(by_bid, how="left")
        pidx = pidx.assign(P = pidx["ON"] / pidx["TOTAL"])
        
        print("-- Calculating diversity indices...")
        diversity_metrics = [] 
        # Diversity calculations  
        if "simpson" in how:
            print("---- Simpson")
            mc = pidx.assign(SIN = pidx["ON"] * (pidx["ON"]-1))
            mc = mc.assign(SID = mc["TOTAL"] * (mc["TOTAL"]-1))
            diversity = mc.groupby("BID").apply(lambda x: sum(x.SIN) / np.unique(x.SID)[0]).reset_index()
            diversity.columns = ["BID","Simpson"]
            diversity_metrics.append(diversity)
        
        if "shannon" in how:
            print("---- Shannon")
            mc = pidx.assign(PLP = mc["P"] * np.log(mc["P"]))
            diversity = mc.groupby("BID").apply(lambda x: sum(x.PLP) * -1).reset_index()
            diversity.columns = ["BID","Shannon"]
            diversity_metrics.append(diversity)
        
        if "berger-parker" in how:
            print("---- Berger-Parker")
            diversity = pidx.groupby("BID").apply(lambda x: max(x.P)).reset_index()
            diversity.columns = ["BID","BergerParker"]
            diversity_metrics.append(diversity)
            
        if "enp" in how:
            print("---- Effective number of parties")
            mc = pidx.assign(P2 = mc["P"] ** 2)
            diversity = mc.groupby("BID").apply(lambda x: 1 / sum(x.P2)).reset_index()
            diversity.columns = ["BID","ENP"]
            diversity_metrics.append(diversity)
            
        if "chi-squared" in how:
            print("---- Chi-squared goodness of fit")
            d = dict()
            ub = np.unique(pidx.BID)
            d["BID"] = np.repeat(ub, len(relevant_land_uses))
            d["LU"] = relevant_land_uses * len(ub)
            lu_dummies = pd.DataFrame(d)
            mc = lu_dummies.merge(pidx, how="left")
            mc = mc.fillna({"ON":0, "TOTAL":0})
            mc = mc.merge(props, how="left")
            mc = mc.drop(columns="TOTAL")
            mc = mc.merge(by_bid, how="left")
            mc = mc.assign(EXP = mc["ChiP"] * mc["TOTAL"])
            diversity = mc.groupby("BID").apply(lambda x: 1 - stats.chisquare(x.ON, x.EXP)[1]).reset_index()
            diversity.columns = ["BID","ChiSquared"]
            diversity_metrics.append(diversity)
        
        print("-- Formatting results...")
        # Merge up
        diversity_metrics = [df.set_index("BID") for df in diversity_metrics]
        diversity_metrics = pd.concat(diversity_metrics, axis=1)
        chunked_diversity.append(diversity_metrics)
        print("-- Done with chunk " + str(i+1))
        
    # Merge back with original parcels ---------------------------------------
    
    print("\nMerging diversity results with parcels data...")
    # Merge chunked results into single dataframe
    chunked_diversity = [df.reset_index() for df in chunked_diversity]
    chunked_diversity = pd.concat(chunked_diversity)
    
    # Merge back with original parcels
    pdiv = parcels.merge(chunked_diversity, how="left")
    pdiv = pdiv.fillna({"Simpson": 1,
                        "Shannon": 0,
                        "BergerParker": 1,
                        "ENP": 1,
                        "ChiSquared": 1})
    
    # Format
    colorder = chunked_diversity.columns.tolist()
    colorder.append("geometry")
    colorder.insert(0, id_field)
    pdiv = pdiv[colorder]
    pdiv = pdiv.sort_values("BID")
    pdiv["geometry"] = pgeo
    pdiv = pdiv.drop(columns="BID")
        
    # Area wide metrics (as a point of comparison) ---------------------------
    
    if regional_adjustment == True:
        print("Calculating diversity indices over the whole region...")
        # Get regional totals
        aw = attr.drop(columns="PIDX").groupby("LU").sum().reset_index()
        aw.columns = ["LU","ON"]
        aw["TOTAL"] = sum(aw.ON)
        aw = aw.assign(P = aw["ON"] / aw["TOTAL"])
        
        # Calculate relevant diversity indices
        area_div = dict()
        if "simpson" in how:
            area_div["Simpson"] = sum(aw.ON * (aw.ON-1)) / (aw.TOTAL[0] * (aw.TOTAL[0] - 1))
        if "shannon" in how:
            area_div["Shannon"] = -1 * sum(aw.P * np.log(aw.P))
        if "berger-parker" in how:
            area_div["BergerParker"] = max(aw.P)
        if "enp" in how:
            area_div["ENP"] = 1 / sum(aw.P ** 2)
        if "chi-squared" in how:
            aw = aw.merge(props, how="left")
            aw = aw.assign(EXP = aw["TOTAL"] * aw["ChiP"])
            area_div["ChiSquared"] = 1 - stats.chisquare(aw.ON, aw.EXP)[1]
        
        print("Adjusting parcel diversity measures by the regional measure...")
        # Add adjusted metric to dataframe
        for key in area_div.keys():
            ncol = pdiv.shape[1]
            value = area_div[key]
            name = '_'.join([key, "Adj"])
            pdiv.insert(ncol-1, name, pdiv[key] / value)
        # Columns in the wrong order
        
        # Reformat area wide indices to dataframe for writing   
        area_div = pd.DataFrame(area_div, index=[0]).reset_index(drop=True)
        
    # Saving -----------------------------------------------------------------
    
    if save_directory is not None:
        print("Saving...")
        save_path_parcel = os.path.join(save_directory, "Parcel_Diversity_Indices.shp")
        save_path_region = os.path.join(save_directory, "Region_Diversity_Indices.csv")
        pdiv.to_file(save_path_parcel)
        area_div.to_csv(save_path_region)
        print("-- saved parcel measures to: " + save_path_parcel)
        print("-- saved region measures to: " + save_path_region)
    
    # GDB writing ------------------------------------------------------------
    
    if gdb_location is not None:
        print("Copying result to geodatabase...")
        try:
            arcpy.FeatureClassToGeodatabase_conversion(save_path_parcel,
                                                       gdb_location)
        except:
            print("-- gdb write failed: maybe the gdb wasn't a valid path?")
    
    # Done -------------------------------------------------------------------
    
    print("Done!\n")
    return dict({"Parcel_Diversity": pdiv,
                 "Region_Diversity": area_div})

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
    
    
    
    
    
