# -*- coding: utf-8 -*-

# Future considerations:
#
# Do we want to make some sort of adjustment for removed land?
# I.e. you get penalized for the proportion of "irrelevant" land uses
# (other, ag, misc, vacant)
# But maybe this is mis-informed... because technically the presence of
# these land uses does improve diversity, and we just don't care about
# them in this particular calculation.
#
# Do we want to make soe sort of adjustment for total building area?
# I.e. we expect that the more square footage you have, the more diverse
# you should be
# But, this would be tough to calibrate. And it also diminishes the integrity
# of these as truly comparable "diversity" metrics... i.e. not having a lot
# of square footage and thus being less diverse (more likely) is itself a
# complete picture of diversity, and statistics that adjust for size begin to
# ask "how diverse do we EXPECT you to be", not "how diverse are you"


# %% IMPORTS

import arcpy
import pandas as pd
import numpy as np
import os
import re

# %% FUNCTIONS

def analyze_lu_diversity(parcels_path,
                         parcels_id_field,
                         parcels_land_use_field,
                         save_gdb_location,
                         on_field = None,
                         aggregate_geometry_path = None,
                         aggregate_geometry_id_field = None,
                         buffer_diversity = 0,
                         relevant_land_uses = ["auto", "civic", "education",
                                               "entertainment", "grocery",
                                               "healthcare", "industrial", 
                                               "lodging", "mf", "office",
                                               "restaurant", "sf", "shopping"],
                         how = ["simpson", "shannon", "berger-parker", 
                                "enp", "chi-squared"],
                         chisq_props = None,
                         regional_adjustment = True,
                         regional_constants = None):
    
    """
    calculates land use diversity within aggregate geometries using parcels
    
    Parameters
    ----------
    parcels_path: path
        path to parcels [which provide the attributes for diversity calcs]
    parcels_id_field: str
        id field for the parcels
    parcels_land_use_field: str
        land use field for the parcels
    save_gdb_location: path
        path to .gdb or a feature dataset within a .gdb in which the diversity
        results will be saved
    on_field: str
        field in the parcels over which diversity is assessed (e.g. land area,
        building square footage, etc.)
        default `None`, diversity will be assessed relative to a parcel count
    aggregate_geometry_path: path
        path to an aggregate geometry within which diversity will be calculated
        (e.g. neighborhoods, street buffers, etc.). It is highly recommended
        that this is provided
        default `None`, parcels themselves will act as the aggregate geometry
    aggregate_geometry_id_field: str
        id field for the aggregate geometry. must be provided if
        `aggregate_geometry_path` is provided
        default `None`, this means the parcels are the aggregate geometry, so
        the `parcels_id_field` acts as the `aggregate_geometry_id_field`
    buffer_diversity: int
        radius (in units of the CRS of the aggregate geometry) to buffer the
        aggregate geometry for calculation of diversity. Not recommended if
        an `aggregate_geometry_path` is provided; recommended if
        `aggregate_geometry_path` is not provided (i.e. the parcels are the
        aggregate geometry)
        default `0`, no buffer
    relevant_land_uses: list of str
        land uses that should be considered for a diversity calculation. must
        include only land uses present in the land use reclass table
        "K:\Projects\MiamiDade\PMT\Data\Reference\Land_Use_Recode.csv"
        the default list removes "vacant", "ag", "misc", and "other" from
        consideration in diversity calculations
    how: list of str
        diversity metrics to calculate. may include any or all of "simpson",
        "shannon", "berger-parker", "enp", and "chi-squared". See notes for
        a description of metrics.
        the default list includes all options, so all 5 metrics will be
        calculated for each feature in the aggregate geometry
    chisq_props: dict of floats
        if "chi-squared" is in `how`, this parameter allows a user to set an
        optimal distribution of land uses to be used in the calculation of 
        chi-squared statistics. The keys must match `relevant_land_uses`
        exactly, and the values must sum to 1. Will be ignored if "chi-squared"
        is not in `how`
        default `None`, the optimal distribution of land uses is assumed to
        be an equal abundance of all `relevant_land_uses`
    regional_adjustment: bool
        should a regional adjustment be performed? If so, each diversity metric
        for each feature of the aggregate geometry will be divided by the
        regional (across all parcels) score for that diversity metric, to give
        a sense how that feature performs relative to the whole area
        default `True`, complete a regional adjustment
    regional_constants: dict of floats
        if `regional_adjustment` is `True`, this parameter allows a user to set
        constants to which to compare the aggregate geometry diversity
        metrics (as opposed to comparing to the calculated regional scores).
        The keys must match `how` exactly, and the values must be between
        0 and 1 (as all scores are adjusted to a 0-1 scale)
        default `None`, complete the regional adjustment by calculating
        regional scores
        
    Notes
    -----
    The diversity measures are defined as followed:
    1. Simpson index: mathematically, the probability that a random draw of
       one unit of land use A would be followed by a random draw of one unit
       of land use B. Ranges from 0 (only one land use present)
       to 1 (all land uses present in equal abundance)
    2. Shannon index: borrowing from information theory, Shannon quantifies
       the uncertainty in predicting the land use of a random one unit draw.
       The higher the uncertainty, the higher the diversity. Ranges from 0
       (only one land use present) to -log(1/|land uses|) (all land uses
       present in equal abundance)
    3. Berger-Parker index: the maximum proportional abundance, giving a
       measure of dominance. Ranges from 1 (only one land use present) to
       1/|land uses| (all land uses present in equal abundance). Lower values
       indicate a more even spread, while high values indicate the dominance
       of one land use.
    4. Effective number of parties (ENP): a count of land uses, as weighted
       by their proportional abundance. A land use contributes less to ENP if
       it is relatively rare, and more if it is relatively common. Ranges from
       1 (only one land use present) to |land uses| (all land uses present in
       equal abunance)
    5. Chi-squared goodness of fit: the ratio of an observed chi-squared
       goodness of fit test statistic to a "worst case scenario" chi-squared
       goodness of fit test statistic. The goodness of fit test requires the
       definition of an "optimal" land use distribution ("optimal" is assumed 
       to be equal abundance of all land uses, but can be specified by the
       user). The "worst case scenario" defines the highest possible
       chi-squared statistic that could be observed under the optimal land use
       distribution. In practice, this "worst case scenario" is the equivalent
       of the least likely land use [according to the optimal distribution]
       comprising the entire area. Ranges from 0 (all land uses present
       in equal abundance) to 1 (only one land use present)
        
    Returns
    -------
    1. a path to a diversity-enriched feature class of the aggregate geometry.
    All diversity scores are normalized to a 0-1 scale (0 low, 1 high) 
    relative to their functional minima and maxima. this feature class will be 
    written to the `save_gdb_location`, and will be attributed with the 
    following:
        1. the `aggregate_geometry_id_field`
        2. the diversity score for each metric in `how`
        3. if `regional_adjustment` is `True`, the adjusted diversity score 
        for each metric in how.
    2. if `regional_adjustment` is `True` and `regional_constants` is `None`,
    a path to a table of regional diversity scores. This table will be written 
    to the `save_gdb_location` as well (or the main gdb, if `save_gdb_location` 
    is a feature dataset). These scores are also normalized to [0,1]
    
    @author: Aaron Weinstock
    """
    
    # Spatial processing -----------------------------------------------------
    
    print("")
    print("Spatial processing for diversity")
    
    # First, we have to set up the process with a few input variables
    print("-- setting up inputs for spatial processing...")
    
    # 1. field names we want to keep from parcels. if 'on_field' is None, that
    # means we're going to do a count based diversity. for this, we'll create
    # the field ourselves, so we don't call it from the parcels
    parcel_fields = [parcels_land_use_field,
                     "SHAPE@X",
                     "SHAPE@Y"]
    if on_field is not None:
        parcel_fields = [on_field] + parcel_fields
    
    # 2. parcel spatial reference (for explicit definition of spatial
    # reference in arcpy operations
    sr = arcpy.Describe(parcels_path).spatialReference
    
    # Now, recognizing that the ID for our aggregate geometry may not be 
    # unique due to singlepart vs multipart issues, we should add a 
    # ~definitively unique~ ID field of our own. We edit the aggregate geometry
    # in place to do this, so we'll delete it once it has served its 
    # purpose so we can maintain the integrity of the original data.
    # If the aggregate geometry is not provided, the parcels themselves become
    # our aggregate geometry
    print("-- adding a definitively unique process ID to the aggregate geometry...")
   
    # 1. define sequential numbers function
    # Thanks to: https://support.esri.com/en/technical-article/000011137
    # indent error when run as function?
    codeblock = 'val = 0 \ndef processID(): \n    global val \n    start = 1 \n    if (val == 0):  \n        val = start\n    else:  \n        val += 1  \n    return val\n'
    # codeblock = """
    # val = 0 
    # def processID(): 
    #     global val 
    #     start = 1 
    #     if (val == 0):  
    #         val = start
    #     else:  
    #         val += 1  
    #     return val
    # """ 
   
    # 2. are the parcels the aggregate geometry?
    if aggregate_geometry_path is None:
        print("---- ** NOTE: parcels will act as the aggregate geometry **")
        aggregate_geometry_id_field = parcels_id_field
        aggregate_geometry_path = parcels_path
   
    # 3. create the field
    arcpy.AddField_management(in_table = aggregate_geometry_path,
                              field_name = "ProcessID",
                              field_type = "LONG",
                              field_is_required = "NON_REQUIRED")
      
    # 4. calculate the field
    arcpy.CalculateField_management(in_table = aggregate_geometry_path,
                                    field = "ProcessID",
                                    expression = "processID()",
                                    expression_type = "PYTHON3",
                                    code_block = codeblock)
    
    # Ultimately, we'll want to merge our diversity results back to the
    # aggregate geometry, which we'll do on the process ID field. So, we'll
    # initialize the process by creating a new parcels feature class containing
    # only ProcessID and the provided ID field.
    print("-- initializing a new feature class for diversity results...")
    # Thanks to: https://gis.stackexchange.com/questions/229187/copying-only-certain-fields-columns-from-shapefile-into-new-shapefile-using-mode
    fkeep = ["ProcessID", aggregate_geometry_id_field]
    fmap = arcpy.FieldMappings()
    fmap.addTable(aggregate_geometry_path)
    fields = {f.name: f for f in arcpy.ListFields(aggregate_geometry_path)}
    for fname, fld in fields.items():
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            if fname not in fkeep:
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    arcpy.conversion.FeatureClassToFeatureClass(in_features = aggregate_geometry_path, 
                                                out_path = save_gdb_location,
                                                out_name = "diversity_metrics",
                                                field_mapping = fmap)
    
    # Now we're ready for the true spatial processing. We start by extracting
    # parcel centroids to numpy array, then converting array to feature class
    print("-- converting parcel polygons to parcel centroid points...")
    parcels_array = arcpy.da.FeatureClassToNumPyArray(in_table = parcels_path,
                                                      field_names = parcel_fields,
                                                      spatial_reference = sr,
                                                      null_value = -1)
    arcpy.da.NumPyArrayToFeatureClass(in_array = parcels_array,
                                      out_table = "in_memory\\centroids",
                                      shape_fields = ["SHAPE@X", "SHAPE@Y"],
                                      spatial_reference = sr)
    
    # Next, if a buffer is requested, this means diversity will be
    # calculated within a buffered version of each feature in the aggregate
    # geometry (which, remember, may be the parcels). I.e., our aggregate
    # geometry becomes a buffered aggregate geometry. So, if requested, we
    # create the buffer, and reset our aggregate geometry path
    if buffer_diversity > 0:
        print("-- buffering the aggregate geometry...")
        arcpy.Buffer_analysis(in_features = aggregate_geometry_path,
                              out_features = "in_memory\\buffer",
                              buffer_distance_or_field = buffer_diversity)
        aggregate_geometry_path = "in_memory\\buffer"
    
    # Now we need to identify parcels within each feature of the aggregate
    # geometry. To do this, we intersect the parcel centroids with the
    # aggregate geometry. This has the effect of tagging the parcels with the
    # unique ProcessID, as well as filtering parcels that don't fall in any
    # feature of the aggregate geometry
    print("-- matching parcels to aggregate geometries...")
    arcpy.Intersect_analysis(in_features = ["in_memory\\centroids", 
                                            aggregate_geometry_path],
                             out_feature_class = "in_memory\\intersect")
    
    # Finally, we have to select out the fields we want to work with for
    # diversity calculations. At this point, we can easily create our data
    # for summarization. Note that if we have no "on_field", this means
    # we're working off a parcel count, so we add a field of 1s to simulate
    # a count when we complete our summarizations
    print("-- loading data for diversity calculations...")
    ret_fields = ["ProcessID",
                  aggregate_geometry_id_field,
                  parcels_land_use_field]
    if on_field is not None:
        ret_fields = ret_fields + [on_field]
    div_array = arcpy.da.FeatureClassToNumPyArray(in_table = "in_memory\\intersect",
                                                  field_names = ret_fields,
                                                  spatial_reference = sr,
                                                  null_value = -1)
    df = pd.DataFrame(div_array)
    if on_field is None:
        on_field = "count_field"
        df[on_field] = 1
    
    # Now we have our data for allocation prepped -- great!
    # The last step in spatial processing is now deleting the intermediates
    # we created along the way
    print("-- deleting intermediates...")
    arcpy.Delete_management("in_memory\\centroids")
    arcpy.Delete_management("in_memory\\intersect")
    if arcpy.Exists("in_memory\\buffer"):
        arcpy.Delete_management("in_memory\\buffer")
    if aggregate_geometry_path != "in_memory\\buffer":
        arcpy.DeleteField_management(in_table = aggregate_geometry_path,
                                     drop_field = "ProcessID")
    
    # Diversity calculations -------------------------------------------------
          
    print("")
    print("Diversity calculations")
    
    # First, we want to do a little formatting
    print("-- formatting the input data...")
    
    # 1. Field name resetting. We do this to make our lives a little easier, 
    # because we allow user input for nearly all of our fields
    df = df.rename(columns={parcels_land_use_field: "LU_DEL",
                            on_field: "ON"})
    
    # 2. Remove the cells that have no land use (i.e. the ones filled with -1)
    # and the ones where our on field is 0 or null (i.e. the ones with value
    # < 0, because it could be an observed 0 or a filled -1)
    df = df[df.LU_DEL != -1]
    df = df[df.ON > 0]
    
    # 3. Now we merge in our new land use definitions. We're hard coding
    # it here because it's a constant for Miami-Dade, but if we ever want
    # to functionalize this for use outside the PMT, we should consider
    # optioning it out
    lu_rc = pd.read_csv(r"K:\Projects\MiamiDade\PMT\Data\Reference\Land_Use_Recode.csv")
    lu_rc = lu_rc[["DOR_UC","DIV_CLASS"]]
    lu_rc = lu_rc.rename(columns={"DOR_UC": "LU_DEL",
                                  "DIV_CLASS": "LU"})
    df = df.merge(lu_rc, how="left")
    df = df.drop(columns="LU_DEL")
    
    # 4. Finally, we filter to only the land uses of interest
    if relevant_land_uses is not None:
        df = df[df.LU.isin(relevant_land_uses)]
    
    # Now we'll do a bit of pre-summarization to give us the components we
    # need for the diversity calculations. These include a "total" (sum of
    # all 'on_field' in the aggregate geometry) and a "percent" (proportion
    # of 'on_field' in each land use in the aggregate geometry)
    print("-- calculating summary values for aggregate geometries...")
    divdf = df.groupby(["ProcessID","LU"])[["ON"]].agg("sum").reset_index()
    tot = divdf.groupby("ProcessID")[["ON"]].agg("sum").reset_index().rename(columns={"ON":"Total"})
    divdf = divdf.merge(tot, how="left")
    divdf = divdf.assign(Percent = divdf["ON"] / divdf["Total"])
    
    # We can now reference this table to calculate our diversity metrics
    print("-- calculating diversity metrics...")
    diversity_metrics = []
    nlu = len(relevant_land_uses)
    
    # 1. Simpson 
    if "simpson" in how:
        print("---- Simpson")
        mc = divdf.assign(SIN = divdf["ON"] * (divdf["ON"]-1))
        mc = mc.assign(SID = mc["Total"] * (mc["Total"]-1))
        diversity = mc.groupby("ProcessID").apply(lambda x: sum(x.SIN) / np.unique(x.SID)[0]).reset_index()
        diversity.columns = ["ProcessID","Simpson"]
        # Adjust to 0-1 scale
        diversity["Simpson"] = 1 - diversity["Simpson"]
        diversity_metrics.append(diversity)
    
    # 2. Shannon 
    if "shannon" in how:
        print("---- Shannon")
        mc = divdf.assign(PLP = divdf["Percent"] * np.log(divdf["Percent"]))
        diversity = mc.groupby("ProcessID").apply(lambda x: sum(x.PLP) * -1).reset_index()
        diversity.columns = ["ProcessID","Shannon"]
        # Adjust to 0-1 scale
        diversity["Shannon"] = diversity["Shannon"] / (-1 * np.log(1/nlu))
        diversity_metrics.append(diversity)
    
    # 3. Berger-Parker 
    if "berger-parker" in how:
        print("---- Berger-Parker")
        diversity = divdf.groupby("ProcessID").apply(lambda x: max(x.Percent)).reset_index()
        diversity.columns = ["ProcessID","BergerParker"]
        # Adjust to 0-1 scale
        diversity["BergerParker"] = 1 - diversity["BergerParker"]
        diversity_metrics.append(diversity)
        
    # 4. ENP
    if "enp" in how:
        print("---- Effective number of parties (ENP)")
        mc = divdf.assign(P2 = divdf["Percent"] ** 2)
        diversity = mc.groupby("ProcessID").apply(lambda x: 1 / sum(x.P2)).reset_index()
        diversity.columns = ["ProcessID","ENP"]
        # Adjust to 0-1 scale
        diversity["ENP"] = (diversity["ENP"] - 1) / (nlu - 1)
        diversity_metrics.append(diversity)
    
    # 5. Chi-squared goodness of fit
    if "chi-squared" in how:
        print("---- Chi-squared goodness of fit")
        if chisq_props is not None:
            props = pd.DataFrame({"LU": list(chisq_props.keys()),
                                  "ChiP": list(chisq_props.values())})
        else:
            chisq_props = dict()
            for lu in relevant_land_uses:
                chisq_props[lu] = 1/nlu                
            props = pd.DataFrame({"LU": list(chisq_props.keys()),
                                  "ChiP": list(chisq_props.values())})
        d = dict()
        ub = np.unique(divdf.ProcessID)
        d["ProcessID"] = np.repeat(ub, len(relevant_land_uses))
        d["LU"] = relevant_land_uses * len(ub)
        lu_dummies = pd.DataFrame(d)
        on = divdf[["ProcessID","LU","ON"]].drop_duplicates()
        totals = divdf[["ProcessID","Total"]].drop_duplicates()
        mc = lu_dummies.merge(on, how="left").merge(totals, how="left")
        mc = mc.fillna({"ON":0})
        mc = mc.merge(props, how="left")
        mc = mc.assign(EXP = mc["ChiP"] * mc["Total"])
        mc = mc.assign(Chi2 = (mc["ON"] - mc["EXP"])**2 / mc["EXP"])
        mc = mc.assign(WCS = (mc["Total"] - mc["EXP"])**2 / mc["EXP"] - mc["EXP"])
        diversity = mc.groupby("ProcessID").apply(lambda x: sum(x.Chi2) / (sum(x.EXP) + max(x.WCS))).reset_index()
        diversity.columns = ["ProcessID","ChiSquared"]
        # Adjust to 0-1 scale
        diversity["ChiSquared"] = 1 - diversity["ChiSquared"]
        diversity_metrics.append(diversity)
    
    # Now that we've calculated all our metrics, we just need to merge
    # into a single data frame for reporting
    print("-- formatting diversity results...")
    diversity_metrics = [df.set_index("ProcessID") for df in diversity_metrics]
    diversity_metrics = pd.concat(diversity_metrics, axis=1)
    diversity_metrics = diversity_metrics.reset_index()
    
    # Regional comparisons ---------------------------------------------------
    
    # Do we want the region score across ALL parcels?
    # Or across parcels within our aggregate geometries?
    # For now, we use the first... i.e. the "region adjustment" is relative
    # to all area of the context of the aggregate geometries
    
    # If regional comparison is requested, we calculate each diversity index
    # at the regional level, and adjust the aggregate geometry scores by
    # a ratio of geom score : region score. If regional constants are provided,
    # we do the same sort of adjustment, but use the provided constants as
    # opposed to doing the calculations here.
    if regional_adjustment == True:
        print("")
        print("Regional adjustment to diversity")
        
        if regional_constants is not None:
            # Set our adjustment dictionary to the provided constants if
            # constants are provided
            area_div = regional_constants
        else:
            # We need to do our own calculations since no constants are given.
            # Like before, we first need to get summary values. But now, we're
            # calculating them over the whole region, not the individual
            # aggregate geometries. NOTE THAT THE "WHOLE REGION" HERE MEANS
            # ALL PARCELS, so we reference back to the parcels_array from
            # spatial processing
            print("-- calculating summary values for region...")
            
            # We'll need to format and summarize the parcels in the same
            # way we did with those in our aggregate geometries
            pdf = pd.DataFrame(parcels_array)
            pdf = pdf.rename(columns={on_field:"ON",
                                      parcels_land_use_field:"LU_DEL"})
            pdf = pdf[["ON","LU_DEL"]]
            pdf = pdf[pdf.LU_DEL != -1]
            pdf = pdf[pdf.ON > 0]
            pdf = pdf.merge(lu_rc, how="left")
            pdf = pdf.drop(columns="LU_DEL")
            if relevant_land_uses is not None:
                pdf = pdf[pdf.LU.isin(relevant_land_uses)]
        
            # Now we can summarize
            reg = pdf.groupby("LU")[["ON"]].agg("sum").reset_index()
            reg["Total"] = sum(reg.ON)
            reg = reg.assign(Percent = reg["ON"] / reg["Total"])
            
            # Now, we calculate each diversity metric for the whole region
            print("-- calculating regional diversity...")
            area_div = dict()
            
            # 1. Simpson
            if "simpson" in how:
                print("---- Simpson")
                area_div["Simpson"] = sum(reg.ON * (reg.ON-1)) / (reg.Total[0] * (reg.Total[0] - 1))
                area_div["Simpson"] = 1 - area_div["Simpson"]
            
            # 2. Shannon
            if "shannon" in how:
                print("---- Shannon")
                area_div["Shannon"] = -1 * sum(reg.Percent * np.log(reg.Percent))
                area_div["Shannon"] = area_div["Shannon"] / (-1 * np.log(1/nlu))
                
            # 3. Berger-Parker
            if "berger-parker" in how:
                print("---- Berger-Parker")
                area_div["BergerParker"] = max(reg.Percent)
                area_div["BergerParker"] = 1 - area_div["BergerParker"]
            
            # 4. ENP
            if "enp" in how:
                print("---- Effective number of parties (ENP)")
                area_div["ENP"] = 1 / sum(reg.Percent ** 2)
                area_div["ENP"] = (area_div["ENP"] - 1) / (nlu - 1)
            
            # 5. Chi-squared goodness of fit
            if "chi-squared" in how:
                print("---- Chi-squared goodness of fit")
                csr = props.merge(reg, how="left")
                csr = csr.assign(EXP = csr["Total"] * csr["ChiP"])
                csr = csr.assign(Chi2 = (csr["ON"] - csr["EXP"])**2 / csr["EXP"])
                csr = csr.assign(WCS = (csr["Total"] - csr["EXP"])**2 / csr["EXP"] - csr["EXP"])
                area_div["ChiSquared"] = sum(csr.Chi2) / (sum(csr.EXP) + max(csr.WCS))
                area_div["ChiSquared"] = 1 - area_div["ChiSquared"]
            
        # Now, we can calculate our "adjusted" diversities by dividing the
        # aggregate geometry value for a metric by the regional value for
        # a metric
        print("-- adjusting diversity by regional score...")
        for key in area_div.keys():
            value = area_div[key]
            name = '_'.join([key, "Adj"])
            diversity_metrics[name] = diversity_metrics[key] / value
        
        # Finally, if we did a regional adjustment, we'll want to write
        # out the region results as well. We'll do this as a simple table,
        # so just create a dataframe
        area_div = pd.DataFrame(area_div, index=[0]).reset_index(drop=True)
    
    # Writing results --------------------------------------------------------
    
    print("")
    print("Writing results")
    
    # For saving, we join the diversity metrics back to the ID shape we
    # initialized during spatial processing
    print("-- merging diversity results back to the aggregate geometries...")
    
    # 1. convert pandas df to numpy array for use with arcpy ExtendTable
    df_et = np.rec.fromrecords(recList = diversity_metrics.values, 
                               names = diversity_metrics.dtypes.index.tolist())
    df_et = np.array(df_et)
    
    # 2. use ExtendTable to modify the parcels data
    diversity_path = os.path.join(save_gdb_location, 
                                   "diversity_metrics")
    arcpy.da.ExtendTable(in_table = diversity_path,
                         table_match_field = "ProcessID",
                         in_array = df_et,
                         array_match_field = "ProcessID")
    
    # 3. delete the ProcessID field from the diversity feature class
    arcpy.DeleteField_management(in_table = diversity_path,
                                 drop_field = "ProcessID")
    
    # If we did a regional adjustment, we also need to write the region
    # metrics table. If the adjustment was done by constant, we won't write
    # (because the user should know these values); we only write if we
    # did the region calculation. The same process for writing is used as 
    # above, except we use "array to table" instead of "extend table"
    # One change is that tables can't be written to feature datasets. So,
    # if the save_gdb_location is a feature dataset, we need to set its
    # resident gdb as the save location
    if regional_adjustment == True and regional_constants is None:
        print("-- writing table of regional diversity results...")
        reg_tt = np.rec.fromrecords(recList = area_div.values,
                                    names = area_div.dtypes.index.tolist())
        reg_tt = np.array(reg_tt)
        gdb_main = re.search("^(.*?)\.gdb", save_gdb_location)[0]
        region_path = os.path.join(gdb_main, 
                                   "regional_diversity")
        arcpy.da.NumPyArrayToTable(in_array = reg_tt,
                                    out_table = region_path)
    
    # ------------------------------------------------------------------------
    
    print("")
    print("Done!")
    print("Diversity results saved to: " + diversity_path)
    if regional_adjustment == True:
        print("Regional diversity saved to: " + region_path)
    print("")
    if regional_adjustment == True and regional_constants is None:
        return({"Diversity_FC": diversity_path,
                "Region_Table": region_path})
    else:
        return(diversity_path)
        
        
# %% MAIN
if __name__ == "__main__":
    # 1. Define the year
    year = 2019
    
    # 2. Define the function inputs
    parcels_path = os.path.join("K:/Projects/MiamiDade/PMT/Data/Cleaned",
                                "Parcels.gdb",
                                '_'.join(["Miami",str(year)]))
    parcels_id_field = "PARCELNO"
    parcels_land_use_field = "DOR_UC"
    save_gdb_location = os.path.join("K:/Projects/MiamiDade/PMT",
                                     ''.join(["PMT_",str(year),".gdb"]),
                                     "StationAreas")
    on_field = "TOT_LVG_AREA"
    aggregate_geometry_path = os.path.join("K:/Projects/MiamiDade/PMT",
                                           "Basic_features.gdb",
                                           "Basic_features_SPFLE",
                                           "SMART_Plan_Station_Areas")
    aggregate_geometry_id_field = "Name"
    buffer_diversity = 0
    relevant_land_uses = ["auto", "civic", "education", "entertainment", 
                          "grocery", "healthcare", "industrial", "lodging", 
                          "mf", "office", "restaurant", "sf", "shopping"]
    how = ["simpson", "shannon", "berger-parker", "enp", "chi-squared"]
    chisq_props = None
    regional_adjustment = True
    regional_constants = None
    
    # 3. Call the function
    analyze_lu_diversity(parcels_path = parcels_path,
                         parcels_id_field = parcels_id_field,
                         parcels_land_use_field = parcels_land_use_field,
                         save_gdb_location = save_gdb_location,
                         on_field = on_field,
                         aggregate_geometry_path = aggregate_geometry_path,
                         aggregate_geometry_id_field = aggregate_geometry_id_field,
                         buffer_diversity = buffer_diversity,
                         relevant_land_uses = relevant_land_uses,
                         how = how,
                         chisq_props = chisq_props,
                         regional_adjustment = regional_adjustment,
                         regional_constants = regional_constants)   