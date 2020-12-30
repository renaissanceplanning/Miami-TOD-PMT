# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 15:52:45 2020

@author: Aaron Weinstock
"""

# %% Imports

import arcpy
import os
import tempfile
import numpy as np
import pandas as pd
# import shutil

# %% Functions

# TO DO: (low priority) -- what if we had a third field? iterables on the 
# secondary summary field

# Set up parameter checking for the secondary fields

# Join on pasted column of all join columns (e.g. "010000-2014-Industrial")

def prep_aggregated_summaries(init_features_path,
                              init_features_id_field,
                              input_feature_class_format,
                              years,
                              fields,
                              save_feature_class_path,
                              secondary_summary_field=None,
                              secondary_reference=None,
                              secondary_match_field=None,
                              secondary_save_path=None):
    '''
    Parameters
    ----------
    init_features_path : Path
        file path to the features used to create `save_feature_class_path`
    init_features_id_field: str
        field name for a unique identifier in `init_features_path`
    input_feature_class_format : Path
        file path to the inputs to be summarized, with a fixed-string wild 
        card for year; see Notes for implementation guidance
    years : int, or list of int
        year(s) of data to be summarized
    fields : dict
        dictionary of format {field_name, null_value}, providing the field 
        name(s) in `input_feature_class_format` to be summarized, and the
        value used to replace any nulls in that field before summarization
    save_feature_class_path : Path
        file path of feature class to save the summarized data; this feature
        class should be created by `prep_aggregated_featureclass` before use 
        of this function
    secondary_summary_field : dict, optional
        dictionary of format {field_name, null_value}, providing the field 
        name(s) in `input_feature_class_format` on which to perform group 
        summaries, and the value used to replace any nulls in that field 
        before summarization.
        The default is None, no grouped summaries will be performed
    secondary_reference : Path, optional
        if `secondary_summary_field` is not in `input_feature_class_format`,
        but is rather in a separate file that can be joined to
        `input_feature_class_format`, give the path to that separate file here.
        The default is None, either no group summaries will be performed,
        or the `secondary_summary_field` is already in 
        `input_feature_class_format`
    secondary_match_field : str, optional
        if `secondary_reference` is provided, give a dictionary of format 
        {field_name, null_value}, providing the field name(s) in 
        `input_feature_class_format` on which to join those 
        `secondary_reference`, and the value used to replace any nulls in 
        that field before summarization. 
        The default is None, either no group summaries will be performed,
        or the `secondary_summary_field` is already in 
        `input_feature_class_format`
    secondary_save_path : Path, optional
        file path of feature class to save the group summarized data; this 
        feature class should be created by `prep_aggregated_featureclass` 
        before use of this function. This MUST be provdied if
        `secondary_summary_field` is provided.
        The default is None, no group summaries are performed.

    Notes
    -----
    1. For `input_feature_class_format`, the assumption is that there is some
    onsistent naming structure that points to the same data over different
    years, and that this naming structure varies only on year. Wherever a year
    Wherever a year appears in file path to the inputs, replace the year with 
    `{Year}` in the provided string. For example, if data for 2010 is stored 
    at `Data_2010.gdb/Features` (and similarly for other years), then 
    `inputs_feature_class_format = 'Data_{Year}.gdb/Features'`. Similarly, if 
    the data for 2010 is stored at 'Data.gdb/Features_2010' (and similarly for 
    other years), then `inputs_feature_class_format = 'Data.gdb/Features_{Year}'`
    
    2. There are 3 use cases for the 'secondary':
        2.1. You don't want group summaries --> leave all secondary parameters
        as default `None`
        2.2. You want group summaries, on a field that is in the inputs -->
        provide `secondary_summary_field` and `secondary_save_path`, leave
        `secondary_reference` and `secondary_match_field` as default `None`
        2.3 You want group summaries, on a field that is in a separate
        reference file that can be joined to the inputs --> provide all 4
        'secondary' parameters
    
    Returns
    -------
    results summarized by year will be joined to `save_feature_class_path`.
    If a `secondary_summary_field` is provided, results summarized by year and
    the secondary field will be joined to `secondary_save_path`. The paths
    of any modified feature classes will be returned.
    '''
    
    # Aggregating data -------------------------------------------------------
    
    print("")
    print("Aggregating and summarizing data")
    
    # Because we're loop processing, we can't use the in_memory space because
    # of locking issues. So we need to set up a temp directory to save our
    # intermediates (that we'll delete later). We'll also need to create a
    # .gdb in this location to save our data. If we save our data as .shp
    # outside of a .gdb, we run the risk of having our field names truncated,
    # which is bad if we want to keep referencing our input fields (which
    # we most definitely do)
    print("-- setting up a temporary location for intermediates")
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path = temp_dir,
                                   out_name = "Intermediates.gdb")
    intmd_gdb = os.path.join(temp_dir, "Intermediates.gdb")
    
    # For loop processing, we'll store the results of each iteration in a list
    # that we'll later concat into a single df. Note that we'll definitively
    # have one for year aggregated data, and potentially one for year +
    # secondary-field aggregated data. We're also relying on list comprehension 
    # of the years, so we'll verify that our input fields are in a list
    print("-- initializing loop")
    
    years_agg = []
    if secondary_summary_field is not None:
        secondary_agg = []
    
    if type(years) is not list:
        years = [years]
    
    # Now, we're ready to complete the consolidation of our historical data.
    # Processing is going to operate in a loop. For each year, we're going
    # to do the following:
    # 1. read our inputs (specifically, the fields in 'fields'), 
    # 2. centroid-ize them
    # 3. intersect them with the init_features, 
    # 4. sum our fields of interest on the 'init_features_id_field'
    
    # NEED TO EDIT THIS TO MATCH NEW INPUTS AND MULTLIST APPEND STRUCTURE
    # Start at fields, because we need to incorporate null replacements
    # also need this in the parametrization of 'secondary_summary_field'
    
    for yr in years:
        syr = str(yr)
        print("-- processing " + str(syr))
        
        # 1. read fields of interest
        print("---- reading fields of interest")
        
        input_path = input_feature_class_format.replace("{Year}", syr)
        sr = arcpy.Describe(input_path).spatialReference
        
        if secondary_summary_field is not None:
            ssf = list(secondary_summary_field.keys())[0]
            if secondary_match_field is None:
                fields_complete = {**fields, **secondary_summary_field}
            else:
                fields_complete = {**fields, **secondary_match_field}
        fn = list(fields_complete.keys()) + ["SHAPE@X", "SHAPE@Y"]
        
        inputs_array = arcpy.da.FeatureClassToNumPyArray(in_table = input_path,
                                                         field_names = fn,
                                                         spatial_reference = sr,
                                                         null_value = fields_complete)
        
        # 2. converting to centroids
        print("---- converting to centroids")
        
        centroids_path = os.path.join(intmd_gdb,
                                      ''.join(["Centroids_", syr]))
        arcpy.da.NumPyArrayToFeatureClass(in_array = inputs_array,
                                          out_table = centroids_path,
                                          shape_fields = ["SHAPE@X", "SHAPE@Y"],
                                          spatial_reference = sr)
        
        # 3. intersect with aggregating geometry
        print("---- intersecting with aggregating geometry")
        intersection_path = os.path.join(intmd_gdb,
                                        ''.join(["Intersection_", syr]))
        arcpy.Intersect_analysis(in_features = [centroids_path, 
                                                init_features_path],
                                 out_feature_class = intersection_path)
        
        # 4. summing on aggregating geometry ID (and, if provided, an LU
        # field). If an LU field is provided, we'll need to tie it back to
        # the reference path with simplified land uses if requested). We'll 
        # also take this opportunity to tag these data with the year, so we 
        # don't confuse years in our final
        # consolidated dataset
        print("---- summing to the aggregate geometry")
        
        # First is loading the data
        keep_vars = [init_features_id_field] + list(fields_complete.keys())
        df = arcpy.da.FeatureClassToNumPyArray(in_table = intersection_path,
                                               field_names = keep_vars,
                                               spatial_reference = sr,
                                               null_value = fields)
        df = pd.DataFrame(df)
        
        # Second is adding the LU reference (if requested)
        if secondary_match_field is not None:
            smf = list(secondary_match_field.keys())[0]
            sec = pd.read_csv(secondary_reference)
            sec = sec[[smf, ssf]]
            df = pd.merge(df, 
                          sec,
                          left_on = smf,
                          right_on = smf,
                          how = "left")
            df = df.drop(columns = smf)
        
        # Third.A is summarization for the YEAR ONLY
        summary_fields = list(fields.keys())
        year_summary = df.groupby(init_features_id_field)[summary_fields].apply(np.sum, axis=0).reset_index()
        year_summary.insert(loc = 1,
                            column = "Year",
                            value = yr)
        years_agg = pd.concat(years_agg, axis = 0)
        
        # Third.B is a summarization for YEAR AND SECONDARY FIELD
        if secondary_summary_field is not None:
            group_fields = [init_features_id_field, ssf]
            sec_summary = df.groupby(group_fields)[summary_fields].apply(np.sum, axis=0).reset_index()
            sec_summary.insert(loc = 2,
                               column = "Year",
                               value = yr)
            secondary_agg = pd.concat(secondary_agg, axis = 0)
    
    # Finally, we delete our intermediates
    # This currently isn't running bc of locks on the .gdb... not a huge deal,
    # but would be nice if it worked...
    # print("-- deleting intermediates")
    # shutil.rmtree(temp_dir)
    
    # Joining results --------------------------------------------------------
    
    print("")
    print("Saving results")
    
    # First, we'll want to 'fill out' our tables. The save feature classes
    # are complete with every combination of grouping fields, while the
    # results are not necessarily. To complete the filling, we read in the
    # save classes, merge with the results, and fill NAs with 0
    print("-- filling NAs with 0 for years summary")
    
    year_gf = [init_features_id_field, "Year"]
    year_fc = arcpy.da.FeatureClassToNumPyArray(in_table = save_feature_class_path,
                                                field_names = year_gf)
    years_agg = pd.merge(year_fc,
                         years_agg,
                         left_on = year_gf,
                         right_on = year_gf,
                         how = "left")
    years_agg = years_agg.fillna(0)
    
    if secondary_summary_field is not None:
        print("-- filling NAs with 0 for secondary summary")
        
        sec_gf = year_gf + [ssf]
        sec_fc = arcpy.da.FeatureClassToNumPyArray(in_table = secondary_save_path,
                                                   field_names = sec_gf)
        secondary_agg = pd.merge(sec_fc,
                                 secondary_agg,
                                 left_on = sec_gf,
                                 right_on = sec_gf,
                                 how = "left")
        secondary_agg = secondary_agg.fillna(0)
    
    # Saving the results is a simple matter of joining the created dataframes
    # back to the appropriate save feature classes. For the year summaries,
    # we join on the init_features_id_field and Year. For the secondary
    # summaries, we join on the init_features_id_field, Year, and
    # secondary_summary_field
    print("-- joining year results to save feature class")
    
    year_et = np.rec.fromrecords(recList = years_agg.values, 
                                 names = years_agg.dtypes.index.tolist())
    year_et = np.array(year_et)
    arcpy.da.ExtendTable(in_table = save_feature_class_path,
                         table_match_field = year_gf,
                         in_array = year_et,
                         array_match_field = year_gf)
    
    if secondary_summary_field is not None:
        print("-- joining year-secondary results to save feature class")

        sec_et = np.rec.fromrecords(recList = secondary_agg.values, 
                                    names = secondary_agg.dtypes.index.tolist())
        sec_et = np.array(sec_et)
        arcpy.da.ExtendTable(in_table = secondary_save_path,
                             table_match_field = sec_gf,
                             in_array = sec_et,
                             array_match_field = sec_gf)
        
    # Done -------------------------------------------------------------------
    
    print("")
    print("Done!")
    print("Year-summarized fields saved to: " + save_feature_class_path)
    if secondary_summary_field is not None:
        print("Year-secondary-summarized fields saved to: " + secondary_save_path)
    print("")
    if secondary_summary_field is not None:
        return({"Year": save_feature_class_path,
                "Year-secondary": secondary_save_path})
    else:
        return(save_feature_class_path)
    
