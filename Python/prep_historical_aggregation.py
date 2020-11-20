# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 12:02:49 2020

@author: AZ7
"""

# %% Imports

import arcpy
import os
import tempfile
import numpy as np
import pandas as pd
# import shutil

# %% Functions

def prep_historical_aggregation(aggregating_geometry_path,
                                aggregating_geometry_id_field,
                                years,
                                inputs_gdb_format,
                                inputs_fc_format,
                                inputs_field_names,
                                save_gdb_path,
                                lu_field = None,
                                lu_reference_path = None,
                                lu_reference_field = None):
    
    # Initialize a template for results ---------------------------------
    
    print("")
    print("Initializing a template for consolidated results")
    
    # First, we need to initialize a template for the feature class we'll save
    # our results to. It's a template for now, because ultimately we'll have
    # to repeat rows to store data for the same geometry across multiple years
    # and land uses. So, our template is just the geometries; once we know how
    # many times we'll need to repeat each geometry, we can use this template
    # to initialize a save feature class, and then delete the template. Our
    # results are going to be at the level of the aggregating geometry, so our
    # initialized feature class can just be a copied version of
    # 'aggregating_geometry_path' that retains 'aggregating_geometry_id_field'
    # and the polygon geometry
    # Thanks to: https://gis.stackexchange.com/questions/229187/copying-only-certain-fields-columns-from-shapefile-into-new-shapefile-using-mode
    gdb_path, save_name = os.path.split(save_gdb_path)
    fmap = arcpy.FieldMappings()
    fmap.addTable(aggregating_geometry_path)
    fields = {f.name: f for f in arcpy.ListFields(aggregating_geometry_path)}
    for fname, fld in fields.items():
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            if fname != aggregating_geometry_id_field:
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    arcpy.conversion.FeatureClassToFeatureClass(in_features = aggregating_geometry_path, 
                                                out_path = gdb_path,
                                                out_name = "template",
                                                field_mapping = fmap)
    template_path = os.path.join(gdb_path,
                                 "template")
    
    # Consolidating the historical data --------------------------------------
    
    print("")
    print("Consolidating historical data")
    
    # Because we're loop processing, we can't use the in_memory space because
    # of locking issues. So we need to set up a temp directory to save our
    # intermediates (that we'll delete later). We'll also need to create a
    # .gdb in this location to save our data. If we save our data as .shp
    # outside of a .gdb, we run the risk of having our field names truncated,
    # which is bad if we want to keep referencing our input fields (which
    # we most definitely do)
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path = temp_dir,
                                   out_name = "Intermediates.gdb")
    intmd_gdb = os.path.join(temp_dir, "Intermediates.gdb")
    
    # For loop processing, we'll store the results of each iteration in a list
    # that we'll later concat into a single df
    agdf = []
    
    # Now, we're ready to complete the consolidation of our historical data.
    # Processing is going to operate in a loop. For each year, we're going
    # to do the following:
    # 1. read our inputs (specifically, the fields in 'inputs_field_names'), 
    # 2. centroid-ize them
    # 3. intersect them with the aggregating geometry, 
    # 4. sum our fields of interest on the 'aggregating_geometry_id_field'
    
    for yr in years:
        syr = str(yr)
        print("-- " + str(syr))
        
        # 1. read fields of interest
        print("---- reading fields of interest")
        inputs_path = os.path.join(inputs_gdb_format.replace("{Year}", syr),
                                   inputs_fc_format.replace("{Year}", syr))
        sr = arcpy.Describe(inputs_path).spatialReference
        fn = inputs_field_names + ["SHAPE@X", "SHAPE@Y"]
        if lu_field is not None:
            fn = [lu_field] + fn
        inputs_array = arcpy.da.FeatureClassToNumPyArray(in_table = inputs_path,
                                                         field_names = fn,
                                                         spatial_reference = sr,
                                                         null_value = 0)
        
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
                                                template_path],
                                 out_feature_class = intersection_path)
        
        # 4. summing on aggregating geometry ID (and, if provided, an LU
        # field). If an LU field is provided, we'll need to tie it back to
        # the reference path with simplified land uses if requested). We'll 
        # also take this opportunity to tag these data with the year, so we 
        # don't confuse years in our final
        # consolidated dataset
        print("---- summing to the aggregate geometry")
        
        # First is loading the data
        keep_vars = [aggregating_geometry_id_field] + inputs_field_names
        if lu_field is not None:
            keep_vars = keep_vars + [lu_field]
        df = arcpy.da.FeatureClassToNumPyArray(in_table = intersection_path,
                                               field_names = keep_vars,
                                               spatial_reference = sr,
                                               null_value = 0)
        df = pd.DataFrame(df)
        
        # Second is adding the LU reference (if requested)
        if lu_reference_path is not None:
            lu = pd.read_csv(lu_reference_path)
            lu = lu[[lu_field, lu_reference_field]]
            df = pd.merge(df, 
                          lu,
                          left_on = lu_field,
                          right_on = lu_field,
                          how = "left")
            df = df.drop(columns = lu_field)
        
        # Third is the summarization
        group_fields = [aggregating_geometry_id_field]
        if lu_field is not None:
            group_fields = group_fields + [lu_reference_field]
        df = df.groupby(group_fields)[inputs_field_names].apply(np.sum, axis=0).reset_index()
       
        # Then we just add a year column and we're done 
        if lu_field is not None:
            idx = 2
        else:
            idx = 1
        df.insert(loc = idx,
                  column = "Year",
                  value = yr)
        agdf.append(df)
        
    # Now that we're done loop processing, we just concat our list of dfs into
    # a single dataframe
    print("-- merging results")
    agdf = pd.concat(agdf, axis = 0)
    
    # Finally, we delete our intermediates
    # This currently isn't running bc of locks on the .gdb... not a huge deal,
    # but would be nice if it worked...
    # print("-- deleting intermediates")
    # shutil.rmtree(temp_dir)
    
    # Setting up our save feature class --------------------------------------
    
    print("")
    print("Saving results")
    
    # Our results feature the same aggregating geometry feature multiple times
    # because of both year and land use. Consequently, the number of
    # repetitions needed varies by feature. So, our first step is to
    # attribute the template feature class with a variable for the number
    # of repetitions
    print("-- attributing template with repetition count")
    
    template = arcpy.da.FeatureClassToNumPyArray(in_table = template_path,
                                                 field_names = aggregating_geometry_id_field,
                                                 spatial_reference = sr,
                                                 null_value = 0)
    template = pd.DataFrame(template)
    counts = agdf[aggregating_geometry_id_field].value_counts()
    counts = counts.reset_index()
    counts.columns = [aggregating_geometry_id_field, "Count"]
    counts = pd.merge(template, counts,
                      left_on = aggregating_geometry_id_field,
                      right_on = aggregating_geometry_id_field,
                      how = "left")
    counts = counts.fillna(0)
    counts["Count"] = [int(x) for x in counts["Count"]]
    counts_et = np.rec.fromrecords(recList = counts.values, 
                                   names = counts.dtypes.index.tolist())
    counts_et = np.array(counts_et)
    arcpy.da.ExtendTable(in_table = template_path,
                         table_match_field = aggregating_geometry_id_field,
                         in_array = counts_et,
                         array_match_field = aggregating_geometry_id_field)
    
    # Now we need to repeat the rows in the template to form a new feature
    # class. We'll use the 'Count' field we just created to do this
    # Thanks to: https://community.esri.com/t5/geoprocessing-questions/copy-features-a-number-of-times-based-on-a-numeric-field-value/m-p/117358#M4021
    print("-- initializing a save feature class with repeated features")

    arcpy.CreateFeatureclass_management(out_path = gdb_path,
                                        out_name = "repeated", 
                                        geometry_type = "POLYGON", 
                                        template = template_path,
                                        has_m = "SAME_AS_TEMPLATE", 
                                        has_z = "SAME_AS_TEMPLATE", 
                                        spatial_reference = sr)
    repeated_path = os.path.join(gdb_path, 
                                 "repeated")
    
    # Im here. Read in agdf from Downloads
    with arcpy.da.SearchCursor(template_path, '*') as curs_in:
        flds_in = curs_in.fields
        idx_cnt = flds_in.index("Count")
        with arcpy.da.InsertCursor(repeated_path, '*') as curs_out:
            for row in curs_in:
                cnt = row[idx_cnt]
                for i in range(0, cnt):
                    curs_out.insertRow(row)
    
    # Next, we need to add a field to join on. Note that because our
    # initialized feature class and our results now have the same
    # 'aggregating geometry id field' column, if we sort on that column,
    # the frames should be in the exact same order. Then, we can just
    # attribute each with a sequential ID for easy binding. First, we'll add
    # the join field to the save feature class by sorting and calculating
    # a new field
    
    
    arcpy.AddField_management(in_table = save_gdb_path,
                              field_name = "RepID",
                              field_type = "SHORT")
    
             
    # Now our feature class and results data match, but we can't do a join 
    # because our 'aggregating_geometry_id_field' is not unique (because of 
    # repetitions). So, we need a repetition ID in both our data and the save 
    # feature class for unique joining
    print("-- attributing the results with a repetition ID")
    
    rep_id = [np.arange(1, n+1).tolist() for n in counts["Count"]]
    rep_id = [i for sublist in rep_id for i in sublist]
    agid = np.repeat(counts[aggregating_geometry_id_field], counts["Count"]).tolist()
    rep_id = pd.DataFrame.from_dict({aggregating_geometry_id_field: agid,
                                     "RepID": rep_id})
    agdf = agdf.sort_values(aggregating_geometry_id_field).reset_index(drop=True)
    rep_id = rep_id.sort_values([aggregating_geometry_id_field, "RepID"]).reset_index(drop=True)
    agdf["RepID"] = rep_id["RepID"]
    
    # Writing ----------------------------------------------------------------
    
    print("")
    print("Joining results to the initialized feature class")
    
    # The last step is then just joining our consolidated data to our
    # initialized feature class  
    df_et = np.rec.fromrecords(recList = all_agg.values, 
                               names = all_agg.dtypes.index.tolist())
    df_et = np.array(df_et)
    arcpy.da.ExtendTable(in_table = ifc_path,
                         table_match_field = aggregating_geometry_id_field,
                         in_array = df_et,
                         array_match_field = aggregating_geometry_id_field)
    
    # Done -------------------------------------------------------------------
    
    print("")
    print("Done!")
    print("Historical data consolidated to: " + save_gdb_path)
    print("")
    return(save_gdb_path)
    
# %% Main

# This is for parcel characteristics (e.g. living area)
if __name__ == "__main__":
    # Inputs
    aggregating_geometry_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Blocks.gdb/Blocks_2019"
    aggregating_geometry_id_field = "GEOID10"
    years = [2014, 2015, 2016, 2017, 2018, 2019]
    inputs_gdb_format = "K:/Projects/MiamiDade/PMT/Data/Cleaned/parcels.gdb"
    inputs_fc_format = "Miami_{Year}"
    inputs_field_names = ["TOT_LVG_AREA", "LND_SQFOOT", "NO_RES_UNTS"]
    save_gdb_path = "K:/Projects/MiamiDade/PMT/PMT_Trend.gdb/parcels/historical_parcel_chars"
    lu_field = "DOR_UC"
    lu_reference_path = "K:/Projects/MiamiDade/PMT/Data/Reference/Land_Use_Recode.csv"
    lu_reference_field = "GN_VA_LU"
    
    # Function
    prep_historical_aggregation(aggregating_geometry_path = aggregating_geometry_path,
                                aggregating_geometry_id_field = aggregating_geometry_id_field,
                                years = years,
                                inputs_gdb_format = inputs_gdb_format,
                                inputs_fc_format = inputs_fc_format,
                                inputs_field_names = inputs_field_names,
                                save_gdb_path = save_gdb_path,
                                lu_field = lu_field,
                                lu_reference_path = lu_reference_path,
                                lu_reference_field = lu_reference_field)