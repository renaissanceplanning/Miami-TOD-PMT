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
    
    '''
    Parameters
    ----------
    aggregating_geometry_path: path
        file path to polygon feature class, to which inputs will be aggregated
        for reporting
    aggregating_geometry_id_field: str
        field name of unique identifier for aggregating geometry features
    years: list of ints
        years of historical data to be aggregated
    inputs_gdb_format: directory
        gdb (/FeatureClass) location of the inputs, with a formatting wild
        card for year; see notes on how formatting should be done
    inputs_fc_format: str
        name of inputs feature class within `inputs_gdb_format`, with a
        formatting wild card for year; see notes on how formatting should
        be done
    inputs_field_names: list of str
        field names of `inputs` attributes to be aggregated
    save_gdb_path: path
        file path for saving the aggregated historical data
    lu_field: str
        land use field in the inputs; should only be provided if the `inputs
        fields` should be summarized by land use as well as `aggregating
        geometry` feature. See notes for usage
    lu_reference_path: path
        path to a land use reference .csv; should only be provided if the
        reference .csv contains the land use field on which you want to
        aggregate, in which case the inputs will be joined to this table
        on `lu_field`. See notes for usage
    lu_reference_field: str
        field name in the land use reference .csv defining the land use
        attribute on which you want to aggregated. See notes for usage
        
    Notes
    -----
    1. For `inputs_gdb_format` and `inputs_fc_format`, the assumption is that
    there is some consistent naming structure that points to the same data
    over different years, and that this naming structure varies only on year.
    Wherever a year appears in either the gdb directory or the feature class
    name, replace the year with `{Year}` in the provided string. For example,
    if data for 2010 is stored at `Data_2010.gdb/Features` (and similarly
    for other years), then `inputs_gdb_format = 'Data_{Year}.gdb'` and 
    `inputs_fc_format = 'Features'`. Similarly, if the data for 2010 is stored
    at 'Data.gdb/Features_2010' (and similarly for other years), then
    `inputs_gdb_format = 'Data.gdb'` and `inputs_fc_format = 'Features_{Year}'`
    
    2. The `lu_{}` fields need only be specified if you want the data
    aggregated by land use as well as the aggregating geometry. `lu_field`
    defines the land use field in the inputs. `lu_reference_path` should
    only be provided if there is a reference table with a more appropriate
    land use field on which to aggregate. If provided, it MUST contain
    `lu_field` as a field, to provide a valid join field to the inputs; if not
    provided but `lu_field` is, land use aggregation will be completed on
    `lu_field` in the inputs. Finally, `lu_reference_field` is the name of the 
    more appropriate land use field mentioned above, and must be provided if
    `lu_reference_path` is.
    
    Returns
    -------
    Saves a feature class to `save_gdb_path` of `inputs_field_names`
    aggregated to the geometries of `aggregating_geometry_path` (and
    potentially a land use field) for all years in `years`. The save location
    will be returned to confirm saving.
    '''
    
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
    print("-- setting up a temporary location for intermediates")
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path = temp_dir,
                                   out_name = "Intermediates.gdb")
    intmd_gdb = os.path.join(temp_dir, "Intermediates.gdb")
    
    # For loop processing, we'll store the results of each iteration in a list
    # that we'll later concat into a single df
    agdf = []
    
    # We're also relying on list comprehension of the input fields, so we'll
    # verify that our input fields are in a list
    if type(inputs_field_names) is not list:
        inputs_field_names = [f for f in [inputs_field_names]]
    
    # Now, we're ready to complete the consolidation of our historical data.
    # Processing is going to operate in a loop. For each year, we're going
    # to do the following:
    # 1. read our inputs (specifically, the fields in 'inputs_field_names'), 
    # 2. centroid-ize them
    # 3. intersect them with the aggregating geometry, 
    # 4. sum our fields of interest on the 'aggregating_geometry_id_field'
    
    for yr in years:
        syr = str(yr)
        print("-- processing " + str(syr))
        
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
    print("-- attributing the template with a repetition count")
    
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
    rep_fields = ["GEOID10","Count","SHAPE@"]
    with arcpy.da.SearchCursor(template_path, rep_fields) as curs_in:
        flds_in = curs_in.fields
        idx_cnt = flds_in.index("Count")
        with arcpy.da.InsertCursor(repeated_path, rep_fields) as curs_out:
            for row in curs_in:
                cnt = row[idx_cnt]
                for i in range(0, cnt):
                    curs_out.insertRow(row)
    
    # We'll delete some intermediates here too
    arcpy.DeleteField_management(repeated_path,
                                  drop_field = "Count")
    arcpy.Delete_management(template_path)
    del curs_in
    del curs_out
    
    # Next, we need to add a field to join on. Note that because our
    # initialized feature class and our results now have the same
    # 'aggregating geometry id field' column, if we sort on that column,
    # the frames should be in the exact same order. Then, we can just
    # attribute each with a sequential ID for easy binding. First, we'll add
    # the join field to the save feature class by sorting and calculating
    # a new field
    print("-- attributing the save feature class with a join field")
    
    arcpy.Sort_management(in_dataset = repeated_path,
                          out_dataset = save_gdb_path,
                          sort_field = aggregating_geometry_id_field)
    arcpy.Delete_management(repeated_path)
    codeblock = 'val = 0 \ndef processID(): \n    global val \n    start = 1 \n    if (val == 0):  \n        val = start\n    else:  \n        val += 1  \n    return val'
    arcpy.AddField_management(in_table = save_gdb_path,
                              field_name = "JoinID",
                              field_type = "LONG",
                              field_is_required = "NON_REQUIRED")
    arcpy.CalculateField_management(in_table = save_gdb_path,
                                    field = "JoinID",
                                    expression = "processID()",
                                    expression_type = "PYTHON3",
                                    code_block = codeblock)
    
    # Now we do the same thing to our results
    print("-- attributing the results with a join field")
    
    agdf = agdf.sort_values(aggregating_geometry_id_field).reset_index(drop=True)
    agdf["JoinID"] = [x for x in np.arange(1, len(agdf.index)+1)]
    agdf = agdf.drop(columns = aggregating_geometry_id_field)
    
    # With both now atttributed with a JoinID, all that's left to do is join
    # up our results
    print("-- joining results to the save feature class")
    
    df_et = np.rec.fromrecords(recList = agdf.values, 
                               names = agdf.dtypes.index.tolist())
    df_et = np.array(df_et)
    arcpy.da.ExtendTable(in_table = save_gdb_path,
                         table_match_field = "JoinID",
                         in_array = df_et,
                         array_match_field = "JoinID")
    arcpy.DeleteField_management(in_table = save_gdb_path,
                                 drop_field = "JoinID")
    
    # Done -------------------------------------------------------------------
    
    print("")
    print("Done!")
    print("Historical data consolidated to: " + save_gdb_path)
    print("")
    return(save_gdb_path)
    
# %% Main

# This is for parcel characteristics (e.g. living area)
if __name__ == "__main__":
    # Inputs for TOTAL LIVING AREA AND PARCEL LAND AREA
    aggregating_geometry_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Blocks.gdb/Blocks_2019"
    aggregating_geometry_id_field = "GEOID10"
    years = [2014, 2015, 2016, 2017, 2018, 2019]
    inputs_gdb_format = "K:/Projects/MiamiDade/PMT/Data/Cleaned/parcels.gdb"
    inputs_fc_format = "Miami_{Year}"
    inputs_field_names = ["TOT_LVG_AREA", "LND_SQFOOT"]
    save_gdb_path = "K:/Projects/MiamiDade/PMT/Data/PMT_Trend.gdb/parcels/historical_FAR_inputs"
    lu_field = "DOR_UC"
    lu_reference_path = "K:/Projects/MiamiDade/PMT/Data/Reference/Land_Use_Recode.csv"
    lu_reference_field = "GN_VA_LU"
    
    # Inputs for NUMBER OF RESIDENTIAL UNITS
    aggregating_geometry_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Blocks.gdb/Blocks_2019"
    aggregating_geometry_id_field = "GEOID10"
    years = [2014, 2015, 2016, 2017, 2018, 2019]
    inputs_gdb_format = "K:/Projects/MiamiDade/PMT/Data/Cleaned/parcels.gdb"
    inputs_fc_format = "Miami_{Year}"
    inputs_field_names = "NO_RES_UNTS"
    save_gdb_path = "K:/Projects/MiamiDade/PMT/Data/PMT_Trend.gdb/parcels/historical_res_units"
    lu_field = None
    lu_reference_path = None
    lu_reference_field = None
    
    # Inputs for TOTAL JOBS AND POPULATION
    aggregating_geometry_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Blocks.gdb/Blocks_2019"
    aggregating_geometry_id_field = "GEOID10"
    years = [2014, 2015, 2016, 2017, 2018, 2019]
    inputs_gdb_format = "K:/Projects/MiamiDade/PMT/PMT_{Year}.gdb/Parcels"
    inputs_fc_format = "socioeconomic_and_demographic"
    inputs_field_names = ["Total_Employment", "Total_Population"]
    save_gdb_path = "K:/Projects/MiamiDade/PMT/Data/PMT_Trend.gdb/parcels/historical_total_jobs_pop"
    lu_field = None
    lu_reference_path = None
    lu_reference_field = None
    
    # Inputs for COMMUTES
    aggregating_geometry_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Blocks.gdb/Blocks_2019"
    aggregating_geometry_id_field = "GEOID10"
    years = [2014, 2015, 2016, 2017, 2018, 2019]
    inputs_gdb_format = "K:/Projects/MiamiDade/PMT/PMT_{Year}.gdb/Parcels"
    inputs_fc_format = "socioeconomic_and_demographic"
    inputs_field_names = ["Total_Commutes",
                          "Drove_PAR","Carpool_PAR","Transit_PAR",
                          "NonMotor_PAR","Work_From_Home_PAR","AllOther_PAR"]
    save_gdb_path = "K:/Projects/MiamiDade/PMT/Data/PMT_Trend.gdb/parcels/historical_commutes"
    lu_field = None
    lu_reference_path = None
    lu_reference_field = None
    
    # Inputs for JOBS BY INDUSTRY CLUSTER
    
    
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