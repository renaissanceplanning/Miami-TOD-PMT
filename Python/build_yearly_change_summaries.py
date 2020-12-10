# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 13:35:11 2020

@author: AZ7
"""

# %% Imports
import arcpy
import os
import re
import numpy as np
import pandas as pd

# %% Functions
def prep_yearly_change_summaries(init_features_path,
                                 init_features_id_field,
                                 aggregated_summaries_path,
                                 save_feature_class_path,
                                 years = None,
                                 secondary_summary_field = None):
    '''
    Parameters
    ----------
    init_features_path : Path
        file path to features used to create the features of
        `aggregated_summaries_path`
    init_features_id_field : str
        field name for unique ID in `init_features`
    aggregated_summaries_path : Path
        file path to feature class output of `prep_aggregated_summaries()`
    save_feature_class_path : Path
        file path to save location for the yearly change summaries
    years : list of ints, or list of list of ints, optional
        length-2 lists specifying year ranges over which to calculate change
        summaries. You can provide as many as you like
        The default is None -- in this case, the min and max years in the data
        will be used
    secondary_summary_field : str, optional
        field name in `aggregated_summaries` that served as the secondary
        grouping field.
        The default is None -- meaning the `aggregated_summaries` were only
        summarized on year

    Returns
    -------
    a feature class with absolute and percent changes for each variable in 
    `aggregated_summaries_path` and each time interval in `years` will be
    saved to `save_feature_class_path`. the file path to the save location
    will be returned
    '''
    
    # Creating the save feature class ----------------------------------------
    
    # This function will create the save feature class if it doesn't exist,
    # or update the feature class if it does exist. This allows for this
    # function to be used multiple times on the same feature class, which is
    # ideal given that not all aggregated summaries may be prepared before
    # calculation of yearly change summaries
    
    if arcpy.Exists(save_feature_class_path):
        print("")
        print("'save_feature_class_path' already exists -- moving to data summaries")
    else:
        print("")
        print("Creating the save feature class")
        
        # Step 1 is loading the aggregated summaries to see how many times the
        # features in "init_features" will need to be repeated in the save
        # feature class
        print("-- calculating a feature repetition count")
        
        if secondary_summary_field is None:
            rep_count = 1
        else:
            ssf = arcpy.da.FeatureClassToNumPyArray(in_table = aggregated_summaries_path,
                                                    field_names = secondary_summary_field)
            uv = np.unique(ssf)
            rep_count = len(uv)
        
        # Step 2 is copying the init features to a template feature class. We
        # retain the geometry type of the aggregated_summaries (either point
        # or polygon)
        print("-- initializing a template")
        
        gdb_path, save_name = os.path.split(save_feature_class_path)
        gtype = arcpy.Describe(aggregated_summaries_path).shapeType
    
        if gtype == "Polygon":
            fmap = arcpy.FieldMappings()
            fmap.addTable(init_features_path)
            fields = {f.name: f for f in arcpy.ListFields(init_features_path)}
            for fname, fld in fields.items():
                if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
                    if fname != init_features_id_field:
                        fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
            arcpy.conversion.FeatureClassToFeatureClass(in_features = init_features_path, 
                                                        out_path = gdb_path,
                                                        out_name = "template",
                                                        field_mapping = fmap)
        else:
            init_fields = [init_features_id_field, "SHAPE@X", "SHAPE@Y"]
            init_sr = arcpy.Describe(init_features_path).spatialReference
            init_array = arcpy.da.FeatureClassToNumPyArray(in_table = init_features_path,
                                                           field_names = init_fields,
                                                           spatial_reference = init_sr)
            arcpy.da.NumPyArrayToFeatureClass(in_array = init_array,
                                              out_table = os.path.join(gdb_path, "template"),
                                              shape_fields = ["SHAPE@X", "SHAPE@Y"],
                                              spatial_reference = init_sr)
        template_path = os.path.join(gdb_path,
                                     "template")
        
        # Step 3 (the final step) is repeating features according to the
        # 'rep_count' calculated above
        
        if rep_count == 1:
            # If rep_count is 1, this means we need no repetitions -- we can
            # simply rename the template to create our save class
            print("-- no repetitions needed, renaming to save_feature_class_path")
            
            arcpy.Rename_management(in_data = template_path,
                                    out_data = save_feature_class_path)
        else:
            # If rep_count is > 1, this means we need repetitions because of
            # the presence of a secondary summary field. This makes our lives
            # more complicated -- we need to repeat the features, create a
            # join field, create an array of appropriately repeated unique
            # values of the secondary summary field (attributed with the join
            # field), and join the repeated features and array
            
            # First we repeat the features 'rep_count' times
            print("-- repeating features in the template")
            
            sr = arcpy.Describe(template_path).spatialReference
            arcpy.CreateFeatureclass_management(out_path = gdb_path,
                                                out_name = save_name, 
                                                # geometry_type = "POLYGON", 
                                                template = template_path,
                                                has_m = "SAME_AS_TEMPLATE", 
                                                has_z = "SAME_AS_TEMPLATE", 
                                                spatial_reference = sr)
            rep_fields = ["GEOID10","SHAPE@"]
            with arcpy.da.SearchCursor(template_path, rep_fields) as curs_in:
                with arcpy.da.InsertCursor(save_feature_class_path, rep_fields) as curs_out:
                    for row in curs_in:
                        for i in range(rep_count):
                            curs_out.insertRow(row)
            arcpy.Delete_management(template_path)
            
            # Then we add the join field to the feature class
            print("-- adding a unique ID field to join secondary summary field")
            
            codeblock = 'val = 0 \ndef processID(): \n    global val \n    start = 1 \n    if (val == 0):  \n        val = start\n    else:  \n        val += 1  \n    return val'
            arcpy.AddField_management(in_table = save_feature_class_path,
                                      field_name = "ProcessID",
                                      field_type = "LONG",
                                      field_is_required = "NON_REQUIRED")
            arcpy.CalculateField_management(in_table = save_feature_class_path,
                                            field = "ProcessID",
                                            expression = "processID()",
                                            expression_type = "PYTHON3",
                                            code_block = codeblock)
            
            # Then we create the join array of the secondary field
            print("-- creating a join array of the secondary summary field")
            
            df = pd.DataFrame(uv, 
                              columns = secondary_summary_field)
            n = int(arcpy.GetCount_management(init_features_path).getOutput(0))
            df = pd.concat([df]*n).reset_index(drop=True)
            unid = arcpy.da.FeatureClassToNumPyArray(in_table = save_feature_class_path,
                                             field_names = "ProcessID")
            unid = unid.tolist()
            df["ProcessID"] = unid
            
            # Finally, we join the array and the feature class
            print("-- joining the secondary summary field to the save feature class")
            
            df_et = np.rec.fromrecords(recList = df.values, 
                               names = df.dtypes.index.tolist())
            df_et = np.array(df_et)
            arcpy.da.ExtendTable(in_table = save_feature_class_path,
                                 table_match_field = "ProcessID",
                                 in_array = df_et,
                                 array_match_field = "ProcessID")
            arcpy.DeleteField_management(in_table = save_feature_class_path,
                                         drop_field = "ProcessID")
    
    # Summarizing the data ---------------------------------------------------
    
    print("")
    print("Calculating the yearly change summaries")
    
    # First, we need to load our data. We'll load all fields that aren't ESRI
    # defaults -- OBJECTID, Shape_Length, Shape_Area, and Shape
    print("-- loading data")
    
    fn = [f.name for f in arcpy.ListFields(aggregated_summaries_path)]
    fn.remove(["Shape","Shape_Length","Shape_Area","OBJECTID"])
    aggsum = arcpy.da.FeatureClassToNumPyArray(in_table = aggregated_summaries_path,
                                               field_names = fn)
    
    # Next, we want to perform our summaries. We'll do these iteratively over
    # the years list. To do this, we need two things: 'years' to be
    # comprehensible as a list, and the field names for which we want to
    # produce the change summaries
    print("-- initializing loop processing")
    
    if type(years[0]) is not list:
        years = [years]
    
    exclude = [init_features_id_field, "Year"]
    if secondary_summary_field is not None:
        exclude = exclude + [secondary_summary_field]
    summary_fields = [c for c in df.columns if c not in exclude]
    index_fields = [f for f in exclude if f != "Year"]
        
    # Now we're into the loop processing. For each year pair, we will take an
    # absolute change and a percent change from the min year to the max year
    for yp in years:
        min_year = yp[0]
        max_year = yp[1]
        print("-- calculating change for " + str(min_year) + " to " + str(max_year))
        
        mymy = aggsum[aggsum.Year.isin(yp)]
        mymy["Year"] = df["Year"].apply(str)
        mymy = mymy.set_index(index_fields).pivot(columns="Year").reset_index()
        colnames = ['_'.join(mli) for mli in mymy.columns.tolist()]
        colnames = [re.sub("_$", "", c) for c in colnames]
        mymy.columns = colnames
        
        # Now we need to do the summarization
