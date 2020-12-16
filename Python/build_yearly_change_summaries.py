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
import tempfile

# %% Functions
def prep_yearly_change_summaries(aggregated_dataset_path,
                                 aggregated_dataset_id_field,
                                 aggregated_dataset_year_field,
                                 aggregating_feature_id_field,
                                 save_path,                                 
                                 fields_to_summarize = None,
                                 grouping_fields = None,
                                 years = None,
                                 init_features_path = None):
    '''
    Parameters
    ----------
    aggregated_dataset_path : Path
        file path to an aggregated dataset
    aggregated_dataset_id_field : str
        field name for unique ID of the features in `aggregated_dataset`
    aggregated_dataset_year_field : str
        field name for the year field in `aggregated_dataset`
    aggregating_feature_id_field : str
        field name for unique ID of the features used to build
        `aggregated_dataset` (i.e. the unique ID for `init_features_path`)
    save_path : Path
        file path to save location for the yearly change summaries
    fields_to_summarize : str, or list of str, optional
        fields in `aggregated_dataset` for which you want a yearly change
        summary
        The default is None -- all non-ESRI, non-ID fields will be summarized
    grouping_fields : str, or list of str, optional
        fields in `aggregated_dataset` on which the data was summarized. 
        Use for files containing multi-level summaries; this parameter should
        define the fields for the second, third, ... level summaries
        The default is None -- assumes the data reflects a single level
        summary on `init_features_id_field` only
    years : list of ints, or list of list of ints, optional
        length-2 lists specifying year ranges over which to calculate change
        summaries. You can provide as many as you like
        The default is None -- in this case, the min and max years in the data
        will be used
    init_features_path : Path, optional
        file path to features used to create the features of
        `aggregated_dataset_path`. Only needs to be provided if `save_path`
        doesn't exist (then this will be used to initialize the feature class)

    Returns
    -------
    a feature class with absolute and percent changes for each of the 
    `to_summarize_fields` in `aggregated_dataset_path` and each time interval in `years` will be
    saved to `save_feature_class_path`. the file path to the save location
    will be returned
    '''
    
    # 1. Summarizing the data
    # -----------------------
    
    print("")
    print("1. Calculating the yearly change summaries")
    
    # First, we need to load relevant attributes from the aggregated dataset.
    # Variables to load can be specified two ways: if 'fields_to_summarize'
    # isn't provided, we need to read all non-ESRI fields. if it is provided,
    # we need (1) aggregated_dataset_id_field, (2) aggregating_feature_id_field,
    # and (3) the fields_to_summarize
    print("1.1 identifying relevant variables to load")
    
    if grouping_fields is not None:
        if type(grouping_fields) is str:
            grouping_fields = [grouping_fields]
        grouping_fields.sort() #ensures consistent data sorting regardless of order of inputs
            
    if fields_to_summarize is None:
        # Give us everything!
        fn = [f.name for f in arcpy.ListFields(aggregated_dataset_path)]
        fn = [f for f in fn if f not in ["Shape","Shape_Length","Shape_Area","OBJECTID"]]
    else:
        # Give us the requisite subset
        if type(fields_to_summarize) is str:
            fields_to_summarize = [fields_to_summarize]
        fn = [aggregated_dataset_id_field, 
              aggregated_dataset_year_field,
              aggregating_feature_id_field] + grouping_fields + fields_to_summarize
    
    # Now, we're ready to load the data
    print("1.2 loading the data")
    
    if arcpy.Describe(aggregated_dataset_path).dataType == "Table":
        df = arcpy.da.TableToNumPyArray(in_table = aggregated_dataset_path,
                                        field_names = fn)
    else:
        df = arcpy.da.FeatureClassToNumPyArray(in_table = aggregated_dataset_path,
                                               field_names = fn)
    df = pd.DataFrame(df)
    df = df.drop(columns = aggregated_dataset_id_field)
    
    # Next, we want to perform our summaries. We'll do these iteratively over
    # the years list. To do this, we need two things (defined in line below)
    print("1.3 initializing loop processing")
    
    # First, we need 'years' to be comprehensible as a list
    # Note that we'll go ahead and create the years here if it is None
    if years is None:
        years = [[min(df[aggregated_dataset_year_field]),
                  max(df[aggregated_dataset_year_field])]]
    else:
        if type(years[0]) is not list:
            years = [years]
    
    # And second, we need the (1) the set of grouping variables and (2) the 
    # field names for which we want to produce the change summaries. If 
    # fields_to_summarize is provided, we don't need to do anything for (2). 
    # Otherwise, we need to set that variable
    exclude_fields = [aggregated_dataset_year_field,
                      aggregating_feature_id_field]
    if grouping_fields is not None:
        exclude_fields = exclude_fields + grouping_fields
    index_fields = [f for f in exclude_fields 
                    if f != aggregated_dataset_year_field]
    if fields_to_summarize is None:
        fields_to_summarize = [c for c in df.columns
                               if c not in exclude_fields]
        
    # Now we're into the loop processing. For each year pair, we will take an
    # absolute change and a percent change from the min year to the max year
    print("1.4 loop processing yearly change")
    
    # (Initialize with a list to store results for each year pair)
    results = []
    
    for yp in years:
        # First, set the years
        min_year = yp[0]
        max_year = yp[1]
        print("---- " + str(min_year) + " to " + str(max_year))
        
        #  Next, filter to only the years of interest. Then, we will want to
        # spread the fields of interest wide on year. Finally, we rename the 
        # columns to reflect the year
        print("1.4.1 formatting data for between-years comparison")
        
        mymy = df[df.Year.isin(yp)]
        mymy["Year"] = df["Year"].apply(str)
        mymy = mymy.set_index(index_fields).pivot(columns="Year").reset_index()
        colnames = ['_'.join(mli) for mli in mymy.columns.tolist()]
        colnames = [re.sub("_$", "", c) for c in colnames]
        mymy.columns = colnames
        
        # Now we need to do the summarization. We do this in a loop over
        # fields_to_summarize
        print("1.4.2 calculating the between-years comparisons")
        
        # (Initialize a dataframe to store results, and a shortened version
        # of the years for variable naming)
        comp_df = pd.DataFrame(mymy[index_fields])
        year_id = ''.join([str(min_year)[-2:], str(max_year)[-2:]])
        
        for field in fields_to_summarize:
            # First, we need to extract the fields we're working with. To do
            # this, we regex match to the field of interest, and use the
            # attached year to identify which field is the base year and which
            # is the future year
            r = re.compile(field)
            comp_fields = [c for c in colnames if r.match(c)]
            yrs = [int(c[-4:]) for c in comp_fields]
            base_field = comp_fields[np.argmin(yrs)]
            future_field = comp_fields[np.argmax(yrs)]
            
            # Then, reference the base/future fields to calculate an absolute
            # and percent change
            absolute = mymy[future_field] - mymy[base_field]
            percent = mymy[future_field] / mymy[base_field] - 1
            
            # Finally, append these to our results df with names reflecting
            # the field of interest, comp type, and years
            
            comp_df['_'.join([field, "ABS", year_id])] = absolute
            comp_df['_'.join([field, "PER", year_id])] = percent
        
        # This completes processing for a single year pair: to finalize, we
        # re-set our index fields, and append to the results list
        comp_df = comp_df.set_index(index_fields)
        results.append(comp_df)
    
    # Once loop processing is complete, we need to merge up everything in
    # our results list. At this point, we'll also want to reset our index
    # to get our index fields as columns, and sort on those columns. This
    # will allow us to add a unique ID that will be used for future joining
    # to a results feature class. The sorting ensures that the addition of the
    # unique ID is consistent across grouping field combinations
    print("1.5 formatting results")
    
    comp_df = pd.concat(results, axis = 1)
    comp_df = comp_df.reset_index()
    comp_df = comp_df.sort_values(index_fields)
    comp_df["ProcessID"] = np.arange(1, len(comp_df.index) + 1)
            
    # 2. Writing results
    # ------------------
    
    print("")
    print("2. Writing results")
     
    # (Initialize with a temporary gdb to save intermediates to)
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path = temp_dir,
                                   out_name = "Intermediates.gdb")
    intmd_gdb = os.path.join(temp_dir, "Intermediates.gdb")
    
    # We need to account for the fact that the file we're saving
    # to already exists! If it does, we want to add all summarized
    # fields that aren't already in the save path. If it doesn't, we
    # have more work to do (enumerated in line)
    
    if arcpy.Exists(save_path):
        print("---- file already exists")
        
        # Identifying any fields that exist in the data already -- 
        # we assume that if they exist but they're being calculated
        # again, this means we want to update them. So, to make the
        # extend table work, we need to delete those fields from the
        # existing dataset
        existing_fields = [f.name for f in arcpy.ListFields(save_path)]
        current_fields = comp_df.columns.tolist()
        current_fields = [c for c in current_fields if c != "ProcessID"]
        del_fields = [c for c in current_fields if c in existing_fields]
        if len(del_fields) > 0:
            print("------ deleting existing fields that are being updated")
            del_df = [d for d in del_fields if d in grouping_fields]
            del_path = [d for d in del_fields if d not in grouping_fields]
            if len(del_df) > 0:
                comp_df = comp_df.drop(columns = del_df)
            if len(del_path) > 0:
                arcpy.DeleteField_management(in_table = save_path,
                                             drop_field = del_fields)
        
        # Now we are ready to execute the merge
        print("------ merging in new fields")
        
        comp_et = np.rec.fromrecords(recList = comp_df.values, 
                                   names = comp_df.dtypes.index.tolist())
        comp_et = np.array(comp_et)
        arcpy.da.ExtendTable(in_table = save_path,
                             table_match_field = "ProcessID",
                             in_array = comp_et,
                             array_match_field = "ProcessID")
    else:
        # So this means our file doesn't exist -- SAD. So we need
        # to create it (if it's a point or polygon) and merge our
        # data in, or simply write the table (if it's a table)
        print("---- file does not exist; must create")
        
        # There's a lot more that needs to be done if we're
        # dealing with a shape. First, we need to initialize a
        # feature class for the shape. We give it a unique name
        # to put in the intermediates gdb, then write it there
        print("------ initializing a results feature class from init features")
        
        fmap = arcpy.FieldMappings()
        fmap.addTable(init_features_path)
        fn = {f.name: f for f in arcpy.ListFields(init_features_path)}
        for fname, fld in fn.items():
            if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
                if fname != aggregating_feature_id_field:
                    fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
        arcpy.conversion.FeatureClassToFeatureClass(in_features = init_features_path, 
                                                    out_path = intmd_gdb,
                                                    out_name = "COPY",
                                                    field_mapping = fmap)
        copy_path = os.path.join(intmd_gdb, "COPY")
            
        # Next, we have to deal with repetitions. If there's only 
        # 1, that  means no repetitions are necessary -- the copy 
        # class is the rep class. Otherwise, we need to complete 
        # the repetitions to build our "complete cases" set. We
        # give it a unique name to put in the intermediates gdb, 
        # then write it there
    
        n = int(arcpy.GetCount_management(init_features_path).getOutput(0))
        reps = len(df[index_fields].drop_duplicates().index) / n
        reps = int(reps)
        rep_path = copy_path.replace("COPY","REP")
        
        if reps == 1:
            arcpy.Rename_management(in_data = copy_path, 
                                    out_data = rep_path)
        else:
            print("------ repeating features in the initialized results feature class")
            sr = arcpy.Describe(copy_path).spatialReference
            arcpy.CreateFeatureclass_management(out_path = intmd_gdb,
                                                out_name = "REP", 
                                                template = copy_path,
                                                has_m = "SAME_AS_TEMPLATE", 
                                                has_z = "SAME_AS_TEMPLATE", 
                                                spatial_reference = sr)
            rep_fields = [aggregating_feature_id_field, "SHAPE@"]
            with arcpy.da.SearchCursor(copy_path, rep_fields) as curs_in:
                with arcpy.da.InsertCursor(rep_path, rep_fields) as curs_out:
                    for row in curs_in:
                        for i in range(reps):
                            curs_out.insertRow(row)
        
        # Now we have to sort according to the init_id -- this
        # will create our save feature class
        print("------ sorting the results feature class on feature ID")
        arcpy.Sort_management(in_dataset = rep_path,
                              out_dataset = save_path,
                              sort_field = aggregating_feature_id_field)
        
        # Then, we add our unique ID. Again, what we've created
        # is the "complete cases" of init_id's, so we've achieved
        # an exact match to the results via sorting. This is 
        # why the unique ID is reliable here.
        print("------ adding a join field to the results feature class")
        
        codeblock = 'val = 0 \ndef processID(): \n    global val \n    start = 1 \n    if (val == 0):  \n        val = start\n    else:  \n        val += 1  \n    return val'
        arcpy.AddField_management(in_table = save_path,
                                  field_name = "ProcessID",
                                  field_type = "LONG",
                                  field_is_required = "NON_REQUIRED")
        arcpy.CalculateField_management(in_table = save_path,
                                        field = "ProcessID",
                                        expression = "processID()",
                                        expression_type = "PYTHON3",
                                        code_block = codeblock)
        
        # At this point, we can actually delete the init_id -- we
        # don't need it, and it will be re-joined when we bring in
        # the results (because they are aggregated by init_id)
        print("------ cleaning the results feature class")
        arcpy.DeleteField_management(in_table = save_path,
                                     drop_field = aggregating_feature_id_field)
        
        # Finally, we can join up our results on ProcessID. We'll
        # retain the ProcessID even after joining for future
        # analyses that would require a similar join. Before we
        # join though, we'll want to add a "year" field -- this
        # will be needed for differentiation in functions that bind 
        # feature classes from different years together
        print("------ joining the results to the results feature class")
        comp_et = np.rec.fromrecords(recList = comp_df.values, 
                               names = comp_df.dtypes.index.tolist())
        comp_et = np.array(comp_et)
        arcpy.da.ExtendTable(in_table = save_path,
                             table_match_field = "ProcessID",
                             in_array = comp_et,
                             array_match_field = "ProcessID")                    

    # 3. Done
    # -------
    print("")
    print("Yearly comparisons saved to: " + save_path)
    print("")
    return(save_path)

# %% Main

if __name__ == "__main__":
    
    # Blocks_by_year
    aggregated_dataset_path = "K:/Projects/MiamiDade/PMT/Data/PMT_Trend.gdb/blocks/Blocks_by_year"
    aggregated_dataset_id_field = "ProcessID"
    aggregated_dataset_year_field = "Year"
    aggregating_feature_id_field = "GEOID10"
    save_path = "K:/Projects/MiamiDade/PMT/Data/PMT_Trend.gdb/blocks/Blocks_by_year_change_14_19"                                 
    fields_to_summarize = None
    grouping_fields = None
    years = None
    init_features_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Blocks.gdb/Blocks_2019"
    
    # Blocks_floor_area_by_use_by_year
    aggregated_dataset_path = "K:/Projects/MiamiDade/PMT/Data/PMT_Trend.gdb/Blocks_floor_area_by_use_by_year"
    aggregated_dataset_id_field = "ProcessID"
    aggregated_dataset_year_field = "Year"
    aggregating_feature_id_field = "GEOID10"
    save_path = "K:/Projects/MiamiDade/PMT/Data/PMT_Trend.gdb/blocks/Blocks_floor_area_by_use_by_year_change_14_19"                                 
    fields_to_summarize = None
    grouping_fields = "GN_VA_LU"
    years = None
    init_features_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Blocks.gdb/Blocks_2019"
    
       
