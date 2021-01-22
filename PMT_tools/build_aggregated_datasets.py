# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 11:07:07 2020

@author: Aaron Weinstock
"""

# %% IMPORTS
import PMT
import arcpy
import re
import os
import tempfile
import numpy as np
import pandas as pd
from six import string_types
from collections.abc import Iterable

# %% Function

# Addressing pathing -- relativizing paths in main using PMT constants
# Assume that you're working within a year -- all you need to know is a year.

# Bring the iteration outside of the function, then all you need is one year,
# then the consistency of processing if there across different scripts
def build_aggregated_datasets(init_features_path,
                              init_features_id_field,
                              inputs_path_format,
                              years,
                              fields,
                              shape_type,
                              save_path_format,
                              other_fields=None,
                              other_combos=None):
    '''
    REWRITE
    creates a feature class for storing aggregated data. minimally, it
    will be long on `years`; it will also be long on any additional fields
    provided in `other_fields`
    
    Parameters
    ----------
    init_features_path : Path
        file path to the features to which data will be aggregated
    init_features_id_field: str
        field name for a unique identifier in `init_features_path`
    inputs_path_format: Path
        file path to the input data to be summarized, with a fixed-string wild
        card for year. See notes for implementation
    years : int, or list of int
        year(s) covering the temporal range of data to be aggregated
    fields : dict
        dictionary of format {field_name: null_value, ...} providing field 
        names of fields in the inputs to be aggregated, and the values used to
        replace nulls in these fields
    shape_type: str
        the desired format of results: either "point", "polygon", or "table"
    save_path_format: Path
        the desired save path/name, with a fixed-string wild card for year.
        See notes for implementation
    other_fields: dict or list of dict, optional
        dicionaries specifying any grouping variables and how to read their
        data. format is `{'field': {}, 'reference_path': 
        'ref_data_match': {}, 'shape_type': '', 'save_path': ''}`. See notes 
        for usage
    other_combos : dict or list of dict, optional
        used to specify writing/saving procedure for 1+ grouping variable at a
        time. format is `{'fields' = [], 'shape_type' = '', 'save_path' = ''}.
        See notes for usage
    
    Notes
    -----
    1. Use `other_fields` to specify any variables on which you want to perform
    group summaries. The dictionary inputs then are as follows:
        1.1 'field': (dict) dictionary of format {field_name: null_value} 
        providing the field name for the grouping variable, and the values
        used to replace nulls in this field
        1.2 'reference_path': (Path) if `field_name` is not in 
        `input_path_format`, provide the path to the dataset containing
        `field_name` here. The reference file MUST be joinable to the inputs
        by a single file. If `field_name` is in the inputs, set this value to
        `None`.
        1.3. 'ref_data_match': (dict) if `reference_path` is provided, give
        a dictionary of format {reference_field: {inputs_field: null_value}} 
        that will be used to match the reference to the inputs (and provide
        a null value for reading that inputs field). If `reference_path` is
        `None`, set this value to `None` as well.
        1.4. 'shape_type': (str) "point", "polygon", or "table" -- how should
        results for this group summary be saved?
        1.5. 'save_path': (Path) save location for the group summarized 
        results; the save path should use the same fixed-string wild card for
        year logic as the `save_path_format` parameter (see Note 3)
    If you do not want group summaries, leave this parameter as the default
    `None`.
    
    2. Use `other_combos` to specify multilevel group summaries. The
    dictionary inputs then are as follows:
        2.1. 'fields': (list of str, at least length 2) fields on which to
        perform multilevel group summaries. all list entries MUST be in the
        set of `field_name` values provided in the dictionaries of
        `other_fields`
        2.2. 'shape_type': (str) "point", "polygon", or "table" -- how should
        results for this group summary be saved?
        2.3. 'save_path': (Path) save location for the group summarized 
        results; the save path should use the same fixed-string wild card for
        year logic as the `save_path_format` parameter (see Note 3)
    If you do not want multilevel group summaries, leave this parameter as the
    default `None`
    
    3. For `inputs_path_format`, the assumption is that there is some consistent 
    naming structure that points to the same data over different years, and 
    that this naming structure varies only on year. Similarly, for
    `save_path_format`, the assumption is that the results will be saved to
    some consistently named file path that varies only on year. Thus, wherever
    a year appears/should appear in these file paths, replace the year with 
    `{Year}` in the provided string. 
        3.1 For example, if data for 2010 is stored at `Data_2010.gdb/Inputs` 
        and you want to save it to `Data_2010.gdb/Results (and similarly for 
        other years), then `inputs_path_format = 'Data_{Year}.gdb/Inputs'` and 
        `save_path_format = 'Data_{Year}.gdb/Results'`.
    
    Returns
    -------
    REWRITE
    A feature class that is long on `years` and, if provided, unique values
    from the fields specified in `other_fields` will be created by this
    function. The path of this feature class will be returned.
    '''
    
    # Validation -------------------------------------------------------------
    
    # 1. 'other_fields' parameter
    # ---------------------------
    if other_fields is not None:
        if type(other_fields) is not list:
            other_fields = [other_fields]
        r = np.arange(len(other_fields))
        missing_df = []
        
        # Are there any missing keys?
        for i in r: 
            d = other_fields[i]
            
            # Missing issues
            needed_keys = ["field", "reference_path", "ref_data_match", 
                           "shape_type", "save_path"]
            present_keys = list(d.keys())
            missing = [p in needed_keys for p in present_keys]
            
            # Format
            issues = pd.DataFrame({"Entry": np.repeat(i, 5),
                                   "Key": needed_keys,
                                   "Missing": missing})
            missing_df.append(issues)
        
        # If there are any missing, break the function and return
        missing_df = pd.concat(missing_df)
        missing_df = missing_df[missing_df.Missing == False]
        
        if len(missing_df.index) > 0:
            print(missing_df)
            raise ValueError("At least one necessary key is missing from a 'fields' entry; see above for reference")
        else:       
            # If nothing is missing, we can deal with other issues
            issues_df = []
            
            for i in r:
                d = other_fields[i]
                
                # field
                field_issues = []
                if type(d["field"]) is not dict:
                    field_issues = field_issues.append("not a dict")
                else:
                    if type(list(d["field"].keys())[0]) is not str:
                        field_issues = field_issues.append("key is not a str")
                field_issues = '; '.join(field_issues)
                
                # reference_path
                reference_issues = []
                if type(d["reference_path"]) is not str:
                    reference_issues.append("not a str")
                reference_issues = '; '.join(reference_issues)
                
                # ref_data_match
                match_issues = []
                if type(d["ref_data_match"]) is not dict:
                    match_issues.append("not a dict")
                else:
                    if type(list(d["ref_data_match"].keys())[0]) is not str:
                        match_issues.append("key is not a str")
                    if type(list(d["ref_data_match"].values())[0]) is not dict:
                        match_issues.append("value is not a dict")
                    else:
                        if type(list(list(d["ref_data_match"].values())[0].keys())[0]) is not str:
                            match_issues.append("value key is not a str")
                match_issues = '; '.join(match_issues)
                
                # shape_type
                shape_issues = []
                if type(d["shape_type"]) is not str:
                    shape_issues.append("not a str")
                else:
                    if d["shape_type"] not in ["point","polygon","table"]:
                        shape_issues.append("not 'point', 'polygon' or 'table'")
                shape_issues = '; '.join(shape_issues)
                    
                # save_path
                save_issues = []
                if type(d["save_path"]) is not str:
                    save_issues.append("not a str")
                else:
                    if not bool(re.search("{Year}", d["save_path"])):
                        save_issues.append("no fixed-string wild card for year")
                save_issues = '; '.join(save_issues)
                        
                # Format results
                issues = pd.DataFrame({"Entry": np.repeat(i, 5),
                                       "Key": needed_keys,
                                       "Issues": [field_issues,
                                                  reference_issues,
                                                  match_issues,
                                                  shape_issues,
                                                  save_issues]})
                issues_df.append(issues)
            
            # If there are any issues, break the function and return
            issues_df = pd.concat(issues_df)
            issues_df = issues_df[issues_df.Issues != ""]
            
            if len(missing_df.index) > 1:
                print(issues_df)
                raise ValueError("At least one value in one 'fields' entry is in error; see above for reference")
        
    # 2. 'other_combos' parameter
    # ---------------------------
    
    # Treat this like the above
    
    # Parcels -- TOT_LVG_AREA, NO_RES_UNTS, LND_SQFOOT 
    # Allocation -- Jobs, Pop, Commutes, Jobs by Type, Population by Race/Eth
    
        
    # Loop setup -------------------------------------------------------------
    
    # Most of our procesing is going to be done in a loop over the years, but
    # there are a few things we don't want to have to do twice. So, we'll do
    # those outside the loop. They are enumerated below.
    
    # 1. Set up a gdb for intermediates created in the loop
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path = temp_dir,
                                   out_name = "Intermediates.gdb")
    intmd_gdb = os.path.join(temp_dir, "Intermediates.gdb")
    
    # 2. Verify that we have list comprehension of things that could be
    # given as a single argument or a list (years, other_fields, 
    # other_combos)
    # ABC collections, import iterable type -- then check directly if it's
    # an iterable
    if type(years) is not list:
        years = [years]
    if other_fields is not None:
        if type(other_fields) is not list:
            other_fields = [other_fields]
    if other_combos is not None:
        if type(other_combos) is not list:
            other_combos = [other_combos]
    
    # 3. Set up the fields to read from the inputs by combining the fields
    # with entries in other_fields
    fields_read_null = fields
    if other_fields is not None:
        for entry in other_fields:
            if entry["ref_data_match"] is not None:
                read_add = list(entry["ref_data_match"].values())[0]
                fields_read_null = {**fields_read_null, **read_add}
            else:
                fields_read_null = {**fields_read_null, **entry["field"]}
    fields_read_names = list(fields_read_null.keys())   
                
    # 4. Set up the fields to aggregate by extracting the keys from the fields
    # dictionary as a list
    fields_sum = list(fields.keys())
                
    # Loop processing --------------------------------------------------------
    
    # Now we can hop into the loop processing
    for yr in years:
        syr = str(yr)
        print("")
        print("Processing " + syr)
        
        # 1. Data prep
        # ------------
        print("1. Data preparation")        
        
        # First, we prep our data. This is 4 step process involving reading
        # (the inputs), centroid-izing (the inputs), intersecting (the inputs
        # with the init_features), and loading (the intersected data)
        
        # Step 1 is reading the inputs data
        print("1.1 reading the inputs data")
        
        shape_fields = ["SHAPE@X", "SHAPE@Y"]
        inputs_path = inputs_path_format.replace("{Year}", syr)
        inputs_array = arcpy.da.FeatureClassToNumPyArray(in_table = inputs_path,
                                                         field_names = fields_read_names + shape_fields,
                                                         null_value = fields_read_null)
    
        # Step 2 is converting the data to centroids (for intersection)
        print("1.2 converting the inputs to centroids")
        
        centroids_path = os.path.join(intmd_gdb,
                                      ''.join(["Centroids_", syr]))
        sr = arcpy.Describe(inputs_path).spatialReference
        arcpy.da.NumPyArrayToFeatureClass(in_array = inputs_array,
                                          out_table = centroids_path,
                                          shape_fields = shape_fields,
                                          spatial_reference = sr)
        
        # Step 3 is intersecting the centroid-ized data with the init_features
        print("1.3 intersection centroid-ized inputs with init features")
        
        intersection_path = os.path.join(intmd_gdb,
                                        ''.join(["Intersection_", syr]))
        arcpy.Intersect_analysis(in_features = [centroids_path, 
                                                init_features_path],
                                 out_feature_class = intersection_path)
        
        # Step 4 is loading the intersected data
        print("1.4 loading the intersected data")
        df = arcpy.da.FeatureClassToNumPyArray(in_table = intersection_path,
                                               field_names = [init_features_id_field] + fields_read_names,
                                               spatial_reference = sr,
                                               null_value = fields_read_null)
        df = pd.DataFrame(df)
        
        # 2. Data processing
        # ------------------
        print("")
        print("2. Data processing")
        
        # Now we're ready to begin our processing. We'll store all our results
        # in a list of dictionaries containing necessary save information, so 
        # we'll initialize an empty list first.
        results = []
        
        # First, we'll complete our "basic" aggregation -- group by 
        # init_features_id_field, sum fields_sum
        # Could use a class to store some of these variables?
        print("2.1 performing 'basic' aggregation (to init features only)")
        
        sdf = df.groupby(init_features_id_field)[fields_sum].apply(np.sum, axis=0).reset_index()
        save_dict = {"reps": 1,
                     "field": [None],
                     "uniques": [None],
                     "type": shape_type,
                     "path": save_path_format.replace("{Year}", syr),
                     "data": sdf}
        results.append(save_dict)
        
        # Next, we loop through our other_fields to do single-level
        # summarization -- group by init_features_id_field and entry["field"],
        # sum fields_sum
        # Note that we're constantly appending to the existing dataframe so
        # that we can build up the set of all grouping variables in the data.
        # This will be helpful for multilevel summaries

        if other_fields is not None:
            print("2.2 performing single-level summarization")
            ref_tables = {}
            
            for entry in other_fields:
                gfield = list(entry["field"].keys())[0]
                print("-- " + gfield)
                
                # If the grouping field is not already in the data, we'll
                # need to load it from a reference file. Otherwise, no work
                # needs to be done
                if gfield not in df.columns.tolist():
                    print("---- merging from reference")
                    
                    # Obtain the fields used to join the reference to the data
                    ref_match = list(entry["ref_data_match"].keys())[0]
                    data_match = list(list(entry["ref_data_match"].values())[0].keys())[0]
                    
                    # If the reference field has already been read, it will
                    # be stored in ref_tables (prevents us from reading files
                    # multiple times), and we can load it from that dictionary.
                    # If not, we read it, and put it in that dictionary for
                    # potential future use
                    if entry["reference_path"] not in list(ref_tables.keys()):
                        # Read from file
                        ref = pd.read_csv(entry["reference_path"]) # other ways to read
                        
                        # Add to dict
                        ref_tables[entry["reference_path"]] = ref
                    else:
                        # Load from dict
                        ref = ref_tables[entry["reference_path"]]
                    
                    # Select out the fields from ref that are useful to us --
                    # the grouping field, and the field that will be used to 
                    # match to the data
                    ref = ref[[gfield, ref_match]]
                    ref.loc[ref[gfield].isna(), gfield] = list(entry["field"].values())[0]
                    # Try using .loc[row_indexer,col_indexer] = value instead
                    
                    # Before we merge, we want the unique values of the
                    # grouping field. This will be necessary for [potential]
                    # feature class initialization later on. We want to pull
                    # this from ref, not the merged class, in case some of the
                    # unique values aren't present in the data (i.e. we want
                    # to reflect all options, even if not present)
                    uniques = np.unique(ref[gfield]).tolist()
                    
                    # Now we merge ref to the data
                    df = pd.merge(df, 
                                  ref,
                                  left_on = data_match,
                                  right_on = ref_match,
                                  how = "left")
                else:
                    # If the grouping field is already in the data, there's no
                    # work to be done other than extracting unique values
                    uniques = np.unique(df[gfield]).tolist()
                
                # Now that the grouping field is merged/verified to be in the
                # data, we group by the init_id and the grouping field, and
                # sum the sum_fields
                print("---- summarizing")
                
                group_fields = [init_features_id_field, gfield]
                sdf = df.groupby(group_fields)[fields_sum].apply(np.sum, axis=0).reset_index()
                save_dict = {"reps": len(uniques),
                             "field": [gfield],
                             "uniques": [uniques],
                             "type": entry["shape_type"],
                             "path": entry["save_path"].replace("{Year}", syr),
                             "data": sdf}
                results.append(save_dict)
        
        # Finally, we complete the multilevel summarizations. The data frame
        # is already prepped for summarization, and we can pull the reps,
        # field, and uniques information from the existing results
        if other_combos is not None:
            print("2.3 performing multi-level summarization")
            
            # Multilevel summaries will reference reps, fields, and uniques
            # from single level summaries, so we'll go ahead and pull those
            # out to their own lists
            rep_counts = [item["reps"] for item in results]
            components = [item["field"] for item in results]
            unique_values = [item["uniques"] for item in results]
            
            # Now, we can loop through the field combinations to summarize
            for entry in other_combos:
                print("-- " + ', '.join(entry["fields"]))
                
                # We can use indexing in the lists above to pull the info
                # we need for each component field in the combo fields
                print("---- concatenating field info")
                
                idx = [components.index(f) for f in entry["fields"]]
                reps = np.product([rep_counts[i] for i in idx])
                uniques = [unique_values[i] for i in idx]
                
                # Summarization operates the same as before
                print("---- summarizing")
                
                group_fields = [init_features_id_field] + entry["fields"]
                sdf = df.groupby(group_fields)[fields_sum].apply(np.sum, axis=0).reset_index()
                save_dict = {"reps": reps,
                             "field": entry["fields"],
                             "uniques": uniques,
                             "type": entry["shape_type"],
                             "path": entry["save_path"].replace("{Year}", syr),
                             "data": sdf}
                results.append(save_dict)
        
        # 3. Writing results
        # ------------------
        print("3. Writing results")
        
        # Now we're ready to save our results. We do so entirely from the
        # save_dict -- we have all the data necessary to initialize a feature
        # class with the necessary repetitions and groups of init_features,
        # and join the summarized data to this feature class
        n = int(arcpy.GetCount_management(init_features_path).getOutput(0))
        init_ids = arcpy.da.FeatureClassToNumPyArray(in_table = init_features_path,
                                                     field_names = init_features_id_field)
        for sd in results:
            if sd["field"][0] is None:
                print("-- basic")
            else:
                print("-- " + ', '.join(sd["field"]))
            
            # We begin by creating a long table of repetitions, because this
            # is the only thing we do the same for table and non-table save
            # types. We want all possible combinations of init_ids and
            # grouping variables
            # Note that if reps = 1, this indicates that there are no grouping
            # variables, so the long table is simply the init_id's
            print("---- creating long table of init feature repetitions")
            
            if sd["reps"] > 1:
                ncol = len(sd["field"])
                mg = pd.DataFrame(np.array(np.meshgrid(*sd["uniques"])).T.reshape(-1, ncol))
                mg.columns = sd["field"]
                mg = pd.concat([mg]*n).reset_index(drop=True)
                mg[init_features_id_field] = np.repeat(init_ids.tolist(), sd["reps"])
            else:
                mg = pd.DataFrame(init_ids)
            
            # Now, we merge our results frame in, and fill any missing data
            # with 0s. This builds a dataframe of "complete cases", where
            # all combinations of init_id and any grouping fields are
            # represented
            # To set up future joins, we'll sort our meshgrid result by the
            # init_id and add a unique ID field. We can rely on sorting because
            # we're always working in complete cases -- so there's a fixed
            # order and index range for each init_id upon sorting
            print("---- filling missing cases in the results with 0")
            
            grouping_fields = mg.columns.tolist()
            mg = pd.merge(mg,
                          sd["data"],
                          left_on = grouping_fields,
                          right_on = grouping_fields,
                          how = "left")
            mg = mg.fillna(0)
            if sd["field"][0] is None:
                sort_by = init_features_id_field
            else:
                sort_by = [init_features_id_field] + sd["field"]
            mg = mg.sort_values(sort_by).reset_index(drop=True)
            mg["ProcessID"] = np.arange(1, len(mg.index)+1)
            
            # Now we need to account for the fact that the file we're saving
            # to already exists! If it does, we want to add all summarized
            # fields that aren't already in the save path. If it doesn't, we
            # have more work to do (enumerated in line)
            
            if arcpy.Exists(sd["path"]):
                print("---- file already exists")
                
                # Identifying any fields that exist in the data already -- 
                # we assume that if they exist but they're being calculated
                # again, this means we want to update them. So, to make the
                # extend table work, we need to delete those fields from the
                # existing dataset
                existing_fields = [f.name for f in arcpy.ListFields(sd["path"])]
                current_fields = mg.columns.tolist()
                current_fields = [c for c in current_fields if c != "ProcessID"]
                del_fields = [c for c in current_fields if c in existing_fields]
                if len(del_fields) > 0:
                    print("------ deleting existing fields that are being updated")
                    del_df = [d for d in del_fields if d in grouping_fields]
                    del_path = [d for d in del_fields if d not in grouping_fields]
                    if len(del_df) > 0:
                        mg = mg.drop(columns = del_df)
                    if len(del_path) > 0:
                        arcpy.DeleteField_management(in_table = sd["path"],
                                                     drop_field = del_fields)
                
                # Now we are ready to execute the merge
                print("------ merging in new fields")
                
                mg_et = np.rec.fromrecords(recList = mg.values, 
                                           names = mg.dtypes.index.tolist())
                mg_et = np.array(mg_et)
                arcpy.da.ExtendTable(in_table = sd["path"],
                                     table_match_field = "ProcessID",
                                     in_array = mg_et,
                                     array_match_field = "ProcessID")
            else:
                # So this means our file doesn't exist -- SAD. So we need
                # to create it (if it's a point or polygon) and merge our
                # data in, or simply write the table (if it's a table)
                print("---- file does not exist; must create")
                
                if sd["type"] == "table":
                    # Data is a table -- all we have to do is write
                    print("------ writing table")
                    mg["Year"] = yr
                    mg_et = np.rec.fromrecords(recList = mg.values, 
                                           names = mg.dtypes.index.tolist())
                    mg_et = np.array(mg_et)
                    arcpy.da.NumPyArrayToTable(in_array = mg_et,
                                               out_table = sd["path"])
                else:
                    # There's a lot more that needs to be done if we're
                    # dealing with a shape. First, we need to initialize a
                    # feature class for the shape. We give it a unique name
                    # to put in the intermediates gdb, then write it there
                    print("------ initializing a results feature class from init features")
                    
                    # Unique name
                    if sd["field"][0] is None:
                        copy_name = '_'.join(["copy", syr])
                    else:
                        copy_name = '_'.join(sd["field"] + [syr, "copy"])
                    
                    # File conversion
                    if sd["type"] == "polygon":
                        print("-------- feature class is POLYGON")
                        fmap = arcpy.FieldMappings()
                        fmap.addTable(init_features_path)
                        fn = {f.name: f for f in arcpy.ListFields(init_features_path)}
                        for fname, fld in fn.items():
                            if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
                                if fname != init_features_id_field:
                                    fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
                        arcpy.conversion.FeatureClassToFeatureClass(in_features = init_features_path, 
                                                                    out_path = intmd_gdb,
                                                                    out_name = copy_name,
                                                                    field_mapping = fmap)
                    else:
                        print("-------- feature class is POINT")
                        init_fields = [init_features_id_field, "SHAPE@X", "SHAPE@Y"]
                        init_sr = arcpy.Describe(init_features_path).spatialReference
                        init_array = arcpy.da.FeatureClassToNumPyArray(in_table = init_features_path,
                                                                       field_names = init_fields,
                                                                       spatial_reference = init_sr)
                        arcpy.da.NumPyArrayToFeatureClass(in_array = init_array,
                                                          out_table = os.path.join(intmd_gdb, copy_name),
                                                          shape_fields = ["SHAPE@X", "SHAPE@Y"],
                                                          spatial_reference = init_sr)
                    copy_path = os.path.join(intmd_gdb,
                                             copy_name)
                    
                    # Next, we have to deal with repetitions. If there's only 
                    # 1, that  means no repetitions are necessary -- the copy 
                    # class is the rep class. Otherwise, we need to complete 
                    # the repetitions to build our "complete cases" set. We
                    # give it a unique name to put in the intermediates gdb, 
                    # then write it there
                    
                    # Unique name
                    rep_name = copy_name.replace("copy","rep")
                    rep_path = copy_path.replace("copy","rep") 
                    
                    if sd["reps"] == 1:
                        arcpy.Rename_management(in_data = copy_path, 
                                                out_data = rep_path)
                    else:
                        print("------ repeating features in the initialized results feature class")
                        sr = arcpy.Describe(copy_path).spatialReference
                        arcpy.CreateFeatureclass_management(out_path = intmd_gdb,
                                                            out_name = rep_name, 
                                                            template = copy_path,
                                                            has_m = "SAME_AS_TEMPLATE", 
                                                            has_z = "SAME_AS_TEMPLATE", 
                                                            spatial_reference = sr)
                        rep_fields = [init_features_id_field, "SHAPE@"]
                        with arcpy.da.SearchCursor(copy_path, rep_fields) as curs_in:
                            with arcpy.da.InsertCursor(rep_path, rep_fields) as curs_out:
                                for row in curs_in:
                                    for i in range(sd["reps"]):
                                        curs_out.insertRow(row)
                    
                    # Now we have to sort according to the init_id -- this
                    # will create our save feature class
                    print("------ sorting the results feature class on init feature ID")
                    arcpy.Sort_management(in_dataset = rep_path,
                                          out_dataset = sd["path"],
                                          sort_field = init_features_id_field)
                    
                    # Then, we add our unique ID. Again, what we've created
                    # is the "complete cases" of init_id's, so we've achieved
                    # an exact match to the results via sorting. This is 
                    # why the unique ID is reliable here.
                    print("------ adding a join field to the results feature class")
                    
                    codeblock = 'val = 0 \ndef processID(): \n    global val \n    start = 1 \n    if (val == 0):  \n        val = start\n    else:  \n        val += 1  \n    return val'
                    arcpy.AddField_management(in_table = sd["path"],
                                              field_name = "ProcessID",
                                              field_type = "LONG",
                                              field_is_required = "NON_REQUIRED")
                    arcpy.CalculateField_management(in_table = sd["path"],
                                                    field = "ProcessID",
                                                    expression = "processID()",
                                                    expression_type = "PYTHON3",
                                                    code_block = codeblock)
                    
                    # At this point, we can actually delete the init_id -- we
                    # don't need it, and it will be re-joined when we bring in
                    # the results (because they are aggregated by init_id)
                    # print("------ cleaning the results feature class")
                    arcpy.DeleteField_management(in_table = sd["path"],
                                                  drop_field = init_features_id_field)
                    
                    # Finally, we can join up our results on ProcessID. We'll
                    # retain the ProcessID even after joining for future
                    # analyses that would require a similar join. Before we
                    # join though, we'll want to add a "year" field -- this
                    # will be needed for differentiation in functions that bind 
                    # feature classes from different years together
                    print("------ joining the results to the results feature class")
                    mg["Year"] = yr
                    mg_et = np.rec.fromrecords(recList = mg.values, 
                                           names = mg.dtypes.index.tolist())
                    mg_et = np.array(mg_et)
                    arcpy.da.ExtendTable(in_table = sd["path"],
                                         table_match_field = "ProcessID",
                                         in_array = mg_et,
                                         array_match_field = "ProcessID")                    
    
    # Done -------------------------------------------------------------------
    print("")
    print("Processing completed -- see printouts above for details")
    print("")
    return(None)
            
# %% Main

if __name__ == "__main__":
    
    # Parcels data: living area, parcel area, res units, market value, taxable value
    init_features_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Blocks.gdb/Blocks_2019"
    init_features_id_field = "GEOID10"
    years = [2014, 2015, 2016, 2017, 2018, 2019]
    inputs_path_format = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Parcels.gdb/Miami_{Year}"
    fields = {"NO_RES_UNTS": 0, "TOT_LVG_AREA": 0, "LND_SQFOOT": 0, "JV": 0, "TV_NSD": 0}
    shape_type = "polygon"
    save_path_format = "K:/Projects/MiamiDade/PMT/Data/PMT_{Year}.gdb/Each_block"
    other_fields = {"field": {"GN_VA_LU": "Other"},
                    "reference_path": "K:/Projects/MiamiDade/PMT/Data/Reference/Land_Use_Recode.csv",
                    "ref_data_match": {"DOR_UC": {"DOR_UC": 999}},
                    "shape_type": "table",
                    "save_path": "K:/Projects/MiamiDade/PMT/Data/PMT_{Year}.gdb/Blocks_floor_area_by_use"}
    other_combos = None
    
    # Run
    build_aggregated_datasets(init_features_path = init_features_path,
                              init_features_id_field = init_features_id_field,
                              inputs_path_format = inputs_path_format,
                              years = years,
                              fields = fields,
                              shape_type = shape_type,
                              save_path_format = save_path_format,
                              other_fields = other_fields,
                              other_combos = other_combos)
    
    # Allocation data: jobs (and by cluster), pop (and by eth), commutes (and by type)
    init_features_path = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Blocks.gdb/Blocks_2019"
    init_features_id_field = "GEOID10"
    years = [2014, 2015, 2016, 2017, 2018, 2019]
    inputs_path_format = "K:/Projects/MiamiDade/PMT/Data/PMT_{Year}.gdb/Parcels/socioeconomic_and_demographic"
    shape_type = "polygon"
    save_path_format = "K:/Projects/MiamiDade/PMT/Data/PMT_{Year}.gdb/Each_block"
    other_fields = {"field": {"GN_VA_LU": "Other"},
                    "reference_path": "K:/Projects/MiamiDade/PMT/Data/Reference/Land_Use_Recode.csv",
                    "ref_data_match": {"DOR_UC": {"DOR_UC": 999}},
                    "shape_type": "table",
                    "save_path": "K:/Projects/MiamiDade/PMT/Data/PMT_{Year}.gdb/Blocks_floor_area_by_use"}
    other_combos = None
    # Grab fields for this from the shape because there's lots
    fn = [f.name for f in arcpy.ListFields("K:/Projects/MiamiDade/PMT/Data/PMT_2014.gdb/Parcels/socioeconomic_and_demographic")]
    fn = [f for f in fn if f not in 
          ["OBJECTID","Shape","PARCELNO","Shape_Length","Shape_Area","GEOID10","DOR_UC","TOT_LVG_AREA"]]
    fields = {}
    for f in fn:
        fields[f] = 0
    del fn
    
    # Run
    build_aggregated_datasets(init_features_path = init_features_path,
                              init_features_id_field = init_features_id_field,
                              inputs_path_format = inputs_path_format,
                              years = years,
                              fields = fields,
                              shape_type = shape_type,
                              save_path_format = save_path_format,
                              other_fields = other_fields,
                              other_combos = other_combos)
    
    
    