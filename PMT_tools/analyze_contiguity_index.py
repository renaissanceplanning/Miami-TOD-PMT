# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 12:00:24 2020

@author: AZ7
"""

# %% Imports

import arcpy
import tempfile
import numpy as np
import pandas as pd
import os
import time

# %% GLOBALS
CODEBLOCK = """
    val=0
    def processID():
        global val
        start=1
        if (val==0):
            val=start
        else:
            val+=1
        return val"""


# %% Functions

def analyze_contiguity_index(parcels_path,
                             buildings_path,
                             save_gdb_location,
                             parcels_id_field,
                             chunks=20,
                             cell_size=40,
                             weights="nn"):
    """
    calculte contiguity of developable area
    
    Parameters
    ----------
    parcels_path: path
        path to parcel polygons; contiguity will be calculated relative to this
    buildings_path: path
        path to building polygons; parcels and buildings will be spatially 
        differenced to define developable area
    save_gdb_location: directory
        .gdb (or feature class with a .gdb) in which summarized parcel results
        will ultimately be saved. a table of sub-parcel results (which this
        function calculates) will be saved to the .gdb specified in this 
        directory
    parcels_id_field: str
        name of a field used to identify the parcels in the future summarized
        parcel results
    chunks: int
        number of chunks in which you want to process contiguity. chunking
        is necessary because of memory issues with rasterization of large
        feature classes, though a user could set `chunks=1` for no chunking.
        Default 20 (works for PMT)
    cell_size: int
        cell size for raster over which contiguity will be calculated. siz
        should be in the units of the parcel/buildings crs
        Default 40 (works for PMT)
    weights: str or dict
        weights for neighbors in contiguity calculation. see notes for how
        to specify weights
        Default "nn", all neighbors carry the same weight, regardless of
        orientation
        
    Notes
    -----
    Weights can be provided in one of two ways:
        
        1. one of three defaults: "rook", "queen", or "nn". 
        "rook" weights give all horizontal/vertical neighbors a weight of 1, 
        and all diagonal neighbors a weight of 0
        "queen" weights give all horizonal/vertical neighbors a weight of 2,
        and all diagonal neighbors a weight of 1
        "nn" (nearest neighbor) weights give all neighbors a weight of 1,
        regardles of orientation
        For developable area, "nn" makes the most sense to describe contiguity, 
        and thus is the recommended option for weights in this function
        
        2. a dictionary of weights for each of 9 possible neighbors. This
        dictionary must have the keys ["top_left", "top_center", "top_right",
        "middle_left", "self", "middle_right", "bottom_left", "bottom_center",
        "bottom_right"]. If providing weights as a dictionary, a good strategy
        is to set "self"=1, and then set other weights according to a
        perceived relative importance to the cell itself. It is recommended,
        however, to use one of the default weighting options; the dictionary
        option should only be used in rare cases.
    
    Returns
    -------
    1. writes polygon-level (sub-parcel) results to a table in the .gdb
    specified or implied by `save_gdb_location` -- this table includes a
    parcel identifier (ProcessID), polygon identifier (PolyID), polygon
    contiguity, and polygon developable area
    2. copies parcels specified by `parcels_path` to `save_gdb_location` with
    the parcel identifier (ProcessID) and user-provided `parcels_id_field`.
    parcel-summarized contiguity and developable area results can then be
    saved to this feature class    
    
    Raises
    ------
    ValueError, if weights are an invalid string or a dictionary with invalid
    keys (see Notes)
    """

    # Weights setup ----------------------------------------------------------

    print("")
    print("Checking weights")

    # Before anything else, we need to make sure the weights are set up
    # properly; if not, we need to kill the function. We'll do that through
    # a series of logicals

    if type(weights) == str:
        weights = weights.lower()
        if weights == "rook":
            weights = dict({"top_left": 0,
                            "top_center": 1,
                            "top_right": 0,
                            "middle_left": 1,
                            "self": 1,
                            "middle_right": 1,
                            "bottom_left": 0,
                            "bottom_center": 1,
                            "bottom_right": 0})
        elif weights == "queen":
            weights = dict({"top_left": 1,
                            "top_center": 2,
                            "top_right": 1,
                            "middle_left": 2,
                            "self": 1,
                            "middle_right": 2,
                            "bottom_left": 1,
                            "bottom_center": 2,
                            "bottom_right": 1})
        elif weights == "nn":
            weights = dict({"top_left": 1,
                            "top_center": 1,
                            "top_right": 1,
                            "middle_left": 1,
                            "self": 1,
                            "middle_right": 1,
                            "bottom_left": 1,
                            "bottom_center": 1,
                            "bottom_right": 1})
        else:
            raise ValueError(''.join(["Invalid string specification for 'weights'; ",
                                      "'weights' can only take 'rook', 'queen', or 'nn' as a string\n"]))
    elif type(weights) == dict:
        k = weights.keys()
        missing = list(set(["top_left", "top_center", "top_right",
                            "middle_left", "self", "middle_right",
                            "bottom_left", "bottom_center", "bottom_right"]) - set(k))
        if len(missing) != 0:
            raise ValueError(''.join(["Necessary keys missing from 'weights'; ",
                                      "missing keys include: ",
                                      ', '.join([str(m) for m in missing]),
                                      "\n"]))
    else:
        raise ValueError(''.join(["'weights' must be a string or dictionary; ",
                                  "if string, it must be 'rook', 'queen', or 'nn', and "
                                  "if dictionary, it must have keys 'top_left','top_center','top_right','middle_left','self','middle_right','bottom_left','bottom_center','bottom_right'\n"]))

    # After this, we can be confident that our weights are properly formatted
    # for how we plan to use them in contiguity
    print("Weights are good -- let's go!")

    # Chunking setup ---------------------------------------------------------

    print("")
    print("1. Set up for chunk processing of contiguity")
    
    # Before anything, recognizing this is going to create a LOT of data,
    # we need to set up a location for intermediate files.
    print("1.1 setting up an intermediates gdb")
    
    temp_dir = tempfile.mkdtemp()
    arcpy.CreateFileGDB_management(out_folder_path = temp_dir,
                                   out_name = "Intermediates.gdb")
    intmd_gdb = os.path.join(temp_dir, "Intermediates.gdb")
    
    

    # First, we're going to create our quadrats for chunking. To do this,
    # we need to start with the extent of our parcels
    print("1.2 extracting parcels extent")
    desc = arcpy.Describe(parcels_path)
    parcels_extent = desc.extent

    # Next, we find the ratio of dimensions for our parcels. This will inform
    # how our quadrats get structured -- we'll pick the orientation that most
    # closely matches our height/width ratio
    print("1.3 determining parcels dimension ratio")
    xmin = parcels_extent.XMin
    xmax = parcels_extent.XMax
    ymin = parcels_extent.YMin
    ymax = parcels_extent.YMax
    hw_ratio = (ymax - ymin) / (xmax - xmin)

    # Now, we define out the orientation of our quadrats by identifying the
    # one that is closest to 'hw_ratio'. This gives us the number of rows
    # and columns for our quadrats
    print("1.4 defining row/column orientation for quadrats")
    candidate_ontns = [[i, chunks//i] 
                        for i in range(1, chunks+1) 
                        if chunks % i == 0]

    ontn_matching = [abs(o[0] / o[1] - hw_ratio) for o in candidate_ontns]
    orientation = candidate_ontns[np.argmin(ontn_matching)]
    quadrat_nrows = orientation[0]
    quadrat_ncols = orientation[1]

    # With the extent information and rows/columns, we can create our quadrats
    # by creating a fishnet over the parcels
    print("1.5 creating quadrats")
    quadrat_origin = ' '.join([str(xmin), str(ymin)])
    quadrat_ycoord = ' '.join([str(xmin), str(ymin + 10)])
    quadrat_corner = ' '.join([str(xmax), str(ymax)])
    quadrats_fc = os.path.join(intmd_gdb,
                               "quadrats")
    arcpy.CreateFishnet_management(
        out_feature_class = quadrats_fc,
        origin_coord = quadrat_origin,
        y_axis_coord = quadrat_ycoord,
        number_rows = quadrat_nrows,
        number_columns = quadrat_ncols,
        corner_coord = quadrat_corner,
        template = parcels_extent,
        geometry_type = "POLYGON"
    )

    # The next step is identifying the quadrat in which each parcel falls.
    # This will give us a "chunk ID", which we can use to process the parcels
    # in chunks. We'll identify quadrat ownership using parcel centroids
    # because a point-polygon intersection will be a lot less expensive, but
    # ultimately we want to merge back to parcel polygons. To do this, we'll
    # need to set up a unique ID field in the polygons that we can carry over
    # to the centroids, then use to merge chunk IDs back to the polygons.
       
    # We start by extracting parcel centroids, maintaining the ID field for a
    # future merge back to the polygons
    print("1.6 extracting parcel centroids")
    parcels_fields = [parcels_id_field, "SHAPE@X", "SHAPE@Y"]

    parcels_sr = desc.spatialReference
    parcels_array = arcpy.da.FeatureClassToNumPyArray(
        in_table=parcels_path,
        field_names=parcels_fields,
        spatial_reference=parcels_sr,
        null_value=-1
    )
    centroids_fc = os.path.join(intmd_gdb,
                                "centroids")
    arcpy.da.NumPyArrayToFeatureClass(
        in_array = parcels_array,
        out_table = centroids_fc,
        shape_fields = ["SHAPE@X", "SHAPE@Y"],
        spatial_reference = parcels_sr
    )

    # Next, we intersect the parcels centroids with the quadrats to
    # identify quadrat ownership -- the parcels will be enriched with the
    # quadrat FIDs, which can be used for chunk identification. We're now
    # done with the quadrats and centroids, so we can delete them
    print("1.7 identifying parcel membership in quadrats")
    intersect_fc = os.path.join(intmd_gdb,
                                "intersect")
    arcpy.Intersect_analysis(
        in_features = [centroids_fc,
                       quadrats_fc],
        out_feature_class = intersect_fc
    )
    arcpy.Delete_management(quadrats_fc)
    arcpy.Delete_management(centroids_fc)
    
    # Now that we've identified membership, we pull the parcels ID and the 
    # chunk ID (stored as "FID_quadrats" by default from the create fishnet 
    # function), and merge back to the parcel polygons to give us the
    # necessary chunk attribution. We'll rename "FID_quadrats" to
    # "ChunkID" for legibility. Also, we're now done with the intersect,
    # so we can delete it
    
    # First, we'll need a copy of the original parcels (this way we don't
    # have to modify the parcels as they are and potentially mess stuff up)
    print("1.8 copying the parcels feature class to avoid overwriting")
    fmap = arcpy.FieldMappings()
    fmap.addTable(parcels_path)
    fields = {f.name: f for f in arcpy.ListFields(parcels_path)}
    for fname, fld in fields.items():
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            if fname != parcels_id_field:
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    arcpy.conversion.FeatureClassToFeatureClass(in_features = parcels_path, 
                                                out_path = intmd_gdb,
                                                out_name = "parcels",
                                                field_mapping = fmap)
    parcels_fc = os.path.join(intmd_gdb,
                              "parcels")
    
    # Now we add the chunk ID to the copied parcels (see the above comment
    # chunk for additional information)
    print("1.9 tagging parcels with a chunk ID")
    itsn_fields = [parcels_id_field, "FID_quadrats"]
    itsn_array = arcpy.da.FeatureClassToNumPyArray(
        in_table = intersect_fc,
        field_names = itsn_fields,
        spatial_reference = parcels_sr,
        null_value = -1
    )
    itsn_array.dtype.names = (parcels_id_field,"ChunkID")
    arcpy.da.ExtendTable(
        in_table = parcels_fc,
        table_match_field = parcels_id_field,
        in_array = itsn_array,
        array_match_field = parcels_id_field
    )
    arcpy.Delete_management("in_memory\\intersect")

    # This completes our chunking setup -- next, we need to take our
    # chunked parcels and difference them with buildings to define
    # developable area    

    # Differencing buildings and parcels -------------------------------------

    print("")
    print("2. Differencing parcels and buildings")
    
    # Contiguity is assessed in terms of parcel area that is not already
    # developed. To do this, we'll need a spatial difference of parcel
    # polygons and building polygons. First, we process the difference
    print("2.1 differencing parcels and buildings")
    union_fc = os.path.join(intmd_gdb,
                            "union")
    arcpy.Union_analysis(
        in_features = [parcels_fc,
                       buildings_path],
        out_feature_class = union_fc
    )
    difference = arcpy.SelectLayerByAttribute_management(
        in_layer_or_view = union_fc,
        selection_type = "NEW_SELECTION",
        where_clause = "\"type\" <> 'way'" #pick any variable from buildings
    )
    difference_fc = os.path.join(intmd_gdb,
                                 "difference")
    arcpy.CopyFeatures_management(
        in_features = difference,
        out_feature_class = difference_fc
    )
    arcpy.Delete_management(union_fc)
    arcpy.Delete_management(difference)

    # When we completed the differencing, we may have split some parcels
    # into 2. This is a problem for reporting, because contiguity of
    # developable area is the relevant for singlepart polygons only. For a
    # multipart result, we'd want to calculate contiguity in each part,
    # and then use a summary function to get a contiguity for the parcel
    # as a whole. So, we need to split the difference result into single
    # part polygons
    print("2.2 converting difference to singlepart polygons")
    difference_sp_fc = os.path.join(intmd_gdb,
                                    "difference_sp")
    arcpy.MultipartToSinglepart_management(
        in_features = difference_fc, 
        out_feature_class = difference_sp_fc
    )
    arcpy.Delete_management(difference_fc)
    
    # Now, we want an ID to identify each unique polygon, as well be 
    # calculating contiguity on a polygon basis. We can create this variable
    # using the same methods as the ProcessID, but we'll call it "PolyID"
    print("2.3 adding a unique ID field for individual polygons")
    codeblock = 'val = 0 \ndef processID(): \n    global val \n    start = 1 \n    if (val == 0):  \n        val = start\n    else:  \n        val += 1  \n    return val\n'
    arcpy.AddField_management(
        in_table = difference_sp_fc,
        field_name = "PolyID",
        field_type = "LONG",
        field_is_required = "NON_REQUIRED"
    )
    arcpy.CalculateField_management(
        in_table = difference_sp_fc,
        field = "PolyID",
        expression = "processID()",
        expression_type = "PYTHON3",
        code_block = codeblock
    )

    # Finally, we can delete every field from 'difference_sp' except
    # ProcessID, PolyID, and ChunkID -- we do this because we're going to
    # be eating a LOT of memory in our contiguity calculations, so every
    # bit counts!
    # Thanks to: https://gis.stackexchange.com/questions/229187/copying-only-certain-fields-columns-from-shapefile-into-new-shapefile-using-mode
    print("2.4 formatting the singlepart difference")
    fkeep = [parcels_id_field, "PolyID", "ChunkID"]
    fmap = arcpy.FieldMappings()
    fmap.addTable(difference_sp_fc)
    fields = {f.name: f for f in arcpy.ListFields(difference_sp_fc)}
    for fname, fld in fields.items():
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            if fname not in fkeep:
                fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    arcpy.conversion.FeatureClassToFeatureClass(in_features = difference_sp_fc, 
                                                out_path = intmd_gdb,
                                                out_name = "diff",
                                                field_mapping = fmap)
    arcpy.Delete_management(difference_sp_fc)
    diff_fc = os.path.join(intmd_gdb,
                           "diff")
    
    # To match contiguity back to our polygons, we'll use the relationship
    # between PolyID and ParcelID. So, we'll create a table of PolyID and
    # ProcessID to merge our contiguity results to. Once we have our results,
    # we can summarize over ProcessID and merge back to the parcel polygons
    print("2.5 extracting a polygon-parcel ID reference table")
    ref_df = arcpy.da.FeatureClassToNumPyArray(
        in_table = diff_fc,
        field_names = [parcels_id_field, "PolyID"],
        spatial_reference = parcels_sr,
        null_value = -1
    )
    ref_df = pd.DataFrame(ref_df)

    # This completes our differencing -- we now are ready to calculate
    # contiguity, which we will do on "diff" relative to "PolyID". But,
    # because we want to take care of as much spatial processing in this
    # function as possible, we'll initialize a save feature class for the
    # future summarized results first
    
    # Now, we're good to move into the meat of this function: calculating
    # contiguity (and developable area)
           
    # Chunk processing of contiguity -----------------------------------------

    print("")
    print("3. Chunk processing contiguity and developable area")
    
    # Chunks are processed in a loop over the chunk IDs, which are simply
    # 1, 2, ..., chunks. So, we need our chunk IDs, and a place to store
    # the results of each chunk
    chunk_ids = np.arange(2, chunks+1)
    ctgy = []
    

    # Now, we loop through the chunks to calculate contiguity:
    for i in chunk_ids:
        sn = ".".join(["3", str(i)])
        print(''.join([sn, " Chunk ", str(i), " of ", str(chunks)]))
        

        # First, we need to select our chunk of interest, which we'll do
        # using select by attribute
        print(sn + ".1 selecting chunk")
        selection = ' '.join(['"ChunkID" =', str(i)])
        parcel_chunk = arcpy.SelectLayerByAttribute_management(
            in_layer_or_view = diff_fc, 
            selection_type = "NEW_SELECTION",
            where_clause = selection
        )

        # Contiguity is calculated over a raster, so we need to rasterize
        # our chunk for processing
        print(sn + ".2 rasterizing chunk")
        rp = os.path.join(intmd_gdb,
                          ''.join(["chunk_raster_", str(i)]))
        arcpy.FeatureToRaster_conversion(in_features = parcel_chunk,
                                         field = "PolyID",
                                         out_raster = rp,
                                         cell_size = cell_size)
        # arcpy.Delete_management(parcel_chunk)

        # Now we can load the data as a numpy array for processing. This is
        # also the end of spatial processing within the chunk loop -- we deal
        # exclusively with the numpy array from here out in the loop
        print(sn + ".3 loading chunk raster")
        ras = arcpy.RasterToNumPyArray(in_raster = rp,
                                       nodata_to_value = -1)
        # arcpy.Delete_management(rp)
        
        # In addition to calculating contiguity, this rasterization gives
        # us an opportunity to calculate total developable area. This area
        # can be defined as the number of cells with a given ID times the
        # cell size squared. Cell size is fixed of course, and we can grab
        # unique values and counts using numpy functions. We'll remove the
        # information regarding the amount of empty space because we don't
        # care about that
        print(sn + ".4 calculating developable area by polygon")
        poly_ids, counts = np.unique(ras, return_counts=True)
        area = pd.DataFrame.from_dict({"PolyID": poly_ids,
                                       "Count": counts})
        area = area[area.PolyID != -1]
        area["Developable_Area"] = area.Count * (cell_size ** 2) / 43560
        # ASSUMES FEET IS THE INPUT CRS, MIGHT WANT TO MAKE THIS AN
        # ACTUAL CONVERSION IF WE USE THIS OUTSIDE OF PMT. SEE THE
        # LINEAR UNITS CODE/NAME BOOKMARKS
        # spatial_reference.linearUnitName and .linearUnitCode
        area = area.drop(columns="Count")

        # If the area dataframe is empty, this means we have no polygons
        # in the quadrat. This can happen because the quadrats are built
        # relative to the parcel *extent*, so not all quadrats will
        # necessarily have parcels in them. If this is the case, there's no
        # need to calculate contiguity, so we skip the rest of the iteration
        npolys = len(area.index)
        if npolys == 0:
            print("*** no polygons in this quadrat, proceeding to next chunk ***")
        else:
            # If there is at least one polygon, though, we're on to contiguity.
            # Contiguity is based off a polygons' raster cells' relationship to 
            # neighboring cells, particularly those of the same polygon. So, to
            # get at contiguity, we first need to know what polygons are 
            # represented in each cell. We'll organize these into two copies of
            # the same table: one will initialize cell ID organization, and the
            # other will initialize neighboring cell ID organization
            print(sn + ".5 initializing cell neighbor identification")
            ras_dim = ras.shape
            nrow = ras_dim[0]
            ncol = ras_dim[1]
            
            id_tab_self = pd.DataFrame({"Row": np.repeat(np.arange(nrow), ncol), 
                                        "Col": np.tile(np.arange(ncol), nrow),
                                        "ID": ras.flatten()}) 
            id_tab_neighbor = pd.DataFrame({"NRow": np.repeat(np.arange(nrow), ncol), 
                                            "NCol": np.tile(np.arange(ncol), nrow),
                                            "NID": ras.flatten()})
            
            # A lot of these cells represent either empty space or building space,
            # which we don't care about. And because neighbor identification is
            # an expensive process, we don't want to calculate neighbors if we
            # don't have to. So, prior to neighbor identification, we'll isolate
            # the cells for which we'll actually calculate contiguity
            print(sn + ".6 identifying non-empty cells")
            row_oi = id_tab_self[id_tab_self.ID != -1].Row.to_list()
            col_oi = id_tab_self[id_tab_self.ID != -1].Col.to_list()
        
            # To know what polygons are represented in a cell's neighbors, we
            # need to know what cells actually are the neighbors (i.e. their
            # indices). Thankfully, rasters are rectangular, so if we know the
            # index of a cell, we can calculate the index of all its neighbors.
            # That is our next step: we'll organize cells and neighbors into
            # a dataframe
            print(sn + ".7 identifying neighbors of non-empty cells")
            row_basic = [np.arange(x-1, x+2) for x in row_oi]
            col_basic = [np.arange(x-1, x+2) for x in col_oi]
            
            meshed = [np.array(np.meshgrid(x,y)).reshape(2,9).T 
                      for x,y in zip(row_basic, col_basic)]
            meshed = np.concatenate(meshed, axis=0)
            meshed = pd.DataFrame(meshed, columns = ["NRow","NCol"])
            
            meshed.insert(1, "Col", np.repeat(col_oi, 9))
            meshed.insert(0, "Row", np.repeat(row_oi, 9))
            
            # When building our neighbor table, we assumed each cell had 9
            # neighbors to rely on easy repeating structure. However, if a cell
            # is on an edge or corner, it has fewer than 9 neighbors. So we now
            # want to remove any neighbors we might have identified that aren't
            # valid cells
            print(sn + ".8 filtering to valid neighbors by index")
            meshed = meshed[(meshed.NRow >= 0) 
                            & (meshed.NRow < nrow)
                            & (meshed.NCol >= 0)
                            & (meshed.NCol < ncol)]
            
            # Now that we've identified neighbors for each cell of interest, 
            # we want to know what the polygon is represented in the cell and
            # what polygons are represented in the neighbors. To do this, we can
            # merge back to our initialized cell-ID and neighbor-ID tables
            print(sn + ".9 tagging cells and their neighbors with polygon IDs")
            meshed = pd.merge(meshed, 
                              id_tab_self, 
                              left_on = ["Row","Col"],
                              right_on = ["Row","Col"],
                              how = "left")
            meshed = pd.merge(meshed, 
                              id_tab_neighbor, 
                              left_on = ["NRow","NCol"],
                              right_on = ["NRow","NCol"],
                              how = "left")
            
            # We now have one more level of filtering to do to complete our
            # neighbor table. We only want "valid" neighbors: ones where the cell
            # ID and the neighbor ID match (i.e. the cell and neighbor are from
            # the same polygon). We'll complete that filtering here, and then
            # drop the neighbor ID (for legibility)
            print(sn + ".10 fitering to valid neighbors by ID")
            meshed = meshed[meshed.ID == meshed.NID]
            meshed = meshed.drop(columns="NID")
            
            # With neighbors identified, we now need to define cell weights
            # for contiguity calculations. These are based off the specifications
            # in the 'weights' inputs to the function. So, we tag each
            # cell-neighbor pair in 'valid_neighbors' with a weight. 
            print(sn + ".11 tagging cells and neighbors with weights")
            conditions = [(np.logical_and(meshed["NRow"] == meshed["Row"] - 1, meshed["NCol"] == meshed["Col"] - 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] - 1, meshed["NCol"] == meshed["Col"])),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] - 1, meshed["NCol"] == meshed["Col"] + 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"] - 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"])),
                          (np.logical_and(meshed["NRow"] == meshed["Row"], meshed["NCol"] == meshed["Col"] + 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] + 1, meshed["NCol"] == meshed["Col"] - 1)),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] + 1, meshed["NCol"] == meshed["Col"])),
                          (np.logical_and(meshed["NRow"] == meshed["Row"] + 1, meshed["NCol"] == meshed["Col"] + 1))]
            choices = ["top_left","top_center","top_right","middle_left","self","middle_right","bottom_left","bottom_center","bottom_right"]
            meshed["Type"] = np.select(conditions, choices)
            meshed["Weight"] = [weights[key] for key in meshed["Type"]]
            
            # To initialize the contiguity calculation, we sum weights by cell.
            # We lose the ID in the groupby though, which we need to get to
            # contiguity, so we need to merge back to our cell-ID table
            print(sn + ".12 summing weight by cell")
            wtab = meshed.groupby(["Row","Col"])[["Weight"]].agg("sum").reset_index()
            wtab = pd.merge(wtab, 
                            id_tab_self, 
                            left_on = ["Row","Col"],
                            right_on = ["Row","Col"],
                            how = "left")        
            
            # We are now finally at the point of calculating contiguity! It's a
            # pretty simple function, which we apply over our IDs. This is the
            # final result of our chunk process, so we'll also rename our "ID"
            # field to "PolyID", because this is the proper name for the ID over
            # which we've calculated contiguity. This will make our life easier
            # when chunk processing is complete, and we move into data formatting
            # and writing
            print(sn + ".13 calculating contiguity by polygon")
            weight_max = sum(weights.values())
            contiguity = wtab.groupby("ID").apply(lambda x: (sum(x.Weight) / len(x.Weight) - 1) / (weight_max - 1)).reset_index(name="Contiguity")
            contiguity.columns = ["PolyID","Contiguity"]
            
            # For reporting results, we'll merge the contiguity and developable
            # area tables
            print(sn + ".14 merging contiguity and developable area information")
            contiguity = pd.merge(contiguity,
                                  area,
                                  left_on = "PolyID",
                                  right_on = "PolyID",
                                  how = "left")
            
            # We're done chunk processing -- we'll put the resulting data frame
            # in our chunk results list as a final step
            print(sn + ".15 appending chunk results to master list")
            ctgy.append(contiguity)
    
    # Contiguity results formatting ------------------------------------------

    print("")
    print("4. Formatting polygon-level results")
    
    # The results of our chunks are stored in a list after the loop -- our
    # first formatting step is thus to merge these into a single dataframe
    print("4.1 combining chunked results into table format")
    ctgy = pd.concat(ctgy, axis = 0)
    
    # Recall that we calculated contiguity relative to ~polygon~, not
    # necessarily the ~parcel~. But, we want our results to be parcel-level.
    # We can merge our contiguity results with our 'ref_df' -- containing
    # ProcessID (parcel) and PolyID (polygon) -- to achieve this
    print("4.2 filling table with missing polygons")
    ctgy = pd.merge(ref_df,
                    ctgy,
                    left_on="PolyID",
                    right_on="PolyID",
                    how="left")

    # It's possible that a polygon winds up not represented because
    # (1) a building covers the whole polygon or (2) the polygon's developable
    # area was not caught by the cell configuration of the raster. Either way,
    # this results in missing data we'll want to fill for completeness.
    # We fill both contiguity and developable area with 0, because these are
    # the values for no contiguity and no area, respectively
    print("4.3 overwriting missing values with 0")
    ctgy = ctgy.fillna(value = {"Contiguity":0,
                                "Developable_Area":0})
    
    # Writing results --------------------------------------------------------
    print("")
    print("5. Writing polygon-level results")
    # Because we can perform a number of different summaries to the parcel,
    # we stop analysis short of the summarization and simply write the table
    # we have. Then, this table will become an input to a 'summary' function
    # that will calculate parcel level contiguity, developable area, and
    # a combined contiguity-area statistic
    print("5.1 saving full results as a table (level of single-part parcels)")
    ctgy_tt = np.rec.fromrecords(recList = ctgy.values,
                                 names = ctgy.dtypes.index.tolist())
    ctgy_tt = np.array(ctgy_tt)
    save_path = os.path.join(save_gdb_location, 
                             "Contiguity_parcels_sp")
    arcpy.da.NumPyArrayToTable(in_array = ctgy_tt,
                               out_table = save_path)
    
    # Done -------------------------------------------------------------------

    print("")
    print("Done!")
    print("Full contiguity and developable area results table saved to: " + 
          save_path)
    print("")
    return(save_path)
    
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

def analyze_contiguity_summary(full_results_table_path,
                               parcels_id_field,
                               save_gdb_location,
                               summary_funs = ["min", "max", "median", "mean"],
                               area_scaling = True):
    """
    summarize contiguity/developable area results from 
    `analyze_contiguity_index` from sub-parcel to parcel
    
    Parameters
    ----------
    full_results_table_path: path
        path to saved table output of `analyze_contiguity_index`
    parcels_id_field: str
        name of a field used to identify the parcels in the summarized
        parcel results
    save_gdb_location: directory
        .gdb (or feature class with a .gdb) in which summarized parcel results
        will be saved
    summary_funs: list of strs
        functions to be used to summarize contiguity to the parcel; available
        options include min, max, mean, and median
        Default is all options
    area_scaling: boolean
        should a scaled version of developable area be calculated? If `True`,
        a "scaled_area" statistic will be calculated as developable area times
        contiguity index (at the parcel level)
        Default True
    
    Returns
    -------
    Summarized results will be merged to the parcels feature class at
    `save_feature_class_path`. It will be attributed with:
        1. A parcel identifier (as specified in `analyze_contiguity_index`
        when the featur class was initialized)
        2. Parcel developable area (summed to the parcel)
        3. {fun}-summarized contiguity, for each function in `summary_funs`
        4. {fun}-scaled area, for each of {fun}-summarized contiguity, if
        `area_scaling = True`
    """

    # Summarizing up to the parcel -------------------------------------------
    print("")
    print("1. Summarizing contiguity and developable area to the parcels")
    
    # This function summarizes the results of "analyze_contiguity_index" up
    # from the polygon (on PolyID) to the parcel (ProcessID). So first
    # we need our full results
    print("1.1 loading the contiguity full results table")
    df = arcpy.da.TableToNumPyArray(in_table = full_results_table_path,
                                    field_names = [parcels_id_field,
                                                   "Contiguity",
                                                   "Developable_Area"])
    df = pd.DataFrame(df)

    # Now we want to summarize contiguity to the parcel. We'll do that using 
    # every function in 'summary_funs'.
    print("1.2 summarizing contiguity to the parcels")
    ctgy_summary = []
    ctgy_variables = []
    for i in summary_funs:
        print("----> " + i)
        var_name = '_'.join([i.title(), "Contiguity"])
        ci = df.groupby(parcels_id_field).agg({"Contiguity": getattr(np, i)}).reset_index()
        ci.columns = [parcels_id_field, var_name]
        ctgy_summary.append(ci)
        ctgy_variables.append(var_name)

    # The results for each function are stored in a separate table, so we now
    # merge them into a single table
    print("1.3 formatting contiguity summary results")
    ctgy_summary = [df.set_index(parcels_id_field) for df in ctgy_summary]
    ctgy_summary = pd.concat(ctgy_summary, axis=1)
    ctgy_summary = ctgy_summary.reset_index()

    # The only way to summarize developable area is by sum, so we'll take
    # care of that now.
    print("1.4 summarizing developable area to the parcels")
    area_summary = df.groupby(parcels_id_field)[["Developable_Area"]].agg("sum").reset_index()
    
    # The final summary step is then merging the contiguity and developable
    # area summary results
    print("1.5 merging contiguity and developable area summaries")
    df = pd.merge(area_summary,
                  ctgy_summary,
                  left_on = parcels_id_field,
                  right_on = parcels_id_field,
                  how = "left")
    
    # If an area scaling is requested (area_scaling = True), that means
    # we need to create a combined statistic for contiguity and area. To do
    # this, we simply multiply contiguity by developable area (essentially,
    # we're weighting developable area by how contiguous it is). We do this
    # for all contiguity summaries we calculated
    if area_scaling == True:
        print("1.6 calculating combined contiguity-developable area statistics")
        for i in ctgy_variables:
            var_name = i.replace("Contiguity", "Scaled_Area")
            df[var_name] = df["Developable_Area"] * df[i]
    else:
        print("1.6 no area scaling requested -- moving on")
    
    # DO WE NEED THIS ADJUSTMENT?
    
    # Finally, we need to fill the table with 0s for all measures for any 
    # missing parcels. So we pull in the parcels, merge our summary stats,
    # and fill NAs with 0.
    # save_path = os.path.join(save_gdb_location,
    #                          "Contiguity_parcels")
    # parcel_ids = arcpy.da.FeatureClassToNumPyArray(in_table = save_path,
    #                                                field_names = parcels_id_field)
    # parcel_ids = pd.DataFrame(parcel_ids)
    # df = pd.merge(parcel_ids,
    #               df,
    #               left_on = "ProcessID",
    #               right_on = "ProcessID",
    #               how = "left")
    # df = df.fillna(0)
    
    # Now that our summaries are completed and missing values are filled,
    # we're ready to write our results

    # Saving results ---------------------------------------------------------

    print("")
    print("2. Writing summarized results")
    
    # In the 'anaylze_contiguity_index' function, we initialized a feature
    # class in which we'd ultimately save our contiguity and developable
    # area results. So now we just merge our summary results to this
    # initialized feature class on the ProcessID
    print("2.1 saving summarized results as a table (level of parcels)")
    df_et = np.rec.fromrecords(recList = df.values, 
                               names = df.dtypes.index.tolist())
    df_et = np.array(df_et)
    save_path = os.path.join(save_gdb_location,
                             "Contiguity_parcels")
    arcpy.da.NumPyArrayToTable(in_array = df_et,
                               out_table = save_path)
    
    # Done -------------------------------------------------------------------

    print("")
    print("Done!")
    print("Parcel contiguity and developable area results saved to: " + 
          save_path)
    print("")
    return(save_path)

# %% Main
if __name__ == "__main__":
    
    for year in [2014, 2015, 2016, 2017, 2018, 2019]:
        print(str(year))
        
        # 1. Define the 'analyze_contiguity_index' function inputs
        parcels_path = os.path.join("K:/Projects/MiamiDade/PMT/Data",
                                    ''.join(["IDEAL_PMT_", str(year), ".gdb"]),
                                    "Polygons/Parcels")
        buildings_path = os.path.join("K:/Projects/MiamiDade/PMT/Data/Cleaned",
                                      "OSM_Buildings",
                                      "OSM_Buildings_20201001111703.shp")
        save_gdb_location =  os.path.join("K:/Projects/MiamiDade/PMT/Data",
                                          ''.join(["IDEAL_PMT_", str(year), ".gdb"]))
        parcels_id_field = "FOLIO"
        chunks = 20
        cell_size = 40
        weights = "nn"
        
        # 2. Derive/define the 'analyze_contiguity_summary' function inputs
        full_results_table_path = os.path.join(save_gdb_location, 
                                               "Contiguity_parcels_sp")
        summary_funs = ["min", "max", "median", "mean"]
        area_scaling = True
        # save_gdb_location and parcels_id_field are the same as above 
        
        # 3. 'analyze_contiguity_index'
        start = time.time()
        aci = analyze_contiguity_index(parcels_path = parcels_path,
                                       buildings_path = buildings_path,
                                       save_gdb_location = save_gdb_location,
                                       parcels_id_field = parcels_id_field,
                                       chunks = chunks,
                                       cell_size = cell_size,
                                       weights = weights)
        
        # 4. 'analyze_contiguity_summary'
        acs = analyze_contiguity_summary(full_results_table_path = full_results_table_path,
                                         parcels_id_field = parcels_id_field,
                                         save_gdb_location = save_gdb_location,
                                         summary_funs = summary_funs,
                                         area_scaling = area_scaling)
        end = time.time()
        
        # 5. How long did this take?
        exec_time = round((end-start) / 60, 1)
        print(str(year) + " took " + str(exec_time) + " minutes to execute")
        print("")