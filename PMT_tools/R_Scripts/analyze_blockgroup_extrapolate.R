
# -----------------------------------------------------------------------------
# --------------------------------- Function ----------------------------------
# -----------------------------------------------------------------------------

analyze_blockgroup_estimation = function(gdb_directory,
                                         years){
  
  # Requirements -------------------------------------------------------------- 
  
  req = c("stringr","dplyr", "sf", "Hmisc", "car", "arcgisbinding")
  loaded = loadedNamespaces()
  already_loaded = req %in% loaded
  if(any(!already_loaded)){
    tl = req[!already_loaded]
    for(i in tl){
      if(!require(i, character.only = TRUE)){
        cat("[Have to install ", i, "]\n", sep="")
        install.packages(i)
        library(i, character_only = TRUE)
      }
    }
  }
  
  # Connect R and Arc
  arc.check_product()
  
  # Validation ----------------------------------------------------------------
  
  # gdb_directory must be valid file path
  # if(!dir.exists(gdb_directory)){
  #   stop("'gdb_directory' does not exists")
  # }
  
  # years must be all integers
  # if(typeof(x) != "double"){
  #   stop("'years' must be numeric)
  # }
  
  # Helper functions for modeling ---------------------------------------------
  
  helper_funs = paste0("K:/Projects/MiamiDade/PMT/code/R_Scripts/",
                       "prep_Modeling_HelperFunctions.R")
  estimate_fun = paste0("K:/Projects/MiamiDade/PMT/code/R_Scripts/",
                        "analyze_blockgroup_apply_models.R")
  apply_fun = paste0("K:/Projects/MiamiDade/PMT/code/R_Scripts/",
                     "analyze_blockgroup_estimation.R")
  source(helper_funs)
  source(estimate_fun)
  source(apply_fun)
  
  # Data read -----------------------------------------------------------------
  
  # Sort the years for loop processing
  years = sort(years)
  
  # Loop process by year
  # If the "EXTRAP" value isn't anywhere in the frame, add data to a modeling 
  # frame. Once it shows up for the first time, fit the models, and use them
  # to predict out every year until the end
  
  # Set up modeling frames, prediction frames, a write list, and a model list
  mod_frames = NULL
  write_list = list()
  models = list("JOB" = NULL, "DEM" = NULL)
  cat("\n")
  
  # Loop
  for(year in years){
    cat("Working on processing", year, "\n")
    
    # 1. Read
    cat("-- reading...\n")
    gdb = file.path(gdb_directory,
                    paste0("PMT_", year, ".gdb"))
    df = st_read("blockgroup_enrich",
                 dsn = gdb,
                 quiet = TRUE)
    
    # 2. Check for the extrap key
    job = "EXTRAP" %in% df[["JOBS_SOURCE"]]
    dem = "EXTRAP" %in% df[["DEM_SOURCE"]]
    
    # 3. Process based on job and dem
    if(!job & !dem){
      cat("-- no extrapolation necessary, formatting results...\n")
      # 1. save to write list
      write_list = append(write_list, list(df))
      # 2. format
      df$Year = as.factor(year)
      df$Since_2013 =  factor_to_num(df$Year) - 2013
      # 3. bind to modeling frames
      mod_frames = rbind(mod_frames, df %>% st_drop_geometry)
      
    } else if(job & !dem){ 
      cat("-- need to extrapolate JOBS\n")
      # 1. models
      if(is.null(models$JOB)){
        cat("---- fitting jobs models...\n")
        models$JOB = analyze_blockgroup_estimation(mod_frame = mod_frames,
                                                   type = "JOBS")
      }
      # 2. apply the models
      cat("---- applying jobs models...\n")
      updf = analyze_blockgroup_apply_models(models = models$JOB,
                                             pred_frame = df,
                                             year = year,
                                             type = "JOBS")
      # 3. format and put into write_list
      cat("---- formatting results...\n")
      write_drop = names(updf)[!names(updf) %in% names(write_list[[1]])]
      write_add = names(write_list[[1]])[!names(write_list[[1]]) %in% names(updf)]
      write_add_df = matrix(NA, nrow = nrow(updf), ncol = length(write_add)) %>%
        data.frame %>%
        setNames(write_add)
      up_write = updf[,-which(names(updf) %in% write_drop)] %>% cbind(., write_add_df)
      write_nm = names(up_write)[order(match(names(up_write),names(write_list[[1]])))]
      up_write = up_write[, write_nm]
      write_list = append(write_list, list(up_write))
      # 4. format and put into modeling frame
      updf = updf %>% st_drop_geometry()
      model_drop = names(updf)[!names(updf) %in% names(mod_frames)]
      model_add = names(mod_frames)[!names(mod_frames) %in% names(updf)]
      model_add_df = matrix(NA, nrow = nrow(updf), ncol = length(model_add)) %>%
        data.frame %>%
        setNames(model_add)
      up_model = updf[,-which(names(updf) %in% model_drop)] %>% cbind(., model_add_df)
      model_nm = names(up_model)[order(match(names(up_model),names(mod_frames)))]
      up_model = up_model[, model_nm]
      mod_frames = rbind(mod_frames, up_model)
      mod_frames$Year = as.character(mod_frames$Year)
      mod_frames$Year[is.na(mod_frames$Year)] = year
      mod_frames$Year = as.factor(mod_frames$Year)
      
    } else if(!job & dem){ 
      cat("-- need to extrapolate DEM\n")
      # 1. models
      if(is.null(models$DEM)){
        cat("---- fitting dem models...\n")
        models$DEM = analyze_blockgroup_estimation(mod_frame = mod_frames,
                                                   type = "DEM")
      }
      # 2. apply the models
      cat("---- applying dem models...\n")
      updf = analyze_blockgroup_apply_models(models = models$DEM,
                                             pred_frame = df,
                                             year = year,
                                             type = "JOBS")
      # 3. format and put into write_list
      cat("---- formatting results...\n")
      write_drop = names(updf)[!names(updf) %in% names(write_list[[1]])]
      write_add = names(write_list[[1]])[!names(write_list[[1]]) %in% names(updf)]
      write_add_df = matrix(NA, nrow = nrow(updf), ncol = length(write_add)) %>%
        data.frame %>%
        setNames(write_add)
      up_write = updf[,-which(names(updf) %in% write_drop)] %>% cbind(., write_add_df)
      write_nm = names(up_write)[order(match(names(up_write),names(write_list[[1]])))]
      up_write = up_write[, write_nm]
      write_list = append(write_list, list(up_write))
      # 4. format and put into modeling frame
      updf = updf %>% st_drop_geometry()
      model_drop = names(updf)[!names(updf) %in% names(mod_frames)]
      model_add = names(mod_frames)[!names(mod_frames) %in% names(updf)]
      model_add_df = matrix(NA, nrow = nrow(updf), ncol = length(model_add)) %>%
        data.frame %>%
        setNames(model_add)
      up_model = updf[,-which(names(updf) %in% model_drop)] %>% cbind(., model_add_df)
      model_nm = names(up_model)[order(match(names(up_model),names(mod_frames)))]
      up_model = up_model[, model_nm]
      mod_frames = rbind(mod_frames, up_model)
      mod_frames$Year = as.character(mod_frames$Year)
      mod_frames$Year[is.na(mod_frames$Year)] = year
      mod_frames$Year = as.factor(mod_frames$Year)
      
    } else{
      cat("-- need to extrapolate JOBS and DEM\n")
      # 1. models
      if(is.null(models$JOB) & !is.null(models$DEM)){
        cat("---- fitting job models...\n")
        models$JOB = analyze_blockgroup_estimation(mod_frame = mod_frames,
                                                   type = "JOBS")
      } else if(!is.null(models$JOB) & is.null(models$DEM)){
        cat("---- fitting dem models...\n")
        models$DEM = analyze_blockgroup_estimation(mod_frame = mod_frames,
                                                   type = "DEM")
      } else if(is.null(models$JOB) & is.null(models$DEM)){
        models = analyze_blockgroup_estimation(mod_frame = mod_frames,
                                               type = c("JOBS","DEM"))
      } else{
        models = models
      }
      # 2. apply the models
      cat("---- applying job and dem models...\n")
      updf = analyze_blockgroup_apply_models(models = c(models$JOB, models$DEM),
                                             pred_frame = df,
                                             year = year,
                                             type = c("JOBS","DEM"))
      # 3. format and put into write_list
      cat("---- formatting results...\n")
      write_drop = names(updf)[!names(updf) %in% names(write_list[[1]])]
      write_add = names(write_list[[1]])[!names(write_list[[1]]) %in% names(updf)]
      write_add_df = matrix(NA, nrow = nrow(updf), ncol = length(write_add)) %>%
        data.frame %>%
        setNames(write_add)
      up_write = updf[,-which(names(updf) %in% write_drop)] %>% cbind(., write_add_df)
      write_nm = names(up_write)[order(match(names(up_write),names(write_list[[1]])))]
      up_write = up_write[, write_nm]
      write_list = append(write_list, list(up_write))
      # 4. format and put into modeling frame
      updf = updf %>% st_drop_geometry()
      model_drop = names(updf)[!names(updf) %in% names(mod_frames)]
      model_add = names(mod_frames)[!names(mod_frames) %in% names(updf)]
      model_add_df = matrix(NA, nrow = nrow(updf), ncol = length(model_add)) %>%
        data.frame %>%
        setNames(model_add)
      up_model = updf[,-which(names(updf) %in% model_drop)] %>% cbind(., model_add_df)
      model_nm = names(up_model)[order(match(names(up_model),names(mod_frames)))]
      up_model = up_model[, model_nm]
      mod_frames = rbind(mod_frames, up_model)
      mod_frames$Year = as.character(mod_frames$Year)
      mod_frames$Year[is.na(mod_frames$Year)] = year
      mod_frames$Year = as.factor(mod_frames$Year)
      
    }
  }
  
  # Saving -----------------------------------------------------------------
  
  cat("\n")
  for(year in years){
    cat("Working on saving", year, "\n")
    cat("-- converting data to `sp` format...\n")
    w = which(years == year)
    df = write_list[[w]]
    # cat("-- writing to shapefile...\n")
    # st_write(df, file.path("D:/Users/AZ7/Downloads",
    #                        year,
    #                        "blockgroup_for_alloc.shp"))
    spdf = as(df, "Spatial")
    cat("-- writing to geodatabase...\n")
    gdb_path = file.path(gdb_directory,
                         paste0("PMT_", year, ".gdb"),
                         "BlockGroups/blockgroup_for_alloc")
    arc.write(path = gdb_path,
              data = spdf,
              overwrite = TRUE)
  }
  cat("\nDone! Data is ready for allocation\n\n")
  return(NULL)
}



