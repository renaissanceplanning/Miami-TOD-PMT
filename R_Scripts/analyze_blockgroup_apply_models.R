
# -----------------------------------------------------------------------------
# --------------------------------- Function ----------------------------------
# -----------------------------------------------------------------------------

# Use roxygen2 style documentation, later...

analyze_blockgroup_apply_models = function(block_group_data_directory,
                                           model_list_path,
                                           save_directory=NULL){
  
  # Requirements -------------------------------------------------------------- 
  
  req = c("stringr","dplyr", "sf", "Hmisc", "car")
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
  
  # Validation ----------------------------------------------------------------
  
  # block_group_data_directory must be valid file path
  # if(!dir.exists(block_group_data_directory)){
  #   stop("'block_group_data_directory' does not exists")
  # }
  
  # model_list_path must be valid file path
  # if(!file.exists(model_list_directory)){
  #   stop("'model_list_path' does not exists")
  # }
  
  # save_directory must be existing or creatable
  # And, if it doesn't exist, we need to create it!
  if(!is.null(save_directory)){
    if(!dir.exists(save_directory)){
      tryCatch(dir.create(save_directory),
               error = function(e){
                 message("'save_directory' is not creatable")
               })
    }
  }
  
  # Helper functions for modeling ---------------------------------------------
  
  cat("\n")
  cat("Loading helper functions for modeling...\n")
  
  helper_funs = paste0("K:/Projects/MiamiDade/PMT/code/R_Scripts/",
                       "prep_Modeling_HelperFunctions.R")
  source(helper_funs)
  
  # Data read -----------------------------------------------------------------
  
  cat("Reading the block group data...\n")
  
  # List the files we need
  csvs = list.files(path = block_group_data_directory, 
                    pattern = ".csv$",
                    full.names = TRUE)
  
  # Iteratively read in the files (and format)
  block_groups = lapply(csvs, function(x){
    df = read.csv(x)
    yr = str_extract(x, "[0-9]{4}") %>% last
    df$Year = as.factor(yr)
    df$Since_2013 =  factor_to_num(df$Year) - 2013
    df = df %>%
      mutate(Total_Emp_Area = CNS01_LVG_AREA + CNS02_LVG_AREA + CNS03_LVG_AREA + 
               CNS04_LVG_AREA + CNS05_LVG_AREA + CNS06_LVG_AREA + 
               CNS07_LVG_AREA + CNS08_LVG_AREA + CNS09_LVG_AREA + 
               CNS10_LVG_AREA + CNS11_LVG_AREA + CNS12_LVG_AREA + 
               CNS13_LVG_AREA + CNS14_LVG_AREA + CNS15_LVG_AREA + 
               CNS16_LVG_AREA + CNS17_LVG_AREA + CNS18_LVG_AREA + 
               CNS19_LVG_AREA + CNS20_LVG_AREA)
    return(df)
  })
    
  # Applying the models -------------------------------------------------------
  
  cat("Applying the models...\n")
  
  # Read in the models
  model_list = readRDS(model_list_path)
  
  # Variable setup
  # Defines our variables of interest for modeling
  dependent_variables_emp = c("CNS01", "CNS02", "CNS03", "CNS04", "CNS05", 
                              "CNS06", "CNS07", "CNS08", "CNS09", "CNS10",
                              "CNS11", "CNS12", "CNS13", "CNS14", "CNS15", 
                              "CNS16", "CNS17", "CNS18", "CNS19", "CNS20")
  dependent_variables_pop = c("Total_Non_Hisp", "Total_Hispanic", 
                              "White_Hispanic", "Black_Hispanic", 
                              "Asian_Hispanic", "Multi_Hispanic",
                              "Other_Hispanic", "White_Non_Hisp", 
                              "Black_Non_Hisp", "Asian_Non_Hisp", 
                              "Multi_Non_Hisp", "Other_Non_Hisp")
  dependent_variables_trn = c("Drove", "Carpooled", "Transit",
                              "NonMotor", "WFH", "Other")
  total_variables = c("Total_Population", "Total_Employment")
  dependent_variables = c(total_variables,
                          dependent_variables_pop,
                          dependent_variables_emp,
                          dependent_variables_trn)
  
  # Apply the models
  modeled = lapply(block_groups, function(x){
    for(var in dependent_variables){
      model = model_list[[var]]
      x[var] = predict(model, 
                       newdata=x, 
                       type='response') 
    }
    return(x)
  })
  
  # Saving --------------------------------------------------------------------
  
  # Save as rds models? or csvs of coefficients?
  if(!is.null(save_directory)){
    cat("Saving modeled results...\n")
    for(i in modeled){
      file_name = paste0("Block_Group_Extrapolated_",
                         unique(i$Year),
                         ".csv")
      save_path = file.path(save_directory, file_name)
      write.csv(i, save_path, row.names=FALSE)
    }
    cat("--", length(modeled), "files saved to:", 
        str_replace_all(file_name, "[0-9]{4}", "\\{Year\\}"))
  }
  
  # Done ----------------------------------------------------------------------
  
  cat("Done!\n\n")
  return(modeled)
}

# -----------------------------------------------------------------------------
# ---------------------------- With PMT Defaults ------------------------------
# -----------------------------------------------------------------------------


bgddir = "K:/Projects/MiamiDade/PMT/Data/Modeling/To_Model"
mlpath = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Block_Groups/Block_Group_Extrapolation_Models.rds"
sdir = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Block_Groups"

analyze_blockgroup_apply_models(block_group_data_directory = bgddir,
                                model_list_path = mlpath,
                                save_directory = sdir)



