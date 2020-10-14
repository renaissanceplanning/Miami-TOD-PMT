
# -----------------------------------------------------------------------------
# --------------------------------- Function ----------------------------------
# -----------------------------------------------------------------------------

# Use roxygen2 style documentation, later...

analyze_blockgroup_estimation = function(block_group_data_directory,
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
                    full.name = TRUE)
  
  # Iteratively read in the files (and format)
  block_groups = lapply(csvs, function(x){
    df = read.csv(x)
    yr = str_extract(x, "[0-9]{4}") %>% last
    df$Year = as.factor(yr)
    df$Since_2013 =  factor_to_num(df$Year) - 2013
    return(df)
  }) %>% 
    bind_rows %>%
    mutate(Total_Population = Total_Non_Hisp + Total_Hispanic,
           Total_Employment = CNS01 + CNS02 + CNS03 + CNS04 + CNS05 + CNS06 + 
             CNS07 + CNS08 + CNS09 + CNS10 + CNS11 + CNS12 + CNS13 + CNS14 + 
             CNS15 + CNS16 + CNS17 + CNS18 + CNS19 + CNS20,
           Total_Emp_Area = CNS01_LVG_AREA + CNS02_LVG_AREA + CNS03_LVG_AREA + 
             CNS04_LVG_AREA + CNS05_LVG_AREA + CNS06_LVG_AREA + CNS07_LVG_AREA + 
             CNS08_LVG_AREA + CNS09_LVG_AREA + CNS10_LVG_AREA + CNS11_LVG_AREA +
             CNS12_LVG_AREA + CNS13_LVG_AREA + CNS14_LVG_AREA + CNS15_LVG_AREA + 
             CNS16_LVG_AREA + CNS17_LVG_AREA + CNS18_LVG_AREA + CNS19_LVG_AREA + 
             CNS20_LVG_AREA)
  
  # Correlations -----------------------------------------------------------------
  
  cat("Calculating correlation matrix for variables of interest...\n")
  
  # Variable setup
  # Defines our variables of interest for modeling
  independent_variables = c("LND_VAL", "LND_SQFOOT", "JV", "TOT_LVG_AREA" ,
                            "NO_BULDNG", "NO_RES_UNTS", "RESIDENTIAL_LVG_AREA",
                            "CNS01_LVG_AREA", "CNS02_LVG_AREA", "CNS03_LVG_AREA",
                            "CNS04_LVG_AREA", "CNS05_LVG_AREA", "CNS06_LVG_AREA",
                            "CNS07_LVG_AREA", "CNS08_LVG_AREA", "CNS09_LVG_AREA",
                            "CNS10_LVG_AREA", "CNS11_LVG_AREA", "CNS12_LVG_AREA",
                            "CNS13_LVG_AREA", "CNS14_LVG_AREA", "CNS15_LVG_AREA",
                            "CNS16_LVG_AREA", "CNS17_LVG_AREA", "CNS18_LVG_AREA",
                            "CNS19_LVG_AREA", "CNS20_LVG_AREA", "Total_Emp_Area",
                            "Year", "Since_2013")
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
  
  # Correlation Matrix
  # rcorr produces 3 matrices:
  # 1. correlation coefficient
  # 2. sample size
  # 3. p-value (presumably of test that R != 0)
  cor_matrix = block_groups[c(independent_variables, dependent_variables_emp,
                              dependent_variables_pop, dependent_variables_trn,
                              total_variables)] %>%
    as.matrix() %>%
    rcorr()
  
  # Modeling ------------------------------------------------------------------
  
  cat("Fitting models informed by the correlation matrix...\n")
  
  # Define the variables we're seeking to predict
  mod_vars = c(total_variables,
               dependent_variables_pop,
               dependent_variables_emp,
               dependent_variables_trn)
  
  # Iteratively fit models
  fits = lapply(mod_vars, function(x){
    # Determining significant variables for predicting 'x'
    # Answers the question, "For each independent variable we could use for
    # prediction, what is the correlation to 'x'? Is it significant?"
    model_inputs = sign_coef_to_formula(cor_matrix = cor_matrix, 
                                        variable = x,
                                        independent_variables = independent_variables)
    
    # Fitting a linear model
    model = lm(model_inputs$formula, 
               data = block_groups)
    return(model)
  })
  names(fits) = mod_vars
  
  # Saving --------------------------------------------------------------------
  
  # Save as rds models? or csvs of coefficients?
  if(!is.null(save_directory)){
    cat("Saving models as rds...\n")
    save_path = file.path(save_directory,
                          "Block_Group_Extrapolation_Models.rds")
    saveRDS(fits, save_path)
    cat("-- saved to:", save_path)
  }
  
  # Done ----------------------------------------------------------------------
  
  cat("Done!\n\n")
  return(fits)
}

# -----------------------------------------------------------------------------
# ---------------------------- With PMT Defaults ------------------------------
# -----------------------------------------------------------------------------

bgddir = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Block_Groups"
sdir = "K:/Projects/MiamiDade/PMT/Data/Cleaned/Block_Groups"

analyze_blockgroup_estimation(block_group_data_directory = bgddir,
                              save_directory = sdir)


