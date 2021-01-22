
# -----------------------------------------------------------------------------
# --------------------------------- Function ----------------------------------
# -----------------------------------------------------------------------------

# Use roxygen2 style documentation, later...

analyze_blockgroup_estimation = function(mod_frame, type){
  
  # Note that package requirements and helper functions for modeling are loaded 
  # in the extrapolate script, so we don't load them here!
  
  # Set up --------------------------------------------------------------------
  
  cat("------ processing the modeling frame...\n")
  
  # Format mod frame
  block_groups = mod_frame %>%
    mutate(Total_Population = Total_Non_Hisp + Total_Hispanic,
           Total_Employment = CNS01 + CNS02 + CNS03 + CNS04 + 
             CNS05 + CNS06 + CNS07 + CNS08 + CNS09 + 
             CNS10 + CNS11 + CNS12 + CNS13 + CNS14 + 
             CNS15 + CNS16 + CNS17 + CNS18 + CNS19 + 
             CNS20,
           Total_Emp_Area = CNS_01_par + CNS_02_par + CNS_03_par + CNS_04_par + 
             CNS_05_par + CNS_06_par + CNS_07_par + CNS_08_par + CNS_09_par + 
             CNS_10_par + CNS_11_par + CNS_12_par + CNS_13_par + CNS_14_par + 
             CNS_15_par + CNS_16_par + CNS_17_par + CNS_18_par + CNS_19_par + 
             CNS_20_par)
  
  # Correlations -----------------------------------------------------------------
  
  cat("------ calculating correlation matrix for variables of interest...\n")
  
  # Variable setup
  # Defines our variables of interest for modeling
  independent_variables = c("LND_VAL", "LND_SQFOOT", "JV", "TOT_LVG_AREA" ,
                            "NO_BULDNG", "NO_RES_UNTS", "RES_par",
                            "CNS_01_par", "CNS_02_par", "CNS_03_par",
                            "CNS_04_par", "CNS_05_par", "CNS_06_par",
                            "CNS_07_par", "CNS_08_par", "CNS_09_par",
                            "CNS_10_par", "CNS_11_par", "CNS_12_par",
                            "CNS_13_par", "CNS_14_par", "CNS_15_par",
                            "CNS_16_par", "CNS_17_par", "CNS_18_par",
                            "CNS_19_par", "CNS_20_par", "Total_Emp_Area",
                            "Since_2013")
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
  dependent_variables_trn = c("Drove", "Carpool", "Transit",
                              "NonMotor", "Work_From_Home", "Other")
  total_variables = c("Total_Population", "Total_Employment")
  
  # Correlation Matrix
  # rcorr produces 3 matrices:
  # 1. correlation coefficient
  # 2. sample size
  # 3. p-value (presumably of test that R != 0)
  cor_matrix = block_groups[,c(independent_variables, dependent_variables_emp,
                               dependent_variables_pop, dependent_variables_trn,
                               total_variables)] %>%
    as.matrix() %>%
    rcorr()
  
  # Modeling ------------------------------------------------------------------
  
  cat("------ fitting models informed by the correlation matrix...\n")
  
  # Define the variables we're seeking to predict
  if(length(type) == 1 & type == "JOBS"){
    mod_vars = dependent_variables_emp
  } else if(length(type) == 1 & type == "DEM"){
    mod_vars = c(dependent_variables_pop,
                 dependent_variables_trn)
  } else{
    mod_vars = c(dependent_variables_emp,
                 dependent_variables_pop,
                 dependent_variables_trn)
  }
  
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
  
  # Set up return -------------------------------------------------------------
  
  # If we fit jobs and dem at the same time, turn into a named list.
  # Otherwise, we know what's what because there's only one, so no edits
  # are necessary
  if(length(type) == 2){
    job = fits[names(fits) %in% dependent_variables_emp]
    dem = fits[names(fits) %in% dependent_variables_pop]
    fits = list(JOB = job,
                DEM = dem)
  }
  
  # Now we can return
  return(fits)
}


