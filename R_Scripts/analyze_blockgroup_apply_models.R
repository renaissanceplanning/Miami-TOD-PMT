
# -----------------------------------------------------------------------------
# --------------------------------- Function ----------------------------------
# -----------------------------------------------------------------------------

# Use roxygen2 style documentation, later...

analyze_blockgroup_apply_models = function(models,
                                           pred_frame,
                                           year,
                                           type){
  
  # Note that package requirements and helper functions for modeling are loaded 
  # in the extrapolate script, so we don't load them here!
  
  # Set up --------------------------------------------------------------------
  
  cat("------ processing the prediction frame...\n")
  
  block_groups = pred_frame %>%
    mutate(Year = year,
           Since_2013 = factor_to_num(Year) - 2013,
           Total_Emp_Area = CNS_01_par + CNS_02_par + CNS_03_par + 
             CNS_04_par + CNS_05_par + CNS_06_par + 
             CNS_07_par + CNS_08_par + CNS_09_par + 
             CNS_10_par + CNS_11_par + CNS_12_par + 
             CNS_13_par + CNS_14_par + CNS_15_par + 
             CNS_16_par + CNS_17_par + CNS_18_par + 
             CNS_19_par + CNS_20_par)
    
  # Applying the models -------------------------------------------------------
  
  cat("------ applying the models...\n")
  
  # Variables
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
  
  # Variables by type
  if(length(type) == 1 & type == "JOBS"){
    dependent_variables = dependent_variables_emp
  } else if(length(type) == 1 & type == "DEM"){
    dependent_variables = c(dependent_variables_pop,
                            dependent_variables_trn)
  } else{
    dependent_variables = c(dependent_variables_emp,
                            dependent_variables_pop,
                            dependent_variables_trn)
  }
  
  # Apply the models
  for(var in dependent_variables){
    model = models[[var]]
    block_groups[var] = predict(model, 
                                newdata = block_groups, 
                                type = 'response') 
  }

  # Now we just return the modified-in-place table  
  return(block_groups)
}




