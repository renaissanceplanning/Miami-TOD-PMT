
# -----------------------------------------------------------------------------
# Calculate a binwidth for a provided variable

calc_bin_width = function(variable){
  range(variable, na.rm = TRUE)[2] / sqrt(length(variable))
}

# -----------------------------------------------------------------------------
# COmbine correlation estimates with tests of significance

flatten_corr_matrix = function(cormat, pmat) {
  require(Hmisc)
  ut = upper.tri(cormat)
  data.frame(row = rownames(cormat)[row(cormat)[ut]],
             column = rownames(cormat)[col(cormat)[ut]],
             cor = (cormat)[ut],
             p = pmat[ut])
}

# -----------------------------------------------------------------------------
# Add centroid X and Y coordinates as columns of a spatial dataframe
# Note: this removes the geometry attribute from the original object

add_centroids = function(df){
  require(sf)
  cbind(st_drop_geometry(df), st_coordinates(st_centroid(df)))
}

# -----------------------------------------------------------------------------
# Convert a factor variable to numeric
# Note: must be a numeric factor. This won't work if the factor is character

factor_to_num = function(variable){
  as.numeric(as.character(variable))
}

# -----------------------------------------------------------------------------
# Create a model formula using variables with significant correlations to a
# dependent variable

sign_coef_to_formula = function(cor_matrix, variable, independent_variables){
  require(Hmisc)
  # Set up naming conventions
  col_order = c("column", "row" , "cor", "p")
  col_names = c("dependent", "independent", "r_value", "p_value")
  
  # Combine correlation coefficients and p value into a dataframe
  correlations = flatten_corr_matrix(cor_matrix$r, cor_matrix$P)
  
  # Subset to only the dependent variable of interest
  flat_matrix = subset(correlations[col_order], 
                       correlations$column == variable)
  
  # Rename columns for clarity
  colnames(flat_matrix) = col_names
  
  # Subset to only rows with significant p value and valid IVs
  flat_matrix = subset(flat_matrix, 
                       flat_matrix$p_value <= 0.05 & 
                         flat_matrix$independent %in% independent_variables)
  
  # Extract unique IVs
  variables = unique(flat_matrix$independent)
  
  # Create a modeling formula
  fm = paste(unique(flat_matrix$dependent),
             paste(variables, collapse = " + "),
             sep = "~") %>%
    as.formula()
  
  # Done
  return(list(flat_matrix = flat_matrix,
              variables = variables,
              formula = fm))
}

# -----------------------------------------------------------------------------
# Residuals vs. fitted diagnostic plot

testing_residuals = function(model){
  require(ggplot2)
  residuals = data.frame("fitted" = model$fitted.values, 
                         "error" = model$residuals)
  ggplot(residuals, 
         aes(x = fitted, y = error)) +
    geom_point() + 
    ggtitle("Fitted vs. residuals")
}