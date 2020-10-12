#### Functions ####
Calc_Bin_Width = function(Variable){
  range(Variable, na.rm = TRUE)[2] / sqrt(length(Variable))
}

flattenCorrMatrix <- function(cormat, pmat) {
  require(Hmisc)
  ut <- upper.tri(cormat)
  data.frame(
    row = rownames(cormat)[row(cormat)[ut]],
    column = rownames(cormat)[col(cormat)[ut]],
    cor  = (cormat)[ut],
    p = pmat[ut]
  )
}

add_centroids = function(df){
  require(sf)
  cbind(st_drop_geometry(df), st_coordinates(st_centroid(df)))
}

factor_to_num = function(variable){
  as.numeric(as.character(variable))
}

sign_coef_to_formula = function(Cor_Matrix, variable){
  require(Hmisc)
  col_order = c("column", "row" , "cor", "p")
  col_names = c("Dependent", "Independent", "R_Value", "P_Value")
  
  cor_matrix  = Cor_Matrix
  
  Correlations = flattenCorrMatrix(cor_matrix$r, cor_matrix$P)
  
  Flat_Matrix      = subset(Correlations[col_order], 
                            Correlations$column == variable)
  
  colnames(Flat_Matrix) = col_names
  
  Flat_Matrix$R_Value = round(Flat_Matrix$R_Value, digits = 4)
  Flat_Matrix$P_Value = round(Flat_Matrix$P_Value, digits = 4)
  
  Flat_Matrix         = subset(Flat_Matrix, Flat_Matrix$P_Value <= 0.05 &
                                 Flat_Matrix$Independent %in% Independent_Variables)
  
  Variables           = unique(Flat_Matrix$Independent)
  
  My_Formula          = paste(unique(Flat_Matrix$Dependent),
                              paste(Variables, collapse = " + "),
                              sep = "~") %>%
    as.formula()
  
  return(list(Flat_Matrix = Flat_Matrix,
              Variables   = Variables,
              Formula     = My_Formula))
}

testing_residuals = function(My_Model){
  require(ggplot2)
  Data = My_Model
  
  residuals = data.frame("fitted" = Data$fitted.values, 
                         "error"  = Data$residuals)
  
  ggplot(residuals, 
         aes(x = fitted, y = error)) +
    geom_point() + 
    ggtitle("Fitted vs Errors")
}