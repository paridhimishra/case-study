# Source global variables and utility functions
source("src/constants.R")
source("src/utils.R")

# Makes predictions for xgb model
#
# Arguments:
#  input list of 10 variables 
# 
# Return:
# predicted aggresiveness score 
predict <- function (input) {

# load the trained model
model_xgb = xgb.load(MODEL_XGB)

# convert the input vector into DMatrix
serving_data <- data.frame(matrix(ncol = 10, nrow = 0))
features <- c("max_velocity", "avg_velocity", "max_coolant_temp", "max_eng_load", 
       "max_fuel_level", "min_fuel_level", "max_iat", "min_iat", "max_rpm", "aggresiveness_score")
colnames(serving_data) <- features

# serving_data[nrow(df) + 1,] = c(98.8, 60.2, 178, 210, 124, 83, 83, 47, 1998, 10)
serving_data[nrow(df) + 1,] = input

serving_data[] <- lapply(serving_data, as.numeric)
data <- xgb.DMatrix(data=data.matrix(serving_data))
serving_data <- convertyDataToXgBoost(data, 'aggressiveness_score')

predictions = predict(model_xgb, serving_data)
return (predictions)
}


