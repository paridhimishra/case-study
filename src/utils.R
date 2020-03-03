library(sparklyr)
library(DBI)
library(data.table)
library(xgboost)
library(dplyr)
library(mleap)
library(r2pmml)
library(Metrics)

# source constants
source("src/constants.R")

# Utility functions

# Splits the dataset into training, test and validation sets
#
# Arguments:
#  df: the dataset
#  TRAIN_TEST_SPLIT_RATIO: ration between train and test sets
#  TRAIN_VALID_SPLIT_RATIO: ration between train and validation sets
# 
# Return:
# List of train, test and validation sets
getTrainTestEvalSplit <- function(df, TRAIN_TEST_SPLIT_RATIO, TRAIN_VALID_SPLIT_RATIO){
  
  nrows_df = nrow(df)
  nrows_df
  train_rows = TRAIN_TEST_SPLIT_RATIO * nrows_df
  test_rows = nrow(df) - train_rows
  
  train = df[1:train_rows-1]
  test = df[train_rows:nrows_df-1]
  nrows_df
  nrows_train = nrow(train)
  train_rows = TRAIN_VALID_SPLIT_RATIO * nrows_train
  train = df[1:train_rows-1]
  eval = df[train_rows:nrows_train-1]
  
  dataset <- list(trainData = train, 
                  testData = test, 
                  evalData = eval)
return (dataset)
}

# Prepares dataset for xgb model
#
# Arguments:
#  x: the dataset
#  target: target variable or label
#  features: optionally provide lsit of features
# 
# Return:
# df - xgboost dataset 
#function converts datasets to DMatrix
convertyDataToXgBoost <- function (x, target, features = NULL) 
{
  if (!target %in% names(x$trainData)) {
    stop("A valid target variable name must be provided")
  }
  if (is.null(features)) {
    features = names(x$trainData)[!names(x$trainData) %in% 
                                    target]
  }
  else {
    if (!all(features %in% names(x$trainData))) {
      stop("Not all features can be found in training data.")
    }
    if ((target %in% features)) {
      warning("You've also included target variable in features and it will be excluded")
      features = features[features != target]
    }
  }
  res = lapply(x, function(y) {
    setDT(y)
    if (nrow(y) > 0) {
      tmptarget = y[, target, with = F][[1]]
      if (is.factor(tmptarget)) {
        lev <- unique(unlist(tmptarget))
        tmptarget <- as.integer(factor(tmptarget, levels = lev)) - 
          1
      }
      tmp = xgb.DMatrix(data.matrix(y[, features, with = F]), 
                        label = tmptarget, missing = 0)
      return(tmp)
    }
    else {
      return(NULL)
    }
  })
  res[["targetTest"]] = x$testData[, target, with = F]
  res[["features"]] = features
  # gc()
  return(res)
}


# Prepare training data from source data
#
# Arguments:
#  spark_conn : spark connection passed from calling fn
#  data_version : default 1, used for versioning training data
#
# Return:
#  training_data 
getTrainingData <- function(spark_conn, data_version) {
  drive <-
    spark_read_parquet(spark_conn, "drive", TRAINING_DATA_DRIVE)
  trip <- spark_read_parquet(spark_conn, "trip", TRAINING_DATA_TRIP)
  weather <-
    spark_read_parquet(spark_conn, "weather", TRAINING_DATA_WEATHER)
  accel <- spark_read_csv(spark_conn, "accel", TRAINING_DATA_ACCEL)
  
  #merge spark tables
  training_data <- dbGetQuery(
    spark_conn,
    " WITH temp_trip AS
                 (SELECT a.vehicle_id, a.trip_id
                  , min(a.datetime) as min_time
                  , max(a.datetime) as max_time
                  , max(a.velocity) as max_velocity
                  , avg(a.velocity) as avg_velocity
                  , max(b.engine_coolant_temp) as max_coolant_temp
                  , max(b.eng_load) as max_eng_load
                  , max(b.fuel_level) as max_fuel_level
                  , min(b.fuel_level) as min_fuel_level
                  , max(b.iat) as max_iat
                  , min(b.iat) as min_iat
                  , max(b.rpm) as max_rpm
                 FROM trip a
                 INNER JOIN drive b
                  ON a.vehicle_id = b.vehicle_id
                  AND a.trip_id = b.trip_id
                  AND a.datetime = b.datetime
                 GROUP BY a.vehicle_id, a.trip_id
                 HAVING max(a.velocity) > 0)
                SELECT b.vehicle_id
                  , b.min_time
                  , b.max_time
                  , b.max_velocity
                  , b.avg_velocity
                  , b.max_coolant_temp
                  , b.max_eng_load
                  , b.max_fuel_level
                  , b.min_fuel_level
                  , b.max_iat
                  , b.min_iat
                  , b.max_rpm
                  , a.ft_sum_hard_brakes_10_flg_val
                  , a.ft_sum_hard_brakes_3_flg_val
                FROM accel a, temp_trip b
                WHERE a.trip_id = b.trip_id
                 "
  )
  return(training_data)
}

# Computes the error between test set and predicted values
#
# Arguments:
#  actuals: test set isolated from start
#  preds: values predicted by xgb
#
# Return:
#  The mean average error of model
evaluateModel <- function(actuals, preds) {
  return (cor(actuals, preds))
  # return (rmse(actuals, preds))
  # return (mae(actuals, preds))
  
}

# Feature engineering some of the input data
#
# Arguments:
#  df: input data frame
#
# Return:
#  df: the updated data frame
featureEngineerData <- function(df) {
  
  # Normalisation of features
  df$duration <- as.numeric((df$max_time - df$min_time) / 3600)
  df$ft_sum_hard_brakes_10_flg_val <-
    df$ft_sum_hard_brakes_10_flg_val / df$duration
  df$ft_sum_hard_brakes_3_flg_val <-
    df$ft_sum_hard_brakes_3_flg_val / df$duration
  
  df <- as.data.table(df)
  
  df[, perc_hard_brakes_10 := ntile(ft_sum_hard_brakes_10_flg_val, 100)]
  df[, perc_hard_brakes_3 := ntile(ft_sum_hard_brakes_3_flg_val, 100)]
  df[, aggressiveness_score := perc_hard_brakes_10 + perc_hard_brakes_3]
  
  # Remove fields
  df[, ':='(
    duration = NULL,
    min_time = NULL,
    max_time = NULL,
    vehicle_id = NULL,
    perc_hard_brakes_10 = NULL,
    perc_hard_brakes_3 = NULL,
    ft_sum_hard_brakes_10_flg_val = NULL,
    ft_sum_hard_brakes_3_flg_val = NULL
  )]
  return (df)
}

