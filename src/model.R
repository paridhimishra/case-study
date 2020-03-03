# Source global variables and utility functions
source("src/constants.R")
source("src/utils.R")

# Create spark instance
sc <- spark_connect(master = SPARK_MODE, version = SPARK_VERSION)

# Get training data as data frame from source (parquet files)
df <- getTrainingData(sc, TRAINING_DATA_VERSION)

# Close spark instance
spark_disconnect(sc)

# Set seed for reporducible results
set.seed(SEED)

# Feature engineering
df <- featureEngineerData(df)

# Split datasets and build model
datasets <-
  getTrainTestEvalSplit(df, TRAIN_TEST_SPLIT_RATIO, TRAIN_VALID_SPLIT_RATIO)

train <- datasets$trainData
test <- datasets$testData

datasets <- convertyDataToXgBoost(datasets, 'aggressiveness_score')


# XGB parameters
paramxgBoost <- list(
  objective  = OBJECTIVE,
  eval_metric = EVAL_METRIC,
  eta =  ETA,
  subsample = SUBSAMPLE,
  colsample_bytree = COLSAMPLE,
  min_child_weight = MIN_CHILD,
  max_depth = MAX_DEPTH
)

watchlist = list(train = datasets$trainData, eval = datasets$evalData)

#Train model
  model_xgb = xgb.train(nrounds = NROUNDS, params = paramxgBoost,
                        data = datasets$trainData,
                        early_stop_round = EARLY_STOP,
                        watchlist = watchlist,
                        print_every_n = PRINT_EVERY_N,
                        verbose = VERBOSE
  )

predictions = predict(model_xgb, datasets$testData)

# This statement prints top 10 nodes of the model
# model <- xgb.dump(model_xgb, with_stats = T)
# model[1:10]

# Compute feature importance matrix
# importance_matrix <- xgb.importance(train, model = model_xgb)
# Plot a graph
# xgb.plot.importance(importance_matrix[1:10, ])

# Generate XGBoost feature map
model_xgb_fmap = genFMap(train)
r2pmml(
  model_xgb,
  "output/model_xgb.pmml",
  fmap = model_xgb_fmap,
  response_name = "aggresiveness_score",
  missing = NULL,
  compact = TRUE
)

#evaluate the performance of the model
evaluateModel(test$aggressiveness_score, predictions)

