# Define global variables

SEED = 100
SPARK_VERSION <-2.4
SPARK_MODE <- "local"

TRAINING_DATA_VERSION = 1.0

TRAIN_TEST_SPLIT_RATIO = 0.8
TRAIN_VALID_SPLIT_RATIO = 0.8

MODEL_XGB_PMML = "output/model_xgb.pmml"

# TRAINING DATA
# TODO use data version here
TRAINING_DATA_DRIVE <- "data/raw/drive"
TRAINING_DATA_TRIP <-  "data/raw/trip"
TRAINING_DATA_WEATHER <- "data/raw/weather"
TRAINING_DATA_ACCEL <- "data/raw/drive_features.csv"

OBJECTIVE = 'reg:squarederror'
EVAL_METRIC = 'rmse'
ETA = 0.1
SUBSAMPLE = 0.75
COLSAMPLE = 0.8076
MIN_CHILD = 16
MAX_DEPTH = 3
SILENT = 0
NROUNDS = 1000
EARLY_STOP = 50
PRINT_EVERY_N = 50
VERBOSE = 1

