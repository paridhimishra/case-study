
# Clean up workspace
rm(list = ls(all.names = TRUE))
# Install the released version from CRAN
install.packages("testthat")
install.packages("devtools")
install.packages("rJava")
install.packages("mleap")
install.packages("sparklyr")
install.packages("r2pmml")
install.packages("Metrics")
install.packages("data.table", type="source", dependencies=TRUE)

library(devtools)
use_testthat()

library(sparklyr)
spark_install()

library(mleap)
install_maven()
install_mleap()

library(sparklyr)
spark_install()

