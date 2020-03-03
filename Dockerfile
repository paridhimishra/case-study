FROM ubuntu:16.04

MAINTAINER Amazon SageMaker Examples <amazon-sagemaker-examples@amazon.com>

RUN apt-get -y update && apt-get install -y --no-install-recommends \
    wget \
    r-base \
    r-base-dev \
    ca-certificates

RUN R -e "install.packages(c('mda', 'plumber'), repos='https://cloud.r-project.org')"

COPY src/utils.R /opt/ml/utils.RCOPY src/installers.R /opt/ml/installers.RCOPY src/predict.R /opt/ml/predict.R

ENTRYPOINT ["/usr/bin/Rscript", "/opt/ml/predict.R", "--no-save"]

