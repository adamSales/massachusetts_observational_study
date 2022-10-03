### look at remnant
library(tidyverse)

dat <- read_csv('remnant_model/results/kfold_remnant_predictions.csv')

with(dat,plot(predicted_performance,actual_performance))
abline(0,1)

with(dat,cor(predicted_performance,actual_performance))

head(dat[order(dat$predicted_performance-dat$actual_performance),])
