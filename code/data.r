library(tidyverse)
library(arm)
library(RItools)
library(broom)
select=dplyr::select

usage <- read_csv('data/2012-2013/assistments_usage.csv')
dat <- read_csv('data/2012-2013/mcas_exports/csv/flat_mass_doe_data.csv')

dat <- inner_join(usage,dat)

users <- filter(dat,in_treatment!=1,used_assistments==1)

dat <- filter(dat,in_treatment==1|used_assistments==0)%>%
  dplyr::select(-used_assistments)

dat <- dat[,sapply(dat,sd,na.rm=TRUE)>0]

dat=rename(dat,"trt"="in_treatment")

write_csv(dat,file='data/data_for_matching.csv')
