library(tidyverse)
library(arm)
library(RItools)
library(broom)
select=dplyr::select

usage <- read_csv('data/assistments_usage.csv')
dat <- read_csv('data/flat_mass_doe_data.csv')

dim(usage)
dim(dat)

dat <- inner_join(usage,dat)

users <- filter(dat,in_treatment!=1,used_assistments==1)

dat <- filter(dat,in_treatment==1|used_assistments==0)%>%
  dplyr::select(-used_assistments)

dat <- dat[,sapply(dat,sd,na.rm=TRUE)>0]

sort(sapply(dat,function(x) mean(is.na(x))))

table(dat$in_treatment)

fmla=as.formula(paste0('in_treatment~`',paste(names(dat)[-c(1,2)],collapse='`+`'),'`'))
xBalance(fmla,data=dat,report=c('std.diffs','z.scores'))

names(dat)=gsub('_','-',names(dat),fixed=TRUE)
dat=rename(dat,"trt"="in-treatment")

www=dat%>%
  summarize(across(c(-trt,-`school-code`),list(wilcox=function(x) wilcox.test(x[trt==1],x[trt==0],na.rm=TRUE)$p.value,trtMean=function(x) mean(x[trt==1],na.rm=TRUE),ctlMean=function(x) mean(x[trt==0],na.rm=TRUE))))

www=www%>%pivot_longer(everything(), names_to=c("covariate","xxx"),values_to="summ",names_sep='_')

www2=www%>%pivot_wider(id_cols=covariate,names_from=xxx,values_from=summ)


www=www2%>%
  mutate(diff=trtMean-ctlMean)

www$pvalFDR=p.adjust(www$wilcox,'fdr')

sigDiff=www%>%filter(pvalFDR<0.05)%>%
  mutate(pvalFDR=round(pvalFDR,4))%>%
  dplyr::select(covariate,trtMean,ctlMean,diff,wilcox,pvalFDR)%>%arrange(pvalFDR)

names(dat)[1]='school_code'

datLog=dat%>%mutate(across(contains('#'),log))

datNAimp=dat%>%mutate(across(c(-trt,-school_code),list(imp=function(x) ifelse(is.na(x),mean(x,na.rm=TRUE),x),
                                                         na=is.na)))%>%
  dplyr::select(trt,school_code,ends_with('_na'),ends_with('_imp'))

num=datNAimp%>%select(ends_with('_na'))%>%summarize(across(everything(), function(x) min(c(sum(x),sum(1-x)))))%>%
  pivot_longer(everything(),names_to='cov',values_to='num')%>%pull(num)


naCols=select(datNAimp,ends_with('na'))
naCols=naCols[,vapply(naCols,function(x) min(c(sum(x),sum(1-x)))>0,TRUE)]

datNAimp=datNAimp%>%select(trt,school_code,ends_with('imp'))%>%
  bind_cols(naCols)


pca=princomp(datNAimp[,-c(1,2)],cor=TRUE)

screeplot(pca)
