library(tidyverse)
library(loop.estimator)

raw <- read_csv('data/flat_mass_doe_data.csv')

matches <- read_csv('matching/adam/results/matches.csv',col_types='cnff')

matchingData <- read_csv('matching/adam/data_for_matching.csv')

### remnant model
preds <- read_csv('remnant_model/results/experiment_predictions.csv')

cv <- read_csv('remnant_model/results/kfold_remnant_predictions.csv')
names(cv)[1] <- 'school_code'


cv%>%ggplot(aes(predicted_performance,actual_performance))+geom_point()+geom_abline(slope=1,intercept=0)+geom_smooth(method='lm')

cv%>%ggplot(aes(predicted_performance,actual_performance-predicted_performance))+geom_point()+geom_hline(yintercept=0)+geom_smooth()

rem <- inner_join(raw,cv)

cors <- rem%>%
  summarize(across(c(-school_code,-predicted_performance,-actual_performance),~cor(.,predicted_performance,method='spearman',use='pairwise')))%>%as.data.frame()%>%t()


################### outcome analysis
dat <- inner_join(matches,preds)%>%droplevels()

dat%>%
  mutate(treat=ifelse(trt==1,'Treatment','Comparison'))%>%
  ggplot(aes(predicted_performance,actual_performance))+geom_point()+geom_abline(intercept=0,slope=1)+
  geom_label(data=dat%>%group_by(trt)%>%
               summarize(
                 treat=ifelse(trt==1,'Treatment','Comparison'),
                 rho=paste('rho=',round(cor(predicted_performance,actual_performance),3)),x=0.4,y=0.8),
             mapping=aes(x,y,label=rho))+
  facet_wrap(~treat)+
  scale_x_continuous(limits=range(dat$actual_performance))+
  scale_y_continuous(limits=range(dat$actual_performance))


diffMean <- lm(actual_performance~trt+pairmatch,data=dat)
confint(diffMean,'trt')
summary(diffMean)$coef['trt',]


rebar <- lm(I(actual_performance-predicted_performance)~trt+pairmatch,data=dat)
confint(rebar,'trt')
summary(rebar)$coef['trt',]



ploopRem <- with(dat,
                 p_loop(
                   actual_performance,
                   trt,
                   Z=as.matrix(predicted_performance),
                   P=as.numeric(pairmatch),
                   pred=p_ols_interp))
ploopRem[1]
sqrt(ploopRem[2])
ploopRem[1]+c(-1,1)*qt(.975,36)*sqrt(ploopRem[2])

### add in ps model covariates
load('results/matches.RData')
Z <- psDat[match(dat$school_code,psDat$school_code),]
stopifnot(all.equal(dat$school_code,Z$school_code))
stopifnot(all.equal(dat$trt,Z$trt))
Z <- Z%>%select(-trt,-school_code)%>%as.matrix()

ploopPlusRF <- with(dat,
                 p_loop(
                   actual_performance,
                   trt,
                   Z=cbind(Z,predicted_performance),
                   P=as.numeric(pairmatch),
                   pred=p_rf_interp))
ploopPlusRF[1]
sqrt(ploopPlusRF[2])
ploopPlusRF[1]+c(-1,1)*qt(.975,36)*sqrt(ploopPlusRF[2])

ploopPlusRebar <- with(dat,
                 p_loop(
                   actual_performance-predicted_performance,
                   trt,
                   Z=Z,
                   P=as.numeric(pairmatch),
                   pred=p_rf_interp))
ploopPlusRebar[1]
sqrt(ploopPlusRebar[2])
ploopPlusRebar[1]+c(-1,1)*qt(.975,36)*sqrt(ploopPlusRebar[2])

ploop <- with(dat,
                 p_loop(
                   actual_performance,
                   trt,
                   Z=Z,
                   P=as.numeric(pairmatch),
                   pred=p_rf_interp))
ploop[1]
sqrt(ploop[2])
ploop[1]+c(-1,1)*qt(.975,36)*sqrt(ploop[2])


results=data.frame(
  method=c('diffInMeans','rebar','reloop','reloopPlusRF','ploop'),
  est=c(coef(diffMean)['trt'],coef(rebar)['trt'],ploopRem[1],ploopPlusRF[1],ploop[1]),
  se=c(summary(diffMean)$coef['trt','Std. Error'],
       summary(rebar)$coef['trt','Std. Error'],
       sqrt(c(ploopRem[2],ploopPlusRF[2],ploop[2]))))
