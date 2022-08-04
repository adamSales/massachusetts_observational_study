library(optmatch)
library(cpt)

### load in data
## source('code/data.r')
dat <- read_csv('data/data_for_matching.csv')

## in model:
## acheivement
## acheivenet trend
## % white (other races?)
## % frl /econ disadvantaged
## # students
## ELL
## s/t ratio
## class size
## whether there are younger grades

### get trend data
trends=dat%>%
  select(school_code,ends_with("P+A %"))%>%#,ends_with("6 P+A %_na"))%>%
  pivot_longer(-school_code,names_to='test',values_to='percent')%>%
  mutate(year=as.numeric(substr(test,3,4)),grade=substr(test,14,14))%>%
  group_by(school_code,grade)%>%
  summarize(beta=tryCatch(coef(lm(percent~year))[2],error=function(cond) return(NA)))%>%
  pivot_wider(names_from=grade,names_prefix='trend',values_from=beta)

### compile covariates for prop. score
psDat=dat%>%
  mutate(msPercent=(`6`+`7`+`8`)/Total)%>%
  select(
    trt,
    school_code,
    starts_with('2011')&ends_with('P+A %'),
    starts_with('2011')&ends_with('P %'),
    `White %`,
    `Student / Teacher Ratio`,
    `Average Class Size`,
    Total,
    msPercent,
    `English Language Learner %`,
  `Students with Disabilities %`,
  `Economically Disadvantaged %`
  )%>%
  full_join(trends)

### add NA flags
for(nn in names(psDat)[-c(1:2)])
  if(any(is.na(psDat[[nn]]))){
    psDat[[paste0(nn,'_na')]]=is.na(psDat[[nn]])
    psDat[[nn]][is.na(psDat[[nn]])]=mean(psDat[[nn]],na.rm=TRUE)
  }


### principal components for the rest
datRest=dat%>%
  select(-c(
            trt,
            starts_with('2011')&ends_with('P+A %'),
            starts_with('2011')&ends_with('P %'),
            `White %`,
            `Student / Teacher Ratio`,
            `Average Class Size`,
            Total,
            `English Language Learner %`,
            `Students with Disabilities %`,
            `Economically Disadvantaged %`
          ))%>%
  mutate(across(c(-school_code),
                list(imp=function(x) ifelse(is.na(x),mean(x,na.rm=TRUE),x),
                     na=is.na)))%>%
  dplyr::select(school_code,ends_with('_na'),ends_with('_imp'))

## more NA flags
naCols=select(datRest,ends_with('na'))
naCols=naCols[,vapply(naCols,function(x) min(c(sum(x),sum(1-x)))>0,TRUE)]

datRest=datRest%>%select(school_code,ends_with('imp'))%>%
  bind_cols(naCols)

pca=princomp(datRest[,-1],cor=TRUE)

plot(pca)
summary(pca)
### first 4 components=70% of variance

pca4=data.frame(school_code=datRest$school_code,pca$scores[,1:4])

psDat=full_join(psDat,pca4)


### check on collinearity by hand
Rho=cor(psDat[,-c(1,2)])
range(Rho[upper.tri(Rho)])

### print out biggest bivariate correlations
www=which(abs(Rho)>0.8,arr.ind=TRUE)
www=www[www[,1]<www[,2],]
big=cbind(rownames(Rho)[www[,1]],rownames(Rho)[www[,2]],apply(www,1,function(x) round(Rho[x[1],x[2]],2)))
rownames(big) <- NULL
big

psDat=psDat%>%
  select(-ends_with("P+A %_na"),-`2011.0_Grade 8 P %_na`, -trend6_na, -trend7_na, -trend8_na)

Rho=cor(psDat[,-c(1,2)])
range(Rho[upper.tri(Rho)])

## look at histograms of covariates, do some transformations
psDat%>%
  pivot_longer(c(-school_code,-trt),names_to='covariate',values_to='value')%>%
  ggplot(aes(value))+geom_histogram()+facet_wrap(~covariate,scales="free_x")

### a lot of 0 for ELL & msPercent
min(psDat$`English Language Learner %`)
sum(psDat$`English Language Learner %`==0)

psDat$justMS=psDat$msPercent==1
psDat$noEll=psDat$`English Language Learner %`==0
psDat$`English Language Learner %`=ifelse(psDat$`English Language Learner %`==0,0,log(psDat$`English Language Learner %`))

### take Z-scores of covariates
psDat=psDat%>%mutate(across(c(-trt,-school_code),scale))

### fix names so they play nice with R
names(psDat) <- gsub("2011.0_Grade","Grade",names(psDat),fixed=TRUE)
names(psDat) <- gsub(" |\\+|%|/","",names(psDat))
#names(psDat) <- gsub("-","_",names(psDat))

### propensity score model
psmod1=bayesglm(
  trt~.-school_code,data=psDat,family=binomial)


br=binned.resids(predict(psmod1,type='response'),psmod1$y)$binned#resid(psmod1,type='response'))$binned
binnedplot(predict(psmod1,type='response'),resid(psmod1,type='response'))
### nearly no trt schools with prop score <0.057. after that model fit looks OK


psDat$ps=psmod1$linear.predictor

psDat%>%ggplot(aes(as.factor(trt),ps))+geom_boxplot(outlier.shape=NA)+geom_jitter()

### there's a treatment school whose prop score is something of an outlier. what's up?
psDat[which.max(psDat$ps),]%>%as.data.frame()

#### construct matches. start with caliper =0.2, allow up to 5 controls per match (save some for remnant)
### also only one trt school per match
dist=match_on(psmod1,caliper=0.2,data=psDat)

summary(m1 <- fullmatch(dist,data=psDat,max.controls=5,min.controls=1))

## excludes that one outlier treatment school. let's make the caliper bigger to include it

dist2=match_on(psmod1,caliper=0.5,data=psDat)

summary(m2 <- fullmatch(dist2,data=psDat,max.controls=5,min.controls=1))

### what if we don't limit # of controls to 5?
summary(m3 <- fullmatch(dist2,data=psDat,min.controls=1))

## what if no caliper and no limits?
summary(m4<- fullmatch(psmod1))

### pair match
summary(pmatch <- pairmatch(psmod1))

save(m1,m2,m3,m4,pmatch,psDat,file='results/matches.RData')

### I like m1 and (less so) pmatch
### csv for ethan
psDat%>%ungroup()%>%select(school_code,trt)%>%mutate(fullmatch=m2,pairmatch=pmatch)%>%
  write_csv(file="results/matches.csv")

bal=xBalance(trt~.-school_code-m1-m2,data=psDat,report=c('std.diffs','chisquare.test'),
             strata=list(unmatched=NULL,
                         pair=~pmatch,
                         fullMatchWCaliper1=~m1,
                         fullMatchWCaliper2=~m2))
bal
#plot(bal,col=c('red','blue','green'))
#abline(v=c(-0.25,-.05,.05,.25),lty=2)

res=NULL
for(i in 1:dim(bal$results)['strata'])
  res=rbind(res,
            as.data.frame(bal$results[,,i])%>%
            rownames_to_column('Covariate')%>%
            mutate(strat=dimnames(bal$results)$strata[i]))

res%>%
  filter(strat!='fullMatchWCaliper1')%>%
  group_by(Covariate)%>%
  mutate(xmin=min(std.diff),xmax=max(std.diff))%>%
  ungroup()%>%
  ggplot(mapping=aes(std.diff,Covariate))+
  geom_point(aes(color=strat,shape=strat),size=2)+
  geom_vline(xintercept=c(-0.25,-.05,.05,.25),linetype='dashed')+
  geom_vline(xintercept=0)+
  geom_segment(aes(x=xmin,xend=xmax,yend=Covariate),color='black')


### unmatched cpt
 cptDat=dat%>%
  select(-school_code)%>%
  mutate(across(c(-trt),list(imp=~ifelse(is.na(.),mean(.,na.rm=TRUE),.),na=is.na)))%>%
  select(trt,ends_with("_imp"),ends_with("_na"))%>%
  select(where(~ifelse(is.factor(.),TRUE,var(.)>0.001)))

unmatched.cpt=cpt(select(cptDat,-trt),cptDat$trt)
save(unmatched.cpt,cptDat,file='results/unmatchedCPT.RData')

### cpt data processing
stopifnot(all.equal(psDat$school_code,dat$school_code))
m2dat=dat%>%
  mutate(m2=m2)%>%
  filter(!is.na(m2))%>%
  select(-school_code)%>%
  mutate(across(c(-trt,-m2),list(imp=~ifelse(is.na(.),mean(.,na.rm=TRUE),.),na=is.na)))%>%
  select(trt,m2,ends_with("_imp"),ends_with("_na"))%>%
  select(where(~ifelse(is.factor(.),TRUE,var(.)>0.001)))


m2.cpt=cpt(select(m2dat,-trt,-m2),m2dat$trt)#,class.methods=c('forest','glmnet2')) glmmet2 takes too long
m2.cpt.blocked=cptMatch(select(m2dat,-trt,-m2),m2dat$trt,blocks=m2dat$m2)#,class.methods=c('forest','glmnet2'))
save(m2.cpt,m2.cpt.blocked,m2,m2dat,file='results/cptM2across.RData')

hist(m2.cpt$nulldist)
abline(v=m2.cpt$teststat)
m2.cpt$pval

hist(m2.cpt.blocked$nulldist)
abline(v=m2.cpt.blocked$teststat)
m2.cpt.blocked$pval

m2.cpt.psdat=cptMatch(psDat[!is.na(m2),]%>%select(-trt,-school_code,-ps),psDat$trt[!is.na(m2)],
                      blocks=m2[!is.na(m2)])

m2.cpt.psdat$pval

#### cpt for pair match
stopifnot(all.equal(psDat$school_code,dat$school_code))
pmatchDat=dat%>%
  mutate(pmatch=pmatch)%>%
  filter(!is.na(pmatch))%>%
  select(-school_code)%>%
  mutate(across(-c(trt,pmatch),list(imp=~ifelse(is.na(.),mean(.,na.rm=TRUE),.),na=is.na)))%>%
  select(trt,pmatch,ends_with("_imp"),ends_with("_na"))%>%
  select(where(~ifelse(is.factor(.),TRUE,var(.)>0.001)))%>%
  arrange(pmatch)%>%select(-pmatch)


pmatch.cpt.paired=cpt(select(pmatchDat,-trt),pmatchDat$trt,paired=TRUE)
pmatch.cpt=cpt(select(pmatchDat,-trt),pmatchDat$trt,paired=FALSE)
save(pmatch.cpt.paired,pmatch.cpt,pmatchDat,file='results/cptPmatch.RData')

pmatch.cpt.paired$pval
pmatch.cpt$pval
