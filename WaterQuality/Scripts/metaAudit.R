#Meta audit, right?
rm(list=ls())
source("h:/ericg/16666LAWA/LAWA2019/scripts/LAWAFunctions.R")

siteTables=dir(path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Data",
                          pattern="siteTable*.*",
                          full.names=T,recursive=T,ignore.case=T)
rm(AR)  #Agency rep in siteTable
for(i in seq_along(siteTables)){
  st = read_csv(siteTables[i])
  ar = table(st$Agency)
  if(!exists('AR')){
    AR=as.data.frame(ar)
    names(AR)[dim(AR)[2]]=paste0('Pull',strFrom(strTo(siteTables[i],'/SiteTable'),'Data/2019-'))
  }else{
    AR=merge(AR,as.data.frame(ar))
    names(AR)[dim(AR)[2]]=paste0('Pull',strFrom(strTo(siteTables[i],'/SiteTable'),'Data/2019-'))
  }
  rm(ar)
}
rm(siteTables)
rownames(AR)=AR[,1]
AR=AR[,-1]
plot(as.numeric(AR[1,]),type='l',ylim=c(0,200))
apply(AR,1,lines)

trendFiles=dir(path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis",
               pattern="RiverWQ_Trend*.*",
               full.names=T,recursive=T,ignore.case=T)
rm(tfAR,tfPR,tfFR,tfSR)
for(i in seq_along(trendFiles)){
  tf = read_csv(trendFiles[i],guess_max = 10000)
  tf$TrendScore[is.na(tf$TrendScore)] <- (-99)
  ar = table(tf$Agency)
  pr = table(tf$period)
  sr = table(tf$TrendScore)
  if('frequency'%in%names(tf)){
    fr = table(tf$frequency)
  }else{
    fr = table(tf$Frequency)
  }
  if(!exists('tfAR')){
    tfAR=as.data.frame(ar)
    names(tfAR)[dim(tfAR)[2]]=paste0('Pull',strFrom(strTo(trendFiles[i],'/RiverWQ'),'Analysis/2019-'))
  }else{
    tfAR=merge(tfAR,as.data.frame(ar),all=T)
    names(tfAR)[dim(tfAR)[2]]=paste0('Pull',strFrom(strTo(trendFiles[i],'/RiverWQ'),'Analysis/2019-'))
  }
  rm(ar)
  if(!exists('tfPR')){
    tfPR=as.data.frame(pr)
    names(tfPR)[dim(tfPR)[2]]=paste0('Pull',strFrom(strTo(trendFiles[i],'/RiverWQ'),'Analysis/2019-'))
  }else{
    tfPR=merge(tfPR,as.data.frame(pr),all=T)
    names(tfPR)[dim(tfPR)[2]]=paste0('Pull',strFrom(strTo(trendFiles[i],'/RiverWQ'),'Analysis/2019-'))
  }
  rm(pr)
  if(!exists('tfFR')){
    tfFR=as.data.frame(fr)
    names(tfFR)[dim(tfFR)[2]]=paste0('Pull',strFrom(strTo(trendFiles[i],'/RiverWQ'),'Analysis/2019-'))
  }else{
    tfFR=merge(tfFR,as.data.frame(fr),all=T)
    names(tfFR)[dim(tfFR)[2]]=paste0('Pull',strFrom(strTo(trendFiles[i],'/RiverWQ'),'Analysis/2019-'))
  }
  rm(fr)
  if(!exists('tfSR')){
    tfSR=as.data.frame(sr)
    names(tfSR)[dim(tfSR)[2]]=paste0('Pull',strFrom(strTo(trendFiles[i],'/RiverWQ'),'Analysis/2019-'))
  }else{
    tfSR=merge(tfSR,as.data.frame(sr),all=T)
    names(tfSR)[dim(tfSR)[2]]=paste0('Pull',strFrom(strTo(trendFiles[i],'/RiverWQ'),'Analysis/2019-'))
  }
  rm(sr)
}
rm(trendFiles)
rownames(tfAR)=tfAR[,1]
tfAR=tfAR[,-1]
rownames(tfPR)=tfPR[,1]
tfPR=tfPR[,-1]
rownames(tfFR)=tfFR[,1]
tfFR=tfFR[,-1]
rownames(tfSR)=tfSR[,1]
tfSR=tfSR[,-1]


plot(as.numeric(tfAR[1,]),type='l',ylim=c(0,5000))
apply(tfAR,MARGIN = 1,lines)

plot(as.numeric(tfPR[1,]),type='l',ylim=c(0,15000))
apply(tfPR[1:3,],MARGIN = 1,lines)

plot(as.numeric(tfFR[1,]),type='l',ylim=c(0,15000))
apply(tfFR,MARGIN = 1,lines)

plot(as.numeric(tfSR[1,]),type='l',ylim=c(0,15000))
apply(tfSR,MARGIN = 1,lines)


stateFiles=dir(path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis",
               pattern="ITERiverState*.*",
               full.names=T,recursive=T,ignore.case=T)
rm(sfAR,sfSR)
for(i in seq_along(stateFiles)){
  sf = read_csv(stateFiles[i],guess_max = 10000)
  ar = table(sf$Agency)
  sr = table(sf$StateScore)
  if(!exists('sfAR')){
    sfAR=as.data.frame(ar)
    names(sfAR)[dim(sfAR)[2]]=paste0('Pull',strFrom(strTo(stateFiles[i],'/AuditRiver'),'Analysis/2019-'))
  }else{
    sfAR=merge(sfAR,as.data.frame(ar),all=T)
    names(sfAR)[dim(sfAR)[2]]=paste0('Pull',strFrom(strTo(stateFiles[i],'/AuditRiver'),'Analysis/2019-'))
  }
  rm(ar)
  if(!exists('sfSR')){
    sfSR=as.data.frame(sr)
    names(sfSR)[dim(sfSR)[2]]=paste0('Pull',strFrom(strTo(stateFiles[i],'/AuditRiver'),'Analysis/2019-'))
  }else{
    sfSR=merge(sfSR,as.data.frame(sr),all=T)
    names(sfSR)[dim(sfSR)[2]]=paste0('Pull',strFrom(strTo(stateFiles[i],'/AuditRiver'),'Analysis/2019-'))
  }
  rm(sr)
}
rm(stateFiles)
rownames(sfAR)=sfAR[,1]
sfAR=sfAR[,-1]
rownames(sfSR)=sfSR[,1]
sfSR=sfSR[,-1]

plot(as.numeric(sfAR[1,]),type='l',ylim=c(0,4000))
apply(sfAR,1,lines)

plot(as.numeric(sfSR[1,]),type='l',ylim=c(0,11000))
apply(sfSR,1,lines)

