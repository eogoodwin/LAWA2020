rm(list=ls())
library(tidyverse)
source("h:/ericg/16666LAWA/LAWA2020/Scripts/LWPTrends_Dec18/LWPTrends_v1811.R")
source("h:/ericg/16666LAWA/LAWA2020/Scripts/LAWAFunctions.R")
#source("h:/ericg/16666LAWA/LAWA2020/WaterQuality/scripts/SWQ_state_functions.R")
try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d"))))

Mode=function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

EndYear <- year(Sys.Date())-1
startYear5 <- EndYear - 5+1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1

lakesSiteTable=loadLatestSiteTableLakes()

#Load the latest made 
if(!exists('lakeData')){
  lakeDataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/Lakes/Data",pattern = "LakesWithMetadata.csv",
                            recursive = T,full.names = T,ignore.case=T),1)
  lakeData=read.csv(lakeDataFileName,stringsAsFactors = F)
  rm(lakeDataFileName)
  lakeData$myDate <- as.Date(as.character(lakeData$Date),"%d-%b-%y")
  lakeData$myDate[which(lakeData$Agency=='arc')] <- as.Date(lubridate::dmy(lakeData$Date[which(lakeData$Agency=='arc')]))
  lakeData <- GetMoreDateInfo(lakeData)
  lakeData$monYear = format(lakeData$myDate,"%b-%Y")
  
  lakeData$Season <- lakeData$Month
  SeasonString <- sort(unique(lakeData$Season))
  lakeData <- lakeData%>%dplyr::rename("CenType"="centype")
  lakeData$CenType[lakeData$CenType%in%c("Left","L")]='lt'
  lakeData$CenType[lakeData$CenType%in%c("Right","R")]='gt'
  lakeData$CenType[!lakeData$CenType%in%c("lt","gt")]='not'
  
  lakeData$NewValues=lakeData$Value
  if(mean(lakeData$Value[which(lakeData$Measurement%in%c('NH4N','TN','TP'))],na.rm=T)<250){
    lakeData$Value[which(lakeData$Measurement%in%c('NH4N','TN','TP'))]=lakeData$Value[which(lakeData$Measurement%in%c('NH4N','TN','TP'))]*1000
  }
  if(lubridate::year(Sys.Date())==2019){
    # Lake Omapere in northland was being measured for chlorophyll in the wrong units.  It's values need converting.
    these = which(lakeData$LawaSiteID=='nrc-00095' & lakeData$Measurement=='CHLA')
    if(mean(lakeData$Value[these],na.rm=T)<1){
      lakeData$Value[these] = lakeData$Value[these]*1000
    }
    rm(these)
  }
  
}


lakeData$uclid = paste(lakeData$CouncilSiteID,lakeData$LawaSiteID,sep='||')


# https://www.lawa.org.nz/learn/factsheets/calculating-water-quality-trends/


#15 year trend ####
lakeDatafor15=lakeData%>%filter(Year>=startYear15 & Year <= EndYear & Measurement!="pH")

uclids=unique(lakeDatafor15$uclid)
uMeasures=unique(lakeDatafor15$Measurement)
nMax=length(table(lakeDatafor15$uclid,lakeDatafor15$Measurement)[table(lakeDatafor15$uclid,lakeDatafor15$Measurement)>0])
cat('\n',length(uclids),'\n')
u=1

library(parallel)
library(doParallel)
workers <- makeCluster(5)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  #library(doBy)
  library(plyr)
  library(dplyr)
  
})
foreach(u = u:length(uclids),.combine=rbind,.errorhandling="stop")%dopar%{
  subDat=lakeDatafor15%>%dplyr::filter(uclid==uclids[u])
  siteTrendTable15=data.frame(LawaSiteID=unique(subDat$LawaSiteID),
                              CouncilSiteID=unique(subDat$CouncilSiteID),
                              Measurement=as.character(uMeasures),nMeasures=NA_integer_,
                              nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                              Observations = NA_real_, KWstat = NA_real_,pvalue = NA_real_,
                              nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                              Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                              prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                              AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                              AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_, 
                              Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                              ConfCat=NA_real_,period=15,frequency=NA_real_)
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])
    siteTrendTable15$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      SSD_med <- subSubDat%>%
        dplyr::group_by(LawaSiteID,monYear,Year,Month,Qtr)%>%
        dplyr::summarise(Value=quantile(Value,prob=c(0.5),type=5,na.rm=T),
                         myDate=mean(myDate,na.rm=T),
                         Censored=any(Censored),
                         CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
        )%>%ungroup%>%as.data.frame
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season=factor(SSD_med$Month)
      siteTrendTable15$nFirstYear[uparam]=length(which(SSD_med$Year==startYear15))
      siteTrendTable15$nLastYear[uparam]=length(which(SSD_med$Year==EndYear))
      siteTrendTable15$numMonths[uparam]=dim(SSD_med)[1]
      siteTrendTable15$numYears[uparam]=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      
      #For 15 year monthly we want 90% of measures and 90% of years
      if(siteTrendTable15$numMonths[uparam] >= 0.9*12*15 & siteTrendTable15$numYears[uparam]>=13){
        siteTrendTable15$frequency[uparam]='monthly'
      }else{
        SSD_med <- subSubDat%>%
          dplyr::group_by(LawaSiteID,Year,Qtr)%>%
          dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                           myDate = mean(myDate,na.rm=T),
                           Censored = any(Censored),
                           CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
                           )%>%ungroup%>%as.data.frame
        SSD_med$Season=SSD_med$Qtr
        SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
        siteTrendTable15$numQuarters[uparam]=dim(SSD_med)[1]
        if(siteTrendTable15$numQuarters[uparam] >= 0.9*4*15 & siteTrendTable15$numYears[uparam] >=13){
          siteTrendTable15$frequency[uparam]='quarterly'
        }else{
          siteTrendTable15$frequency[uparam]=paste0('unassessed, nM=',siteTrendTable15$numMonths[uparam],'(162);',
                                                   ' nQ=',siteTrendTable15$numQuarters[uparam],'(54);',
                                                   ' nY=',siteTrendTable15$numYears[uparam],'(13)')
        }
      }
      if(siteTrendTable15$frequency[uparam]%in%c("quarterly","monthly")){
        SeasonString <- sort(unique(SSD_med$Season))
        st <- SeasonalityTest(x = SSD_med,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable15[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = F)
          siteTrendTable15[uparam,names(sk)] <- sk
          siteTrendTable15[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = F)
          siteTrendTable15[uparam,names(mk)] <- mk
          siteTrendTable15[uparam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(subSubDat)
      rm(SSD_med)
    }
  }
  rm(subDat,uparam)
  return(siteTrendTable15)
}-> trendTable15
stopCluster(workers)
rm(workers,uclids,uMeasures,u,lakeDatafor15)
rownames(trendTable15) <- NULL
trendTable15$Sen_Probability[trendTable15$Measurement!="Secchi"]=1-(trendTable15$Sen_Probability[trendTable15$Measurement!="Secchi"])
trendTable15$Probabilitymin[trendTable15$Measurement!="Secchi"]=1-(trendTable15$Probabilitymin[trendTable15$Measurement!="Secchi"])
trendTable15$Probabilitymax[trendTable15$Measurement!="Secchi"]=1-(trendTable15$Probabilitymax[trendTable15$Measurement!="Secchi"])
trendTable15$MKProbability[trendTable15$Measurement!="Secchi"]=1-(trendTable15$MKProbability[trendTable15$Measurement!="Secchi"])
trendTable15$Agency=lakesSiteTable$Agency[match(trendTable15$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable15$Region=lakesSiteTable$Region[match(trendTable15$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable15$LFENZID =    lakesSiteTable$LFENZID[match(trendTable15$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable15$LType =    lakesSiteTable$LType[match(trendTable15$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable15$ConfCat <- cut(trendTable15$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable15$ConfCat=factor(trendTable15$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable15$TrendScore=as.numeric(trendTable15$ConfCat)-3
trendTable15$TrendScore[is.na(trendTable15$TrendScore)]<-(NA)

save(trendTable15,file=paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend15Year.rData"))
rm(trendTable15)



#10 year trend ####
lakeDatafor10=lakeData%>%filter(Year>=startYear10 & Year <= EndYear & Measurement!="pH")

uclids=unique(lakeDatafor10$uclid)
uMeasures=unique(lakeDatafor10$Measurement)
nMax=length(table(lakeDatafor10$LawaSiteID,lakeDatafor10$Measurement)[table(lakeDatafor10$LawaSiteID,lakeDatafor10$Measurement)>0])

cat('\n',length(uclids),'\n')
u=1
library(parallel)
library(doParallel)
workers <- makeCluster(5)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  
})
foreach(u = u:length(uclids),.combine=rbind,.errorhandling="stop")%dopar%{
  subDat=lakeDatafor10%>%filter(uclid==uclids[u])
  siteTrendTable10=data.frame(LawaSiteID=unique(subDat$LawaSiteID),
                              CouncilSiteID=unique(subDat$CouncilSiteID),
                              Measurement=as.character(uMeasures),nMeasures = NA_integer_,
                              nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                              Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                              nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                              Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                              prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                              AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                              AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                              Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                              ConfCat=NA_real_,period=10,frequency=NA_real_)
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])
    siteTrendTable10$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      SSD_med <- subSubDat%>%
        dplyr::group_by(LawaSiteID,monYear,Year,Month,Qtr)%>%
        dplyr::summarise(Value=quantile(Value,prob=c(0.5),type=5,na.rm=T),
                         myDate=mean(myDate,na.rm=T),
                         Censored=any(Censored),
                         CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
                         )%>%ungroup%>%as.data.frame
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season = SSD_med$Month
      siteTrendTable10$nFirstYear[uparam]=length(which(SSD_med$Year==startYear10))
      siteTrendTable10$nLastYear[uparam]=length(which(SSD_med$Year==EndYear))
      siteTrendTable10$numMonths[uparam]=dim(SSD_med)[1]
      siteTrendTable10$numYears[uparam]=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      
      #For 10 year monthly we want 90% of measures and 90% of years
      if(siteTrendTable10$numMonths[uparam] >= 0.9*12*10 & siteTrendTable10$numYears[uparam]>=9){
        siteTrendTable10$frequency[uparam]='monthly'
      }else{
        SSD_med <- subSubDat%>%
          dplyr::group_by(LawaSiteID,Year,Qtr)%>%
          dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                           myDate = mean(myDate,na.rm=T),
                           Censored = any(Censored),
                           CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
                           )%>%ungroup%>%as.data.frame
        SSD_med$Season=SSD_med$Qtr
        SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
        siteTrendTable10$numQuarters[uparam]=dim(SSD_med)[1]
        if(siteTrendTable10$numQuarters[uparam] >= 0.9*4*10 & siteTrendTable10$numYears[uparam] >=9){
          siteTrendTable10$frequency[uparam]='quarterly'
        }else{
          siteTrendTable10$frequency[uparam]=paste0('unassessed, nM=',siteTrendTable10$numMonths[uparam],'(108);',
                                                    ' nQ=',siteTrendTable10$numQuarters[uparam],'(36);',
                                                    ' nY=',siteTrendTable10$numYears[uparam],'(9)')
        }
      }
      if(siteTrendTable10$frequency[uparam]%in%c("monthly","quarterly")){
        st <- SeasonalityTest(x = SSD_med,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable10[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = F)
          siteTrendTable10[uparam,names(sk)] <- sk
          siteTrendTable10[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = F)
          siteTrendTable10[uparam,names(mk)] <- mk
          siteTrendTable10[uparam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(subSubDat)
      rm(SSD_med)
    }
  }
  rm(subDat,uparam)
  return(siteTrendTable10)
}-> trendTable10
stopCluster(workers)
rm(workers,uclids,uMeasures,u,lakeDatafor10)
rownames(trendTable10) <- NULL
trendTable10$Sen_Probability[trendTable10$Measurement!="Secchi"]=1-(trendTable10$Sen_Probability[trendTable10$Measurement!="Secchi"])
trendTable10$Probabilitymin[trendTable10$Measurement!="Secchi"]=1-(trendTable10$Probabilitymin[trendTable10$Measurement!="Secchi"])
trendTable10$Probabilitymax[trendTable10$Measurement!="Secchi"]=1-(trendTable10$Probabilitymax[trendTable10$Measurement!="Secchi"])
trendTable10$MKProbability[trendTable10$Measurement!="Secchi"]=1-(trendTable10$MKProbability[trendTable10$Measurement!="Secchi"])
trendTable10$Agency=lakesSiteTable$Agency[match(trendTable10$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable10$Region=lakesSiteTable$Region[match(trendTable10$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable10$LFENZID =    lakesSiteTable$LFENZID[match(trendTable10$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable10$LType =    lakesSiteTable$LType[match(trendTable10$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable10$ConfCat <- cut(trendTable10$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable10$ConfCat=factor(trendTable10$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable10$TrendScore=as.numeric(trendTable10$ConfCat)-3
trendTable10$TrendScore[is.na(trendTable10$TrendScore)]<-(NA)
save(trendTable10,file=paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10Year.rData"))
rm(trendTable10)





#5 year trend ####
lakeDatafor5=lakeData%>%filter(Year>=startYear5 & Year <= EndYear & Measurement!="pH")
lakeDatafor5$Season=lakeDatafor5$Month

uclids=unique(lakeDatafor5$uclid)
uMeasures=unique(lakeDatafor5$Measurement)
nMax=length(table(lakeDatafor5$LawaSiteID,lakeDatafor5$Measurement)[table(lakeDatafor5$LawaSiteID,lakeDatafor5$Measurement)>0])

cat('\n',length(uclids),'\n')
u=1
workers <- makeCluster(5)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  
})
foreach(u = u:length(uclids),.combine=rbind,.errorhandling="stop")%dopar%{
  subDat=lakeDatafor5%>%filter(uclid==uclids[u])
  siteTrendTable5=data.frame(LawaSiteID=unique(subDat$LawaSiteID),
                             CouncilSiteID=unique(subDat$CouncilSiteID),
                             Measurement=as.character(uMeasures),nMeasures = NA_integer_,
                             nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                             Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                             nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                             Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                             prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                             AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                             AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                             Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                             ConfCat=NA_real_,period=5,frequency=NA_real_)
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])
    siteTrendTable5$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      SSD_med <- subSubDat%>%
        dplyr::group_by(LawaSiteID,monYear,Year,Month,Qtr)%>%
        dplyr::summarise(Value=quantile(Value,prob=c(0.5),type=5,na.rm=T),
                         myDate=mean(myDate,na.rm=T),
                         Censored=any(Censored),
                         CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
        )%>%ungroup%>%as.data.frame
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season = SSD_med$Month
      siteTrendTable5$nFirstYear[uparam]=length(which(SSD_med$Year==startYear5))
      siteTrendTable5$nLastYear[uparam]=length(which(SSD_med$Year==EndYear))
      siteTrendTable5$numMonths[uparam]=dim(SSD_med)[1]
      siteTrendTable5$numYears[uparam]=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      #For 5 year monthly we want 90% of measures 
      if(siteTrendTable5$numMonths[uparam] >= 0.9*12*5 & siteTrendTable5$numYears[uparam]>=4){
        siteTrendTable5$frequency[uparam]='monthly'
      }else{
        siteTrendTable5$frequency[uparam]=paste0('unassessed, nM=',siteTrendTable5$numMonths[uparam],'(54);',
                                                 ' nY=',siteTrendTable5$numYears[uparam],'(4)')
      }
      if(siteTrendTable5$frequency[uparam]=="monthly"){
        st <- SeasonalityTest(x = SSD_med,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable5[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable5[uparam,names(sk)] <- sk
          siteTrendTable5[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable5[uparam,names(mk)] <- mk
          siteTrendTable5[uparam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(subSubDat)
      rm(SSD_med)
    }
  }
  rm(subDat,uparam)
  return(siteTrendTable5)
}->trendTable5
stopCluster(workers)
rm(workers,uclids,uMeasures,u,lakeDatafor5)
rownames(trendTable5) <- NULL
trendTable5$Sen_Probability[trendTable5$Measurement!="Secchi"]=1-(trendTable5$Sen_Probability[trendTable5$Measurement!="Secchi"])
trendTable5$Probabilitymin[trendTable5$Measurement!="Secchi"]=1-(trendTable5$Probabilitymin[trendTable5$Measurement!="Secchi"])
trendTable5$Probabilitymax[trendTable5$Measurement!="Secchi"]=1-(trendTable5$Probabilitymax[trendTable5$Measurement!="Secchi"])
trendTable5$MKProbability[trendTable5$Measurement!="Secchi"]=1-(trendTable5$MKProbability[trendTable5$Measurement!="Secchi"])
trendTable5$Agency=lakesSiteTable$Agency[match(trendTable5$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable5$Region =    lakesSiteTable$Region[match(trendTable5$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable5$LFENZID =    lakesSiteTable$LFENZID[match(trendTable5$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable5$LType =    lakesSiteTable$LType[match(trendTable5$LawaSiteID,lakesSiteTable$LawaSiteID)]
trendTable5$ConfCat <- cut(trendTable5$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                           labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable5$ConfCat=factor(trendTable5$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable5$TrendScore=as.numeric(trendTable5$ConfCat)-3
trendTable5$TrendScore[is.na(trendTable5$TrendScore)]<-(NA)
save(trendTable5,file=paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend5Year.rData"))
rm(trendTable5)

#Reload, combine and output ####

load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",
              pattern = "Trend15Year.rData",full.names = T,recursive = T),1),verbose =T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose = T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",
              pattern = "Trend5Year.rData",full.names = T,recursive = T),1),verbose = T)


combTrend <- rbind(rbind(trendTable15,trendTable10),trendTable5)%>%
  dplyr::select(LawaSiteID,CouncilSiteID,LFENZID,Agency,Region,LType,
                Measurement,nMeasures,frequency,period,TrendScore,ConfCat,AnalysisNote,AnalysisNoteSS,everything())

write.csv(combTrend,
          paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",
                 format(Sys.Date(),"%Y-%m-%d"),"/LakesWQ_Trend",
                 format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)

rm(combTrend)

#For ITE
#LakeSiteTrend5
trendTable5$TrendScore[is.na(trendTable5$TrendScore)] <- (-99)
trendTable10$TrendScore[is.na(trendTable10$TrendScore)] <- (-99)
trendTable15$TrendScore[is.na(trendTable15$TrendScore)] <- (-99)
write.csv(trendTable5%>%dplyr::transmute(LAWAID=LawaSiteID,
                                         Parameter=Measurement,
                                         Trend5=TrendScore),
          file=paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteTrend5",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
write.csv(trendTable10%>%dplyr::transmute(LAWAID=LawaSiteID,
                                         Parameter=Measurement,
                                         Trend10=TrendScore),
          file=paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteTrend10",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
write.csv(trendTable15%>%dplyr::transmute(LAWAID=LawaSiteID,
                                         Parameter=Measurement,
                                         Trend15=TrendScore),
          file=paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteTrend15",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)


# combTrend <- read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/","LakesWQ_Trend",
# 		recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)

