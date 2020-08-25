rm(list=ls())
library(tidyverse)
library(parallel)
library(doParallel)
source("h:/ericg/16666LAWA/LWPTrends_v1901/LWPTrends_v1901.R")
source("h:/ericg/16666LAWA/LAWA2020/Scripts/LAWAFunctions.R")
source("h:/ericg/16666LAWA/LAWA2020/WaterQuality/scripts/SWQ_state_functions.R")
dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

startTime=Sys.time()
Mode=function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

EndYear <- lubridate::year(Sys.Date())-1
startYear5 <- EndYear - 5+1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1

riverSiteTable=loadLatestSiteTableRiver()

#Load the latest made 
if(!exists('wqdata')){
  wqdataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data",pattern = "AllCouncils.csv",recursive = T,full.names = T),1)
  cat(wqdataFileName)
  wqdata=read_csv(wqdataFileName,guess_max = 100000)%>%as.data.frame
  rm(wqdataFileName)
  # wqdata$SWQLanduse[is.na(wqdata$SWQLanduse)]=riverSiteTable$SWQLanduse[match(wqdata$LawaSiteID[is.na(wqdata$SWQLanduse)],riverSiteTable$LawaSiteID)]
  # wqdata$SWQAltitude[is.na(wqdata$SWQAltitude)]=riverSiteTable$SWQAltitude[match(wqdata$LawaSiteID[is.na(wqdata$SWQAltitude)],riverSiteTable$LawaSiteID)]
  # wqdata$SWQLanduse[tolower(wqdata$SWQLanduse)%in%c("unstated","")] <- NA
  # wqdata$SWQLanduse[tolower(wqdata$SWQLanduse)%in%c("forest","native","exotic","natural")] <- "Forest"
  wqdata$myDate <- as.Date(as.character(wqdata$Date),"%d-%b-%Y")
  wqdata$myDate[wqdata$myDate<as.Date('2000-01-01')] <- as.Date(as.character(wqdata$Date[wqdata$myDate<as.Date('2000-01-01')]),"%d-%b-%y")
  wqdata <- GetMoreDateInfo(wqdata)
  wqdata$monYear = format(wqdata$myDate,"%b-%Y")
  
  
  wqdata$CenType[wqdata$CenType%in%c("Left","L")]='lt'
  wqdata$CenType[wqdata$CenType%in%c("Right","R")]='gt'
  wqdata$CenType[!wqdata$CenType%in%c("lt","gt")]='not'
  
  wqdata$NewValues=wqdata$Value
}

if(0){
#Audit presence of site/measurement combos
  SiteMeasureCombos=apply(expand.grid(unique(wqdata$LawaSiteID),unique(wqdata$Measurement)),1,paste,collapse='][')
  wqdataCombos=unique(paste(wqdata$LawaSiteID,wqdata$Measurement,sep=']['))
  missingCombos=SiteMeasureCombos[!SiteMeasureCombos%in%wqdataCombos]  
  missingMeasures=unlist(strsplit(missingCombos,split = "\\]\\["))[seq(1,728/2)*2]
  missingSites=unlist(strsplit(missingCombos,split = "\\]\\["))[seq(1,728/2)*2-1]
  knitr::kable(table(missingMeasures))
  knitr::kable(table(riverSiteTable$Agency[match(missingSites,riverSiteTable$LawaSiteID)]))
  }


#15 year trend ####
datafor15=wqdata%>%filter(Year>=startYear15 & Year <= EndYear & !Measurement%in%c("PH","TURBFNU"))

usites=unique(datafor15$LawaSiteID)
uMeasures=unique(datafor15$Measurement)
if("TURBFNU"%in%uMeasures){uMeasures=uMeasures[-which(uMeasures=="TURBFNU")]}
cat('\n',length(usites),'\n')
usite=1
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  # library(doBy)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  siteTrendTable15=data.frame(LawaSiteID=usites[usite],Measurement=as.character(uMeasures),nMeasures = NA_integer_,
                              nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                              Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                              nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                              Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                              prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                              AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                              AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                              Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                              ConfCat=NA_real_,period=15,frequency=NA_real_)
  subDat=datafor15%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])
    siteTrendTable15$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      SSD_med <- subSubDat%>%
        dplyr::group_by(LawaSiteID,monYear,Year,Month,Qtr)%>%
        dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                         myDate = mean(myDate,na.rm=T),
                         Censored = any(Censored),
                         CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
        )%>%ungroup%>%as.data.frame
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season = factor(SSD_med$Month)
      siteTrendTable15$nFirstYear[uparam]=length(which(SSD_med$Year==startYear15))
      siteTrendTable15$nLastYear[uparam]=length(which(SSD_med$Year==EndYear))
      siteTrendTable15$numMonths[uparam]=dim(SSD_med)[1]
      siteTrendTable15$numYears[uparam]=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      
      #For 15 year monthly we want 90% of measures and 90% of years
      if(siteTrendTable15$numMonths[uparam] >= 0.9*12*15 & siteTrendTable15$numYears[uparam]>=13){ #162
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
          siteTrendTable15$frequency[uparam]='unassessed'
        }
      }
      if(siteTrendTable15$frequency[uparam]!='unassessed'){
        SeasonString <- sort(unique(SSD_med$Season))
        st <- SeasonalityTest(x = SSD_med,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable15[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable15[uparam,names(sk)] <- sk
          siteTrendTable15[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
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
rm(workers,usites,uMeasures,usite,datafor15)
cat(Sys.time()-startTime) #Jun23 2.41

rownames(trendTable15) <- NULL
trendTable15$Sen_Probability[trendTable15$Measurement!="BDISC"]=1-(trendTable15$Sen_Probability[trendTable15$Measurement!="BDISC"])
trendTable15$Probabilitymin[trendTable15$Measurement!="BDISC"]=1-(trendTable15$Probabilitymin[trendTable15$Measurement!="BDISC"])
trendTable15$Probabilitymax[trendTable15$Measurement!="BDISC"]=1-(trendTable15$Probabilitymax[trendTable15$Measurement!="BDISC"])
trendTable15$MKProbability[trendTable15$Measurement!="BDISC"]=1-(trendTable15$MKProbability[trendTable15$Measurement!="BDISC"])
trendTable15$Agency=riverSiteTable$Agency[match(trendTable15$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable15$SWQAltitude =  riverSiteTable$SWQAltitude[match(trendTable15$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable15$SWQLanduse =   riverSiteTable$SWQLanduse[match(trendTable15$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable15$Region =    riverSiteTable$Region[match(trendTable15$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable15$ConfCat <- cut(trendTable15$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable15$ConfCat=factor(trendTable15$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable15$TrendScore=as.numeric(trendTable15$ConfCat)-3
trendTable15$TrendScore[is.na(trendTable15$TrendScore)]<-(NA)
save(trendTable15,file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend15Year.rData"))
rm(trendTable15)



#10 year trend ####
datafor10=wqdata%>%filter(Year>=startYear10 & Year <= EndYear & !Measurement%in%c("PH","TURBFNU"))%>%drop_na(LawaSiteID)

usites=unique(datafor10$LawaSiteID)
uMeasures=unique(datafor10$Measurement)
cat('\n',length(usites),'\n')
usite=1
library(parallel)
library(doParallel)
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  siteTrendTable10=data.frame(LawaSiteID=usites[usite],Measurement=as.character(uMeasures),nMeasures = NA_integer_,
                              nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                              Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                              nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                              Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                              prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                              AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                              AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                              Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                              ConfCat=NA_real_,period=10,frequency=NA_real_)
  subDat=datafor10%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])
    siteTrendTable10$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      SSD_med <- subSubDat%>%
        dplyr::group_by(LawaSiteID,monYear,Year,Month,Qtr)%>%
        dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                         myDate = mean(myDate,na.rm=T),
                         Censored = any(Censored),
                         CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
        )%>%ungroup%>%as.data.frame
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season = SSD_med$Month
      siteTrendTable10$nFirstYear[uparam]=length(which(SSD_med$Year==startYear10))
      siteTrendTable10$nLastYear[uparam]=length(which(SSD_med$Year==EndYear))
      siteTrendTable10$numMonths[uparam]=dim(SSD_med)[1]
      siteTrendTable10$numYears[uparam]=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      
      #For 10 year monthly we want 90% of measures (108) and 90% of years
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
        #or 36 quarters
        if(siteTrendTable10$numQuarters[uparam] >= 0.9*4*10 & siteTrendTable10$numYears[uparam] >=9){
          siteTrendTable10$frequency[uparam]='quarterly'
        }else{
          siteTrendTable10$frequency[uparam]='unassessed'
        }
      }
      if(siteTrendTable10$frequency[uparam]!='unassessed'){
        st <- SeasonalityTest(x = SSD_med,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable10[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          sk$AnalysisNote=as.character(sk$AnalysisNote)
          sss$AnalysisNoteSS=as.character(sss$AnalysisNoteSS)
          siteTrendTable10[uparam,names(sk)] <- sk
          siteTrendTable10[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          mk$AnalysisNote=as.character(mk$AnalysisNote)
          ss$AnalysisNoteSS=as.character(ss$AnalysisNoteSS)
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
rm(workers,usites,uMeasures,usite,datafor10)
cat(Sys.time()-startTime) #2.48

rownames(trendTable10) <- NULL
trendTable10$Sen_Probability[trendTable10$Measurement!="BDISC"]=1-(trendTable10$Sen_Probability[trendTable10$Measurement!="BDISC"])
trendTable10$Probabilitymin[trendTable10$Measurement!="BDISC"]=1-(trendTable10$Probabilitymin[trendTable10$Measurement!="BDISC"])
trendTable10$Probabilitymax[trendTable10$Measurement!="BDISC"]=1-(trendTable10$Probabilitymax[trendTable10$Measurement!="BDISC"])
trendTable10$MKProbability[trendTable10$Measurement!="BDISC"]=1-(trendTable10$MKProbability[trendTable10$Measurement!="BDISC"])
trendTable10$Agency=riverSiteTable$Agency[match(trendTable10$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable10$SWQAltitude =  riverSiteTable$SWQAltitude[match(trendTable10$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable10$SWQLanduse =   riverSiteTable$SWQLanduse[match(trendTable10$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable10$Region =    riverSiteTable$Region[match(trendTable10$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable10$ConfCat <- cut(trendTable10$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable10$ConfCat=factor(trendTable10$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable10$TrendScore=as.numeric(trendTable10$ConfCat)-3
trendTable10$TrendScore[is.na(trendTable10$TrendScore)]<-(NA)
save(trendTable10,file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10Year.rData"))
rm(trendTable10)


#5 year trend ####
datafor5=wqdata%>%filter(Year>=startYear5 & Year <= EndYear & !Measurement%in%c("PH","TURBFNU"))

usites=unique(datafor5$LawaSiteID)
uMeasures=unique(datafor5$Measurement)
cat('\n',length(usites),'\n')
usite=1
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  siteTrendTable5=data.frame(LawaSiteID=usites[usite],Measurement=as.character(uMeasures),nMeasures = NA_integer_,
                             nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                             Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                             nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                             Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                             prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                             AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                             AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                             Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                             ConfCat=NA_real_,period=5,frequency=NA_real_)
  subDat=datafor5%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])
    siteTrendTable5$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      SSD_med <- subSubDat%>%
        dplyr::group_by(LawaSiteID,monYear,Year,Month,Qtr)%>%
        dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                         myDate = mean(myDate,na.rm=T),
                         Censored = any(Censored),
                         CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
        )%>%ungroup%>%as.data.frame
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season = SSD_med$Month
      siteTrendTable5$nFirstYear[uparam]=length(which(SSD_med$Year==startYear5))
      siteTrendTable5$nLastYear[uparam]=length(which(SSD_med$Year==EndYear))
      siteTrendTable5$numMonths[uparam]=dim(SSD_med)[1]
      siteTrendTable5$numYears[uparam]=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      
      #For 5 year monthly we want 90% of measures 
      if(siteTrendTable5$numMonths[uparam] >= 0.9*12*5){
        siteTrendTable5$frequency[uparam]='monthly'
      }else{
        siteTrendTable5$frequency[uparam]='unassessed'
      }
      if(siteTrendTable5$frequency[uparam]!='unassessed'){
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
}-> trendTable5
stopCluster(workers)
rm(workers,usites,uMeasures,usite,datafor5)
cat(Sys.time()-startTime) #23Jun 52.5

rownames(trendTable5) <- NULL
trendTable5$Sen_Probability[trendTable5$Measurement!="BDISC"]=1-(trendTable5$Sen_Probability[trendTable5$Measurement!="BDISC"])
trendTable5$Probabilitymin[trendTable5$Measurement!="BDISC"]=1-(trendTable5$Probabilitymin[trendTable5$Measurement!="BDISC"])
trendTable5$Probabilitymax[trendTable5$Measurement!="BDISC"]=1-(trendTable5$Probabilitymax[trendTable5$Measurement!="BDISC"])
trendTable5$MKProbability[trendTable5$Measurement!="BDISC"]=1-(trendTable5$MKProbability[trendTable5$Measurement!="BDISC"])
trendTable5$Agency=riverSiteTable$Agency[match(trendTable5$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable5$SWQAltitude =  riverSiteTable$SWQAltitude[match(trendTable5$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable5$SWQLanduse =   riverSiteTable$SWQLanduse[match(trendTable5$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable5$Region =    riverSiteTable$Region[match(trendTable5$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable5$ConfCat <- cut(trendTable5$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                           labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable5$ConfCat=factor(trendTable5$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable5$TrendScore=as.numeric(trendTable5$ConfCat)-3
trendTable5$TrendScore[is.na(trendTable5$TrendScore)]<-(NA)
save(trendTable5,file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend5Year.rData"))
rm(trendTable5)


riverSiteTable=loadLatestSiteTableRiver()
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
              pattern = "Trend15Year.rData",full.names = T,recursive = T),1),verbose =T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose = T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
              pattern = "Trend5Year.rData",full.names = T,recursive = T),1),verbose = T)


#number parameters per agency with trends, on average per site
table(trendTable15$Agency)/table(riverSiteTable$Agency)[match(names(table(trendTable15$Agency)),names(table(riverSiteTable$Agency)))]
table(trendTable10$Agency)/table(riverSiteTable$Agency)[match(names(table(trendTable10$Agency)),names(table(riverSiteTable$Agency)))]
table(trendTable5$Agency)/table(riverSiteTable$Agency)[match(names(table(trendTable5$Agency)),names(table(riverSiteTable$Agency)))]

trendTable5%>%group_by(Agency)%>%dplyr::select(Agency,frequency)%>%table
trendTable10%>%group_by(Agency)%>%dplyr::select(Agency,frequency)%>%table
trendTable15%>%group_by(Agency)%>%dplyr::select(Agency,frequency)%>%table

MCItrend=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis",
                           pattern='MacroMCI_Trend',full.names = T,recursive = T,ignore.case = T),1),stringsAsFactors = F)
MCItrend%>%group_by(Agency)%>%dplyr::select(Agency,frequency)%>%table

# #Remove MDC DRP and ECOLI
# #See email Steffi Henkel 14/9/2018 to Kati Doehring, Eric Goodwin, Abi Loughnan and Peter Hamill
if(any(trendTable10$Agency=="mdc" & trendTable10$Measurement=="DRP")){
  trendTable10 <- trendTable10[-which(trendTable10$Agency=='mdc'& trendTable10$Measurement=="DRP"),]
  # 7872 to 7840
}
if(any(trendTable10$Agency=="mdc" & trendTable10$Measurement=="ECOLI")){
  trendTable10 <- trendTable10[-which(trendTable10$Agency=='mdc'& trendTable10$Measurement=="ECOLI"),]
  #7840 to 7808
}
if(any(trendTable15$Agency=="mdc" & trendTable15$Measurement=="DRP")){
  trendTable15 <- trendTable15[-which(trendTable15$Agency=='mdc'& trendTable15$Measurement=="DRP"),]
  # 7872 to 7840
}
if(any(trendTable15$Agency=="mdc" & trendTable15$Measurement=="ECOLI")){
  trendTable15 <- trendTable15[-which(trendTable15$Agency=='mdc'& trendTable15$Measurement=="ECOLI"),]
  #7840 to 7808
}


#Combine WQ trends
combTrend <- rbind(rbind(trendTable15,trendTable10),trendTable5)
combTrend$CouncilSiteID = riverSiteTable$CouncilSiteID[match(tolower(gsub('_NIWA','',combTrend$LawaSiteID)),tolower(riverSiteTable$LawaSiteID))]
#19840 23Jun
#24664 21Aug

#Save for ITE
combTrend$SWQAltitude=pseudo.titlecase(combTrend$SWQAltitude)
combTrend$SWQLanduse=pseudo.titlecase(combTrend$SWQLanduse)
combTrend$TrendScore[is.na(combTrend$TrendScore)] <- -99
write.csv(combTrend%>%transmute(LAWAID=LawaSiteID,
                                Parameter=Measurement,
                                Altitude=SWQAltitude,
                                Landuse=SWQLanduse,
                                TrendScore=TrendScore,
                                TrendFrequency=frequency,
                                TrendPeriod=period),
          file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITERiverTrend",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)



#Add MCI trend and save ####
MCItrend=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis",
                           pattern='MacroMCI_Trend',full.names = T,recursive = T,ignore.case = T),1),stringsAsFactors = F)
MCItrend$TrendScore[is.na(MCItrend$TrendScore)] <- -99
combTrend <- rbind(combTrend%>%
                     dplyr::select(LawaSiteID,CouncilSiteID,Agency,Region,Measurement,
                                   nMeasures,frequency,period,TrendScore,ConfCat,AnalysisNote,AnalysisNoteSS,everything()),
                   MCItrend%>%
                     dplyr::filter(period>=10)%>%
                     dplyr::select(LawaSiteID,CouncilSiteID,Agency,Region,Measurement,
                                   nMeasures,frequency,period,TrendScore,ConfCat,AnalysisNote,AnalysisNoteSS,everything()))
table(combTrend$frequency,combTrend$period)

write.csv(combTrend,
          paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                 "/RiverWQ_Trend",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)

# combTrend=read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/","RiverWQ_Trend",	recursive = T,full.names = T,ignore.case=T),1),stringsAsFactors = F)

if(0){
#Audit trend attrition
  trendTable15$AnalysisNote[trendTable15$numMonths<163&trendTable15$numQuarters<54] <- '<54 quarters & <162 months'
  trendTable15$AnalysisNote[trendTable15$nMeasures==0] <- 'no data'
  trendTable10$AnalysisNote[trendTable10$numMonths<108&trendTable10$numQuarters<36] <- '<36 quarters & <108 months'
  trendTable10$AnalysisNote[trendTable10$nMeasures==0] <- 'no data'
  trendTable5$AnalysisNote[trendTable5$numMonths<54] <- '<54 months'
  trendTable5$AnalysisNote[trendTable5$nMeasures==0] <- 'no data'
  table(trendTable15$AnalysisNote[trendTable15$frequency=='unassessed'])
  table(trendTable15$frequency,is.na(trendTable15$TrendScore))
  table(trendTable10$frequency,is.na(trendTable10$TrendScore))
  table(trendTable5$frequency,is.na(trendTable5$TrendScore))
  knitr::kable(table(trendTable15$AnalysisNote[is.na(trendTable15$TrendScore)&trendTable15$frequency!='unassessed']),format='rst')
  knitr::kable(table(trendTable10$AnalysisNote[is.na(trendTable10$TrendScore)&trendTable10$frequency!='unassessed']),format='rst')
  knitr::kable(table(trendTable5$AnalysisNote[is.na(trendTable5$TrendScore)&trendTable5$frequency!='unassessed']),format='rst')
}


#Refresh-document plots ####
# wqdata$myDate <- as.Date(as.character(wqdata$Date),"%d-%b-%Y")
# wqdata$myDate[wqdata$myDate<as.Date('2000-01-01')] <- as.Date(as.character(wqdata$Date[wqdata$myDate<as.Date('2000-01-01')]),"%d-%b-%y")
# wqdata <- GetMoreDateInfo(wqdata)
# wqdata$monYear = format(wqdata$myDate,"%b-%Y")
wqdata$Season=wqdata$Month
wqdata$Year = lubridate::year(lubridate::dmy(wqdata$Date))
SeasonString=sort(unique(wqdata$Season))
savePlott=T
usites=unique(combTrend$LawaSiteID)
uMeasures=unique(wqdata$Measurement)%>%as.character
#Exclude pH, becuas we dont do pH trends
if("PH"%in%uMeasures){
  uMeasures=uMeasures[-which(uMeasures=='PH')]
}
for(uparam in seq_along(uMeasures)){
  subwq=wqdata[wqdata$Measurement==uMeasures[uparam],]
  eval(parse(text=paste0('names(subwq)[which(names(subwq)=="Value")]="',uMeasures[uparam],'"')))
  subTrend=trendTable10[which(trendTable10$Measurement==uMeasures[uparam]&trendTable10$frequency=='monthly'),]
  worstDeg <- which.max(subTrend$MKProbability) 
  bestImp <- which.min(subTrend$MKProbability)
  leastKnown <- which.min(abs(subTrend$MKProbability-0.5))
  if(savePlott){
    tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/BestWorst",uMeasures[uparam],".tif"),
         width = 8,height=12,units='in',res=300,compression='lzw',type='cairo')
  }else{
    windows()
  }
  par(mfrow=c(3,1),mar=c(2,4,1,2))
  theseDeg <- which(subwq$LawaSiteID==subTrend$LawaSiteID[worstDeg] & subwq$Year>=startYear10)
  theseInd <- which(subwq$LawaSiteID==subTrend$LawaSiteID[leastKnown] & subwq$Year>=startYear10)
  theseImp <- which(subwq$LawaSiteID==subTrend$LawaSiteID[bestImp] & subwq$Year>=startYear10)
  if(length(theseDeg)>0){
    Deg_med <- eval(parse(text=paste0("summaryBy(formula=",uMeasures[uparam],"~LawaSiteID+monYear,
                         id=~Censored+CenType+myDate+Year+Month+Qtr+Season,
                         data=subwq[theseDeg,], 
                         FUN=quantile, prob=c(0.5), type=5, na.rm=TRUE, keep.name=TRUE)")))
  }
  if(length(theseInd)>0){
    Ind_med <- eval(parse(text=paste0("summaryBy(formula=",uMeasures[uparam],"~LawaSiteID+monYear,
                         id=~Censored+CenType+myDate+Year+Month+Qtr+Season,
                                      data=subwq[theseInd,], 
                                      FUN=quantile, prob=c(0.5), type=5, na.rm=TRUE, keep.name=TRUE)")))
  }
  if(length(theseImp)>0){
    Imp_med <- eval(parse(text=paste0("summaryBy(formula=",uMeasures[uparam],"~LawaSiteID+monYear,
                         id=~Censored+CenType+myDate+Year+Month+Qtr+Season,
                                      data=subwq[theseImp,], 
                                      FUN=quantile, prob=c(0.5), type=5, na.rm=TRUE, keep.name=TRUE)")))
  }
  
  if(length(theseDeg)>0){
    st <- SeasonalityTest(x = Deg_med,main=uMeasures[uparam],ValuesToUse = uMeasures[uparam],do.plot =F)
    if(!is.na(st$pvalue)&&st$pvalue<0.05){
      SeasonalKendall(x = Deg_med,ValuesToUse = uMeasures[uparam],doPlot = F)
      SeasonalSenSlope(HiCensor=T,x = Deg_med,ValuesToUse = uMeasures[uparam],ValuesToUseforMedian = uMeasures[uparam],doPlot = T,mymain = subTrend$LawaSiteID[worstDeg])
    }else{
      MannKendall(HiCensor=T,x = Deg_med,ValuesToUse = uMeasures[uparam],doPlot=F)
      SenSlope(HiCensor=T,x = Deg_med,ValuesToUse = uMeasures[uparam],ValuesToUseforMedian = uMeasures[uparam],doPlot = T,mymain = subTrend$LawaSiteID[worstDeg])
    }
  }
  if(length(theseInd)>0){
    st <- SeasonalityTest(x = Ind_med,main=uMeasures[uparam],ValuesToUse = uMeasures[uparam],do.plot =F)
    if(!is.na(st$pvalue)&&st$pvalue<0.05){
      SeasonalKendall(HiCensor=T,x = Ind_med,ValuesToUse = uMeasures[uparam],doPlot = F)
      SeasonalSenSlope(HiCensor=T,x = Ind_med,ValuesToUse = uMeasures[uparam],ValuesToUseforMedian = uMeasures[uparam],doPlot = T,mymain = subTrend$LawaSiteID[leastKnown])
    }else{
      MannKendall(HiCensor=T,x = Ind_med,ValuesToUse = uMeasures[uparam],doPlot=F)
      SenSlope(HiCensor=T,x = Ind_med,ValuesToUse = uMeasures[uparam],ValuesToUseforMedian = uMeasures[uparam],doPlot = T,mymain = subTrend$LawaSiteID[leastKnown])
    }
  }
  if(length(theseImp)>0){
    st <- SeasonalityTest(x = Imp_med,main=uMeasures[uparam],ValuesToUse = uMeasures[uparam],do.plot =F)
    if(!is.na(st$pvalue)&&st$pvalue<0.05){
      SeasonalKendall(HiCensor=T,x = Imp_med,ValuesToUse = uMeasures[uparam],doPlot = F)
      if(is.na(SeasonalSenSlope(HiCensor=T,x = Imp_med,ValuesToUse = uMeasures[uparam],ValuesToUseforMedian = uMeasures[uparam],doPlot = T,mymain = subTrend$LawaSiteID[bestImp])$Sen_Probability)){
        SenSlope(HiCensor=T,x = Imp_med,ValuesToUse = uMeasures[uparam],ValuesToUseforMedian = uMeasures[uparam],doPlot = T,mymain = subTrend$LawaSiteID[bestImp])
      }
    }else{
      MannKendall(HiCensor=T,x = Imp_med,ValuesToUse = uMeasures[uparam],doPlot=F)
      SenSlope(HiCensor=T,x = Imp_med,ValuesToUse = uMeasures[uparam],ValuesToUseforMedian = uMeasures[uparam],doPlot = T,mymain = subTrend$LawaSiteID[bestImp])
    }
  }
  if(names(dev.cur())=='tiff'){dev.off()}
  rm(theseDeg,theseImp,theseInd)
  rm(Deg_med,Ind_med,Imp_med)
  rm(worstDeg,bestImp,leastKnown)
}
rm(uparam,uMeasures)

#Plot ten year trends ####

TrendsForPlotting = combTrend%>%
  dplyr::filter(period==10&(frequency=='monthly'|Measurement=='MCI'))#%>%
# dplyr::select(LawaSiteID,Measurement,Region,TrendScore)
TrendsForPlotting$TrendScore[TrendsForPlotting$TrendScore==-99] <- NA
table(TrendsForPlotting$Measurement[which(is.na(TrendsForPlotting$TrendScore))]) ->naTab
# BDISC   DRP ECOLI   NH4    TN   TON    TP  TURB   MCI 
#     2    36    16   105    12    30    11     5   489 
#Drop the NAs
TrendsForPlotting = TrendsForPlotting%>%tidyr::drop_na(TrendScore)
#4450 to 3940
#Starts as DRP   NH4   TP    TON   TURB  ECOLI TN    BDISC  MCI
#labelled  DRP   NH4   TP    TON   TURB  ECOLI TN    CLAR   MCI
#Reorder 2 CLAR TURB   TN    TON   NH4   TP    DRP   ECOLI  MCI
TrendsForPlotting$Measurement <- factor(TrendsForPlotting$Measurement,
                                        levels=c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")) #,"MCI"

#In LWPTrends MannKendall()
# KendalTest$MKProbability<-1-0.5*KendalTest$p
# KendalTest$MKProbability[which(KendalTest$S>0)]<-0.5*KendalTest$p[which(KendalTest$S>0)]
if(0){
tfp=TrendsForPlotting
  lawaMacroState5yr = read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/',pattern='MACRO_STATE_ForITE|MacroState',recursive = T,full.names = T),1),stringsAsFactors = F)
  lawaMacroState5yr = lawaMacroState5yr%>%filter(Parameter=="MCI")%>%dplyr::rename(LawaSiteID=LAWAID,Measurement=Parameter,Q50=Median)
  sa <- read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",pattern=paste0("^sa",startYear5,"-",EndYear,".csv"),recursive=T,full.names=T,ignore.case=T),1),stringsAsFactors = F)
sa <- sa%>%filter(Scope=="Site")%>%select(LawaSiteID,Measurement,Q50)
sa <- rbind(sa,lawaMacroState5yr)
tfp <- merge(tfp,sa)
rm(sa,lawaMacroState5yr)

par(mfrow=c(1,1))
stdise <- function(x){(x-min(x,na.rm=T))/(max(x-min(x,na.rm=T),na.rm=T))}
BDISCdens=with(tfp%>%filter(Measurement=="BDISC"),density(MKProbability,na.rm=T,from=0,to=1))
TURBdens=with(tfp%>%filter(Measurement=="TURB"),density(MKProbability,na.rm=T,from=0,to=1))
TNdens=with(tfp%>%filter(Measurement=="TN"),density(MKProbability,na.rm=T,from=0,to=1))
TONdens=with(tfp%>%filter(Measurement=="TON"),density(MKProbability,na.rm=T,from=0,to=1))
NH4dens=with(tfp%>%filter(Measurement=="NH4"),density(MKProbability,na.rm=T,from=0,to=1))
TPdens=with(tfp%>%filter(Measurement=="TP"),density(MKProbability,na.rm=T,from=0,to=1))
DRPdens=with(tfp%>%filter(Measurement=="DRP"),density(MKProbability,na.rm=T,from=0,to=1))
ECOLIdens=with(tfp%>%filter(Measurement=="ECOLI"),density(MKProbability,na.rm=T,from=0,to=1))
MCIdens=with(tfp%>%filter(Measurement=="MCI"),density(MKProbability,na.rm=T,from=0,to=1))
plot(stdise(BDISCdens$y),BDISCdens$x,type='l',xlim=c(0,9),xlab='',xaxt='n',col='blue',lwd=2)
lines(stdise(TURBdens$y)+1,TURBdens$x,col='blue',lwd=2)
lines(stdise(TNdens$y)+2,TNdens$x,col='blue',lwd=2)
lines(stdise(TONdens$y)+3,TONdens$x,col='blue',lwd=2)
lines(stdise(NH4dens$y)+4,NH4dens$x,col='blue',lwd=2)
lines(stdise(TPdens$y)+5,TPdens$x,col='blue',lwd=2)
lines(stdise(DRPdens$y)+6,DRPdens$x,col='blue',lwd=2)
lines(stdise(ECOLIdens$y)+7,ECOLIdens$x,col='blue',lwd=2)
lines(stdise(MCIdens$y)+8,MCIdens$x,col='blue',lwd=2)
abline(v=c(0:9),lty=2)
axis(side = 1,at=colMPs-0.5,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                   'DRP',expression(italic(E.~coli)),'MCI'),las=2)
BDISCdens=with(tfp%>%filter(Measurement=="BDISC"),density(p,na.rm=T,from=0,to=1))
TURBdens=with(tfp%>%filter(Measurement=="TURB"),density(p,na.rm=T,from=0,to=1))
TNdens=with(tfp%>%filter(Measurement=="TN"),density(p,na.rm=T,from=0,to=1))
TONdens=with(tfp%>%filter(Measurement=="TON"),density(p,na.rm=T,from=0,to=1))
NH4dens=with(tfp%>%filter(Measurement=="NH4"),density(p,na.rm=T,from=0,to=1))
TPdens=with(tfp%>%filter(Measurement=="TP"),density(p,na.rm=T,from=0,to=1))
DRPdens=with(tfp%>%filter(Measurement=="DRP"),density(p,na.rm=T,from=0,to=1))
ECOLIdens=with(tfp%>%filter(Measurement=="ECOLI"),density(p,na.rm=T,from=0,to=1))
MCIdens=with(tfp%>%filter(Measurement=="MCI"),density(p,na.rm=T,from=0,to=1))
lines(stdise(BDISCdens$y),BDISCdens$x,lwd=2)
lines(stdise(TURBdens$y)+1,TURBdens$x,lwd=2)
lines(stdise(TNdens$y)+2,TNdens$x,lwd=2)
lines(stdise(TONdens$y)+3,TONdens$x,lwd=2)
lines(stdise(NH4dens$y)+4,NH4dens$x,lwd=2)
lines(stdise(TPdens$y)+5,TPdens$x,lwd=2)
lines(stdise(DRPdens$y)+6,DRPdens$x,lwd=2)
lines(stdise(ECOLIdens$y)+7,ECOLIdens$x,lwd=2)
lines(stdise(MCIdens$y)+8,MCIdens$x,lwd=2)


par(mfrow=c(1,1),mar=c(5,4,4,2),xpd=F)
plot(stdise(BDISCdens$y)+0.2,rev(BDISCdens$x),lwd=2,xlim=c(0,11),type='l')
lines(stdise(TURBdens$y)+1.4,rev(TURBdens$x),lwd=2)
lines(stdise(TNdens$y)+2.6,rev(TNdens$x),lwd=2)
lines(stdise(TONdens$y)+3.8,rev(TONdens$x),lwd=2)
lines(stdise(NH4dens$y)+5.0,rev(NH4dens$x),lwd=2)
lines(stdise(TPdens$y)+6.2,rev(TPdens$x),lwd=2)
lines(stdise(DRPdens$y)+7.4,rev(DRPdens$x),lwd=2)
lines(stdise(ECOLIdens$y)+8.6,rev(ECOLIdens$x),lwd=2)
lines(stdise(MCIdens$y)+9.8,rev(MCIdens$x),lwd=2)
abline(h=c(0.1,0.33,0.67,0.90),lty=2)


tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendStateBDISC.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
with(tfp%>%filter(Measurement=="BDISC"),{par(mfrow=c(2,1),cex.axis=0.75)
  boxplot(Q50~ConfCat,xlim=c(5.25,0.75),varwidth=T,log='y',main="BDISC",ylab='Median (Q50)')
  plot(p,Q50,log='y')
  abline(v=c(0.1,0.33,0.67,0.90),lty=2)
})
if(names(dev.cur())=='tiff')dev.off()
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendStateTURB.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
with(tfp%>%filter(Measurement=="TURB"),{par(mfrow=c(2,1),cex.axis=0.75)
  boxplot(Q50~ConfCat,xlim=c(5.25,0.75),varwidth=T,log='y',main='TURB',ylab='Median (Q50)')
  plot(p,Q50,log='y');abline(v=c(0.1,0.33,0.67,0.90),lty=2)
})
if(names(dev.cur())=='tiff')dev.off()
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendStateTN.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
with(tfp%>%filter(Measurement=="TN"),{par(mfrow=c(2,1),cex.axis=0.75)
  boxplot(Q50~ConfCat,xlim=c(5.25,0.75),varwidth=T,log='y',main="TN",ylab='Median (Q50)')
  plot(p,Q50,log='y');abline(v=c(0.1,0.33,0.67,0.90),lty=2)
})
if(names(dev.cur())=='tiff')dev.off()
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendStateTON.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
with(tfp%>%filter(Measurement=="TON"),{par(mfrow=c(2,1),cex.axis=0.75)
  boxplot(Q50~ConfCat,xlim=c(5.25,0.75),varwidth=T,log='y',main="TON",ylab='Median (Q50)')
  plot(p,Q50,log='y');abline(v=c(0.1,0.33,0.67,0.90),lty=2)
})
if(names(dev.cur())=='tiff')dev.off()
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendStateNH4.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
with(tfp%>%filter(Measurement=="NH4"),{par(mfrow=c(2,1),cex.axis=0.75)
  boxplot(Q50~ConfCat,xlim=c(5.25,0.75),varwidth=T,log='y',main="NH4",ylab='Median (Q50)')
  plot(p,Q50,log='y');abline(v=c(0.1,0.33,0.67,0.90),lty=2)
})
if(names(dev.cur())=='tiff')dev.off()
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendStateTP.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
with(tfp%>%filter(Measurement=="TP"),{par(mfrow=c(2,1),cex.axis=0.75)
  boxplot(Q50~ConfCat,xlim=c(5.25,0.75),varwidth=T,log='y',main="TP",ylab='Median (Q50)')
  plot(p,Q50,log='y');abline(v=c(0.1,0.33,0.67,0.90),lty=2)
})
if(names(dev.cur())=='tiff')dev.off()
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendStateDRP.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
with(tfp%>%filter(Measurement=="DRP"),{par(mfrow=c(2,1),cex.axis=0.75)
  boxplot(Q50~ConfCat,xlim=c(5.25,0.75),varwidth=T,log='y',main="DRP",ylab='Median (Q50)')
  plot(p,Q50,log='y');abline(v=c(0.1,0.33,0.67,0.90),lty=2)
})
if(names(dev.cur())=='tiff')dev.off()
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendStateECOLI.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
with(tfp%>%filter(Measurement=="ECOLI"),{par(mfrow=c(2,1),cex.axis=0.75)
  boxplot(Q50~ConfCat,xlim=c(5.25,0.75),varwidth=T,log='y',main="ECOLI",ylab='Median (Q50)')
  plot(p,Q50,log='y');abline(v=c(0.1,0.33,0.67,0.90),lty=2)
})
if(names(dev.cur())=='tiff')dev.off()
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendStateMCI.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
with(tfp%>%filter(Measurement=="MCI"),{par(mfrow=c(2,1),cex.axis=0.75)
  boxplot(Q50~ConfCat,xlim=c(5.25,0.75),varwidth=T,main="MCI",ylab='Median (Q50)')
  plot(p,Q50);abline(v=c(0.1,0.33,0.67,0.90),lty=2)
})
if(names(dev.cur())=='tiff')dev.off()
}

#Make the coloured plot
par(mfrow=c(1,1))
colMPs=-0.5+(1:9)*1.2
tb <- plot(factor(TrendsForPlotting$Measurement),
           factor(TrendsForPlotting$TrendScore),
           col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"), #"#dddddd",
           main="Ten year trends")
tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
mbp <- rbind(rep(0,9),mbp)
mbp = (mbp[-1,]+mbp[-6,])/2

# TenYearMonthlyTrendsClean.tif ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TenYearMonthlyTrendsClean.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),mar=c(5,10,6,2))
barplot(tbp,main="Ten year monthly trends",las=2,
        col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                   'DRP',expression(italic(E.~coli)),'MCI'),las=2)
axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 1,lwd = 0,lwd.ticks = 0)
axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 0,lwd.ticks = 1,line=-1)
par(xpd=NA)
for(cc in 1:9){
  text(colMPs[cc],1.025,sum(tb[cc,]))
}
if(names(dev.cur())=='tiff'){dev.off()}

# TenYearMonthlyTrendsNumbres.tif ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TenYearMonthlyTrendsNumbres.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),mar=c(5,10,6,2))
barplot(tbp,main="Ten year monthly trends",las=2,
        col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                   'DRP',expression(italic(E.~coli)),'MCI'),las=2)
axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 1,lwd = 0,lwd.ticks = 0)
axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 0,lwd.ticks = 1,line=-1)
par(xpd=NA)
for(cc in 1:9){
  text(rep(colMPs[cc],6),mbp[,cc],tb[cc,])
  text(colMPs[cc],1.025,sum(tb[cc,]))
}
if(names(dev.cur())=='tiff'){dev.off()}

# TenYearMonthlyTrendsPercentage.tif ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TenYearMonthlyTrendsPercentage.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),mar=c(5,10,6,2))
barplot(tbp,main="Ten year monthly trends",las=2,
        col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                   'DRP',expression(italic(E.~coli)),'MCI'),las=2)
axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
par(xpd=NA)
for(cc in 1:9){
  text(rep(colMPs[cc],6),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
  text(colMPs[cc],1.025,sum(tb[cc,]))
}
if(names(dev.cur())=='tiff'){dev.off()}



#Plot fifteen year trends ####

TrendsForPlotting = combTrend%>%
  dplyr::filter(period==15)#%>%
# dplyr::select(LawaSiteID,Measurement,Region,TrendScore)
TrendsForPlotting$TrendScore[TrendsForPlotting$TrendScore==-99] <- NA

table(TrendsForPlotting$Measurement[which(is.na(TrendsForPlotting$TrendScore))]) ->naTab
# BDISC   DRP ECOLI   NH4    TN   TON    TP  TURB   MCI 
#   678   532   483   595   631   645   599   503   544
#Drop the  NAs
TrendsForPlotting = TrendsForPlotting%>%drop_na(TrendScore)
#8771 to 3561
#Starts as DRP   NH4   TP    TON   TURB  ECOLI TN    BDISC  MCI
#labelled  DRP   NH4   TP    TON   TURB  ECOLI TN    CLAR   MCI
#Reorder 2 CLAR TURB   TN    TON   NH4   TP    DRP   ECOLI  MCI
TrendsForPlotting$Measurement <- factor(TrendsForPlotting$Measurement,
                                        levels=c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")) #,"MCI"

#Make the coloured plot
par(mfrow=c(1,1))
colMPs=-0.5+(1:9)*1.2
tb <- plot(factor(TrendsForPlotting$Measurement),
           factor(TrendsForPlotting$TrendScore),
           col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF"), #"#dddddd",
           main="Ten year trends")
tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
mbp <- rbind(rep(0,9),mbp)
mbp = (mbp[-1,]+mbp[-6,])/2

#FifteenYearTrendsClean.tif ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/FifteenYearTrendsClean.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),mar=c(5,10,6,2))
barplot(tbp,main="Fifteen year trends",las=2,
        col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                   'DRP',expression(italic(E.~coli)),'MCI'),las=2)
axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 1,lwd = 0,lwd.ticks = 0)
axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 0,lwd.ticks = 1,line=-1)
par(xpd=NA)
for(cc in 1:9){
  text(rep(colMPs[cc],6),mbp[,cc],tb[cc,])
  text(colMPs[cc],1.025,sum(tb[cc,]))
}
if(names(dev.cur())=='tiff'){dev.off()}

#FifteenYearTrendsNumbres.tif ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/FifteenYearTrendsNumbres.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),mar=c(5,10,6,2))
barplot(tbp,main="Fifteen year trends",las=2,
        col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                   'DRP',expression(italic(E.~coli)),'MCI'),las=2)
axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 1,lwd = 0,lwd.ticks = 0)
axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 0,lwd.ticks = 1,line=-1)
par(xpd=NA)
for(cc in 1:9){
  text(rep(colMPs[cc],6),mbp[,cc],tb[cc,])
  text(colMPs[cc],1.025,sum(tb[cc,]))
}
if(names(dev.cur())=='tiff'){dev.off()}

#FifteenYearTrendsPercentage.tif ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/FifteenYearTrendsPercentage.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),mar=c(5,10,6,2))
barplot(tbp,main="Fifteen year trends",las=2,
        col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                   'DRP',expression(italic(E.~coli)),'MCI'),las=2)
axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
par(xpd=NA)
for(cc in 1:9){
  text(rep(colMPs[cc],6),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
  text(colMPs[cc],1.025,sum(tb[cc,]))
}
if(names(dev.cur())=='tiff'){dev.off()}






par(mfrow=c(3,1))
t5 <- plot(factor(trendTable5$Measurement),trendTable5$ConfCat,col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),main="5 Year monthly")
t10 <- plot(factor(trendTable10$Measurement),trendTable10$ConfCat,col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),main="10 Year monthly")
t15 <- plot(factor(trendTable15$Measurement),trendTable15$ConfCat,col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),main="15 Year monthly")
# write.csv(t5,paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend5Year.csv"))
# write.csv(t10,paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10Year.csv"))
# write.csv(t15,paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10YearQuarterly.csv"))
t5p <- apply(X = t5,MARGIN = 1,FUN = function(x)x/sum(x))
t10p <- apply(X = t10,MARGIN = 1,FUN = function(x)x/sum(x))
t15p <- apply(X = t15,MARGIN = 1,FUN = function(x)x/sum(x))

m5p <- apply(t5p,MARGIN = 2,FUN=cumsum)
m5p <- rbind(rep(0,8),m5p)
m5p = (m5p[-1,]+m5p[-6,])/2

m10p <- apply(t10p,MARGIN = 2,FUN=cumsum)
m10p <- rbind(rep(0,8),m10p)
m10p = (m10p[-1,]+m10p[-6,])/2

m15p <- apply(t15p,MARGIN = 2,FUN=cumsum)
m15p <- rbind(rep(0,8),m15p)
m15p = (m15p[-1,]+m15p[-6,])/2

colMPs=-0.5+(1:8)*1.2

tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendByPeriod.tif"),
     width = 10,height=15,units='in',res=350,compression='lzw',type='cairo')
par(mfrow=c(3,1),mar=c(5,10,4,2))
barplot(t5p,main="5 Year",las=2,
        col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n')
axis(side = 2,at = m5p[,1],labels = colnames(t5),las=2,lty = 0)
for(cc in 1:8){
  text(rep(colMPs[cc],5),m5p[,cc],paste0(t5[cc,],'\n(',round(t5p[,cc]*100,0),'%)'))
}
barplot(t10p,main="10 Year",las=2,
        col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n')
axis(side = 2,at = m10p[,1],labels = colnames(t10),las=2,lty = 0)
for(cc in 1:8){
  text(rep(colMPs[cc],5),m10p[,cc],paste0(t10[cc,],'\n(',round(t10p[,cc]*100,0),'%)'))
}
barplot(t15p,main="15 Year",las=2,
        col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n')
axis(side = 2,at = m15p[,1],labels = colnames(t15),las=2,lty = 0)
for(cc in 1:8){
  text(rep(colMPs[cc],5),m15p[,cc],paste0(t15[cc,],'\n(',round(t15p[,cc]*100,0),'%)'))
}
if(names(dev.cur())=='tiff'){dev.off()}
par(mfrow=c(1,1))


Sys.time()-startTime



EndYear <- lubridate::year(Sys.Date())-1
startYear5 <- EndYear - 5+1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1

combTrend=read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/","RiverWQ_Trend",	recursive = T,full.names = T,ignore.case=T),1),stringsAsFactors = F)
combTrend$ConfCat=factor(combTrend$ConfCat,levels=c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading" ))
Trends10yrMonthly = combTrend%>%
dplyr::filter(period==10&(frequency=='monthly'|Measurement=='MCI'))#%>%
# dplyr::select(LawaSiteID,Measurement,Region,TrendScore)
Trends10yrMonthly$TrendScore[Trends10yrMonthly$TrendScore==-99] <- NA
table(Trends10yrMonthly$Measurement[which(is.na(Trends10yrMonthly$TrendScore))]) ->naTab
naTab
#    DRP ECOLI   NH4    TN   TON    TP  TURB   MCI
#     29    16    96     9    26     8     5   332
#Drop the NAs
Trends10yrMonthly = Trends10yrMonthly%>%tidyr::drop_na(TrendScore)
#4620 to 4099
#Starts as DRP   NH4   TP    TON   TURB  ECOLI TN    BDISC  MCI
#labelled  DRP   NH4   TP    TON   TURB  ECOLI TN    CLAR   MCI
#Reorder 2 CLAR TURB   TN    TON   NH4   TP    DRP   ECOLI  MCI
Trends10yrMonthly$Measurement <- factor(Trends10yrMonthly$Measurement,
                                        levels=c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")) #,"MCI"
tfp10m=Trends10yrMonthly
lawaMacroState5yr = read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/',pattern='MACRO_STATE_ForITE|MacroState',recursive = T,full.names = T),1),stringsAsFactors = F)
lawaMacroState5yr = lawaMacroState5yr%>%filter(Parameter=="MCI")%>%dplyr::rename(LawaSiteID=LAWAID,Measurement=Parameter,Q50=Median)
sa <- read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",pattern=paste0("^sa",startYear5,"-",EndYear,".csv"),recursive=T,full.names=T,ignore.case=T),1),stringsAsFactors = F)
sa <- sa%>%filter(Scope=="Site")%>%select(LawaSiteID,Measurement,Q50)
sa <- rbind(sa,lawaMacroState5yr)
tfp10m <- merge(tfp10m,sa)
rm(sa,lawaMacroState5yr)

tfp10m$ConfCat=factor(tfp10m$ConfCat)
RiverNOF=read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",pattern='ITERiverNOF',recursive=T,full.names=T),1),stringsAsFactors=F)
RiverNOF$Parameter[RiverNOF$Parameter=="EcoliSummaryband"] <- "ECOLI"
RiverNOF$Parameter[RiverNOF$Parameter=="AmmoniacalMed_Band"] <- "NH4"
RiverNOF$Parameter[RiverNOF$Parameter=="Nitrate_Toxicity_Band"] <- "TON"

RiverNOF <- RiverNOF%>%filter(Parameter%in%c("ECOLI","NH4","TON"))%>%
  dplyr::rename(Measurement=Parameter,LawaSiteID=LAWAID)%>%select(-Year,-Value,-SiteName)

tfp10m <- merge(x=tfp10m,RiverNOF,by=c("LawaSiteID","Measurement"),all.x=T)%>%arrange(LawaSiteID)
tfp10m$Band[tfp10m$Measurement=="MCI"]=as.character(cut(tfp10m$Q50[tfp10m$Measurement=="MCI"],
                                                        breaks = c(0,90,110,130,200),
                                                        labels = c('D',"C",'B',"A")))
tfp10m$ConfCat=factor(tfp10m$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))



tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendByNOFState.tif"),
     width = 10,height=15,units='in',res=350,compression='lzw',type='cairo')
par(mfrow=c(4,1),las=1,mar=c(5,20,4,8),mgp=c(3,1,0),cex.axis=2,cex.main=2)
tfp10m%>%filter(Measurement=="ECOLI")%>%drop_na(Band)%>%transmute(x=factor(Band),y=factor(ConfCat))%>%
plot(xlab="NOF Band",ylab="",col=(c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF")),main="ECOLI")->sp1
tfp10m%>%filter(Measurement=="NH4")%>%transmute(x=factor(Band),y=factor(ConfCat))%>%
plot(xlab="NOF Band",ylab="",col=(c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF")),main="NH4 toxicity")
tfp10m%>%filter(Measurement=="MCI")%>%transmute(x=factor(Band),y=factor(ConfCat))%>%
  plot(xlab="NOF Band",ylab="",col=(c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF")),main="MCI")
tfp10m%>%filter(Measurement=="TON")%>%transmute(x=factor(Band),y=factor(ConfCat))%>%
  plot(xlab="NOF Band",ylab="",col=(c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF")),main="TON toxicity")
if(names(dev.cur())=='tiff')dev.off()


