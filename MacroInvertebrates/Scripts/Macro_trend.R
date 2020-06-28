rm(list=ls())
library(tidyverse)
source("h:/ericg/16666LAWA/LAWA2020/Scripts/LWPTrends_Dec18/LWPTrends_v1811.R")
source("h:/ericg/16666LAWA/LAWA2020/Scripts/LAWAFunctions.R")
source("h:/ericg/16666LAWA/LAWA2020/WaterQuality/scripts/SWQ_state_functions.R")
try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d")),recursive = T))

Mode=function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

EndYear <- year(Sys.Date())-1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1

siteTable=loadLatestSiteTableMacro()
#Load the latest made
if(!exists('macroData')){
  macroData=read.csv(tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data",
                              pattern = "MacrosWithMetadata.csv",recursive = T,full.names = T),1),stringsAsFactors = F)

  macroData$myDate <- as.Date(lubridate::dmy(as.character(macroData$Date)),"%d-%b-%Y")
  macroData$Year=lubridate::isoyear(lubridate::dmy(macroData$Date))
  macroData=macroData[which(macroData$Year<=EndYear),]  #41740 for startYear15
  # macroData$sYear=macroData$Year   #change to hydro year in 2020
  macroData$month=lubridate::month(lubridate::dmy(macroData$Date))
  macroData <- GetMoreDateInfo(macroData)
  macroData$monYear = format(macroData$myDate,"%b-%Y")
  # macroData$Season <- macroData$Month
  SeasonString <- sort(unique(macroData$Season))
  macroData$Season=macroData$month
  macroData$Censored=F
  macroData$CenType="FALSE"
}






#15 year trend ####
datafor15=macroData%>%filter(Year>=startYear15 & Year <= EndYear)
usites=unique(datafor15$LawaSiteID)
cat(length(usites),'\n')
usite=1
library(parallel)
library(doParallel)
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(doBy)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  siteTrendTable15=data.frame(LawaSiteID=usites[usite],Measurement='MCI',nMeasures = NA_integer_,
                              nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                              Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                              nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                              Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                              prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                              AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                              AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                              Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                              ConfCat=NA_real_,period=15,frequency=NA_real_)
  subDat=datafor15%>%dplyr::filter(LawaSiteID==usites[usite],Measurement=='MCI')
    siteTrendTable15$nMeasures=dim(subDat)[1]
    if(dim(subDat)[1]>0){
      SSD_med <- subDat%>%
        dplyr::group_by(LawaSiteID,Year)%>%
        dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                         myDate = mean(myDate,na.rm=T),
                         Censored = any(Censored),
                         CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
        )%>%ungroup%>%as.data.frame
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season = paste0('y',SSD_med$Year)
      SeasonString = sort(unique(sapply(startYear15:EndYear,function(x)paste0('y',x))))
      siteTrendTable15$nFirstYear=length(which(SSD_med$Year==startYear15))
      siteTrendTable15$nLastYear=length(which(SSD_med$Year==EndYear))
      siteTrendTable15$numYears=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      
      #For 15 year we want 13 years out of 15
      if(siteTrendTable15$numYears >= 13){
        siteTrendTable15$frequency='yearly'
      }else{
          siteTrendTable15$frequency='unassessed'
      }
      if(siteTrendTable15$frequency!='unassessed'){
        st <- SeasonalityTest(x = SSD_med,main='MCI',ValuesToUse = "Value",do.plot =F)
        siteTrendTable15[names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable15[names(sk)] <- sk
          siteTrendTable15[names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable15[names(mk)] <- mk
          siteTrendTable15[names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(SSD_med)
    }
    rm(subDat)
  return(siteTrendTable15)
}-> trendTable15
stopCluster(workers)
rm(workers,usites,usite,datafor15)
Sys.time()-startTime
#23 Jun 3.8s
rownames(trendTable15) <- NULL
trendTable15$Agency=siteTable$Agency[match(trendTable15$LawaSiteID,siteTable$LawaSiteID)]
trendTable15$SWQAltitude =  siteTable$SWQAltitude[match(trendTable15$LawaSiteID,siteTable$LawaSiteID)]
trendTable15$SWQLanduse =   siteTable$SWQLanduse[match(trendTable15$LawaSiteID,siteTable$LawaSiteID)]
trendTable15$Region =    siteTable$Region[match(trendTable15$LawaSiteID,siteTable$LawaSiteID)]

trendTable15$ConfCat <- cut(trendTable15$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable15$ConfCat=factor(trendTable15$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable15$TrendScore=as.numeric(trendTable15$ConfCat)-3
trendTable15$TrendScore[is.na(trendTable15$TrendScore)]<-(NA)
save(trendTable15,file=paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend15Year.rData"))
rm(trendTable15)





#10 year trend ####
datafor10=macroData%>%filter(Year>=startYear10 & Year <= EndYear)
usites=unique(datafor10$LawaSiteID)
cat(length(usites),'\n')
usite=1
library(parallel)
library(doParallel)
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  #library(doBy)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  siteTrendTable10=data.frame(LawaSiteID=usites[usite],Measurement='MCI',nMeasures = NA_integer_,
                              nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                              Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                              nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                              Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                              prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                              AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                              AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                              Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                              ConfCat=NA_real_,period=10,frequency=NA_real_)
  subDat=datafor10%>%dplyr::filter(LawaSiteID==usites[usite],Measurement=='MCI')
  siteTrendTable10$nMeasures=dim(subDat)[1]
  if(dim(subDat)[1]>0){
    SSD_med <- subDat%>%
      dplyr::group_by(LawaSiteID,Year)%>%
      dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                       myDate = mean(myDate,na.rm=T),
                       Censored = any(Censored),
                       CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
      )%>%ungroup%>%as.data.frame
    SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
    SSD_med$Season = paste0('y',SSD_med$Year)
    SeasonString = sort(unique(sapply(startYear10:EndYear,function(x)paste0('y',x))))
    siteTrendTable10$nFirstYear=length(which(SSD_med$Year==startYear10))
    siteTrendTable10$nLastYear=length(which(SSD_med$Year==EndYear))
    siteTrendTable10$numYears=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
    
    #For 10 year we want 8 years out of 10
    if(siteTrendTable10$numYears >= 8){
      siteTrendTable10$frequency='yearly'
    }else{
      siteTrendTable10$frequency='unassessed'
    }
    if(siteTrendTable10$frequency!='unassessed'){
      st <- SeasonalityTest(x = SSD_med,main='MCI',ValuesToUse = "Value",do.plot =F)
      siteTrendTable10[names(st)] <- st
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
        sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
        siteTrendTable10[names(sk)] <- sk
        siteTrendTable10[names(sss)] <- sss
        rm(sk,sss)
      }else{
        mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
        ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
        siteTrendTable10[names(mk)] <- mk
        siteTrendTable10[names(ss)] <- ss
        rm(mk,ss)
      }
      rm(st)
    }
    rm(SSD_med)
  }
  rm(subDat)
  return(siteTrendTable10)
}-> trendTable10
stopCluster(workers)
rm(workers,usites,usite,datafor10)
Sys.time()-startTime
#23 Jun 3.7s
rownames(trendTable10) <- NULL
trendTable10$Agency=siteTable$Agency[match(trendTable10$LawaSiteID,siteTable$LawaSiteID)]
trendTable10$SWQAltitude =  siteTable$SWQAltitude[match(trendTable10$LawaSiteID,siteTable$LawaSiteID)]
trendTable10$SWQLanduse =   siteTable$SWQLanduse[match(trendTable10$LawaSiteID,siteTable$LawaSiteID)]
trendTable10$Region =    siteTable$Region[match(trendTable10$LawaSiteID,siteTable$LawaSiteID)]

trendTable10$ConfCat <- cut(trendTable10$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable10$ConfCat=factor(trendTable10$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable10$TrendScore=as.numeric(trendTable10$ConfCat)-3
trendTable10$TrendScore[is.na(trendTable10$TrendScore)]<-(NA)
save(trendTable10,file=paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10Year.rData"))
rm(trendTable10)


# 
# 
# #max year trend ####
# dataformax=macroData%>%filter(Year <= EndYear)
# usites=unique(dataformax$LawaSiteID)
# cat(length(usites),'\n')
# usite=1
# library(parallel)
# library(doParallel)
# workers <- makeCluster(7)
# registerDoParallel(workers)
# clusterCall(workers,function(){
#   library(magrittr)
#   #library(doBy)
#   library(plyr)
#   library(dplyr)
#   
# })
# startTime=Sys.time()
# foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
#   siteTrendTablemax=data.frame(LawaSiteID=usites[usite],Measurement='MCI',nMeasures = NA_integer_,
#                               nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
#                               Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
#                               nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
#                               Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
#                               prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
#                               AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
#                               AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
#                               Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
#                               ConfCat=NA_real_,period='max',frequency=NA_real_)
#   subDat=dataformax%>%dplyr::filter(LawaSiteID==usites[usite],Measurement=='MCI')
#   siteTrendTablemax$nMeasures=dim(subDat)[1]
#   if(dim(subDat)[1]>0){
#     SSD_med <- subDat%>%
#       dplyr::group_by(LawaSiteID,Year)%>%
#       dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
#                        myDate = mean(myDate,na.rm=T),
#                        Censored = any(Censored),
#                        CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
#       )%>%ungroup%>%as.data.frame
#     SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
#     SSD_med$Season = paste0('y',SSD_med$Year)
#     SeasonString = sapply(sort(unique(SSD_med$Year)),function(x)paste0('y',x))
#     siteTrendTablemax$nFirstYear=length(which(SSD_med$Year==min(SSD_med$Year)))
#     siteTrendTablemax$nLastYear=length(which(SSD_med$Year==EndYear))
#     siteTrendTablemax$numYears=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
#     siteTrendTablemax$period=EndYear-min(SSD_med$Year,na.rm=T)+1
#     
#     #For max period monthly we want 90% of measures 
#     if(siteTrendTablemax$numYears >= 0.9*siteTrendTablemax$period){
#       siteTrendTablemax$frequency='yearly'
#     }else{
#       siteTrendTablemax$frequency='unassessed'
#     }
#     if(siteTrendTablemax$frequency!='unassessed'){
#       st <- SeasonalityTest(x = SSD_med,main='MCI',ValuesToUse = "Value",do.plot =F)
#       siteTrendTablemax[names(st)] <- st
#       if(!is.na(st$pvalue)&&st$pvalue<0.05){
#         sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
#         sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
#         siteTrendTablemax[names(sk)] <- sk
#         siteTrendTablemax[names(sss)] <- sss
#         rm(sk,sss)
#       }else{
#         mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
#         ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
#         siteTrendTablemax[names(mk)] <- mk
#         siteTrendTablemax[names(ss)] <- ss
#         rm(mk,ss)
#       }
#       rm(st)
#     }
#     rm(SSD_med)
#   }
#   rm(subDat)
#   return(siteTrendTablemax)
# }-> trendTablemax
# stopCluster(workers)
# rm(workers,usites,usite,dataformax)
# Sys.time()-startTime
# rownames(trendTablemax) <- NULL
# trendTablemax$Agency=siteTable$Agency[match(trendTablemax$LawaSiteID,siteTable$LawaSiteID)]
# trendTablemax$SWQAltitude =  siteTable$SWQAltitude[match(trendTablemax$LawaSiteID,siteTable$LawaSiteID)]
# trendTablemax$SWQLanduse =   siteTable$SWQLanduse[match(trendTablemax$LawaSiteID,siteTable$LawaSiteID)]
# trendTablemax$Region =    siteTable$Region[match(trendTablemax$LawaSiteID,siteTable$LawaSiteID)]
# 
# trendTablemax$ConfCat <- cut(trendTablemax$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
#                             labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
# trendTablemax$ConfCat=factor(trendTablemax$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
# trendTablemax$TrendScore=as.numeric(trendTablemax$ConfCat)-3
# trendTablemax$TrendScore[is.na(trendTablemax$TrendScore)]<-(NA)
# trendTablemax$period=as.numeric(trendTablemax$period)
# save(trendTablemax,file=paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TrendmaxYear.rData"))
# rm(trendTablemax)




load(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",pattern="Trend15Year.rData",recursive = T,full.names = T),1),verbose=T)
load(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",pattern="Trend10Year.rData",recursive = T,full.names = T),1),verbose=T)
# load(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",pattern="TrendmaxYear.rData",recursive = T,full.names = T),1),verbose=T)
# trendTablemax$period=as.numeric(trendTablemax$period)

trendTable15 <- trendTable15%>%drop_na(LawaSiteID)
trendTable10 <- trendTable10%>%drop_na(LawaSiteID)
# trendTablemax <- trendTablemax%>%drop_na(LawaSiteID)%>%filter(period>15)

combTrend <- rbind(trendTable15,trendTable10)
# combTrend <- rbind(rbind(trendTable15,trendTable10),trendTablemax)
# %>%dplyr::select(LawaSiteID,Agency,Region,Altitude,Landcover,Measurement,TrendScore,nMeasures,frequency,period,ConfCat,AnalysisNote,AnalysisNoteSS)

combTrend$CouncilSiteID = siteTable$CouncilSiteID[match(tolower(combTrend$LawaSiteID),tolower(siteTable$LawaSiteID))]

#Write outputs ####
write.csv(combTrend,
          paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                                    "/MacroMCI_Trend",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
rm(combTrend)



#And for ITE ####
# write.csv(trendTablemax%>%
#             transmute(LAWAID=LawaSiteID,
#                       Parameter=Measurement,
#                       Trend=TrendScore),
#           paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
#                  "/ITEMacroTrendmax",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
write.csv(trendTable10%>%
            transmute(LAWAID=LawaSiteID,
                      Parameter=Measurement,
                      Trend=TrendScore),
          paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                 "/ITEMacroTrend10",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
write.csv(trendTable15%>%
            transmute(LAWAID=LawaSiteID,
                      Parameter=Measurement,
                      Trend=TrendScore),
          paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                 "/ITEMacroTrend15",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)

# combTrend=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis",pattern="MacroMCI_Trend",recursive = T,full.names = T),1),stringsAsFactors = F)

savePlott=F
usites=unique(trendTable10$LawaSiteID)
uMeasures=unique(trendTable10$Measurement)
for(uparam in seq_along(uMeasures)){
  subTrend=trendTable10[which(trendTable10$Measurement==uMeasures[uparam]),]
  worstDeg <- which.max(subTrend$MKProbability) 
  bestImp <- which.min(subTrend$MKProbability)
  cat(subTrend$MKProbability[worstDeg],'\t')
  cat(subTrend$MKProbability[bestImp],'\n')
  if(savePlott){
    tiff(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/BestWorstTenYear",uMeasures[uparam],".tif"),
         width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
  }else{
    windows()
  }
    par(mfrow=c(2,1),mar=c(2,4,1,2))
    theseDeg <- which(macroData$LawaSiteID==subTrend$LawaSiteID[worstDeg] &
                        macroData$Measurement==uMeasures[uparam] & dmy(macroData$Date)>dmy("1-1-2009"))
    theseImp <- which(macroData$LawaSiteID==subTrend$LawaSiteID[bestImp] &
                        macroData$Measurement==uMeasures[uparam] & dmy(macroData$Date)>dmy("1-1-2009"))
    
      MannKendall(x = macroData[theseDeg,],ValuesToUse = "Value",doPlot=F)
      SenSlope(x = macroData[theseDeg,],ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = T)
      MannKendall(x = macroData[theseImp,],ValuesToUse = "Value",doPlot=F)
      SenSlope(x = macroData[theseImp,],ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = T)
    if(names(dev.cur())=='tiff'){dev.off()}
    rm(theseDeg,theseImp)
  
  rm(worstDeg,bestImp)
}


#Make the coloured plot
par(mfrow=c(1,1))
tb <- plot(factor(trendTable10$Measurement),trendTable10$ConfCat,
           col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF"),main="Ten year trends")
tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
mbp <- rbind(0,mbp)
mbp = (mbp[-1,]+mbp[-6,])/2
if(savePlott){
 tiff(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TenYearTrends.tif"),
      width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
}
par(mfrow=c(1,1),mar=c(5,10,4,2))
barplot(tbp,main="Ten year trends",las=2,
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF"),yaxt='n')
axis(side = 2,at = mbp,labels = colnames(tb),las=2,lty = 0)
text(0.75,mbp,tb)

if(names(dev.cur())=='tiff'){dev.off()}


