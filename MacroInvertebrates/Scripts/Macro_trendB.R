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
  macroData=macroData[which(macroData$Year>=startYear15 & macroData$Year<=EndYear),]  #28301 for startYear15
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
# for(usite in usite:length(usites)){
  siteTrendTable15=data.frame(LawaSiteID=NA_character_,nMeasures=NA_integer_,numYears=NA_integer_,
                              Observations = NA_integer_, KWstat = NA_real_, 
                              pvalue = NA_real_,
                              nObs = NA_integer_, S = NA_real_, VarS = NA_real_, 
                              D = NA_real_, tau = NA_real_, Z = NA_real_, p = NA_real_, 
                              MKProbability = NA_real_, prop.censored = NA_real_, prop.unique = NA_real_, 
                              no.censorlevels = NA_integer_, Median = NA_real_, Sen_VarS = NA_real_, 
                              AnnualSenSlope = NA_real_, Intercept = NA_real_, Lci = NA_real_, 
                              Uci = NA_real_, AnalysisNoteSS = NA, Sen_Probability = NA_real_, 
                              Probabilitymax = NA_real_, Probabilitymin = NA_real_, Percent.annual.change = NA_real_, 
                              TrendCategory = NA, TrendDirection = NA)
  subDat=datafor15%>%dplyr::filter(LawaSiteID==usites[usite],Measurement=='MCI')
  siteTrendTable15$nMeasures=dim(subDat)[1]
  if(dim(subDat)[1]>0){
    siteTrendTable15$LawaSiteID=usites[usite]
    SSD_med <- subDat%>%
      dplyr::group_by(LawaSiteID,Year)%>%
      dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                       myDate = mean(myDate,na.rm=T),
                       Censored = any(Censored),
                       CenType = paste0(unique(CenType),collapse=''))%>%ungroup%>%as.data.frame
    SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
    SSD_med$Season = paste0('y',SSD_med$Year)
    SeasonString = sort(unique(sapply(startYear15:EndYear,function(x)paste0('y',x))))
    # siteTrendTable15$nFirstYear=length(which(SSD_med$Year==startYear15))
    # siteTrendTable15$nLastYear=length(which(SSD_med$Year==EndYear))
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
        A1=SeasonalTrendAnalysis(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=T,ValuesToUseforMedian="Value")
        # sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
        # sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
        siteTrendTable15[names(A1)] <- A1
        rm(A1)
      }else{
        A1=NonSeasonalTrendAnalysis(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=T,ValuesToUseforMedian="Value")
        siteTrendTable15[names(A1)] <- A1
        rm(A1)
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

rownames(trendTable15) <- NULL
trendTable15$Agency=siteTable$Agency[match(trendTable15$LawaSiteID,siteTable$LawaSiteID)]
trendTable15$Altitude =  siteTable$Altitude[match(trendTable15$LawaSiteID,siteTable$LawaSiteID)]
trendTable15$Landcover =   siteTable$Landcover[match(trendTable15$LawaSiteID,siteTable$LawaSiteID)]
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
                       CenType = paste0(unique(CenType),collapse=''))%>%ungroup%>%as.data.frame
    SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
    SSD_med$Season = paste0('y',SSD_med$Year)
    SeasonString = sort(unique(sapply(startYear10:EndYear,function(x)paste0('y',x))))
    siteTrendTable10$nFirstYear=length(which(SSD_med$Year==startYear10))
    siteTrendTable10$nLastYear=length(which(SSD_med$Year==EndYear))
    siteTrendTable10$numYears=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
    
    #For 10 year we want 13 years out of 10
    if(siteTrendTable10$numYears >= 9){
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
rownames(trendTable10) <- NULL
trendTable10$Agency=siteTable$Agency[match(trendTable10$LawaSiteID,siteTable$LawaSiteID)]
trendTable10$Altitude =  siteTable$Altitude[match(trendTable10$LawaSiteID,siteTable$LawaSiteID)]
trendTable10$Landcover =   siteTable$Landcover[match(trendTable10$LawaSiteID,siteTable$LawaSiteID)]
trendTable10$Region =    siteTable$Region[match(trendTable10$LawaSiteID,siteTable$LawaSiteID)]

trendTable10$ConfCat <- cut(trendTable10$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable10$ConfCat=factor(trendTable10$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable10$TrendScore=as.numeric(trendTable10$ConfCat)-3
trendTable10$TrendScore[is.na(trendTable10$TrendScore)]<-(NA)
save(trendTable10,file=paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10Year.rData"))
rm(trendTable10)




load(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",pattern="Trend15Year.rData",recursive = T,full.names = T),1),verbose=T)
load(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",pattern="Trend10Year.rData",recursive = T,full.names = T),1),verbose=T)

trendTable10 <- trendTable10%>%drop_na(LawaSiteID)
trendTable15 <- trendTable15%>%drop_na(LawaSiteID)

combTrend <- rbind(trendTable15,trendTable10)%>%
  dplyr::select(LawaSiteID,Agency,Region,Altitude,Landcover,Measurement,TrendScore,nMeasures,frequency,period,ConfCat)
combTrend$CouncilSiteID = siteTable$CouncilSiteID[match(tolower(combTrend$LawaSiteID),tolower(siteTable$LawaSiteID))]
write.csv(combTrend,paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                                    "/MacroMCI_Trend",
                         format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
rm(combTrend)

# combTrend=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis",pattern="MacroMCI_Trend",recursive = T,full.names = T),1),stringsAsFactors = F)

savePlott=T
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
 tiff(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TenYearTrends.tif"),
      width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),mar=c(5,10,4,2))
barplot(tbp,main="Ten year trends",las=2,
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF"),yaxt='n')
axis(side = 2,at = mbp,labels = colnames(tb),las=2,lty = 0)
text(0.75,mbp,tb)

if(names(dev.cur())=='tiff'){dev.off()}


