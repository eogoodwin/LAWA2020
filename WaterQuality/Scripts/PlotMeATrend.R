# Plot me a trend example function


PlotMeATrend <- function(LSIin,measureIn,startYearIncl,stopYearIncl,period=10,
                         mymain=paste(LSIin,riverSiteTable$CouncilSiteID[match(LSIin,riverSiteTable$LawaSiteID)],measureIn),...){
  source("h:/ericg/16666LAWA/LWPTrends_v1901/LWPTrends_v1901.R")
  source("h:/ericg/16666LAWA/LAWA2019/Scripts/LAWAFunctions.R")
  source("h:/ericg/16666LAWA/LAWA2019/WaterQuality/scripts/SWQ_state_functions.R")
  
  Mode=function(x) {
    ux <- unique(x)
    ux[which.max(tabulate(match(x, ux)))]
  }
  
  EndYear <- year(Sys.Date())-1
  startYear = EndYear - period+1
EndYear = stopYearIncl
startYear=startYearIncl
period=EndYear-startYear+1
    # startYear5 <- EndYear - 5+1
  # startYear10 <- EndYear - 10+1
  # startYear15 <- EndYear - 15+1
  yearReq=ceiling(period*0.9)
  if (period==5){yearReq=1}
  if (period==10){yearReq=9}
  if (period==15){yearReq=13}
  quartReq = ceiling(period*4*9)
  if (period==5){quartReq=1000} #We dont do quarterly five year trends
  if (period==10){quartReq=36}
  if (period==15){quartReq=54}
  monReq = ceiling(period*12*9)
  if (period==5){monReq=54}
  if (period==10){monReq=108}
  if (period==15){monReq=162}
  
  if(!exists('riverSiteTable')){riverSiteTable=loadLatestSiteTableRiver()}
  
  if(!exists('wqdata')){
    wqdataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2019/WaterQuality/Data",pattern = "AllCouncils.csv",recursive = T,full.names = T),1)
    wqdata=read_csv(wqdataFileName,col_types = cols(
      .default = col_character(),
      Value = col_double(),
      Censored = col_logical(),
      NZReach = col_double(),
      Lat = col_double(),
      Long = col_double(),
      Altitude = col_double()
    ))%>%as.data.frame
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
    wqdata<<-wqdata
  }
  
  datafortrend=wqdata%>%dplyr::filter(Year>=startYear & Year <= EndYear & Measurement==measureIn & LawaSiteID==LSIin)
  
  siteTrendTable=data.frame(LawaSiteID=LSIin,Measurement=measureIn,nMeasures = NA_integer_,
                            nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                            Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                            nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                            Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                            prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                            AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                            AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                            Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                            ConfCat=NA_real_,period=period,frequency=NA_real_)
  siteTrendTable$nMeasures=dim(datafortrend)[1]
  if(dim(datafortrend)[1]>0){
    SSD_med <- datafortrend%>%
      dplyr::group_by(LawaSiteID,monYear,Year,Month,Qtr)%>%
      dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                       myDate = mean(myDate,na.rm=T),
                       Censored = any(Censored),
                       CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
      )%>%ungroup%>%as.data.frame
    SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
    SSD_med$Season = factor(SSD_med$Month)
    siteTrendTable$nFirstYear=length(which(SSD_med$Year==startYear))
    siteTrendTable$nLastYear=length(which(SSD_med$Year==EndYear))
    siteTrendTable$numMonths=dim(SSD_med)[1]
    siteTrendTable$numYears=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
    
    #For monthly we want 90% of measures and 90% of years
    if(siteTrendTable$numMonths >= monReq & siteTrendTable$numYears>=yearReq){
      siteTrendTable$frequency='monthly'
    }else{
      SSD_med <- datafortrend%>%
        dplyr::group_by(LawaSiteID,Year,Qtr)%>%
        dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                         myDate = mean(myDate,na.rm=T),
                         Censored = any(Censored),
                         CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
        )%>%ungroup%>%as.data.frame
      SSD_med$Season=SSD_med$Qtr
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      siteTrendTable$numQuarters=dim(SSD_med)[1]
      
      if(siteTrendTable$numQuarters >= quartReq & siteTrendTable$numYears>=yearReq){
        siteTrendTable$frequency='quarterly'
      }else{
        siteTrendTable$frequency='unassessed'
      }
    }

    if(siteTrendTable$frequency!='unassessed'){
      SeasonString <- sort(unique(SSD_med$Season))
      st <- SeasonalityTest(x = SSD_med,main=uMeasures,ValuesToUse = "Value",do.plot =F)
      siteTrendTable[names(st)] <- st
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
        sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = T,
                                mymain=mymain,...)
        siteTrendTable[names(sk)] <- sk
        siteTrendTable[names(sss)] <- sss
        rm(sk,sss)
      }else{
        mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
        ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = T,
                       mymain=mymain,...)
        siteTrendTable[names(mk)] <- mk
        siteTrendTable[names(ss)] <- ss
        rm(mk,ss)
      }
      rm(st)
    }
    rm(datafortrend)
    rm(SSD_med)
    
    
    rownames(siteTrendTable) <- NULL
    siteTrendTable$Sen_Probability[siteTrendTable$Measurement!="BDISC"]=1-(siteTrendTable$Sen_Probability[siteTrendTable$Measurement!="BDISC"])
    siteTrendTable$Probabilitymin[siteTrendTable$Measurement!="BDISC"]=1-(siteTrendTable$Probabilitymin[siteTrendTable$Measurement!="BDISC"])
    siteTrendTable$Probabilitymax[siteTrendTable$Measurement!="BDISC"]=1-(siteTrendTable$Probabilitymax[siteTrendTable$Measurement!="BDISC"])
    siteTrendTable$MKProbability[siteTrendTable$Measurement!="BDISC"]=1-(siteTrendTable$MKProbability[siteTrendTable$Measurement!="BDISC"])
    siteTrendTable$Agency=riverSiteTable$Agency[match(siteTrendTable$LawaSiteID,riverSiteTable$LawaSiteID)]
    siteTrendTable$SWQAltitude =  riverSiteTable$SWQAltitude[match(siteTrendTable$LawaSiteID,riverSiteTable$LawaSiteID)]
    siteTrendTable$SWQLanduse =   riverSiteTable$SWQLanduse[match(siteTrendTable$LawaSiteID,riverSiteTable$LawaSiteID)]
    siteTrendTable$Region =    riverSiteTable$Region[match(siteTrendTable$LawaSiteID,riverSiteTable$LawaSiteID)]
    siteTrendTable$ConfCat <- cut(siteTrendTable$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                                labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
    siteTrendTable$ConfCat=factor(siteTrendTable$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
    siteTrendTable$TrendScore=as.numeric(siteTrendTable$ConfCat)-3
    siteTrendTable$TrendScore[is.na(siteTrendTable$TrendScore)]<-(NA)
  }
  return(siteTrendTable)
}


PlotMeAMancroTrend <- function(LSIin,measureIn,startYearIncl,stopYearIncl,period=10,
                         mymain=paste(LSIin,riverSiteTable$CouncilSiteID[match(LSIin,riverSiteTable$LawaSiteID)],measureIn),...){
  source("h:/ericg/16666LAWA/LWPTrends_v1901/LWPTrends_v1901.R")
  source("h:/ericg/16666LAWA/LAWA2019/Scripts/LAWAFunctions.R")
  source("h:/ericg/16666LAWA/LAWA2019/WaterQuality/scripts/SWQ_state_functions.R")
  
  Mode=function(x) {
    ux <- unique(x)
    ux[which.max(tabulate(match(x, ux)))]
  }
  
  EndYear <- year(Sys.Date())-1
  startYear = EndYear - period+1
  EndYear = stopYearIncl
  startYear=startYearIncl
  period=EndYear-startYear+1
  # startYear5 <- EndYear - 5+1
  # startYear10 <- EndYear - 10+1
  # startYear15 <- EndYear - 15+1
  yearReq=ceiling(period*0.9)
  if (period==5){yearReq=1}
  if (period==10){yearReq=9}
  if (period==15){yearReq=13}
  quartReq = ceiling(period*4*9)
  if (period==5){quartReq=1000} #We dont do quarterly five year trends
  if (period==10){quartReq=36}
  if (period==15){quartReq=54}
  monReq = ceiling(period*12*9)
  if (period==5){monReq=54}
  if (period==10){monReq=108}
  if (period==15){monReq=162}
  
  if(!exists('macroSiteTable')){riverSiteTable=loadLatestSiteTableMacro()}
  
  if(!exists('macroData')){
    macroData=loadLatestDataMacro()
    macroData$myDate <- as.Date(as.character(macroData$Date),"%d-%b-%Y")
    macroData$myDate[macroData$myDate<as.Date('2000-01-01')] <- as.Date(as.character(macroData$Date[macroData$myDate<as.Date('2000-01-01')]),"%d-%b-%y")
    macroData <- GetMoreDateInfo(macroData)
    macroData$monYear = format(macroData$myDate,"%b-%Y")
    macroData$CenType='not'
    macroData$Censored=FALSE
    macroData$CenType[macroData$CenType%in%c("Left","L")]='lt'
    macroData$CenType[macroData$CenType%in%c("Right","R")]='gt'
    macroData$CenType[!macroData$CenType%in%c("lt","gt")]='not'
    
    macroData$NewValues=macroData$Value
    macroData<<-macroData
  }
  
  datafortrend=macroData%>%dplyr::filter(Year>=startYear & Year <= EndYear & Measurement==measureIn & LawaSiteID==LSIin)
  
  siteTrendTable=data.frame(LawaSiteID=LSIin,Measurement=measureIn,nMeasures = NA_integer_,
                            nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                            Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                            nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                            Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                            prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                            AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                            AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                            Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                            ConfCat=NA_real_,period=period,frequency=NA_real_)
  siteTrendTable$nMeasures=dim(datafortrend)[1]
  if(dim(datafortrend)[1]>0){
    SSD_med <- datafortrend%>%
      dplyr::group_by(LawaSiteID,monYear,Year,Month,Qtr)%>%
      dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                       myDate = mean(myDate,na.rm=T),
                       Censored = any(Censored),
                       CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
      )%>%ungroup%>%as.data.frame
    SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
    SSD_med$Season = factor(SSD_med$Month)
    siteTrendTable$nFirstYear=length(which(SSD_med$Year==startYear))
    siteTrendTable$nLastYear=length(which(SSD_med$Year==EndYear))
    siteTrendTable$numMonths=dim(SSD_med)[1]
    siteTrendTable$numYears=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
    
    #For 10 year we want 8 years out of 10
    if(siteTrendTable$numYears >= 8){
      siteTrendTable$frequency='yearly'
    }else{
      siteTrendTable$frequency='unassessed'
    }
    
    if(siteTrendTable$frequency!='unassessed'){
      SeasonString <- sort(unique(SSD_med$Season))
      st <- SeasonalityTest(x = SSD_med,main=uMeasures,ValuesToUse = "Value",do.plot =F)
      siteTrendTable[names(st)] <- st
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
        sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = T,
                                mymain=mymain,...)
        siteTrendTable[names(sk)] <- sk
        siteTrendTable[names(sss)] <- sss
        rm(sk,sss)
      }else{
        mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
        ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = T,
                       mymain=mymain,...)
        siteTrendTable[names(mk)] <- mk
        siteTrendTable[names(ss)] <- ss
        rm(mk,ss)
      }
      rm(st)
    }
    rm(datafortrend)
    rm(SSD_med)
    
    
    rownames(siteTrendTable) <- NULL
    siteTrendTable$Sen_Probability[siteTrendTable$Measurement!="BDISC"]=1-(siteTrendTable$Sen_Probability[siteTrendTable$Measurement!="BDISC"])
    siteTrendTable$Probabilitymin[siteTrendTable$Measurement!="BDISC"]=1-(siteTrendTable$Probabilitymin[siteTrendTable$Measurement!="BDISC"])
    siteTrendTable$Probabilitymax[siteTrendTable$Measurement!="BDISC"]=1-(siteTrendTable$Probabilitymax[siteTrendTable$Measurement!="BDISC"])
    siteTrendTable$MKProbability[siteTrendTable$Measurement!="BDISC"]=1-(siteTrendTable$MKProbability[siteTrendTable$Measurement!="BDISC"])
    siteTrendTable$Agency=riverSiteTable$Agency[match(siteTrendTable$LawaSiteID,riverSiteTable$LawaSiteID)]
    siteTrendTable$SWQAltitude =  riverSiteTable$SWQAltitude[match(siteTrendTable$LawaSiteID,riverSiteTable$LawaSiteID)]
    siteTrendTable$SWQLanduse =   riverSiteTable$SWQLanduse[match(siteTrendTable$LawaSiteID,riverSiteTable$LawaSiteID)]
    siteTrendTable$Region =    riverSiteTable$Region[match(siteTrendTable$LawaSiteID,riverSiteTable$LawaSiteID)]
    siteTrendTable$ConfCat <- cut(siteTrendTable$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                                  labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
    siteTrendTable$ConfCat=factor(siteTrendTable$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
    siteTrendTable$TrendScore=as.numeric(siteTrendTable$ConfCat)-3
    siteTrendTable$TrendScore[is.na(siteTrendTable$TrendScore)]<-(NA)
  }
  return(siteTrendTable)
}