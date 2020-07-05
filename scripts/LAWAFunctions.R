library(lubridate)
library(XML)

#Is the core of the funcgorithm
source('h:/ericg/16666LAWA/LAWA2020/scripts/LWPTrends_Dec18/LWPTrends_v1811.R')
trendCore <- function(subDat,periodYrs,proportionNeeded=0.5){
  siteTrendTable=data.frame(LawaSiteID=unique(subDat$LawaSiteID),Measurement=unique(subDat$Measurement),
                            nMeasures = NA_integer_,nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,
                            numQuarters=NA_real_,numYears=NA_real_,proportionNeeded=proportionNeeded,
                            Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_, #from seasonality test
                            nObs = NA_integer_, S = NA_real_, VarS = NA_real_,
                            D = NA_real_, tau = NA_real_,Z = NA_real_, p = NA_real_,
                            MKProbability=NA_real_,AnalysisNote="Insufficient data",prop.censored=NA_real_,
                            prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                            AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                            AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                            Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                            ConfCat=NA_real_,period=periodYrs,frequency=NA_real_)
  subDat <- subDat%>%dplyr::filter(lubridate::year(myDate)>=(EndYear-periodYrs+1)&lubridate::year(myDate)<=EndYear)
  subDat <- subDat%>%tidyr::drop_na(Value)
  if(dim(subDat)[1]==0){return(siteTrendTable)}
  siteTrendTable$nMeasures=dim(subDat)[1]
  SSD_med <- subDat%>%
    dplyr::group_by(LawaSiteID,Year,Month,Qtr)%>%
    dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                     myDate = mean(myDate,na.rm=T),
                     Censored = any(Censored),
                     CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
    )%>%ungroup%>%as.data.frame
  SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
  SSD_med$Season = factor(SSD_med$Month)
  siteTrendTable$nFirstYear=length(which(SSD_med$Year==(EndYear-periodYrs+1)))
  siteTrendTable$nLastYear=length(which(SSD_med$Year==EndYear))
  siteTrendTable$numMonths=dim(SSD_med)[1]
  siteTrendTable$numYears=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
  
  #For monthly we want x% of measures, x% in the last year, and a certain number of years (9 out of 10, 13 out of 15)
  if((siteTrendTable$numMonths >= proportionNeeded*12*periodYrs) &
     (siteTrendTable$nLastYear >= proportionNeeded*12) &
     (siteTrendTable$numYears > 0.85*periodYrs)){ #New rule, only implemented 27/3/20
    siteTrendTable$frequency='monthly'
  }else{
    SSD_med <- subDat%>%
      dplyr::group_by(LawaSiteID,Year,Qtr)%>%
      dplyr::summarise(Value = quantile(Value,prob=c(0.5),type=5,na.rm=T),
                       myDate = mean(myDate,na.rm=T),
                       Censored = any(Censored),
                       CenType = ifelse(any(CenType=='lt'),'lt',ifelse(any(CenType=='gt'),'gt','not'))
      )%>%ungroup%>%as.data.frame
    SSD_med$Season=SSD_med$Qtr
    SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
    siteTrendTable$numQuarters=dim(SSD_med)[1]
    
    #For quarterly we want x% of measures, x% in the last year, and a certain number of years (9/10, 13/15)
    if((siteTrendTable$numQuarters >= proportionNeeded*4*periodYrs) &
       (siteTrendTable$nLastYear >= proportionNeeded*4) &
       (siteTrendTable$numYears >= 0.85*periodYrs)){ #New rule, only implemented 27/3/20
      siteTrendTable$frequency='quarterly'
    }else{
      siteTrendTable$frequency=paste('unassessed',
                                     ifelse(siteTrendTable$numQuarters >= proportionNeeded*4*periodYrs,
                                            '',paste0('nQ=',siteTrendTable$numQuarters,'/',4*periodYrs)),
                                     ifelse(siteTrendTable$nLastYear >= proportionNeeded*4,
                                            '',paste0('nQinLY=',siteTrendTable$nLastYear,'/',4)),
                                     ifelse(siteTrendTable$numYears >= 0.85*periodYrs,'',
                                            paste0('nY=',siteTrendTable$numYears,'/',periodYrs)))
    }
  }
  if(!grepl('^unassess',siteTrendTable$frequency)){
    st <- SeasonalityTest(x = SSD_med,main=uMeasures,ValuesToUse = "Value",do.plot =F)
    siteTrendTable[names(st)] <- st
    if(!is.na(st$pvalue)&&st$pvalue<0.05){
      sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
      sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
      siteTrendTable[names(sk)] <- sk
      siteTrendTable[names(sss)] <- sss
      rm(sk,sss)
    }else{
      mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
      ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
      siteTrendTable[names(mk)] <- mk
      siteTrendTable[names(ss)] <- ss
      rm(mk,ss)
    }
    rm(st)
  }#else{
    #siteTrendTable$frequency=paste0('too few, n=',dim(SSD_med)[1],'Q/',proportionNeeded*4*periodYrs)
#  }
  rm(SSD_med)
  return(siteTrendTable)
}


pseudo.titlecase = function(str)
{
  substr(str, 1, 1) = toupper(substr(str, 1, 1))
  return(str)
}

cc <- function(file){
  x <- readLines(file,encoding='UTF-8-BOM')
  y <- gsub( "SITEID",            "SiteID",            x, ignore.case = TRUE  )
  y <- gsub( "ELEVATION",         "Elevation",         y, ignore.case = TRUE  )
  y <- gsub( "COUNCILSITEID",     "CouncilSiteID",     y, ignore.case = TRUE  )
  y <- gsub( "LAWASITEID",        "LawaSiteID",        y, ignore.case = TRUE  )
  y <- gsub( "SWMANAGEMENTZONE",  "SWManagementZone",  y, ignore.case = TRUE  )
  y <- gsub( "GWMANAGEMENTZONE",  "GWManagementZone",  y, ignore.case = TRUE  )
  y <- gsub( "CATCHMENT",         "Catchment",         y, ignore.case = TRUE  )
  y <- gsub( "NZREACH",           "NZReach",           y, ignore.case = TRUE  )
  y <- gsub( "DESCRIPTION",       "Description",       y, ignore.case = TRUE  )
  y <- gsub( "PHOTOGRAPH",        "Photograph",        y, ignore.case = TRUE  )
  y <- gsub( "SWQUALITY",         "SWQuality",         y, ignore.case = TRUE  )
  y <- gsub( "SWQUALITYSTART",    "SWQualityStart",    y, ignore.case = TRUE  )
  y <- gsub( "SWQFREQUENCYALL",   "SWQFrequencyAll",   y, ignore.case = TRUE  )
  y <- gsub( "SWQFREQUENCYLAST5", "SWQFrequencyLast5", y, ignore.case = TRUE  )
  y <- gsub( "SWQALTITUDE",       "SWQAltitude",       y, ignore.case = TRUE  )
  y <- gsub( "SWQLANDUSE",        "SWQLanduse",        y, ignore.case = TRUE  )
  y <- gsub( "RWQUALITY",         "RWQuality",         y, ignore.case = TRUE  )
  y <- gsub( "RWQUALITYSTART",    "RWQualityStart",    y, ignore.case = TRUE  )
  y <- gsub( "LWQUALITY",         "LWQuality",         y, ignore.case = TRUE  )
  y <- gsub( "LWQUALITYSTART",    "LWQualityStart",    y, ignore.case = TRUE  )
  y <- gsub( "GEOMORPHICLTYPE",   "GeomorphicLType",   y, ignore.case = TRUE  )
  y <- gsub( "^LTYPE",            "LType",             y, ignore.case = TRUE  )
  y <- gsub( "emar:LTYPE",        "emar:LType",        y, ignore.case = TRUE  )
  y <- gsub( "LFENZID",           "LFENZID",           y, ignore.case = TRUE  )
  y <- gsub( "MACRO",             "Macro",             y, ignore.case = TRUE  )
  y <- gsub( "MACROSTART",        "MacroStart",        y, ignore.case = TRUE  )
  y <- gsub( "SWQUANTITY",        "SWQuantity",        y, ignore.case = TRUE  )
  y <- gsub( "SWQUANTITYSTART",   "SWQuantityStart",   y, ignore.case = TRUE  )
  y <- gsub( "REGION",            "Region",            y, ignore.case = TRUE  )
  y <- gsub( "AGENCY",            "Agency",            y, ignore.case = TRUE  ) 
  y <- gsub( "ns2.",              "",                  y, ignore.case = TRUE  ) 
  y <- gsub( "ns3.",              "",                  y, ignore.case = TRUE  ) 
  
  writeLines(y,file)
  
}

#Derive frequencies per site
freqCheck <- function(sm){
  if(dim(sm)[1]==0){
    return('never')
  }
  if(dim(sm)[1]==1){
    return('quarterly')
  }
  sm$Date=round.POSIXt(sm$Date,units = 'day')
  dd=diff(sort(sm$Date))
  if(attributes(dd)$units=='secs'){dd = dd/60/60/24}
  if(attributes(dd)$units=='mins'){dd = dd/60/24}
  if(attributes(dd)$units=='hours'){dd = dd/24}
  Mdd=Mode(dd)
  mdd=as.numeric(median(dd))
  rm(dd)
  diffFromWeeklyM=abs(Mdd-7)
  diffFromWeeklym=abs(mdd-7)
  diffFromBiWeeklyM=abs(Mdd-14)
  diffFromBiWeeklym=abs(mdd-14)
  diffFromMonthlyM=abs(Mdd-30)
  diffFromMonthlym=abs(mdd-30)
  diffFromBimonthlyM=abs(Mdd-60)
  diffFromBimonthlym=abs(mdd-60)
  diffFromQuarterlyM=abs(Mdd-90)
  diffFromQuarterlym=abs(mdd-90)
  ModeFreq=c('weekly','biweekly','monthly','bimonthly','quarterly')[which.min(c(diffFromWeeklyM,diffFromBiWeeklyM,diffFromMonthlyM,diffFromBimonthlyM,diffFromQuarterlyM))]
  medianFreq=c('weekly','biweekly','monthly','bimonthly','quarterly')[which.min(c(diffFromWeeklym,diffFromBiWeeklym,diffFromMonthlym,diffFromBimonthlym,diffFromQuarterlym))]
  if(length(ModeFreq)==0|length(medianFreq)==0){browser()}
  if(ModeFreq != medianFreq){
    leastFreq = c('weekly','biweekly','monthly','bimonthly','quarterly')[
      max(as.numeric(factor(ModeFreq,levels=c('weekly','biweekly','monthly','bimonthly','quarterly'))),
          as.numeric(factor(medianFreq,levels=c('weekly','biweekly','monthly','bimonthly','quarterly'))))]
    return(leastFreq)
  }else{
    return(ModeFreq)
  }
}


Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

strTo=function(s,c=":"){
  require(stringr)
  cpos=str_locate(string = s,pattern = c)[1]
  if(!all(is.na(cpos))){
    substr(s,1,cpos-1)
  }else{
    s
  }
}

strFrom=function(s,c=":"){
  require(stringr)
  cpos=str_locate(string = s,pattern = c)[1]
  if(!all(is.na(cpos))){
    substr(s,cpos+nchar(c),nchar(s))
  }else{
    s
  }
}

ldWFS <- function(urlIn,dataLocation,agency,case.fix=TRUE,method='curl'){
  require(XML)
  if(dataLocation=="web"){
    dl=try(download.file(urlIn,destfile=paste0("WFS",agency),method=method,quiet=T),silent=T)  #curl worked for um for nrc, wininet worked for others
    if('try-error'%in%attr(dl,'class')|
       'try-error'%in%attr(try(xmlParse(file = paste0("WFS",agency)),silent=T),'class')){
      method=ifelse(method=='curl',yes = 'wininet',no = 'curl')
      dl=try(download.file(urlIn,destfile=paste0("WFS",agency),method=method,quiet=T),silent=T)  #curl worked for um for nrc, wininet worked for others
    }
    if('try-error'%in%attr(dl,'class')){
      return(NULL)
    }
    if(case.fix)  cc(paste0("WFS",agency))
    xmlfile <- try(xmlParse(file = paste0("WFS",agency)),silent=T)
    unlink(paste0("WFS",agency))
    if('try-error'%in%attr(xmlfile,'class')){return(NULL)}
  } else if(dataLocation=="file"){
    cc(urlIn)
    message("trying file",urlIn,"\nContent type  'text/xml'\n")
    if(grepl("xml$",urlIn)){
      xmlfile <- xmlParse(urlIn)
    } else {
      xmlfile=FALSE
    }
  }
  return(xmlfile)
}

ldWQ <- function(url,agency,method='curl',module='',...){
  tempFileName=paste0("D:/LAWA/2020/tmp",module,agency,".xml")
  dl=try(download.file(url,destfile=tempFileName,method=method,quiet=T,...),silent = T)
  xmlfile <- try(xmlParse(file = tempFileName),silent=T)
  error<-try(as.character(sapply(getNodeSet(doc=xmlfile, path="//Error"), xmlValue)),silent=T)
  if('try-error'%in%attr(dl,'class')||is.null(xmlfile)||'try-error'%in%attr(xmlfile,'class')||length(error)>0){
    method=ifelse(method=='curl',yes = 'wininet',no = 'curl')
    dl=try(download.file(url,destfile=tempFileName,method=method,quiet=T,...),silent = T)
    xmlfile <- try(xmlParse(file = tempFileName),silent=T)
    error<-as.character(sapply(getNodeSet(doc=xmlfile, path="//Error"), xmlValue))
  }
  if('try-error'%in%attr(dl,'class')|is.null(xmlfile)|'try-error'%in%attr(xmlfile,'class')||length(error)>0){
    return(NULL)
  }
  # xmlfile <- try(xmlParse(file = tempFileName),silent=T)
  unlink(tempFileName)
  # if(is.null(xmlfile)|('try-error'%in%attr(xmlfile,'class'))){
  # return(NULL)
  # }else{
  # error<-as.character(sapply(getNodeSet(doc=xmlfile, path="//Error"), xmlValue))
  # if(length(error)==0){
  return(xmlfile)   # if no error, return the data
  # } else {
  # return(NULL)
  # }
  # }
}

ldLWQ <- function(...){
  ldWQ(...,module='L')
}

ldMWQ <- function(...){
  ldWQ(...,module='M')
}

# 
# ldLWQ <- function(url,agency,method='curl',...){
#   tempFileName=paste0("D:/LAWA/2020/tmpL",agency,".xml")
#   dl=try(download.file(url,destfile=tempFileName,method=method,quiet=T,...),silent = T)
#   xmlfile <- try(xmlParse(file = tempFileName),silent=T)
#   error<-try(as.character(sapply(getNodeSet(doc=xmlfile, path="//Error"), xmlValue)),silent=T)
#   if('try-error'%in%attr(dl,'class')||is.null(xmlfile)||'try-error'%in%attr(xmlfile,'class')||length(error)>0){
#     method=ifelse(method=='curl',yes = 'wininet',no = 'curl')
#     dl=try(download.file(url,destfile=tempFileName,method=method,quiet=T,...),silent = T)
#     xmlfile <- try(xmlParse(file = tempFileName),silent=T)
#     error<-as.character(sapply(getNodeSet(doc=xmlfile, path="//Error"), xmlValue))
#   }
#   if('try-error'%in%attr(dl,'class')|is.null(xmlfile)|'try-error'%in%attr(xmlfile,'class')||length(error)>0){
#     return(NULL)
#   }
#   # xmlfile <- try(xmlParse(file = tempFileName),silent=T)
#   unlink(tempFileName)
#   # if(is.null(xmlfile)|('try-error'%in%attr(xmlfile,'class'))){
#     # return(NULL)
#   # }else{
#     # error<-as.character(sapply(getNodeSet(doc=xmlfile, path="//Error"), xmlValue))
#     # if(length(error)==0){
#       return(xmlfile)   # if no error, return the data
#     # } else {
#       # return(NULL)
#     # }
#   # }
# }
# 
# ldMWQ <- function(url,agency,method='curl',...){
#   dl=try(download.file(url,destfile=paste0("D:/LAWA/2020/tmpM",agency,".xml"),method=method,quiet=T,...),silent = T)
#   if('try-error'%in%attr(dl,'class')){
#     method=ifelse(method=='curl',yes = 'wininet',no = 'curl')
#     dl=try(download.file(url,destfile=paste0("D:/LAWA/2020/tmpM",agency,".xml"),method=method,quiet=T,...),silent = T)
#   }
#   if('try-error'%in%attr(dl,'class')){
#     return(NULL)
#   }
#   xmlfile <- try(xmlParse(file = paste0("D:/LAWA/2020/tmpM",agency,".xml")),silent=T)
#   unlink(paste0("tmpM",agency,".xml"))
#   if(is.null(xmlfile)|('try-error'%in%attr(xmlfile,'class'))){
#     return(NULL)
#   }else{
#     error<-as.character(sapply(getNodeSet(doc=xmlfile, path="//Error"), xmlValue))
#     if(length(error)==0){
#       return(xmlfile)   # if no error, return the data
#     } else {
#       return(NULL)
#     }
#   }
# }
checkCSVageRiver <- function(agency,maxHistory=100){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"))){
        cat(agency,"CSV from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(stepBack)
      }
    }
    stepBack=stepBack+1
  }
  if(stepBack==maxHistory){
    cat(agency,"CSV not found within",maxHistory,"days\n")
    return(maxHistory)
  }
}

checkXMLageRiver <- function(agency,maxHistory=100){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"swq.xml"))){
        if(file.info(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/"
                            ,format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"swq.xml"))$size>1000){
          cat(agency,"XML from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
          return(stepBack)
        }
      }
    }
    stepBack=stepBack+1
  }
  if(stepBack==maxHistory){
    cat(agency,"not found within",maxHistory,"days\n")
    return(maxHistory)
  }
}

xml2csvRiver <- function(maxHistory=365,quiet=F,reportCensor=F,agency,reportVars=F,ageCheck=T,saves=T){
  if(!exists('transfers')){stop("Need to load the transfers table first")}
  suppressWarnings(try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),'%Y-%m-%d'),"/"))))
  suppressWarnings(rm(forcsv,xmlIn))
  if(ageCheck){
    CSVage = checkCSVageRiver(agency,maxHistory)
    XMLage = checkXMLageRiver(agency,maxHistory)
  }else{
    # DummyPlaceHolders to force run
    CSVage=5
    XMLage=1
  }
  if(CSVage>=XMLage){
    #Find XML file
    stepBack=0;rm(xmlIn)
    while(stepBack<maxHistory){
      if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                           format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
        if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/"
                              ,format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"swq.xml"))){
          if(file.info(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/"
                              ,format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"swq.xml"))$size>1000){
            if(!quiet)cat("loading",agency,"from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
            xmlIn <- xmlParse(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                                     format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"swq.xml"))
            break
          }
        }
      }
      stepBack=stepBack+1
    }
    if(!exists("xmlIn",inherits = F)){
      cat(agency,"not found, check abbrev. or increase search history\n")
      return(NULL)
    }else{
      varNames = unique(sapply(getNodeSet(doc=xmlIn,path="//Measurement/DataSource"),xmlGetAttr,name="Name"))
      if("WQ Sample"%in%varNames){
        cat("\t\tExcluding WQ Sample\n")
        varNames=varNames[-which(varNames=="WQ Sample")]
      }
      if(reportVars){
        cat(paste(varNames,collapse=",\t"),"\n")
      }
      siteTerm='CouncilSiteID'
      CouncilSiteIDs = unique(sapply(getNodeSet(doc=xmlIn,path=paste0("//Measurement")),xmlGetAttr,name="CouncilSiteID"))
      if(is.null(CouncilSiteIDs[[1]])){
        CouncilSiteIDs = unique(sapply(getNodeSet(doc=xmlIn,path=paste0("//Measurement")),xmlGetAttr,name="SiteName"))
        siteTerm='SiteName'
      }
      CouncilSiteIDslc=tolower(CouncilSiteIDs)
      #WFS returns a list of CouncilSiteID, SiteID and LawaSiteID
      #We call the SOS server with CouncilSiteID and it returns SiteName if its Hilltop, CouncilSiteID if its SOS
      #This script side effects turns them all into CouncilSiteID
      
      # table(CouncilSiteIDslc%in%tolower(siteTable$CouncilSiteID))
      # table(CouncilSiteIDslc%in%tolower(siteTable$SiteID))
      
      cat(length(CouncilSiteIDs),"\n")
      newRN=1
      for(sn in 1:length(CouncilSiteIDs)){
        siteDeets = grep(pattern = CouncilSiteIDslc[sn],x = siteTable$CouncilSiteID,ignore.case=T)
        if(length(siteDeets)==0){
          cat("A")
          siteDeets = which(tolower(siteTable$CouncilSiteID)==tolower(CouncilSiteIDs[sn]))
        }
        if(length(siteDeets)>1){
          cat("B")
          siteDeets=siteDeets[1]
        }
        if(length(siteDeets)!=1){
          cat("C")
          siteDeets=which(CouncilSiteIDslc[sn]==tolower(siteTable$LawaSiteID))
        }
        if(length(siteDeets)!=1){
          cat("D")
          siteDeets=which(CouncilSiteIDslc[sn]==tolower(siteTable$SiteID))
        }
        if(length(siteDeets)!=1){
          #This site may have been retired or removed, it didnt come up in the WFS 
          cat("\t",CouncilSiteIDs[sn],"missing from the WFS feed\n")
        }
        
        for(vn in 1:length(varNames)){
          dt=sapply(getNodeSet(doc=xmlIn, 
                               path=paste0("//Measurement[@",
                                           siteTerm," = '",CouncilSiteIDs[sn],
                                           "']/DataSource[@Name='",varNames[vn],"']/..//T")), xmlValue)
          vv=as.character(
            sapply(getNodeSet(doc=xmlIn, 
                              path=paste0("//Measurement[@",
                                          siteTerm," = '",CouncilSiteIDs[sn],
                                          "']/DataSource[@Name='",varNames[vn],"']/..//I1")), xmlValue))
          cens=sapply(getNodeSet(doc=xmlIn, 
                                 path=paste0("//Measurement[@",
                                             siteTerm," = '",CouncilSiteIDs[sn],
                                             "']/DataSource[@Name='",varNames[vn],"']/..//I2")),xmlValue)
          cenL=grepl(x = cens,pattern = "ND.<")
          cenR=grepl(x = cens,"ND.>")
          if(reportCensor){
            if(any(cenL)){cat(varNames[vn],"left censored\n")}
            if(any(cenR)){cat(varNames[vn],"right censored\n")}
          }
          if(length(dt)!=length(vv)){
            browser()
          }
          if(length(dt)>0){
            formattedDate=format.Date(strptime(dt,format="%Y-%m-%dT%H:%M:%S"),'%d-%b-%y')
            if(all(is.na(formattedDate))){
              formattedDate=format.Date(strptime(dt,format="%d/%m/%Y %H:%M"),'%d-%b-%y')
            }
            dtvv=data.frame(CouncilSiteID=rep(CouncilSiteIDslc[sn],length(dt)),
                            Date=formattedDate,
                            Value = vv,
                            # Method="",
                            Measurement=varNames[vn],
                            Censored=F,
                            CenType=F,
                            stringsAsFactors = F)
            rm(formattedDate)
            if(any(cenL)|any(cenR)){
              dtvv$Censored[cenL|cenR]=TRUE
              dtvv$CenType[which(cenL)]="Left"
              dtvv$CenType[which(cenR)]="Right"
            }
            if(length(siteDeets)==1){
              suppressWarnings({dtvv=cbind(dtvv,siteTable[siteDeets,-1])})  
            }else{
              stToBind=siteTable[1:dim(dtvv)[1],]
              for(cc in 1:dim(stToBind)[2]){
                stToBind[,cc]=NA
              }
              stToBind$Agency=agency
              dtvv=cbind(dtvv,stToBind)
              rm(stToBind)
            }
            eval(parse(text=paste0("dtvv",newRN,"=dtvv")))
            newRN=newRN+1
            # if(!exists("forcsv")){
            #   forcsv=dtvv
            # }else{
            #   forcsv=merge(forcsv,dtvv,all=T)
            # }
          }
        }
        if(!quiet){
          cat("\t",CouncilSiteIDs[sn],"\t",sn," of ",length(CouncilSiteIDs),"\n")
        }else{cat(".")}
      }
      
      listToCombine=ls(pattern="dtvv[[:digit:]]")
      eval(parse(text=paste0("forcsv=rbind.data.frame(",paste(listToCombine,collapse=","),")")))
      eval(parse(text=paste0("rm('",paste(listToCombine,collapse="','"),"')")))
      rm(listToCombine)
      
      #Can we deal here with the situation that we've got lab as well as field measures for things.
      labMeasureNames=unique(grep(pattern = 'lab',x = forcsv$Measurement,ignore.case=T,value = T))
      fieldMeasureNames=unique(grep(pattern = 'field',x = forcsv$Measurement,ignore.case=T,value = T))
      if(length(labMeasureNames)>0 & length(fieldMeasureNames)>0){
        if(any(gsub(pattern = 'Lab|lab',replacement = '',x = labMeasureNames)%in%
               gsub(pattern = 'Field|field',replacement = '',x = fieldMeasureNames))){
          matchy=gsub(pattern = 'Lab|lab',replacement = '',x = labMeasureNames)[gsub(pattern = 'Lab|lab',replacement = '',x = labMeasureNames)%in%
                                                                                  gsub(pattern = 'Field|field',replacement = '',x = fieldMeasureNames)]
          matchy=gsub(pattern = "[[:punct:]]|[[:space:]]",replacement='',x = matchy)
          cat(agency,'\t',matchy,'\n')
          for(mm in seq_along(matchy)){
            forsub=forcsv[grep(pattern = matchy[mm],x = forcsv$Measurement,ignore.case = T),]
            teppo = forsub%>%group_by(CouncilSiteID,Date,Measurement)%>%
              dplyr::summarise(Value=mean(as.numeric(Value),na.rm=T),
                               Censored=any(Censored),
                               CenType=ifelse(all(CenType=="FALSE"),
                                              'FALSE',
                                              ifelse(any(tolower(CenType)=='left'),
                                                     'Left',
                                                     ifelse(any(tolower(CenType)=='right'),
                                                            'Right',NA))))%>%
              spread(key = Measurement,value = Value)
            LabCol=grep('lab',names(teppo),ignore.case=T)
            FieldCol=grep('field',names(teppo),ignore.case=T)
            teppo$Value=unlist(teppo[,LabCol])
            teppo$Value[is.na(teppo$Value)]=unlist(teppo[is.na(teppo$Value),FieldCol])
            teppo=teppo[,-c(LabCol,FieldCol)]
            teppo$Measurement=matchy[mm]
            teppo <- left_join(teppo,unique(forcsv[,c(1,7:20)]),by="CouncilSiteID")%>%as.data.frame
            forcsv=rbind(forcsv[-grep(pattern = matchy[mm],x = forcsv$Measurement,ignore.case = T),],teppo)
            rm(teppo)
          }
        }
      }      
      
      
      #Rename the measurements that are in this council's transfers table held at this end
      thisAgencysCallNames=tolower(transfers$CallName[tolower(transfers$Agency) %in% tolower(agency)])
      thisAgencysLAWANames=transfers$LAWAName[tolower(transfers$Agency) %in% tolower(agency)]
      matchingMeasures=tolower(forcsv$Measurement) %in% thisAgencysCallNames
      forcsv$Measurement[matchingMeasures] =
        thisAgencysLAWANames[match(tolower(forcsv$Measurement[matchingMeasures]),thisAgencysCallNames)]
      
      excess=unique(forcsv$Measurement)[!unique(forcsv$Measurement)%in%lawaset]
      if(length(excess)>0){
        forcsv=forcsv[-which(forcsv$Measurement%in%excess),]
      }
      rm(excess)
      if(saves==T){
      write.csv(forcsv,
                file =paste0( "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                              format(Sys.Date(),'%Y-%m-%d'),"/",agency,".csv"),row.names=F)
      }
    }
  }else{
    print("Not converted, CSV is already newer than XML.  Or maybe there's a tiny XML indicating it failed for some reason.")
  }
}

loadLatestSiteTableRiver <- function(maxHistory=365){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/SiteTable_river",
                            format(Sys.Date()-stepBack,'%d%b%y'),".csv"))){
        cat("loading siteTable from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(read.csv(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                               format(Sys.Date()-stepBack,'%Y-%m-%d'),"/SiteTable_river",
                               format(Sys.Date()-stepBack,'%d%b%y'),".csv"),
                        stringsAsFactors = F))
      }
    }
    stepBack=stepBack+1
  }
  return(NULL)
}

loadLatestSiteTableLakes <- function(maxHistory=100){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/SiteTable_lakes",
                            format(Sys.Date()-stepBack,'%d%b%y'),".csv"))){
        cat("loading siteTable from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(read.csv(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                               format(Sys.Date()-stepBack,'%Y-%m-%d'),"/SiteTable_lakes",
                               format(Sys.Date()-stepBack,'%d%b%y'),".csv"),
                        stringsAsFactors = F))
      }
    }
    stepBack=stepBack+1
  }
  return(NULL)
}

loadLatestDataLakes <- function(){
  lakeDataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/Lakes/Data",pattern = "LakesWithMetadata.csv",
                            recursive = T,full.names = T,ignore.case=T),1)
  cat(lakeDataFileName)
  lakeData=read.csv(lakeDataFileName,stringsAsFactors = F)
  rm(lakeDataFileName)
  lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]=
    lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]*1000  #mg/L to mg/m3   #Until 4/10/18 also NH4N
  return(lakeData)
}

loadLatestSiteTableMacro <- function(maxHistory=365){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Macroinvertebrates/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Macroinvertebrates/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/SiteTable_macro",
                            format(Sys.Date()-stepBack,'%d%b%y'),".csv"))){
        cat("loading siteTable from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(read.csv(paste0("h:/ericg/16666LAWA/LAWA2020/Macroinvertebrates/Data/",
                               format(Sys.Date()-stepBack,'%Y-%m-%d'),"/SiteTable_macro",
                               format(Sys.Date()-stepBack,'%d%b%y'),".csv"),
                        stringsAsFactors = F))
      }
    }
    stepBack=stepBack+1
  }
  return(NULL)
}

loadLatestDataMacro <- function(){
  macroDataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/Macroinvertebrates/Data",pattern = "MacrosWithMetadata.csv",
                             recursive = T,full.names = T,ignore.case=T),1)
  cat(macroDataFileName)
  macroData=read.csv(macroDataFileName,stringsAsFactors = F)
  return(macroData)
}

loadLatestCSVRiver <- function(agency,maxHistory=365,silent=F){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"))){
        if(!silent)cat("loading",agency,"from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(read.csv(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                               format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"),stringsAsFactors = F))
      }
    }
    stepBack=stepBack+1
  }
  if(!silent)cat(agency,"not found within",maxHistory,"days\n")
  return(NULL)
}

loadLatestDataRiver <- function(){
  wqdataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data",
                          pattern = "AllCouncils.csv",
                          recursive = T,
                          full.names = T,
                          ignore.case = T),1)
  cat(wqdataFileName)
  wqdata=readr::read_csv(wqdataFileName,guess_max=150000)%>%as.data.frame
  rm(wqdataFileName)
  return(wqdata)
}

searchCSVRiver <- function(agency,maxHistory=365){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"))){
        cat(agency,"from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\t")
        # cat(paste(unique(forcsv$Measurement),collapse="\t"),"\n")
      }
    }
    stepBack=stepBack+1
  }
  return(NULL)
}

searchXMLRiver <- function(agency,maxHistory=365){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"swq.xml"))){
        cat(agency,"from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\t")
        xmlIn <- xmlParse(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"swq.xml"))
        varNames = unique(sapply(getNodeSet(doc=xmlIn,path="//Measurement/DataSource"),xmlGetAttr,name="Name"))
        cat(paste(varNames,collapse=",\t"),"\n")
      }
    }
    stepBack=stepBack+1
  }
}

checkReturnNamesRiver <- function(maxHistory=365,quiet=F,reportCensor=F,agency){
  #WFS returns a list of CouncilSiteID, SiteID and LawaSiteID
  #We call the SOS server with CouncilSiteID and it returns SiteName
  #So, the SiteNames should match the CouncilSiteID, right?!
  #What proportion do?
  rm(forcsv)
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"swq.xml"))){
        cat("loading",agency,"from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        xmlIn <- xmlParse(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                                 format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"swq.xml"))
        break
      }
    }
    stepBack=stepBack+1
  }
  if(!exists("xmlIn",inherits = F)){
    cat(agency,"not found, check abbrev. or increase search history\n")
    return(NULL)
  }else{
    siteNames = unique(tolower(sapply(getNodeSet(doc=xmlIn,path=paste0("//Measurement")),xmlGetAttr,name="SiteName")))
    cat(sum(siteNames%in%tolower(siteTable$CouncilSiteID))/length(siteNames),"\t")
    cat(sum(siteNames%in%tolower(siteTable$LAWASiteID))/length(siteNames),"\t")
    cat(sum(siteNames%in%tolower(siteTable$SiteID))/length(siteNames),"\n")
  }
}


loadLatestCSVmacro <- function(agency,maxHistory=365,quiet=F){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"))){
        if(!quiet)cat("loading",agency,"from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(read.csv(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                               format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"),
                        stringsAsFactors = F))
      }
    }
    stepBack=stepBack+1
  }
  cat("\n************",agency,"not found*************\n\n")
  return(NULL)
}

loadLatestMacroCombo <- function(maxHistory=100,quiet=F){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/MacrosCombined.csv"))){
        if(!quiet)cat("loading combined macros from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(read.csv(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                               format(Sys.Date()-stepBack,'%Y-%m-%d'),"/MacrosCombined.csv"),
                        stringsAsFactors = F))
      }
    }
    stepBack=stepBack+1
  }
  cat("\n************macros combo not found*************\n\n")
  return(NULL)
}

xml2csvMacro <- function(agency,maxHistory=365,quiet=F){
  suppressWarnings(try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),'%Y-%m-%d'),"/"))))
  stepBack=0
  suppressWarnings({rm(xmlIn)})
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"Macro.xml"))){
        cat("loading",agency,"from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        xmlIn <- xmlParse(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                                 format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"Macro.xml"))
        break
      }
    }
    stepBack=stepBack+1
  }
  if(!exists("xmlIn",inherits = F)){
    cat(agency,"not found, check abbrev. or increase search history\n")
    return(NULL)
  }else{
    
    varNames = unique(sapply(getNodeSet(doc=xmlIn,path="//Measurement/DataSource"),xmlGetAttr,name="Name"))
    if(varNames[1]=="WQ Sample"){
      varNames=c(varNames[-1],"WQ Sample")
    }
    siteTerm='CouncilSiteID'
    CouncilSiteIDs = unique(sapply(getNodeSet(doc=xmlIn,path=paste0("//Measurement")),xmlGetAttr,name="CouncilSiteID"))
    if(is.null(CouncilSiteIDs[[1]])){
      CouncilSiteIDs = unique(sapply(getNodeSet(doc=xmlIn,path=paste0("//Measurement")),xmlGetAttr,name="SiteName"))
      siteTerm='SiteName'
    }
    CouncilSiteIDslc=tolower(CouncilSiteIDs)
    cat(length(CouncilSiteIDslc),':')
    for(sn in 1:length(CouncilSiteIDs)){
      cat('.')
      for(vn in 1:length(varNames)){
        if(varNames[vn]!="WQ Sample"){
          dtv=sapply(getNodeSet(doc=xmlIn, paste0("//Measurement[@",siteTerm," = '",CouncilSiteIDs[sn],
                                                  "']/DataSource[@Name='",varNames[vn],"']/..//T")), xmlValue)
          vv=as.character(sapply(getNodeSet(doc=xmlIn, paste0("//Measurement[@",siteTerm," = '",CouncilSiteIDs[sn],
                                                              "']/DataSource[@Name='",varNames[vn],"']/..//I1")), xmlValue))
          
          if((length(dtv)*length(vv))>0){
            dtvv=data.frame(CouncilSiteID=rep(CouncilSiteIDslc[sn],length(dtv)),
                            Date=format.Date(strptime(dtv,format="%Y-%m-%dT%H:%M:%S"),'%d-%b-%y'),
                            Value = vv,
                            # Method="",
                            Measurement=varNames[vn],
                            stringsAsFactors = F)
            
            if(!exists("forcsv",inherits = F)){
              forcsv=dtvv
            }else{
              forcsv=merge(forcsv,dtvv,all=T)
            }
          }
        }else{
          next
          # browser()
          #Add WQ Sample deets to existing table
          dtv=sapply(getNodeSet(doc=xmlIn, paste0("//Measurement[@SiteName = '",CouncilSiteIDs[sn],
                                                  "']/DataSource[@Name='",varNames[vn],"']/..//T")), xmlValue)
          vv=strsplit(as.character(sapply(getNodeSet(doc=xmlIn,
                                                     paste0("//Measurement[@SiteName = '",CouncilSiteIDs[sn],
                                                            "']/DataSource[@Name='",varNames[vn],"']/..//I1")), xmlValue)),split = "\t")
          vv=lapply(X = vv,FUN=function(x){matrix(x,ncol=2,byrow = T)})
          for(stdt in 1:length(dtv)){
            #Find which existing rows to add WQ sample data to
            these = which(forcsv$CouncilSiteID==CouncilSiteIDs[sn] &
                            forcsv$Date==format.Date(strptime(dtv[stdt],format="%Y-%m-%dT%H:%M:%S"),'%d-%b-%y'))
            if(length(these)>0){
              #Check the column names exist, and add them if they dont
              if(any(!vv[[stdt]][,1]%in%names(forcsv))){
                for(newCol in which(!vv[[stdt]][,1]%in%names(forcsv))){
                  eval(parse(text=paste0("forcsv$`",vv[[stdt]][newCol,1],"`=NA")))
                }
              }
              #All columns now exist, add the values into them
              targetColumns=match(vv[[stdt]][,1],names(forcsv))
              for(rr in these){
                forcsv[rr,targetColumns]=(vv[[stdt]][,2])
              }
            }
          }
        }
      }
      if(!quiet){
        cat(paste0(CouncilSiteIDs[sn],"\t",sn," of ",length(CouncilSiteIDs),"\n"))
      }
    }
  }
  return(forcsv)
}

checkXMLageLakes <- function(agency,maxHistory=100){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"lwq.xml"))){
        cat(agency,"XML from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(stepBack)
      }
    }
    stepBack=stepBack+1
  }
  if(stepBack==maxHistory){
    cat(agency,"not found within",maxHistory,"days\n")
    return(NA)
  }
}
checkXMLageMacro <- function(agency,maxHistory=100){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"Macro.xml"))){
        cat(agency,"XML from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(stepBack)
      }
    }
    stepBack=stepBack+1
  }
  if(stepBack==maxHistory){
    cat(agency,"not found within",maxHistory,"days\n")
    return(NA)
  }
}
checkCSVageMacros <- function(agency,maxHistory=100){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"))){
        cat(agency,"CSV from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(stepBack)
      }
    }
    stepBack=stepBack+1
  }
  if(stepBack==maxHistory){
    cat(agency,"not found within",maxHistory,"days\n")
    return(NA)
  }
}
checkCSVageLakes <- function(agency,maxHistory=100){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"))){
        cat(agency,"CSV from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        return(stepBack)
      }
    }
    stepBack=stepBack+1
  }
  if(stepBack==maxHistory){
    cat(agency,"not found within",maxHistory,"days\n")
    return(NA)
  }
}
loadLatestCSVLake <- function(agency,maxHistory=365,quiet=F){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"))){
        if(!quiet){
          cat("loading",agency,"from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        }
        return(read.csv(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                               format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,".csv"),
                        stringsAsFactors = F))
      }
    }
    stepBack=stepBack+1
  }
  cat("\n************",agency,"not found*************\n\n")
  return(NULL)
}
loadLatestColumnHeadingLake <- function(agency,maxHistory=365,quiet=F){
  stepBack=0
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"LakeDataColumnLabels.csv"))){
        if(!quiet){
          cat("loading",agency,"column names from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        }
        return(read.csv(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                               format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"LakeDataColumnLabels.csv"),
                        stringsAsFactors = F))
      }
    }
    stepBack=stepBack+1
  }
  cat("\n************",agency,"not found*************\n\n")
  return(NULL)
}
xml2csvLake <- function(agency,quiet=F,maxHistory=100){
  suppressWarnings(try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),'%Y-%m-%d'),"/"))))
  suppressWarnings(rm(forcsv,xmlIn))
  stepBack=0;rm(xmlIn)
  while(stepBack<maxHistory){
    if(dir.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                         format(Sys.Date()-stepBack,'%Y-%m-%d'),"/"))){
      if(file.exists(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                            format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"lwq.xml"))){
        cat("\nloading",agency,"from",stepBack,"days ago,",format(Sys.Date()-stepBack,'%Y-%m-%d'),"\n")
        xmlIn <- xmlParse(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                                 format(Sys.Date()-stepBack,'%Y-%m-%d'),"/",agency,"lwq.xml"))
        break
      }
    }
    stepBack=stepBack+1
  }
  if(!exists("xmlIn",inherits = F)){
    cat("\n************",agency,"not found*************\n\n")
    return(NULL)
  }else{
    varNames = unique(sapply(getNodeSet(doc=xmlIn,path="//Measurement/DataSource"),xmlGetAttr,name="Name"))
    if(varNames[1]=="WQ Sample"){
      varNames=c(varNames[-1],"WQ Sample")
    }
    siteTerm='CouncilSiteID'
    CouncilSiteIDs = unique(sapply(getNodeSet(doc=xmlIn,path=paste0("//Measurement")),xmlGetAttr,name="CouncilSiteID"))
    if(is.null(CouncilSiteIDs[[1]])){
      CouncilSiteIDs = unique(sapply(getNodeSet(doc=xmlIn,path=paste0("//Measurement")),xmlGetAttr,name="SiteName"))
      siteTerm='SiteName'
    }
    CouncilSiteIDslc=tolower(CouncilSiteIDs)
    for(sn in 1:length(CouncilSiteIDs)){
      for(vn in 1:length(varNames)){
        if(varNames[vn]!="WQ Sample"){
          dtv=sapply(getNodeSet(doc=xmlIn, paste0("//Measurement[@",siteTerm," = '",CouncilSiteIDs[sn],
                                                  "']/DataSource[@Name='",varNames[vn],"']/..//T")), xmlValue)
          vv=as.character(sapply(getNodeSet(doc=xmlIn, paste0("//Measurement[@",siteTerm," = '",CouncilSiteIDs[sn],
                                                              "']/DataSource[@Name='",varNames[vn],"']/..//I1")), xmlValue))
          vv[vv=="NA"]=NA
          vv[vv==""]=NA
          if((length(dtv)*length(vv))>0){
            datesToAdd=format.Date(strptime(dtv,format="%Y-%m-%dT%H:%M:%S"),'%d-%b-%y')
            if(all(is.na(datesToAdd))){
              datesToAdd=format.Date(strptime(dtv,format="%Y-%m-%d"),'%d-%b-%y')
            }
            if(all(is.na(datesToAdd))){
              stopifnot(!any(is.na(as.numeric(dtv))))
              datesToAdd=format.Date(as.POSIXct(as.numeric(dtv)-946771200,origin=lubridate::origin),'%d-%b-%y')
            }
            
            i2=as.character(sapply(getNodeSet(doc=xmlIn, paste0("//Measurement[@",siteTerm," = '",CouncilSiteIDs[sn],
                                                                "']/DataSource[@Name='",varNames[vn],"']/..//I2")), xmlValue))
            centype = rep(F,length(vv))
            suppressWarnings({rm(cenL,cenR,isCensored)})
            cenL=rep(FALSE,length(vv))
            cenR=rep(FALSE,length(vv))            
            if(length(i2)>0){
              #if censoring is coded on tag I2
              cenL = grepl(x=i2,pattern="&lt|<",ignore.case = T)
              cenR = grepl(x=i2,pattern="&gt|>",ignore.case = T)
            }
            #If censoring is coded on the value
            cenL = cenL|grepl(x=vv,pattern="&lt|<",ignore.case = T)
            cenR = cenR|grepl(x=vv,pattern="&gt|>",ignore.case = T)
            # centype = rep(F,length(vv))
            # centype[which(cenL)]="Left"       #Changes made here because hrc had I2 tags but not for coding censoring
            # centype[which(cenR)]="Right"
            # isCensored=cenL|cenR
            isCensored=cenL|cenR
            if(any(grepl(x=vv,pattern="<|>",ignore.case = T))){
              vv[grepl(x=vv,pattern="<|>",ignore.case = T)]=substr(vv[grepl(x=vv,pattern="<|>",ignore.case = T)],
                                                                   2,nchar(vv[grepl(x=vv,pattern="<|>",ignore.case = T)]))
            }
            if(any(grepl(x=vv,pattern="&lt|&gt",ignore.case = T))){
              vv[grepl(x=vv,pattern="&lt|&gt",ignore.case = T)]=substr(vv[grepl(x=vv,pattern="&lt|&gt",ignore.case = T)],
                                                                       3,nchar(vv[grepl(x=vv,pattern="&lt|&gt",ignore.case = T)]))
            }
            centype[which(cenL)]="Left"
            centype[which(cenR)]="Right"
            stopifnot(sum(is.na(as.numeric(vv[!is.na(vv)])))==0)
            
            dtvv=data.frame(CouncilSiteID=rep(CouncilSiteIDslc[sn],length(dtv)),
                            Date=datesToAdd,
                            Value = vv,
                            Method="",
                            Measurement=varNames[vn],
                            Censored=isCensored,
                            centype=centype,
                            stringsAsFactors = F)
            
            if(!exists("forcsv",inherits = F)){
              forcsv=dtvv
            }else{
              forcsv=merge(forcsv,dtvv,all=T)
            }
          }
        }else{
          next
          # browser()
          #Add WQ Sample deets to existing table
          dtv=sapply(getNodeSet(doc=xmlIn, paste0("//Measurement[@",siteTerm," = '",CouncilSiteIDs[sn],
                                                  "']/DataSource[@Name='",varNames[vn],"']/..//T")), xmlValue)
          vv=strsplit(as.character(sapply(getNodeSet(doc=xmlIn,
                                                     paste0("//Measurement[@",siteTerm," = '",CouncilSiteIDs[sn],
                                                            "']/DataSource[@Name='",varNames[vn],"']/..//I1")), xmlValue)),split = "\t")
          vv=lapply(X = vv,FUN=function(x){matrix(x,ncol=2,byrow = T)})
          for(stdt in 1:length(dtv)){
            #Find which existing rows to add WQ sample data to
            these = which(forcsv[,siteTerm]==CouncilSiteIDs[sn] &
                            forcsv$Date==format.Date(strptime(dtv[stdt],format="%Y-%m-%dT%H:%M:%S"),'%d-%b-%y'))
            if(length(these)>0){
              #Check the column names exist, and add them if they dont
              if(any(!vv[[stdt]][,1]%in%names(forcsv))){
                for(newCol in which(!vv[[stdt]][,1]%in%names(forcsv))){
                  eval(parse(text=paste0("forcsv$`",vv[[stdt]][newCol,1],"`=NA")))
                }
              }
              #All columns now exist, add the values into them
              targetColumns=match(vv[[stdt]][,1],names(forcsv))
              for(rr in these){
                forcsv[rr,targetColumns]=(vv[[stdt]][,2])
              }
            }
          }
        }
      }
      if(!quiet){
        cat(paste0(CouncilSiteIDs[sn],"\t",sn," of ",length(CouncilSiteIDs),"\n"))
      }
    }
    return(forcsv)
  }
}

