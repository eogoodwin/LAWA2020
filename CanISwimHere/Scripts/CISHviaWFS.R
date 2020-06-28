rm(list=ls())
library(tidyverse)
source('H:/ericg/16666LAWA/LAWA2019/Scripts/LAWAFunctions.R')
try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)
try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)

checkXMLFile <- function(regionName,siteName,propertyName){
  xmlfile<-read_xml(paste0('./data/DataCache/',regionName,siteName,propertyName,'.xml'))
  if('ows'%in%names(xml_ns(xmlfile)) && length(xml_find_all(xmlfile,'ows:Exception'))>0){
    #Dont wanna keep exception files
    file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
    return(1);#next
  }
  #Got Data
  if('wml2'%in%names(xml_ns(xmlfile))){
    mvals <- xml_find_all(xmlfile,'//wml2:value')
    if(length(mvals)>0){
      return(2);#hoorah break
    }else{
      #Dont wanna keep empty files
      file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
    }
    return(1);#next
  }else{
    if(propertyName=="WQ.sample"){
      pvals=xml_find_all(xmlfile,'//Parameter')
      if(length(pvals)>0){
        return(2);
      }else{
        file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
      }
      rm(pvals)
    }else{
      #Remove non WML files
      file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
      return(1);#next
    }
  }
}
readXMLFile <- function(regionName,siteName,propertyName,property){
  xmlfile<-read_xml(paste0('./data/DataCache/',regionName,siteName,propertyName,'.xml'))
  #Check for exceptions
  if('ows'%in%names(xml_ns(xmlfile)) && length(xml_find_all(xmlfile,'ows:Exception'))>0){
    cat('-')
    excCode <- try(xml_find_all(xmlfile,'//ows:Exception'))
    if(length(excCode)>0){
      commentToAdd=xml_text(excCode)
    }else{
      commentToAdd='exception'
    }
    #Dont wanna keep exception files
    file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
    return(1);#next
  }
  #Check for Data
  if('wml2'%in%names(xml_ns(xmlfile))){
    mvals <- xml_find_all(xmlfile,'//wml2:value')
    if(length(mvals)>0){
      cat('*')
      mvals=xml_text(mvals)
      faceVals=as.numeric(mvals)
      if(any(lCens<-unlist(lapply(mvals,FUN = function(x)length(grepRaw(pattern = '<',x=x))>0)))){
        faceVals[lCens]=readr::parse_number(mvals[lCens])
      }
      if(any(rCens<-unlist(lapply(mvals,FUN = function(x)length(grepRaw(pattern = '>',x=x))>0)))){
        faceVals[rCens]=readr::parse_number(mvals[rCens])
      }
      
      mT<-xml_find_all(xmlfile,'//wml2:time')
      mT<-xml_text(mT)  # get time text
      mT <- strptime(mT,format='%Y-%m-%dT%H:%M:%S')
      siteDat=data.frame(region=SSMregion,
                         # LawaSiteID=trimws(ssm$LawaId[siteNum]),
                         siteName=siteName,
                         # siteType=trimws(ssm$SiteType[siteNum]),
                         propertyName=propertyName,
                         property=property,
                         dateCollected=mT,
                         val=faceVals,
                         lCens=lCens,
                         rCens=rCens)%>%
        plyr::mutate(week=lubridate::isoweek(mT),
                     month=lubridate::month(mT),
                     year=lubridate::isoyear(mT),
                     YW=paste0(year,week))
      eval(parse(text=paste0(make.names(paste0(regionName,siteName,propertyName)),"<<-siteDat")))
      listToCombine <<- c(listToCombine,make.names(paste0(regionName,siteName,propertyName)))
      rm(siteDat)
      return(2)#        break
    }else{
      #Dont wanna keep empty files
      file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
    }
    return(1);#next
  }else{
    #Check is it WQSample
    if(propertyName=="WQ.sample"){
      xmlfile=xmlParse(paste0('./data/DataCache/',regionName,siteName,propertyName,'.xml'))
      xmltop<-xmlRoot(xmlfile)
      m<-xmltop[['Measurement']]
      if(!is.null(m)){
        dtV <- xpathApply(m,"//T",xmlValue)
        dtV <- unlist(dtV)
        siteMetaDat=data.frame(regionName=regionName,siteName=siteName,property=property,SampleDate=dtV)
        for(k in 1:length(dtV)){
          p <- m[["Data"]][[k]]
          c <- length(xmlSApply(p, xmlSize))
          if(c==1){next}
          newDF=data.frame(regionName=regionName,siteName=siteName,property=property,SampleDate=dtV[k])
          for(n in 2:c){   
            if(!is.null(p[[n]])){
              metaName  <- as.character(xmlToList(p[[n]])[1])   ## Getting the name attribute
              metaValue <- as.character(xmlToList(p[[n]])[2])   ## Getting the value attribute
              metaValue <- gsub('\\"|\\\\','',metaValue)
              eval(parse(text=paste0("newDF$`",metaName,"`=\"",metaValue,"\"")))
            }
          }
          siteMetaDat=merge(siteMetaDat,newDF,all = T)
        }
      }
      mtCols = which(apply(siteMetaDat,2,function(x)all(is.na(x)|x=="NA")))
      if(length(mtCols)>0){
        siteMetaDat=siteMetaDat[,-mtCols]
      }
      eval(parse(text=paste0(make.names(paste0(regionName,siteName,propertyName)),"<<-siteMetaDat")))
      metaToCombine <<- c(metaToCombine,make.names(paste0(regionName,siteName,propertyName)))
      rm(siteMetaDat)
      return(2)#        break
    }else{
      file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
      return(1);#next
    }
  }
}


ssm = read.csv('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/MetaData/SwimSiteMonitoringResults-2019-08-01.csv',stringsAsFactors = F)
WFSsiteTable = read.csv(tail(dir(path="H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data",pattern='SiteTable',
                                 recursive=T,full.names=T,ignore.case=T),1),stringsAsFactors = F)
RegionTable=data.frame(ssm=unique(ssm$Region),
                       wfs=c("bay of plenty","canterbury","gisborne","hawkes bay",
                             "horizons","marlborough","nelson","northland",
                             "otago","southland","taranaki","tasman",
                             "waikato","wellington","west coast"))


addSSMsites=F
#Download data to XML files ####
library(xml2)
agencyURLtable=data.frame(region="",server="",property='',feature='')
library(parallel)
library(doParallel)
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  listToCombine=NULL
  library(magrittr)
  library(doBy)
  library(plyr)
  library(dplyr)
  library(stringr)
  library(xml2)
})
foreach(SSMregion = unique(ssm$Region),.combine=rbind,.errorhandling="stop")%dopar%{
  regionName=word(SSMregion,1,1)
  WFSregion=RegionTable$wfs[RegionTable$ssm==SSMregion]
  cat('\n\n\n',SSMregion,'\n')
  #Find agency URLs
  (agURLs <- sort(unique(c(unlist(sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],
                                         FUN = function(x)unlist(strsplit(x,split='&')))))))%>%
      grep('http',x = .,ignore.case=T,value=T))%>%paste(collapse='\t')%>%cat
  if(any(!grepl('^http',agURLs))){
    agURLs = agURLs[grepl('^http',agURLs)]
  }
  # if(regionName=="Manawatu-Wanganui"){
  #   agURLs = gsub(pattern = 'hilltopserver',replacement = 'tsdata',x = agURLs)
  # }
  if(regionName=='Canterbury'){
    agURLs = 'http://wateruse.ecan.govt.nz/WQRecLawa.hts?Service=Hilltop'
  }
  if(regionName=="West"){
    agURLs = gsub(pattern = 'data.wcrc.govt.nz:9083',replacement = 'hilltop.wcrc.govt.nz',x = agURLs)
  }
  if(regionName=="Waikato"){
    agURLs = gsub(pattern="datasource=0$",replacement = "datasource=0&service=SOS&agency=LAWA&procedure=CBACTO.SampleResults.P",agURLs)
  }
  if(regionName=="Hawke's"){
    agURLs = gsub(pattern="EMAR.hts",replacement = "Recreational_WQ.hts",agURLs)%>%unique
  }
  
  #Find agency properties from the SSM file
  observedProperty <- sapply(X = ssm%>%filter(Region==SSMregion,TimeseriesUrl!='')%>%select(TimeseriesUrl),
                             FUN = function(x){gsub('(.+)http.+','\\1',x=x)%>%
                                 strsplit(split='&')%>%unlist%>%
                                 grep('observedproperty',x = .,ignore.case=T,value=T)%>%
                                 gsub('observedproperty=','',x=.,ignore.case = T)})%>%
    sapply(.,URLdecode)%>%unname
  propertyType = ssm%>%
    filter(Region==SSMregion&TimeseriesUrl!='')%>%
    select(Property)%>%
    unlist%>%
    unname
  stopifnot(length(observedProperty)==length(propertyType))
  agProps=unique(data.frame(observedProperty,propertyType,stringsAsFactors=F))
  agProps=rbind(agProps,c('WQ sample','WQ sample'))
  rm(observedProperty,propertyType)
  
  #Find agency Sites from the WFS reportage and add SSM-sourced sites if necessary
  agSites = WFSsiteTable%>%
    dplyr::filter(Region==WFSregion)%>%dplyr::select(CouncilSiteID)%>%
    unlist%>%trimws%>%unique%>%sort
  if(addSSMsites){
    agSites <- c(agSites,
                 c(unlist(sapply(X = ssm%>%filter(Region==SSMregion)%>%select(TimeseriesUrl),FUN = function(x)unlist(strsplit(x,split='&')))))%>%
                   grep('featureofinterest',x = .,ignore.case=T,value=T)%>%gsub('featureofinterest=','',x=.,ignore.case = T)%>%sapply(.,URLdecode)%>%trimws%>%tolower%>%unique)
  }
  
  #Find agency format keys
  (agFormats <- sort(unique(c(unlist(sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],
                                            FUN = function(x)unlist(strsplit(x,split='&')))))))%>%
      grep('format',x = .,ignore.case=T,value=T))%>%cat
  
  #Load each site/property combo, by trying each known URL for that council.  Already-existing files will not be reloaded
  cat('\n',regionName,'\n')
  cat(length(agSites)*length(agProps)*length(agURLs),'\n')
  for(as in seq_along(agSites)){
    siteName=make.names(agSites[as])
    for(ap in seq_along(agProps$observedProperty)){
      propertyName=make.names(agProps$observedProperty[ap])
      fname = paste0('./data/DataCache/',regionName,siteName,propertyName,'.xml')
      if(file.exists(fname)&&file.info(fname)$size>2000){
        next
      }else{
        for(ur in seq_along(agURLs)){
          urlToTry = paste0(agURLs[ur],
                            '&request=GetObservation',
                            '&featureOfInterest=',agSites[as],
                            '&observedProperty=',agProps$observedProperty[ap],
                            '&temporalfilter=om:phenomenonTime,P15Y/2019-06-01')
          if(length(agFormats)==1){
            urlToTry = paste0(urlToTry,'&',agFormats[1])
          }
          
          if(propertyName=="WQ.sample"){
            #http://wateruse.ecan.govt.nz/bathing.hts?service=SOS&request=GetObservation&featureOfInterest=sq30925&observedProperty=WQ sample&temporalfilter=om:phenomenonTime,P15Y/2019-06-01
            #http://wateruse.ecan.govt.nz/WQRecLawa.hts?Service=Hilltop&Request=GetData&Site=SQ30925&Measurement=WQ%20Sample&From=01/01/2019&To=31/01/2019
            urlToTry=gsub(x = urlToTry,pattern='ObservedProperty=.*&',replacement="Measurement=WQ Sample&",ignore.case = T)
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop',ignore.case = T)
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData',ignore.case = T)
            urlToTry=gsub(x=urlToTry,pattern='FeatureOfInterest',replacement = 'Site',ignore.case = T)
            urlToTry=gsub(x=urlToTry,pattern='temporalfilter=om:phenomenonTime,P15Y/2019-06-01',
                          replacement = 'From=1/6/2004&To=1/6/2019',ignore.case = T)
          }
          urlToTry=URLencode(urlToTry)
          dlTry=try(curl::curl_fetch_disk(urlToTry,path = fname),silent=T)
          if('try-error'%in%attr(x = dlTry,which = 'class')){
            file.remove(fname)
            cat('-')
            next
          }
          if(dlTry$status_code%in%c(400,500,501,503,404)){
            file.remove(fname)
            cat('.')
            next
          }
          if(dlTry$status_code==408){
            file.remove(fname)
            cat(agURLS[ur],'TO\n')
            next
          }
          response=checkXMLFile(regionName,siteName,propertyName)
          if(response==1){next}  #try the other URL
          if(response==2){break} #got a data
        }
      }
    }
  }
  return(NULL)
}
stopCluster(workers)
rm(workers)




#Load XML files and combine ####
agencyURLtable=data.frame(region="",server="",property='',feature='')
listToCombine=NULL
metaToCombine=NULL
for(SSMregion in unique(ssm$Region)){
  regionName=word(SSMregion,1,1)
  WFSregion=RegionTable$wfs[RegionTable$ssm==SSMregion]
  cat('\n',regionName,'\t')
  
  #Find agency properties from the SSM file
  observedProperty <- sapply(X = ssm%>%filter(Region==SSMregion,TimeseriesUrl!='')%>%select(TimeseriesUrl),
                             FUN = function(x){gsub('(.+)http.+','\\1',x=x)%>%
                                 strsplit(split='&')%>%unlist%>%
                                 grep('observedproperty',x = .,ignore.case=T,value=T)%>%
                                 gsub('observedproperty=','',x=.,ignore.case = T)})%>%
    sapply(.,URLdecode)%>%unname
  propertyType = ssm%>%
    filter(Region==SSMregion&TimeseriesUrl!='')%>%
    select(Property)%>%
    unlist%>%
    unname
  stopifnot(length(observedProperty)==length(propertyType))
  agProps=unique(data.frame(observedProperty,propertyType,stringsAsFactors=F))
  agProps=rbind(agProps,c('WQ sample','WQ sample'))
  rm(observedProperty,propertyType)
  
  #Find agency Sites from the WFS reportage and add SSM-sourced sites if necessary
  agSites = WFSsiteTable%>%
    dplyr::filter(Region==WFSregion)%>%dplyr::select(CouncilSiteID)%>%
    unlist%>%trimws%>%unique%>%sort
  if(addSSMsites){
    agSites <- c(agSites,
                 c(unlist(sapply(X = ssm%>%filter(Region==SSMregion)%>%select(TimeseriesUrl),FUN = function(x)unlist(strsplit(x,split='&')))))%>%
                   grep('featureofinterest',x = .,ignore.case=T,value=T)%>%gsub('featureofinterest=','',x=.,ignore.case = T)%>%sapply(.,URLdecode)%>%trimws%>%tolower%>%unique)
  }
  
  #Step through sites and properties, loading XML files to memory for subsequent combination
  for(as in seq_along(agSites)){
    cat('.')
    siteName=make.names(agSites[as])
    for(ap in seq_along(agProps$observedProperty)){
      propertyName=make.names(agProps$observedProperty[ap])
      if(file.exists(paste0('./data/DataCache/',regionName,siteName,propertyName,'.xml'))&
         file.info(paste0('./data/DataCache/',regionName,siteName,propertyName,'.xml'))$size>2000){
        readXMLFile(regionName,siteName,propertyName,property=agProps$propertyType[ap]) #returns 1 or 2 
      }
    }
  }
  rm(agProps,agSites)
  dflist <- lapply(grep(regionName,metaToCombine,ignore.case = T,value=T),get)
  recMetaData=Reduce(function(x,y) dplyr::full_join(x,y),dflist)
  eval(parse(text=paste0(make.names(regionName),"MD=recMetaData")))
}

eval(parse(text=paste0("recData=rbind.data.frame(",paste(listToCombine,collapse=','),")")))
eval(parse(text=paste0("rm('",paste(listToCombine,collapse="','"),"')")))
rm(listToCombine)

MDlist <- sapply(ls(pattern='MD$'),get)
MDlist[sapply(MDlist,is.null)] <- NULL
sapply(MDlist,names)
recMetaData=Reduce(function(x,y) dplyr::full_join(x,y),MDlist)
print(dim(recMetaData))
save(recMetaData,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                           "/RecMetaData",format(Sys.time(),'%Y-%b-%d'),".Rdata"))

if(0){
  rm(list=ls(pattern="WQ.sample$"))
}
apply(CanterburyMD,2,function(x)any(grepl('retest|routine|resample',x,ignore.case=T)))%>%which
apply(CanterburyMD,2,function(x)grep('retest|routine|resample',x,ignore.case=T,value=T)%>%unlist%>%unique)
apply(recMetaData,2,function(x)any(grepl('retest|routine|resample',x,ignore.case=T)))%>%which

#In each of the columns containing retest or routine or resample, what are the terms containing the words retest, routine or resample?
apply(recMetaData[recMetaData$regionName!="Canterbury",
                  apply(recMetaData[recMetaData$regionName!="Canterbury",],
                        2,function(x)any(grepl('retest|routine|resample',x,ignore.case=T)))],
      2,function(x)grep('retest|routine|resample',x,ignore.case=T,value=T)%>%unlist%>%table)

apply(recMetaData%>%dplyr::select(-"Analysts Comments",-"Sample Comment")
      [,which(apply(recMetaData%>%dplyr::select(-"Analysts Comments",-"Sample Comment"),
              2,
              function(x)any(grepl('retest|routine|resample',x,ignore.case=T))))],
      2,
      function(x)grep('retest|routine|resample',x,ignore.case=T,value=T)%>%unlist%>%table)



save(recData,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                           "/RecData",format(Sys.time(),'%Y-%b-%d'),".Rdata"))
# load(tail(dir(path="h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/",pattern="RecData2019",recursive = T,full.names = T,ignore.case=T),1))  

recData$LawaSiteID = WFSsiteTable$LawaSiteID[match(tolower(as.character(recData$siteName)),
                                                   tolower(make.names(WFSsiteTable$CouncilSiteID)))]
recData$siteType = ssm$SiteType[match(tolower(recData$LawaSiteID),tolower(ssm$LawaId))]

recData%>%dplyr::group_by(region)%>%dplyr::summarise(nSite=length(unique(siteName)))
recData%>%dplyr::filter(region=="Canterbury region")%>%select(LawaSiteID,property)%>%table



#Deal with censored data
#Anna Madarasz-Smith 1/10/2018
#Hey guys,
# We (HBRC) have typically used the value with the >reference 
# (eg >6400 becomes 6400), and half the detection of the less than values (e.g. <1 becomes 0.5).  I think realistically you can treat
# these any way as long as its consistent beccuase it shouldn’t matter to your 95th%ile.  These values should be in the highest and lowest of the range.
# Hope this helps Cheers
# Anna
recData$val=as.numeric(recData$val)
recData$fVal=recData$val
recData$fVal[recData$lCens]=recData$val[recData$lCens]/2
recData$fVal[recData$rCens]=recData$val[recData$rCens]
table(recData$lCens)
table(recData$rCens)


#Make all yearweeks six characters
recData$week = lubridate::week(recData$dateCollected)
recData$week=as.character(recData$week)
recData$week[nchar(recData$week)==1]=paste0('0',recData$week[nchar(recData$week)==1])

recData$year = lubridate::year(recData$dateCollected)

recData$YW=as.numeric(paste0(recData$year,recData$week))

#Create bathing seasons
bs=strTo(recData$dateCollected,'-')
bs[recData$month>6]=paste0(recData$year[recData$month>6],'/',recData$year[recData$month>6]+1)
bs[recData$month<7]=paste0(recData$year[recData$month<7]-1,'/',recData$year[recData$month<7])
recData$bathingSeason=bs
table(recData$bathingSeason)



recData%>%
  # filter(YW>201525)%>%
  dplyr::filter(LawaSiteID!='')%>%drop_na(LawaSiteID)%>%
  dplyr::filter(property!='Cyanobacteria')%>%
  dplyr::filter(year>2009&(month>10|month<4))%>% #bathign season months only
  dplyr::group_by(LawaSiteID,YW,property)%>%
  dplyr::arrange(YW)%>%                   
  dplyr::summarise(dateCollected=first(dateCollected),
                   region=unique(region),
                   n=length(fVal),            #Count number of weeks recorded per season
                   fVal=first(fVal),
                   bathingSeason=unique(bathingSeason))%>%
  ungroup->graphData

write.csv(graphData,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",
                                  format(Sys.Date(),'%Y-%m-%d'),
                                  "/CISHgraph",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)

##############################
##############################
graphData%>%
  select(-dateCollected)%>%
  group_by(LawaSiteID,property)%>%            #For each site
  dplyr::summarise(region=unique(region),nBS=length(unique(bathingSeason)),
                   nPbs=paste(as.numeric(table(bathingSeason)),collapse=','),
                   min=min(fVal,na.rm=T),
                   max=max(fVal,na.rm=T),
                   haz95=quantile(fVal,probs = 0.95,type = 5,na.rm = T),
                   haz50=quantile(fVal,probs = 0.5,type = 5,na.rm = T))%>%ungroup->CISHsiteSummary

#instantaneous thresholds, shouldnt be applied to percentiles
# CISHsiteSummary$BathingRisk=cut(x = CISHsiteSummary$haz95,breaks = c(-0.1,140,280,Inf),labels=c('surveillance','warning','alert'))

#https://www.mfe.govt.nz/sites/default/files/microbiological-quality-jun03.pdf
#For enterococci, marine:                      E.coli, freshwater
#table D1 A is 95th %ile < 40       table E1     <= 130
#         B is 95th %ile 41-200                  131 - 260
#         C is 95th %ile 201-500                 261 - 550
#         D is 95th %ile >500                       >550
CISHsiteSummary$marineEnt=cut(x=CISHsiteSummary$haz95,breaks = c(-0.1,40,200,500,Inf),labels=c("A","B","C","D"))
CISHsiteSummary$marineEnt[CISHsiteSummary$property!='Enterococci'] <- NA
CISHsiteSummary$fwEcoli=cut(x=CISHsiteSummary$haz95,breaks = c(-0.1,130,260,550,Inf),labels=c("A","B","C","D"))
CISHsiteSummary$fwEcoli[CISHsiteSummary$property!='E-coli'] <- NA
table(CISHsiteSummary$marineEnt)
table(CISHsiteSummary$fwEcoli)

#For LAWA bands
#30 needed over 3 seasons and 10 per year
#              marine               fresh
#              enterococci          e.coli
#A            <200                  <260
#B             201-500              261-550
#C                >500                 >550
CISHsiteSummary$LawaBand=cut(x=CISHsiteSummary$haz95,breaks=c(-0.1,200,500,Inf),labels=c("A","B","C"))
CISHsiteSummary$LawaBand[CISHsiteSummary$property!='Enterococci'] <- 
  cut(x=CISHsiteSummary$haz95[CISHsiteSummary$property!='Enterococci'],
      breaks=c(-0.1,260,550,Inf),labels=c("A","B","C"))

nPbs=do.call(rbind,lapply(CISHsiteSummary$nPbs,FUN = function(x)lapply(strsplit(x,split = ','),as.numeric))) #get the per-season counts
tooFew = which(do.call('rbind',lapply(nPbs,function(x)any(x<10)|length(x)<3)))  #see if any per-season counts are below ten, or there are fewer than three seasons
CISHsiteSummary$LawaBand[tooFew]=NA #set those data-poor sites' grade to NA

write.csv(CISHsiteSummary,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",
                                        format(Sys.Date(),'%Y-%m-%d'),
                                        "/CISHsiteSummary",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)
# CISHsiteSummary=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",pattern="CISHsiteSummary",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)


#Export only the data-rich sites
CISHwellSampled=CISHsiteSummary%>%dplyr::filter(nBS>=3)
lrsY=do.call(rbind,str_split(string = CISHwellSampled$nPbs,pattern = ',')) #Repeats to fill rows, doesnt matter, as any<10 excludes
lrsY=apply(lrsY,2,as.numeric)
NElt10=which(apply(lrsY,1,FUN=function(x)any(x<10)))
CISHwellSampled=CISHwellSampled[-NElt10,]
CISHwellSampled <- left_join(CISHwellSampled,recData%>%select(LawaSiteID,region,siteType,siteName)%>%distinct)
write.csv(CISHwellSampled,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",
                                        format(Sys.Date(),'%Y-%m-%d'),
                                        "/CISHwellSampled",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)


# CISHsiteSummary$region = recData$region[match(CISHsiteSummary$LawaSiteID,recData$LawaSiteID)]
CISHsiteSummary$siteName = recData$siteName[match(CISHsiteSummary$LawaSiteID,recData$LawaSiteID)]
CISHsiteSummary$siteType = recData$siteType[match(CISHsiteSummary$LawaSiteID,recData$LawaSiteID)]

#Write individual regional files: data from recData and scores from CISHsiteSummary 
uReg=unique(recData$region)
for(reg in seq_along(uReg)){
  toExport=recData[recData$region==uReg[reg],]
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recData_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 ".csv"),row.names = F)
  toExport=CISHsiteSummary%>%filter(region==uReg[reg])%>%as.data.frame
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recScore_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 ".csv"),row.names = F)
}
rm(reg,uReg,toExport)

#Write export file for ITEffect to use for the website
recDataITE <- CISHsiteSummary%>%
  transmute(LAWAID=LawaSiteID,
            Region=region,
            Site=siteName,
            Hazen=haz95,
            NumberOfPoints=unlist(lapply(str_split(nPbs,','),function(x)sum(as.numeric(x)))),
            DataMin=min,
            DataMax=max,
            RiskGrade=LawaBand,
            Module=siteType)
recDataITE$Module[recDataITE$Module=="Site"] <- "River"
recDataITE$Module[recDataITE$Module=="LakeSite"] <- "Lake"
recDataITE$Module[recDataITE$Module=="Beach"] <- "Coastal"
write.csv(recDataITE,paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                            "/ITERecData",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)  
