rm(list=ls())
library(xml2)
library(stringr)
library(tidyverse)
source("H:/ericg/16666LAWA/LAWA2019/scripts/LAWAFunctions.R")
setwd('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/')
try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)
try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)

if(0){
'
BayOfPlenty
Canterbury           exclusive URL   add ashley enterococci
Gisborne             metadata URL
HawkesBay            metadata URL
Manawatu(Horizons)   exclusive URL
Marlborough          metadata URL
Nelson              "The Nelson LAWA data.hts file only includes routine samples" PFisher 5/10/18
Northland            metadata URL
Southland            "All samples provided are routine". LHayward 9/10/18
Taranaki             metadata URL
Wellington           metadata URL
WestCoast            exclusive URL
Tasman               replacement file
Waikato              replacement file
Otago                replacement file'
}

ssm = read.csv('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/MetaData/SwimSiteMonitoringResults-2019-08-01.csv',stringsAsFactors = F)

ssm$TimeseriesUrl=gsub(ssm$TimeseriesUrl,pattern = 'data.wcrc.govt.nz:9083/',replacement = 'hilltop.wcrc.govt.nz/')
# ssm$TimeseriesUrl=gsub(ssm$TimeseriesUrl,pattern = 'ecan.govt.nz/bathing.hts/',replacement = 'ecan.govt.nz/WQRecLawa.hts/')
# ssm$TimeseriesUrl=gsub(ssm$TimeseriesUrl,pattern = "http://hilltopserver.horizons.govt.nz/cr_provisional.hts",
#                        replacement="http://tsdata.horizons.govt.nz/LAWA-Rec.hts")
# ssm$TimeseriesUrl=gsub(ssm$TimeseriesUrl,pattern = 'FO398217',replacement = 'FO397216')

#Ignore these welly entries, because we've got new ones to put in 
# ssm$TimeseriesUrl[ssm$Region=='Wellington region']=''
#replace welly sites
# welly = read.csv('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/SwimSiteMonitoringResults-2018-11-21.csv',stringsAsFactors=F)
# welly = welly%>%filter(Region=='Wellington region')
# ssm=rbind(ssm,welly)
# rm(welly)

ssm$LAWA2019n=0
ssm$LAWA2019comment=as.character("notyet")
ssm$LAWA2019comment[ssm$TimeseriesUrl=='']='NoTimeSeriesURL'

regions=c("Bay of Plenty region", "Canterbury region", "Gisborne region", 
          "Hawke's Bay region", "Manawatu-Wanganui region", "Marlborough region", 
          "Nelson region", "Northland region", "Otago region", "Southland region", 
          "Taranaki region", "Tasman region", "Waikato region", "Wellington region", 
          "West Coast region")

#Remove file selection to restart try again
if(0){
  thisCouncil=which(ssm$Region%in%regions)  #subset here to remove only from one council if necessary
  for (sn in thisCouncil){
    if(file.exists(paste0('./data/dataCache/site',sn,'.xml'))){
      cat('-')
      file.remove(paste0('./data/dataCache/site',sn,'.xml'))
    }else{
      cat('.')
    }
    if(file.exists(paste0('./data/dataCache/site',sn,'_MD.xml'))){
      cat('-')
      file.remove(paste0('./data/dataCache/site',sn,'_MD.xml'))
    }else{
      cat('.')
    }
  }
  rm(thisCouncil,sn)
}



yesstop=FALSE
#Repeat pulls until dataset is pretty much almost complete
while(sum(!ssm$LAWA2019comment%in%c("","NoTimeSeriesURL"))>90){
  table(ssm$LAWA2019comment==""|ssm$LAWA2019comment=="NoTimeSeriesURL")
  listToCombine=NULL
  latestAgency=''
  siteNum=1
  for(siteNum in siteNum:dim(ssm)[1]){
    if(ssm$LAWA2019comment[siteNum]==''|ssm$LAWA2019comment[siteNum]=="NoTimeSeriesURL"){
      next
    }
    #Output agency label 
    if(ssm$Region[siteNum]!=latestAgency){
      latestAgency=ssm$Region[siteNum]
      cat('\n',latestAgency,'\t',siteNum,' out of ',dim(ssm)[1],'\n')
      sitePerAg=1
    }
    if(round(sitePerAg/50)*50==sitePerAg){cat('|\n')}
    sitePerAg=sitePerAg+1
    
    urlToTry=ssm$TimeseriesUrl[siteNum]
    if(urlToTry==''){next}
    
    if(!file.exists(paste0('./data/dataCache/site',siteNum,'.xml')) || 
       (file.exists(paste0('./data/dataCache/site',siteNum,'.xml'))&file.info(paste0('./data/dataCache/site',siteNum,'.xml'))$size<1280)){
      
      if(grepl(pattern = 'observedProperty',x = urlToTry,ignore.case = T)){
        urlToTry=paste0(urlToTry,"&temporalfilter=om:phenomenonTime,P15Y/2019-01-01")
        # urlToTry=paste0(urlToTry,"&temporalfilter=om:phenomenonTime,2015-06-01T00:00:00/2019-06-01T00:00:00")
      #BOP example from SWQ
          # http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?
          # service=SOS&version=2.0.0&request=GetObservation&
          # observedProperty=",Measurements[j],
          # &featureOfInterest=",sites[i],
          # &temporalfilter=om:phenomenonTime,P15Y/2019-01-01
        
      }
      cat('v')
      dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                      path = paste0('./data/DataCache/site',siteNum,'.xml')),silent=T)
      if('try-error'%in%attr(x = dlTry,which = 'class')){
        cat('-')
        ssm$LAWA2019comment[siteNum]='try error'
        next
      }
      if(dlTry$status_code%in%c(500,501,503,404)){
        cat('.')
        ssm$LAWA2019comment[siteNum]=paste0(ssm$LAWA2019comment[siteNum],'DownloadFail')
        next
      }
      if(dlTry$status_code==408){
        cat('TO')
        ssm$LAWA2019comment[siteNum]=paste0(ssm$LAWA2019comment[siteNum],'timeOut')
        next
      }
    }
    xmlfile<-read_xml(paste0('./data/DataCache/site',siteNum,'.xml'))
    if('ows'%in%names(xml_ns(xmlfile)) && length(xml_find_all(xmlfile,'ows:Exception'))>0){
      cat('-')
      excCode <- try(xml_find_all(xmlfile,'//ows:Exception'))
      if(length(excCode)>0){
        commentToAdd=xml_text(excCode)
      }else{
        commentToAdd='exception'
      }
      ssm$LAWA2019comment[siteNum]=paste0(ssm$LAWA2019comment[siteNum],commentToAdd)
      #Dont wanna keep exception files
      file.remove(paste0('./data/dataCache/site',siteNum,'.xml'))
      next
    }
    #Here we've got good data
    cat('+')
    if('wml2'%in%names(xml_ns(xmlfile))){
      mvals <- xml_find_all(xmlfile,'//wml2:value')
      if(length(mvals)>0){
        cat('*')
        ssm$LAWA2019comment[siteNum]=''
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
        siteDat=data.frame(region=latestAgency,
                           LawaSiteID=trimws(ssm$LawaId[siteNum]),
                           siteName=trimws(ssm$SiteName[siteNum]),
                           siteType=trimws(ssm$SiteType[siteNum]),
                           property=trimws(ssm$Property[siteNum]),
                           dateCollected=mT,
                           val=faceVals,
                           lCens=lCens,
                           rCens=rCens)%>%
          plyr::mutate(week=lubridate::isoweek(mT),
                       month=lubridate::month(mT),
                       year=lubridate::isoyear(mT),
                       YW=paste0(year,week))
        
        #Get metadata to include/exclude test/retest
        if(latestAgency=="Gisborne region"){
          if(!file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml'))&file.info(paste0('./data/dataCache/site',siteNum,'_MD.xml'))$size<1280)){
            urlToTry=ssm$TimeseriesUrl[siteNum]
            urlToTry=gsub(x = urlToTry,pattern='ObservedProperty=.*&',replacement="Measurement=WQ Sample&")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='FeatureOfInterest',replacement = 'Site')
            urlToTry=gsub(x=urlToTry,pattern='temporalfilter=om:phenomenonTime,2015-06-01T00:00:00/2019-06-01T00:00:00',
                          replacement = 'From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/site',siteNum,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/site',siteNum,'_MD.xml'))
          mdText=xml_find_all(xmlfile,"//Parameter")
          mdT=xml_find_all(xmlfile,"//T")
          mdT=xml_text(mdT)
          testRetest=data.frame(ts=rep('0',length(mdT)),TR=rep('unknown',length(mdT)),stringsAsFactors = F)
          for(tt in seq_along(mdT)){
            wqsamp=xml_parent(xml_find_all(xmlfile,paste0("//T[text()='",mdT[tt],"']")))
            sampTime=xml_find_all(wqsamp,".//T")
            if(length(sampTime)==1){
              testRetest$ts[tt]=as.character(strptime(xml_text(sampTime),"%Y-%m-%dT%H:%M:%S"))
            }
            sampParams=xml_find_all(wqsamp,"Parameter")
            tr=sampParams[xml_attr(sampParams,"Name")=="SampleEventType"]
            if(length(tr)==1){
              testRetest$TR[tt]=xml_attr(tr,"Value")
            }
          }
          # cat(unique(testRetest$TR))
          siteDat$testRetest=testRetest$TR[match(as.character(siteDat$dateCollected),testRetest$ts)]
          if(any(siteDat$testRetest=="Pollution",na.rm=T)){
            siteDat=siteDat[-which(siteDat$testRetest=="Pollution"),]
          }
        }
        if(latestAgency=="Marlborough region"){
          if(!file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml'))&file.info(paste0('./data/dataCache/site',siteNum,'_MD.xml'))$size<1280)){
            urlToTry=ssm$TimeseriesUrl[siteNum]
            urlToTry=gsub(x = urlToTry,pattern='observedProperty=.*',replacement="Measurement=WQ Sample")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
          urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
          urlToTry=gsub(x=urlToTry,pattern='featureOfInterest',replacement = 'Site')
          urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
          dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                          path = paste0('./data/dataCache/site',siteNum,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/site',siteNum,'_MD.xml'))
          mdText=xml_find_all(xmlfile,"//Parameter")
          mdT=xml_find_all(xmlfile,"//T")
          mdT=xml_text(mdT)
          testRetest=data.frame(ts=rep('0',length(mdT)),TR=rep('unknown',length(mdT)),stringsAsFactors = F)
          if(yesstop){browser()}
          for(tt in seq_along(mdT)){
            wqsamp=xml_parent(xml_find_all(xmlfile,paste0("//T[text()='",mdT[tt],"']")))
            sampTime=xml_find_all(wqsamp,".//T")
            if(length(sampTime)==1){
              testRetest$ts[tt]=as.character(strptime(xml_text(sampTime),"%Y-%m-%dT%H:%M:%S"))
            }
            sampParams=xml_find_all(wqsamp,"Parameter")
            tr=sampParams[xml_attr(sampParams,"Name")=="Project"]
            if(length(tr)==1){
              testRetest$TR[tt]=xml_attr(tr,"Value")
            }
          }
            # cat(unique(testRetest$TR))
          siteDat$testRetest=testRetest$TR[match(as.character(siteDat$dateCollected),testRetest$ts)]
          if(any(siteDat$testRetest=="Recreational Bathing - Follow-up",na.rm=T)){
            siteDat=siteDat[-which(siteDat$testRetest=="Recreational Bathing - Follow-up"),]
          }
        }
        if(latestAgency=="Northland region"){
          if(!file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml'))&file.info(paste0('./data/dataCache/site',siteNum,'_MD.xml'))$size<1280)){
            urlToTry=ssm$TimeseriesUrl[siteNum]
          urlToTry=gsub(x = urlToTry,pattern='ObservedProperty=.*',replacement="Measurement=WQ Sample")
          urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
          urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
          urlToTry=gsub(x=urlToTry,pattern='FeatureOfInterest',replacement = 'Site')
          urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
          dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                          path = paste0('./data/dataCache/site',siteNum,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/site',siteNum,'_MD.xml'))
          mdText=xml_find_all(xmlfile,"//Parameter")
          mdT=xml_find_all(xmlfile,"//T")
          mdT=xml_text(mdT)
          testRetest=data.frame(ts=rep('0',length(mdT)),TR=rep('unknown',length(mdT)),stringsAsFactors = F)
          for(tt in seq_along(mdT)){
            wqsamp=xml_parent(xml_find_all(xmlfile,paste0("//T[text()='",mdT[tt],"']")))
            sampTime=xml_find_all(wqsamp,".//T")
            if(length(sampTime)==1){
              testRetest$ts[tt]=as.character(strptime(xml_text(sampTime),"%Y-%m-%dT%H:%M:%S"))
            }
            sampParams=xml_find_all(wqsamp,"Parameter")
            tr=sampParams[xml_attr(sampParams,"Name")=="Run Type"]
            
            if(length(tr)==1){
              testRetest$TR[tt]=xml_attr(tr,"Value")
            }
          }
          # cat(unique(testRetest$TR))
          siteDat$testRetest=testRetest$TR[match(as.character(siteDat$dateCollected),testRetest$ts)]
          if(any(siteDat$testRetest=="RSWQP - follow up",na.rm=T)){
            siteDat=siteDat[-which(siteDat$testRetest=="RSWQP - follow up"),]
          }
        }
        if(latestAgency=="Taranaki region"){
          if(!file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml'))&file.info(paste0('./data/dataCache/site',siteNum,'_MD.xml'))$size<1280)){
            urlToTry=ssm$TimeseriesUrl[siteNum]
            urlToTry=gsub(x=urlToTry,pattern='observedProperty=',replacement="Measurement=")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='featureOfInterest',replacement = 'Site')
            urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/site',siteNum,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/site',siteNum,'_MD.xml'))
          mdText=xml_find_all(xmlfile,"//Parameter")
          mdT=xml_find_all(xmlfile,"//T")
          mdT=xml_text(mdT)
          testRetest=data.frame(ts=rep('0',length(mdT)),TR=rep('unknown',length(mdT)),stringsAsFactors = F)
          for(tt in seq_along(mdT)){
            wqsamp=xml_parent(xml_find_all(xmlfile,paste0("//T[text()='",mdT[tt],"']")))
            sampTime=xml_find_all(wqsamp,".//T")
            if(length(sampTime)==1){
              testRetest$ts[tt]=as.character(strptime(xml_text(sampTime),"%Y-%m-%dT%H:%M:%S"))
            }
            sampParams=xml_find_all(wqsamp,"Parameter")
            tr=sampParams[xml_attr(sampParams,"Name")=="Sample Type"]
            if(length(tr)==1){
              testRetest$TR[tt]=xml_attr(tr,"Value")
            }
          }
          # cat(unique(testRetest$TR))
          siteDat$testRetest=testRetest$TR[match(as.character(siteDat$dateCollected),testRetest$ts)]
          if(any(siteDat$testRetest=="Followup",na.rm=T)){
            siteDat=siteDat[-which(siteDat$testRetest=="Followup"),]
          }
        }
        if(latestAgency=="Hawke's Bay region"){
          # browser()
          if(!file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml'))&file.info(paste0('./data/dataCache/site',siteNum,'_MD.xml'))$size<1280)){
            urlToTry=ssm$TimeseriesUrl[siteNum]
            urlToTry=gsub(x=urlToTry,pattern='ObservedProperty=.*',replacement="Measurement=WQ%20Sample")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='FeatureOfInterest',replacement = 'Site')
            urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/site',siteNum,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/site',siteNum,'_MD.xml'))
          mdText=xml_find_all(xmlfile,"//Parameter")
          mdT=xml_find_all(xmlfile,"//T")
          mdT=xml_text(mdT)
          testRetest=data.frame(ts=rep('0',length(mdT)),TR=rep('unknown',length(mdT)),stringsAsFactors = F)
          for(tt in seq_along(mdT)){
            wqsamp=xml_parent(xml_find_all(xmlfile,paste0("//T[text()='",mdT[tt],"']")))
            sampTime=xml_find_all(wqsamp,".//T")
            if(length(sampTime)==1){
              testRetest$ts[tt]=as.character(strptime(xml_text(sampTime),"%Y-%m-%dT%H:%M:%S"))
            }
            sampParams=xml_find_all(wqsamp,"Parameter")
            tr=sampParams[xml_attr(sampParams,"Name")=="Project ID"]
            if(length(tr)==1){
              testRetest$TR[tt]=xml_attr(tr,"Value")
            }
          }
          siteDat$testRetest=testRetest$TR[match(substr(as.character(siteDat$dateCollected),start = 1,stop=10),
                                                 substr(testRetest$ts,1,10))]
          if(any(siteDat$testRetest=="BB-Ex",na.rm=T)){
            siteDat=siteDat[-which(siteDat$testRetest=="BB-Ex"),]
          }
        }
        if(latestAgency=="Wellington region"){
          if(yesstop){
          browser()}
          if(!file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/site',siteNum,'_MD.xml'))&file.info(paste0('./data/dataCache/site',siteNum,'_MD.xml'))$size<1280)){
            urlToTry=ssm$TimeseriesUrl[siteNum]
            urlToTry=gsub(x=urlToTry,pattern='observedProperty=.*',replacement="Measurement=WQ%20Sample")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='featureOfInterest',replacement = 'Site')
            urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/site',siteNum,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/site',siteNum,'_MD.xml'))
          mdText=xml_find_all(xmlfile,"//Parameter")
          mdT=xml_find_all(xmlfile,"//T")
          mdT=xml_text(mdT)
          testRetest=data.frame(ts=rep('0',length(mdT)),TR=rep('unknown',length(mdT)),stringsAsFactors = F)
          for(tt in seq_along(mdT)){
            wqsamp=xml_parent(xml_find_all(xmlfile,paste0("//T[text()='",mdT[tt],"']")))
            sampTime=xml_find_all(wqsamp,".//T")
            if(length(sampTime)==1){
              testRetest$ts[tt]=as.character(strptime(xml_text(sampTime),"%Y-%m-%dT%H:%M:%S"))
            }
            sampParams=xml_find_all(wqsamp,"Parameter")
            tr=sampParams[xml_attr(sampParams,"Name")=="Sample type"]
            if(length(tr)==1){
              testRetest$TR[tt]=xml_attr(tr,"Value")
            }
          }
          siteDat$testRetest=testRetest$TR[match(substr(as.character(siteDat$dateCollected),start = 1,stop=10),
                                                 substr(testRetest$ts,1,10))]
          if(any(siteDat$testRetest=="F",na.rm=T)){
            cat(length(which(siteDat$testRetest=="F")),'\t')
            siteDat=siteDat[-which(siteDat$testRetest=="F"),]
          }
        }

        eval(parse(text=paste0('site',siteNum,'=siteDat')))
        listToCombine=c(listToCombine,paste0('site',siteNum))
      }else{
        #Dont wanna keep empty files
        file.remove(paste0('./data/dataCache/site',siteNum,'.xml'))
      }
      next
    }else{
      #Dont wanna keep empty files
      file.remove(paste0('./data/dataCache/site',siteNum,'.xml'))
    }
    cat('/')
    ssm$LAWA2019comment[siteNum]=paste0(ssm$LAWA2019comment[siteNum],'NoDataReturned')
  }
  cat('\n',sum(!ssm$LAWA2019comment%in%c("","NoTimeSeriesURL")),'\n')
  Sys.sleep(time = 6)
}

rm(siteDat)
rm(testRetest)
rm(faceVals,lCens,mdT,mT,mvals,rCens,siteNum,sitePerAg,tt,urlToTry,commentToAdd,
   latestAgency,dlTry,excCode,mdText,sampParams,sampTime,tr,wqsamp,xmlfile)
# ssm[ssm[,14]!='',c(2,14)]


#Combine and save raw data
listToCombine=ls(pattern='^site[[:digit:]]')
for(ltc in seq_along(listToCombine)){
  for(cc in 1:6){
    eval(parse(text=paste0(listToCombine[ltc],"[,cc]=as.character(",listToCombine[ltc],"[,cc])")))
  }
  #drop TestRetest column
  eval(parse(text=paste0(listToCombine[ltc],"=",listToCombine[ltc],"[,1:13]")))
}
rm(ltc,cc)

if(exists('recData')){
  eval(parse(text=paste0("recData=rbind(recData%>%select(names(",listToCombine[1],")),",paste(listToCombine,collapse=','),")")))
}else{
  eval(parse(text=paste0("recData=rbind.data.frame(",paste(listToCombine,collapse=','),")")))
}
eval(parse(text=paste0("rm('",paste(listToCombine,collapse="','"),"')")))
rm(listToCombine)


#recData 30557 x 13 2018
#        30510 x 13 10/6/19
#        38772 x 13 18/7/19
#        78741 x 13 1/8/19

#Drop retests as indicated by councils
TDCtestRetest=read.csv('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Metadata/TDCTestIndication.csv',stringsAsFactors = F)[,1:17]
TDCtestRetest=TDCtestRetest%>%filter(X=="")
TDCtestRetest$dateCollected=as.POSIXct(TDCtestRetest$dateCollected*60*60*24-(13*60*60),origin="1899-12-30")
recData=recData%>%filter(region!="Tasman region")
recData$dateCollected=as.POSIXct(recData$dateCollected)
recData$YW=as.integer(recData$YW)
recData=bind_rows(recData,TDCtestRetest%>%select(names(recData)))
rm(TDCtestRetest)

#recData 30540 x 13 2018
#        29782 x 13
#         77447 x 13 1/8/2019

WRCtestRetest=read.csv('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/MetaData/WRCTestIndication.csv',stringsAsFactors = F)#[,1:17]
WRCtestRetest=WRCtestRetest%>%filter(Retest.result...Y.N.=='')
WRCtestRetest$dateCollected=as.POSIXct(WRCtestRetest$dateCollected*60*60*24-(13*60*60),origin="1899-12-30")
recData=recData%>%filter(region!="Waikato region")
recData$dateCollected=as.POSIXct(recData$dateCollected)
recData$YW=as.integer(recData$YW)
recData=bind_rows(recData,WRCtestRetest%>%select(names(recData)))
rm(WRCtestRetest)

#recData 30508 x 13 2018
#        30491      10.6.19
#         37932    18.7.19
#         75692    1.8.19

ORCtestRetest=read.csv('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/MetaData/ORCTestIndication.csv',stringsAsFactors = F)#[,1:17]
ORCtestRetest=ORCtestRetest%>%filter(Retest.result...Y.N.=='')
ORCtestRetest$dateCollected=as.POSIXct(ORCtestRetest$dateCollected*60*60*24-(13*60*60),origin="1899-12-30")
recData=recData%>%filter(region!="Otago region")
recData$dateCollected=as.POSIXct(recData$dateCollected)
recData$YW=as.integer(recData$YW)
recData=bind_rows(recData,ORCtestRetest%>%select(names(recData)))
rm(ORCtestRetest)

#recData 30483 x 13 2018
#        30028 x 13 10.6.19
#        37469      18.7.19
#        72702        1.8.19

#Make all yearweeks six characters
recData$week=as.character(recData$week)
recData$week[nchar(recData$week)==1]=paste0('0',recData$week[nchar(recData$week)==1])
recData$YW=as.numeric(paste0(recData$year,recData$week))

#Deal with censored data
#Anna Madarasz-Smith 1/10/2018
#Hey guys,
# We (HBRC) have typically used the value with the >reference 
# (eg >6400 becomes 6400), and half the detection of the less than
# values (e.g. <1 becomes 0.5).  I think realistically you can treat
# these any way as long as its consistent beccuase it shouldn’t matter
# to your 95th%ile.  These values should be in the highest and lowest of the range.
# Hope this helps
# Cheers
# Anna
recData$val=as.numeric(recData$val)
recData$fVal=recData$val
recData$fVal[recData$lCens]=recData$val[recData$lCens]/2
recData$fVal[recData$rCens]=recData$val[recData$rCens]
table(recData$lCens)
table(recData$rCens)

#Create bathing seasons
bs=strTo(recData$dateCollected,'-')
bs[recData$month>6]=paste0(recData$year[recData$month>6],'/',recData$year[recData$month>6]+1)
bs[recData$month<7]=paste0(recData$year[recData$month<7]-1,'/',recData$year[recData$month<7])
recData$bathingSeason=bs

save(recData,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                            "/RecData",format(Sys.time(),'%Y-%b-%d'),".Rdata"))
# load(dir(path=           'h:/ericg/16666LAWA/2018/RecECOLI',pattern='*RecData*.*Rdata',recursive=T,full.names=T)[6],verbose = T)
recData$week=lubridate::week(recData$dateCollected)
recData$YW = paste0(strTo(recData$dateCollected,'-'),recData$week)
# write.csv(recData%>%filter(YW>201525)%>%filter(month>10|month<4),
#           file = paste0('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/CISHRaw',format(Sys.time(),'%Y-%b-%d'),'.csv'),row.names = F)




test <- recData%>%
  # filter(YW>201525)%>%
  dplyr::filter(LawaSiteID!='')%>%drop_na(LawaSiteID)%>%
  dplyr::filter(property!='Cyanobacteria')%>%
  dplyr::filter(year>2014&(month>10|month<4))%>% #bathign season months only
  dplyr::group_by(LawaSiteID,YW,property)%>%
  dplyr::arrange(YW)%>%                    #Count number of weeks recorded per season
  dplyr::summarise(dateCollected=first(dateCollected),
            region=unique(region),
            n=length(fVal),
            fVal=first(fVal),
            bathingSeason=unique(bathingSeason))%>%
  ungroup->graphData
write.csv(graphData,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",
                                  format(Sys.Date(),'%Y-%m-%d'),
                                  "/CISHgraph",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)

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
CISHsiteSummary$LawaBand[CISHsiteSummary$property!='Enterococci']=cut(x=CISHsiteSummary$haz95[CISHsiteSummary$property!='Enterococci'],
                                                      breaks=c(-0.1,260,550,Inf),labels=c("A","B","C"))
nPbs=do.call("rbind",lapply(CISHsiteSummary$nPbs,FUN = function(x)lapply(strsplit(x,split = ','),as.numeric))) #get the per-season counts
tooFew = which(do.call('rbind',lapply(nPbs,function(x)any(x<10)|length(x)<3)))  #see if any per-season counts are below ten, or there are fewer than three seasons
CISHsiteSummary$LawaBand[tooFew]=NA #set those data-poor sites' grade to NA

write.csv(CISHsiteSummary,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",
                                  format(Sys.Date(),'%Y-%m-%d'),
                                  "/CISHsiteSummary",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)
# CISHsiteSummary=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",pattern="CISHsiteSummary",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)

#Export only the data-rich sites
CISHwellSampled=CISHsiteSummary%>%filter(nBS>=3)
lrsY=do.call(rbind,str_split(string = CISHwellSampled$nPbs,pattern = ','))
lrsY=apply(lrsY,2,as.numeric)
NElt10=which(apply(lrsY,1,FUN=function(x)any(x<10)))
CISHwellSampled=CISHwellSampled[-NElt10,]
CISHwellSampled <- left_join(CISHwellSampled,recData%>%select(LawaSiteID,region,siteType,siteName)%>%distinct)
write.csv(CISHwellSampled,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",
                                  format(Sys.Date(),'%Y-%m-%d'),
                                  "/CISHwellSampled",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)

CISHsiteSummary$region = recData$region[match(CISHsiteSummary$LawaSiteID,recData$LawaSiteID)]
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
rm(reg,uReg)
 
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

