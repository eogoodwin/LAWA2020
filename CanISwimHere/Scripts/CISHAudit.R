library(tidyverse)
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
    #Remove non WML files
    file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
    return(1);#next
  }
}

readXMLFile <- function(regionName,siteName,propertyName,property){
  xmlfile<-read_xml(paste0('./data/DataCache/',regionName,siteName,propertyName,'.xml'))
  if('ows'%in%names(xml_ns(xmlfile)) && length(xml_find_all(xmlfile,'ows:Exception'))>0){
    cat('-')
    excCode <- try(xml_find_all(xmlfile,'//ows:Exception'))
    if(length(excCode)>0){
      commentToAdd=xml_text(excCode)
    }else{
      commentToAdd='exception'
    }
    # ssm$LAWA2019comment[siteNum]=paste0(ssm$LAWA2019comment[siteNum],commentToAdd)
    #Dont wanna keep exception files
    file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
    return(1);#next
  }
  #Got Data
  if('wml2'%in%names(xml_ns(xmlfile))){
    mvals <- xml_find_all(xmlfile,'//wml2:value')
    if(length(mvals)>0){
      cat('*')
      # ssm$LAWA2019comment[siteNum]=''
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
                         siteType=trimws(ssm$SiteType[siteNum]),
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
      if(0){
        #Get metadata to include/exclude test/retest
        if(SSMregion=="Gisborne region"){
          if(!file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))&file.info(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))$size<1280)){
            urlToTry=gsub(x = urlToTry,pattern='ObservedProperty=.*&',replacement="Measurement=WQ Sample&")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='FeatureOfInterest',replacement = 'Site')
            urlToTry=gsub(x=urlToTry,pattern='temporalfilter=om:phenomenonTime,2015-06-01T00:00:00/2019-06-01T00:00:00',
                          replacement = 'From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))
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
        if(SSMregion=="Marlborough region"){
          if(!file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))&file.info(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))$size<1280)){
            urlToTry=gsub(x = urlToTry,pattern='observedProperty=.*',replacement="Measurement=WQ Sample")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='featureOfInterest',replacement = 'Site')
            urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))
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
        if(SSMregion=="Northland region"){
          if(!file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))&file.info(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))$size<1280)){
            urlToTry=gsub(x = urlToTry,pattern='ObservedProperty=.*',replacement="Measurement=WQ Sample")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='FeatureOfInterest',replacement = 'Site')
            urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))
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
        if(SSMregion=="Taranaki region"){
          if(!file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))&file.info(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))$size<1280)){
            urlToTry=gsub(x=urlToTry,pattern='observedProperty=',replacement="Measurement=")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='featureOfInterest',replacement = 'Site')
            urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))
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
        if(SSMregion=="Hawke's Bay region"){
          # browser()
          if(!file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))&file.info(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))$size<1280)){
            urlToTry=gsub(x=urlToTry,pattern='ObservedProperty=.*',replacement="Measurement=WQ%20Sample")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='FeatureOfInterest',replacement = 'Site')
            urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))
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
        if(SSMregion=="Wellington region"){
          if(yesstop){
            browser()}
          if(!file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')) || 
             (file.exists(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))&file.info(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))$size<1280)){
            urlToTry=gsub(x=urlToTry,pattern='observedProperty=.*',replacement="Measurement=WQ%20Sample")
            urlToTry=gsub(x=urlToTry,pattern='SOS',replacement = 'Hilltop')
            urlToTry=gsub(x=urlToTry,pattern='GetObservation',replacement = 'GetData')
            urlToTry=gsub(x=urlToTry,pattern='featureOfInterest',replacement = 'Site')
            urlToTry=paste0(urlToTry,'&From=1/6/2015&To=1/6/2019')
            dlTry=try(curl::curl_fetch_disk(gsub(x = urlToTry,pattern = ' ',replacement = '%20'),
                                            path = paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml')),silent=T)
          }
          xmlfile<-read_xml(paste0('./data/dataCache/',regionName,siteName,propertyName,'_MD.xml'))
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
      }
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
    file.remove(paste0('./data/dataCache/',regionName,siteName,propertyName,'.xml'))
    return(1);#next
  }
}

  
ssm = read.csv('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/MetaData/SwimSiteMonitoringResults-2019-08-01.csv',stringsAsFactors = F)


load(tail(dir(path="h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/",
              pattern="RecData2019",recursive = T,full.names = T,ignore.case=T),1))  


ssm$LawaSiteID=tolower(ssm$LawaId)

recData$LawaSiteID=tolower(recData$LawaSiteID)

table(ssm$LawaSiteID%in%recData$LawaSiteID)

sort(unique(ssm$LawaSiteID[!ssm$LawaSiteID%in%recData$LawaSiteID]))
sort(unique(ssm$SiteName[!ssm$LawaSiteID%in%recData$LawaSiteID]))
sort(unique(ssm$Region[!ssm$LawaSiteID%in%recData$LawaSiteID]))

knitr::kable(table(ssm$Region[!ssm$LawaSiteID%in%recData$LawaSiteID]),format='rst')
knitr::kable(table(ssm$Region[ssm$LawaSiteID%in%recData$LawaSiteID]),format='rst')



RegionTable=data.frame(ssm=unique(ssm$Region),
                       wfs=c("bay of plenty","canterbury","gisborne","hawkes bay",
                             "horizons","marlborough","nelson","northland",
                             "otago","southland","taranaki","tasman",
                             "waikato","wellington","west coast"))



#Load all previous CISH datasets, and merge
CISHdata = dir(path="h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/",
               pattern="RecData2019",recursive = T,full.names = T,ignore.case=T)
collectCISH=NULL
for (renminbi in 1:dim(CISHdata)){
  load(CISHdata[renminbi])
  # cat(str(recData),'\n')
  if('lawaSiteID'%in%names(recData)){
    recData <- recData%>%rename(LawaSiteID=lawaSiteID)
  }
  recData$LawaSiteID = tolower(recData$LawaSiteID)
  collectCISH = merge(collectCISH,recData,all.y=T)
  cat(dim(collectCISH),'\n')
}
rm(recData,CISHdata,renminbi)


#Compare the number of sites retrieved via SSM with that via WFS enquiry.
WFSsiteTable = read.csv(tail(dir(path="H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data",pattern='SiteTable',
                                 recursive=T,full.names=T,ignore.case=T),1),stringsAsFactors = F)
length(unique(ssm$LawaId))              #624
length(unique(collectCISH$LawaSiteID))  #484
length(unique(WFSsiteTable$LawaSiteID)) #683  673 september

length(unique(c(unique(tolower(ssm$LawaId)),unique(tolower(WFSsiteTable$LawaSiteID)))))              #729


sort(unique((unlist(sapply(X = ssm$TimeseriesUrl,FUN = function(x)unlist(strsplit(x,split='&')))))))
sort(unique(tolower(unlist(sapply(X = ssm$TimeseriesUrl,FUN = function(x)unlist(strsplit(x,split='&')))))))

"http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?service=sos"         #BOPRC 
"http://data.ecan.govt.nz/wqreclawa.hts?service=sos"                                 #ECAN
"http://wateruse.ecan.govt.nz/bathing.hts?service=sos"                               #ECAN
"http://odp.es.govt.nz/coastal.hts?service=sos"                                       #ES
"http://odp.es.govt.nz/cyanoalert.hts?service=sos"                                    #ES
"http://hilltop.gdc.govt.nz/data.hts?service=sos"                                     #GDC
"http://hilltop.gw.govt.nz/cwq_web.hts?service=sos"                                   #GW
"http://hilltop.gw.govt.nz/data.hts?service=sos"                                      #GW
"https://data.hbrc.govt.nz/envirodata/recreational_wq.hts?service=sos"                #HBRC
"https://data.hbrc.govt.nz/envirodata/emar.hts?service=sos"                           #HBRC
"http://hilltopserver.horizons.govt.nz/cr_provisional.hts?service=sos"                #HRC
"http://hydro.marlborough.govt.nz/boo.hts?service=sos"                                #MDC
"http://hydro.marlborough.govt.nz/data.hts?service=sos"                               #MDC
"http://envdata.nelson.govt.nz/lawa.hts?service=sos"                                  #NCC
"http://hilltop.nrc.govt.nz/soeswimmingcoastal.hts?service=sos"                       #NRC
"http://hilltop.nrc.govt.nz/soeswimmingfw.hts?service=sos"                            #NRC
"http://gisdata.orc.govt.nz/hilltop/orcrec.hts?service=sos"                           #ORC
"http://envdata.tasman.govt.nz/waterquality.hts?service=sos"                          #TDC
"https://extranet.trc.govt.nz/getdata/lawa_benthic_wq.hts?service=sos"                #TRC
"https://extranet.trc.govt.nz/getdata/lawa_rec_wq.hts?service=sos"                    #TRC
"http://data.wcrc.govt.nz:9083/contactrecreation.hts?service=sos"                     #WCRC
"http://envdata.waikatoregion.govt.nz:8080/kiwis/kiwis?datasource=0"                  #WRC

table(ssm$Region)

# Bay of Plenty region        Canterbury region          Gisborne region       Hawke's Bay region Manawatu-Wanganui region 
#                       90                      141                       20                       42                      147 
#       Marlborough region            Nelson region         Northland region             Otago region         Southland region 
#                       18                       15                       60                       27                       28 
#          Taranaki region            Tasman region           Waikato region        Wellington region        West Coast region 
#                       39                        8                       28                      111                       18 







for(SSMregion in unique(ssm$Region)){
  #Find agency Sites
  agSites <- c(unlist(sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],FUN = function(x)unlist(strsplit(x,split='&')))))%>%
    grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
    gsub('featureofinterest=','',x=.,ignore.case = T)%>%
    sapply(.,URLdecode)%>%trimws%>%tolower%>%unique
  siteTableSites = WFSsiteTable$CouncilSiteID[WFSsiteTable$Region==RegionTable$wfs[RegionTable$ssm==SSMregion]]%>%
    trimws%>%tolower%>%unique%>%sort
  cat('\n\n',SSMregion,'\n')
  #Find agency URLs
  agURLs <- unlist(sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],
                          FUN = function(x)unlist(strsplit(x,split='&'))))%>%
    grep('http',x = .,ignore.case=T,value=T)%>%tolower%>%unique%>%sort
  cat("URL(s) specified on umbraco:\n")
  cat(paste(agURLs,collapse='\n'))
  cat('\n\n')
  cat("Sites in SSM but not on WFS:\n")
  cat(paste(agSites[!agSites%in%siteTableSites],collapse=';\t'),'\n')
  cat("\nSites on WFS but not in SSM:\n")
  cat(paste(siteTableSites[!siteTableSites%in%agSites],collapse=';\t'),'\n')
}  




#Make all yearweeks six characters
recData$week=as.character(recData$week)
recData$week[nchar(recData$week)==1]=paste0('0',recData$week[nchar(recData$week)==1])
recData$YW=as.numeric(paste0(recData$year,recData$week))

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

#Create bathing seasons
bs=strTo(recData$dateCollected,'-')
bs[recData$month>6]=paste0(recData$year[recData$month>6],'/',recData$year[recData$month>6]+1)
bs[recData$month<7]=paste0(recData$year[recData$month<7]-1,'/',recData$year[recData$month<7])
recData$bathingSeason=bs
table(recData$bathingSeason)

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
rm(test)

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
            RiskGrade=LawaBand)
# recDataITE$Module[recDataITE$Module=="Site"] <- "River"
# recDataITE$Module[recDataITE$Module=="LakeSite"] <- "Lake"
# recDataITE$Module[recDataITE$Module=="Beach"] <- "Coastal"
write.csv(recDataITE,paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                            "/ITERecData",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)  





