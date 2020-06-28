rm(list = ls())
library(parallel)
library(doParallel)
setwd("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates")
source("H:/ericg/16666LAWA/LAWA2020/scripts/lawaFunctions.R")
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')

try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2020/Macroinvertebrates/Data/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)



urls          <- read.csv("H:/ericg/16666LAWA/LAWA2020/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)
"https://mapspublictest.aklc.govt.nz/arcgis3/services/NonCouncil/LAWA/MapServer/WFSServer?VERSION=1.1.0&request=GetFeature&service=WFS&typename=MonitoringSiteReferenceData&srsName=EPSG:4326"                                    
"http://geospatial.boprc.govt.nz/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=getfeature&service=WFS&VERSION=1.1.0&typename=MonitoringSiteReferenceData&srsName=EPSG:4326"                        
"https://gis.ecan.govt.nz/arcgis/services/emar/MapServer/WFSServer?version=1.1.0&request=GetFeature&service=WFS&typename=emar:MonitoringSiteReferenceData&srsName=EPSG:4326"                                                      
"http://gis.es.govt.nz/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?SERVICE=WFS&VERSION=1.1.0&REQUEST=getfeature&typename=MonitoringSiteReferenceData&srsName=urn:ogc:def:crs:EPSG:6.9:4326"              
"http://hilltop.gdc.govt.nz/lawa.hts?service=WFS&request=GetFeature&typename=MonitoringSiteReferenceData&version=1.1.0"                                                                                                           
"http://hilltop.gw.govt.nz/data.hts?Service=WFS&Request=GetFeature&TypeName=MonitoringSiteReferenceData&version=1.1.0"                                                                                                            
"https://hbmaps.hbrc.govt.nz/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=GetFeature&service=WFS&typename=MonitoringSiteReferenceData&srsName=urn:ogc:def:crs:EPSG:6.9:4326&Version=1.1.0"        
"https://dservices1.arcgis.com/VuN78wcRdq1Oj69W/arcgis/services/MonitoringSiteReferenceData/WFSServer?service=wfs&request=GetFeature&version=1.1.0&typeName=MonitoringSiteReferenceData&version=1.1.0"                            
"http://gisdmz.horizons.govt.nz/arcgis/services/Emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=getfeature&service=WFS&typename=MonitoringSiteReferenceData"                                                         
"https://hydro.marlborough.govt.nz/data.hts?Service=WFS&Request=GetFeature&TypeName=MonitoringSiteReferenceData&version=1.1.0"                                                                                                    
"http://envdata.nelson.govt.nz/anything.hts?Service=WFS&Request=GetFeature&TypeName=MonitoringSiteReferenceData&version=1.1.0"                                                                                                    
"http://gs.niwa.co.nz/nemo/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=nemo:fchem_locations"                                                                                                                        
"http://hilltop.nrc.govt.nz/PublicTelemetry.hts?Service=WFS&request=GetFeature&TypeName=MonitoringSiteReferenceData&version=1.1.0"                                                                                                
"http://gisdata.orc.govt.nz/Hilltop/Global.hts?Service=WFS&Request=GetFeature&TypeName=MonitoringSiteReferenceData&version=1.1.0"                                                                                                 
"http://envdata.tasman.govt.nz/anything.hts?Service=WFS&Request=GetFeature&TypeName=MonitoringSiteReferenceData&version=1.1.0"                                                                                                    
"https://extranet.trc.govt.nz/getgis/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=GetFeature&Version=1.1.0&service=WFS&typename=MonitoringSiteReferenceData&srsName=urn:ogc:def:crs:EPSG:6.9:4326"
"http://hilltop.wcrc.govt.nz/data.hts?Service=WFS&Request=GetFeature&TypeName=MonitoringSiteReferenceData&version=1.1.0"                                                                                                          
"https://maps.waikatoregion.govt.nz/arcgis/services/LAWA/LAWA/MapServer/WFSServer?service=WFS&VERSION=1.1.0&request=Getfeature&typename=MonitoringSiteReferenceData&srsname=epsg:4326"
# Config for data extract from WFS
#The first on the list prioritises the ID to be used and match every else to it.
vars <- c("CouncilSiteID","SiteID","LawaSiteID","NZReach","Region","Agency","SWQAltitude","SWQLanduse","Catchment")

if(exists('siteTable')){
  rm(siteTable)
}
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(XML)
  library(dplyr)
})
foreach(h = 1:length(urls$URL),.combine = rbind,.errorhandling = "stop")%dopar%{
  if(grepl("^x", urls$Agency[h])){ #allow agency switch-off by 'x' prefix
    return(NULL)
  } 
  if(!nzchar(urls$URL[h])){  ## returns false if the URL string is missing
    return(NULL)
  }
  
  xmldata<-try(ldWFS(urlIn = urls$URL[h],agency=urls$Agency[h],dataLocation = urls$Source[h],case.fix = TRUE))
  
  if('try-error'%in%attr(xmldata,'class')||grepl(pattern = '^501|error',
                                                 x = xmlValue(getNodeSet(xmldata,'/')[[1]]),
                                                 ignore.case=T)){
    cat('Failed for ',urls$Agency[h],'\n')
    return(NULL)
  }
  
  if(urls$Source[h]=="file" & grepl("csv$",urls$URL[h])){
    cc(urls$URL[h])
    tmp <- read.csv(urls$URL[h],stringsAsFactors=FALSE,strip.white = TRUE,sep=",")
    
    tmp <- tmp[,c(5,7,8,9,30,10,28,29,20,21,22)]
    tmp$Lat <- sapply(strsplit(as.character(tmp$pos),' '), "[",1)
    tmp$Long <- sapply(strsplit(as.character(tmp$pos),' '), "[",2)
    tmp <- tmp[-11]
    if(!exists("siteTable")){
      siteTable<-as.data.frame(tmp,stringsAsFactors=FALSE)
    } else{
      siteTable<-rbind.data.frame(siteTable,tmp,stringsAsFactors=FALSE)
    }
    rm(tmp)
  } else {
    ### Determine the values used in the [emar:Macro] element
    emarSTR="emar:"
    macroData<-unique((sapply(getNodeSet(doc=xmldata, path=paste0("//",emarSTR,"MonitoringSiteReferenceData/emar:Macro")), xmlValue)))
    ## if emar namespace does not occur before TypeName in element,then try without namespace
    ## Hilltop Server v1.80 excludes name space from element with TypeName tag
    if(any(macroData=="")){
      macroData=macroData[-which(macroData=="")]
    }    
    if(length(macroData)==0){
      macroData<-unique(sapply(getNodeSet(doc=xmldata, path="//MonitoringSiteReferenceData/emar:Macro"), xmlValue))
      if(any(macroData=="")){
        macroData=macroData[-which(macroData=="")]
      }
      if(length(macroData)>0){
        emarSTR <- ""
      }
    }
    
    macroData<-macroData[order(macroData,na.last = TRUE)]
    
    if(length(macroData)==2){
      module <- paste("[emar:Macro='",macroData[2],"']",sep="")
    } else {
      module <- paste("[emar:Macro='",tail(macroData,1),"']",sep="")
    }
    
    cat("\n",urls$Agency[h],"\n---------------------------\n",urls$URL[h],"\n",module,"\n",sep="")
    
    # Determine number of records in a wfs with module before attempting to extract all the necessary columns needed
    if(length(sapply(getNodeSet(doc=xmldata, 
                                path=paste0("//",emarSTR,"MonitoringSiteReferenceData",module,"/emar:",vars[1])),
                     xmlValue))==0){
      cat(urls$Agency[h],"has no records for <emar:Macro>\n")
    } else {
      
      for(i in 1:length(vars)){
        if(i==1){
          # for the first var, the CouncilSiteID
          a<- sapply(getNodeSet(doc=xmldata, 
                                path=paste0("//emar:CouncilSiteID/../../",
                                            emarSTR,"MonitoringSiteReferenceData",
                                            module,"/emar:",vars[i])), xmlValue)
          cat(vars[i],":\t",length(a),"\n")
          #Cleaning var[i] to remove any leading and trailing spaces
          a <- trimws(a)
          nn <- length(a)
          theseSites=a
          a=as.data.frame(a,stringsAsFactors=F)
          names(a)=vars[i]
        } else {
          #Get the new Measurement for each site already obtained.  
          #If the new Measurement is not there for a certain site, it will give it an NA 
          # for all subsequent vars
          b=NULL
          for(thisSite in 1:length(theseSites)){
            newb<- sapply(getNodeSet(doc=xmldata, 
                                     path=paste0("//emar:CouncilSiteID/../../",
                                                 emarSTR,"MonitoringSiteReferenceData[emar:CouncilSiteID=\"",
                                                 theseSites[thisSite],"\"] ",module,"/emar:",vars[i])),xmlValue)
            newb=unique(trimws(tolower(newb)))
            if(any(newb=="")){cat("#");newb=newb[-which(newb=="")]}
            if(length(newb)==0){newb=NA}#cat("$");
            if(length(newb)>1){
              cat(paste(newb,collapse='\t\t'),'\n')
              #If you get multiple responses for a given CouncilSiteID,
              #Check if there's multiple of this CouncilSiteID in the list.
              #If there is, figure which one you're up to, and apply that ones newb to this.
              if(length(which(theseSites==theseSites[thisSite]))>1){
                replicates=which(theseSites==theseSites[thisSite])
                whichReplicate = which(replicates==thisSite)
                newb=newb[whichReplicate]
                rm(replicates,whichReplicate)
              }
            }
            b=c(b,newb)
          }
          b=as.data.frame(b,stringsAsFactors = F)
          names(b)=vars[i]
          
          cat(vars[i],":\t",length(b[!is.na(b)]),"\n")
          if(any(is.na(b))){
            if(vars[i]=="Region"){
              b[is.na(b)] <-urls$Agency[h]
            } else if(vars[i]=="Agency"){
              b[is.na(b)]<-urls$Agency[h]
            } else {
              b[is.na(b)]<-""
            }
          }
          #b=apply(b,2,FUN=function(x)trimws(tolower(x)))
          #nra=nrow(a)
          #nrb=nrow(b)
          
          #wn=options('warn')$warn
          #options(warn=2)
          a <- cbind.data.frame(a,b)
          #options(warn=wn)
          rm(wn,b)
        }
      }
      a <- as.data.frame(a,stringsAsFactors=FALSE)
      #Do lat longs separately from other WQParams because they are value pairs and need separating
      b=matrix(data = 0,nrow=0,ncol=3)
      for(thisSite in 1:length(theseSites)){
        latlong=sapply(getNodeSet(doc=xmldata, path=paste0("//gml:Point[../../../",
                                                           emarSTR,"MonitoringSiteReferenceData[emar:CouncilSiteID='",
                                                           theseSites[thisSite],"'] ",module,"]")),xmlValue)  
        if(length(latlong)>0){
          if(length(latlong)>1){
            latlong <- t(apply(matrix(data = sapply(sapply(latlong,strsplit," "),as.numeric),ncol = 2),1,mean))
          }else{
            latlong <- as.numeric(unlist(strsplit(latlong,' ')))
          }
        } else {
          latlong=matrix(data = NA,nrow = 1,ncol=2)
        }
        latlong=c(theseSites[thisSite],latlong)
        b=rbind(b,latlong)
        rm(latlong)      
      }
      b=as.data.frame(b,stringsAsFactors=F)
      names(b)=c("CouncilSiteID","Lat","Long")
      b$Lat=as.numeric(b$Lat)
      b$Long=as.numeric(b$Long)
      
      a <- left_join(a,b)
      a$accessDate=format(Sys.Date(),"%d-%b-%Y")
      rm(b)
      return(a)
    }
  }
}->siteTable
stopCluster(workers)
rm(workers)

siteTable$SiteID=as.character(siteTable$SiteID)
siteTable$LawaSiteID=as.character(siteTable$LawaSiteID)

siteTable$Region=tolower(as.character(siteTable$Region))
siteTable$Agency=tolower(as.character(siteTable$Agency))

# #Load Auckland metadata separately.  
# acMetaData=read.csv("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/1.Imported/acRiverEcologyMetaDataC.csv",encoding='UTF-8',stringsAsFactors = F)
# names(acMetaData)=c("CouncilSiteID","LawaSiteID","Catchment","SiteID","Long","Lat","SWQAltitude","SWQLanduse","SWQFrequencyLast5","SWQFrequencyAll")
# acMetaData$Region='auckland'
# acMetaData$Agency='ac'
# acMetaData$Macro='yes'
# acMetaData$accessDate=format(file.info("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/2018_csv_config_files/acMacroMetaDataB.csv")$ctime,"%d-%b-%Y")
# acMetaData=acMetaData[which(acMetaData$LawaSiteID!=""),]
# 
# source('K:/R_functions/DMS2DD.r')
# latlon <- DMS2DD(cbind(acMetaData$Lat,acMetaData$Long))
# acMetaData$Lat=latlon[,1]
# acMetaData$Long=latlon[,2]
# rm(latlon)
# 
# 
# siteTable <- merge(siteTable,acMetaData%>%
#                      select("SiteID","CouncilSiteID", "LawaSiteID",
#                             "Macro","Region","Agency",
#                             "Lat","Long","SWQLanduse","SWQAltitude" ),all=T)
# rm(acMetaData)

table(siteTable$Agency)
siteTable$Agency[siteTable$Agency%in%c('ac','auckland council')] <- 'arc'
siteTable$Agency[tolower(siteTable$Agency)%in%c("christchurch", "environment canterbury")] <- 'ecan'

table(siteTable$Region)
siteTable$Region[siteTable$Region=='gdc'] <- 'gisborne'
siteTable$Region[siteTable$Region=='gwrc'] <- 'wellington'
siteTable$Region[siteTable$Region=='mdc'] <- 'marlborough'
siteTable$Region[siteTable$Region=='ncc'] <- 'nelson'
siteTable$Region[siteTable$Region=='nrc'] <- 'northland'
siteTable$Region[siteTable$Region=='orc'] <- 'otago'
siteTable$Region[siteTable$Region=='tdc'] <- 'tasman'
siteTable$Region[siteTable$Region=='trc'] <- 'taranaki'
siteTable$Region[siteTable$Region=='wcrc'] <- 'west coast'

## Swapping coordinate values where necessary
plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))

toSwitch=which(siteTable$Long<0 & siteTable$Lat>0)
if(length(toSwitch)>0){
  unique(siteTable$Agency[toSwitch])
  newLon=siteTable$Lat[toSwitch]
  siteTable$Lat[toSwitch] <- siteTable$Long[toSwitch]
  siteTable$Long[toSwitch]=newLon
  rm(newLon)
}
rm(toSwitch)

plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Region)))
table(siteTable$Agency)

siteTable=unique(siteTable)

plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))
these=which(siteTable$Long<(160))
if(length(these)>0){
  siteTable$Long[these] -> store
  siteTable$Long[these] <- siteTable$Lat[these]
  siteTable$Lat[these] <-  -store
  rm(store)
}
rm(these)

plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))
these=which(siteTable$Long<(0))
if(length(these)>0){
  siteTable$Long[these] -> store
  siteTable$Long[these] <- -siteTable$Lat[these]
  siteTable$Lat[these] <-  store
  plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))
  rm(store)
}
rm(these)


plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))
points(siteTable$Long,siteTable$Lat,pch=16,cex=0.2)
table(siteTable$Agency)

tolower(urls$Agency)[!tolower(urls$Agency)%in%tolower(siteTable$Agency)]

#siteTable$Long[siteTable$LawaSiteID=="NRWQN-00022"] <-siteTable$Long[siteTable$LawaSiteID=="NRWQN-00022"][2]

# For WCRC-00031 - location is wrong in WFS
# NZTM coordinates from WCRC website: 1466541,5295450
# WGS84, now:   Latitude	Longitude  	-42.48179737	171.37623113

# siteTable$Lat[siteTable$LawaSiteID=="WCRC-00031"]  <- -42.48179737
# siteTable$Long[siteTable$LawaSiteID=="WCRC-00031"] <- 171.37623113

## Correcting variations in Region names

# siteTable$Region[siteTable$Region=="BayOfPlenty"]   <- "Bay of Plenty"
# siteTable$Region[siteTable$Region=="WaikatoRegion"] <- "Waikato"
# siteTable$Region[siteTable$Region=="HawkesBay"]     <- "Hawkes Bay"
# siteTable$Region[siteTable$Region=="WestCoast"]     <- "West Coast"



# }

## Output for next script
# siteTable$Agency=tolower(siteTable$Agency)

library(tidyverse)
# Pull in land use from LCDB
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')
fwgr <- read_csv('d:/RiverData/FWGroupings.txt')
rec=read_csv("D:/RiverData/RECnz.txt")
rec$easting=fwgr$easting[match(rec$NZREACH,fwgr$NZREACH)]
rec$northing=fwgr$northing[match(rec$NZREACH,fwgr$NZREACH)]
rm(fwgr)
latLong=nzmg2wgs(East = rec$easting,North = rec$northing)
rec$Long=latLong[,2]
rec$Lat=latLong[,1]
rm(latLong)
rec2=read_csv("D:/RiverData/REC2Reaches.txt")
latLong=nztm2wgs(ce = rec2$upcoordX,cn = rec2$upcoordY)
rec2$Long=latLong[,2]
rec2$Lat=latLong[,1]
rm(latLong)

siteTable$NZReach=as.numeric(siteTable$NZReach)
siteTable$Landcover=NA #rec
siteTable$Altitude=NA  #rec2
siteTable$Order=NA
siteTable$StreamOrder=NA
st=1
for(st in st:dim(siteTable)[1]){
  if(is.na(siteTable$NZReach[st])){
    dists=sqrt((siteTable$Long[st]-rec$Long)^2+(siteTable$Lat[st]-rec$Lat)^2)
    mindist=which.min(dists)
    siteTable$NZReach[st] = rec$NZREACH[mindist]
    rm(dists,mindist)
  }
  recMatch = which(rec$NZREACH==siteTable$NZReach[st])
  if(length(recMatch)==0){
    cat('D')
    dists=sqrt((siteTable$Long[st]-rec$Long)^2+(siteTable$Lat[st]-rec$Lat)^2)
    recMatch=which.min(dists)
  }
  siteTable$Order[st]=paste(unique(rec$ORDER_[recMatch]),collapse='&')
  siteTable$Landcover[st]=paste(unique(rec$LANDCOVER[recMatch]),collapse='&')
  rec2match = which(rec2$nzreach_re == siteTable$NZReach[st]) 
  if(length(rec2match)==0){
    cat('d')
    dists=sqrt((siteTable$Long[st]-rec2$Long)^2+(siteTable$Lat[st]-rec2$Lat)^2)
    rec2match=which.min(dists)
  }
  siteTable$StreamOrder[st]=paste(unique(rec2$StreamOrde[rec2match]),collapse='&')
  siteTable$Altitude[st]=mean(rec2$upElev[rec2match],na.rm=T)
  rm(rec2match)
}
# table(siteTable$Order,siteTable$StreamOrder)

 write.csv(siteTable,'H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Metadata/SiteTableRawLandUse.csv',row.names=F)

#Categorise landcover and altitude
siteTable$SWQLanduse=tolower(as.character(siteTable$SWQLanduse))
siteTable$SWQLanduse[tolower(siteTable$SWQLanduse)%in%c("unstated","")] <- NA
siteTable$SWQLanduse[tolower(siteTable$SWQLanduse)%in%c("reference","forest","forestry","native","exotic","natural")] <- "forest"

table(siteTable$SWQLanduse,siteTable$Landcover)

siteTable$Landcover=tolower(as.character(siteTable$Landcover))
siteTable$Landcover[siteTable$Landcover%in%c('if','ef','s','t','w')] <- 'Forest'
siteTable$Landcover[siteTable$Landcover%in%c('p','b','m')] <- 'Rural'
siteTable$Landcover[siteTable$Landcover%in%c('u')] <- 'Urban'

table(siteTable$SWQLanduse,siteTable$Landcover)


siteTable$SWQAltitude[siteTable$SWQAltitude==""] <- "Unstated"
by(data = siteTable$Altitude,INDICES = siteTable$SWQAltitude,FUN = summary)
plot(siteTable$Altitude~factor(tolower(siteTable$SWQAltitude)))

altroc=pROC::roc(response=droplevels(factor(tolower(siteTable$SWQAltitude[siteTable$SWQAltitude!='unstated']))),
                 predictor=siteTable$Altitude[siteTable$SWQAltitude!='unstated'])
highlow=pROC::coords(roc=altroc,'best')[1]$threshold
rm(altroc)
abline(h=highlow,lty=2,lwd=2)
lowland = which(siteTable$Altitude<highlow)
siteTable$AltitudeCl='Upland'
siteTable$AltitudeCl[lowland]='Lowland'
rm(lowland)

siteTable$Agency=tolower(siteTable$Agency)


write.csv(x = siteTable,file = paste0("H:/ericg/16666LAWA/LAWA2020/Macroinvertebrates/data/",
                                      format(Sys.Date(),'%Y-%m-%d'),
                                      "/SiteTable_Macro",format(Sys.Date(),'%d%b%y'),".csv"),row.names = F)

#Get numbers of sites per agency ####
AgencyRep=table(factor(tolower(siteTable$Agency),levels=c("arc", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                          "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
AgencyRep=data.frame(agency=names(AgencyRep),count=as.numeric(AgencyRep))
names(AgencyRep)[2]=format(Sys.Date(),"%d%b%y")
MacroWFSsiteFiles=dir(path = "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                      pattern = 'SiteTable_Macro',
                      recursive = T,full.names = T)
for(wsf in MacroWFSsiteFiles){
  stin=read.csv(wsf,stringsAsFactors=F)
  agencyRep=table(factor(tolower(stin$Agency),levels=c("arc", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                       "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
  AgencyRep = cbind(AgencyRep,as.numeric(agencyRep))
  colnames(AgencyRep)[dim(AgencyRep)[2]] = strTo(strFrom(wsf,'_Macro'),'.csv')
  rm(agencyRep)
}
AgencyRep=AgencyRep[,-2]

rm(MacroWFSsiteFiles)
write.csv(AgencyRep,'h:/ericg/16666LAWA/LAWA2020/Metadata/AgencyRepMacroWFS.csv',row.names=F)

#     agency 23Jun20 25Jun20
# 1     arc      61      61
# 2   boprc     129     129
# 3    ecan     134     134
# 4      es      87      87
# 5     gdc      80      80
# 6    gwrc      53      53
# 7    hbrc      71      71
# 8     hrc      81      81
# 9     mdc      31      31
# 10    ncc      26      26
# 11   niwa       0       0
# 12    nrc      19      19
# 13    orc      30      30
# 14    tdc      25      25
# 15    trc      60      60
# 16   wcrc      34      34
# 17    wrc       0      74

# agency 07Jun19 10Jun19 28Jun19 08Jul19 11Jul19 17Jul19 22Jul19 29Jul19 02Aug19 05Aug19 12Aug19 19Aug19 26Aug19
# 1     arc       0       0       0       0      61      61      61      61      61      61      61      61      61
# 2   boprc     138     138     138     138     129     129     129     129     129     129     129     129     129
# 3    ecan     135     135     135     134     134     134     134     134     134     134     134     134     134
# 4      es      83      83      87      87      87      87      87      87      87      87      87      87      87
# 5     gdc      81      81      81      81      81      81      81      81      81      81      81      80      80
# 6    gwrc      53      53      53      53      53      53      53      53      53      53      53      53      53
# 7    hbrc      70      70      70      70      70      70      70      70      70      70      70      71      71
# 8     hrc       0      93      84      84      84      84      84      81      81      81      81      81      81
# 9     mdc      31      31      31      31      31      31      31      31      31      31      31      31      31
# 10    ncc      26      26      26      26      26      26      26      26       0      26      26      26       0
# 11   niwa       0       0       0       0       0       0       0       0       0       0       0       0       0
# 12    nrc      20      20      20      20      20      19      19      19      19      19      19      19      19
# 13    orc      30      30      30      30      30      30      30      30      30      30      30      30      30
# 14    tdc      49      49      49      49      49      58      58      58      25      25      25      25      25
# 15    trc      58     116      58      58      58      58      58      58      58      58      58      58      58
# 16   wcrc      34      34      34       0      34      34      34      34      34      34      34      34      34
# 17    wrc      74      74      74      74      74      74      74      74      74      74      74      74      74


# 
# oldsiteTable=read.csv('h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/2019-08-19/SiteTable_Macro19Aug19.csv',stringsAsFactors=F)
# siteTable=rbind(siteTable,oldsiteTable%>%filter(Agency=='ncc'))
# rm(oldsiteTable)
