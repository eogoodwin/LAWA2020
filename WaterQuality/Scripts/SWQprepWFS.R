rm(list = ls())
library(tidyverse)
library(parallel)
library(doParallel)
setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality/")
source("H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R")
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')

# https://www.lawa.org.nz/media/18225/nrwqn-monitoring-sites-sheet1-sheet1.pdf

dir.create(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/", format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

urls          <- read.csv("H:/ericg/16666LAWA/LAWA2020/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)

if(0){
  if(HRCsWFSisMisBehaving){
    urls$URL[8] <- "H:\\ericg\\16666LAWA\\LAWA2020\\WaterQuality\\Data\\2020-07-31\\HRC_WFS.xml"
  }
}

#The first on the list prioritises the ID to be used and match every else to it.
vars <- c("CouncilSiteID","SiteID","LawaSiteID","NZReach","Region","Agency","SWQAltitude","SWQLanduse","Catchment")

if(exists('siteTable')){
  rm(siteTable)
}

#For each council, find the "emar:SWQuality" sites that are switched on, and get their details
startTime=Sys.time()
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
  
  if(is.null(xmldata)||'try-error'%in%attr(xmldata,'class')||grepl(pattern = '^501|error',
                                                                   x=xmlValue(getNodeSet(xmldata,'/')[[1]]),
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
  }else {
    ### Determine the values used in the [emar:SWQuality] element
    emarSTR="emar:"
    swq<-unique(sapply(getNodeSet(doc=xmldata, path=paste0("//",emarSTR,"MonitoringSiteReferenceData/emar:SWQuality")), xmlValue))
    ## if emar namespace does not occur before TypeName in element,then try without namespace
    ## Hilltop Server v1.80 excludes name space from element with TypeName tag
    if(any(swq=="")){
      swq=swq[-which(swq=='')]
    }
    if(length(swq)==0){
      swq<-unique(sapply(getNodeSet(doc=xmldata, path="//MonitoringSiteReferenceData/emar:SWQuality"), xmlValue))
      if(any(swq=="")){
        swq=swq[-which(swq=='')]
      }
      if(length(swq)>0){
        emarSTR <- ""
      }
    }
    
    swq<-swq[order(swq,na.last = TRUE)]
    
    if(length(swq)==2){
      module <- paste("[emar:SWQuality='",swq[2],"']",sep="")
    } else {
      if(all(swq%in%c("NO","Yes","YES"))){   #This copes specifically with ecan, which had these three present
        module <- paste("[emar:SWQuality=",c("'Yes'","'YES'")[which(c("Yes","YES")%in%swq)],"]",sep='')
      }else{
        module <- paste("[emar:SWQuality='",swq[tolower(swq)%in%c('yes','true',1)],"']",sep="")
      }
    }
    
    cat("\n",urls$Agency[h],"\n---------------------------\n",urls$URL[h],"\n",module,"\n",sep="")
    
    # Determine number of records in a wfs with module before attempting to extract all the necessary columns needed
    if(length(sapply(getNodeSet(doc=xmldata, 
                                path=paste0("//",emarSTR,"MonitoringSiteReferenceData",module,"/emar:",vars[1])),
                     xmlValue))==0){
      cat(urls$Agency[h],"has no records for <emar:SWQuality>\n")
    } else {
      
      for(i in 1:length(vars)){
        if(i==1){
          # for the first var, the CouncilSiteID
          a<- sapply(getNodeSet(doc=xmldata, 
                                path=paste0("//emar:CouncilSiteID/../../",
                                            emarSTR,"MonitoringSiteReferenceData",
                                            module,"/emar:CouncilSiteID")),xmlValue)
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
                                                 emarSTR,"MonitoringSiteReferenceData[emar:CouncilSiteID='",
                                                 theseSites[thisSite],"'] ",module,"/emar:",vars[i])),xmlValue)
            newb=unique(trimws(tolower(newb)))
            if(any(newb=="")){cat("#");newb=newb[-which(newb=="")]}
            if(length(newb)==0){cat("$");newb=NA}
            if(length(newb)>1){
              cat(theseSites[thisSite],paste('\t',newb,collapse='\t\t'),'\n')
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
          a <- cbind.data.frame(a,b)
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
        }else{
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
      return(a)
    }
  }
}->siteTable
stopCluster(workers)
rm(workers)

Sys.time()-startTime  #20s

siteTable$SiteID=as.character(siteTable$SiteID)
siteTable$LawaSiteID=as.character(siteTable$LawaSiteID)
siteTable$Region=tolower(as.character(siteTable$Region))
siteTable$Agency=tolower(as.character(siteTable$Agency))

#Add NIWA sites separately ####
if(file.exists("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/NIWALawaSiteIDs.csv")){
  niwaSub=read.csv("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/NIWALawaSiteIDs.csv",stringsAsFactors=F)
}else{
  df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/niwaSWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
  sites=df$Value[which(df$Type=='site')]
  niwaSub=data.frame(CouncilSiteID=sites,LawaSiteID=NA,SiteID=NA,NZReach=NA,
                     Region=NA,Agency='niwa',Catchment=NA,Lat=NA,Long=NA,accessDate=Sys.Date(),stringsAsFactors=F)
  #Get WFS infos
  for(niwaSite in seq_along(sites)){
    urlwfs = paste0("https://hydro-sos.niwa.co.nz/?service=SOS&version=2.0.0&request=GetFeatureOfInterest",
                    "&featureOfInterest=",sites[niwaSite])
    urlwfs <- URLencode(urlwfs)
    xmlfile <- ldWFS(urlIn=urlwfs,dataLocation = 'web',agency = 'niwa',case.fix = F,method = 'curl')
    niwaSub$CouncilSiteID[niwaSite]=sapply(getNodeSet(doc=xmlfile,path = "//gml:identifier",namespaces=c(gml="http://www.opengis.net/gml/3.2")),xmlValue)
    niwaSub$SiteID[niwaSite]=sapply(getNodeSet(doc=xmlfile,path = "//gml:name",namespaces=c(gml="http://www.opengis.net/gml/3.2")),xmlValue)
    pos=sapply(getNodeSet(doc=xmlfile,path = "//ns:pos",namespaces=c(ns="http://www.opengis.net/gml/3.2")),xmlValue)
    pos=sapply(strsplit(pos,' '),readr::parse_number)%>%as.numeric()
    niwaSub$Long[niwaSite]=pos[2]
    niwaSub$Lat[niwaSite]=pos[1]
    rm(urlwfs,xmlfile)
  }
  rm(sites,df)
  
  # Fix NIWA region assignment
  plot(niwaSub$Long,niwaSub$Lat)
  for(ns in seq_along(niwaSub$SiteID)){
    dists=sqrt((niwaSub$Long[ns]-siteTable$Long)^2+(niwaSub$Lat[ns]-siteTable$Lat)^2)
    closest=which.min(dists)
    segments(x0 = niwaSub$Long[ns],y0=niwaSub$Lat[ns],
             x1 = siteTable$Long[closest],y1=siteTable$Lat[closest])
    if(dists[closest]<0.71){
      niwaSub$Region[ns]=siteTable$Region[closest]
    }else{
      browser()
    }
  }
  
  # monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2019/Metadata/Monitoring Sites - 8 July 2019.xlsx',guess_max = 5000)
  monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/Metadata/LAWA Masterlist of Umbraco Sites as at 30 July 2020.xlsx',
                              sheet=1)
  lawaMasterList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/Metadata/LAWA Site Master List.xlsx',sheet=2)
  # lawaMasterList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/Metadata/LAWA Masterlist of Umbraco Sites as at 15 June 2020.xlsx',
                                     # sheet=1)
  
  #
  for(ns in seq_along(niwaSub$SiteID)){
    dists = adist(tolower(strTo(niwaSub$SiteID[ns],' \\(')),
                  trimws(tolower(monList$Name)))
    clM = which.min(dists)
    # geoDist=sqrt((niwaSub$Long[ns]-monList$Long[clM])^2+(niwaSub$Lat[ns]-monList$Lat[clM])^2)
    cat(tolower(strTo(niwaSub$SiteID[ns],' \\(')),'\t',monList$Name[clM],'\t',min(dists),'\n')
    check=readline()
    if(check==""){
      niwaSub$LawaSiteID[ns]=monList$LAWAID[clM]
    }
    segments(x0=niwaSub$Long[ns],y0=niwaSub$Lat[ns],
             x1=monList$Long[clM],y1=monList$Lat[clM],col='blue',lwd=20)
  }
  sum(is.na(niwaSub$LawaSiteID))
  niwaSub[is.na(niwaSub$LawaSiteID),]
  these=which(is.na(niwaSub$LawaSiteID))

  # monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2019/Metadata/Monitoring Sites - 8 July 2019.xlsx',guess_max = 5000)
  monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/Metadata/LAWA Masterlist of Umbraco Sites as at 30 July 2020.xlsx',
                                     sheet=1)
  # monList = rbind(monList,newmonList)
  lawaMasterList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/Metadata/LAWA Site Master List.xlsx',sheet=2)
  
  
  lawaMasterList$Long = nztm2wgs(ce = as.numeric(lawaMasterList$NZTM_Easting),cn = as.numeric(lawaMasterList$NZTM_Northing))[,2]
  lawaMasterList$Lat = nztm2wgs(ce = as.numeric(lawaMasterList$NZTM_Easting),cn = as.numeric(lawaMasterList$NZTM_Northing))[,1]
  lawaMasterList$Lat[lawaMasterList$Long>200] <- NA
  lawaMasterList$Long[lawaMasterList$Long>200] <- NA
  
  lawaMasterList$Long[is.na(lawaMasterList$Long)] <- siteTable$Long[match(lawaMasterList$`LAWA ID`[is.na(lawaMasterList$Long)],
                                                                          siteTable$LawaSiteID)]
  lawaMasterList$Lat[is.na(lawaMasterList$Lat)] <- siteTable$Lat[match(lawaMasterList$`LAWA ID`[is.na(lawaMasterList$Lat)],
                                                                       siteTable$LawaSiteID)]
  lawaMasterList$Long[is.na(lawaMasterList$Long)] <- siteTable$Long[match(lawaMasterList$`Site Name`[is.na(lawaMasterList$Long)],
                                                                          siteTable$LawaSiteID)]
  lawaMasterList$Lat[is.na(lawaMasterList$Lat)] <- siteTable$Lat[match(lawaMasterList$`Site Name`[is.na(lawaMasterList$Lat)],
                                                                       siteTable$LawaSiteID)]
  
  monList$Long = lawaMasterList$Long[match(monList$LAWAID,lawaMasterList$`LAWA ID`)]
  monList$Lat = lawaMasterList$Lat[match(monList$LAWAID,lawaMasterList$`LAWA ID`)]
  monList$Long = siteTable$Long[match(tolower(monList$LAWAID),tolower(siteTable$LawaSiteID))]
  monList$Lat = siteTable$Lat[match(tolower(monList$LAWAID),tolower(siteTable$LawaSiteID))]
  
  
  plot(niwaSub$Long[these],niwaSub$Lat[these],xlim=range(monList$Long,na.rm=T),ylim=range(monList$Lat,na.rm=T))
  for(ns in seq_along(these)){
    dists=sqrt((niwaSub$Long[these[ns]]-siteTable$Long)^2+(niwaSub$Lat[these[ns]]-siteTable$Lat)^2)
    closest=which.min(dists)
    segments(x0 = niwaSub$Long[these[ns]],y0=niwaSub$Lat[these[ns]],
             x1 = siteTable$Long[closest],y1=siteTable$Lat[closest],col='blue',lwd=2)
    cat(these[ns],'\t',niwaSub$SiteID[these[ns]],'\n')
    cat("siteTable\t","distance\t",min(dists,na.rm=T),'\t',siteTable$SiteID[closest],'\n')
    
    dists=sqrt((niwaSub$Long[these[ns]]-monList$Long)^2+(niwaSub$Lat[these[ns]]-monList$Lat)^2)
    closest=which.min(dists)
    segments(x0 = niwaSub$Long[these[ns]],y0=niwaSub$Lat[these[ns]],
             x1 = monList$Long[closest],y1=monList$Lat[closest],col='red',lwd=2)
    cat("monList\t\t","distance\t",min(dists,na.rm=T),'\t',monList$Name[closest],'\n')
  }
  
  #Try filling gaps from old NIWA data
  load('H:/ericg/16666LAWA/2018/WaterQuality/1.Imported/NIWAwqData.rData',verbose=T)  #NIWAdataInB
  cbind(niwaSub$SiteID[is.na(niwaSub$LawaSiteID)],
        NIWAdataInB[match(niwaSub$CouncilSiteID[is.na(niwaSub$LawaSiteID)],
                          NIWAdataInB$SiteName),c("CouncilSiteID","SiteName","LawaSiteID")])
  
  niwaSub$LawaSiteID[is.na(niwaSub$LawaSiteID)] = 
    NIWAdataInB$LawaSiteID[match(niwaSub$CouncilSiteID[is.na(niwaSub$LawaSiteID)],
                                 NIWAdataInB$SiteName)]
  #Check names match
  cbind(niwaSub$SiteID,NIWAdataInB[match(niwaSub$CouncilSiteID,NIWAdataInB$SiteName),c("CouncilSiteID","SiteName")])
  
  niwaSub <- left_join(niwaSub,NIWAdataInB%>%select(LawaSiteID,SWQAltitude,SWQLanduse)%>%distinct,by="LawaSiteID")
  rm(NIWAdataInB)
  niwaSub$LawaSiteID=tolower(niwaSub$LawaSiteID)
  write.csv(niwaSub,"H:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/NIWALawaSiteIDs.csv",row.names=F)
}##
siteTable=merge(x=siteTable,y=niwaSub,all.x=T,all.y=T)
rm(niwaSub)



table(siteTable$Region)
siteTable$Region=tolower(siteTable$Region)
# siteTable$Region[siteTable$Region=='auckland council'] <- 'auckland'
siteTable$Region[siteTable$Region=='orc'] <- 'otago'
siteTable$Region[siteTable$Region=='gdc'] <- 'gisborne'
siteTable$Region[siteTable$Region=='wcrc'] <- 'west coast'
siteTable$Region[siteTable$Region=='ncc'] <- 'nelson'
siteTable$Region[siteTable$Region=='gwrc'] <- 'wellington'
siteTable$Region[siteTable$Region=='nrc'] <- 'northland'
siteTable$Region[siteTable$Region=='mdc'] <- 'marlborough'
siteTable$Region[siteTable$Region=='tdc'] <- 'tasman'
siteTable$Region[siteTable$Region=='trc'] <- 'taranaki'
siteTable$Region[siteTable$Region=='horizons'] <- 'manawatu-whanganui'
table(siteTable$Region)


table(siteTable$Agency)
siteTable$Agency[siteTable$Agency%in%c('arc','auckland','auckland council')] <- 'ac'
siteTable$Agency[siteTable$Agency=='christchurch'] <- 'ecan'
siteTable$Agency[siteTable$Agency=='environment canterbury'] <- 'ecan'
siteTable$Agency[siteTable$Agency%in%c('boprc/niwa','niwa/boprc')] <- 'boprc'
table(siteTable$Agency)

agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
(agencies[!agencies%in%siteTable$Agency]->missingAgencies)
unique(siteTable$Agency[!siteTable$Agency%in%agencies])



## Changing BOP Site names that use extended characters
## Waiōtahe at Toone Rd             LAWA-100395   Waiotahe at Toone Rd 
## Waitahanui at Ōtamarākau Marae   EBOP-00038    Waitahanui at Otamarakau Marae
siteTable$SiteID=as.character(siteTable$SiteID)
grep(pattern = "[^a-z,A-Z, ,/,@,0-9,.,-,[:punct:]]",x = siteTable$SiteID,value=T,ignore.case = T)
# siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("LAWA-100395")] <- "Waiotahe at Toone Rd"
# siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("EBOP-00038")] <- "Waitahanui at Otamarakau Marae"
# siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("LAWA-102380")] <- "Wairepo Creek at Ohau Downs"
grep(pattern = "[^a-z,A-Z, ,/,@,0-9,.,-,[:punct:]]",x = siteTable$SiteID,value=T,ignore.case = T)


## Swapping coordinate values where necessary
plot(siteTable$Long,siteTable$Lat)

# toSwitch=which(siteTable$Long<0 & siteTable$Lat>0)
# if(length(toSwitch)>0){
#   unique(siteTable$Agency[toSwitch])
#   newLon=siteTable$Lat[toSwitch]
#   siteTable$Lat[toSwitch] <- siteTable$Long[toSwitch]
#   siteTable$Long[toSwitch]=newLon
#   rm(newLon)
# }
# rm(toSwitch)

plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Region)))
points(siteTable$Long[siteTable$Agency=='niwa'],
       siteTable$Lat[siteTable$Agency=='niwa'],pch=16,cex=0.5,
       col=as.numeric(factor(siteTable$Region[siteTable$Agency=='niwa'])))
table(siteTable$Agency)



siteTable=unique(siteTable)  #boprc had four dupliates, boprc had two duplicates
#31/7/2020 1055 to 1049  
by(INDICES = siteTable$Agency,data = siteTable,FUN = function(x)head(x))


#Pull in land use from LCDB
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')
fwgr <- read_csv('d:/RiverData/FWGroupings.txt')
rec=read_csv("d:/RiverData/RECnz.txt")
rec$easting=fwgr$easting[match(rec$NZREACH,fwgr$NZREACH)]
rec$northing=fwgr$northing[match(rec$NZREACH,fwgr$NZREACH)]
rm(fwgr)
latLong=nzmg2wgs(East = rec$easting,North = rec$northing)
rec$Long=latLong[,2]
rec$Lat=latLong[,1]
rm(latLong)
rec2=read_csv("d:/RiverData/REC2Reaches.txt")
latLong=nztm2wgs(ce = rec2$upcoordX,cn = rec2$upcoordY)
rec2$Long=latLong[,2]
rec2$Lat=latLong[,1]
rm(latLong)

siteTable$NZReach=as.numeric(siteTable$NZReach)
siteTable$NZReach[siteTable$NZReach<min(rec$NZREACH)|siteTable$NZReach>max(rec$NZREACH)] <- NA
siteTable$Landcover=NA #rec
siteTable$Altitude=NA  #rec2
siteTable$Order=NA       #rec
siteTable$StreamOrder=NA #rec2
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
    cat('No REC reachmatch\t',siteTable$NZReach[st],'\t\n')
    dists=sqrt((siteTable$Long[st]-rec$Long)^2+(siteTable$Lat[st]-rec$Lat)^2)
    recMatch=which.min(dists)
  }
  siteTable$Order[st]=paste(unique(rec$ORDER_[recMatch]),collapse='&')
  siteTable$Landcover[st]=paste(unique(rec$LANDCOVER[recMatch]),collapse='&')

  rec2match = which(rec2$nzreach_re == siteTable$NZReach[st]) 
  if(length(rec2match)==0){
    dists=sqrt((siteTable$Long[st]-rec2$Long)^2+(siteTable$Lat[st]-rec2$Lat)^2)
    rec2match=which.min(dists)
    cat('No REC2 reachmatch\t',siteTable$Agency[st],'\t',siteTable$NZReach[st],'\t',min(dists,na.rm=T),rec2$nzreach_re[rec2match],'\n')
    
  }
  siteTable$StreamOrder[st]=paste(unique(rec2$StreamOrde[rec2match]),collapse='&')
  siteTable$Altitude[st]=mean(rec2$upElev[rec2match],na.rm=T)
  rm(rec2match)
}
# table(siteTable$StreamOrder,siteTable$Order)

siteTable <- siteTable%>%select(-StreamOrder,-Order)


if(!all(agencies%in%unique(siteTable$Agency))){  #actually if you need to pull sites in from an old WFS sesh, like if one of them doesn respond
  oldsiteTable = read.csv("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/2020-07-29/SiteTable_River29Jul20.csv",stringsAsFactors = F)
  missingCouncils = agencies[!agencies%in%unique(siteTable$Agency)]
  oldsiteTable=oldsiteTable%>%filter(Agency%in%missingCouncils)
  if(dim(oldsiteTable)[1]>0){
    siteTable = rbind(siteTable,oldsiteTable[,names(siteTable)])
  }
  rm(oldsiteTable)
}

# write.csv(siteTable,'h:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/SiteTableRawLandUse.csv',row.names=F)

#Categorise landcover and altitude
siteTable$SWQLanduse=tolower(as.character(siteTable$SWQLanduse))
siteTable$SWQLanduse[tolower(siteTable$SWQLanduse)%in%c("unstated","")] <- NA
siteTable$SWQLanduse[tolower(siteTable$SWQLanduse)%in%c("reference","forest","forestry","native","exotic","natural")] <- "Forest"
siteTable$SWQLanduse=pseudo.titlecase(siteTable$SWQLanduse)

table(siteTable$SWQLanduse,siteTable$Landcover)

#Landcover's from REC, SWQLanduse is coucnil-provided
siteTable$Landcover=tolower(as.character(siteTable$Landcover))
siteTable$Landcover[siteTable$Landcover%in%c('if','ef','s','t','w')] <- 'Forest' #indigenous forest, exotic forest, scrub, tussock, wetland
siteTable$Landcover[siteTable$Landcover%in%c('p','b','m')] <- 'Rural'            #pastoral, bare, ??
siteTable$Landcover[siteTable$Landcover%in%c('u')] <- 'Urban'                    #urban

table(siteTable$SWQLanduse,siteTable$Landcover)


#A per-council audit of conflincts between their WFSed land uses and those proposed by REC.
perRegionAudit=siteTable%>%dplyr::select(-accessDate)%>%filter(SWQLanduse!=Landcover)%>%
  dplyr::select(Region,Agency,CouncilSiteID:NZReach,Lat,Long,SWQLanduse,Landcover)%>%split(.$Region)

for(ag in seq_along(perRegionAudit)){
  thagency=unique(perRegionAudit[[ag]]$Agency)
  if('niwa'%in%thagency){thagency=thagency[thagency!='niwa']}
  write.csv(perRegionAudit[[ag]],paste0('h:/ericg/16666LAWA/LAWA2020/WaterQuality/Audit/',thagency,'LandUseAudit.csv'),row.names=F)
}


siteTable$SWQAltitude[is.na(siteTable$SWQAltitude)|siteTable$SWQAltitude==""] <- "unstated"
siteTable$SWQAltitude=tolower(siteTable$SWQAltitude)
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

table(siteTable$AltitudeCl,siteTable$SWQAltitude)

#Per-council audit of altitude
perRegAltAudit=siteTable%>%dplyr::select(-accessDate)%>%filter(tolower(SWQAltitude)!=tolower(AltitudeCl))%>%
  dplyr::select(Region,Agency,CouncilSiteID:NZReach,Lat,Long,SWQAltitude,AltitudeCl,Altitude)%>%split(.$Region)
sapply(perRegAltAudit,dim)
for(ag in seq_along(perRegAltAudit)){
  thagency=unique(perRegAltAudit[[ag]]$Agency)
  if('niwa'%in%thagency){thagency=thagency[thagency!='niwa']}
  write.csv(perRegAltAudit[[ag]],paste0('h:/ericg/16666LAWA/LAWA2020/WaterQuality/Audit/',thagency,'AltitudeAudit.csv'),row.names=F)
}


siteTable$SWQAltitude[siteTable$SWQAltitude=='unstated']=siteTable$AltitudeCl[siteTable$SWQAltitude=='unstated']
siteTable$SWQLanduse[is.na(siteTable$SWQLanduse)]=siteTable$Landcover[is.na(siteTable$SWQLanduse)]


# siteTable$CouncilSiteID = gsub(pattern = 'Wairaki River at',replacement = "Wairaki River ds",x = siteTable$CouncilSiteID)
# siteTable$CouncilSiteID = gsub(pattern = 'Dipton Rd',replacement = "Dipton Road",x = siteTable$CouncilSiteID)
# siteTable$CouncilSiteID = gsub(pattern = 'Makarewa Confl$',replacement = "Makarewa Confluence",x = siteTable$CouncilSiteID)

write.csv(x = siteTable,#%>%dplyr::select(-StreamOrder,-Order),
          file = paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                        format(Sys.Date(),"%Y-%m-%d"),
                        "/SiteTable_River",format(Sys.Date(),"%d%b%y"),".csv"),row.names = F)







#Get numbers of sites per agency ####
AgencyRep=table(factor(tolower(siteTable$Agency),levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                          "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
AgencyRep=data.frame(agency=names(AgencyRep),count=as.numeric(AgencyRep))
names(AgencyRep)[2]=format(Sys.Date(),"%d%b%y")

WQWFSsiteFiles=dir(path = "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                   pattern = 'SiteTable_River',
                   recursive = T,full.names = T)
for(wsf in WQWFSsiteFiles){
  stin=read.csv(wsf,stringsAsFactors=F)
  agencyRep=table(factor(stin$Agency,levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                              "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
  AgencyRep = cbind(AgencyRep,as.numeric(agencyRep))
  names(AgencyRep)[dim(AgencyRep)[2]] = strTo(strFrom(wsf,'River'),'.csv')
  rm(agencyRep)
}
AgencyRep=AgencyRep[,-2]
rm(WQWFSsiteFiles)

# agency 23Jun20 25Jun20 03Jul20 09Jul20 16Jul20 24Jul20 29Jul20 31Jul20 14Aug20 17Aug20 21Aug20
# 1      ac       0       0       0       0       0      41      36      35      35      35      35
# 2   boprc      47      47      47      47      47      47      47      47      55      55      55
# 3    ecan     183     183     184     184     184     184     184     184     184     184     184
# 4      es      60      60      60      60      60      60      60      60      60      60      60
# 5     gdc      39      39      39      39      39      39      39      39      39      39      39
# 6    gwrc      43      43      43      43      43      43      43      43      43      43      43
# 7    hbrc      71      71      85      85      85      85      85      85      86      86      86
# 8     hrc     138     138     138     138     138     138     138     138     137     137     137
# 9     mdc      32      32      32      32      32      32      32      32      32      32      32
# 10    ncc      25      25      25      25      25      25      25      25      25      25      25
# 11   niwa      77      77      77      77      77      77      77      77      77      77      77
# 12    nrc      32      32      32      32      32      32      32      32      32      32      32
# 13    orc      50      50      50      50      50      50      50      50      50      50      50
# 14    tdc      26      26      26      26      26      26      26      26      26      26      26
# 15    trc      22      22      22      22      22      22      22      22      22      22      22
# 16   wcrc      38      38      38      38      38      38      38      38      38      38      38
# 17    wrc       0     108     108     108     108     108     108     108     108     108     108
   

plot(x=as.numeric(AgencyRep[1,-1]),type='l',ylim=c(10,200))
apply(AgencyRep[,-1],1,function(x)lines(x))
text(rep(par('usr')[2]*0.95,dim(AgencyRep)[1]),AgencyRep[,dim(AgencyRep)[2]],AgencyRep[,1])

#    agency 23Jun20 25Jun20 03Jul20
# 1     arc      41      41      41
# 2   boprc      47      47      47
# 3    ecan     183     183     184
# 4      es      60      60      60
# 5     gdc      39      39      39
# 6    gwrc      43      43      43
# 7    hbrc      71      71      85
# 8     hrc     138     138     138
# 9     mdc      32      32      32
# 10    ncc      25      25      25
# 11   niwa      77      77      77
# 12    nrc      32      32      32
# 13    orc      50      50      50
# 14    tdc      26      26      26
# 15    trc      22      22      22
# 16   wcrc      38      38      38
# 17    wrc       0     108     108

# agency 07Jun19 10Jun19 19Jun19 19Jun19am 20Jun19 08Jul19 11Jul19 17Jul19 22Jul19 29Jul19 02Aug19 05Aug19 12Aug19 19Aug19   26Aug19
#   arc       5       5      41        36      41      41      36      36      36      36      41      41      41      41       41
# boprc      54      54      54        54      54      54      54      47      47      47      47      47      47      47       47
#  ecan     181     181     181       181     181     183     183     183     183     183     183     183     183     183      183
#    es      60      60      64        60      64      60      60      60      60      60      60      60      60      60       60
#   gdc      39      39      39        39      39      39      39      39      39      39      39      39      39      39       39
#  gwrc      43      43      43        43      43      43      43      43      43      43      43      43      43      43       43
#  hbrc      70      70      70        70      70      70      70      70      70      70      70      70      70      71       71
#   hrc       0     137     137       137     137     137     137     137     137     137     137     137     137     137      137
#   mdc      32      32      32        32      32      32      32      32      32      32      32      32      32      32       32
#   ncc      25      25      25        25      25      25      25      25      25      25       0      25      25      25        25
#  niwa       0       0       0         0      50      77      77      77      77      77      77      77      77      77       77
#   nrc      32      32      32        32      32      32      32      32      32      32      32      32      32      32       32
#   orc      47      47      47        47      47      47      47      47      47      47      47      47      47      50       50
#   tdc      45      45      46        45      46      46      46      55      55      55      26      26      26      26       26
#   trc      13      13      20        13      20      20      20      20      20      20      20      20      20      20       20
#  wcrc      38      38      38        38      38      38      38      38      38      38      38      38      38      38       38
#   wrc     108     108     108       108     108     108     108     108     108     108     108     108     108     108      108
 
# oldsitetable=read.csv("h:/ericg/16666LAWA/LAWA2019/WaterQuality/Data/2019-08-26/SiteTable_River26Aug19.csv",stringsAsFactors=F)
# siteTable=rbind(siteTable,oldsitetable%>%filter(Agency=='ncc'))

