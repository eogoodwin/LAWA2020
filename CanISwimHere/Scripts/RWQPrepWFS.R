rm(list = ls())
library(tidyverse)
library(parallel)
library(doParallel)
source('H:/ericg/16666LAWA/LAWA2019/scripts/LAWAFunctions.R')

try(shell(paste0('mkdir "H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)

urls          <- read.csv("H:/ericg/16666LAWA/LAWA2019/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)

#The first on the list prioritises the ID to be used and match every else to it.
vars <- c("CouncilSiteID","SiteID","LawaSiteID","NZReach","Region","Agency","RWQAltitude","RWQLanduse")

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
  
  xmldata<-try(ldWFS(urlIn = URLencode(urls$URL[h]),agency=urls$Agency[h],dataLocation = urls$Source[h],case.fix = TRUE))
  
  if(is.null(xmldata)||'try-error'%in%attr(xmldata,'class')||grepl(pattern = '^501|error',
                                                                   x=xmlValue(getNodeSet(xmldata,'/')[[1]]),
                                                                   ignore.case=T)){
    cat('Failed for ',urls$Agency[h],'\n')
     return(NULL)
  }
  
  emarSTR="emar:"
  rwq<-unique(sapply(getNodeSet(doc=xmldata, path=paste0("//",emarSTR,"MonitoringSiteReferenceData/emar:RWQuality")), xmlValue))
  ## if emar namespace does not occur before TypeName in element,then try without namespace
  ## Hilltop Server v1.80 excludes name space from element with TypeName tag
  if(any(rwq=="")){
    rwq=rwq[-which(rwq=='')]
  }
  if(length(rwq)==0){
    rwq<-unique(sapply(getNodeSet(doc=xmldata, path="//MonitoringSiteReferenceData/emar:RWQuality"), xmlValue))
    if(any(rwq=="")){
      rwq=rwq[-which(rwq=='')]
    }
    if(length(rwq)>0){
      emarSTR <- ""
    }else{
      return(NULL)
    }
  }
  
  rwq<-rwq[order(rwq,na.last = TRUE)]
  
  if(length(rwq)==2){
    module <- paste("[emar:RWQuality='",rwq[2],"']",sep="")
  } else {
    if(all(rwq%in%c("NO","Yes","YES"))){   #This copes specifically with ecan, which had these three present
      module <- paste("[emar:RWQuality=",c("'Yes'","'YES'")[which(c("Yes","YES")%in%rwq)],"]",sep='')
    }else{
      module <- paste("[emar:RWQuality='",rwq[tolower(rwq)%in%c("yes","true","1")],"']",sep="")
    }
  }
  
  cat("\n",urls$Agency[h],"\n---------------------------\n",urls$URL[h],"\n",module,"\n",sep="")
  
  # Determine number of records in a wfs with module before attempting to extract all the necessary columns needed
  if(length(sapply(getNodeSet(doc=xmldata, 
                              path=paste0("//",emarSTR,"MonitoringSiteReferenceData",module,"/emar:",vars[1])),
                   xmlValue))==0){
    cat(urls$Agency[h],"has no records for <emar:RWQuality>\n")
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
}->siteTable
stopCluster(workers)
rm(workers)

siteTable$SiteID=as.character(siteTable$SiteID)
siteTable$LawaSiteID=as.character(siteTable$LawaSiteID)
siteTable$Region=tolower(as.character(siteTable$Region))
siteTable$Agency=tolower(as.character(siteTable$Agency))




table(siteTable$Region)
siteTable$Region=tolower(siteTable$Region)
siteTable$Region[siteTable$Region=='auckland council'] <- 'auckland'
siteTable$Region[siteTable$Region=='orc'] <- 'otago'
siteTable$Region[siteTable$Region=='gdc'] <- 'gisborne'
siteTable$Region[siteTable$Region=='wcrc'] <- 'west coast'
siteTable$Region[siteTable$Region=='ncc'] <- 'nelson'
siteTable$Region[siteTable$Region=='gwrc'] <- 'wellington'
siteTable$Region[siteTable$Region=='nrc'] <- 'northland'
siteTable$Region[siteTable$Region=='mdc'] <- 'marlborough'
siteTable$Region[siteTable$Region=='tdc'] <- 'tasman'
siteTable$Region[siteTable$Region=='trc'] <- 'taranaki'
table(siteTable$Region)


table(siteTable$Agency)
siteTable$Agency[siteTable$Agency%in%c('ac','auckland','auckland council')] <- 'arc'
siteTable$Agency[siteTable$Agency=='christchurch'] <- 'ecan'
siteTable$Agency[siteTable$Agency=='environment canterbury'] <- 'ecan'
table(siteTable$Agency)


siteTable$SiteID=as.character(siteTable$SiteID)
siteTable$LawaSiteID[grepl(pattern = "[^a-z,A-Z, ,/,@,0-9,.,-,[:punct:]]",x = siteTable$SiteID,ignore.case = T)]
grep(pattern = "[^a-z,A-Z, ,/,@,0-9,.,-,[:punct:]]",x = siteTable$SiteID,value=T,ignore.case = T)
siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("EBOP-10007")] <- "lake okaro at boat ramp"
siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("EBOP-10041")] <- "maketa at surf club"
siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("LAWA-100485")] <- "ohau channel at lake rotorua outlet"
siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("EBOP-10061")] <- "ohiwa harbour at reserve boat ramp"
siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("EBOP-10044")] <- "omanu surf club"
siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("EBOP-10054")] <- "waiha beach at 3 mile creek"
siteTable$SiteID[tolower(siteTable$LawaSiteID)==tolower("EBOP-10055")] <- "waiha beach at surf club"
grep(pattern = "[^a-z,A-Z, ,/,@,0-9,.,-,[:punct:]]",x = siteTable$SiteID,value=T,ignore.case = T)

## Swapping coordinate values where necessary
plot(siteTable$Long,siteTable$Lat)


siteTable=unique(siteTable)


write.csv(x = siteTable,
          file = paste0("H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/",
                        format(Sys.Date(),"%Y-%m-%d"),
                        "/SiteTable_CISH",format(Sys.Date(),"%d%b%y"),".csv"),row.names = F)







#Get numbers of sites per agency ####
AgencyRep=table(factor(tolower(siteTable$Agency),levels=c("arc", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                          "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
AgencyRep=data.frame(agency=names(AgencyRep),count=as.numeric(AgencyRep))
names(AgencyRep)[2]=format(Sys.Date(),"%d%b%y")

RWQWFSsiteFiles=dir(path = "H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/",
                   pattern = 'SiteTable_CISH',
                   recursive = T,full.names = T)
for(wsf in RWQWFSsiteFiles){
  stin=read.csv(wsf,stringsAsFactors=F)
  agencyRep=table(factor(stin$Agency,levels=c("arc", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                              "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
  AgencyRep = cbind(AgencyRep,as.numeric(agencyRep))
  names(AgencyRep)[dim(AgencyRep)[2]] = strTo(strFrom(wsf,'CISH'),'.csv')
  rm(agencyRep)
}
AgencyRep=AgencyRep[,-2]
rm(RWQWFSsiteFiles)

write.csv(AgencyRep,'h:/ericg/16666LAWA/LAWA2019/Metadata/AgencyRepCISHWFS.csv',row.names=F)





