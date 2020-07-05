rm(list = ls())
library(tidyverse)
library(parallel)
library(doParallel)
setwd('h:/ericg/16666LAWA/LAWA2020/Lakes')
source("h:/ericg/16666LAWA/LAWA2020/scripts/lawaFunctions.R")
try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2020/Lakes/Data/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)


urls          <- read.csv("H:/ericg/16666LAWA/LAWA2020/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)

# Config for data extract from WFS
vars <- c("CouncilSiteID","SiteID","LawaSiteID",
          "LFENZID","LType","GeomorphicLType",
          "Region","Agency","Catchment")



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
  } else {
    ### Determine the values used in the [emar:SWQuality] element
    emarSTR="emar:"
    lwq<-unique(sapply(getNodeSet(doc=xmldata, path="//emar:MonitoringSiteReferenceData/emar:LWQuality"), xmlValue))
    ## if emar namespace does not occur before TypeName in element,then try without namespace
    ## Hilltop Server v1.80 excludes name space from element with TypeName tag
    if(any(lwq=="")){
      lwq=lwq[-which(lwq=='')]
    }
    if(length(lwq)==0){
      lwq<-unique(sapply(getNodeSet(doc=xmldata, path="//MonitoringSiteReferenceData/emar:LWQuality"), xmlValue))
      if(any(lwq=="")){
        lwq=lwq[-which(lwq=='')]
      }
      if(length(lwq)>0){
        emarSTR <- ""
      }
    }
    
    lwq<-lwq[order(lwq,na.last = TRUE)]
    
    # if the only value returned is a No, NO, N, False or false, then no lake records in WFS
    if(length(lwq)==1){
      if(lwq %in% c("no","No","NO","N","F","false","FALSE","False")){
        cat(urls$Agency[h],"has no records for <emar:LWQuality>\n")
        return(NULL)
      }
    } else {
      # If there are three or more values that LWQuality can take in the WFS
      # this needs to be feed back to the Council to get it resolved.
      # in the meantime, just reduce it to two items, and check if the second item starts
      # with a "y" or "t". If second item doesn't, force it.
      if(length(lwq)>=3){
        lwq<-lwq[-1]
        if(!grepl(lwq[2],pattern="^[YyTt]")) lwq[2]<-"TRUE"
      }
    }
    if(length(lwq)==2){
      module <- paste("[",emarSTR,"LWQuality='",lwq[2],"']",sep="")
    } else {
      if(all(lwq%in%c("NO","Yes","YES"))){   #This copes specifically with ecan, which had these three present
        module <- paste("[",emarSTR,"LWQuality=",c("'Yes'","'YES'")[which(c("Yes","YES")%in%lwq)],"]",sep='')
      }else{
        module <- paste("[",emarSTR,"LWQuality='",lwq,"']",sep="")
      }
    }
    
    cat("\n",urls$Agency[h],"\n---------------------------\n",urls$URL[h],"\n",module,"\n",sep="")
    
    # Determine number of records in a wfs with module before attempting to extract all the necessary columns needed
    if(length(sapply(getNodeSet(doc=xmldata, 
                                path=paste0("//",emarSTR,"MonitoringSiteReferenceData",module,"/",emarSTR,vars[1])),
                     xmlValue))==0){
      cat(urls$Agency[h],"has no records for <emar:LWQuality>\n")
    } else {
      
      for(i in 1:length(vars)){
        if(i==1){
          # for the first var, the CouncilSiteID
          a<- sapply(getNodeSet(doc=xmldata,
                                path=paste0("//",emarSTR,"CouncilSiteID/../../",
                                            emarSTR,"MonitoringSiteReferenceData",
                                            module,"/",emarSTR,"",vars[1])), xmlValue)
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
  }
}->siteTable
stopCluster(workers)
rm(workers)
siteTable$SiteID=as.character(siteTable$SiteID)
siteTable$LawaSiteID=as.character(siteTable$LawaSiteID)
siteTable$Region=as.character(siteTable$Region)
siteTable$Agency=as.character(siteTable$Agency)

# 
# ################################################################################################
# #Load Auckland metadata separately.  
# acMetaData=read.csv("H:/ericg/16666LAWA/LAWA2020/Lakes/Metadata/ACLakesMetaData.csv",stringsAsFactors = F)[1:5,]
# names(acMetaData)=c("CouncilSiteID","SiteID","NZTME","NZMTN","SWQAltitude","Depth","SWQLanduse","SWQFrequencyLast5","SWQFrequencyAll")
# acMetaData$Region='auckland'
# acMetaData$Agency='arc'
# acMetaData$accessDate=format(file.info("H:/ericg/16666LAWA/LAWA2020/Lakes/Metadata/ACLakesMetaData.csv")$mtime,"%d-%b-%Y")
# source("k:/R_functions/nztm2wgs.r")
# latlon=nztm2wgs(ce = acMetaData$NZTME,cn = acMetaData$NZMTN)
# acMetaData$Long=latlon[,2]
# acMetaData$Lat=latlon[,1]
# rm(latlon)

# lawaIDs=read.csv("H:/ericg/16666LAWA/LAWA2020/Metadata/LAWAMasterSiteListasatMarch2018.csv",stringsAsFactors = F)
# lawaIDs=lawaIDs[lawaIDs$Module=="Lakes",]
# lawaIDs$Lat=as.numeric(lawaIDs$Latitude)
# lawaIDs$Long=as.numeric(lawaIDs$Longitude)
# sum(is.na(lawaIDs$Lat))
# sum(is.na(lawaIDs$Long))
# 
# md=rep(0,dim(acMetaData)[1])
# nameMatch=rep("",dim(acMetaData)[1])
# bestMatch=rep(NA,dim(acMetaData)[1])
# for(ast in 1:dim(acMetaData)[1]){
#   dists=sqrt((acMetaData$Lat[ast]-lawaIDs$Lat)^2+(acMetaData$Long[ast]-lawaIDs$Long)^2)
#   cat(min(dists,na.rm=T)*111000,'\t')
#   bestMatch[ast]=which.min(dists)
#   md[ast]=min(dists,na.rm=T)
#   nameMatch[ast]=lawaIDs$SiteName[which.min(dists)]
# }
# bestMatch[md>0.01] <- NA
# nameMatch[md>0.01] <- NA
# mean(md)*111000 #9.8 km?
# cbind(acMetaData[,1:2],nameMatch,md*111000)
# acMetaData$LawaSiteID=lawaIDs$LawaID[bestMatch]
# siteTable <- merge(siteTable,acMetaData,all=T)%>%select(c("CouncilSiteID", "LawaSiteID", "SiteID", "LFENZID",
#                                                           "LType","GeomorphicLType", 
#                                                           "Region", "Agency", "Lat", "Long", "accessDate"))
# rm(acMetaData,nameMatch,dists,md,ast,bestMatch)


siteTable$Region=tolower(siteTable$Region)
siteTable$Region[siteTable$Region=='alexandra'] <- 'otago'
siteTable$Region[siteTable$Region=='dunedin'] <- 'otago'
siteTable$Region[siteTable$Region=='tekapo'] <- 'otago'
siteTable$Region[siteTable$Region=='orc'] <- 'otago'
siteTable$Region[siteTable$Region=='christchurch'] <- 'canterbury'
siteTable$Region[siteTable$Region=='gdc'] <- 'gisborne'
siteTable$Region[siteTable$Region=='greymouth'] <- 'west coast'
siteTable$Region[siteTable$Region=='wcrc'] <- 'west coast'
siteTable$Region[siteTable$Region=='hamilton'] <- 'waikato'
siteTable$Region[siteTable$Region=='turangi'] <- 'waikato'
siteTable$Region[siteTable$Region=='wrc'] <- 'waikato'
siteTable$Region[siteTable$Region=='havelock nth'] <- 'hawkes bay'
siteTable$Region[siteTable$Region=='hbrc'] <- 'hawkes bay'
siteTable$Region[siteTable$Region=='ncc'] <- 'nelson'
siteTable$Region[siteTable$Region=='rotorua'] <- 'bay of plenty'
siteTable$Region[siteTable$Region=='wanganui'] <- 'horizons'
siteTable$Region[siteTable$Region=='gwrc'] <- 'wellington'
siteTable$Region[siteTable$Region=='whangarei'] <- 'northland'
siteTable$Region[siteTable$Region=='nrc'] <- 'northland'
siteTable$Region[siteTable$Region=='mdc'] <- 'marlborough'
siteTable$Region[siteTable$Region=='tdc'] <- 'tasman'
siteTable$Region[siteTable$Region=='trc'] <- 'taranaki'
siteTable$Region[siteTable$Region=='gwrc'] <- 'wellington'
table(siteTable$Region)

################################################################################################



siteTable$Agency=tolower(siteTable$Agency)
siteTable$Agency[siteTable$Agency=='ac'] <- 'arc'
siteTable$Agency[siteTable$Agency=='auckland council'] <- 'arc'
siteTable$Agency[siteTable$Agency=='christchurch'] <- 'ecan'
siteTable$Agency[siteTable$Agency=='environment canterbury'] <- 'ecan'
table(siteTable$Agency)

par(mfrow=c(1,1))
plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)),asp=1)
points(siteTable$Long,siteTable$Lat,pch=16,cex=0.2)

#Find which agencies have no lakes yet
tolower(urls$Agency)[!tolower(urls$Agency)%in%tolower(siteTable$Agency)]


# siteTable$LawaSiteID[siteTable$CouncilSiteID=="Lake Rotoiti Site 3"]='EBOP-00094'


#Lesley Walls 25/9/2018
#These are the BOP LFENZIDs that I have:
#    Lake                         FENZID
# Lake Okareka                    15325
# Lake Okaro                      14290
# Lake Okataina                   54731
# Lake Rerewhakaaitu              40071
# Lake Rotoehu                    40188
# Lake Rotoiti                    54730
# Lake Rotokakahi                 15621
# Lake Rotoma                     40102
# Lake Rotomahana                 54733
# Lake Rotorua                    11133
# Lake Tarawera                   54732
# Lake Tikitapu                   15312

siteTable$LFENZID[grep('okareka',siteTable$SiteID,ignore.case = T)] <- 15325
siteTable$LFENZID[grep('Okaro',siteTable$SiteID,ignore.case = T)] <- 14290
siteTable$LFENZID[grep('Okataina',siteTable$SiteID,ignore.case = T)] <- 54731
siteTable$LFENZID[grep('Rerewhakaaitu',siteTable$SiteID,ignore.case = T)] <- 40071
siteTable$LFENZID[grep('Rotoehu',siteTable$SiteID,ignore.case = T)] <- 40188
siteTable$LFENZID[which(grepl('Rotoiti',siteTable$SiteID,ignore.case = T)&tolower(siteTable$Agency)=='boprc')] <- 54730
siteTable$LFENZID[grep('Rotokakahi',siteTable$SiteID,ignore.case = T)] <- 15621
siteTable$LFENZID[grep('Rotoma site',siteTable$SiteID,ignore.case = T)] <- 40102
siteTable$LFENZID[grep('Rotomahana',siteTable$SiteID,ignore.case = T)] <- 54733
siteTable$LFENZID[grep('Rotorua site',siteTable$SiteID,ignore.case = T)] <- 11133
siteTable$LFENZID[grep('Tarawera',siteTable$SiteID,ignore.case = T)] <- 54732
siteTable$LFENZID[grep('Tikitapu',siteTable$SiteID,ignore.case = T)] <- 15312

siteTable$LFENZID[grep(x = siteTable$CouncilSiteID,pattern = "Wainamu",ignore.case = T)] <- "45819"
siteTable$LFENZID[grep(x = siteTable$CouncilSiteID,pattern = "Ototoa",ignore.case = T)] <- "50270"
siteTable$LFENZID[grep(x = siteTable$CouncilSiteID,pattern = "Tomarata",ignore.case = T)] <- "21871"
siteTable$LFENZID[grep(x = siteTable$CouncilSiteID,pattern = "Pupuke",ignore.case = T)] <- "50151"

siteTable$LFENZID[which(siteTable$CouncilSiteID=="SQ36148")] <- (-16)
siteTable$LFENZID[grepl('brunner',siteTable$CouncilSiteID,ignore.case=T)&siteTable$Agency=='wcrc'] <- 38974
siteTable$LFENZID[grepl('haupiri',siteTable$CouncilSiteID,ignore.case=T)&siteTable$Agency=='wcrc'] <- 39225

# siteTable$GeomorphicLType[grepl('brunner',siteTable$CouncilSiteID,ignore.case = T)&siteTable$Agency=='wcrc'] <- 'Glacial'
# siteTable$GeomorphicLType[grepl('haupiri',siteTable$CouncilSiteID,ignore.case = T)&siteTable$Agency=='wcrc'] <- 'Riverine'

siteTable$GeomorphicLType[grepl('opouahi',siteTable$CouncilSiteID,ignore.case=T)] <- 'Landslide'

FENZlake = read.csv("D:/RiverData/FENZLake.txt",stringsAsFactors = F)[,c(2,3,13,14,15,17,26,27,32,33,34,35,36,38)]
cbind(siteTable$Agency[siteTable$GeomorphicLType==""],
      siteTable$CouncilSiteID[siteTable$GeomorphicLType==""],
      FENZlake$GeomorphicType[match(siteTable$LFENZID[siteTable$GeomorphicLType==""],FENZlake$LID)])

siteTable$GeomorphicLType[siteTable$GeomorphicLType==""] <- 
  FENZlake$GeomorphicType[match(siteTable$LFENZID[siteTable$GeomorphicLType==""],FENZlake$LID)]

siteTable$GeomorphicLType[siteTable$Agency=='boprc'] <- "volcanic"  #email Lisa Naysmith 2/8/twentynineteen

siteTable$LType[which(siteTable$LType%in%c("polymictc","artificall"))] <- "polymictic"
siteTable$LType[which(siteTable$LType%in%c("monomictic"))] <- "stratified"
siteTable$LType=pseudo.titlecase(tolower(siteTable$LType))
siteTable$GeomorphicLType=pseudo.titlecase(tolower(siteTable$GeomorphicLType))

table(siteTable$Agency,siteTable$LType)
table(siteTable$Agency,siteTable$GeomorphicLType)

## Output for next script
write.csv(x = siteTable,file = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                                      format(Sys.Date(),'%Y-%m-%d'),
                                      "/SiteTable_Lakes",format(Sys.Date(),'%d%b%y'),".csv"),row.names=F)

AgencyRep=table(factor(tolower(siteTable$Agency),levels=c("arc", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                          "ncc", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
LakeWFSsiteFiles=dir(path = "H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                     pattern = 'SiteTable_Lakes',
                     recursive = T,full.names = T)
for(wsf in LakeWFSsiteFiles){
  stin=read.csv(wsf,stringsAsFactors=F)
  agencyRep=table(factor(tolower(stin$Agency),levels=c("arc", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                       "ncc", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
  AgencyRep = cbind(AgencyRep,as.numeric(agencyRep))
  colnames(AgencyRep)[dim(AgencyRep)[2]] = strTo(strFrom(wsf,'_Lakes'),'.csv')
  rm(agencyRep)
}
AgencyRep=AgencyRep[,-1]

rm(LakeWFSsiteFiles)
write.csv(AgencyRep,'h:/ericg/16666LAWA/LAWA2020/Metadata/AgencyRepLakeWFS.csv')

#           arc boprc ecan es gdc gwrc hbrc hrc mdc ncc nrc orc tdc trc wcrc wrc

#       14Apr20 23Jun20 25Jun20 03Jul20
# arc         4       5       5      5
# boprc      14      14      14     14
# ecan       43      43      43     43
# es         18      18      18     18
# gdc         0       0       0      0
# gwrc        5       5       5      5
# hbrc        7       7       7      7
# hrc        10      10      10     10
# mdc         0       0       0      0
# ncc         0       0       0      0
# nrc        26      26      26     26
# orc         9       9       9     14
# tdc         0       0       0      0
# trc         9       9       9      9
# wcrc        3       3       3      3
# wrc        12       0      12     12
# 
# 
# 
# 