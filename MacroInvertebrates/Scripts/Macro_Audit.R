rm(list=ls())
library(tidyverse)
library(lubridate)

source("H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R")
try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Audit/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)

macroSiteTable=loadLatestSiteTableMacro()
# extraSites=read.csv('H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/MetaData/ExtraMacrotable.csv',stringsAsFactors = F)
# extraSites=extraSites[!tolower(extraSites$CouncilSiteID)%in%tolower(macroSiteTable$CouncilSiteID),]
# macroSiteTable <- rbind(macroSiteTable,extraSites)
# rm(extraSites)

macroData=loadLatestDataMacro()


#Combined audit ####
macroAudit=data.frame(agency=NULL,var=NULL,earliest=NULL,latest=NULL,nMeas=NULL,nSite=NULL,meanMeas=NULL,maxMeas=NULL,minMeas=NULL,nNA=NULL)
for(agency in c("arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
  xmlAge= checkXMLageMacro(agency)
  if(is.na(xmlAge)){next}
  forcsv=loadLatestCSVmacro(agency,maxHistory = 20)
  if(length(forcsv)==0){next}
  siteTerm='SiteName'
  if(!siteTerm%in%names(forcsv)){
    siteTerm="CouncilSiteID"
  }
  if(agency=='arc'){
    forcsv$Date = format(lubridate::ymd(forcsv$Date),'%d-%b-%Y')
  }
  newRows=data.frame(agency=rep(agency,length(unique(forcsv$Measurement))),
                     xmlAge=rep(xmlAge,length(unique(forcsv$Measurement))),
                     var=sort(unique(forcsv$Measurement)),
                     earliest=rep("",length(unique(forcsv$Measurement))),
                     latest=rep("",length(unique(forcsv$Measurement))),
                     nMeas=rep(0,length(unique(forcsv$Measurement))),
                     nSite=rep(NA,length(unique(forcsv$Measurement))),
                     meanMeas=rep(NA,length(unique(forcsv$Measurement))),
                     maxMeas=rep(NA,length(unique(forcsv$Measurement))),
                     minMeas=rep(NA,length(unique(forcsv$Measurement))),
                     nNA=rep(NA,length(unique(forcsv$Measurement))),
                     stringsAsFactors = F)
  if(!is.null(forcsv)){
    for(v in 1:dim(newRows)[1]){
      newRows$earliest[v]=format(min(dmy(forcsv$Date[which(forcsv$Measurement==newRows$var[v])])),"%d-%b-%Y")
      newRows$latest[v]=format(max(dmy(forcsv$Date[which(forcsv$Measurement==newRows$var[v])])),"%d-%b-%Y")
      newRows$nMeas[v]=sum(forcsv$Measurement==newRows$var[v])
      newRows$nSite[v]=length(unique(forcsv[,siteTerm][which(forcsv$Measurement==newRows$var[v] & !is.na(forcsv$Value))]))
      newRows$meanMeas[v]=round(mean(forcsv$Value[forcsv$Measurement==newRows$var[v]],na.rm=T),1)
      newRows$maxMeas[v]=round(max(forcsv$Value[forcsv$Measurement==newRows$var[v]],na.rm=T),1)
      newRows$minMeas[v]=round(min(forcsv$Value[forcsv$Measurement==newRows$var[v]],na.rm=T),1)
      newRows$nNA[v]=sum(is.na(forcsv$Value[forcsv$Measurement==newRows$var[v]]))
    }
  }
  macroAudit <- rbind.data.frame(macroAudit,newRows)
}
write.csv(macroAudit,paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Audit/",format(Sys.Date(),'%Y-%m-%d'),"/macroAudit.csv"))

macroAudit%>%dplyr::group_by(agency)%>%dplyr::summarise(xmlAge=mean(xmlAge,na.rm=T),
                                                        endDate=max(lubridate::dmy(latest),na.rm=T))%>%knitr::kable(format='rst')

# lawaIDs=read.csv("H:/ericg/16666LAWA/LAWA2020/WaterQuality/R/lawa_state/2018_csv_config_files/LAWAMasterSiteListasatMarch2018.csv",stringsAsFactors = F)


table(unique(tolower(macroData$CouncilSiteID))%in%tolower(c(macroSiteTable$SiteID,macroSiteTable$CouncilSiteID)))


#Per agency audit site/measurement start, stop, n and range ####
for(agency in c("arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
  forcsv=loadLatestCSVmacro(agency,maxHistory = 20)
  if(!is.null(forcsv)){
    if('parameter'%in%names(forcsv)){
      names(forcsv)[which(names(forcsv)=='parameter')] <- 'Measurement'
    }
    
    if(agency=='arc'){
      forcsv$Date = format(lubridate::ymd(forcsv$Date),'%d-%b-%Y')
    }
    
    nvar=length(uvars <- unique(forcsv$Measurement))
    siteTerm="SiteName"
    nsite=length(usites <- unique(forcsv$SiteName))
    if(nsite==0){
      nsite=length(usites <- unique(forcsv$CouncilSiteID))
      siteTerm='CouncilSiteID'
    }
    agencyDeets=as.data.frame(matrix(nrow=nvar*nsite,ncol=7))
    names(agencyDeets)=c("Site","Var","StartDate","EndDate","nMeas","MinVal","MaxVal")
    r=1
    for(ns in 1:nsite){
      for(nv in 1:nvar){
        these=which(forcsv[,siteTerm]==usites[ns]&forcsv$Measurement==uvars[nv])
        agencyDeets[r,]=c(usites[ns],uvars[nv],
                          as.character(min(dmy(forcsv$Date[these]))),
                          as.character(max(dmy(forcsv$Date[these]))),
                          length(these),
                          min(forcsv$Value[these],na.rm=T),max(forcsv$Value[these],na.rm=T))
        r=r+1
      }
    }
    write.csv(agencyDeets,paste0( 'H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Audit/',format(Sys.Date(),"%Y-%m-%d"),'/',agency,'Audit.csv'),row.names=F)
  }
}

macroData$Measurement[macroData$Measurement=="% EPT Richness"] <- 'pcEPTRichness'

#Audit plots to allow comparison between agencies - check units consistency etc  ####
upara=unique(macroData$Measurement)
ucounc=c("arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")
for(up in seq_along(upara)){
  png(filename = paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Audit/",format(Sys.Date(),"%Y-%m-%d"),"/",
                        upara[up],format(Sys.Date(),'%d%b%y'),".png"),
      width = 12,height=9,units='in',res=300,type='cairo')
  pvals=macroData[macroData$Measurement==upara[up],]
  p1=quantile(pvals$Value,p=0.01,na.rm=T)
  p5=quantile(pvals$Value,p=0.05,na.rm=T)
  p75=quantile(pvals$Value,p=0.75,na.rm=T)
  p95=quantile(pvals$Value,p=0.95,na.rm=T)
  p999=quantile(pvals$Value,p=0.999,na.rm=T)
  pvals=pvals[pvals$Value<p999,]
  par(mfrow=c(4,4),mar=c(3,1,2,1))
  for(cc in seq_along(ucounc)){
    cvals=pvals$Value[pvals$Agency==ucounc[cc]]
    if(length(cvals[!is.na(cvals)])>2){
      plot(density(cvals,na.rm=T,from = p1,to=p95),
           main=paste(ucounc[cc],upara[up]),xlab='',xlim=c(p1,p95),log='')
    }else{
      plot(0,0,main=paste(ucounc[cc],upara[up]))
    }
  }
  if(names(dev.cur())=='png'){dev.off()}
}



startTime=Sys.time()
urls          <- read.csv("H:/ericg/16666LAWA/LAWA2020/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)
workers <- makeCluster(7)
registerDoParallel(workers)
foreach(agency = c("arc", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc", "hrc", 
                   "mdc", "ncc", "nrc", "orc", "tdc", "trc", "wcrc", "wrc"),
        .combine=rbind,.errorhandling="stop")%dopar%{
          if(length(dir(path = paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Audit/",format(Sys.Date(),"%Y-%m-%d")),
                        pattern = paste0('^',agency,".*audit\\.csv"),
                        recursive = T,full.names = T,ignore.case = T))>0){
            rmarkdown::render('H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/AuditDocument.Rmd',
                              params=list(agency=agency,
                                          sos=urls$SOSwq[which(tolower(urls$Agency)==agency)]),
                              output_dir = paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Audit/",format(Sys.Date(),"%Y-%m-%d")),
                              output_file = paste0(toupper(agency),"Audit",format(Sys.Date(),'%d%b%y'),".html"),
                              envir = new.env())
          }
          return(NULL)
        }
stopCluster(workers)
remove(workers)
Sys.time()-startTime


# macroTrend%>%dplyr::filter(period==10)%>%select(frequency,Agency)%>%table
# macroTrend%>%dplyr::filter(period==15)%>%select(frequency,Agency)%>%table
