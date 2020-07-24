rm(list=ls())
# library(XML)
library(lubridate)
source("H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R")
dir.create(path = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Audit/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)
EndYear <- lubridate::isoyear(Sys.Date())-1
StartYear10 <- EndYear-9
StartYear5 <- EndYear-4

urls <- read.csv("H:/ericg/16666LAWA/LAWA2020/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)
siteTable=loadLatestSiteTableLakes(maxHistory=90)
LWQdata=read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2020/Lakes',pattern='LakesCombined.csv',recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)
LWQdata$NewValues=LWQdata$Value
if(mean(LWQdata$Value[which(LWQdata$Measurement%in%c('NH4N','TN','TP'))],na.rm=T)<250){
  LWQdata$Value[which(LWQdata$Measurement%in%c('NH4N','TN','TP'))]=LWQdata$Value[which(LWQdata$Measurement%in%c('NH4N','TN','TP'))]*1000
}

#Let's can we find out, what servers were used by which councils
urls$Agency[grep(pattern = 'SOS',x = urls$SOSwq)]     #AC, BOPRC, HRC, WRC
urls$Agency[grep(pattern = 'Hilltop',x = urls$SOSwq)] # "ECAN" "ES"   "GDC"  "GWRC" "HBRC" "MDC"  "NCC"  "xNRC" "NRC"  "ORC"  "TDC"  "TRC"  "WCRC"

urls$SOSwq[urls$Agency%in%c("AC","BOPRC","HRC","WRC")]
urls$SOSwq[urls$Agency%in%c("ES","ECAN","ORC","NRC","HBRC","TRC")] #These have the column label info comign out nicely
urls$SOSwq[urls$Agency%in%c("GDC","GWRC","MDC","NCC","WCRC")]      #These use hilltop, but dont have column labels coming out.
                                                                  #But some of them dont have data either

#WCRC, WRC and BOPRC currently (30/6/2020) have data but no column label info
# WCRC uses the Hilltop, so why is that one not workling?    WCRC has only "E"s in its Measurement$Data, which have only times and values
# WRC and ARC are in the habit of using KiWIS; HRC are indeed using hilltop, and BOP is an amazon based 52North

#         Hilltop  KiWIS 52N  XML  Data  ColLabs
"ac                 X              X           ?       "   #Have provided a format to get the QC codes from KiWIS Kisters
"boprc                    X    X    X           ?       "   #Format matches same as HRC       MeasurementTVPs
"ecan       X                  X    X        X          "
"es         X                  X    X        X          "
"gdc        X                                           "
"gwrc       X                  O    X           ?       "   #Seem to be having today a no-data situation
"hbrc       X                  X    X        X          "
"hrc        X                  O    X           ?       "   #jeez just a completely different format, like the KiWIS.   MeasurementTVPs
"mdc        X                                           "
"ncc        X                                           "
"nrc        X                  X    X        X          "
"orc        X                  X    X        X          "
"tdc        X                                           "
"trc        X                  X    X        X          "
"wcrc       X                  X    X     only 'e's     "   #Not much metadata published. 
"wrc                X          X    X           ?       "   #Format same as matches HRC       MeasurementTVPs



if(lubridate::year(Sys.Date())==2019){
  # Lake Omapere in northland was being measured for chlorophyll in the wrong units.  It's values need converting.
  these = which(LWQdata$CouncilSiteID==100501 & LWQdata$Measurement=='CHLA')
  if(mean(LWQdata$Value[these],na.rm=T)<1){
    LWQdata$Value[these] = LWQdata$Value[these]*1000
  }
  rm(these)
}


lawalakenames=c("TN", "NH4N", "TP", "CHLA", "pH", "Secchi", "ECOLI")

#Combined audit ####
LWQaudit=data.frame(agency=NULL,xmlAge=NULL,var=NULL,earliest=NULL,latest=NULL,nMeas=NULL,nSite=NULL,meanMeas=NULL,maxMeas=NULL,minMeas=NULL,nNA=NULL)
for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
  forcsv=loadLatestCSVLake(agency,maxHistory = 100)
  if(!is.null(forcsv)){
    newRows=data.frame(agency=rep(agency,length(unique(forcsv$Measurement))),
                       xmlAge=checkXMLageLakes(agency = agency,maxHistory = 30),
                       var=sort(unique(forcsv$Measurement)),
                       earliest=rep("",length(unique(forcsv$Measurement))),
                       latest=rep("",length(unique(forcsv$Measurement))),
                       nMeas=rep(0,length(unique(forcsv$Measurement))),
                       nMeas5=rep(0,length(unique(forcsv$Measurement))),
                       nSite=rep(NA,length(unique(forcsv$Measurement))),
                       meanMeas=rep(NA,length(unique(forcsv$Measurement))),
                       maxMeas=rep(NA,length(unique(forcsv$Measurement))),
                       minMeas=rep(NA,length(unique(forcsv$Measurement))),
                       nNA=rep(NA,length(unique(forcsv$Measurement))),
                       stringsAsFactors = F)
    for(v in 1:dim(newRows)[1]){
      these=which(forcsv$Measurement==newRows$var[v] &!is.na(forcsv$Value))
      these5=which(forcsv$Measurement==newRows$var[v]&lubridate::isoyear(lubridate::dmy(forcsv$Date))>StartYear5&!is.na(forcsv$Value))
      newRows$earliest[v]=format(min(dmy(forcsv$Date[these]),na.rm=T),"%d-%b-%Y")
      newRows$latest[v]=format(max(dmy(forcsv$Date[these]),na.rm=T),"%d-%b-%Y")
      newRows$nMeas[v]=length(these)
      newRows$nMeas5[v]=length(these5)
      newRows$nSite[v]=length(unique(forcsv$CouncilSiteID[these]))
      newRows$meanMeas[v]=round(mean(forcsv$Value[these],na.rm=T),1)
      newRows$maxMeas[v]=round(max(forcsv$Value[these],na.rm=T),1)
      newRows$minMeas[v]=round(min(forcsv$Value[these],na.rm=T),1)
      newRows$nNA[v]=sum(is.na(forcsv$Value[forcsv$Measurement==newRows$var[v]]))
    }
    LWQaudit <- rbind.data.frame(LWQaudit,newRows)
  }
}
by(INDICES = LWQaudit$var,data = LWQaudit$agency,FUN=function(x)unique(as.character(x)))
by(INDICES = LWQaudit$agency,data = LWQaudit$var,FUN=function(x)unique(as.character(x)))

LWQaudit%>%dplyr::group_by(agency)%>%dplyr::summarise(xmlAge=mean(xmlAge,na.rm=T),
                                                      endDate=max(lubridate::dmy(latest),na.rm=T))%>%knitr::kable(format='rst')

write.csv(LWQaudit,paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Audit/",format(Sys.Date(),"%Y-%m-%d"),"/LWQaudit.csv"))

if(0){
allLabs=NULL
for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
  colLab = loadLatestColumnHeadingLake(agency,maxHistory = 100)
  if(!is.null(colLab)){
    eval(parse(text=paste0('colLab',agency,'=colLab')))
    allLabs = sort(unique(c(allLabs,colLab[,2])))
  }
  rm(colLab)
}
}

#Per agency audit site/measurement start, stop, n and range ####
for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
  forcsv=loadLatestCSVLake(agency,maxHistory = 6)
  if(!is.null(forcsv)){
    nvar=length(uvars <- unique(forcsv$Measurement))
    nsite=length(usites <- unique(forcsv$CouncilSiteID))
    agencyDeets=as.data.frame(matrix(nrow=nvar*nsite,ncol=8))
    names(agencyDeets)=c("Site","Var","StartDate","EndDate","nMeas","nMeas5","MinVal","MaxVal")
    r=1
    for(ns in 1:nsite){
      for(nv in 1:nvar){
        these=which(forcsv$CouncilSiteID==usites[ns]&forcsv$Measurement==uvars[nv])
        these5=which(forcsv$CouncilSiteID==usites[ns]&forcsv$Measurement==uvars[nv]&lubridate::isoyear(lubridate::dmy(forcsv$Date))>StartYear5)
        agencyDeets[r,]=c(usites[ns],uvars[nv],
                           as.character(min(dmy(forcsv$Date[these]),na.rm=T)),
                           as.character(max(dmy(forcsv$Date[these]),na.rm=T)),
                           length(these),
                          length(these5),
                           min(forcsv$Value[these],na.rm=T),max(forcsv$Value[these],na.rm=T))
        r=r+1
      }
    }
    write.csv(agencyDeets,paste0( 'H:/ericg/16666LAWA/LAWA2020/Lakes/Audit/',format(Sys.Date(),"%Y-%m-%d"),
                                  '/',agency,'Audit.csv'),row.names=F)
  }
}



#Audit plots to allow comparison between agencies - check units consistency etc  ####
upara=unique(LWQdata$Measurement)
ucounc=unique(LWQdata$agency)
 for(up in seq_along(upara)){
   png(filename = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Audit/",format(Sys.Date(),"%Y-%m-%d"),"/",
                         upara[up],format(Sys.Date(),'%d%b%y'),".png"),
       width = 12,height=9,units='in',res=300,type='cairo')
  pvals=LWQdata[LWQdata$Measurement==upara[up],]
  p1=quantile(pvals$Value,p=0.01,na.rm=T)
  p5=quantile(pvals$Value,p=0.05,na.rm=T)
  p75=quantile(pvals$Value,p=0.75,na.rm=T)
  p95=quantile(pvals$Value,p=0.95,na.rm=T)
  p999=quantile(pvals$Value,p=0.999,na.rm=T)
  pvals=pvals[pvals$Value<p999,]
  if(tolower(upara[up])%in%c("secchi","ph")){
    plot(factor(pvals$agency),pvals$Value,las=2,ylab=upara[up],main=upara[up])
    }else{
    plot(factor(pvals$agency),log10(pvals$Value),las=2,ylab=upara[up],main=upara[up])
    }
  
  # par(mfrow=c(4,3),mar=c(3,1,2,1))
  # for(cc in seq_along(ucounc)){
  #   cvals=pvals$Value[pvals$agency==ucounc[cc]]
  #   if(length(cvals[!is.na(cvals)])>2){
  #     plot(density(cvals,na.rm=T,from = p1,to=p95),
  #          main=paste(ucounc[cc],upara[up]),xlab='',xlim=c(p1,p95),log=ifelse(upara[up]%in%c("Secchi","pH"),'','x'))
  #   }else{
  #     plot(0,0)
  #   }
  #  }
  if(names(dev.cur())=='png'){dev.off()}
 }



urls <- read.csv("H:/ericg/16666LAWA/LAWA2020/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)
# library(parallel)
# library(doParallel)
# workers <- makeCluster(7)
# registerDoParallel(workers)
# foreach(agency = c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc"),
#         .combine=rbind,.errorhandling='stop')%dopar%{
for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
  if(length(dir(path = paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Audit/",format(Sys.Date(),"%Y-%m-%d")),
                        pattern = paste0('^',agency,".*audit\\.csv"),
                        recursive = T,full.names = T,ignore.case = T))>0){
            rmarkdown::render('H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/AuditDocument.Rmd',
                              params=list(agency=agency,
                                          sos=urls$SOSwq[which(tolower(urls$Agency)==agency)]),
                              output_dir = paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Audit/",format(Sys.Date(),"%Y-%m-%d")),
                              output_file = paste0(toupper(agency),"Audit",format(Sys.Date(),'%d%b%y'),".html"),
                              envir = new.env())
          }
          # return(NULL)
        }
# stopCluster(workers)
# rm(workers)



lwqtrend=read_csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis",
                           pattern="LakesWQ_Trend*.*",
                           full.names=T,recursive=T,ignore.case=T),1))
lwqtrend$frequency[grep('unassessed',lwqtrend$frequency,ignore.case=T)]<-'unassessed'
lwqtrend%>%dplyr::filter(period==5)%>%select(frequency,Agency)%>%table
lwqtrend%>%dplyr::filter(period==10)%>%select(frequency,Agency)%>%table
lwqtrend%>%dplyr::filter(period==15)%>%select(frequency,Agency)%>%table
