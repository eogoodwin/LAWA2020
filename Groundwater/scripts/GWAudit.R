rm(list=ls())
library(XML)
library(tidyverse)
library(parallel)
library(doParallel)
source('H:/ericg/16666LAWA/LAWA2019/Scripts/LAWAFunctions.R')
suppressWarnings(try(dir.create(paste0("H:/ericg/16666LAWA/LAWA2019/GW/Audit/",format(Sys.Date(),"%Y-%m-%d")))))
StartYear5 <- lubridate::isoyear(Sys.Date())-5  #2014
EndYear <- 2018#lubridate::isoyear(Sys.Date())-1    #2018
periodYrs=5

GWdata=read.csv('h:/ericg/16666LAWA/LAWA2019/GW/Data/GWdata.csv',stringsAsFactors = F)
GWdata$Agency=factor(GWdata$Source,labels=c('ac','boprc','ecan','gdc','hbrc','hrc','mdc',
                                            'nrc','orc','es','trc','tdc','wrc','gwrc','wcrc'))

#Per agency audit site/measurement start, stop, n and range ####
GWdata=read.csv('h:/ericg/16666LAWA/LAWA2019/GW/Data/GWdata.csv',stringsAsFactors = F)
GWdata$Agency=factor(GWdata$Source,labels=c('ac','boprc','ecan','gdc','hbrc','hrc','mdc',
                                            'nrc','orc','es','trc','tdc','wrc','gwrc','wcrc'))
workers <- makeCluster(4)
registerDoParallel(workers)
clusterCall(cl = workers,fun = function(){
    library(tidyverse)
  library(lubridate)
})
# for(agency in c("ac", "boprc", "ecan", "gdc", "hbrc", "hrc", "mdc","nrc", "orc", "es", "trc", "tdc", "wrc", "gwrc", "wcrc")){
foreach(agency = c("ac", "boprc", "ecan", "gdc", "hbrc", "hrc", "mdc",
                   "nrc", "orc", "es", "trc", "tdc", "wrc", "gwrc", "wcrc"),
        .combine=rbind,.errorhandling="stop")%dopar%{
          agencyGWdata=GWdata%>%filter(Agency==agency)%>%
            dplyr::filter(lubridate::year(myDate)>=(EndYear-periodYrs+1)&lubridate::year(myDate)<=EndYear)
          if(!is.null(agencyGWdata)&&dim(agencyGWdata)[1]>0){
            nvar=length(uvars <- unique(agencyGWdata$Measurement))
            nsite=length(usites <- unique(agencyGWdata$LawaSiteID))
            siteTerm="LawaSiteID"
            agencyDeets=as.data.frame(matrix(nrow=nvar*nsite,ncol=8))
            names(agencyDeets)=c("Site",'Bore',"Var","StartDate","EndDate","nMeas","MinVal","MaxVal") #'Aquifer',
            r=1
            for(ns in 1:nsite){
              boreID=GWdata$Bore_ID[match(tolower(usites[ns]),tolower(GWdata$LawaSiteID))]
              if(is.null(boreID)||is.na(boreID)){
                boreID=GWdata$Site_ID[match(tolower(usites[ns]),tolower(GWdata$LawaSiteID))]
              }
              # aquiferID=GWdata$Aquifer_Object_ID[match(tolower(usites[ns]),tolower(GWdata$LawaSiteID))]
              # if(is.null(aquiferID)||is.na(aquiferID)){
              #   aquiferID=GWdata$Aquifer_Name[match(tolower(usites[ns]),tolower(GWdata$LawaSiteID))]
              # }
              # if(is.null(aquiferID)||is.na(aquiferID)){
              #   aquiferID = 'NA'
              # }
              for(nv in 1:nvar){
                these=which(agencyGWdata[,siteTerm]==usites[ns]&agencyGWdata$Measurement==uvars[nv])
                agencyDeets[r,]=c(usites[ns],                               #Site
                                  boreID,                                   #Bore
                                  # aquiferID,                                #Aquifer
                                  uvars[nv],                                #Var
                                  as.character(min(ymd(agencyGWdata$myDate[these]))),  #StartDate
                                  as.character(max(ymd(agencyGWdata$myDate[these]))),  #EndDate
                                  length(these),                            #nMeas
                                  min(agencyGWdata$Value[these],na.rm=T),            #minval
                                  max(agencyGWdata$Value[these],na.rm=T))            #maxval
                r=r+1
              }
            }
            write.csv(agencyDeets,paste0("h:/ericg/16666LAWA/LAWA2019/GW/Audit/",format(Sys.Date(),"%Y-%m-%d"),
                                         "/",agency,format(Sys.Date(),'%d%b%y'),"audit.csv"),row.names = F)
          }
          rm(agencyGWdata)
          return(NULL)
        }
stopCluster(workers)
remove(workers)



#Audit plots to allow comparison between agencies - check units consistency etc  ####
wqd=doBy::summaryBy(data=GWdata,
                    formula=Value~LawaSiteID+Measurement+myDate,
                    id=~Agency,
                    FUN=median)
wqds=spread(wqd,Measurement,Value.median)

params=unique(GWdata$Measurement)
for(param in 1:length(params)){
  png(filename = paste0("h:/ericg/16666LAWA/LAWA2019/GW/Audit/",format(Sys.Date(),"%Y-%m-%d"),"/",
                        gsub(pattern = '\\.',replacement='',make.names(names(wqds)[param+3])),format(Sys.Date(),'%d%b%y'),".png"),
       width = 12,height=9,units='in',res=300,type='cairo')
    plot(as.factor(wqds$Agency[wqds[,param+3]>0]),wqds[wqds[,param+3]>0,param+3],
         ylab=names(wqds)[param+3],main=names(wqds)[param+3],log='y')
  if(names(dev.cur())=='png'){dev.off()}
}



#And the ubercool html summary audit report doncuments per council!
workers <- makeCluster(5)
registerDoParallel(workers)
foreach(agency = c("ac", "boprc", "ecan", "gdc", "hbrc", "hrc", "mdc",
                   "nrc", "orc", "es", "trc", "tdc", "wrc", "gwrc", "wcrc"),
        .combine=rbind,.errorhandling="stop")%dopar%{
          if(length(dir(path = paste0("H:/ericg/16666LAWA/LAWA2019/GW/Audit/",format(Sys.Date(),"%Y-%m-%d")),
                pattern = paste0(agency,".*audit\\.csv"),
                recursive = T,full.names = T,ignore.case = T))>0){
            rmarkdown::render('H:/ericg/16666LAWA/LAWA2019/GW/Scripts/GWAuditDocument.Rmd',
                              params=list(agency=agency),
                              intermediates_dir=paste0("H:/ericg/16666LAWA/LAWA2019/GW/Audit/",format(Sys.Date(),"%Y-%m-%d"),'/',agency),
                              output_dir = paste0("H:/ericg/16666LAWA/LAWA2019/GW/Audit/",format(Sys.Date(),"%Y-%m-%d")),
                              output_file = paste0(toupper(agency),"Audit",format(Sys.Date(),'%d%b%y'),".html"),
                              envir = new.env())
          }
  return(NULL)
}
stopCluster(workers)
remove(workers)

# Audit the presence of measurements between config files and transfer table file, to allow the transfer table
# to be used as the source of names to call variables for from
# transfers=read.table("h:/ericg/16666LAWA/LAWA2019/GW/Metadata/transfers_plain_english_view.txt",
#                      sep=',',header = T,stringsAsFactors = F)
# for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
#   df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2019/GW/MetaData/",agency,"SWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
#   ta <- transfers%>%filter(Agency==agency)  
#   if(any(!tolower(subset(df,df$Type=="Measurement")[,2])%in%tolower(ta$CallName))){
#     cat(agency,'\n')
#     print(table(tolower(subset(df,df$Type=="Measurement")[,2])%in%tolower(ta$CallName)))
#     print(subset(df,df$Type=="Measurement")[!tolower(subset(df,df$Type=="Measurement")[,2])%in%tolower(ta$CallName),2])
#     cat('\n')
#   }
#   if(any(!tolower(ta$CallName)%in%tolower(subset(df,df$Type=="Measurement")[,2]))){
#     cat(agency,'\n')
#     print(ta$CallName[!tolower(ta$CallName)%in%tolower(subset(df,df$Type=="Measurement")[,2])])
#     cat('\n')
#   }
# }
# 


#Look at total values per measurement 


