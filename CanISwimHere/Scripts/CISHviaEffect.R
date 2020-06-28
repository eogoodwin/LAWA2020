rm(list=ls())
library(tidyverse)
source('H:/ericg/16666LAWA/LAWA2019/Scripts/LAWAFunctions.R')
try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)
try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)
ssm = read.csv('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/MetaData/SwimSiteMonitoringResults-2019-08-01.csv',stringsAsFactors = F)
WFSsiteTable = read.csv(tail(dir(path="H:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data",pattern='SiteTable',
                                 recursive=T,full.names=T,ignore.case=T),1),stringsAsFactors = F)
RegionTable=data.frame(ssm=unique(ssm$Region),
                       wfs=c("bay of plenty","canterbury","gisborne","hawkes bay",
                             "horizons","marlborough","nelson","northland",
                             "otago","southland","taranaki","tasman",
                             "waikato","wellington","west coast"))


# recData = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/2019-10-24/2019-10-24 - Swim Site Extract-Non-Auckland-BOP.xlsx')
#30939
# recData = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/2019-10-31/Swim Site Extract-Non-Auckland-31-10-2019.xlsx')
#35922
# recData = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/2019-11-07/Swim Site Extract-Non-Auckland-07-11-2019.xlsx')
#38482 of 11
# recData = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/2019-11-21/Swim Site Extract-Non-Auckland-15-11-2019_lessHRC duplicates.xlsx')
# recData <- recData%>%select(-Column1)
#38476 of 11
#recData = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/2019-11-29/Swim Site Extract-Non-Auckland-29-11-2019.xlsx')
#39178 of 11

#Then Abi "removed duplicates"
recData = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Data/2019-11-29/Swim Site Extract-Non-Auckland-29-11-2019.xlsx')
# 47630 of 11
mtRows = which(apply(recData,1,FUN=function(x)all(is.na(x))))
if(length(mtRows)>0){
  recData=recData[-mtRows,]
}
rm(mtRows)
#39064

recData%>%dplyr::group_by(Region)%>%dplyr::summarise(nSite=length(unique(SiteName)))

#Deal with censored data
#Anna Madarasz-Smith 1/10/2018
#Hey guys,
# We (HBRC) have typically used the value with the >reference 
# (eg >6400 becomes 6400), and half the detection of the less than values (e.g. <1 becomes 0.5).  I think realistically you can treat
# these any way as long as its consistent beccuase it shouldn’t matter to your 95th%ile.  These values should be in the highest and lowest of the range.
# Hope this helps Cheers
# Anna
# recData$fVal=recData$Value
# recData$fVal[recData$lCens]=recData$Value[recData$lCens]/2
# recData$fVal[recData$rCens]=recData$Value[recData$rCens]
# table(recData$lCens)
# table(recData$rCens)


#Make all yearweeks six characters
recData$jday = lubridate::yday(recData$ValueDate)
recData$week = lubridate::week(recData$ValueDate)
recData$week=as.character(recData$week)
recData$week[nchar(recData$week)==1]=paste0('0',recData$week[nchar(recData$week)==1])
recData$month = lubridate::month(recData$ValueDate)
recData$year = lubridate::year(recData$ValueDate)


recData$YW=as.numeric(paste0(recData$year,recData$week))

#Create bathing seasons
bs=strTo(recData$ValueDate,'-')
bs[recData$month>6]=paste0(recData$year[recData$month>6],'/',recData$year[recData$month>6]+1)
bs[recData$month<7]=paste0(recData$year[recData$month<7]-1,'/',recData$year[recData$month<7])
recData$bathingSeason=bs
table(recData$bathingSeason)


#Email AbiL 21/11/19
# Include monitoring values from 25 Oct - 31 Mar (the inclusion of late Oct is 
# to capture those councils that start monitoring in the last week Oct/first week Nov).
lubridate::yday('2018-10-25') #298
lubridate::yday('2018-03-31') #90

recData%>%
  # filter(YW>201525)%>%
  dplyr::filter(LawaId!='')%>%drop_na(LawaId)%>%
  dplyr::filter(ObservedProperty!='Cyanobacteria')%>%
  dplyr::filter(jday>=298 | jday<=90)%>%
#  dplyr::filter(year>2009&(month>9|month<5))%>% #bathign season months only
  # dplyr::group_by(LawaId,YW,ObservedProperty)%>%
  # dplyr::arrange(YW)%>%                   
  # dplyr::summarise(ValueDate=median(ValueDate),
  #                  Region=unique(Region),
  #                  n=length(Value),            #Count number of weeks recorded per sampling period
  #                  Value=median(Value),
  #                  bathingSeason=unique(bathingSeason))%>%
  ungroup->graphData

write.csv(graphData,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",
                                  format(Sys.Date(),'%Y-%m-%d'),
                                  "/CISHgraph",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)
##############################
#Graph data is NO LONGER temporally averaged
##############################
graphData%>%
  select(-ValueDate)%>%
  group_by(LawaId,ObservedProperty)%>%            #For each site
  dplyr::summarise(Region=unique(Region),nBS=length(unique(bathingSeason)),
                   nPbs=paste(as.numeric(table(bathingSeason)),collapse=','),
                   min=min(Value,na.rm=T),
                   max=max(Value,na.rm=T),
                   haz95=quantile(Value,probs = 0.95,type = 5,na.rm = T),
                   haz50=quantile(Value,probs = 0.5,type = 5,na.rm = T))%>%ungroup->CISHsiteSummary
#so the CISHsiteSummary is no longer temporally averaged
#instantaneous thresholds, shouldnt be applied to percentiles
# CISHsiteSummary$BathingRisk=cut(x = CISHsiteSummary$haz95,breaks = c(-0.1,140,280,Inf),labels=c('surveillance','warning','alert'))

#https://www.mfe.govt.nz/sites/default/files/microbiological-quality-jun03.pdf
#For enterococci, marine:                      E.coli, freshwater
#table D1 A is 95th %ile < 40       table E1     <= 130
#         B is 95th %ile 41-200                  131 - 260
#         C is 95th %ile 201-500                 261 - 550
#         D is 95th %ile >500                       >550
CISHsiteSummary$marineEnt=cut(x=CISHsiteSummary$haz95,breaks = c(-0.1,40,200,500,Inf),labels=c("A","B","C","D"))
CISHsiteSummary$marineEnt[CISHsiteSummary$ObservedProperty!='Enterococci'] <- NA
CISHsiteSummary$fwEcoli=cut(x=CISHsiteSummary$haz95,breaks = c(-0.1,130,260,550,Inf),labels=c("A","B","C","D"))
CISHsiteSummary$fwEcoli[CISHsiteSummary$ObservedProperty!='E-coli'] <- NA
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
CISHsiteSummary$LawaBand[CISHsiteSummary$ObservedProperty!='Enterococci'] <- 
  cut(x=CISHsiteSummary$haz95[CISHsiteSummary$ObservedProperty!='Enterococci'],
      breaks=c(-0.1,260,550,Inf),labels=c("A","B","C"))


#Email, Abi L 15/10/2019
#2)	Greater Wellington and minimum sample size
#   So as a starter for 10, can we propose alternative rule if the above isn’t met, that:
#   If in one year, there is less than 10 but at least 7 data points, 
#   then as long as the total number of data points over 3 years are at least 35, 
#   then these will be eligible for an overall bacto risk grade?  
#   This would ensure that the majority of sites in the spreadsheet attached would be eligible for a grade - 
#   but wanted to test with you that this is a statistically robust approach before we sign off on this.

nPbs=do.call(rbind,lapply(CISHsiteSummary$nPbs,FUN = function(x)lapply(strsplit(x,split = ','),as.numeric))) #get the per-season counts
tooFew = which(do.call('rbind',lapply(nPbs,function(x){
  any(x<10)|length(x)<3
})))  #see if any per-season counts are below ten, or there are fewer than three seasons
tooFewNew = which(do.call('rbind',lapply(nPbs,function(x){
  (any(x<10)&sum(x,na.rm=T)<35)|(length(x)<3)|any(x<7)
}))) 

CISHsiteSummary$Region[tooFew[!tooFew%in%tooFewNew]]
cbind(CISHsiteSummary$Region[tooFew[!tooFew%in%tooFewNew]],t(sapply((tooFew[!tooFew%in%tooFewNew]),FUN=function(x)nPbs[[x]])))



CISHsiteSummarySiteName = recData$SiteName[match(CISHsiteSummary$LawaId,recData$LawaId)]
CISHsiteSummarySiteName[tooFew[!tooFew%in%tooFewNew]]

CISHsiteSummary$LawaBand[tooFewNew]=NA #set those data-poor sites' grade to NA

write.csv(CISHsiteSummary,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",
                                        format(Sys.Date(),'%Y-%m-%d'),
                                        "/CISHsiteSummary",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)
# CISHsiteSummary=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",pattern="CISHsiteSummary",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)


#Export only the data-rich sites
CISHwellSampled=CISHsiteSummary%>%dplyr::filter(nBS>=3)
lrsY=do.call(rbind,str_split(string = CISHwellSampled$nPbs,pattern = ',')) #Repeats to fill rows, doesnt matter, as any<10 excludes
lrsY=apply(lrsY,2,as.numeric)
NElt10=which(apply(lrsY,1,FUN=function(x){
  (any(x<10)&sum(x,na.rm=T)<35)|any(x<7)
  }))
CISHwellSampled=CISHwellSampled[-NElt10,]
CISHwellSampled <- left_join(CISHwellSampled,recData%>%select(LawaId,Region,SiteType,SiteName)%>%distinct)
write.csv(CISHwellSampled,file = paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",
                                        format(Sys.Date(),'%Y-%m-%d'),
                                        "/CISHwellSampled",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)


# CISHsiteSummary$Region = recData$Region[match(CISHsiteSummary$LawaId,recData$LawaId)]
CISHsiteSummary$SiteName = recData$SiteName[match(CISHsiteSummary$LawaId,recData$LawaId)]
CISHsiteSummary$SiteType = recData$SiteType[match(CISHsiteSummary$LawaId,recData$LawaId)]

#Write individual Regional files: data from recData and scores from CISHsiteSummary 
uReg=unique(recData$Region)
for(reg in seq_along(uReg)){
  toExport=recData[recData$Region==uReg[reg],]
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recData_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 ".csv"),row.names = F)
  toExport=CISHsiteSummary%>%filter(Region==uReg[reg])%>%as.data.frame
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recScore_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 ".csv"),row.names = F)
}
rm(reg,uReg,toExport)

#Write export file for ITEffect to use for the website
recDataITE <- CISHsiteSummary%>%
  transmute(LAWAID=LawaId,
            Region=Region,
            Site=SiteName,
            Hazen=haz95,
            NumberOfPoints=unlist(lapply(str_split(nPbs,','),function(x)sum(as.numeric(x)))),
            DataMin=min,
            DataMax=max,
            RiskGrade=LawaBand,
            Module=SiteType)
recDataITE$Module[recDataITE$Module=="Site"] <- "River"
recDataITE$Module[recDataITE$Module=="LakeSite"] <- "Lake"
recDataITE$Module[recDataITE$Module=="Beach"] <- "Coastal"
write.csv(recDataITE,paste0("h:/ericg/16666LAWA/LAWA2019/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                            "/ITERecData",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)  
