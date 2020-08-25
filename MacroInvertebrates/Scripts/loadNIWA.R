#load NIWA
rm(list=ls())
library(tidyverse)
source("H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R")
#NIWA data has lats and longs which we can match to the lawa ids,
# to get a lawasiteID assigned to it.  Most of those lawasiteIDs should be in the siteTable
# But who cares anyway, just keep the lawaIDs trackign across to the end. 

macroSiteTable=loadLatestSiteTableMacro()
# macroSiteTable$CouncilSiteIDlc=tolower(macroSiteTable$CouncilSiteID)
extraMacroTable=macroSiteTable#[macroSiteTable$CouncilSiteIDlc%in%NIWAMCIl$SiteNamelc,]
rm(macroSiteTable)
extraMacroTable=rbind(extraMacroTable,
                      data.frame(CouncilSiteID="SQ10067",SiteID="Hakataramea River u/s SH82 bridge",LawaSiteID="ECAN-10004",
                                 NZReach=NA,Region='canterbury',Agency='niwa',
                                 SWQAltitude="Upland",SWQLanduse='Rural',Catchment=NA,Lat=-44.72610032651481,
                                                 Long=170.4903439007408,accessDate='19-Sep-2018',
                                 Landcover=NA,Altitude=NA,Order=NA,StreamOrder=NA,AltitudeCl=NA))
extraMacroTable=rbind(extraMacroTable,
                      data.frame(CouncilSiteID="103248",SiteID="Waipapa @ Forest Ranger",LawaSiteID="NRC-10008",
                                 NZReach=NA,Region="northland",Agency='niwa',
                                 SWQAltitude="",SWQLanduse='',Catchment=NA,Lat=-35.2763146207668,
                                                 Long= 173.684049395477,accessDate='19-Sep-2018',
                                 Landcover=NA,Altitude=NA,Order=NA,StreamOrder=NA,AltitudeCl=NA))
extraMacroTable$Macro=F
extraMacroTable <- extraMacroTable%>%select("SiteID","CouncilSiteID","LawaSiteID","Macro","Region","Agency","SWQLanduse","SWQAltitude","Lat","Long")
write.csv(extraMacroTable,file='h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/ExtraMacrotable.csv',row.names=F)






source('k:/R_functions/nzmg2WGS.r')
NIWAmacroSites=readxl::read_xlsx('h:/ericg/16666LAWA/2018/MacroInvertebrates/1.Imported/NIWAInvertebrate.xlsx',sheet = 'site metadata')
lawaIDs=read.csv("H:/ericg/16666LAWA/LAWA2019/Metadata/LAWAMasterSiteListasatMarch2018.csv",stringsAsFactors = F)
lawaIDs <- lawaIDs%>%dplyr::filter(Module=="Freshwater Quality")
lawaIDs$Long=as.numeric(lawaIDs$Longitude)
lawaIDs$Lat=as.numeric(lawaIDs$Latitude)

latlon=nzmg2wgs(East =  NIWAmacroSites$nzmge,North =  NIWAmacroSites$nzmgn)
NIWAmacroSites$Long=latlon[,2]
NIWAmacroSites$Lat=latlon[,1]
rm(latlon)

minDists=rep(0,dim(NIWAmacroSites)[1])
NIWAmacroSites$LawaSiteID=NA
NIWAmacroSites$lidname=NA
# NIWAmacroSites$Region=NA
for(nms in 1:(dim(NIWAmacroSites)[1])){
  dists=sqrt((NIWAmacroSites$Long[nms]-lawaIDs$Long)^2+(NIWAmacroSites$Lat[nms]-lawaIDs$Lat)^2)
  minDists[nms]=min(dists,na.rm=T)
  NIWAmacroSites$LawaSiteID[nms]=lawaIDs$LawaID[which.min(dists)]
  NIWAmacroSites$lidname[nms]=lawaIDs$SiteName[which.min(dists)]
}
NIWAmacroSites$dist=minDists

# NIWAmacroSites[,c(8,15)]%>%as.data.frame

#2020 delivery from RickStoffels
niwamacroraw=read.csv('./Data/NIWAMacros.csv',stringsAsFactors = F,skip=2)%>%dplyr::rename(Year=Group)
niwamacromet=read_lines(file = './Data/NIWAMacros.csv',skip=0,n_max = 2)%>%str_split(',')%>%unlist%>%matrix(nrow=2,byrow = T)
niwamacromet=niwamacromet[,-c(1,2,3,117)]%>%t

niwaEPTcount = niwamacroraw[,grepl("Ephemeroptera|Plecoptera|Trichoptera",names(niwamacroraw),T)]
niwaEPTcount =apply(niwaEPTcount,1,FUN=function(x)sum(x>0,na.rm=T))
niwaTAXONrich=niwamacroraw[,-c(1,2,3,117)]
niwaTAXONrich = 
  apply(X = niwaTAXONrich,MARGIN = 1,FUN=function(x)length(which(as.numeric(x)>0)))
niwaPCEPT=(niwaEPTcount/niwaTAXONrich)*100
                 
niwaMCIpa = niwamacroraw[,-c(1,2,3,117)]  #1823 rows of 113 
niwaMCIpa = apply(niwaMCIpa,1,FUN=function(x)as.numeric(as.logical(as.numeric(x))))%>%t  #1823 of 113
niwaMCIsc = apply(niwaMCIpa,MARGIN = 1,FUN=function(x)x*as.numeric(niwamacromet[,2]))%>%t
niwaMCI = apply(niwaMCIsc,MARGIN = 1,FUN=function(x)mean(x[x>0],na.rm=T))*20
#use like pairs(indices_dat%>%select(N,RI_AMBI,S,TN),lower.panel=panel.smooth,upper.panel=panel.cor)
# pairs(cbind(niwaMCI,niwaPCEPT,niwaTAXONrich),lower.panel=panel.smooth,upper.panel=panel.cor)


niwaMacro=data.frame(Date=niwamacroraw$Date,Site.code=trimws(niwamacroraw$Site.code),Year=niwamacroraw$Year,MCI=niwaMCI,pcEPT=niwaPCEPT,taxonRich=niwaTAXONrich)
rm(niwamacromet,niwaEPTcount,niwaMCIpa,niwaMCIsc,niwamacroraw)

niwaMacro$Site.code[niwaMacro$Site.code%in%c("DNI","ND1")] <- "DN1"
niwaMacro$Site.code[niwaMacro$Site.code%in%c("ND2")] <- "DN2"
NIWAmacroSites$Site.code = strFrom(s = NIWAmacroSites$nemarid,c='-')
NIWAmacroSites$Site.code = gsub('([[:alpha:]])0','\\1',NIWAmacroSites$Site.code)

table(unique(niwaMacro$Site.code)%in%NIWAmacroSites$Site.code)
unique(niwaMacro$Site.code)[!unique(niwaMacro$Site.code)%in%NIWAmacroSites$Site.code]

niwaMacro <- merge(niwaMacro,NIWAmacroSites,by="Site.code")
niwaMacro$Agency='niwa'
niwaMacro <- niwaMacro%>%dplyr::rename(CouncilSiteID=lawaid,PercentageEPTTaxa=pcEPT,TaxaRichness=taxonRich)
niwaMacrol = niwaMacro%>%pivot_longer(cols=c("MCI",'PercentageEPTTaxa',"TaxaRichness"),names_to="Measurement",values_to="Value")
niwaMacrol$Date=as.character(niwaMacrol$Date)
niwaMacrol$Date[niwaMacrol$Date=='13/82018']="13/8/2018"
niwaMacrol$Date = format(lubridate::dmy(niwaMacrol$Date),'%d-%b-%Y')
niwaMacrol$Value=as.numeric(niwaMacrol$Value)

niwaMacrol <- niwaMacrol%>%drop_na(Date)

write.csv(niwaMacrol%>%select(LawaSiteID,CouncilSiteID,Date,Measurement,Value,Agency),
          file=paste0( 'H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),'/','NIWA.csv'),
          row.names=F)


if(0){
  NIWAMCI=readxl::read_xlsx('h:/ericg/16666LAWA/2018/MacroInvertebrates/1.Imported/NIWAInvertebrate.xlsx',sheet = 'MCI',na = 'NA')
  NIWAMCI$lawaid=gsub(x = NIWAMCI$lawaid,pattern = '-ND',replacement = '-DN')
  NIWAMCI$LawaSiteID = tolower(NIWAmacroSites$LawaSiteID[match(NIWAMCI$lawaid,NIWAmacroSites$lawaid)])
  names(NIWAMCI)[7]="TaxaRichness"
  
  NIWAEPT = readxl::read_xlsx('h:/ericg/16666LAWA/2018/MacroInvertebrates/1.Imported/NIWAInvertebrate.xlsx',sheet=3,na = 'not sampled',skip=2)
  NIWAEPT$Group[NIWAEPT$Group=="DNI"] <- "DN1"
  NIWAEPT$LawaSiteID = NIWAMCI$LawaSiteID[match(tolower(NIWAEPT$Group),tolower(NIWAMCI$Site.code))]
  NIWAEPT$LawaSiteID[NIWAEPT$Group=="DN4"] <- "nrwqn-00025"
  names(NIWAEPT)[1]="Date"
  names(NIWAEPT)[124]="PercentageEPTTaxa"
  
  NIWAMCI <- merge(NIWAMCI,NIWAEPT%>%select(LawaSiteID,PercentageEPTTaxa,Date),by=c("LawaSiteID","Date"))
  
  NIWAMCI$SiteName = lawaIDs$SiteName[match(tolower(NIWAMCI$LawaSiteID),tolower(lawaIDs$LawaID))]
  NIWAMCI=NIWAMCI%>%select(-'QMCI',-'ntotal',-'Site.code')
  NIWAMCI$agency='niwa'
  
  
  NIWAMCIl=NIWAMCI%>%pivot_longer(cols=c(c("MCI","TaxaRichness","PercentageEPTTaxa")),names_to="Measurement",values_to='Value')
  # NIWAMCIl=NIWAMCI%>%gather(key = 'parameter',value = 'Value',c("MCI","TaxaRichness")) #TaxaRichness
  NIWAMCIl$Date = format((NIWAMCIl$Date),'%d-%b-%Y')
  NIWAMCIl$Value=as.numeric(NIWAMCIl$Value)
  
  #Round the year to that of the nearest Jan1 
  NIWAMCIl$Year = lubridate::year(round_date(lubridate::dmy(NIWAMCIl$Date),unit = 'year'))
  NIWAMCIl$SiteNamelc=tolower(NIWAMCIl$SiteName)
  NIWAMCIl$Method=NA


  rm(lawaIDs)
  # NIWAMCIl$Measurement=NIWAMCIl$parameter
  write.csv(NIWAMCIl%>%select(LawaSiteID,CouncilSiteID=lawaid,Date,Measurement,Value,Agency=agency),
            file=paste0( 'H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),'/','NIWA.csv'),
            row.names=F)
}



