rm(list=ls())
library(tidyverse)
library(doBy)
EndYear <- lubridate::isoyear(Sys.Date())-1
StartYear10 <- EndYear-9
StartYear5 <- EndYear-4
startYear15 <- EndYear - 15+1

# hStartYear10 <- StartYear10-1
# hStartYear5 <- StartYear5-1
# hEndYear <- EndYear-1
source("h:/ericg/16666LAWA/LAWA2020/Scripts/lawa_state_functions.R")
source("h:/ericg/16666LAWA/LAWA2020/Scripts/LAWAFunctions.R")

lakeSiteTable <- loadLatestSiteTableLakes(maxHistory = 90)
lakeSiteTable$LType = pseudo.titlecase(tolower(lakeSiteTable$LType))
lakeSiteTable$GeomorphicLType = pseudo.titlecase(tolower(lakeSiteTable$GeomorphicLType))
# lawaIDs=read.csv("H:/ericg/16666LAWA/LAWA2020/Metadata/LAWAMasterSiteListAsAtMarch2018.csv",stringsAsFactors = F)
# lawaIDs <- lawaIDs%>%filter(Module=="Lakes")

#hydrological year runs e.g. 1 July 2017 to 30 June 2018, and is named like 2017/18
#data from months in the first half of the year have a hydrological year one less than their calendar year
#In 2019 we can report on data to June 2018, which is the 2017/18 hydrological year
#Coding hydrological years as the first year and 'h', this means that in 2019 we want data up to hydrological year 2017h.

dir.create(paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d")),recursive = T,showWarnings = F)
# write.csv(lakeData,paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/lakesAllCouncils.csv"),row.names = F)

#Load the latest made 
if(!exists('lakeData')){
  lakeDataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/Lakes/Data",pattern = "LakesWithMetadata.csv",
                            recursive = T,full.names = T,ignore.case=T),1)
  cat(lakeDataFileName)
  lakeData=read.csv(lakeDataFileName,stringsAsFactors = F)
  rm(lakeDataFileName)
  lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]=
    lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]*1000  #mg/L to mg/m3   #Until 4/10/18 also NH4N
  
  if(lubridate::year(Sys.Date())==2020){
    # Lake Omapere in northland was being measured for chlorophyll in the wrong units.  It's values need converting.
    these = which(lakeData$LawaSiteID=='nrc-00095' & lakeData$Measurement=='CHLA')
    if(mean(lakeData$Value[these],na.rm=T)<1){
      lakeData$Value[these] = lakeData$Value[these]*1000
    }
    rm(these)
  }
}

lakeData$month=lubridate::month(lubridate::dmy(lakeData$Date))
lakeData$Year=lubridate::isoyear(lubridate::dmy(lakeData$Date))
# lakeData$hYear = lakeData$Year
# lakeData$hYear[lakeData$month<7] = lakeData$hYear[lakeData$month<7]-1
# lakeData$hYear = paste0('h',lakeData$hYear)
lakeData$monYear=paste0(lakeData$month,lakeData$Year)

lakeData=lakeData[which( lakeData$Year<=EndYear),] #lakeData$Year>=StartYear10 &
#67285 to 66233
if(0){
  #Marchtwentynineteen PaulScholes identifies that the wrong LFENZ IDs have been applied to the BOPRC lakeData
  #They're correct in the lakeSiteTable, but wrong in the data
  # lakeData$LFENZID[lakeData$Agency=='boprc']=
  #   lakeSiteTable$LFENZID[match(lakeData$LawaSiteID[lakeData$Agency=='boprc'],lakeSiteTable$LawaSiteID)]
  # lakeData$LFENZID[lakeData$SiteID=="Lake Rotomanuka at Lake Centre (Surface)"] <- 14428
  # lakeData$LFENZID[lakeData$SiteID=="Lake Rotorua by east arm"] <- 25994
  
  # lakeData <- left_join(lakeData,lakeSiteTable%>%select(-Agency,-Region),by="LawaSiteID",suffix=c("",".y"))
  
  
  # lakeData <- lakeData%>%select(-SWQLanduse,-SWQAltitude)
  # lakeData$SWQLanduse[lakeData$SWQLanduse%in%c("native","exotic/rural","Exotic","Native","Forest","Natural","forestry","Native forest","reference")]="Forest"
  # lakeData$SWQLanduse[lakeData$SWQLanduse%in%c("Unstated","","<Null>")]=NA
  # lakeData$SWQLanduse[lakeData$SWQLanduse%in%c("rural")]="Rural"
  # lakeData$SWQLanduse[lakeData$SWQLanduse%in%c("urban")]="Urban"
  # 
  # lakeData$SWQAltitude[lakeData$SWQAltitude%in%c("Unstated","")]=NA
  # lakeData$SWQAltitude[lakeData$SWQAltitude%in%c("lowland")]="Lowland"
  
  # table(lakeData$SWQLanduse)
  # table(lakeData$SWQAltitude)
  # sum(is.na(lakeData$SWQLanduse))
  # sum(is.na(lakeData$SWQAltitude))
  # unique(lakeData$LawaSiteID[is.na(lakeData$SWQLanduse)])
  # 
  # unique(cbind(lakeData$SiteID[!lakeData$SiteID==lakeData$SiteID.y],lakeData$SiteID.y[!lakeData$SiteID==lakeData$SiteID.y]))
  # unique(cbind(lakeData$CouncilSiteID[!lakeData$CouncilSiteID==lakeData$CouncilSiteID.y],lakeData$CouncilSiteID.y[!lakeData$CouncilSiteID==lakeData$CouncilSiteID.y]))
  # apply(unique(cbind(lakeData$Lat[!lakeData$Lat==lakeData$Lat.y],lakeData$Lat.y[!lakeData$Lat==lakeData$Lat.y])),1,diff)%>%summary
}

#Check 15/8/twentynineteen on the sampling frequency between lake sites
lakeData$Date=lubridate::dmy(lakeData$Date)
# freqs <- lakeData%>%split(.$LawaSiteID)%>%purrr::map(~freqCheck(.))%>%unlist
# lakeData$Frequency=freqs[lakeData$LawaSiteID]  
# rm(freqs)


# IMPLEMENT TROPHIC LEVEL INDEX FOR LAKES TLI ###
#Annual medians
TLI=lakeData%>%
  dplyr::group_by(LawaSiteID,Year,Measurement)%>%
  dplyr::mutate(n=n())%>%
  dplyr::summarise(mean=mean(Value,na.rm=T))%>%
  tidyr::spread(data=.,key=Measurement,value=mean)%>%as.data.frame
TLc=2.22+2.54*log(TLI$CHLA,base = 10)
TLs=5.1+2.27*log((1/TLI$Secchi)-(1/40),base=10) #was 5.1+2.6 for 20eighteen, until 26/6/twentynineteen Deniz Ozkundakci email 
TLn=-3.61+3.01*log(TLI$TN,base=10)
TLp=0.218+2.92*log(TLI$TP,base=10)
TLI3=(TLc+TLn+TLp)/3 #If no Secchi
TLI4=(TLc+TLs+TLn+TLp)/4
TLI$TLI = TLI4
TLI$TLI[is.na(TLI$TLI)] <- TLI3[is.na(TLI$TLI)]
rm(TLc,TLs,TLn,TLp,TLI3,TLI4)

TLI <- TLI%>%
  dplyr::select(LawaSiteID,Year,TLI)%>%
  tidyr::gather(Measurement,Value,TLI)%>%
  left_join(lakeSiteTable%>%select(LawaSiteID,LFENZID,SiteID,Agency,Region,Long,Lat,LType,GeomorphicLType))



library(parallel)
library(doParallel)
lakeParam <- c("TP", "NH4N", "TN", "Secchi", "CHLA", "pH", "ECOLI")
#TLI will not be included in the monthly medians, because it's calculated on annual average values 
suppressWarnings(rm(lakeData_A,lakeData_med,lakeData_n,lakeMonthlyMedian))
workers=makeCluster(4)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(tidyverse)
})
foreach(i = 1:length(lakeParam),.combine=rbind,.errorhandling='stop')%dopar%{
  lakeData_A = lakeData[tolower(lakeData$Measurement)==tolower(lakeParam[i]),]
  #CENSORING
  #Previously for state, left-censored was repalced by half the limit value, and right-censored was imputed
  lakeData_A$origValue=lakeData_A$Value
  if(any(lakeData_A$centype=='Left')){
    lakeData_A$Value[lakeData_A$centype=="Left"] <- lakeData_A$Value[lakeData_A$centype=="Left"]/2
    options(warn=-1)
    lcenreps=lakeData_A%>%drop_na(CouncilSiteID)%>%split(.$CouncilSiteID)%>%purrr::map(~max(.$Value[.$centype=='Left'],na.rm=T))
    options(warn=0)
    lakeData_A$lcenrep=as.numeric(lcenreps[match(lakeData_A$CouncilSiteID,names(lcenreps))])
    lakeData_A$Value[lakeData_A$centype=='Left'] <- lakeData_A$lcenrep[lakeData_A$centype=='Left']
    lakeData_A <- lakeData_A%>%select(-lcenrep)
    rm(lcenreps)
  }
  if(any(lakeData_A$centype=='Right')){
    lakeData_A$Value[lakeData_A$centype=="Right"] <- lakeData_A$Value[lakeData_A$centype=="Right"]*1.1
    options(warn=-1)
    rcenreps=lakeData_A%>%split(.$CouncilSiteID)%>%purrr::map(~min(.$Value[.$centype=='Right'],na.rm=T))
    options(warn=0)
    lakeData_A$rcenrep=as.numeric(rcenreps[match(lakeData_A$CouncilSiteID,names(rcenreps))])
    lakeData_A$Value[lakeData_A$centype=='Right'] <- lakeData_A$rcenrep[lakeData_A$centype=='Right']
    lakeData_A <- lakeData_A%>%select(-rcenrep)
    rm(rcenreps)
  }
	lakeData_A <- as.data.frame(lakeData_A)

  lakeData_A$Date=lubridate::ymd(lakeData_A$Date)
  freqs <- lakeData_A%>%split(.$LawaSiteID)%>%purrr::map(~freqCheck(.))%>%unlist
  lakeData_A$Frequency=freqs[lakeData_A$LawaSiteID]  
  rm(freqs)
  
  lakeData_med <- lakeData_A%>%
    dplyr::group_by(monYear,LawaSiteID,CouncilSiteID)%>%  #13/8/twentynineteen added CouncilSiteID in addition to LAWASiteID to separate TRC sites
    dplyr::summarise(
      # CouncilSiteID=paste(unique(CouncilSiteID),collapse='&'),
      Year = unique(Year,na.rm=T),
      month = unique(month,na.rm=T),
      # SiteID=paste(unique(SiteID),collapse='&'),
      SiteID=unique(SiteID),
      LFENZID=unique(LFENZID,na.rm=T),
      # Agency=paste(unique(Agency),collapse='&'),
      Agency=unique(Agency),
      # Region=paste(unique(Region),collapse='&'),
      Region=unique(Region),
      Value=median(Value,na.rm=T),
      Measurement=unique(Measurement,na.rm=T),
      n=n(),
      Censored=any(Censored),
      centype=paste(unique(centype[centype!='FALSE']),collapse=''),
      LType=pseudo.titlecase(unique(tolower(LType),na.rm=T)),
      GeomorphicLType=pseudo.titlecase(unique(tolower(GeomorphicLType),na.rm=T))
    )%>%ungroup
  rm(lakeData_A)
  
  return(lakeData_med)
}->lakeMonthlyMedian
stopCluster(workers)
rm(workers)


#5Year medians
lake5YearMedian <- lakeMonthlyMedian%>%
  filter(!is.na(LawaSiteID))%>%
  filter(Year>=StartYear5)%>%
  dplyr::group_by(LawaSiteID,Measurement)%>%
  dplyr::summarise(Median=quantile(Value,prob=0.5,type=5,na.rm=T),
                   n=n())%>%ungroup
TLI5Year = TLI%>%
  filter(!is.na(LawaSiteID))%>%
  filter(Year>=StartYear5)%>%
  dplyr::group_by(LawaSiteID,Measurement)%>%
  dplyr::summarise(Median = quantile(Value,prob=0.5,type=5,na.rm=T),
                   n=n())%>%ungroup
lake5YearMedian <- rbind(lake5YearMedian,TLI5Year)%>%arrange(LawaSiteID,Measurement)
rm(TLI5Year)
sum(lake5YearMedian$n>=30)/dim(lake5YearMedian)[1]
#351 out of 738  0.476
lake5YearMedian <- lake5YearMedian%>%filter(n>=30|Measurement=="TLI") #Require 30 monthly values to calculate 5-year state median
#467 from 738



#Save outputs ####

# Saving the lakeMonthlyMedian table  USED in NOF calculations
write.csv(lakeMonthlyMedian,file=paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                            "/lakeMonthlyMedian",StartYear10,"-",EndYear,format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
# lakeMonthlyMedian=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis",
#                                    pattern = "lakeMonthlyMedian.*",recursive = T,full.names = T,ignore.case=T),1),stringsAsFactors = F)

#LakeSiteState for ITE
write.csv(lake5YearMedian%>%drop_na(Median)%>%
            dplyr::transmute(LAWAID=LawaSiteID,
                      Parameter=Measurement,
                      Year=EndYear,
                      Median=Median),
          file=paste0('h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/',format(Sys.Date(),'%Y-%m-%d'),
                      '/ITELakeSiteState',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
# lake5YearMedian=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis",pattern="LakeSiteState.*csv",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors=F)

# LakeTLI
write.csv(TLI,paste0('h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/',format(Sys.Date(),'%Y-%m-%d'),
                     '/auditLakeTLI',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
write.csv(TLI%>%drop_na(Value)%>%
            dplyr::transmute(Lake=SiteID,
                             FENZID=LFENZID,
                             TLIYear=Year,
                             TLI=signif(Value,2),
                             MixingPattern=LType,
                             GeomorphicType=GeomorphicLType),
          paste0('h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/',format(Sys.Date(),'%Y-%m-%d'),
                 '/ITELakeTLI',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
# TLI=read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis',pattern='LakeTLIAnnual',recursive = T,full.names = T),1),stringsAsFactors = F)
# LakeTLI=read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis',pattern='LakeTLI[[:digit:]]',recursive = T,full.names = T),1),stringsAsFactors = F)
