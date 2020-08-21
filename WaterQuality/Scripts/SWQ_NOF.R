#===================================================================================================
#  LAWA NATIONAL OBJECTIVES FRAMEWORK
#  Horizons Regional Council
#
#  4 September 2016
#
#  Creator: Kelvin Phan  2014
#
#  Updated by: Maree Patterson 2016
#              Sean Hodges
#             Eric Goodwin 2018 Cawthron Institute
#  Horizons Regional Council
#===================================================================================================


rm(list = ls())
library(tidyr)
library(parallel)
library(doParallel)
require(reshape2)
library(areaplot)
setwd("h:/ericg/16666LAWA/LAWA2020/WaterQuality/")
source("h:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R")
source("h:/ericg/16666LAWA/LAWA2020/WaterQuality/scripts/SWQ_NOF_Functions.R")
try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),'%Y-%m-%d'))),silent=T)
riverSiteTable=loadLatestSiteTableRiver()

## Load NOF Bands
NOFbandDefinitions <- read.csv("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/NOFbandDefinitions3.csv", header = TRUE, stringsAsFactors=FALSE)
NOFbandDefinitions <- NOFbandDefinitions[,1:9]
# Band Median.Nitrate X95th.Percentile.Nitrate Median.Ammoniacal.N Max.Ammoniacal.N E..coli Ecoli95 EcoliRec540 EcoliRec260
# 1    A         x<=1.0                   x<=1.5             x<=0.03          x<=0.05  x<=130   <=540         x<5        x<20
# 2    B     x<1&x<=2.4             x>1.5&x<=3.5      x>0.03&x<=0.24    x>0.05&x<=0.4  x<=130 x<=1000  x>=5&x<=10 x>=20&x<=30
# 3    C   x>2.4&x<=6.9             x>3.5&x<=9.8       x>0.24&x<=1.3     x>0.4&x<=2.2  x<=130 x<=1200 x>=10&x<=20 x>=20&x<=34
# 4    D          x>6.9                    x>9.8              x>1.30            x>2.2   x>130  x>1200 x>=20&x<=30        x>34
# 5    E          x>Inf                    x>Inf               x>Inf            x>Inf   x>260  x>1200        x>30        x>50

#===================================================================================================
## Load LAWA Data

if(!exists('wqdata')){
  combowqdata=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data",pattern = "AllCouncils.csv",recursive = T,full.names = T),1)
  cat(combowqdata)
  wqdata=readr::read_csv(combowqdata,guess_max=150000)%>%as.data.frame
  rm(combowqdata)
  wqdYear=lubridate::isoyear(dmy(wqdata$Date))
  #Reference Dates
  EndYear <- lubridate::isoyear(Sys.Date())-1
  StartYear5 <- EndYear - 5 + 1
  firstYear = min(wqdYear,na.rm=T)
  firstYear=2005
  wqdata <- wqdata[which(wqdYear>=firstYear & wqdYear<=EndYear),]
  # |(wqdYear>=(StartYear5-1) & wqdYear<=EndYear & wqdata$Measurement=='ECOLI')),]
  yr <- c(as.character(firstYear:EndYear),paste0(as.character(firstYear:(EndYear-4)),'to',as.character((firstYear+4):EndYear)))
  rollyrs=which(grepl('to',yr))
  nonrollyrs=which(!grepl('to',yr))
  reps <- length(yr)
  rm(wqdYear)
}

wqparam <- c("BDISC","TURB","NH4",
             "PH","TON","TN",
             "DRP","TP","ECOLI") 

#Replace censored values with 0.5 or 1.1 x max censored or min censored, per site, 
#then calculate per site&date median values to take forward
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
})
startTime=Sys.time()
foreach(i = 1:length(wqparam),.combine = rbind,.errorhandling = "stop")%dopar%{
  wqdata_A = wqdata%>%dplyr::filter(tolower(Measurement)==tolower(wqparam[i]))
  #CENSORING
  #Previously for state, left-censored was repalced by half the limit value, and right-censored was imputed
  #left-censored replacement value is the maximum of left-censored values, per site
  wqdata_A$origValue=wqdata_A$Value
  if(any(wqdata_A$CenType=='Left')){
    wqdata_A$Value[wqdata_A$CenType=="Left"] <- wqdata_A$Value[wqdata_A$CenType=="Left"]/2
    options(warn=-1)
    lcenreps=wqdata_A%>%split(.$CouncilSiteID)%>%purrr::map(~max(.$Value[.$CenType=='Left'],na.rm=T)) #"Left Censor Replacements"
    options(warn=0)
    wqdata_A$lcenrep=as.numeric(lcenreps[match(wqdata_A$CouncilSiteID,names(lcenreps))])
    wqdata_A$Value[wqdata_A$CenType=='Left'] <- wqdata_A$lcenrep[wqdata_A$CenType=='Left']
    wqdata_A <- wqdata_A%>%dplyr::select(-lcenrep)
    rm(lcenreps)
  }
  if(any(wqdata_A$CenType=='Right')){
    wqdata_A$Value[wqdata_A$CenType=="Right"] <- wqdata_A$Value[wqdata_A$CenType=="Right"]*1.1
    options(warn=-1)
    rcenreps=wqdata_A%>%split(.$CouncilSiteID)%>%purrr::map(~min(.$Value[.$CenType=="Right"],na.rm=T))
    options(warn=0)
    wqdata_A$rcenrep=as.numeric(rcenreps[match(wqdata_A$CouncilSiteID,names(rcenreps))])
    wqdata_A$Value[wqdata_A$CenType=='Right'] <- wqdata_A$rcenrep[wqdata_A$CenType=='Right']
    wqdata_A <- wqdata_A%>%dplyr::select(-rcenrep)
    rm(rcenreps)
  }
  wqdata_A <- as.data.frame(wqdata_A)  
  
  wqdata_A$Date=lubridate::dmy(wqdata_A$Date)
  
  freqs <- wqdata_A%>%split(.$LawaSiteID)%>%purrr::map(~freqCheck(.))%>%unlist
  wqdata_A$Frequency=freqs[wqdata_A$LawaSiteID]  
  rm(freqs)
  #This medianing was until July 5 20nineteen in the SWQ_State script
  wqdata_med <- wqdata_A%>%
    dplyr::group_by(LawaSiteID,Date)%>%
    dplyr::summarise(
      SiteID=paste(unique(SiteID),collapse='&'),
      CouncilSiteID=paste(unique(CouncilSiteID),collapse='&'),
      Agency=paste(unique(Agency),collapse='&'),
      Region=paste(unique(Region),collapse='&'),
      Value=median(Value,na.rm=T),
      Measurement=unique(Measurement,na.rm=T),  
      n=n(),
      Censored=any(Censored),
      CenType=paste(unique(CenType[CenType!='FALSE']),collapse=''),
      Landcover=paste(unique(Landcover),collapse=''),
      SWQAltitude=paste(unique(SWQAltitude),collapse='')
    )%>%ungroup

  return(wqdata_med)
}->wqdataPerDateMedian
stopCluster(workers)
rm(workers)
cat(Sys.time()-startTime)  
#14.4 seconds June23
#36.2 s june30, now keeping all years
#44.5 aug21 all years
#44.8 aug21 from 2005 onward

# Saving the wqdataPerDateMedian table to be USED in NOF calculations. 
# NOTE AFTER HAVING OUT-COMMENTED THE 51/52 LINE, WE'VE NOW GOT ALL YEARS, FOR THE ROLLING NOF 
# Has six years available for ECOli if necessary

save(wqdataPerDateMedian,file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),
                          "/wqdataPerDateMedian",StartYear5,"-",EndYear,"ec6.RData"))
# load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",pattern='wqdataPerDateMedian',
#                                   recursive = T,full.names=T,ignore.case=T),1),verbose = T)

# Subset to just have the variables that are tested against NOF standards
sub_swq <- wqdataPerDateMedian%>%dplyr::select(c("LawaSiteID","CouncilSiteID","Date","Measurement","Value"))%>%
  dplyr::filter(tolower(Measurement)%in%tolower(c("NH4","TON","ECOLI","PH")))%>%
  dplyr::filter(lubridate::isoyear(Date)<=EndYear)
#1097095 - 514222

table(lubridate::isoyear(sub_swq$Date),sub_swq$Measurement)
#       ECOLI   NH4    PH   TON
# 2005  4841  5195  4297  3796
# 2006  5386  6130  5241  4667
# 2007  6272  6848  6277  5386
# 2008  7031  7616  6859  6153
# 2009  7654  8187  7848  6734
# 2010  7620  8207  7737  6752
# 2011  8067  8570  8232  7125
# 2012  8336  9012  8759  7566
# 2013 10157  9965  9749  8501
# 2014 10745 10414 10171  8993
# 2015 11205 10903 10719  9687
# 2016 11473 11147 10770 10118
# 2017 11755 11498 10885 10406
# 2018 11548 11442 10431 10440
# 2019 11430 11322  9792 10155

#+++++++++++++++++++++++++++++ Ammonia adjustment for pH++++++++++++++++++++++++++++++++++++
adjnh4="H:/ericg/16666LAWA/LAWA2020/WaterQuality/metadata/NOFAmmoniaAdjustment.csv"
adjnh4=NH4adj(sub_swq,meas=c("NH4","PH"),csv = adjnh4)
sub_swq<-rbind(sub_swq,adjnh4)
rm(adjnh4)
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

uLAWAids <- unique(sub_swq$LawaSiteID)
# uLAWAids = sample(uLAWAids,size=5,replace = F)
cat(length(uLAWAids),'\t')
if(exists("NOFSummaryTable")) { rm("NOFSummaryTable") }

workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  # library(doBy)
  library(plyr)
  library(dplyr)
  source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
})
startTime=Sys.time()

foreach(i = 1:length(uLAWAids),.combine=rbind,.errorhandling="stop",.inorder=F)%dopar%{
  suppressWarnings(rm(tonsite,nh4site,ecosite,rightSite,value,Value)  )
  # rightSite=sub_swq[(sub_swq$LawaSiteID==uLAWAids[i]),]
  # rightSite=rightSite[!is.na(rightSite$Value),]
  rightSite <- sub_swq%>%
    dplyr::filter(LawaSiteID==uLAWAids[i])%>%
    tidyr::drop_na(Value)%>%
    mutate(Year=format(Date,'%Y'))
  # create table of compliance with proposed National Objectives Framework
  Com_NOF <- data.frame (LawaSiteID               = rep(uLAWAids[i],length(yr)),
                         Year                     = yr,
                         NitrateMed               = as.numeric(rep(NA,reps)),
                         NitrateMed_Band          = rep(as.character(NA),reps),#factor(rep(NA,reps),levels = c("A","B","C","D")),
                         Nitrate95                = as.numeric(rep(NA,reps)),
                         Nitrate95_Band           = rep(as.character(NA)),#factor(rep(NA,reps),levels = c("A","B","C","D")),
                         Nitrate_Toxicity_Band    = rep(as.character(NA),reps),#factor(rep(NA,reps),levels = c("A","B","C","D")),
                         NitrateAnalysisNote      = rep('',reps),
                         AmmoniacalMed            = as.numeric(rep(NA,reps)),
                         AmmoniacalMed_Band       = rep(as.character(NA),reps),#factor(rep(NA,reps),levels = c("A","B","C","D")),
                         Ammoniacal95             = as.numeric(rep(NA,reps)),
                         Ammoniacal95_Band        = rep(as.character(NA),reps),#factor(rep(NA,reps),levels = c("A","B","C","D")),
                         Ammonia_Toxicity_Band    = rep(as.character(NA),reps),#factor(rep(NA,reps),levels = c("A","B","C","D")),
                         AmmoniaAnalysisNote      = rep('',reps),
                         EcoliPeriod              = as.numeric(rep(NA,reps)),
                         EcoliMed                 = as.numeric(rep(NA,reps)),
                         EcoliMed_Band            = rep(as.character(NA),reps),
                         Ecoli95                  = as.numeric(rep(NA,reps)),
                         Ecoli95_Band             = rep(as.character(NA),reps),
                         EcoliRecHealth540        = as.numeric(rep(NA,reps)),
                         EcoliRecHealth540_Band   = rep(as.character(NA),reps),
                         EcoliRecHealth260        = as.numeric(rep(NA,reps)),
                         EcoliRecHealth260_Band   = rep(as.character(NA),reps),
                     #    EcoliSummaryband         = rep(as.character(NA),reps),#factor(rep(NA,reps),levels=c("A","B","C","D","E")),
                         EcoliAnalysisNote        = rep('',reps),
                         stringsAsFactors = FALSE)
  
  ###################### Nitrate  ########################################
  # rolling5 <- function(siteChemSet,quantProb){In SWQ_NOF functions}
    tonsite <- rightSite%>%dplyr::filter(Measurement=="TON")
  #Median Nitrate
  annualMedian <- tonsite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5))
  if(dim(annualMedian)[1]!=0){
    Com_NOF$NitrateMed <- annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds=rolling5(siteChemSet = tonsite,quantProb = 0.5)
    rollFails=which(is.na(as.numeric(rollingMeds)))
    Com_NOF$NitrateMed[yr%in%names(rollingMeds)] <- as.numeric(rollingMeds)
    Com_NOF$NitrateAnalysisNote[rollyrs[rollFails]] <- paste0(' Need 30 values for 5yr median, have',
                                                                          strFrom(s = rollingMeds[rollFails],c = 'y'))
    #find the band which each value belong to
    Com_NOF$NitrateMed_Band <- sapply(Com_NOF$NitrateMed,NOF_FindBand,bandColumn = NOFbandDefinitions$Median.Nitrate)
    Com_NOF$NitrateMed_Band[!is.na(Com_NOF$NitrateMed_Band)] <- 
      sapply(Com_NOF$NitrateMed_Band[!is.na(Com_NOF$NitrateMed_Band)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(annualMedian,rollingMeds,rollFails)
    
    #95th percentile Nitrate
    annual95 <- tonsite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.95,type=5,na.rm=T))
    
    Com_NOF$Nitrate95 = annual95$value[match(Com_NOF$Year,annual95$Year)]
    #Rolling 5yr 95%ile
    rolling95=rolling5(tonsite,0.95)
    rollFails=which(is.na(as.numeric(rolling95)))
    Com_NOF$Nitrate95[yr%in%names(rolling95)] <- as.numeric(rolling95)
    Com_NOF$NitrateAnalysisNote[rollyrs[rollFails]] <- paste0(Com_NOF$NitrateAnalysisNote[is.na(as.numeric(rolling95))],
                                                                          ' Need 30 values for 5yr 95%ile, have',
                                                                          strFrom(s = rolling95[rollFails],c = 'y'))
    #find the band which each value belong to
    Com_NOF$Nitrate95_Band <- sapply(Com_NOF$Nitrate95,NOF_FindBand,bandColumn = NOFbandDefinitions$X95th.Percentile.Nitrate)
    Com_NOF$Nitrate95_Band[!is.na(Com_NOF$Nitrate95_Band)] <- 
      sapply(Com_NOF$Nitrate95_Band[!is.na(Com_NOF$Nitrate95_Band)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
    
    #Nitrate Toxicity
    #The worse of the two nitrate bands
    Com_NOF$Nitrate_Toxicity_Band = apply(Com_NOF%>%dplyr::select(NitrateMed_Band, Nitrate95_Band),1,max,na.rm=T)
    rm(annual95,rolling95,rollFails)
  }else{
    Com_NOF$NitrateAnalysisNote = paste0('n = ',sum(!is.na(tonsite$Value)),' Insufficient to calculate annual medians ')
  }
  rm(tonsite)
  
  ###################### Ammonia  ############################
  nh4site=rightSite%>%dplyr::filter(Measurement=="NH4adj")
  if(all(nh4site$Value==-99)){
    Com_NOF$AmmoniaAnalysisNote=paste0('No pH data available for NH4 adjustment, so NH4 cannot be judged against NOF standards. ')
  }else{
    nh4site=nh4site[!(nh4site$Value==(-99)),]
    #Median Ammoniacal Nitrogen
    annualMedian <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))
    if(dim(annualMedian)[1]!=0){
      Com_NOF$AmmoniacalMed = annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
      #Rolling 5yr median
      rollingMeds=rolling5(nh4site,0.5)
      rollFails=which(is.na(as.numeric(rollingMeds)))
      Com_NOF$AmmoniacalMed[yr%in%names(rollingMeds)] <- as.numeric(rollingMeds)
      Com_NOF$AmmoniaAnalysisNote[rollyrs[rollFails]] <- paste0(' Need 30 values for 5yr median, have',
                                                                            strFrom(s = rollingMeds[rollFails],c = 'y'))      
      Com_NOF$AmmoniacalMed_Band <- sapply(Com_NOF$AmmoniacalMed,NOF_FindBand,bandColumn=NOFbandDefinitions$Median.Ammoniacal.N) 
      Com_NOF$AmmoniacalMed_Band[!is.na(Com_NOF$AmmoniacalMed_Band)] <- 
        sapply(Com_NOF$AmmoniacalMed_Band[!is.na(Com_NOF$AmmoniacalMed_Band)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
      rm(annualMedian,rollingMeds,rollFails)
      
      #95th percentile Ammoniacal Nitrogen
      annual95 <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.95,type=5,na.rm=T))
      Com_NOF$Ammoniacal95 <- annual95$value[match(Com_NOF$Year,annual95$Year)]
 
      #Rolling 5yr 95%ile
      rolling95=rolling5(nh4site,0.95)
      rollFails=which(is.na(as.numeric(rolling95)))
      Com_NOF$Ammoniacal95[yr%in%names(rolling95)] <- as.numeric(rolling95)
      Com_NOF$AmmoniaAnalysisNote[rollyrs[rollFails]] <- paste0(Com_NOF$AmmoniaAnalysisNote[rollFails],
                                                                            ' Need 30 values for 5yr 95%ile, have',
                                                                            strFrom(s = rolling95[rollFails],c = 'y'))
      Com_NOF$Ammoniacal95_Band <-sapply(Com_NOF$Ammoniacal95,NOF_FindBand,bandColumn=NOFbandDefinitions$Max.Ammoniacal.N)
      Com_NOF$Ammoniacal95_Band <- sapply(Com_NOF$Ammoniacal95_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
      
      #Ammonia Toxicity
      Com_NOF$Ammonia_Toxicity_Band=apply(Com_NOF%>%dplyr::select(AmmoniacalMed_Band, Ammoniacal95_Band),1,max,na.rm=T)
    }else{
      Com_NOF$AmmoniaAnalysisNote=paste0(Com_NOF$AmmoniaAnalysisNote,'n = ',sum(!is.na(nh4site$Value)),
                                         ' Insufficient to calculate annual medians. ')
    }  
    rm(annual95,rolling95)
  }
  rm(nh4site)
  ######################  E.Coli #########################################
  suppressWarnings(rm(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band,ecosite,ecosite))
  ecosite=rightSite%>%dplyr::filter(Measurement=="ECOLI")
  
  #E coli median
  annualMedian <- ecosite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))
  if(dim(annualMedian)[1]!=0){
    Com_NOF$EcoliMed <- annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    Com_NOF$EcoliPeriod=ifelse(is.na(Com_NOF$EcoliMed),NA,1)
    #rolling 5yr or 6yr median
    rollingMeds=rolling5(ecosite,0.5,extendToSix = T,nreq=60)
    rollFails=is.na(as.numeric(rollingMeds))
    Com_NOF$EcoliMed[yr%in%names(rollingMeds)] = readr::parse_number(rollingMeds) #not "as.numeric", because good results include a years suffix
    Com_NOF$EcoliPeriod[yr%in%names(rollingMeds)] = ifelse(rollFails,NA,ifelse(grepl(pattern = '_6',rollingMeds),6,5))
    #bands
    Com_NOF$EcoliMed_Band <- sapply(Com_NOF$EcoliMed,NOF_FindBand,bandColumn=NOFbandDefinitions$E..coli)
  }else{
    Com_NOF$EcoliAnalysisNote=paste0(Com_NOF$EcoliAnalysisNote,"n = ",sum(!is.na(ecosite$Value)),
                                     " Insufficient data to calculate annual medians")
  }
  rm(annualMedian,rollingMeds,rollFails)
  
  #Ecoli 95th percentile 
  annual95 <- ecosite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.95,type=5,na.rm=T))
  if(length(annual95)!=0){
    Com_NOF$Ecoli95 <- annual95$value[match(Com_NOF$Year,annual95$Year)]
    #rolling 5yr or 6yr 95%ile
    rolling95 = rolling5(ecosite,0.95,extendToSix = T,nreq=60)
    rollFails=which(is.na(as.numeric(rolling95)))
    Com_NOF$Ecoli95[yr%in%names(rolling95)] <- readr::parse_number(rolling95)#not "as.numeric", because good results include a years suffix
    
    
    #bands 
    Com_NOF$Ecoli95_Band <- sapply(Com_NOF$Ecoli95,NOF_FindBand,bandColumn=NOFbandDefinitions$Ecoli95)
  }else{
    Com_NOF$EcoliAnalysisNote=paste0(Com_NOF$EcoliAnalysisNote,"n = ",sum(!is.na(ecosite$Value)),
                                      " Insufficient data to calculate annual max")
  }
  rm(annual95,rolling95,rollFails)
  
  #Exceedance percentages
  options(warn=-1)
  for(yy in 1:length(Com_NOF$Year)){
    if(!is.na(as.numeric(Com_NOF$Year[yy]))){
      ecv=ecosite$Value[which(ecosite$Year==Com_NOF$Year[yy])]
    }else{
      startYear = strTo(s = Com_NOF$Year[yy],c = 'to')
      stopYear = strFrom(s= Com_NOF$Year[yy],c = 'to')
      ecv=ecosite$Value[ecosite$Year>=startYear & ecosite$Year<=stopYear]
    }
    if(length(ecv)>0){
      Com_NOF$EcoliRecHealth540[yy]=sum(ecv>540)/length(ecv)*100
      Com_NOF$EcoliRecHealth260[yy]=sum(ecv>260)/length(ecv)*100
    }
  }
  options(warn=0)
  #Bands
   suppressWarnings(Com_NOF$EcoliRecHealth540_Band <- sapply(Com_NOF$EcoliRecHealth540,NOF_FindBand,bandColumn=NOFbandDefinitions$EcoliRec540))
   suppressWarnings(Com_NOF$EcoliRecHealth260_Band <- sapply(Com_NOF$EcoliRecHealth260,NOF_FindBand,bandColumn=NOFbandDefinitions$EcoliRec260))

   these=which(is.na(Com_NOF$EcoliPeriod))
   if(length(these)>0)  { #Set bands to NA, if insufficent data
     Com_NOF$EcoliAnalysisNote[these]=paste0(Com_NOF$EcoliAnalysisNote,
                                             ' Need ',rep(c(12,60),c(sum(!grepl('to',yr)),sum(grepl('to',yr)))),
                                             ' values for ',rep(c('annual','5yr'),c(sum(!grepl('to',yr)),sum(grepl('to',yr)))),
                                             ', have  ',count5(ecosite,T))[these]
   }
  
  rm(ecosite)
  rm(rightSite)
  return(Com_NOF)
}->NOFSummaryTable
stopCluster(workers)
rm(workers)
cat(Sys.time()-startTime)  
#6.5 seconds 23June
#23.4s 2 July rolling medians
#25s aug21

#These contain the best case out of these scorings, the worst of which contributes.
suppressWarnings(cnEc_Band <- sapply(NOFSummaryTable$EcoliMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEc95_Band <- sapply(NOFSummaryTable$Ecoli95_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth540_Band <- sapply(NOFSummaryTable$EcoliRecHealth540_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth260_Band <- sapply(NOFSummaryTable$EcoliRecHealth260_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))  
NOFSummaryTable$EcoliSummaryband = as.character(apply(cbind(pmax(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band)),1,max))
rm("cnEc_Band","cnEc95_Band","cnEcRecHealth540_Band","cnEcRecHealth260_Band")

if(0){
  with(NOFSummaryTable,plot(as.factor(NitrateMed_Band),NitrateMed))
  with(NOFSummaryTable,plot(as.factor(Nitrate95_Band),Nitrate95))
  with(NOFSummaryTable,plot(as.factor(Nitrate_Toxicity_Band),Nitrate95))
  with(NOFSummaryTable,plot(as.factor(AmmoniacalMed_Band),AmmoniacalMed+0.5*min(AmmoniacalMed[AmmoniacalMed>0],na.rm=T),log='y'))
  table(NOFSummaryTable$NitrateMed_Band,NOFSummaryTable$Nitrate_Toxicity_Band)
  table(NOFSummaryTable$Nitrate95_Band,NOFSummaryTable$Nitrate_Toxicity_Band)
  table(NOFSummaryTable$AmmoniacalMed_Band,NOFSummaryTable$Ammonia_Toxicity_Band)
  table(NOFSummaryTable$Ammoniacal95_Band,NOFSummaryTable$Nitrate_Toxicity_Band)
  table(NOFSummaryTable$Ecoli95_Band,NOFSummaryTable$EcoliSummaryband)
}
cat('\n')

NOFSummaryTable$CouncilSiteID=riverSiteTable$CouncilSiteID[match(NOFSummaryTable$LawaSiteID,riverSiteTable$LawaSiteID)]
NOFSummaryTable$SiteID=riverSiteTable$SiteID[match(NOFSummaryTable$LawaSiteID,riverSiteTable$LawaSiteID)]
NOFSummaryTable <- NOFSummaryTable%>%dplyr::select(LawaSiteID,CouncilSiteID,SiteID,Year:EcoliAnalysisNote)
NOFSummaryTable <- merge(NOFSummaryTable, riverSiteTable) 

#############################Save the output table ############################
#For audit
write.csv(NOFSummaryTable%>%filter(grepl(pattern = 'to',x = Year)),
          file = paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                        "/NOFSummaryTable_Rolling.csv"),row.names=F)
# ************
# Note this is not the full summary table - only rolling years
# NOFSummaryTable <- read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",pattern="NOFSummaryTable",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)
# ************

NOFSummaryTable$SWQAltitude=pseudo.titlecase(NOFSummaryTable$SWQAltitude)
NOFSummaryTable$SWQLanduse=pseudo.titlecase(NOFSummaryTable$SWQLanduse)

#For ITE
#Make outputs for ITE
# Reshape Output
RiverNOF <-
  NOFSummaryTable%>%
  dplyr::filter(grepl(pattern = 'to',x = Year))%>%
  #And now we're not doing the nitrate ones!  Because, toxicity!
  # dplyr::select(-NitrateMed,-NitrateMed_Band,-Nitrate95,-Nitrate95_Band,-Nitrate_Toxicity_Band,-NitrateAnalysisNote)%>%
  dplyr::rename(LAWAID=LawaSiteID,
                SiteName=CouncilSiteID,
                Year=Year)%>%
  dplyr::select(-SiteID,-accessDate,-Lat,-Long,-AltitudeCl,-SWQAltitude,-SWQLanduse,
                -Altitude,-Landcover,-NZReach,-Agency,-Region,-Catchment,-ends_with('Note'))%>%
  tidyr::drop_na(LAWAID)%>%
  dplyr::mutate_if(is.factor,as.character)%>%
  melt(id.vars=c("LAWAID","SiteName","Year"))%>%
  dplyr::rename(Parameter=variable,Value=value)

RiverNOF$Band=RiverNOF$Value
RiverNOF$Value=as.numeric(RiverNOF$Value)
RiverNOF$Band[!is.na(RiverNOF$Value)] <- NA

write.csv(RiverNOF,
          file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITERiverNOF",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
rm(RiverNOF)

NOFSummaryTable <- NOFSummaryTable%>%filter(grepl('to',Year,ignore.case=T))



tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendNitrateMed.tif"),
     width = 8,height=12,units='in',res=300,compression='lzw',type='cairo')
NitroSitesPerYear = wqdata%>%filter(Measurement=="TON")%>%filter(LawaSiteID%in%NOFSummaryTable$LawaSiteID)%>%
  group_by(lubridate::isoyear(dmy(Date)))%>%dplyr::summarise(nsites=length(unique(LawaSiteID)))%>%ungroup
names(NitroSitesPerYear)=c("Year","nSites")
NOFSummaryTable$NitrateMed_BandF=factor(as.character(NOFSummaryTable$NitrateMed_Band),levels=rev(c("A","B","C","D","NA")),labels=rev(c("A","B","C","D","NA")))
NOFSummaryTable$NitrateMed_BandF[is.na(NOFSummaryTable$NitrateMed_Band)] <- "NA"

par(mfrow=c(3,1))
NitrateEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,NitrateMed_BandF)%>%
  # drop_na(NitrateMed_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,NitrateMed_BandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(NitrateMed_BandF))%>%t%>%data.frame
rnames=rownames(NitrateEvolutionF)
names(NitrateEvolutionF)=sapply(NitrateEvolutionF[1,],as.character)
NitrateEvolutionF=NitrateEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(NitrateEvolutionF) <- rnames[-1]
areaplot(NitrateEvolutionF,xlab='Year',
         col=rev(c("#aaaaaaFF","#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),main='Median Nitrate',ylab='Count of sites')
lines(NitroSitesPerYear$Year[-c(1:4)],NitroSitesPerYear$nSites[-c(1:4)],lwd=2)
text(NitroSitesPerYear$Year[5],NitroSitesPerYear$nSites[5]*0.9,"total sites available",cex=1,pos = 4)

NitrateEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,NitrateMed_Band)%>%
  drop_na(NitrateMed_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,NitrateMed_Band,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange((NitrateMed_Band))%>%t%>%data.frame
rnames=rownames(NitrateEvolution)
names(NitrateEvolution)=sapply(NitrateEvolution[1,],as.character)
NitrateEvolution=NitrateEvolution[-1,]%>%apply(2,as.numeric)
rownames(NitrateEvolution) <- rnames[-1]
areaplot(NitrateEvolution,col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),ylim=c(0,1.1),main='Median Nitrate',ylab='Proportion of sites')
text(NitroSitesPerYear$Year[-c(1:4)],1.05,NitroSitesPerYear$nSites[-c(1:4)],lwd=2)

bp=barplot(t(NitrateEvolution),col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
           legend=T,args.legend=list(x='bottomleft',inset=0.04),ylim=c(0,1.1),main='Median Nitrate',ylab='Proportion of sites')
text(bp,1.05,NitroSitesPerYear$nSites[-c(1:4)],lwd=2)
if(names(dev.cur())=='tiff'){dev.off()}


tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendNitrate95.tif"),
     width = 8,height=12,units='in',res=300,compression='lzw',type='cairo')
NitroSitesPerYear = wqdata%>%filter(Measurement=="TON")%>%filter(LawaSiteID%in%NOFSummaryTable$LawaSiteID)%>%
  group_by(lubridate::isoyear(dmy(Date)))%>%dplyr::summarise(nsites=length(unique(LawaSiteID)))%>%ungroup
names(NitroSitesPerYear)=c("Year","nSites")
NOFSummaryTable$Nitrate95_BandF=factor(as.character(NOFSummaryTable$Nitrate95_Band),levels=rev(c("A","B","C","D","NA")),labels=rev(c("A","B","C","D","NA")))
NOFSummaryTable$Nitrate95_BandF[is.na(NOFSummaryTable$Nitrate95_Band)] <- "NA"

par(mfrow=c(3,1))
NitrateEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,Nitrate95_BandF)%>%
  # drop_na(Nitrate95_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,Nitrate95_BandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(Nitrate95_BandF))%>%t%>%data.frame
rnames=rownames(NitrateEvolutionF)
names(NitrateEvolutionF)=sapply(NitrateEvolutionF[1,],as.character)
NitrateEvolutionF=NitrateEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(NitrateEvolutionF) <- rnames[-1]
areaplot(NitrateEvolutionF,xlab='Year',
         col=rev(c("#aaaaaaFF","#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),main='95th %ile Nitrate',ylab='Count of sites')
lines(NitroSitesPerYear$Year[-c(1:4)],NitroSitesPerYear$nSites[-c(1:4)],lwd=2)
text(NitroSitesPerYear$Year[5],NitroSitesPerYear$nSites[5]*0.9,"total sites available",cex=1,pos = 4)

NitrateEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,Nitrate95_Band)%>%
  drop_na(Nitrate95_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,Nitrate95_Band,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange((Nitrate95_Band))%>%t%>%data.frame
rnames=rownames(NitrateEvolution)
names(NitrateEvolution)=sapply(NitrateEvolution[1,],as.character)
NitrateEvolution=NitrateEvolution[-1,]%>%apply(2,as.numeric)
rownames(NitrateEvolution) <- rnames[-1]
areaplot(NitrateEvolution,col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),ylim=c(0,1.1),main='95th %ile Nitrate',ylab='Proportion of sites')
text(NitroSitesPerYear$Year[-c(1:4)],1.05,NitroSitesPerYear$nSites[-c(1:4)],lwd=2)

bp=barplot(t(NitrateEvolution),col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
           legend=T,args.legend=list(x='bottomleft',inset=0.04),ylim=c(0,1.1),main='95th %ile Nitrate',ylab='Proportion of sites')
text(bp,1.05,NitroSitesPerYear$nSites[-c(1:4)],lwd=2)
if(names(dev.cur())=='tiff'){dev.off()}

tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendNitrateTox.tif"),
     width = 8,height=12,units='in',res=300,compression='lzw',type='cairo')
NitroSitesPerYear = wqdata%>%filter(Measurement=="TON")%>%filter(LawaSiteID%in%NOFSummaryTable$LawaSiteID)%>%
  group_by(lubridate::isoyear(dmy(Date)))%>%dplyr::summarise(nsites=length(unique(LawaSiteID)))%>%ungroup
names(NitroSitesPerYear)=c("Year","nSites")
NOFSummaryTable$Nitrate_Toxicity_BandF=factor(as.character(NOFSummaryTable$Nitrate_Toxicity_Band),levels=rev(c("A","B","C","D","NA")),labels=rev(c("A","B","C","D","NA")))
NOFSummaryTable$Nitrate_Toxicity_BandF[is.na(NOFSummaryTable$Nitrate_Toxicity_Band)] <- "NA"

par(mfrow=c(3,1))
NitrateEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,Nitrate_Toxicity_BandF)%>%
  # drop_na(Nitrate_Toxicity_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,Nitrate_Toxicity_BandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(Nitrate_Toxicity_BandF))%>%t%>%data.frame
rnames=rownames(NitrateEvolutionF)
names(NitrateEvolutionF)=sapply(NitrateEvolutionF[1,],as.character)
NitrateEvolutionF=NitrateEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(NitrateEvolutionF) <- rnames[-1]
areaplot(NitrateEvolutionF,xlab='Year',
         col=rev(c("#aaaaaaFF","#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),main='Nitrate toxicity',ylab='Count of sites')
lines(NitroSitesPerYear$Year[-c(1:4)],NitroSitesPerYear$nSites[-c(1:4)],lwd=2)
text(NitroSitesPerYear$Year[5],NitroSitesPerYear$nSites[5]*0.9,"total sites available",cex=1,pos = 4)

NitrateEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,Nitrate_Toxicity_Band)%>%
  drop_na(Nitrate_Toxicity_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,Nitrate_Toxicity_Band,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange((Nitrate_Toxicity_Band))%>%t%>%data.frame
rnames=rownames(NitrateEvolution)
names(NitrateEvolution)=sapply(NitrateEvolution[1,],as.character)
NitrateEvolution=NitrateEvolution[-1,]%>%apply(2,as.numeric)
rownames(NitrateEvolution) <- rnames[-1]
areaplot(NitrateEvolution,col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),ylim=c(0,1.1),main='Nitrate toxicity',ylab='Proportion of sites')
text(NitroSitesPerYear$Year[-c(1:4)],1.05,NitroSitesPerYear$nSites[-c(1:4)],lwd=2)

bp=barplot(t(NitrateEvolution),col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
           legend=T,args.legend=list(x='bottomleft',inset=0.04),ylim=c(0,1.1),main='Nitrate toxicity',ylab='Proportion of sites')
text(bp,1.05,NitroSitesPerYear$nSites[-c(1:4)],lwd=2)
if(names(dev.cur())=='tiff'){dev.off()}
rm(NitroSitesPerYear,NitrateEvolution,NitrateEvolutionF)


tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendAmmoniacalMed.tif"),
     width = 8,height=12,units='in',res=300,compression='lzw',type='cairo')
AmmonSitesPerYear = wqdata%>%filter(Measurement=="NH4")%>%filter(LawaSiteID%in%NOFSummaryTable$LawaSiteID)%>%
  group_by(lubridate::isoyear(dmy(Date)))%>%dplyr::summarise(nsites=length(unique(LawaSiteID)))%>%ungroup
names(AmmonSitesPerYear)=c("Year","nSites")
NOFSummaryTable$AmmoniacalMed_BandF=factor(as.character(NOFSummaryTable$AmmoniacalMed_Band),levels=rev(c("A","B","C","D","NA")),labels=rev(c("A","B","C","D","NA")))
NOFSummaryTable$AmmoniacalMed_BandF[is.na(NOFSummaryTable$AmmoniacalMed_Band)] <- "NA"

par(mfrow=c(3,1))
AmmoniacalEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,AmmoniacalMed_BandF)%>%
  # drop_na(AmmoniacalMed_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,AmmoniacalMed_BandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(AmmoniacalMed_BandF))%>%t%>%data.frame
rnames=rownames(AmmoniacalEvolutionF)
names(AmmoniacalEvolutionF)=sapply(AmmoniacalEvolutionF[1,],as.character)
AmmoniacalEvolutionF=AmmoniacalEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(AmmoniacalEvolutionF) <- rnames[-1]
areaplot(AmmoniacalEvolutionF,xlab='Year',
         col=rev(c("#aaaaaaFF","#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),main='Median Ammonia',ylab='Count of sites')
lines(AmmonSitesPerYear$Year[-c(1:4)],AmmonSitesPerYear$nSites[-c(1:4)],lwd=2)
text(AmmonSitesPerYear$Year[5],AmmonSitesPerYear$nSites[5]*0.9,"total sites available",cex=1,pos = 4)

AmmoniacalEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,AmmoniacalMed_Band)%>%
  drop_na(AmmoniacalMed_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,AmmoniacalMed_Band,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange((AmmoniacalMed_Band))%>%t%>%data.frame
rnames=rownames(AmmoniacalEvolution)
names(AmmoniacalEvolution)=sapply(AmmoniacalEvolution[1,],as.character)
AmmoniacalEvolution=AmmoniacalEvolution[-1,]%>%apply(2,as.numeric)
rownames(AmmoniacalEvolution) <- rnames[-1]
areaplot(AmmoniacalEvolution,col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),ylim=c(0,1.1),main='Median Ammonia',ylab='Proportion of sites')
text(AmmonSitesPerYear$Year[-c(1:4)],1.05,AmmonSitesPerYear$nSites[-c(1:4)],lwd=2)

bp=barplot(t(AmmoniacalEvolution),col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
           legend=T,args.legend=list(x='bottomleft',inset=0.04),ylim=c(0,1.1),main='Median Ammonia',ylab='Proportion of sites')
text(bp,1.05,AmmonSitesPerYear$nSites[-c(1:4)],lwd=2)
if(names(dev.cur())=='tiff'){dev.off()}




tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendAmmoniacal95.tif"),
     width = 8,height=12,units='in',res=300,compression='lzw',type='cairo')
AmmonSitesPerYear = wqdata%>%filter(Measurement=="NH4")%>%filter(LawaSiteID%in%NOFSummaryTable$LawaSiteID)%>%
  group_by(lubridate::isoyear(dmy(Date)))%>%dplyr::summarise(nsites=length(unique(LawaSiteID)))%>%ungroup
names(AmmonSitesPerYear)=c("Year","nSites")
NOFSummaryTable$Ammoniacal95_BandF=factor(as.character(NOFSummaryTable$Ammoniacal95_Band),levels=rev(c("A","B","C","D","NA")),labels=rev(c("A","B","C","D","NA")))
NOFSummaryTable$Ammoniacal95_BandF[is.na(NOFSummaryTable$Ammoniacal95_Band)] <- "NA"

par(mfrow=c(3,1))
AmmoniacalEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,Ammoniacal95_BandF)%>%
  # drop_na(Ammoniacal95_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,Ammoniacal95_BandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(Ammoniacal95_BandF))%>%t%>%data.frame
rnames=rownames(AmmoniacalEvolutionF)
names(AmmoniacalEvolutionF)=sapply(AmmoniacalEvolutionF[1,],as.character)
AmmoniacalEvolutionF=AmmoniacalEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(AmmoniacalEvolutionF) <- rnames[-1]
areaplot(AmmoniacalEvolutionF,xlab='Year',
         col=rev(c("#aaaaaaFF","#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),main='95th %ile Ammonia',ylab='Count of sites')
lines(AmmonSitesPerYear$Year[-c(1:4)],AmmonSitesPerYear$nSites[-c(1:4)],lwd=2)
text(AmmonSitesPerYear$Year[5],AmmonSitesPerYear$nSites[5]*0.9,"total sites available",cex=1,pos = 4)

AmmoniacalEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,Ammoniacal95_Band)%>%
  drop_na(Ammoniacal95_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,Ammoniacal95_Band,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange((Ammoniacal95_Band))%>%t%>%data.frame
rnames=rownames(AmmoniacalEvolution)
names(AmmoniacalEvolution)=sapply(AmmoniacalEvolution[1,],as.character)
AmmoniacalEvolution=AmmoniacalEvolution[-1,]%>%apply(2,as.numeric)
rownames(AmmoniacalEvolution) <- rnames[-1]
areaplot(AmmoniacalEvolution,col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),ylim=c(0,1.1),main='95th %ile Ammonia',ylab='Proportion of sites')
text(AmmonSitesPerYear$Year[-c(1:4)],1.05,AmmonSitesPerYear$nSites[-c(1:4)],lwd=2)

bp=barplot(t(AmmoniacalEvolution),col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
           legend=T,args.legend=list(x='bottomleft',inset=0.04),ylim=c(0,1.1),main='95th %ile Ammonia',ylab='Proportion of sites')
text(bp,1.05,AmmonSitesPerYear$nSites[-c(1:4)],lwd=2)
if(names(dev.cur())=='tiff'){dev.off()}

tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendAmmoniacalTox.tif"),
     width = 8,height=12,units='in',res=300,compression='lzw',type='cairo')
AmmonSitesPerYear = wqdata%>%filter(Measurement=="NH4")%>%filter(LawaSiteID%in%NOFSummaryTable$LawaSiteID)%>%
  group_by(lubridate::isoyear(dmy(Date)))%>%dplyr::summarise(nsites=length(unique(LawaSiteID)))%>%ungroup
names(AmmonSitesPerYear)=c("Year","nSites")
NOFSummaryTable$Ammonia_Toxicity_BandF=factor(as.character(NOFSummaryTable$Ammonia_Toxicity_Band),levels=rev(c("A","B","C","D","NA")),labels=rev(c("A","B","C","D","NA")))
NOFSummaryTable$Ammonia_Toxicity_BandF[is.na(NOFSummaryTable$Ammonia_Toxicity_Band)] <- "NA"

par(mfrow=c(3,1))
AmmoniacalEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,Ammonia_Toxicity_BandF)%>%
  # drop_na(Ammonia_Toxicity_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,Ammonia_Toxicity_BandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(Ammonia_Toxicity_BandF))%>%t%>%data.frame
rnames=rownames(AmmoniacalEvolutionF)
names(AmmoniacalEvolutionF)=sapply(AmmoniacalEvolutionF[1,],as.character)
AmmoniacalEvolutionF=AmmoniacalEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(AmmoniacalEvolutionF) <- rnames[-1]
areaplot(AmmoniacalEvolutionF,xlab='Year',
         col=rev(c("#aaaaaaFF","#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),main='Ammonia toxicity',ylab='Count of sites')
lines(AmmonSitesPerYear$Year[-c(1:4)],AmmonSitesPerYear$nSites[-c(1:4)],lwd=2)
text(AmmonSitesPerYear$Year[5],AmmonSitesPerYear$nSites[5]*0.9,"total sites available",cex=1,pos = 4)

AmmoniacalEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,Ammonia_Toxicity_Band)%>%
  drop_na(Ammonia_Toxicity_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,Ammonia_Toxicity_Band,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange((Ammonia_Toxicity_Band))%>%t%>%data.frame
rnames=rownames(AmmoniacalEvolution)
names(AmmoniacalEvolution)=sapply(AmmoniacalEvolution[1,],as.character)
AmmoniacalEvolution=AmmoniacalEvolution[-1,]%>%apply(2,as.numeric)
rownames(AmmoniacalEvolution) <- rnames[-1]
areaplot(AmmoniacalEvolution,col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),ylim=c(0,1.1),main='Ammonia toxicity',ylab='Proportion of sites')
text(AmmonSitesPerYear$Year[-c(1:4)],1.05,AmmonSitesPerYear$nSites[-c(1:4)],lwd=2)

bp=barplot(t(AmmoniacalEvolution),col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF")),
           legend=T,args.legend=list(x='bottomleft',inset=0.04),ylim=c(0,1.1),main='Ammonia toxicity',ylab='Proportion of sites')
text(bp,1.05,AmmonSitesPerYear$nSites[-c(1:4)],lwd=2)
if(names(dev.cur())=='tiff'){dev.off()}

rm(AmmonSitesPerYear,AmmoniacalEvolution,AmmoniacalEvolutionF)





tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendEcoliMed.tif"),
     width = 8,height=12,units='in',res=300,compression='lzw',type='cairo')
EcoliSitesPerYear = wqdata%>%filter(Measurement=="ECOLI")%>%filter(LawaSiteID%in%NOFSummaryTable$LawaSiteID)%>%
  group_by(lubridate::isoyear(dmy(Date)))%>%dplyr::summarise(nsites=length(unique(LawaSiteID)))%>%ungroup
names(EcoliSitesPerYear)=c("Year","nSites")
NOFSummaryTable$EcoliMed_BandF=factor(as.character(NOFSummaryTable$EcoliMed_Band),levels=rev(c("ABC","DE","D","NA")),labels=rev(c("ABC","DE","D","NA")))
NOFSummaryTable$EcoliMed_BandF[is.na(NOFSummaryTable$EcoliMed_Band)] <- "NA"

par(mfrow=c(3,1))
EColiEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,EcoliMed_BandF)%>%
  # drop_na(EcoliMed_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,EcoliMed_BandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(EcoliMed_BandF))%>%t%>%data.frame
rnames=rownames(EColiEvolutionF)
names(EColiEvolutionF)=sapply(EColiEvolutionF[1,],as.character)
EColiEvolutionF=EColiEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(EColiEvolutionF) <- rnames[-1]
areaplot(EColiEvolutionF,xlab='Year',
         col=rev(c("#aaaaaaFF","#dd1111FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),main='Median E. coli',ylab='Count of sites')
lines(EcoliSitesPerYear$Year[-c(1:4)],EcoliSitesPerYear$nSites[-c(1:4)],lwd=2)
text(EcoliSitesPerYear$Year[5],EcoliSitesPerYear$nSites[5]*0.9,"total sites available",cex=1,pos = 4)

EColiEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,EcoliMed_Band)%>%
  drop_na(EcoliMed_Band)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,EcoliMed_Band,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange((EcoliMed_Band))%>%t%>%data.frame
rnames=rownames(EColiEvolution)
names(EColiEvolution)=sapply(EColiEvolution[1,],as.character)
EColiEvolution=EColiEvolution[-1,]%>%apply(2,as.numeric)
rownames(EColiEvolution) <- rnames[-1]
areaplot(EColiEvolution,col=rev(c("#dd1111FF","#55bb66FF","#008800FF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),ylim=c(0,1.1),main='Median E. coli',ylab='Proportion of sites')
text(EcoliSitesPerYear$Year[-c(1:4)],1.05,EcoliSitesPerYear$nSites[-c(1:4)],lwd=2)

bp=barplot(t(EColiEvolution),col=rev(c("#dd1111FF","#55bb66FF","#008800FF")),
           legend=T,args.legend=list(x='bottomleft',inset=0.04),ylim=c(0,1.1),main='Median E. coli',ylab='Proportion of sites')
text(bp,1.05,EcoliSitesPerYear$nSites[-c(1:4)],lwd=2)
if(names(dev.cur())=='tiff'){dev.off()}

rm(EcoliSitesPerYear,EColiEvolution,EColiEvolutionF)



tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendEcoliSummary.tif"),
     width = 8,height=12,units='in',res=300,compression='lzw',type='cairo')
EcoliSitesPerYear = wqdata%>%filter(Measurement=="ECOLI")%>%filter(LawaSiteID%in%NOFSummaryTable$LawaSiteID)%>%
  group_by(lubridate::isoyear(dmy(Date)))%>%dplyr::summarise(nsites=length(unique(LawaSiteID)))%>%ungroup
names(EcoliSitesPerYear)=c("Year","nSites")
NOFSummaryTable$EcoliSummarybandF=factor(as.character(NOFSummaryTable$EcoliSummaryband),levels=rev(c("A","B","C","D","E","NA")),labels=rev(c("A","B","C","D","E","NA")))
NOFSummaryTable$EcoliSummarybandF[is.na(NOFSummaryTable$EcoliSummaryband)] <- "NA"

par(mfrow=c(3,1))
EColiEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,EcoliSummarybandF)%>%
  # drop_na(EcoliSummaryband)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,EcoliSummarybandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(EcoliSummarybandF))%>%t%>%data.frame
rnames=rownames(EColiEvolutionF)
names(EColiEvolutionF)=sapply(EColiEvolutionF[1,],as.character)
EColiEvolutionF=EColiEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(EColiEvolutionF) <- rnames[-1]
areaplot(EColiEvolutionF,xlab='Year',
         col=rev(c("#aaaaaaFF","#dd1111FF","#cc7766FF","#55bb66FF","#008800FF","#00CCCCFF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),main='E. coli summary',ylab='Count of sites')
lines(EcoliSitesPerYear$Year[-c(1:4)],EcoliSitesPerYear$nSites[-c(1:4)],lwd=2)
text(EcoliSitesPerYear$Year[5],EcoliSitesPerYear$nSites[5]*0.9,"total sites available",cex=1,pos = 4)

EColiEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,EcoliSummaryband)%>%
  drop_na(EcoliSummaryband)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,EcoliSummaryband,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange((EcoliSummaryband))%>%t%>%data.frame
rnames=rownames(EColiEvolution)
names(EColiEvolution)=sapply(EColiEvolution[1,],as.character)
EColiEvolution=EColiEvolution[-1,]%>%apply(2,as.numeric)
rownames(EColiEvolution) <- rnames[-1]
areaplot(EColiEvolution,col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF","#00CCCCFF")),
         legend=T,args.legend=list(x='bottomleft',inset=0.05),ylim=c(0,1.1),main='E. coli summary',ylab='Proportion of sites')
text(EcoliSitesPerYear$Year[-c(1:4)],1.05,EcoliSitesPerYear$nSites[-c(1:4)],lwd=2)

bp=barplot(t(EColiEvolution),col=rev(c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF","#00CCCCFF")),
           legend=T,args.legend=list(x='bottomleft',inset=0.04),ylim=c(0,1.1),main='E. coli summary',ylab='Proportion of sites')
text(bp,1.05,EcoliSitesPerYear$nSites[-c(1:4)],lwd=2)
if(names(dev.cur())=='tiff'){dev.off()}

rm(EcoliSitesPerYear,EColiEvolution,EColiEvolutionF)


tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendEcoliMed.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
NOFSummaryTable$EcoliSummarybandF=factor(as.character(NOFSummaryTable$EcoliMed_band),levels=c("A","B","C","D","E","NA"),labels=c("A","B","C","D","E","NA"))
NOFSummaryTable$EcoliSummarybandF[is.na(NOFSummaryTable$EcoliMed_band)] <- "NA"
par(mfrow=c(2,1))
EColiEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,EcoliSummarybandF)%>%
  # drop_na(EcoliSummaryband)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,EcoliSummarybandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(EcoliSummarybandF))%>%t%>%data.frame
rnames=rownames(EColiEvolutionF)
names(EColiEvolutionF)=sapply(EColiEvolutionF[1,],as.character)
EColiEvolutionF=EColiEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(EColiEvolutionF) <- rnames[-1]
areaplot(EColiEvolutionF,xlab='Year',
         col=c("#aaaaaaFF","#dd1111FF","#cc7766FF","#55bb66FF","#008800FF","#3388DDFF"),legend=T,args.legend=list(x='bottomleft'))
EColiEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,EcoliSummaryband)%>%
  drop_na(EcoliSummaryband)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,EcoliSummaryband,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange(desc(EcoliSummaryband))%>%t%>%data.frame
rnames=rownames(EColiEvolution)
names(EColiEvolution)=sapply(EColiEvolution[1,],as.character)
EColiEvolution=EColiEvolution[-1,]%>%apply(2,as.numeric)
rownames(EColiEvolution) <- rnames[-1]
areaplot(EColiEvolution,col=c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF","#3388DDFF"),legend=T,args.legend=list(x='bottomleft'))
if(names(dev.cur())=='tiff'){dev.off()}



tiff(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendEcoliSummary.tif"),
     width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
NOFSummaryTable$EcoliSummarybandF=factor(as.character(NOFSummaryTable$EcoliSummaryband),levels=c("A","B","C","D","E","NA"),labels=c("A","B","C","D","E","NA"))
NOFSummaryTable$EcoliSummarybandF[is.na(NOFSummaryTable$EcoliSummaryband)] <- "NA"
par(mfrow=c(2,1))
EColiEvolutionF <- NOFSummaryTable%>%
  dplyr::select(Year,EcoliSummarybandF)%>%
  # drop_na(EcoliSummaryband)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,EcoliSummarybandF,.drop = F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  pivot_wider(names_from = c("Year"),values_from = 'count')%>%arrange(desc(EcoliSummarybandF))%>%t%>%data.frame
rnames=rownames(EColiEvolutionF)
names(EColiEvolutionF)=sapply(EColiEvolutionF[1,],as.character)
EColiEvolutionF=EColiEvolutionF[-1,]%>%apply(2,as.numeric)
rownames(EColiEvolutionF) <- rnames[-1]
areaplot(EColiEvolutionF,xlab='Year',
         col=c("#aaaaaaFF","#dd1111FF","#cc7766FF","#55bb66FF","#008800FF","#3388DDFF"),legend=T,args.legend=list(x='bottomleft'))
EColiEvolution <- NOFSummaryTable%>%
  dplyr::select(Year,EcoliSummaryband)%>%
  drop_na(EcoliSummaryband)%>%
  mutate(Year=strFrom(Year,'to'))%>%
  group_by(Year,EcoliSummaryband,.drop=F)%>%
  dplyr::summarise(count=n())%>%
  ungroup%>%
  group_by(Year,.drop=F)%>%
  dplyr::mutate(tot=sum(count),prop=count/tot)%>%
  select(-count,-tot)%>%
  pivot_wider(names_from = c("Year"),values_from = 'prop',values_fill = list(prop=0))%>%
  arrange(desc(EcoliSummaryband))%>%t%>%data.frame
rnames=rownames(EColiEvolution)
names(EColiEvolution)=sapply(EColiEvolution[1,],as.character)
EColiEvolution=EColiEvolution[-1,]%>%apply(2,as.numeric)
rownames(EColiEvolution) <- rnames[-1]
areaplot(EColiEvolution,col=c("#dd1111FF","#cc7766FF","#55bb66FF","#008800FF","#3388DDFF"),legend=T,args.legend=list(x='bottomleft'))
if(names(dev.cur())=='tiff'){dev.off()}






names(NOFSummaryTable)[grepl('band',names(NOFSummaryTable),T)]


