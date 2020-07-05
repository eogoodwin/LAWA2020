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
setwd("h:/ericg/16666LAWA/LAWA2020/WaterQuality/")
source("h:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R")
source("h:/ericg/16666LAWA/LAWA2020/WaterQuality/scripts/SWQ_NOF_Functions.R")
try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),'%Y-%m-%d'))))
riverSiteTable=loadLatestSiteTableRiver()

## Load NOF Bands
NOFbandDefinitions <- read.csv("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/NOFbandDefinitions3.csv", header = TRUE, stringsAsFactors=FALSE)
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
  yr <- c(as.character(firstYear:EndYear),paste0(as.character(firstYear:(EndYear-4)),'to',as.character((firstYear+4):EndYear)))
  reps <- length(yr)
  # wqdata <- wqdata[which((wqdYear>=StartYear5 & wqdYear<=EndYear)|
  #                          (wqdYear>=(StartYear5-1) & wqdYear<=EndYear & wqdata$Measurement=='ECOLI')),]
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
    wqdata_A <- wqdata_A%>%select(-lcenrep)
    rm(lcenreps)
  }
  if(any(wqdata_A$CenType=='Right')){
    wqdata_A$Value[wqdata_A$CenType=="Right"] <- wqdata_A$Value[wqdata_A$CenType=="Right"]*1.1
    options(warn=-1)
    rcenreps=wqdata_A%>%split(.$CouncilSiteID)%>%purrr::map(~min(.$Value[.$CenType=="Right"],na.rm=T))
    options(warn=0)
    wqdata_A$rcenrep=as.numeric(rcenreps[match(wqdata_A$CouncilSiteID,names(rcenreps))])
    wqdata_A$Value[wqdata_A$CenType=='Right'] <- wqdata_A$rcenrep[wqdata_A$CenType=='Right']
    wqdata_A <- wqdata_A%>%select(-rcenrep)
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


# Saving the wqdataPerDateMedian table to be USED in NOF calculations. 
# NOTE AFTER HAVING OUT-COMMENTED THE 51/52 LINE, WE'VE NOW GOT ALL YEARS, FOR THE ROLLING NOF 
# Has six years available for ECOli if necessary
try(dir.create(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/", format(Sys.Date(),"%Y-%m-%d"))))
save(wqdataPerDateMedian,file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),
                          "/wqdataPerDateMedian",StartYear5,"-",EndYear,"ec6.RData"))
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",pattern='wqdataPerDateMedian',
                                  recursive = T,full.names=T,ignore.case=T),1),verbose = T)
# Subset to just have the variables that are tested against NOF standards
sub_swq <- wqdataPerDateMedian%>%dplyr::select(c("LawaSiteID","CouncilSiteID","Date","Measurement","Value"))%>%
  dplyr::filter(tolower(Measurement)%in%tolower(c("NH4","TON","ECOLI","PH")))%>%
  dplyr::filter(lubridate::isoyear(Date)<2020)
#993860 - 463433

table(lubridate::isoyear(sub_swq$Date),sub_swq$Measurement)
#       ECOLI   NH4    PH   TON
# 2005  4785  4916  4277  3782
# 2006  5331  5848  5219  4653
# 2007  6012  6365  5802  5175
# 2008  6654  6994  6234  5843
# 2009  7181  7474  6993  6357
# 2010  7113  7435  6863  6346
# 2011  7553  7741  7403  6690
# 2012  7807  7974  7794  7079
# 2013  9645  8717  8841  8077
# 2014 10184  9182  9199  8560
# 2015 10626  9586  9692  9217
# 2016 10921  9790  9733  9603
# 2017 11139 10037  9786  9823
# 2018 11108  9897  9521  9989
# 2019  7738  6359  6338  6432

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
                         NitrateMed_Band          = rep(as.character(NA)),#factor(rep(NA,reps),levels = c("A","B","C","D")),
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
    
    Com_NOF$NitrateMed[yr%in%names(rollingMeds)] <- as.numeric(rollingMeds)
    Com_NOF$NitrateAnalysisNote[is.na(as.numeric(rollingMeds))] <- paste0(Com_NOF$NitrateAnalysisNote[is.na(as.numeric(rollingMeds))],
                                                                          ' Need 30 values for 5yr median, have',
                                                                          strFrom(s = rollingMeds[is.na(as.numeric(rollingMeds))],c = 'y'))
    #find the band which each value belong to
    Com_NOF$NitrateMed_Band <- sapply(Com_NOF$NitrateMed,NOF_FindBand,bandColumn = NOFbandDefinitions$Median.Nitrate)
    Com_NOF$NitrateMed_Band[!is.na(Com_NOF$NitrateMed_Band)] <- 
      sapply(Com_NOF$NitrateMed_Band[!is.na(Com_NOF$NitrateMed_Band)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(annualMedian,rollingMeds)
    
    #95th percentile Nitrate
    annual95 <- tonsite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.95,type=5,na.rm=T))
    
    Com_NOF$Nitrate95 = annual95$value[match(Com_NOF$Year,annual95$Year)]
    #Rolling 5yr 95%ile
    rolling95=rolling5(tonsite,0.95)
    Com_NOF$Nitrate95[yr%in%names(rolling95)] <- as.numeric(rolling95)
    Com_NOF$NitrateAnalysisNote[is.na(as.numeric(rolling95))] <- paste0(Com_NOF$NitrateAnalysisNote[is.na(as.numeric(rolling95))],
                                                                          ' Need 30 values for 5yr 95%ile, have',
                                                                          strFrom(s = rolling95[is.na(as.numeric(rolling95))],c = 'y'))
    #find the band which each value belong to
    Com_NOF$Nitrate95_Band <- sapply(Com_NOF$Nitrate95,NOF_FindBand,bandColumn = NOFbandDefinitions$X95th.Percentile.Nitrate)
    Com_NOF$Nitrate95_Band[!is.na(Com_NOF$Nitrate95_Band)] <- 
      sapply(Com_NOF$Nitrate95_Band[!is.na(Com_NOF$Nitrate95_Band)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
    
    #Nitrate Toxicity
    #The worse of the two nitrate bands
    Com_NOF$Nitrate_Toxicity_Band = apply(Com_NOF%>%select(NitrateMed_Band, Nitrate95_Band),1,max,na.rm=T)
    rm(annual95,rolling95)
  }else{
    Com_NOF$NitrateAnalysisNote = paste0(Com_NOF$NitrateAnalysisNote,'n = ',sum(!is.na(tonsite$Value)),
                                         ' Insufficient  to calculate annual medians ')
  }
  rm(tonsite)
  
  ###################### Ammonia  ############################
  nh4site=rightSite%>%dplyr::filter(Measurement=="NH4adj")
  if(all(nh4site$Value==-99)){
    Com_NOF$AmmoniaAnalysisNote=paste0(Com_NOF$AmmoniaAnalysisNote,'n = ',sum(!is.na(nh4site$Value)),
                                       ' No pH data available for NH4 adjustment, so NH4 cannot be judged against NOF standards. ')
  }else{
    nh4site=nh4site[!(nh4site$Value==(-99)),]
    #Median Ammoniacal Nitrogen
    annualMedian <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))
    if(dim(annualMedian)[1]!=0){
      Com_NOF$AmmoniacalMed = annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
      #Rolling 5yr median
      rollingMeds=rolling5(nh4site,0.5)
      Com_NOF$AmmoniacalMed[yr%in%names(rollingMeds)] <- as.numeric(rollingMeds)
      Com_NOF$AmmoniaAnalysisNote[is.na(as.numeric(rollingMeds))] <- paste0(Com_NOF$AmmoniaAnalysisNote[is.na(as.numeric(rollingMeds))],
                                                                            ' Need 30 values for 5yr median, have',
                                                                            strFrom(s = rollingMeds[is.na(as.numeric(rollingMeds))],c = 'y'))      
      Com_NOF$AmmoniacalMed_Band <- sapply(Com_NOF$AmmoniacalMed,NOF_FindBand,bandColumn=NOFbandDefinitions$Median.Ammoniacal.N) 
      Com_NOF$AmmoniacalMed_Band[!is.na(Com_NOF$AmmoniacalMed_Band)] <- 
        sapply(Com_NOF$AmmoniacalMed_Band[!is.na(Com_NOF$AmmoniacalMed_Band)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
      rm(annualMedian,rollingMeds)
      
      #95th percentile Ammoniacal Nitrogen
      annual95 <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.95,type=5,na.rm=T))
      Com_NOF$Ammoniacal95 <- annual95$value[match(Com_NOF$Year,annual95$Year)]
 
      #Rolling 5yr 95%ile
      rolling95=rolling5(nh4site,0.95)
      Com_NOF$Ammoniacal95[yr%in%names(rolling95)] <- as.numeric(rolling95)
      Com_NOF$AmmoniaAnalysisNote[is.na(as.numeric(rolling95))] <- paste0(Com_NOF$AmmoniaAnalysisNote[is.na(as.numeric(rolling95))],
                                                                            ' Need 30 values for 5yr 95%ile, have',
                                                                            strFrom(s = rolling95[is.na(as.numeric(rolling95))],c = 'y'))
      Com_NOF$Ammoniacal95_Band <-sapply(Com_NOF$Ammoniacal95,NOF_FindBand,bandColumn=NOFbandDefinitions$Max.Ammoniacal.N)
      Com_NOF$Ammoniacal95_Band <- sapply(Com_NOF$Ammoniacal95_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
      
      #Ammonia Toxicity
      Com_NOF$Ammonia_Toxicity_Band=apply(Com_NOF%>%select(AmmoniacalMed_Band, Ammoniacal95_Band),1,max,na.rm=T)
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
    Com_NOF$EcoliMed[yr%in%names(rollingMeds)] = readr::parse_number(rollingMeds)
    options(warn=-1) #if I'm testing to see if somethign's NA when I caast it to numeric, I dont want a warning that some of them are NA
    Com_NOF$EcoliPeriod[yr%in%names(rollingMeds)] = ifelse(is.na(as.numeric(rollingMeds)),NA,ifelse(grepl(pattern = '_6',rollingMeds),6,5))
    options(warn=0)
    #bands
    Com_NOF$EcoliMed_Band <- sapply(Com_NOF$EcoliMed,NOF_FindBand,bandColumn=NOFbandDefinitions$E..coli)
    #Median EColi can meet multiple bands. cnec finds the lowest
    # suppressWarnings(cnEc_Band <- sapply(Com_NOF$EcoliMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
  }else{
    Com_NOF$EcoliAnalysisNote=paste0(Com_NOF$EcoliAnalysisNote,"n = ",sum(!is.na(ecosite$Value)),
                                     " Insufficient data to calculate annual medians")
  }
  rm(annualMedian,rollingMeds)
  
  #Ecoli 95th percentile 
  annual95 <- ecosite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.95,type=5,na.rm=T))
  # annual95 <- tapply(ecosite$Value,format(ecosite$Date, '%Y'),na.rm=TRUE, quantile,prob=c(0.95),type=5)
  if(length(annual95)!=0){
    Com_NOF$Ecoli95 <- annual95$value[match(Com_NOF$Year,annual95$Year)]
    #rolling 5yr or 6yr 95%ile
    rolling95 = rolling5(ecosite,0.95,extendToSix = T,nreq=60)
    Com_NOF$Ecoli95[yr%in%names(rolling95)] <- readr::parse_number(rolling95)
    
    #bands 
    Com_NOF$Ecoli95_Band <- sapply(Com_NOF$Ecoli95,NOF_FindBand,bandColumn=NOFbandDefinitions$Ecoli95)
    #Ecoli95 can meet multiple bands
    # suppressWarnings(cnEc95_Band <- sapply(Com_NOF$Ecoli95_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
  }else{
    Com_NOF$EcoliAnalysisNote=paste0(Com_NOF$EcoliAnalysisNote,"n = ",sum(!is.na(ecosite$Value)),
                                      " Insufficient data to calculate annual max")
  }
  rm(annual95,rolling95)
  
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

  #These contain the best case out of these scorings, the worst of which contributes.
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
suppressWarnings(cnEc_Band <- sapply(NOFSummaryTable$EcoliMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEc95_Band <- sapply(NOFSummaryTable$Ecoli95_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth540_Band <- sapply(NOFSummaryTable$EcoliRecHealth540_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth260_Band <- sapply(NOFSummaryTable$EcoliRecHealth260_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))  

if(all(sapply(c("cnEc_Band","cnEc95_Band","cnEcRecHealth540_Band","cnEcRecHealth260_Band"),exists))){
  NOFSummaryTable$EcoliSummaryband = as.character(apply(cbind(pmax(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band)),1,max))
}
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
NOFSummaryTable <- NOFSummaryTable%>%select(LawaSiteID,CouncilSiteID,SiteID,Year:EcoliAnalysisNote)
NOFSummaryTable <- merge(NOFSummaryTable, riverSiteTable) 

#############################Save the output table ############################
#For audit
write.csv(NOFSummaryTable%>%filter(grepl(pattern = 'to',x = Year)),
          file = paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                        "/NOFSummaryTable_Rolling.csv"),row.names=F)
# NOFSummaryTable <- read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",pattern="NOFSummaryTable",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)
NOFSummaryTable$SWQAltitude=pseudo.titlecase(NOFSummaryTable$SWQAltitude)
NOFSummaryTable$SWQLanduse=pseudo.titlecase(NOFSummaryTable$SWQLanduse)

#For ITE
#Make outputs for ITE
# Reshape Output
RiverNOF <-
  NOFSummaryTable%>%
  dplyr::filter(grepl(pattern = 'to',x = Year))%>%
  #And now we're not doing the nitrate ones!  Because, toxicity!
  dplyr::select(-NitrateMed,-NitrateMed_Band,-Nitrate95,-Nitrate95_Band,-Nitrate_Toxicity_Band,-NitrateAnalysisNote)%>%
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



# 
# 
# write.csv(NOFSummaryTableOverall, file = paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
#                                                format(Sys.Date(),"%Y-%m-%d"),"/NOFSummaryTable_Overall.csv"),row.names=F)
# rm(NOFSummaryTableOverall)
# 
# write.csv(NOFSummaryTableLong, file = paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
#                                              format(Sys.Date(),"%Y-%m-%d"),"/NOFSummaryTableLong.csv"),row.names=F)
# 
# write.csv(NOFSummaryTableLongOverall, file = paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
#                                                    format(Sys.Date(),"%Y-%m-%d"),"/NOF_STATE_",EndYear,".csv"),row.names=F)
# 
# #Round them off.  For web display?
# NOFRound <- NOFSummaryTableLongOverall
# NOFRound$variable <- as.character(NOFRound$variable)
# 
# variables<-as.character(unique(NOFSummaryTableLongOverall$variable))
# variables <- variables[order(variables)]
# # [1] "Ammonia_Toxicity_Band"   "AmmoniaAnalysisNote"     "E_coli_Median_Band"             "E_coli_Median"           "E_coli_Period"          
# # [6] "E_coli95"                "E_coli95_Band"           "E_coliAnalysisNote"      "E_coliRecHealth260"      "E_coliRecHealth260_Band"
# # [11] "E_coliRecHealth540"      "E_coliRecHealth540_Band" "E_coliSummaryband"       "Ammoniacal95"          "Ammoniacal95_Band"    
# # [16] "AmmoniacalMed_Band"     "NitrateMed_Band"        "AmmoniacalMed"       "NitrateMed"          "Nitrate_Toxicity_Band"  
# # [21] "NitrateAnalysisNote"     "Nitrate95"             "Nitrate95_Band"   
# 
# # Decimal places for variables
# dp <- rep(NA,length(variables))
# dp[variables%in%c("E_coli_Median", "E_coli95", "E_coliRecHealth260", "E_coliRecHealth540")] <- 0
# dp[variables%in%c("Ammoniacal95", "AmmoniacalMed", "NitrateMed", "Nitrate95")] <- 4
# 
# MeasurementInvolved <- variables
# MeasurementInvolved <- gsub(pattern = "Band",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "band",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "AnalysisNote",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "_Period",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "_$",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Med_",replacement = "Median_",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "E_coli_Median$",replacement = "E_coliMedian",x = MeasurementInvolved)
# 
# desc = rep('value',length(variables))
# desc[grepl(variables,pattern = 'band',ignore.case = T)] <- 'band'
# desc[grepl(variables,pattern = 'Note',ignore.case = T)] <- 'note'
# desc[grepl(variables,pattern = 'Period',ignore.case = T)] <- 'yearsUsed'
# # desc[variables%in%c("Agency", "SWQAltitude","Landcover","SiteID","CATCH_LBL","CatchID",
# #                     "CatchType","Comment","LAWA_CATCH","Region","SOE_FW_RIV",
# #                     "SWQFrequencyAll","SWQFrequencyLast5","SWQuality","TermReach")] <- 'meta'
# 
# dfp <- data.frame(variables,MeasurementInvolved,desc,dp,stringsAsFactors=FALSE,row.names=NULL)
# NOFRound <- merge(NOFRound,dfp,by.x="variable",by.y="variables",all=TRUE)
# 
# rm(variables,MeasurementInvolved,desc,dp)
# # POST PROCESSING NOF RESULTS
# # Round values to appropriate DP's
# 
# # Note that for rounding off a 5, the IEC 60559 standard is expected to be used, ‘go to the even digit’. Therefore round(0.5) is 0 and round(-1.5) is -2
# # This is not the desired behaviour here. It is expected that 0.5 rounds to 1, 2.5 rounds, to 3 and so on.
# # Therefore for all even values followed by exactly .5 needs to have a small number added (like 0.00001) to get them rounding in the right direction (unless there is 
# # a flag in the function that provides for this behaviour), or to redefine the round function. 
# # (see http://theobligatescientist.blogspot.co.nz/2010/02/r-i-still-love-you-but-i-hate-your.html)
# 
# # As all values are positive, we'll add a small number, related to the degree of rounding required.
# # If I was smarter, I would redefine the round function
# 
# 
# for(i in 1:length(dfp$variables)){
#   if(!is.na(dfp$dp[i])){
#     NOFRound$value[NOFRound$variable==dfp$variables[i]] <- as.character(as.numeric(NOFRound$value[NOFRound$variable==dfp$variables[i]]) + 0.000001)
#     NOFRound$value[NOFRound$variable==dfp$variables[i]] <- as.character(round(as.numeric(NOFRound$value[NOFRound$variable==dfp$variables[i]]),digits = dfp$dp[i]))
#   }
# }
# 
# 
# NOFRound$value[is.na(NOFRound$value)] <- "NA"
# NOFRound <- NOFRound[order(NOFRound$LawaSiteID,NOFRound$MeasurementInvolved,NOFRound$desc),]
# write.csv(NOFRound, file = paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
#                                   format(Sys.Date(),"%Y-%m-%d"),"/NOF_STATE_",EndYear,"_Rounded_NAs.csv"),row.names=F)
# 
# 
# 
# # Transform (tidyr::spread) data in NOFRound to the following form to supply to IT Effect
# # LawaSiteID,CouncilSiteID,Year,Measurement,value,Band
# # ARC-00001,44603,Overall,Ammoniacal95N,NA,NA
# # ARC-00001,44603,Overall,AmmoniacalMed,NA,NA
# # ARC-00001,44603,Overall,Median_Ecoli,28,A
# # ARC-00001,44603,Overall,NitrateMed,0.0079,A
# 
# NOF_value <- NOFRound%>%filter(desc=="value")%>%select("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","variable","value","MeasurementInvolved")
# names(NOF_value) <- c("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","Measurement","Value",'MeasurementInvolved')
# NOF_Band  <- NOFRound%>%filter(desc=="band")%>%select("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","variable","value","MeasurementInvolved")
# names(NOF_Band) <- c("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","BandingRule","BandScore",'MeasurementInvolved')
# 
# NOF_wide <- dplyr::left_join(NOF_Band,NOF_value,by = c("LawaSiteID","CouncilSiteID","SiteID","Agency", "Year", "MeasurementInvolved"))
# NOF_wide <- unique(NOF_wide)
# 
# write.csv(NOF_wide, file = paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
#                                   format(Sys.Date(),"%Y-%m-%d"),"/RiverWQ_NOF_forITE_",
#                                   format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
# 
# # NOF_wide=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis",pattern="RiverWQ_NOF_forITE_",recursive = T,full.names = T),1),stringsAsFactors = F)
