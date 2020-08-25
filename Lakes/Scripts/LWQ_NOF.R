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
setwd("h:/ericg/16666LAWA/LAWA2020/Lakes/")
source("h:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R")
source("h:/ericg/16666Lawa/LAWA2020/WaterQuality/scripts/SWQ_NOF_Functions.R")

try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d')),showWarnings = F))

#hydrological year runs e.g. 1 July 2017 to 30 June 2018, and is named like 2017/18

lakeSiteTable <- loadLatestSiteTableLakes(maxHistory = 90)



lakeSiteTable$LType=tolower(lakeSiteTable$LType)
table(lakeSiteTable$LType)

## Load NOF Bands
NOFbandDefinitions <- read.csv("H:/ericg/16666LAWA/LAWA2020/Lakes/Metadata/NOFlakeBandDefinitions.csv", header = TRUE, stringsAsFactors=FALSE)
#   Band TotalNitrogenseasonally.stratified.and.brackish TotalNitrogenpolymictic TotalPhosphorus Median.Ammoniacal.N Max.Ammoniacal.N E..coli Ecoli95 EcoliRec540 EcoliRec260  Clarity ChlAMedian    ChlAMax
# 1    A                                          x<=160                  x<=300           x<=10             x<=0.03          x<=0.05  x<=130  x<=540         x<5        x<20      x>7       x<=2      x<=10
# 2    B                                    x>160&x<=350            x>300&x<=500      x>10&x<=20      x>0.03&x<=0.24    x>0.05&x<=0.4  x<=130 x<=1000  x>=5&x<=10 x>=20&x<=30 x>3&x<=7   x>2&x<=5 x>10&x<=25
# 3    C                                    x>350&x<=750            x>500&x<=800      x>20&x<=50       x>0.24&x<=1.3     x>0.4&x<=2.2  x<=130 x<=1200 x>=10&x<=20 x>=20&x<=34 x>1&x<=3  x>5&x<=12 x>25&x<=60
# 4    D                                           x>750                   x>800            x>50              x>1.30            x>2.2   x>130  x>1200 x>=20&x<=30        x>34     x<=1       x>12       x>60
# 5    E                                           x>Inf                   x>Inf           x>Inf               x>Inf            x>Inf   x>260  x>1200        x>30        x>50   x<(-1)      x>Inf      x>Inf




#===================================================================================================
## Load LAWA Data
# loads lawaLakesdata dataframe  from LAWA_State.r  - has altered values from censoring, and calculated medians
lakesMonthlyMedians=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis",
                                      pattern="lakeMonthlyMedian.*csv",  #"ForITE"
                                      recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors=F)
stopifnot(min(lakesMonthlyMedians$Year,na.rm=T)>1990)
#Reference Dates
EndYear <- lubridate::isoyear(Sys.Date())-1
StartYear5 <- EndYear - 5 + 1
firstYear = min(lakesMonthlyMedians$Year,na.rm=T)
yr <- c(as.character(firstYear:EndYear),paste0(as.character(firstYear:(EndYear-4)),'to',as.character((firstYear+4):EndYear)))
rollyrs=which(grepl('to',yr))
reps <- length(yr)

lakesMonthlyMedians$Date=lubridate::dmy(paste0('1-',lakesMonthlyMedians$month,'-',lakesMonthlyMedians$Year))
lakesMonthlyMedians$Measurement=toupper(lakesMonthlyMedians$Measurement)

if(0){
# To run chl and clarity manually 
# #2 October 2018
# #Clarity bands
clarSub=lakesMonthlyMedians%>%dplyr::select(c("LawaSiteID","CouncilSiteID","Measurement","Value","Date"))%>%
  dplyr::filter(tolower(Measurement)=='secchi')
clarSub$Year=lubridate::isoyear(clarSub$Date)
# clarSub$hYear = clarSub$Year
# clarSub$hYear[lubridate::month(clarSub$Date)<7] = clarSub$hYear[lubridate::month(clarSub$Date)<7]+1
annualClar <- clarSub%>%dplyr::filter(Year>=StartYear5)%>%
  dplyr::group_by(LawaSiteID)%>%dplyr::mutate(count=n())%>%ungroup%>%
  dplyr::group_by(LawaSiteID,Year)%>%dplyr::summarise(medVal=median(Value,na.rm=T),count=unique(count))%>%filter(count>=30)
medianClar <- annualClar%>%dplyr::group_by(LawaSiteID)%>%dplyr::summarise(medClar=median(medVal,na.rm=T))%>%ungroup
medianClar$ClarityScore <- cut(medianClar$medClar,breaks=c(-10,1,3,7,Inf),labels=c('D','C','B','A'))
write.csv(medianClar,paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),"/LakesClarBandForITE_",
                            format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
# 
# ChlA bands
chlSub=lakesMonthlyMedians%>%dplyr::select(c("LawaSiteID","CouncilSiteID","Measurement","Value","Date"))%>%
  dplyr::filter(tolower(Measurement)=='chla')
chlSub$Year=lubridate::isoyear(chlSub$Date)
# chlSub$yYear=chlSub$Year
# chlSub$hYear[lubridate::month(chlSub$Date)<7] = chlSub$hYear[lubridate::month(chlSub$Date)<7]+1
annualChl <- chlSub%>%dplyr::filter(Year>=StartYear5)%>%
  dplyr::group_by(LawaSiteID)%>%dplyr::mutate(count=n())%>%ungroup%>%
  dplyr::group_by(LawaSiteID,Year)%>%dplyr::summarise(medVal=median(Value,na.rm=T),count=unique(count))%>%filter(count>=30)
medianChl <- annualChl%>%dplyr::group_by(LawaSiteID)%>%dplyr::summarise(medChl=median(medVal,na.rm=T))
medianChl$MedChlScore <- cut(medianChl$medChl,breaks=c(-1,2,5,12,Inf),labels=c('A','B','C','D'))
maxChl <- annualChl%>%dplyr::group_by(LawaSiteID)%>%dplyr::summarise(maxChl=max(medVal,na.rm=T))
maxChl$MaxChlScore <- cut(maxChl$maxChl,breaks=c(-10,10,25,60,Inf),labels=c('A','B','C','D'))
chlNOF=cbind(medianChl,maxChl[,-1])
chlNOF$overall=apply(chlNOF[,2:3],MARGIN = 1,FUN=max)
write.csv(chlNOF,paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),"/LakesChlBandForITE_",
                        format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
# 
# #/2 October 2018
# 
}




# Subset to just have the variables that are tested against NOF standards
sub_lwq <- lakesMonthlyMedians%>%dplyr::filter(Year>=StartYear5)%>%
  dplyr::select(c("LawaSiteID","CouncilSiteID","Measurement","Value","Date","Year"))%>%
  dplyr::filter(tolower(Measurement)%in%tolower(c("NH4N","TN","TP","ECOLI","PH","SECCHI","CHLA")))
sub_lwq$Measurement[sub_lwq$Measurement=="NH4N"] <- "NH4"




lakeSiteTable$uclid=paste(tolower(lakeSiteTable$LawaSiteID),tolower(lakeSiteTable$CouncilSiteID),sep='||')
sub_lwq$uclid      =paste(tolower(      sub_lwq$LawaSiteID),tolower(      sub_lwq$CouncilSiteID),sep='||')

# lakeSiteTable$uclid[which(!lakeSiteTable$uclid%in%sub_lwq$uclid)]=paste(lakeSiteTable$LawaSiteID,lakeSiteTable$SiteID,sep='||')[which(!lakeSiteTable$uclid%in%sub_lwq$uclid)]

uclids = unique(sub_lwq$uclid)

if(exists("NOFSummaryTable")) { rm("NOFSummaryTable") }

cat(length(uclids),'\t')
library(parallel)
library(doParallel)
workers=makeCluster(4)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(tidyverse)
})
startTime=Sys.time()
foreach(i = 1:length(uclids),.combine=rbind,.errorhandling='stop')%dopar%{
  suppressWarnings(rm(tnsite,tpsite,nh4site,ecosite,rightSite,value,Value)  )
  rightSite=sub_lwq[(sub_lwq$uclid==uclids[i]),]
  rightSite=rightSite[!is.na(rightSite$Value),]
  # create table of compliance with proposed National Objectives Framework
  Com_NOF <- data.frame (LawaSiteID               = rep(unique(rightSite$LawaSiteID),reps),
                         CouncilSiteID            = rep(unique(rightSite$CouncilSiteID),reps),
                         Year                     = yr,
                         NitrogenMed            = rep(as.numeric(NA),reps),
                         NitrogenMed_Band       = rep(as.character(NA),reps),
                         NitrateAnalysisNote      = rep('',reps),
                         PhosphorusMed          = rep(as.numeric(NA),reps),
                         PhosphorusMed_Band     = rep(as.character(NA),reps),
                         PhosphorusAnalysisNote   = rep('',reps),
                         AmmoniacalMed            = rep(as.numeric(NA),reps),
                         AmmoniacalMed_Band       = rep(as.character(NA),reps),
                         AmmoniacalMax            = rep(as.numeric(NA),reps),
                         AmmoniacalMax_Band       = rep(as.character(NA),reps),
                         Ammonia_Toxicity_Band    = rep(as.character(NA),reps),
                         AmmoniaAnalysisNote      = rep('',reps),
                         ClarityMedian            = rep(as.numeric(NA),reps),
                         ClarityMedian_Band       = rep(as.character(NA),reps),
                         ClarityAnalysisNote      = rep('',reps),
                         ChlAMed                  = rep(as.numeric(NA),reps),
                         ChlAMed_Band             = rep(as.character(NA),reps),
                         ChlAMax                  = rep(as.numeric(NA),reps),
                         ChlAMax_Band             = rep(as.character(NA),reps),
                         ChlASummaryBand          = rep(as.character(NA),reps),
                         ChlAAnalysisNote         = rep('',reps),
                         EcoliPeriod              = rep(as.numeric(NA),reps),
                         EcoliMedian              = rep(as.numeric(NA),reps),
                         EcoliBand                = rep(NA,reps),
                         Ecoli95                  = rep(as.numeric(NA),reps),
                         Ecoli95_Band             = rep(NA,reps),
                         EcoliRecHealth540        = rep(as.numeric(NA),reps),
                         EcoliRecHealth540_Band   = rep(NA,reps),
                         EcoliRecHealth260        = rep(as.numeric(NA),reps),
                         EcoliRecHealth260_Band   = rep(NA,reps),
                         EcoliSummaryBand         = rep(as.character(NA),reps),
                         EcoliAnalysisNote        = rep('',reps),
                         stringsAsFactors = FALSE)
  
  
  ######################  Nitrogen  ########################################
  tnsite=rightSite[rightSite$Measurement=="TN",]
  annualMedian <- tnsite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5))
  
  if(dim(annualMedian)[1]!=0){
    Com_NOF$NitrogenMed <- annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds=rolling5(siteChemSet=tnsite,quantProb=0.5)
    rollFails=is.na(as.numeric(rollingMeds))
    Com_NOF$NitrogenMed[yr%in%names(rollingMeds)] <- as.numeric(rollingMeds)
    Com_NOF$NitrateAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have ",
                                                              strFrom(s=rollingMeds[rollFails],c='y'))
    #find the Band which each value belong to
    if(lakeSiteTable$LType[which(lakeSiteTable$uclid==uclids[i])]%in%c("stratified","brackish","icoll","monomictic")){
      Com_NOF$NitrogenMed_Band <- sapply(Com_NOF$NitrogenMed,NOF_FindBand,bandColumn = NOFbandDefinitions$TotalNitrogenseasonally.stratified.and.brackish)
      Com_NOF$NitrogenMed_Band <- sapply(Com_NOF$NitrogenMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    }else{ #assuming polymictic
      Com_NOF$NitrogenMed_Band <- sapply(Com_NOF$NitrogenMed,NOF_FindBand,bandColumn = NOFbandDefinitions$TotalNitrogenpolymictic)
      Com_NOF$NitrogenMed_Band <- sapply(Com_NOF$NitrogenMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    }
    rm(rollingMeds,rollFails)
  }
  rm(tnsite,annualMedian)
  
  ######################  Phosphorus  ########################################
  tpsite=rightSite[rightSite$Measurement=="TP",]
  annualMedian <- tpsite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))
  
  if(dim(annualMedian)[1]!=0){
    Com_NOF$PhosphorusMed <- annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds=rolling5(siteChemSet=tpsite,quantProb=0.5)
    rollFails=is.na(as.numeric(rollingMeds))
    Com_NOF$PhosphorusMed[yr%in%names(rollingMeds)] <- as.numeric(rollingMeds)
    Com_NOF$PhosphorusAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have",
                                                                 strFrom(s=rollingMeds[rollFails],c='y'))
    #find the band which each value belong to
    Com_NOF$PhosphorusMed_Band <- sapply(Com_NOF$PhosphorusMed,NOF_FindBand,bandColumn = NOFbandDefinitions$TotalPhosphorus)
    Com_NOF$PhosphorusMed_Band <- sapply(Com_NOF$PhosphorusMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(rollingMeds,rollFails)
  }
  rm(tpsite,annualMedian)
  
  ###################### Ammonia  ############################
  nh4site=rightSite[rightSite$Measurement=="NH4",]
  if(dim(nh4site)[1]==0){
    Com_NOF$AmmoniaAnalysisNote='No NH4 data '
  }else{
    if(all(nh4site$Value==-99)){
      Com_NOF$AmmoniaAnalysisNote='No pH data available for NH4 adjustment, so NH4 cannot be judged against NOF standards.'
    }else{
      #Median Ammoniacal Nitrogen
      annualMedian <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5))
      if(dim(annualMedian)[1]!=0){
        Com_NOF$AmmoniacalMed = annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
        #Rolling 5yr median
        rollingMeds=rolling5(siteChemSet=nh4site,quantProb=0.5)
        rollFails=is.na(as.numeric(rollingMeds))
        Com_NOF$AmmoniacalMed[yr%in%names(rollingMeds)] <- as.numeric(rollingMeds)
        Com_NOF$AmmoniaAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have",
                                                                     strFrom(s=rollingMeds[rollFails],c='y'))
        Com_NOF$AmmoniacalMed_Band <- sapply(Com_NOF$AmmoniacalMed,NOF_FindBand,bandColumn=NOFbandDefinitions$Median.Ammoniacal.N)
        Com_NOF$AmmoniacalMed_Band <- sapply(Com_NOF$AmmoniacalMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
        rm(rollingMeds,rollFails)
        
        #-------maximum annual Ammoniacal Nitrogen--------------------------------------
        annualMax <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(value=max(Value,na.rm=T))
        Com_NOF$AmmoniacalMax <- annualMax$value[match(Com_NOF$Year,annualMax$Year)]
        
        #rolling 5yr max
        rollingMax=rolling5(nh4site,quantProb=1.0)
        rollFails=is.na(as.numeric(rollingMax))
        Com_NOF$AmmoniacalMax[yr%in%names(rollingMax)]=as.numeric(rollingMax)
        Com_NOF$AmmoniaAnalysisNote[rollyrs[rollFails]] <- paste0('Need 30 values for median, have ',
                                                                     strFrom(s=rollingMax[rollFails],c='y'))
        Com_NOF$AmmoniacalMax_Band <-sapply(Com_NOF$AmmoniacalMax,NOF_FindBand,bandColumn=NOFbandDefinitions$Max.Ammoniacal.N) 
        Com_NOF$AmmoniacalMax_Band <- sapply(Com_NOF$AmmoniacalMax_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
        rm(rollingMax,rollFails,annualMax)
        #------------------Finding the band for Ammonia Toxicity-------------------------------
        Com_NOF$Ammonia_Toxicity_Band=apply(select(Com_NOF,AmmoniacalMed_Band, AmmoniacalMax_Band),1,max,na.rm=T)
      }else{
        Com_NOF$AmmoniaAnalysisNote=paste0(Com_NOF$AmmoniaAnalysisNote,'n = ',sum(!is.na(nh4site$Value)),
                                           ' Insufficient to calculate annual medians. ')
      }
      rm(annualMedian)
    }
  }
  rm(nh4site)
  
  ######################  E.Coli #########################################
  suppressWarnings(rm(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band,ecosite,rawEcoli))
  ecosite=rightSite[rightSite$Measurement=="ECOLI",]
  # ecosite$year=lubridate::isoyear(ecosite$Date)
  # rawEcoli=ecosite%>%dplyr::select(year,Value)%>%filter(!is.na(Value)&year>=StartYear5)
  annualMedian <- ecosite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))  
  
  if(dim(annualMedian)[1]!=0){
    Com_NOF$EcoliMedian <- annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    Com_NOF$EcoliPeriod=ifelse(is.na(Com_NOF$EcoliMedian),NA,1)
    #rolling 5yr or 6yr median
    rollingMeds=rolling5(ecosite,0.5,extendToSix = T,nreq=60)
    rollFails=is.na(as.numeric(rollingMeds))
    Com_NOF$EcoliMedian[yr%in%names(rollingMeds)] = readr::parse_number(rollingMeds) #not "as.numeric", because good results include a years suffix
    Com_NOF$EcoliPeriod[yr%in%names(rollingMeds)] = ifelse(rollFails,NA,ifelse(grepl(pattern = '_6',rollingMeds),6,5))
    #bands
    Com_NOF$EcoliBand <- sapply(Com_NOF$EcoliMedian,NOF_FindBand,bandColumn=NOFbandDefinitions$E..coli)
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
  
  # Exceedance percentages
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
  if(length(these)>0){
    Com_NOF$EcoliAnalysisNote[these]=paste0(Com_NOF$EcoliAnalysisNote,
                                            ' Need ',rep(c(12,60),c(sum(!grepl('to',yr)),sum(grepl('to',yr)))),
                                            ' values for ',rep(c('annual','5yr'),c(sum(!grepl('to',yr)),sum(grepl('to',yr)))),
                                            ' have ',count5(ecosite,T))[these]
  }
  
  
  #################### Clarity ############
  clarsite=rightSite[rightSite$Measurement=="SECCHI",]
  annualMedian <- clarsite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))

  if(length(annualMedian)!=0){
    Com_NOF$ClarityMedian = annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds = rolling5(siteChemSet=clarsite,quantProb=0.5)
    rollFails=is.na(as.numeric(rollingMeds))
    Com_NOF$ClarityMedian[yr%in%names(rollingMeds)] <- as.numeric(rollingMeds)
    Com_NOF$ClarityAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have",strFrom(s=rollingMeds[rollFails],c='y'))

    #find the band which each value belong to
    Com_NOF$ClarityMedian_Band <- sapply(Com_NOF$ClarityMedian,NOF_FindBand,bandColumn=NOFbandDefinitions$Clarity)
    Com_NOF$ClarityMedian_Band <- sapply(Com_NOF$ClarityMedian_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(rollingMeds,rollFails)
  }  
  rm(clarsite,annualMedian)
  
  
  #################### ChlA ############
  chlSite=rightSite[rightSite$Measurement=="CHLA",]
  annualMedian <- chlSite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))

  if(length(annualMedian)!=0){
    Com_NOF$ChlAMed = annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5 yr median
    rollingMeds = rolling5(siteChemSet=chlSite,quantProb=0.5)
    rollFails = is.na(as.numeric(rollingMeds))
    Com_NOF$ChlAMed[yr%in%names(rollingMeds)] <- as.numeric(rollingMeds)
    Com_NOF$ChlAAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have ",strFrom(s=rollingMeds[rollFails],c='y'))

    #find the band which each value belong to
    Com_NOF$ChlAMed_Band <- sapply(Com_NOF$ChlAMed,NOF_FindBand,bandColumn=NOFbandDefinitions$ChlAMedian)
    Com_NOF$ChlAMed_Band <- sapply(Com_NOF$ChlAMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(rollingMeds,rollFails)
  }
  rm(annualMedian)
  #ChlA max
  annualMax <- chlSite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=max(Value,na.rm=T))
  
  if(length(annualMax)!=0){
    Com_NOF$ChlAMax = annualMax$value[match(Com_NOF$Year,annualMax$Year)]
    #Rolling max
    rollingMax=rolling5(chlSite,quantProb=0.95)
    rollFails=is.na(as.numeric(rollingMax))
    Com_NOF$ChlAMax[yr%in%names(rollingMax)]=as.numeric(rollingMax)
    Com_NOF$ChlAAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for max, have ",strFrom(s=rollingMax[rollFails],c='y'))
    #find the band which each value belong to
    Com_NOF$ChlAMax_Band <- sapply(Com_NOF$ChlAMax,NOF_FindBand,bandColumn=NOFbandDefinitions$ChlAMax)
    Com_NOF$ChlAMax_Band <- sapply(Com_NOF$ChlAMax_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(rollingMax,rollFails)
  }  
    #------------------Finding the band for ChlA Summary-------------------------------
    Com_NOF$ChlASummaryBand=apply(select(Com_NOF,ChlAMed_Band, ChlAMax_Band),1,max,na.rm=T)
  rm(chlSite,annualMax)
  return(Com_NOF)
}->NOFSummaryTable
stopCluster(workers)
rm(workers)

Sys.time()-startTime
rm(startTime)
#July6  3.8 secs

#These contain the best case out of these scorings, the worst of which contributes.
suppressWarnings(cnEc_Band <- sapply(NOFSummaryTable$EcoliBand,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEc95_Band <- sapply(NOFSummaryTable$Ecoli95_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth540_Band <- sapply(NOFSummaryTable$EcoliRecHealth540_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth260_Band <- sapply(NOFSummaryTable$EcoliRecHealth260_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))  
NOFSummaryTable$EcoliSummaryBand = as.character(apply(cbind(pmax(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band)),1,max))
rm("cnEc_Band","cnEc95_Band","cnEcRecHealth540_Band","cnEcRecHealth260_Band")


if(0){
  with(NOFSummaryTable,plot(as.factor(NitrogenMed_Band),NitrogenMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(PhosphorusMed_Band),PhosphorusMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(AmmoniacalMed_Band),AmmoniacalMed+0.01,log='y'))
  with(NOFSummaryTable,plot(as.factor(AmmoniacalMax_Band),AmmoniacalMax,log='y'))
  with(NOFSummaryTable,plot(as.factor(ChlAMed_Band),ChlAMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(ChlAMax_Band),ChlAMax,log='y'))
  with(NOFSummaryTable,plot(as.factor(ClarityMedian_Band),ClarityMedian,log='y'))
  
  table(NOFSummaryTable$AmmoniacalMed_Band,NOFSummaryTable$Ammonia_Toxicity_Band)
  table(NOFSummaryTable$AmmoniacalMax_Band,NOFSummaryTable$Ammonia_Toxicity_Band)
  table(NOFSummaryTable$AmmoniacalMax_Band,NOFSummaryTable$AmmoniacalMax_Band)
  table(NOFSummaryTable$Ecoli95_Band,NOFSummaryTable$EcoliSummaryBand)
}


NOFSummaryTable$CouncilSiteID=lakeSiteTable$CouncilSiteID[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable$Agency=lakeSiteTable$Agency[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable$Region=lakeSiteTable$Region[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable$SiteID=lakeSiteTable$SiteID[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable <- NOFSummaryTable%>%select(LawaSiteID:SiteID,Year:EcoliSummaryBand)




#############################Save output tables ############################
#Audit wants NOFLakesOverall.
write.csv(NOFSummaryTable%>%dplyr::filter(grepl('to',Year)),
          file = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                        "/NOFLakesOverall",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)



#Make outputs for ITE
# Reshape Output
NOFSummaryTableLong <- melt(data=NOFSummaryTable,
                            id.vars=c("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","Region"))
LakeSiteNOF <- NOFSummaryTableLong%>%
  dplyr::select(LawaSiteID,variable,Year,value)%>%
  dplyr::filter(grepl('Band',variable,ignore.case=T))%>%
  dplyr::filter(grepl('to',Year))

LakeSiteNOF$parameter=LakeSiteNOF$variable
LakeSiteNOF$parameter[which(LakeSiteNOF$variable%in%c("Ecoli95","Ecoli95_Band","EcoliPeriod",
                                                     "EcoliMedian","EcoliBand",
                                                     "EcoliRecHealth260","EcoliRecHealth260_Band",
                                                     "EcoliRecHealth540","EcoliRecHealth540_Band",
                                                     "AmmoniacalMax_Band","AmmoniacalMed_Band",
                                                     "Nitrate_Toxicity_Band"))] <- NA

LakeSiteNOF$parameter <- gsub(pattern = "_*Band",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Tot_",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Max_",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "95",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Med_",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Summary",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "_Toxicity",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "RecHealth...",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Ammoniacal",replacement = "Ammonia",x = LakeSiteNOF$parameter,ignore.case = T)

LakeSiteNOF <- LakeSiteNOF%>%drop_na(LawaSiteID)%>%filter(LawaSiteID!="")

write.csv(LakeSiteNOF%>%transmute(LAWAID=LawaSiteID,
                                  BandingRule=variable,
                                  #Parameter=parameter,
                                  Year=Year,
                                  NOFMedian=value)%>%filter(Year=="2015to2019"),
          file=paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteNOF",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
rm(LakeSiteNOF)

#LakeSiteNOFGraph
write.csv(lakesMonthlyMedians%>%transmute(LAWAID=LawaSiteID,
                                          Parameter=Measurement,
                                          NOFDate=format(Date,'%Y-%m-%d'),
                                          NOFValue=Value),
          file=paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteNOFGraph",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)

cat("Nuffadem ouptuts")
#--------------------------------------
# 
# write.csv(NOFSummaryTable,
#           file = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
#                         "/NOFLakes",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
# 
# 
# write.csv(NOFSummaryTableLong,
#           file = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
#                         "/NOFSummaryTableLong.csv"),row.names=F)
# 
# write.csv(NOFSummaryTableLong%>%dplyr::filter(grepl('to',Year))%>%drop_na(LawaSiteID),
#           file = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
#                         "/NOF_STATE_2018.csv"),row.names=F)
# 
# 
# 
# #Round them off.  For web display?
# NOFRound <- NOFSummaryTableLong%>%dplyr::filter(grepl('to',Year))%>%drop_na(LawaSiteID)
# NOFRound$variable <- as.character(NOFRound$variable)
# 
# variables<-as.character(unique(NOFSummaryTableLongSubset$variable))
# variables <- variables[order(variables)]
# # [1] "Ammonia_Toxicity_Band"   "ChlASummaryBand"         "Ecoli"                  "EcoliBand"             "Ecoli95"               
# # [6] "Ecoli95_Band"           "EcoliRecHealth260"      "EcoliRecHealth260_Band" "EcoliRecHealth540"      "EcoliRecHealth540_Band"
# # [11] "EcoliSummaryBand"       "AmmoniacalMax"          "AmmoniacalMax_Band"     "ChlAMax"                "ChlAMax_Band"          
# # [16] "AmmoniacalMed_Band"     "ChlAMed_Band"           "ClarityMedian_Band"        "AmmoniacalMed"       "ChlAMed"            
# # [21] "ClarityMedian"          "NitrogenMed_Band"           "PhosphorusMed_Band"           "NitrogenMed"          "PhosphorusMed" 
# 
# # Decimal places for variables
# dp <- rep(NA,length(variables))
# dp[variables%in%c("Ecoli", "Ecoli95", "EcoliRecHealth260", "EcoliRecHealth540")] <- 0
# dp[variables%in%c("AmmoniacalMax", "AmmoniacalMed", "NitrogenMed", "PhosphorusMed","ChlAMed","ChlAMax")] <- 4
# dp[variables%in%c("ClarityMedian")] <- 2
# 
# MeasurementInvolved <- variables
# MeasurementInvolved <- gsub(pattern = "Band",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Band",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "_$",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Med_",replacement = "Median_",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Tot_",replacement = "Total_",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Ecoli$",replacement = "EcoliMedian",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Phosphorus",replacement = "Phos",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Nitrogen",replacement = "Nitr",x = MeasurementInvolved)
# 
# 
# desc = rep('value',length(variables))
# desc[grepl(variables,pattern = 'Band',ignore.case = T)] <- 'Band'
# desc[variables%in%c("Agency", "SWQAltitude","SWQLanduse","SiteID","CATCH_LBL","CatchID",
#                     "CatchType","Comment","LAWA_CATCH","Region","SOE_FW_RIV",
#                     "SWQFrequencyAll","SWQFrequencyLast5","SWQuality","TermReach")] <- 'meta'
# 
# dfp <- data.frame(variables,MeasurementInvolved,desc,dp,stringsAsFactors=FALSE,row.names=NULL)
# NOFRound <- merge(NOFRound,dfp,by.x="variable",by.y="variables",all=TRUE)
# 
# rm(variables,MeasurementInvolved,desc,dp)
# # POST PROCESSING NOF RESULTS
# # Round values to appropriate DP's
# 
# # Note that for rounding off a 5, the IEC 60559 standard is expected to be used, ‘go to the even digit’.
# # Therefore round(0.5) is 0 and round(-1.5) is -2
# # This is not the desired behaviour here. It is expected that 0.5 rounds to 1, 2.5 rounds, to 3 and so on.
# # Therefore for all even values followed by exactly .5 needs to have a small number added (like 0.00001)
# # to get them rounding in the right direction (unless there is a flag in the function that provides for this behaviour),
# # or to redefine the round function. 
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
# NOFRound$value[is.na(NOFRound$value)] <- "NA"
# NOFRound <- NOFRound[order(NOFRound$LawaSiteID,NOFRound$MeasurementInvolved,NOFRound$desc),]
# write.csv(NOFRound,
#           file = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
#                         "/NOF_LAKES_STATE_2020_Rounded_NAs.csv"),row.names=F)
# 
# # Transform (tidyr::spread) data in NOFRound to the following form to supply to IT Effect
# # LawaSiteID,CouncilSiteID,Year,Measurement,value,Band
# # ARC-00001,44603,Overall,AmmoniacalMaxN,NA,NA
# # ARC-00001,44603,Overall,AmmoniacalMed,NA,NA
# # ARC-00001,44603,Overall,Median_Ecoli,28,A
# # ARC-00001,44603,Overall,NitrogenMed,0.0079,A
# 
# NOF_value <- NOFRound%>%filter(desc=="value")%>%select("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","variable","value","MeasurementInvolved")
# names(NOF_value) <- c("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","Measurement","Value",'MeasurementInvolved')
# NOF_Band  <- NOFRound%>%filter(desc=="Band")%>%select("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","variable","value","MeasurementInvolved")
# names(NOF_Band) <- c("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","BandingRule","BandScore",'MeasurementInvolved')
# 
# NOF_wide <- dplyr::left_join(NOF_Band,NOF_value,by = c("LawaSiteID","CouncilSiteID","SiteID","Agency", "Year", "MeasurementInvolved"))
# NOF_wide <- unique(NOF_wide)
# 
# 
# # #add medianClar and ChlNOF
# # medianClar$Year="Overall"
# # medianClar$SiteID=NOF_wide$SiteID[match(medianClar$LawaSiteID,NOF_wide$LawaSiteID)]
# # medianClar$CouncilSiteID=NOF_wide$CouncilSiteID[match(medianClar$LawaSiteID,NOF_wide$LawaSiteID)]
# # medianClar$Agency=NOF_wide$Agency[match(medianClar$LawaSiteID,NOF_wide$LawaSiteID)]
# # medianClar$BandingRule="CustomClar"
# # medianClar <- medianClar%>%rename("BandScore"="ClarityScore","Value"="medClar")
# # medianClar$MeasurementInvolved="Clarity"
# # medianClar$Measurement="Clarity"
# # medianClar <- medianClar%>%select(names(NOF_wide))%>%as.data.frame
# # 
# # chlNOF$Year="Overall"
# # chlNOF$SiteID=NOF_wide$SiteID[match(chlNOF$LawaSiteID,NOF_wide$LawaSiteID)]
# # chlNOF$CouncilSiteID=NOF_wide$CouncilSiteID[match(chlNOF$LawaSiteID,NOF_wide$LawaSiteID)]
# # chlNOF$Agency=NOF_wide$Agency[match(chlNOF$LawaSiteID,NOF_wide$LawaSiteID)]
# # # medChl,maxChl,
# # chlNOFb <- chlNOF%>%select(-medChl,-maxChl)%>%gather(BandingRule,BandScore,c(MedChlScore,MaxChlScore,overall))
# # chlNOFb$Value=NA
# # chlNOFb$Value[chlNOFb$BandingRule=="MedChlScore"]=chlNOF$medChl[match(chlNOFb$LawaSiteID[chlNOFb$BandingRule=="MedChlScore"],chlNOF$LawaSiteID)]
# # chlNOFb$Value[chlNOFb$BandingRule=="MaxChlScore"]=chlNOF$maxChl[match(chlNOFb$LawaSiteID[chlNOFb$BandingRule=="MaxChlScore"],chlNOF$LawaSiteID)]
# # chlNOFb$Measurement <- "ChlA"
# # chlNOFb$MeasurementInvolved="ChlA"
# # chlNOFb$BandingRule[chlNOFb$BandingRule=="overall"]="ChlSummary"
# # # chlNOFb <- chlNOFb%>%rename("BandScore"="overall","Value"="medClar")
# # # chlNOF$Measurement="Clarity"
# # chlNOFb <- chlNOFb%>%select(names(NOF_wide))%>%as.data.frame
# # 
# # if(!"Clarity"%in%unique(NOF_wide$Measurement)){
# #   NOF_wide = rbind(NOF_wide,medianClar,chlNOFb)
# # }
# # 
# 
# write.csv(NOF_wide, file = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
#                                   "/Lakes_NOF_forITE_",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
# # NOF_wide <- read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/","Lakes_NOF_forITE_",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)
# 
