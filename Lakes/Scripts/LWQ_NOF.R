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

try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'))))

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
#Reference Dates
EndYear <- lubridate::isoyear(Sys.Date())-1
StartYear5 <- EndYear - 5 + 1
yr  <- c(as.character(StartYear5:EndYear),"Overall")
reps  <- length(yr)

# loads lawaLakesdata dataframe  from LAWA_State.r  - has altered values from censoring, and calculated medians
lakesMonthlyMedians=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis",
                                      pattern="lakeMonthlyMedian.*csv",  #"ForITE"
                                      recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors=F)
# lakesMonthlyMedians$Value[which(lakesMonthlyMedians$Measurement%in%c('NH4N'))]= #,'TN','TP'
#   lakesMonthlyMedians$Value[which(lakesMonthlyMedians$Measurement%in%c('NH4N'))]/1000  #mg/L to mg/m3

lakesMonthlyMedians$Date=lubridate::dmy(paste0('1-',lakesMonthlyMedians$month,'-',lakesMonthlyMedians$Year))
lakesMonthlyMedians$Measurement=toupper(lakesMonthlyMedians$Measurement)


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
medianClar <- annualClar%>%dplyr::group_by(LawaSiteID)%>%dplyr::summarise(medClar=median(medVal,na.rm=T),)%>%ungroup
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





# Subset to just have the variables that are tested against NOF standards
sub_lwq <- lakesMonthlyMedians%>%dplyr::filter(Year>=StartYear5)%>%
  dplyr::select(c("LawaSiteID","CouncilSiteID","Measurement","Value","Date"))%>%
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

foreach(i = 1:length(uclids),.combine=rbind,.errorhandling='stop')%dopar%{
  suppressWarnings(rm(tnsite,tpsite,nh4site,ecosite,rightSite,value,Value)  )
  rightSite=sub_lwq[(sub_lwq$uclid==uclids[i]),]
  rightSite=rightSite[!is.na(rightSite$Value),]
  # create table of compliance with proposed National Objectives Framework
  Com_NOF <- data.frame (LawaSiteID               = rep(unique(rightSite$LawaSiteID),length(yr)),
                         CouncilSiteID            = rep(unique(rightSite$CouncilSiteID),length(yr)),
                         Year                     = yr,
                         Total_Nitrogen           = as.numeric(rep(NA,reps)),
                         Tot_Nitr_Band            = factor(rep(NA,reps),levels=c("A","B","C","D")),
                         NitrateAnalysisNote      = rep('',reps),
                         Total_Phosphorus         = as.numeric(rep(NA,reps)),
                         Tot_Phos_Band            = factor(rep(NA,reps),levels = c("A","B","C","D")),
                         PhosphorusAnalysisNote      = rep('',reps),
                         AmmoniacalMed        = as.numeric(rep(NA,reps)),
                         Med_Ammoniacal_Band      = factor(rep(NA,reps),levels = c("A","B","C","D")),
                         Max_Ammoniacal           = as.numeric(rep(NA,reps)),
                         Max_Ammoniacal_Band      = factor(rep(NA,reps),levels = c("A","B","C","D")),
                         Ammonia_Toxicity_Band    = factor(rep(NA,reps),levels = c("A","B","C","D")),
                         AmmoniaAnalysisNote      = rep('',reps),
                         Median_Clarity           = as.numeric(rep(NA,reps)),
                         Med_Clarity_Band         = factor(rep(NA,reps),levels = c("A","B","C","D")),
                         ClarityAnalysisNote      = rep('',reps),
                         Median_ChlA              = as.numeric(rep(NA,reps)),
                         Med_ChlA_Band            = factor(rep(NA,reps),levels = c("A","B","C","D")),
                         Max_ChlA                 = as.numeric(rep(NA,reps)),
                         Max_ChlA_Band            = factor(rep(NA,reps),levels = c("A","B","C","D")),
                         ChlASummaryBand          = factor(rep(NA,reps),levels = c("A","B","C","D")),
                         ChlAAnalysisNote      = rep('',reps),
                         E_coli_Period            = as.numeric(rep(NA,reps)),
                         E_coli_Median            = as.numeric(rep(NA,reps)),
                         E_coli_band              = rep(NA,reps),
                         E_coli95                 = as.numeric(rep(NA,reps)),
                         E_coli95_band            = rep(NA,reps),
                         E_coliRecHealth540       = as.numeric(rep(NA,reps)),
                         E_coliRecHealth540_Band  = rep(NA,reps),
                         E_coliRecHealth260       = as.numeric(rep(NA,reps)),
                         E_coliRecHealth260_Band  = rep(NA,reps),
                         E_coliSummaryband        = factor(rep(NA,reps),levels=c("A","B","C","D","E")),
                         E_coliAnalysisNote       = rep('',reps),
                         stringsAsFactors = FALSE)
  
  
  ######################  Nitrogen  ########################################
  tnsite=rightSite[rightSite$Measurement=="TN",]
  annualMedian <- tapply(tnsite$Value, format(tnsite$Date, '%Y'), na.rm=TRUE, quantile,prob=c(0.5),type=5)
  
  if(length(annualMedian)!=0){
    Com_NOF$Total_Nitrogen <- annualMedian[match(Com_NOF$Year,names(annualMedian))]
    #Overall median
    
    if(length(tnsite$Value)>=30){  #6 measurements per year please
      Com_NOF$Total_Nitrogen[nrow(Com_NOF)] <- quantile(tnsite$Value,prob=c(0.5),type=5,na.rm=T)
    }else{
      Com_NOF$NitrateAnalysisNote[nrow(Com_NOF)] = paste0(Com_NOF$NitrateAnalysisNote[nrow(Com_NOF)],
                                                          ' Need 30 nitrate values for overall median, have ',sum(!is.na(tnsite$Value)))
    }
    #find the band which each value belong to
    if(lakeSiteTable$LType[which(lakeSiteTable$uclid==uclids[i])]%in%c("stratified","brackish","icoll","monomictic")){
      Com_NOF$Tot_Nitr_Band <- unlist(lapply(Com_NOF$Total_Nitrogen,NOF_FindBand,bandColumn = NOFbandDefinitions$TotalNitrogenseasonally.stratified.and.brackish))
      Com_NOF$Tot_Nitr_Band <- unlist(lapply(Com_NOF$Tot_Nitr_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
    }else{ #assuming polymictic
      Com_NOF$Tot_Nitr_Band <- unlist(lapply(Com_NOF$Total_Nitrogen,NOF_FindBand,bandColumn = NOFbandDefinitions$TotalNitrogenpolymictic))
      Com_NOF$Tot_Nitr_Band <- unlist(lapply(Com_NOF$Tot_Nitr_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
    }
  }
  rm(tnsite,annualMedian)
  
  ######################  Phosphorus  ########################################
  tpsite=rightSite[rightSite$Measurement=="TP",]
  annualMedian <- tapply(tpsite$Value, format(tpsite$Date, '%Y'), na.rm=TRUE, quantile,prob=c(0.5),type=5)
  
  if(length(annualMedian)!=0){
    #adding annualMedians into Com_NOF table
    Com_NOF$Total_Phosphorus <- annualMedian[match(Com_NOF$Year,names(annualMedian))]
    
    if(length(tpsite$Value)>=30){
      Com_NOF$Total_Phosphorus[nrow(Com_NOF)] <- quantile(tpsite$Value,prob=c(0.5),type=5,na.rm=T)
    }else{
      Com_NOF$PhosphorusAnalysisNote[nrow(Com_NOF)] = paste0(Com_NOF$PhosphorusAnalysisNote[nrow(Com_NOF)],
                                                             ' Need 30 nitrate values for overall median, have ',
                                                             sum(!is.na(tpsite$Value)))
    }
    #find the band which each value belong to
    Com_NOF$Tot_Phos_Band <- unlist(lapply(Com_NOF$Total_Phosphorus,NOF_FindBand,bandColumn = NOFbandDefinitions$TotalPhosphorus))
    Com_NOF$Tot_Phos_Band <- unlist(lapply(Com_NOF$Tot_Phos_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
  }
  rm(tpsite,annualMedian)
  
  ###################### Ammonia  ############################
  nh4site=rightSite[rightSite$Measurement=="NH4",]
  if(dim(nh4site)[1]==0){
    Com_NOF$AmmoniaAnalysisNote=paste0(Com_NOF$AmmoniaAnalysisNote,'n = ',sum(!is.na(nh4site$Value)),
                                       ' No NH4 data ')
  }else{
    if(all(nh4site$Value==-99)){
      Com_NOF$AmmoniaAnalysisNote=paste0(Com_NOF$AmmoniaAnalysisNote,'n = ',sum(!is.na(nh4site$Value)),
                                         ' No pH data available for NH4 adjustment, so NH4 cannot be judged against NOF standards. ')
    }
    #Median Ammoniacal Nitrogen
    annualMedian <- tapply(nh4site$Value,format(nh4site$Date, '%Y'), na.rm=TRUE, quantile,prob=c(0.5),type=5)
    
    if(length(annualMedian)!=0){
      Com_NOF$AmmoniacalMed = annualMedian[match(Com_NOF$Year,names(annualMedian))]
      
      #overall median
      if(length(nh4site$Value)>=30){
        Com_NOF$AmmoniacalMed[nrow(Com_NOF)] <- quantile(nh4site$Value,prob=c(0.5),type=5)
      }else{
        Com_NOF$AmmoniaAnalysisNote[nrow(Com_NOF)]=paste0(Com_NOF$AmmoniaAnalysisNote[nrow(Com_NOF)],
                                                          ' Need 30 values for overall median, have ',
                                                          sum(!is.na(nh4site$Value)))
      }
      Com_NOF$Med_Ammoniacal_Band <- unlist(lapply(Com_NOF$AmmoniacalMed,NOF_FindBand,bandColumn=NOFbandDefinitions$Median.Ammoniacal.N)) 
      Com_NOF$Med_Ammoniacal_Band <- unlist(lapply(Com_NOF$Med_Ammoniacal_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
      rm(annualMedian)
      #-------maximum annual Ammoniacal Nitrogen--------------------------------------
      annualMax<- tapply(nh4site$Value, format(nh4site$Date, '%Y'), na.rm=TRUE, max)
      Com_NOF$Max_Ammoniacal <- annualMax[match(Com_NOF$Year,names(annualMax))]
      
      #overall 95th%ile
      if(length(nh4site$Value)>=30){
        Com_NOF$Max_Ammoniacal[nrow(Com_NOF)] <- quantile(nh4site$Value,prob=c(0.95),type=5)
      }else{
        Com_NOF$AmmoniaAnalysisNote[nrow(Com_NOF)]=paste0(Com_NOF$AmmoniaAnalysisNote[nrow(Com_NOF)],
                                                          ' Need 30 values for overall 95%ile, have ',sum(!is.na(nh4site$Value)))
      }
      
      #find the band which each value belong to
      Com_NOF$Max_Ammoniacal_Band <-unlist(lapply(Com_NOF$Max_Ammoniacal,NOF_FindBand,
                                                  bandColumn=NOFbandDefinitions$Max.Ammoniacal.N)) 
      Com_NOF$Max_Ammoniacal_Band <- unlist(lapply(Com_NOF$Max_Ammoniacal_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
      
      #------------------Finding the band for Ammonia Toxicity-------------------------------
      Com_NOF$Ammonia_Toxicity_Band=apply(select(Com_NOF,Med_Ammoniacal_Band, Max_Ammoniacal_Band),1,max,na.rm=T)
    }  
  }
  rm(nh4site)
  
  ######################  E.Coli #########################################
  suppressWarnings(rm(cnEc_band,cnEc95_band,cnEcRecHealth540_Band,cnEcRecHealth260_Band,ecosite,rawEcoli))
  ecosite=rightSite[rightSite$Measurement=="ECOLI",]
  ecosite$year=lubridate::isoyear(ecosite$Date)
  rawEcoli=ecosite%>%dplyr::select(year,Value)%>%filter(!is.na(Value)&year>=StartYear5)
  if(dim(rawEcoli)[1]>=60){ #data requirement for band determination, footnote 1, table  NPS. 60 over 5 years is monthly
    
    Com_NOF$E_coli_Period[Com_NOF$Year=='Overall']=5
    ecosite=ecosite%>%filter(lubridate::year(Date)>=StartYear5)
  }else{
    #Relax the rules, try for 60 values in 6 years
    rawEcoli=ecosite%>%dplyr::select(year,Value)%>%filter(!is.na(Value))
    if(dim(rawEcoli)[1]>=60){ #data requirement for band determination, footnote 1, table  NPS
      Com_NOF$E_coli_Period[Com_NOF$Year=='Overall']=6
    }
  }
  
  #E coli median
  if(dim(ecosite)[1]>=60){
    annualMedian <- tapply(ecosite$Value, format(ecosite$Date, '%Y'), na.rm=TRUE, quantile,prob=c(0.5),type=5)
    if(length(annualMedian)!=0){
      Com_NOF$E_coli_Median <- annualMedian[match(Com_NOF$Year,names(annualMedian))]
      #overall median calculated on previous 5 years only, even if six years were used to calculate NOF band
      Com_NOF$E_coli_Median[nrow(Com_NOF)] <- quantile(ecosite$Value[ecosite$year>=StartYear5],prob=c(0.5),type=5,na.rm=T)
      #bands
      Com_NOF$E_coli_band <- unlist(lapply(Com_NOF$E_coli_Median,NOF_FindBand,bandColumn=NOFbandDefinitions$E..coli))
      #Median EColi can meet multiple bands
      cnEc_band <- unlist(lapply(Com_NOF$E_coli_band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
    }
    #else they're left as NA
    rm(annualMedian)
    
    #Ecoli 95th percentile
    annualMax <- tapply(ecosite$Value,format(ecosite$Date, '%Y'),na.rm=TRUE, quantile,prob=c(0.95),type=5)
    if(length(annualMax)!=0){
      Com_NOF$E_coli95 <- annualMax[match(Com_NOF$Year,names(annualMax))]
      #overall 95 %ile calculated on previous 5 years only, even if six years were used to calculate NOF band
      Com_NOF$E_coli95[nrow(Com_NOF)] <- quantile(ecosite$Value[ecosite$year>=StartYear5],prob=c(0.95),type=5,na.rm=T)
      #bands 
      Com_NOF$E_coli95_band <- unlist(lapply(Com_NOF$E_coli95,NOF_FindBand,bandColumn=NOFbandDefinitions$Ecoli95))
      #Ecoli95 can meet multiple bands
      cnEc95_band <- unlist(lapply(Com_NOF$E_coli95_band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
    }  
    rm(annualMax)
    
    # Exceedance
    for(yy in 1:length(Com_NOF$Year)){
      ecv=rawEcoli$Value[which(rawEcoli$year==Com_NOF$Year[yy])]
      if(length(ecv)>0){
        Com_NOF$E_coliRecHealth540[yy]=sum(ecv>540)/length(ecv)*100
        Com_NOF$E_coliRecHealth260[yy]=sum(ecv>260)/length(ecv)*100
      }
    }
    #Overall exceedance
    Com_NOF$E_coliRecHealth540[nrow(Com_NOF)] <- sum(rawEcoli$Value>540)/length(rawEcoli$Value)*100
    Com_NOF$E_coliRecHealth260[nrow(Com_NOF)] <- sum(rawEcoli$Value>260)/length(rawEcoli$Value)*100
    #Bands
    Com_NOF$E_coliRecHealth540_Band <- unlist(lapply(Com_NOF$E_coliRecHealth540,NOF_FindBand,bandColumn=NOFbandDefinitions$EcoliRec540))
    cnEcRecHealth540_Band <- unlist(lapply(Com_NOF$E_coliRecHealth540_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
    
    Com_NOF$E_coliRecHealth260_Band <- unlist(lapply(Com_NOF$E_coliRecHealth260,NOF_FindBand,bandColumn=NOFbandDefinitions$EcoliRec260))
    cnEcRecHealth260_Band <- unlist(lapply(Com_NOF$E_coliRecHealth260_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))  
    #else they're left as NA
  }
  
  if(is.na(Com_NOF$E_coli_Period[Com_NOF$Year=='Overall'])){ #Set bands to NA, if insufficent data
    Com_NOF$E_coli_band[Com_NOF$Year=='Overall']=NA
    Com_NOF$E_coli95_band[Com_NOF$Year=='Overall']           =NA
    Com_NOF$E_coliRecHealth540_Band[Com_NOF$Year=='Overall'] =NA
    Com_NOF$E_coliRecHealth260_Band[Com_NOF$Year=='Overall']=NA
    Com_NOF$E_coliSummaryband[Com_NOF$Year=='Overall']=NA
    Com_NOF$E_coliAnalysisNote[Com_NOF$Year=='Overall']=paste0(Com_NOF$E_coliAnalysisNote[Com_NOF$Year=='Overall'],
                                                               ' Need 60 values, have ',
                                                               sum(!is.na(ecosite$Value)))
  }
  
  #These contain the best case out of these scorings, the worst of which contributes.
  if(all(exists(c("cnEc_band","cnEc95_band","cnEcRecHealth540_Band","cnEcRecHealth260_Band")))){
    Com_NOF$E_coliSummaryband = apply(cbind(pmax(cnEc_band,cnEc95_band,cnEcRecHealth540_Band,cnEcRecHealth260_Band)),1,max)
  }else{
    Com_NOF$E_coliSummaryband = NA
  }
  rm(ecosite)
  
  #################### Clarity ############
  clarsite=rightSite[rightSite$Measurement=="SECCHI",]
  annualMedian <- tapply(clarsite$Value, format(clarsite$Date, '%Y'),na.rm=TRUE, quantile,prob=c(0.5),type=5)
  
  if(length(annualMedian)!=0){
    #adding annualMedians into Com_NOF table
    Com_NOF$Median_Clarity = annualMedian[match(Com_NOF$Year,names(annualMedian))]
    
    #calculate the overall median
    if(length(clarsite$Value)>=30){
      Com_NOF$Median_Clarity[nrow(Com_NOF)] <- quantile(clarsite$Value,prob=c(0.5),type=5)
    }else{
      Com_NOF$ClarityAnalysisNote[nrow(Com_NOF)]=paste0(Com_NOF$ClarityAnalysisNote[nrow(Com_NOF)],
                                                       ' Need 30 values for overall median, have ',
                                                       sum(!is.na(clarsite$Value)))
    }
    
    #find the band which each value belong to
    Com_NOF$Med_Clarity_Band <- unlist(lapply(Com_NOF$Median_Clarity,NOF_FindBand,bandColumn=NOFbandDefinitions$Clarity)) 
    Com_NOF$Med_Clarity_Band <- unlist(lapply(Com_NOF$Med_Clarity_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
  }  
  rm(clarsite,annualMedian)
  
  
  #################### ChlA ############
  chlSite=rightSite[rightSite$Measurement=="CHLA",]
  annualMedian <- tapply(chlSite$Value, format(chlSite$Date, '%Y'),na.rm=TRUE, quantile,prob=c(0.5),type=5)
  
  if(length(annualMedian)!=0){
    Com_NOF$Median_ChlA = annualMedian[match(Com_NOF$Year,names(annualMedian))]
    
    #calculate the overall median
    if(length(chlSite$Value)>=30){
      Com_NOF$Median_ChlA[nrow(Com_NOF)] <- quantile(chlSite$Value,prob=c(0.5),type=5)
    }else{
      Com_NOF$ChlAAnalysisNote[nrow(Com_NOF)]=paste0(Com_NOF$ChlAAnalysisNote[nrow(Com_NOF)],
                                                     ' Need 30 values for overall median, have ',
                                                     sum(!is.na(chlSite$Value)))
    }
    
    #find the band which each value belong to
    Com_NOF$Med_ChlA_Band <- unlist(lapply(Com_NOF$Median_ChlA,NOF_FindBand,bandColumn=NOFbandDefinitions$ChlAMedian)) 
    Com_NOF$Med_ChlA_Band <- unlist(lapply(Com_NOF$Med_ChlA_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
  }
  rm(annualMedian)
  #ChlA max
  annualMax <- tapply(chlSite$Value,format(chlSite$Date, '%Y'),na.rm=TRUE, max)
  
  if(length(annualMax)!=0){
    Com_NOF$Max_ChlA = annualMax[match(Com_NOF$Year,names(annualMax))]
    
    #calculate the overall max
    if(length(chlSite$Value)>=30){
      Com_NOF$Max_ChlA[nrow(Com_NOF)] <- quantile(chlSite$Value,prob=c(0.95),type=5)
    }else{
      Com_NOF$ChlAAnalysisNote[nrow(Com_NOF)]=paste0(Com_NOF$ChlAAnalysisNote[nrow(Com_NOF)],
                                                     ' Need 30 values for overall Max. ')
    }
    
    #find the band which each value belong to
    Com_NOF$Max_ChlA_Band <- unlist(lapply(Com_NOF$Max_ChlA,NOF_FindBand,bandColumn=NOFbandDefinitions$ChlAMax)) 
    Com_NOF$Max_ChlA_Band <- unlist(lapply(Com_NOF$Max_ChlA_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))}))
    
    #------------------Finding the band for ChlA Summary-------------------------------
    Com_NOF$ChlASummaryBand=apply(select(Com_NOF,Med_ChlA_Band, Max_ChlA_Band),1,max,na.rm=T)
    
  }  
  rm(chlSite,annualMax)
  
  # if(!exists("NOFSummaryTable")){
  #   NOFSummaryTable <- Com_NOF
  # } else {
  #   NOFSummaryTable <- rbind.data.frame(NOFSummaryTable,Com_NOF,stringsAsFactors = FALSE)
  # }
  if(any(Com_NOF$LawaSiteID=='hrc-00331'&Com_NOF$CouncilSiteID=='lake wairarapa alsops bay')){browser()}
  return(Com_NOF)
}->NOFSummaryTable
stopCluster(workers)
rm(workers)


if(0){
  with(NOFSummaryTable,plot(as.factor(Tot_Nitr_Band),Total_Nitrogen,log='y'))
  with(NOFSummaryTable,plot(as.factor(Tot_Phos_Band),Total_Phosphorus,log='y'))
  with(NOFSummaryTable,plot(as.factor(Med_Ammoniacal_Band),AmmoniacalMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(Max_Ammoniacal_Band),Max_Ammoniacal,log='y'))
  with(NOFSummaryTable,plot(as.factor(Med_ChlA_Band),Median_ChlA,log='y'))
  with(NOFSummaryTable,plot(as.factor(Max_ChlA_Band),Max_ChlA,log='y'))
  with(NOFSummaryTable,plot(as.factor(Med_Clarity_Band),Median_Clarity,log='y'))
  
  table(NOFSummaryTable$Med_Ammoniacal_Band,NOFSummaryTable$Ammonia_Toxicity_Band)
  table(NOFSummaryTable$Max_Ammoniacal_Band,NOFSummaryTable$Ammonia_Toxicity_Band)
  table(NOFSummaryTable$Max_Ammoniacal_Band,NOFSummaryTable$Max_Ammoniacal_Band)
  table(NOFSummaryTable$E_coli95_band,NOFSummaryTable$E_coliSummaryband)
}


NOFSummaryTable$CouncilSiteID=lakeSiteTable$CouncilSiteID[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable$Agency=lakeSiteTable$Agency[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable$Region=lakeSiteTable$Region[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable$SiteID=lakeSiteTable$SiteID[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable <- NOFSummaryTable%>%select(LawaSiteID:SiteID,Year:E_coliSummaryband)


# Reshape Output
NOFSummaryTableLong <- melt(data=NOFSummaryTable,
                            id.vars=c("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","Region"))


#############################Save output tables ############################
#Audit wants NOFLakesOverall.
write.csv(NOFSummaryTable%>%dplyr::filter(Year=="Overall"),
          file = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                        "/NOFLakesOverall",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)

#Make outputs for ITE
LakeSiteNOF <- NOFSummaryTableLong%>%
  dplyr::select(LawaSiteID,variable,Year,value)%>%
  dplyr::filter(grepl('band',variable,ignore.case=T))%>%
  dplyr::filter(Year=="Overall")
LakeSiteNOF$parameter=LakeSiteNOF$variable
LakeSiteNOF$parameter[which(LakeSiteNOF$variable%in%c("E_coli95","E_coli95_Band","E_coli_Period",
                                                     "E_coli_Median","E_coli_band",
                                                     "E_coliRecHealth260","E_coliRecHealth260_Band",
                                                     "E_coliRecHealth540","E_coliRecHealth540_Band",
                                                     "Max_Ammoniacal_Band","Med_Ammoniacal_Band",
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

write.csv(LakeSiteNOF%>%transmute(LAWAID=LawaSiteID,
                                  BandingRule=variable,
                                  #Parameter=parameter,
                                  Year=EndYear,
                                  NOFMedian=value),
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
# write.csv(NOFSummaryTableLong%>%dplyr::filter(Year=="Overall")%>%drop_na(LawaSiteID),
#           file = paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
#                         "/NOF_STATE_2018.csv"),row.names=F)
# 
# 
# 
# #Round them off.  For web display?
# NOFRound <- NOFSummaryTableLong%>%dplyr::filter(Year=="Overall")%>%drop_na(LawaSiteID)
# NOFRound$variable <- as.character(NOFRound$variable)
# 
# variables<-as.character(unique(NOFSummaryTableLongSubset$variable))
# variables <- variables[order(variables)]
# # [1] "Ammonia_Toxicity_Band"   "ChlASummaryBand"         "E_coli"                  "E_coli_band"             "E_coli95"               
# # [6] "E_coli95_band"           "E_coliRecHealth260"      "E_coliRecHealth260_Band" "E_coliRecHealth540"      "E_coliRecHealth540_Band"
# # [11] "E_coliSummaryband"       "Max_Ammoniacal"          "Max_Ammoniacal_Band"     "Max_ChlA"                "Max_ChlA_Band"          
# # [16] "Med_Ammoniacal_Band"     "Med_ChlA_Band"           "Med_Clarity_Band"        "AmmoniacalMed"       "Median_ChlA"            
# # [21] "Median_Clarity"          "Tot_Nitr_Band"           "Tot_Phos_Band"           "Total_Nitrogen"          "Total_Phosphorus" 
# 
# # Decimal places for variables
# dp <- rep(NA,length(variables))
# dp[variables%in%c("E_coli", "E_coli95", "E_coliRecHealth260", "E_coliRecHealth540")] <- 0
# dp[variables%in%c("Max_Ammoniacal", "AmmoniacalMed", "Total_Nitrogen", "Total_Phosphorus","Median_ChlA","Max_ChlA")] <- 4
# dp[variables%in%c("Median_Clarity")] <- 2
# 
# MeasurementInvolved <- variables
# MeasurementInvolved <- gsub(pattern = "Band",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "band",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "_$",replacement = "",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Med_",replacement = "Median_",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Tot_",replacement = "Total_",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "E_coli$",replacement = "E_coliMedian",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Phosphorus",replacement = "Phos",x = MeasurementInvolved)
# MeasurementInvolved <- gsub(pattern = "Nitrogen",replacement = "Nitr",x = MeasurementInvolved)
# 
# 
# desc = rep('value',length(variables))
# desc[grepl(variables,pattern = 'band',ignore.case = T)] <- 'band'
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
# # ARC-00001,44603,Overall,Max_AmmoniacalN,NA,NA
# # ARC-00001,44603,Overall,AmmoniacalMed,NA,NA
# # ARC-00001,44603,Overall,Median_Ecoli,28,A
# # ARC-00001,44603,Overall,Total_Nitrogen,0.0079,A
# 
# NOF_value <- NOFRound%>%filter(desc=="value")%>%select("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","variable","value","MeasurementInvolved")
# names(NOF_value) <- c("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","Measurement","Value",'MeasurementInvolved')
# NOF_band  <- NOFRound%>%filter(desc=="band")%>%select("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","variable","value","MeasurementInvolved")
# names(NOF_band) <- c("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","BandingRule","BandScore",'MeasurementInvolved')
# 
# NOF_wide <- dplyr::left_join(NOF_band,NOF_value,by = c("LawaSiteID","CouncilSiteID","SiteID","Agency", "Year", "MeasurementInvolved"))
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
