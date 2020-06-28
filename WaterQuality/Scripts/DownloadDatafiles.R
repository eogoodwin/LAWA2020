rm(list=ls())
library(tidyverse)
source('H:/ericg/16666LAWA/LAWA2019/scripts/LAWAFunctions.R')
EndYear <- lubridate::isoyear(Sys.Date())-1
startYear15 <- EndYear - 15+1
StartYear10 <- EndYear - 10+1
StartYear5 <- EndYear -   5+1



# #####################################################################################
# LAKES:
lakeSiteTable=loadLatestSiteTableLakes()
#   Lake water quality monitoring dataset (2004-2018)
# Columns: Agency / Region / SiteName / SiteID / CouncilSiteID / LAWASiteID / LFENZID / Date / Indicator / Value 
# We usually include license / ownership and disclaimer text - I can add this in if you like.
# Do you pull the locations?  If so, it would be useful to include that lat and longs for each site also in the file
lakeDataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2019/Lakes/Data",pattern = "LakesWithMetadata.csv",
                          recursive = T,full.names = T,ignore.case=T),1)
lakeData=read.csv(lakeDataFileName,stringsAsFactors = F)
rm(lakeDataFileName)
lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]=
  lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]*1000  #mg/L to mg/m3   #Until 4/10/18 also NH4N
lakeData$month=lubridate::month(lubridate::dmy(lakeData$Date))
lakeData$Year=lubridate::isoyear(lubridate::dmy(lakeData$Date))
lakeData$monYear=paste0(lakeData$month,lakeData$Year)
lakeData=lakeData[which(lakeData$Year>=startYear15 & lakeData$Year<=EndYear),] #75659 to 74449

lakeWQmon <- lakeData%>%transmute(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,LFENZID,Date,Indicator=Measurement,Value=signif(Value,4))
write.csv(lakeWQmon,
          paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                 "DownloadDatafiles/LakeWQMonDat0418.csv"),row.names = F)
rm(lakeWQMon,lakeData)

# Lake TLI (2004-2018)
# Columns:  Agency / Region / Lake name / LFENZID / Date / TLI 
# We usually include license / ownership and disclaimer text - I can add this in if you like.
lakeTLI=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                 pattern="ITELakeTLI",recursive=T,full.names = T,ignore.case = T),1)
lakeTLI=read.csv(lakeTLI,stringsAsFactors = F)
lakeTLI=lakeTLI[which(lakeTLI$TLIYear>=startYear15 & lakeTLI$TLIYear<=EndYear),] #1853 to 1853
lakeTLI <- merge(lakeTLI%>%dplyr::rename(LFENZID=FENZID,Year=TLIYear),
                 lakeSiteTable%>%select(LFENZID,CouncilSiteID,SiteID,LawaSiteID,Agency,Region,))%>%
  select(Agency,Region,SiteID,CouncilSiteID,LFENZID,Year,TLI)
write.csv(lakeTLI,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                         "DownloadDatafiles/LakeTLI0418.csv"),row.names=F)
rm(lakeTLI)

# Lake State 
# Columns: Agency / Region / SiteName / SiteID / CouncilSiteID / LAWASiteID / LFENZID / 
#             Date / Indicator / Median Value / NOF band 
# We usually include license / ownership and disclaimer text - I can add this in if you like.
lakesState=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                       pattern="ITELakeSiteState",recursive=T,full.names = T,ignore.case = T),1)
lakesState=read.csv(lakesState,stringsAsFactors = F)%>%
  dplyr::rename(LawaSiteID=LAWAID,Indicator=Parameter)%>%filter(Indicator!="TLI")
unique(lakesState$Indicator)

lakeNOF=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                     pattern="ITELakeSiteNOF[^G]",recursive=T,full.names = T,ignore.case = T),1)
lakeNOF = read.csv(lakeNOF,stringsAsFactors = F)%>%
  dplyr::rename(LawaSiteID=LAWAID,NOFband=NOFMedian)
unique(lakeNOF$BandingRule)
lakeNOF <- lakeNOF%>%filter(BandingRule%in%c("Tot_Nitr_Band","Tot_Phos_Band","Ammonia_Toxicity_Band","Med_Clarity_Band",
                                             "ChlASummaryBand","E_coliSummaryband"))
lakeNOF$Indicator = gsub(pattern = 'Tot_|Med_|Max_|_Band|_Toxicity|Summary|band|RecHealth|260|540|_band|95|cal|Band',
                         replacement = '',lakeNOF$BandingRule)
lakeNOF$Indicator[lakeNOF$Indicator=="Ammonia"] <- "NH4N"
lakeNOF$Indicator[lakeNOF$Indicator=="ChlA"] <- "CHLA"
lakeNOF$Indicator[lakeNOF$Indicator=="Clarity"] <- "Secchi"
lakeNOF$Indicator[lakeNOF$Indicator=="E_coli"] <- "ECOLI"
lakeNOF$Indicator[lakeNOF$Indicator=="Nitr"] <- "TN"
lakeNOF$Indicator[lakeNOF$Indicator=="Phos"] <- "TP"


lakesState = merge(x = lakeSiteTable%>%select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,LFENZID),
                  y = lakesState%>%select(LawaSiteID,Indicator,Year,Median),all.y=T)
lakeNOF = merge(lakeSiteTable%>%select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,LFENZID),
                lakeNOF%>%select(LawaSiteID,Year,NOFband,Indicator),all.y=T)

lakeState = merge(lakesState,lakeNOF)%>%
                     select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,LFENZID,Year,Indicator,Median,NOFband)%>%distinct
write.csv(lakeState,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                           "DownloadDatafiles/LakeState0418.csv"),row.names=F)
rm(lakeState,lakesState,lakeNOF)

# Lake Trends
# Columns: Agency / Region / SiteName / SiteID / CouncilSiteID / LAWASiteID / LFENZID / Indicator / Trend period / Trend score 
lst5=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                        pattern="ITELakeSiteTrend5",recursive=T,full.names = T,ignore.case = T),1)
lst10=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                         pattern="ITELakeSiteTrend10",recursive=T,full.names = T,ignore.case = T),1)
lst15=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                         pattern="ITELakeSiteTrend15",recursive=T,full.names = T,ignore.case = T),1)
lst5 = read.csv(lst5,stringsAsFactors = F)
lst10 = read.csv(lst10,stringsAsFactors = F)
lst15 = read.csv(lst15,stringsAsFactors = F)
lst = merge(lst5,lst10,by = c("LAWAID","Parameter"))
lst$Trend15 = -99
for(pp in unique(lst$Parameter)){
  lst$Trend15[lst$Parameter==pp] <-  (lst15[lst15$Parameter==pp,"Trend15"])[match(lst[lst$Parameter==pp,"LAWAID"],
                                                                                    lst15[lst15$Parameter==pp,"LAWAID"])]
}
rm(lst5,lst10,lst15)
lst <- merge(x=lakeSiteTable%>%select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,LFENZID),all.x=F,
             y=lst%>%transmute(LawaSiteID=LAWAID,Indicator=Parameter,Trend5,Trend10,Trend15),all.y=T)
write.csv(lst,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                     "DownloadDatafiles/LakeTrend0418.csv"),row.names=F)

# The river trend file you sent for the science managers - do you think that it would be good 
# to include all the statistical analysis details in too?
rm(lst)
rm(lakeSiteTable)  



#####################################################################
#RIVIERAS
riverSiteTable=loadLatestSiteTableRiver()
rd <- loadLatestDataRiver()

# River water quality monitoring dataset (2004-2018)  - this is the physio-chem WQ indicators
# Columns: Agency / Region / SiteName / SiteID / CouncilSiteID / LAWASiteID / Land use / Altitude / Date / Indicator / Value 
# We usually include license / ownership and disclaimer text - I can add this in if you like.
# Do you pull the locations?  If so, it would be useful to include that lat and longs for each site also in the file
rrd <- rd%>%filter(lubridate::year(lubridate::dmy(Date))>=2004&lubridate::year(lubridate::dmy(Date))<=2018)%>%
  transmute(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,SWQLanduse,SWQAltitude,Lat,Long,Date,Indicator=Measurement,RawValue,Symbol,Value)

# split(wqtpreMK,f = wqtpreMK$Measurement)%>%
#   purrr::map(~{plot(.$NZMGE,.$NZMGN,xlab='',ylab='',xaxt='n',bty='n',yaxt='n',main=unique(.$Measurement),asp=1,pch=16,
#                     col=c("#dd0000FF","#ee5500FF","#aaaaaaFF","#00cc00FF","#008800FF","#dddddd")[as.numeric(.$wqtpreMK)])
#     with(wqt[as.character(wqt$Measurement)==unique(.$Measurement),],
#          points(NZMGE,NZMGN,pch=16,cex=0.5,
#                 col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd")[as.numeric(TrendScore)])

rrd%>%split(.$Region)%>%purrr::map(~write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                                                      "DownloadDatafiles/RiverWQData",unique(.$Region),"0418.csv")))
write.csv(rrd,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                     "DownloadDatafiles/RiverWQData0418.csv"),row.names=F)
rm(rrd)

# River water quality State
# Columns: Agency / Region / SiteName / SiteID / CouncilSiteID / LAWASiteID / Land use / Altitude /
#  Date / Indicator / Median Value / Quartile / NOF band 
riverWQState=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/WaterQuality/",
                    pattern="ITERiverState",recursive=T,full.names = T,ignore.case = T),1)
riverWQState = read_csv(riverWQState)%>%
  dplyr::filter(Altitude=="All"&Landuse=="All")
riverNOF=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/WaterQuality/",
                  pattern="ITERiverNOF",recursive=T,full.names = T,ignore.case = T),1)
riverNOF=read_csv(riverNOF)%>%dplyr::rename(LawaSiteID=LAWAID,Indicator=Parameter)%>%
  dplyr::filter(!Indicator%in%c("E_coli_Median_Band","E_coli95_Band","E_coliRecHealth260_Band","E_coliRecHealth540_Band",
                                "Ammoniacal95_Band","AmmoniacalMed_Band"))%>%
  drop_na(Band)%>%select(-Value)
riverNOF <- riverNOF%>%filter(Band!="NaN")
riverNOF$Indicator = gsub(pattern = 'Tot_|Med_|Max_|_Band|_Toxicity|Summary|band|RecHealth|260|540|_band|95|cal|Band',
                         replacement = '',riverNOF$Indicator)
riverNOF$Indicator[riverNOF$Indicator=="Ammonia"] <- "NH4"
riverNOF$Indicator[riverNOF$Indicator=="E_coli"] <- "ECOLI"
riverWQState <- merge(x=riverWQState%>%
                        dplyr::transmute(LawaSiteID=LawaID,Indicator=Parameter,Median,Quartile=StateScore),all.x=T,
                      y=riverSiteTable%>%
                        dplyr::select(LawaSiteID,Agency,Region,SiteID,CouncilSiteID,SWQLanduse,SWQAltitude),all.y=F)%>%
  dplyr::select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,SWQLanduse,SWQAltitude,Indicator,Median,Quartile)
riverWQState <- merge(x=riverWQState,all.x=T,
                      y=riverNOF%>%dplyr::select(-Year,-SiteName),all.y=F)
write.csv(riverWQState,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                              "DownloadDatafiles/RiverState0418.csv"),row.names=F)
rm(riverWQState)

#Do one fo rhte councils, with all comparison-ways
riverWQState=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/WaterQuality/",
                      pattern="ITERiverState",recursive=T,full.names = T,ignore.case = T),1)
riverWQState = read_csv(riverWQState)
riverWQState$Comparison=paste0(riverWQState$Altitude,'|',riverWQState$Landuse)
riverWQState <- merge(x=riverWQState%>%
                        dplyr::transmute(LawaSiteID=LawaID,Indicator=Parameter,Median,Comparison,Quartile=StateScore),all.x=T,
                      y=riverSiteTable%>%
                        dplyr::select(LawaSiteID,Agency,Region,SiteID,CouncilSiteID,SWQLanduse,SWQAltitude),all.y=F)%>%
  dplyr::select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,SWQLanduse,SWQAltitude,Indicator,Median,Comparison,Quartile)
write.csv(riverWQState,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                              "DownloadDatafiles/RiverState0418.csv"),row.names=F)
rm(riverWQState)



# River water quality Trends
# Columns: Agency / Region / SiteName / SiteID / CouncilSiteID / LAWASiteID / Land use / Altitude /
#  Date / Indicator / Trend Period / Trend Score
# The river trend file you sent for the science managers - do you think that it would be good to include all the statistical analysis details in too?  Extra detail for the experts!
riverTrend=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/WaterQuality/",
                      pattern="ITERiverTrend",recursive=T,full.names = T,ignore.case = T),1)
riverTrend <- read_csv(riverTrend)%>%as.data.frame
riverTrend <- merge(x=riverSiteTable%>%
                      select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,SWQLanduse,SWQAltitude),all.x=F,
                    y=riverTrend%>%
                      transmute(LawaSiteID=LAWAID,Indicator=Parameter,TrendPeriod,TrendFrequency,TrendScore),all.y=T)
write.csv(riverTrend,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                             "DownloadDatafiles/RiverTrend0418.csv"),row.names=F)
rm(riverTrend)

rm(rd,riverSiteTable)
  

#####################################################################
#Macros
macroSiteTable <- loadLatestSiteTableMacro()

  
# River macroinvertebrate monitoring dataset
# Columns: Agency / Region / SiteName / SiteID / CouncilSiteID / LAWASiteID / Date / Indicator / Value 
# We usually include license / ownership and disclaimer text - I can add this in if you like.
# Do you pull the locations?  If so, it would be useful to include that lat and longs for each site also in the file
macroData=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/",
                           pattern="ITEMacroHistoric",recursive=T,full.names = T,ignore.case = T),1)
macroData <- read_csv(macroData)%>%as.data.frame
macroData <- merge(x=macroSiteTable%>%dplyr::select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID),all.x=F,
                   y=macroData%>%transmute(LawaSiteID=LAWAID,Date=CollectionDate,Indicator=Metric,Value=Value),all.y=T)
write.csv(macroData,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                           "DownloadDatafiles/MacroData.csv"),row.names=F)

# River macroinvertebrate State
# Columns: Agency / Region / SiteName / SiteID / CouncilSiteID / LAWASiteID / Date / Indicator / Median Value / NOF band
macroState=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/",
                    pattern="ITEMacroState",recursive=T,full.names = T,ignore.case = T),1)
macroState = read_csv(macroState)%>%as.data.frame
macroState <- merge(macroSiteTable%>%dplyr::select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID),
                    macroState%>%dplyr::transmute(LawaSiteID=LAWAID,Indicator=Parameter,Median))
write.csv(macroState,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                           "DownloadDatafiles/MacroState.csv"),row.names=F)



# River macroinvertebrate Trends
# Columns: Agency / Region / SiteName / SiteID / CouncilSiteID / LAWASiteID / Date / Indicator / Trend Period / Trend Score
macroTrend10=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/",
                      pattern="ITEMacroTrend10",recursive=T,full.names = T,ignore.case = T),1)
macroTrend10 <- read_csv(macroTrend10)%>%as.data.frame
macroTrend15=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/",
                      pattern="ITEMacroTrend15",recursive=T,full.names = T,ignore.case = T),1)  
macroTrend15 <- read_csv(macroTrend15)%>%as.data.frame
mTrend <- merge(macroTrend10%>%transmute(LawaSiteID=LAWAID,Indicator=Parameter,Trend10=Trend),
                macroTrend15%>%transmute(LawaSiteID=LAWAID,Indicator=Parameter,Trend15=Trend),all=T)
mTrend <- merge(x=macroSiteTable%>%select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID),all.x=T,
                y=mTrend,all.y=T)
write.csv(mTrend,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2019 WIP files/",
                        "DownloadDatafiles/MacroTrend.csv"),row.names=F)


  