rm(list=ls())
library(tidyverse)
library(dplyr)
library(doBy)
source("h:/ericg/16666LAWA/LAWA2019/scripts/LAWAFunctions.R")
source("h:/ericg/16666LAWA/LWPTrends_v1901/LWPTrends_v1901.R")
source("h:/ericg/16666LAWA/LAWA2019/WaterQuality/scripts/PlotMeATrend.R")
pseudo.titlecase = function(str)
{
  substr(str, 1, 1) = toupper(substr(str, 1, 1))
  return(str)
}

EndYear <- year(Sys.Date())-1
startYear5 <- EndYear - 5+1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1


if(!exists('wqdata')){
  # riverSiteTable = loadLatestSiteTableRiver()
  wqdata=loadLatestDataRiver()%>%filter(Measurement=="ECOLI")

  # macroSiteTable = loadLatestSiteTableMacro()%>%dplyr::rename(AltitudeCl=Altitude)
  macroData=loadLatestDataMacro()%>%filter(Measurement=="MCI")
  macroData$Censored=F
  macroData$CenType='not'

  wqdata = merge(wqdata,macroData,all=T)
   rm(macroData)
  
  wqdata$myDate <- as.Date(as.character(wqdata$Date),"%d-%b-%Y")
  wqdata$myDate[wqdata$myDate<as.Date('2000-01-01')] <- as.Date(as.character(wqdata$Date[wqdata$myDate<as.Date('2000-01-01')]),"%d-%b-%y")
  wqdata <- GetMoreDateInfo(wqdata)
  wqdata$monYear = format(wqdata$myDate,"%b-%Y")
  wqdata$CenType[wqdata$CenType%in%c("Left","L")]='lt'
  wqdata$CenType[wqdata$CenType%in%c("Right","R")]='gt'
  wqdata$CenType[!wqdata$CenType%in%c("lt","gt")]='not'

  riverSiteTable=loadLatestSiteTableRiver()
  macroSiteTable=loadLatestSiteTableMacro()
}

#Load trend datas for macros and WQ
#Five year
load(tail(dir(ignore.case=T,
              path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/",
              pattern = "Trend5Year.rData",full.names = T,recursive = T),1),verbose =T)
trendTable5 <- trendTable5%>%drop_na(LawaSiteID)%>%
  mutate(LawaSiteID=as.character(LawaSiteID),
         Measurement=as.character(Measurement),
         AnalysisNoteSS=as.character(AnalysisNoteSS),
         ConfCat=as.character(ConfCat))
tt5=trendTable5%>%filter(frequency!='unassessed')
rm(trendTable5)

#Ten year
load(tail(dir(ignore.case=T,
              path = "h:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose =T)
macrotrend10=trendTable10%>%drop_na(LawaSiteID)%>%
  mutate(LawaSiteID=as.character(LawaSiteID),
         Measurement=as.character(Measurement),
         AnalysisNoteSS=as.character(AnalysisNoteSS),
         ConfCat=as.character(ConfCat))
load(tail(dir(ignore.case=T,
              path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose =T)
trendTable10 <- trendTable10%>%drop_na(LawaSiteID)%>%
  mutate(LawaSiteID=as.character(LawaSiteID),
          Measurement=as.character(Measurement),
          AnalysisNoteSS=as.character(AnalysisNoteSS),
          ConfCat=as.character(ConfCat))
tt10=merge(macrotrend10,trendTable10,all=T)%>%
  filter(Measurement%in%c("ECOLI","MCI"))%>%
  filter(frequency!='unassessed')
rm(macrotrend10,trendTable10)

# Fifteen year
load(tail(dir(ignore.case=T,
              path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/",
              pattern = "Trend15Year.rData",full.names = T,recursive = T),1),verbose =T)
trendTable15=trendTable15%>%drop_na(LawaSiteID)%>%
  mutate(LawaSiteID=as.character(LawaSiteID),Measurement=as.character(Measurement),AnalysisNoteSS=as.character(AnalysisNoteSS),ConfCat=as.character(ConfCat))
tt15=trendTable15%>%filter(frequency!='unassessed')
rm(trendTable15)

write.csv(x = tt10,'H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/TrendTable10Year.csv',row.names = F)
# write.csv(x = tt15,'H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/TrendTable15Year.csv',row.names = F)

measurepresn10 <- tt10%>%filter(TrendScore==2)%>%
  group_by(LawaSiteID)%>%
  dplyr::transmute(Agency,nmeas=length(unique(Measurement)))%>%
  distinct%>%
  filter(nmeas==2)
# measurepresn15 <- tt15%>%filter(TrendScore==2)%>%
#   group_by(LawaSiteID)%>%
#   dplyr::transmute(Agency,nmeas=length(unique(Measurement)))%>%
#   distinct%>%
#   filter(nmeas==2)

length(unique(measurepresn10$LawaSiteID))
# length(unique(measurepresn15$LawaSiteID))
table(measurepresn10$Agency)
# table(measurepresn15$Agency)

table(trendTable$Measurement)

#Remove MDC ECOLI
#See email Steffi Henkel 14/9/2018 to Kati Doehring, Eric Goodwin, Abi Loughnan and Peter Hamill
if(any(tt10$Agency=="mdc" & tt10$Measurement=="ECOLI")){
  tt10 <- tt10[-which(tt10$Agency=='mdc'& tt10$Measurement=="ECOLI"),]
  }

tt10 <- tt10%>%drop_na(LawaSiteID)
tt10 <- tt10%>%drop_na(ConfCat) # 3865 to 2154




if we use fifteen years included, we need down here to chagen to the fifteen year data set

# Pull bug infos ####
wqdatafor10=wqdata%>%dplyr::filter(Year>=startYear10 & Year <= EndYear & Measurement%in%c("MCI","ECOLI"))
wqdatafor10$Season=wqdatafor10$Month

MCIfos = tt10%>%
  dplyr::filter(Measurement=="MCI",ConfCat=="Very likely improving",!is.na(AnnualSenSlope))%>%
  arrange(Percent.annual.change)%>%
  top_n(n=10,wt=Percent.annual.change)
MCIfos$CouncilSiteID=wqdata$CouncilSiteID[match(MCIfos$LawaSiteID,wqdata$LawaSiteID)]
MCIfos$SiteID=wqdata$CouncilSiteID[match(MCIfos$LawaSiteID,wqdata$LawaSiteID)]
ECOLIfos = tt10%>%
  dplyr::filter(Measurement=="ECOLI",ConfCat=="Very likely improving",!is.na(AnnualSenSlope))%>%
  arrange(Percent.annual.change)%>%
  top_n(n=-10,wt=Percent.annual.change)
ECOLIfos$CouncilSiteID=wqdata$CouncilSiteID[match(ECOLIfos$LawaSiteID,wqdata$LawaSiteID)]
ECOLIfos$SiteID=wqdata$CouncilSiteID[match(ECOLIfos$LawaSiteID,wqdata$LawaSiteID)]

# bugInfos=dplyr::filter(trendTable,Measurement%in%c("MCI","ECOLI"),ConfCat=='Very likely improving',!is.na(AnnualSenSlope))
# bugInfos=bugInfos[order(bugInfos$Measurement,bugInfos$Percent.annual.change),]
# bugInfos = bugInfos%>%group_by(Measurement)%>%top_n(n = 10,wt = Percent.annual.change)%>%ungroup
# bugInfos$CouncilSiteID = wqdata$CouncilSiteID[match(bugInfos$LawaSiteID,wqdata$LawaSiteID)]
# bugInfos$SiteID = wqdata$SiteID[match(bugInfos$LawaSiteID,wqdata$LawaSiteID)]
# 
# MCIfos=bugInfos%>%filter(Measurement=="MCI")



##################################################################################################
#Find the sites with very likely improving ten year MCI and ECOLI
#Get the other parameters' trend infos for these sites
load(tail(dir(ignore.case=T,
              path = "h:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose =T)
macrotrend10=trendTable10%>%drop_na(LawaSiteID)%>%
  mutate(LawaSiteID=as.character(LawaSiteID),
         Measurement=as.character(Measurement),
         AnalysisNoteSS=as.character(AnalysisNoteSS),
         ConfCat=as.character(ConfCat))
rm(trendTable10)
load(tail(dir(ignore.case=T,
              path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose =T)
WQtrend10 <- trendTable10%>%drop_na(LawaSiteID)%>%
  mutate(LawaSiteID=as.character(LawaSiteID),
         Measurement=as.character(Measurement),
         AnalysisNoteSS=as.character(AnalysisNoteSS),
         ConfCat=as.character(ConfCat))
rm(trendTable10)

tt10=merge(macrotrend10,WQtrend10,all=T)
uID=data.frame(LawaSiteID=unique(tt10$LawaSiteID))
MCItrend = tt10%>%filter(Measurement=="MCI")
ECOLItrend = tt10%>%filter(Measurement=="ECOLI")
uID$MCItrend = MCItrend$ConfCat[match(uID$LawaSiteID,MCItrend$LawaSiteID)]
uID$ECOLItrend = ECOLItrend$ConfCat[match(uID$LawaSiteID,ECOLItrend$LawaSiteID)]

bothMeasuresGood <- uID%>%dplyr::filter(MCItrend=="Very likely improving"&ECOLItrend=="Very likely improving")%>%select(LawaSiteID)
# -------------------------------------
#Now the spatial one, post-identified a fithf site to add 
source("K:/R_functions/wgs2NZTM.r")
#Get the MCI very likely improving
MCIVLI = macrotrend10%>%dplyr::filter(TrendScore==2)
MCIVLI$Long = macroSiteTable$Long[match(MCIVLI$LawaSiteID,macroSiteTable$LawaSiteID)]
MCIVLI$Lat = macroSiteTable$Lat[match(MCIVLI$LawaSiteID,macroSiteTable$LawaSiteID)]
WQtrend10$Long = riverSiteTable$Long[match(WQtrend10$LawaSiteID,riverSiteTable$LawaSiteID)]
WQtrend10$Lat = riverSiteTable$Lat[match(WQtrend10$LawaSiteID,riverSiteTable$LawaSiteID)]
tm=wgs2nztm(ln = MCIVLI$Long,lt = MCIVLI$Lat)
MCIVLI$nztme=tm[,1]
MCIVLI$nztmn=tm[,2]
tm=wgs2nztm(ln = WQtrend10$Long,lt = WQtrend10$Lat)
WQtrend10$nztme=tm[,1]
WQtrend10$nztmn=tm[,2]
rm(tm)
par(mfrow=c(1,1))
plot(MCIVLI$nztme,MCIVLI$nztmn,pch=16)
with(WQtrend10%>%filter(Measurement=="ECOLI"&TrendScore==2),points(nztme,nztmn,pch=16,cex=0.6,col='red'))
MCIVLI$distToWQ = NA
MCIVLI$NearestECOLI=NA
goodECOLIS=WQtrend10%>%filter(Measurement=="ECOLI"&TrendScore==2)
for(mv in seq_along(MCIVLI$LawaSiteID)){
  dists = sqrt((MCIVLI$nztme[mv]-goodECOLIS$nztme)^2+(MCIVLI$nztmn[mv]-goodECOLIS$nztmn)^2)
  # cat(min(dists,na.rm=T),'\n')
  MCIVLI$distToWQ[mv]=min(dists,na.rm=T)
  MCIVLI$NearestECOLI[mv] = goodECOLIS$LawaSiteID[which.min(dists)]
  segments(x0 = MCIVLI$nztme[mv],x1=goodECOLIS$nztme[which.min(dists)],
           y0 = MCIVLI$nztmn[mv],y1=goodECOLIS$nztmn[which.min(dists)])
  if(min(dists,na.rm=T)==0){
     points(MCIVLI$nztme[mv],MCIVLI$nztmn[mv],pch=16,cex=1.25,col='green')
  }
}
sort(MCIVLI$distToWQ)
MCIVLI$MCISiteID = macroSiteTable$SiteID[match(MCIVLI$LawaSiteID,macroSiteTable$LawaSiteID)]
MCIVLI$ECOLISiteID = riverSiteTable$SiteID[match(MCIVLI$NearestECOLI,riverSiteTable$LawaSiteID)]
MCIVLI$MCICouncilSiteID = macroSiteTable$CouncilSiteID[match(MCIVLI$LawaSiteID,macroSiteTable$LawaSiteID)]
MCIVLI$ECOLICouncilSiteID = riverSiteTable$CouncilSiteID[match(MCIVLI$NearestECOLI,riverSiteTable$LawaSiteID)]
MCIVLI$SWQLanduse = macroSiteTable$SWQLanduse[match(MCIVLI$LawaSiteID,macroSiteTable$LawaSiteID)]
MCIVLI$SWQAltitude = macroSiteTable$SWQAltitude[match(MCIVLI$LawaSiteID,macroSiteTable$LawaSiteID)]
MCIVLI%>%filter(distToWQ<1000)%>%arrange(distToWQ)%>%select(distToWQ,LawaSiteID,NearestECOLI,MCISiteID,ECOLISiteID,SWQLanduse,SWQAltitude)
# --------------------------------
bothMeasuresGood = c(as.character(bothMeasuresGood$LawaSiteID),'lawa-100420','ccc-00007')

tt10 <- tt10%>%
  dplyr::filter(LawaSiteID%in%bothMeasuresGood)%>%
  select(LawaSiteID,Region,SWQLanduse, SWQAltitude, Measurement,ConfCat,TrendScore,Percent.annual.change)
tt10$SWQLanduse = pseudo.titlecase(tt10$SWQLanduse)
tt10$SWQAltitude = pseudo.titlecase(tt10$SWQAltitude)
tt10$SiteID=riverSiteTable$SiteID[match(tt10$LawaSiteID,riverSiteTable$LawaSiteID)]
tt10$SiteID[is.na(tt10$SiteID)]=macroSiteTable$SiteID[match(tt10$LawaSiteID[is.na(tt10$SiteID)],macroSiteTable$LawaSiteID)]
tt10$CouncilSiteID=riverSiteTable$CouncilSiteID[match(tt10$LawaSiteID,riverSiteTable$LawaSiteID)]
tt10$CouncilSiteID[is.na(tt10$CouncilSiteID)]=macroSiteTable$CouncilSiteID[match(tt10$LawaSiteID[is.na(tt10$CouncilSiteID)],macroSiteTable$LawaSiteID)]

tt10w <- tt10%>%select(-TrendScore,-Percent.annual.change)%>%spread(key="Measurement",value="ConfCat")
# Fill empties from tt5 or tt15
# tt10w$TON[tt10w$LawaSiteID=='hrc-00002'] = tt15$TrendScore[which(tt15$LawaSiteID=='hrc-00002'&tt15$Measurement=="TON")]
# tt10w$NH4[tt10w$LawaSiteID=='ncc-00017'] = tt15$TrendScore[which(tt15$LawaSiteID=='ncc-00017'&tt15$Measurement=="TP")]
# Ooops, none there
tt10w$DRP[tt10w$LawaSiteID=='ccc-00007'] = tt5$ConfCat[which(tt5$LawaSiteID=='ccc-00007'&tt5$Measurement=="DRP")]
# 
tt10tsw <- tt10%>%select(-TrendScore,-ConfCat)%>%spread(key="Measurement",value="Percent.annual.change")
tt10tsw$DRP[tt10tsw$LawaSiteID=='ccc-00007'] = tt5$Percent.annual.change[which(tt5$LawaSiteID=='ccc-00007'&tt5$Measurement=="DRP")]

names(tt10tsw)[7:15]=paste0(names(tt10tsw)[7:15],"pcAC")
tt10w <- tt10w%>%select(LawaSiteID,SiteID,CouncilSiteID,Region,SWQLanduse,SWQAltitude,everything())
tt10w = cbind(tt10w,tt10tsw[,7:15])
tt10w = tt10w[,c(1:6,7,16,8,17,9,18,10,19,11,20,12,21,13,22,14,23,15,24)]%>%arrange(Region)
write.csv(tt10w,'h:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/FiveSitesAllTrends.csv',row.names=T)

#Get the timeseries of raw data for these sites
ECOLIfiveSite = wqdata%>%filter(LawaSiteID%in%as.character(bothMeasuresGood) & Measurement=="ECOLI")
MCIfiveSite = wqdata%>%filter(LawaSiteID%in%as.character(bothMeasuresGood) & Measurement=="MCI")
fiveSiteTwoIndicators = rbind(ECOLIfiveSite,MCIfiveSite)
par(mfrow=c(1,1))
# PlotMeATrend <- function(LSIin,measureIn,startYearIncl,stopYearIncl,period=10,
#                          mymain=paste(LSIin,riverSiteTable$CouncilSiteID[match(LSIin,riverSiteTable$LawaSiteID)],measureIn),...){
tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/WaihopaiEcoli.tif',width = 8,height=12,res=1200,units='in',compression='lzw')
par(mfrow=c(2,1))
PlotMeATrend(LSIin = 'es-00018',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,ylab="n / 100 mL")  
PlotMeATrend(LSIin = 'es-00018',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,log='y',ylimlow=10,ylab='n / 100 mL')  
dev.off()
tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/WaihopaiMCI.tif',width = 8,height=8,res=1200,units='in',compression='lzw')
par(mfrow=c(1,1))
PlotMeAMancroTrend(LSIin = 'es-00018',measureIn = "MCI",startYearIncl=2009,stopYearIncl=2018,ylab="MCI units",ylimlow=50)  
dev.off()

tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/MangatarereEcoli.tif',width = 8,height=12,res=1200,units='in',compression='lzw')
par(mfrow=c(2,1))
PlotMeATrend(LSIin = 'gw-00050',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,ylab="n / 100 mL")  
PlotMeATrend(LSIin = 'gw-00050',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,log='y',ylimlow=10,ylab='n / 100 mL')  
dev.off()
tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/MangatarereMCI.tif',width = 8,height=8,res=1200,units='in',compression='lzw')
par(mfrow=c(1,1))
PlotMeAMancroTrend(LSIin = 'gw-00050',measureIn = "MCI",startYearIncl=2009,stopYearIncl=2018,ylab="MCI units",ylimlow=50)  
dev.off()

tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/HautapuEcoli.tif',width = 8,height=12,res=1200,units='in',compression='lzw')
par(mfrow=c(2,1))
PlotMeATrend(LSIin = 'hrc-00002',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,ylab="n / 100 mL")  
PlotMeATrend(LSIin = 'hrc-00002',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,log='y',ylimlow=10,ylab='n / 100 mL')  
dev.off()
tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/HautapuMCI.tif',width = 8,height=8,res=1200,units='in',compression='lzw')
par(mfrow=c(1,1))
PlotMeAMancroTrend(LSIin = 'hrc-00002',measureIn = "MCI",startYearIncl=2009,stopYearIncl=2018,ylab="MCI units",ylimlow=50)  
dev.off()

tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/SharlandsEcoli.tif',width = 8,height=12,res=1200,units='in',compression='lzw')
par(mfrow=c(2,1))
PlotMeATrend(LSIin = 'ncc-00017',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,ylab="n / 100 mL")  
PlotMeATrend(LSIin = 'ncc-00017',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,log='y',ylimlow=1,ylab='n / 100 mL')  
dev.off()
tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/SharlandMCI.tif',width = 8,height=8,res=1200,units='in',compression='lzw')
par(mfrow=c(1,1))
PlotMeAMancroTrend(LSIin = 'ncc-00017',measureIn = "MCI",startYearIncl=2009,stopYearIncl=2018,ylab="MCI units",ylimlow=50)  
dev.off()

tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/AvonEcoli.tif',width = 8,height=12,res=1200,units='in',compression='lzw')
par(mfrow=c(2,1))
PlotMeATrend(LSIin = 'ccc-00007',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,ylab="n / 100 mL")  
PlotMeATrend(LSIin = 'ccc-00007',measureIn = "ECOLI",startYearIncl=2009,stopYearIncl=2018,log='y',ylimlow=10,ylab='n / 100 mL')  
dev.off()
tiff('H:/ericg/16666LAWA/LAWA2019/WaterQuality/riverJudge/AvonMCI.tif',width = 8,height=8,res=1200,units='in',compression='lzw')
par(mfrow=c(1,1))
PlotMeAMancroTrend(LSIin = 'lawa-100420',measureIn = "MCI",startYearIncl=2009,stopYearIncl=2018,ylab="MCI units",ylimlow=50)  
dev.off()



#########################################################################################################

tiff(paste0('h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/',format(Sys.Date(),'%Y-%m-%d'),'/Top5MCIImprovers.tif'),
     height=12,width=10,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(5,1),mar=c(2,4,1,2))
for(bestImp in 1:5){
  theseImp=wqdatafor10%>%filter(LawaSiteID==MCIfos$LawaSiteID[bestImp]&Measurement=='MCI')
  SSD_med <- summaryBy(formula=Value~LawaSiteID+monYear,
                       id=~Censored+CenType+myDate+Year+Month+Qtr+Season,
                       data=theseImp, 
                       FUN=quantile, prob=c(0.5), type=5, na.rm=TRUE, keep.name=TRUE)
  SSD_med$MCI=SSD_med$Value
  if(!length(unique(SSD_med$Season))==1){
    st <- SeasonalityTest(x = SSD_med,main="MCI",ValuesToUse = "Value",do.plot =F)
  }else{
    st=data.frame(pvalue=1)
  }
  if(!is.na(st$pvalue)&&st$pvalue<0.05){
    SeasonalKendall(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",doPlot = F)
    if(is.na(SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",ValuesToUseforMedian = "MCI",doPlot = T,ylimhi=200,ylimlow=0)$Sen_Probability)){
      SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",
               ValuesToUseforMedian = "MCI",doPlot = T,mymain = bugInfos$SiteID[bestImp],ylimhi=200,ylimlow=0)
    }else{cat('.')}
  }else{
    MannKendall(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",doPlot=F)
    SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",
             ValuesToUseforMedian = "MCI",doPlot = T,mymain = bugInfos$SiteID[bestImp],ylimhi=200,ylimlow=0)
  }
}
rm(bestImp)
if(names(dev.cur())=='tiff'){dev.off()}
par(mfrow=c(1,1))

tiff(paste0('h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/',format(Sys.Date(),'%Y-%m-%d'),'/6To10MCIImprovers.tif'),
     height=12,width=10,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(5,1),mar=c(2,4,1,2))
for(bestImp in 6:10){
  theseImp=wqdatafor10%>%filter(LawaSiteID==MCIfos$LawaSiteID[bestImp]&Measurement=='MCI')
  SSD_med <- summaryBy(formula=Value~LawaSiteID+monYear,
                       id=~Censored+CenType+myDate+Year+Month+Qtr+Season,
                       data=theseImp, 
                       FUN=quantile, prob=c(0.5), type=5, na.rm=TRUE, keep.name=TRUE)
  SSD_med$MCI=SSD_med$Value
  if(!length(unique(SSD_med$Season))==1){
    st <- SeasonalityTest(x = SSD_med,main="MCI",ValuesToUse = "Value",do.plot =F)
  }else{
    st=data.frame(pvalue=1)
  }
  if(!is.na(st$pvalue)&&st$pvalue<0.05){
    SeasonalKendall(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",doPlot = F)
    if(is.na(SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",ValuesToUseforMedian = "MCI",doPlot = T,ylimhi=200,ylimlow=0)$Sen_Probability)){
      SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",
               ValuesToUseforMedian = "MCI",doPlot = T,mymain = bugInfos$SiteID[bestImp],ylimhi=200,ylimlow=0)
    }else{cat('.')}
  }else{
    MannKendall(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",doPlot=F)
    SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "MCI",
             ValuesToUseforMedian = "MCI",doPlot = T,mymain = bugInfos$SiteID[bestImp],ylimhi=200,ylimlow=0)
  }
}
rm(bestImp)
if(names(dev.cur())=='tiff'){dev.off()}






tiff(paste0('h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/',format(Sys.Date(),'%Y-%m-%d'),'/Top5ECOLIImprovers.tif'),
     height=12,width=10,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(5,1),mar=c(2,4,1,2))
for(bestImp in 1:5){
  theseImp=wqdatafor10%>%filter(LawaSiteID==ECOLIfos$LawaSiteID[bestImp]&Measurement=='ECOLI')
  SSD_med <- summaryBy(formula=Value~LawaSiteID+monYear,
                       id=~Censored+CenType+myDate+Year+Month+Qtr+Season,
                       data=theseImp, 
                       FUN=quantile, prob=c(0.5), type=5, na.rm=TRUE, keep.name=TRUE)
  SSD_med$ECOLI=SSD_med$Value
  if(!length(unique(SSD_med$Season))==1){
    st <- SeasonalityTest(x = SSD_med,main="ECOLI",ValuesToUse = "Value",do.plot =F)
  }else{
    st=data.frame(pvalue=1)
  }
  if(!is.na(st$pvalue)&&st$pvalue<0.05){
    SeasonalKendall(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",doPlot = F)
    if(is.na(SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",
                              ValuesToUseforMedian = "ECOLI",doPlot = T,ylimlow=1)$Sen_Probability)){
      SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",
               ValuesToUseforMedian = "ECOLI",doPlot = T,mymain = bugInfos$SiteID[bestImp],ylimlow=1)
    }else{cat('.')}
  }else{
    MannKendall(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",doPlot=F)
    SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",
             ValuesToUseforMedian = "ECOLI",doPlot = T,mymain = bugInfos$SiteID[bestImp],ylimlow=1)
  }
}
rm(bestImp)
if(names(dev.cur())=='tiff'){dev.off()}
par(mfrow=c(1,1))

tiff(paste0('h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/',format(Sys.Date(),'%Y-%m-%d'),'/6To10ECOLIImprovers.tif'),
     height=12,width=10,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(5,1),mar=c(2,4,1,2))
for(bestImp in 6:10){
  theseImp=wqdatafor10%>%filter(LawaSiteID==ECOLIfos$LawaSiteID[bestImp]&Measurement=='ECOLI')
  SSD_med <- summaryBy(formula=Value~LawaSiteID+monYear,
                       id=~Censored+CenType+myDate+Year+Month+Qtr+Season,
                       data=theseImp, 
                       FUN=quantile, prob=c(0.5), type=5, na.rm=TRUE, keep.name=TRUE)
  SSD_med$ECOLI=SSD_med$Value
  if(!length(unique(SSD_med$Season))==1){
    st <- SeasonalityTest(x = SSD_med,main="ECOLI",ValuesToUse = "Value",do.plot =F)
  }else{
    st=data.frame(pvalue=1)
  }
  if(!is.na(st$pvalue)&&st$pvalue<0.05){
    SeasonalKendall(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",doPlot = F)
    if(is.na(SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",
                              ValuesToUseforMedian = "ECOLI",doPlot = T,ylimlow=1)$Sen_Probability)){
      SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",
               ValuesToUseforMedian = "ECOLI",doPlot = T,mymain = bugInfos$SiteID[bestImp],ylimlow=1)
    }else{cat('.')}
  }else{
    MannKendall(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",doPlot=F)
    SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "ECOLI",
             ValuesToUseforMedian = "ECOLI",doPlot = T,mymain = bugInfos$SiteID[bestImp],ylimlow=1)
  }
}
rm(bestImp)
if(names(dev.cur())=='tiff'){dev.off()}




#Make a table for the river board of judgment
params=unique(trendTable$Measurement)
otherTrends=matrix(data = 0,nrow=10,ncol=2*(length(params)-1)+length(c('CouncilSiteID','SiteID','region','landuse','altitude',
                                                                 'DRP','Probability','ConfCat')))
colnames(otherTrends)=c('CouncilSiteID','SiteID','Region','Landuse','Altitude',"DRP","Probability","ConfidenceCategory",
                        "NH4", "Nh4cc","TP","TPcc", "TON","TONcc", "TURB","TURBcc","ECOLI","ECOLIcc", "TN","TNcc", "BDISC","BDISCcc")
rownames(otherTrends)=bugInfos$LawaSiteID[1:10]
otherTrends <- as.data.frame(otherTrends)
otherTrends$ConfidenceCategory=factor(otherTrends$ConfidenceCategory,levels=levels(bugInfos$ConfCat))
for(cc in c(10,12,14,16,18,20,22)){
  otherTrends[,cc]=factor(otherTrends[,cc],levels=levels(bugInfos$ConfCat))
}
for(bestImp in 1:10){
  otherTrends$CouncilSiteID[bestImp]=bugInfos$CouncilSiteID[bestImp]
  otherTrends$SiteID[bestImp]=bugInfos$SiteID[bestImp]
  otherTrends$Region[bestImp]=bugInfos$Region[bestImp]
  otherTrends$Landuse[bestImp]=bugInfos$Landuse[bestImp]
  otherTrends$Altitude[bestImp]=bugInfos$Altitude[bestImp]
  otherTrends$Probability[bestImp]=format(bugInfos$Probability[bestImp],scientific = T,format='e',digits=3)
  otherTrends$ConfidenceCategory[bestImp]=bugInfos$ConfCat[bestImp]
  for(param in seq_along(params)){
    otherTrends[bestImp,match(params[param],names(otherTrends))]=round(trendTable$Percent.annual.change[
      which(trendTable$LawaSiteID==bugInfos$LawaSiteID[bestImp] &
              trendTable$Measurement==params[param])[1]],1)
    otherTrends[bestImp,match(params[param],names(otherTrends))+1]=trendTable$ConfCat[
      which(trendTable$LawaSiteID==bugInfos$LawaSiteID[bestImp] &
              trendTable$Measurement==params[param])[1]]
  }
}
otherTrends$nTrend = apply(otherTrends[,c(9,11,13,15,17,19,21)],1,FUN=function(x)sum(!is.na(x)))
write.csv(otherTrends,file=paste0('h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/',format(Sys.Date(),'%Y-%m-%d'),'/TrendsOfTopDRPSites.csv'))

rm(bestImp)


#per region top three?
ureg=unique(bugInfos$Region)
unique(wqdata$Region)[!unique(wqdata$Region)%in%unique(bugInfos$Region)]
for(thisReg in seq_along(ureg)){
  nSiteInRegWithDRPAtAll = length(unique(wqdata$LawaSiteID[which(wqdata$Region==ureg[thisReg]&wqdata$Measurement=='DRP')]))
  tiff(paste0('h:/ericg/16666LAWA/LAWA2019/WaterQuality/4.Analysis/',format(Sys.Date(),'%Y-%m-%d'),
              '/RegionalDRPImprovers',pseudo.titlecase(ureg[thisReg]),'.tif'),
       height=12,width=10,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(3,1))
  regbugInfos=bugInfos[bugInfos$Region==ureg[thisReg],]
  for(bestImp in 1:(min(c(3,dim(regbugInfos)[1])))){
    thisSite=wqdatafor10[which(wqdatafor10$LawaSiteID==regbugInfos$LawaSiteID[bestImp]),]
    SSD_med <- summaryBy(formula=Value~LawaSiteID+monYear,
                         id=~Censored+CenType+myDate+Year+Month+Qtr+Season,
                         data=thisSite, 
                         FUN=quantile, prob=c(0.5), type=5, na.rm=TRUE, keep.name=TRUE)
    SSD_med$DRP=SSD_med$Value
    st <- SeasonalityTest(x = SSD_med,main="DRP",ValuesToUse = "Value",do.plot =F)
    if(!is.na(st$pvalue)&&st$pvalue<0.05){
      SeasonalKendall(HiCensor=T,x = SSD_med,ValuesToUse = "DRP",doPlot = F)
      if(is.na(SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "DRP",ValuesToUseforMedian = "DRP",doPlot = T,
                                mymain = paste0(ureg[thisReg],": ",regbugInfos$SiteID[bestImp],' ',bestImp,' out of ',nSiteInRegWithDRPAtAll),
                                ylim=c(min(wqdatafor10$Value[wqdatafor10$LawaSiteID%in%regbugInfos$LawaSiteID[1:3]],na.rm=T),
                                       max(wqdatafor10$Value[wqdatafor10$LawaSiteID%in%regbugInfos$LawaSiteID[1:3]],na.rm=T)))$Sen_Probability)){
        SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "DRP",ValuesToUseforMedian = "DRP",doPlot = T,
                 mymain = paste0(ureg[thisReg],": ",regbugInfos$SiteID[bestImp],' ',bestImp,' out of ',nSiteInRegWithDRPAtAll),
                 ylim=c(min(wqdatafor10$Value[wqdatafor10$LawaSiteID%in%regbugInfos$LawaSiteID[1:3]],na.rm=T),
                        max(wqdatafor10$Value[wqdatafor10$LawaSiteID%in%regbugInfos$LawaSiteID[1:3]],na.rm=T)),log='')
      }else{cat('.')}
    }else{
      MannKendall(HiCensor=T,x = SSD_med,ValuesToUse = "DRP",doPlot=F)
      SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "DRP",ValuesToUseforMedian = "DRP",doPlot = T,
               mymain = paste0(ureg[thisReg],": ",regbugInfos$SiteID[bestImp],' ',bestImp,' out of ',nSiteInRegWithDRPAtAll),
               ylim=c(min(wqdatafor10$Value[wqdatafor10$LawaSiteID%in%regbugInfos$LawaSiteID[1:3]],na.rm=T),
                      max(wqdatafor10$Value[wqdatafor10$LawaSiteID%in%regbugInfos$LawaSiteID[1:3]],na.rm=T)),log='')
    }
  }
  if(names(dev.cur())=='tiff'){dev.off()}
}


#Make a table for the river board of judgment
params=unique(trendTable$Measurement)
ureg=unique(bugInfos$Region)
regionalOtherTrends=matrix(data = 0,nrow=length(ureg)*3,
                           ncol=2*(length(params)-1)+length(c('CouncilSiteID','SiteID','region','landuse','altitude','DRP','Probability','ConfCat')))
colnames(regionalOtherTrends)=c('CouncilSiteID','SiteID','Region','Landuse','Altitude',"DRP","Probability","ConfidenceCategory",
                                "NH4", "Nh4cc","TP","TPcc", "TON","TONcc", "TURB","TURBcc","ECOLI","ECOLIcc", "TN","TNcc", "BDISC","BDISCcc")

regionalOtherTrends <- as.data.frame(regionalOtherTrends)
regionalOtherTrends$ConfidenceCategory=factor(regionalOtherTrends$ConfidenceCategory,levels=levels(bugInfos$ConfCat))
for(cc in c(10,12,14,16,18,20,22)){
  otherTrends[,cc]=factor(otherTrends[,cc],levels=levels(bugInfos$ConfCat))
}
tableInsertRow=1
for(thisReg in seq_along(ureg)){
  regbugInfos=bugInfos[bugInfos$Region==ureg[thisReg],]
  regOtherTrendInfos=trendTable%>%filter(standard=='gold',Region==ureg[thisReg])
  
  for(bestImp in 1:(min(c(3,dim(regbugInfos)[1])))){
    rownames(regionalOtherTrends)[tableInsertRow]=as.character(regbugInfos$LawaSiteID[bestImp])
    regionalOtherTrends$CouncilSiteID[tableInsertRow]=regbugInfos$CouncilSiteID[bestImp]
    regionalOtherTrends$SiteID[tableInsertRow]=regbugInfos$SiteID[bestImp]
    regionalOtherTrends$Region[tableInsertRow]=regbugInfos$Region[bestImp]
    regionalOtherTrends$Landuse[tableInsertRow]=regbugInfos$Landuse[bestImp]
    regionalOtherTrends$Altitude[tableInsertRow]=regbugInfos$Altitude[bestImp]
    regionalOtherTrends$Probability[tableInsertRow]=format(regbugInfos$Probability[bestImp],scientific = T,format='e',digits=3)
    regionalOtherTrends$ConfidenceCategory[tableInsertRow]=regbugInfos$ConfCat[bestImp]
    #Pull in the data for other trends, from regOtherTrendInfos
    for(param in seq_along(params)){
      regionalOtherTrends[tableInsertRow,match(params[param],names(regionalOtherTrends))]=round(regOtherTrendInfos$Percent.annual.change[
        which(regOtherTrendInfos$LawaSiteID==regbugInfos$LawaSiteID[bestImp] &
                regOtherTrendInfos$Measurement==params[param])[1]],1)
      regionalOtherTrends[tableInsertRow,match(params[param],names(regionalOtherTrends))+1]=regOtherTrendInfos$ConfCat[
        which(regOtherTrendInfos$LawaSiteID==regbugInfos$LawaSiteID[bestImp] &
                regOtherTrendInfos$Measurement==params[param])[1]]
    }
    tableInsertRow=tableInsertRow+1
  }
}
regionalOtherTrends$nTrend = apply(regionalOtherTrends[,c(9,11,13,15,17,19,21)],1,FUN=function(x)sum(!is.na(x)))
write.csv(regionalOtherTrends,file=paste0('h:/ericg/16666LAWA/LAWA2019/WaterQuality/4.Analysis/',format(Sys.Date(),'%Y-%m-%d'),
                                          '/TrendsOfRegionalTopDRPSites.csv'))

rm(bestImp)
