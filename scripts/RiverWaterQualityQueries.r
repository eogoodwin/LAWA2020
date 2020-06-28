#Kati query 21 Feb 2019
#How many sites have very likely improving trends in both MCI and ECOLI over ten years?
rm(list=ls())
library(tidyverse)
wqTrends = read.csv("H:/ericg/16666LAWA/2018/WaterQuality/4.Analysis/2018-10-04/RiverWQ_Trend_ForITE17h22m-04Oct2018.csv")
ecoliTrendsVLI = wqTrends%>%filter(parameter=="ECOLI"&TrendScore==2&period==10)

MCITrends = read.csv("H:/ericg/16666LAWA/2018/MacroInvertebrates/4.Analysis/2018-09-26/MacroMCI_Trend_ForITE14h00m-26Sep2018.csv")
MCITrendsVLI = MCITrends%>%filter(TrendScore==2&period==10)

intersect(ecoliTrendsVLI$LawaSiteID,MCITrendsVLI$LawaSiteID)
#Four!


#How many sites have both EColi trends and MCI trends calculated
ecoliTrends = wqTrends%>%filter(parameter=="ECOLI"&TrendScore!=(-99)&period==10)
MCITrends = MCITrends%>%filter(period==10&TrendScore!=(-99))

length(intersect(ecoliTrends$LawaSiteID,MCITrends$LawaSiteID))
sort(intersect(ecoliTrends$LawaSiteID,MCITrends$LawaSiteID))
trendySites=intersect(ecoliTrends$LawaSiteID,MCITrends$LawaSiteID)

commonTrend=data.frame(LawaSiteID=trendySites,
  MCI=MCITrends$TrendScore[match(trendySites,MCITrends$LawaSiteID)],
                       EColi=ecoliTrends$TrendScore[match(trendySites,ecoliTrends$LawaSiteID)])
table(commonTrend$MCI,commonTrend$EColi)
table(commonTrend[,-1])

strTo=function(s,c=':'){
  cpos=str_locate(string = s,pattern = c)[1]
  substr(s,1,cpos-1)
}

commonTrend$region = sapply(commonTrend$LawaSiteID,strTo,'-')
table(commonTrend$region)


#11 March 2019
#Email from KD what are the sites with EColi trend improving and MCI trend improving?
which(commonTrend$MCI>0 & commonTrend$EColi>0)

sort(commonTrend$LawaSiteID[which(commonTrend$MCI>0 & commonTrend$EColi>0)])

