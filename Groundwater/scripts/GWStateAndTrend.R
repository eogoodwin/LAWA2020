#Groudnwater state analysis
rm(list=ls())
# library(readxl)
library(tidyverse)
source('H:/ericg/16666LAWA/LAWA2019/Scripts/LAWAFunctions.R')
EndYear <- year(Sys.Date())-1
startYear5 <- EndYear - 5+1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1

# GWdata = read.csv('h:/ericg/16666LAWA/LAWA2019/GW/Data/2019-09-10/MfEExport_20190902.csv',stringsAsFactors = F)

GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2019-09-10/GWQImport_20190902.xlsx",sheet="TS Readings")%>%as.data.frame

GWdata <- GWdata%>%mutate(LawaSiteID=`LAWA Site Id`,
                          Measurement=`Parameter Name`,
                          Region=`Region Name`,
                          Date = `Value date`,
                          value=`Numeric value`,
                          siteMeas=paste0(LawaSiteID,'.',Measurement))

#Some LawaSiteIDs went by two different names.  Often they were similarity.
#This finds the commonality and uses that
#It has a side effect of dropping repeated words. Toi Toi St would become Toi St.
SiteID=split(GWdata,GWdata$LawaSiteID)%>%
  purrr::map(~paste(Reduce(intersect,x = strsplit(c(.$`Site Id`,.$`Monitoring Site Id`),' ')),collapse=' '))
GWdata$SiteID=SiteID[GWdata$LawaSiteID]
ambigSet = GWdata%>%filter(SiteID!=`Site Id`)%>%select(LawaSiteID)%>%distinct%>%unlist
GWdata%>%filter(LawaSiteID%in%ambigSet)%>%select(LawaSiteID,`Site Id`,`Monitoring Site Id`,SiteID)%>%arrange(LawaSiteID)%>%distinct

GWdata <- GWdata%>%select(-`Site Id`,-`Monitoring Site Id`)%>%distinct
#124598

GWdata$`Value prefix` <- ifelse(grepl(pattern = '^[<|>]',GWdata$`Text value`),
                                substr(x = GWdata$`Text value`,1,1),'')
GWdata$value[which(GWdata$`Value prefix`=='<')] <- GWdata$`Numeric value`[which(GWdata$`Value prefix`=='<')]*0.5
GWdata$value[which(GWdata$`Value prefix`=='>')] <- GWdata$`Numeric value`[which(GWdata$`Value prefix`=='>')]*1.1

GWdata$Censored=FALSE
GWdata$Censored[grepl(pattern = '^[<|>]',GWdata$`Text value`)]=TRUE
GWdata$CenType='not'
GWdata$CenType[grepl(pattern = '^<',GWdata$`Text value`)]='lt'
GWdata$CenType[grepl(pattern = '^>',GWdata$`Text value`)]='gt'

table(GWdata$Region,GWdata$Measurement)

noData = which(is.na(GWdata$`Value date`)&is.na(GWdata$`Text value`))
GWdata <- GWdata[-noData,]
rm(noData)
#121478

#FreqCheck expects a one called "Date"
GWdata$myDate <- as.Date(as.character(GWdata$Date))
GWdata <- GetMoreDateInfo(GWdata)
GWdata$monYear = format(GWdata$myDate,"%b-%Y")
GWdata <- GWdata%>%dplyr::rename(Value=value)

freqs <- split(x=GWdata,f=GWdata$siteMeas)%>%purrr::map(~freqCheck(.))%>%unlist
table(freqs)
# freqs
# bimonthly   monthly quarterly 
# 31       243      3751 
GWdata$Frequency=freqs[GWdata$siteMeas]  
rm(freqs)
# GWdata <- GWdata%>%select(-siteMeas)


#Carl Hanson 11/9/2019:  We only need state and trend for five parameters: 
#    nitrate nitrogen, chloride, DRP, electrical conductivity and E. coli.

#Calculate state, median ####

#Get a median value per site/Measurement combo
periodYrs=5
GWmedians <- GWdata%>%filter(Measurement%in%c("Nitrate nitrogen","Chloride",
                                              "Dissolved reactive phosphorus",
                                              "Electrical conductivity/salinity",
                                              "E.coli","Ammoniacal nitrogen"))%>%
  group_by(LawaSiteID,Measurement)%>%
   dplyr::filter(lubridate::year(myDate)>(EndYear-periodYrs+1)&lubridate::year(myDate)<=EndYear)%>%
  dplyr::summarise(median=quantile(Value,probs = 0.5,type=5,na.rm=T),
                   MAD = quantile(abs(median-Value),probs=0.5,type=5,na.rm=T),
                   count = n(),
                   n2018 = sum(lubridate::year(myDate)==2018),
                   censoredCount = sum(`Value prefix`!=''),
                   Frequency=unique(Frequency))%>%ungroup%>%
  dplyr::rename(LawaSiteID=LawaSiteID,Measurement=Measurement)%>%
  mutate(censoredPropn = censoredCount/count)

GWmedians$meas = factor(GWmedians$Measurement,labels=c("Cl","DRP","ECOLI","NaCl","NO3"))

plot(GWmedians$median,GWmedians$MAD,log='xy',xlab='Median',ylab='MAD',
     col=as.numeric(factor(GWmedians$Measurement)),pch=16,cex=1)
plot(GWmedians$median,GWmedians$MAD/GWmedians$median,log='x',xlab='Median',ylab='MAD/Median',
     col=as.numeric(factor(GWmedians$Measurement)),pch=16,cex=1)
abline(0,1)
GWmedians$Exclude<-FALSE
GWmedians$Exclude[tolower(GWmedians$Frequency)=="monthly" & GWmedians$count<(0.75*12*periodYrs)] <- TRUE    #out of 5*12 = 60
GWmedians$Exclude[tolower(GWmedians$Frequency)=="bimonthly" & GWmedians$count<(0.75*6*periodYrs)] <- TRUE   #50% of 6 bimonthlys per year * 5 years
GWmedians$Exclude[tolower(GWmedians$Frequency)=="quarterly" & GWmedians$count<(0.75*4*periodYrs)] <- TRUE    #10 out of 5*4 = 20
table(GWmedians$Exclude)
# FALSE  TRUE 
# 856    1218
GWmedians$Exclude[tolower(GWmedians$Frequency)=="monthly" & GWmedians$n2018<(0.75*12)] <- TRUE
GWmedians$Exclude[tolower(GWmedians$Frequency)=="bimonthly" & GWmedians$n2018<(0.75*6)] <- TRUE   
GWmedians$Exclude[tolower(GWmedians$Frequency)=="quarterly" & GWmedians$n2018<(0.75*4)] <- TRUE
table(GWmedians$Exclude)
# FALSE  TRUE 
# 819  1255
par(mfrow=c(3,2))
plot(GWmedians$median,GWmedians$MAD/(GWmedians$median*GWmedians$count^0.5),
     log='xy',pch=c(1,16)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
GWmedians%>%split(GWmedians$Measurement)%>%
  purrr::map(~plot(.$median,.$MAD/(.$median*.$count^0.5),
                   log='xy',pch=c(1,16)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count)))

par(mfrow=c(3,2))
plot(GWmedians$median,GWmedians$MAD/(GWmedians$median),
     log='xy',lwd=c(2,1)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
abline(h=1)
GWmedians%>%split(GWmedians$Measurement)%>%
  purrr::map(~{plot(.$median,.$MAD/(.$median),
                   log='xy',lwd=c(2,1)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count),ylim=c(0.001,5))
             abline(h=1)})

par(mfrow=c(3,2))
plot(GWmedians$median,GWmedians$MAD,
     log='xy',pch=c(1,16)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
abline(0,1)
GWmedians%>%split(GWmedians$Measurement)%>%
  purrr::map(~{plot(.$median,.$MAD,
                   log='xy',pch=c(1,16)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count))
    abline(0,1)})

table(GWmedians$count)
with(GWmedians%>%filter(Exclude),points(median,MAD,pch=16,col='white',cex=0.5))
GWmedians <- GWmedians%>%filter(!Exclude)
table(GWmedians$count)
table(GWmedians$count,GWmedians$meas)

par(mfrow=c(1,1))
plot(density(GWmedians$count,from=min(GWmedians$count)),log='x',xlim=range(GWmedians$count))

uMeasures=unique(GWmedians$Measurement)

par(mfrow=c(2,3))
for(measure in uMeasures){
  dtp = GWmedians%>%filter(Measurement==measure,median>=0)
  rtp = GWdata%>%filter(Measurement==measure,Value>=0)
  if(!measure%in%('Water Level')){
    if(measure%in%c('Ammoniacal nitrogen','E.coli')){adjust=2}else{adjust=1}
    plot(density(log(dtp$median),na.rm=T,adjust=adjust),main=measure,xaxt='n',xlab='')
    lines(density(log(rtp$Value),na.rm=T,adjust=2*adjust),col='grey')
    rug(log(dtp$median))
    axis(side = 1,at = pretty(log(dtp$median)),labels=signif(exp(pretty(log(dtp$median))),2))
  }else{
    plot(density(dtp$median,na.rm=T),main=measure,xlab='')
    rug(dtp$median)
    lines(density(rtp$Value,na.rm=T),col='grey')
  }
}


#pick a site, pick a measurement

site=sample(x = unique(GWmedians$LawaSiteID),size = 1)
meas=sample(x = unique(GWmedians$Measurement),size = 1)
if(length(which(GWmedians$LawaSiteID==site&GWmedians$Measurement==meas))>0){
  toPlot = GWdata%>%filter(LawaSiteID==site&Measurement==meas)%>%select(Value)%>%drop_na%>%unlist
  plot(density(toPlot,na.rm=T,from=0),xlab='',main=paste(site,meas))
  rug(toPlot)
  abline(v = GWmedians%>%filter(LawaSiteID==site&Measurement==meas)%>%select(median))
}







#Cranculate treds ####
GWtrendData <- GWdata%>%filter(Measurement%in%c("Nitrate nitrogen","Chloride",
                                                "Dissolved reactive phosphorus",
                                                "Electrical conductivity/salinity",
                                                "E.coli","Ammoniacal nitrogen"))
library(parallel)
library(doParallel)

workers <- makeCluster(2)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
})
foreach(pn = c(0.75,0.8),.combine=rbind,.errorhandling="stop")%dopar%{
#for(pn in c(0.25,0.5,0.625,0.75,0.8,0.9)){
  treds15 <- split(GWtrendData,GWtrendData$siteMeas)%>%purrr::map(~trendCore(.,periodYrs=15,proportionNeeded=pn)) #Proportion needed is greater or equal
  treds15=do.call(rbind.data.frame,treds15)
  row.names(treds15)=NULL
  
  treds10 <- split(GWtrendData,GWtrendData$siteMeas)%>%purrr::map(~trendCore(.,periodYrs=10,proportionNeeded=pn))
  treds10=do.call(rbind.data.frame,treds10)
  row.names(treds10)=NULL
  
  # treds5 <- split(GWtrendData,GWtrendData$siteMeas)%>%purrr::map(~trendCore(.,periodYrs=5,proportionNeeded=pn))
  # treds5=do.call(rbind.data.frame,treds5)
  # row.names(treds5)=NULL
  GWtrends=rbind(treds10,treds15) #treds5,
  return(GWtrends)
}->GWtrends
stopCluster(workers)
rm(workers)

GWtrends$ConfCat <- cut(GWtrends$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
GWtrends$ConfCat=factor(GWtrends$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
GWtrends$TrendScore=as.numeric(GWtrends$ConfCat)-3
GWtrends$TrendScore[is.na(GWtrends$TrendScore)]<-(NA)

fGWt = GWtrends%>%filter(!grepl('^unassess',GWtrends$frequency)&!grepl('^Insufficient',GWtrends$AnalysisNote))
#    nitrate nitrogen, chloride, DRP, electrical conductivity and E. coli.
# fGWt = fGWt%>%filter(Measurement %in% c("Nitrate nitrogen","Chloride","Dissolved reactive phosphorus",
#                                         "Electrical conductivity/salinity","E.coli"))

table(GWtrends$proportionNeeded,GWtrends$period)
knitr::kable(table(fGWt$proportionNeeded,fGWt$period),format='rst')

with(fGWt[fGWt$proportionNeeded==0.75,],knitr::kable(table(AnalysisNote,period),format='rst'))

knitr::kable(with(fGWt%>%filter(period==10 & proportionNeeded==0.75)%>%droplevels,table(Measurement,TrendScore)),format='rst')

table(GWtrends$numQuarters,GWtrends$period)

table(GWtrends$Measurement,GWtrends$ConfCat,GWtrends$proportionNeeded,GWtrends$period)
table(fGWt$Measurement,fGWt$ConfCat,fGWt$proportionNeeded,fGWt$period)

# with(GWtrends[GWtrends$period==5,],table(Measurement,ConfCat))
with(GWtrends[GWtrends$period==10,],table(Measurement,ConfCat))
with(GWtrends[GWtrends$period==15,],table(Measurement,ConfCat))

table(GWtrends$Measurement,GWtrends$period)
table(GWtrends$ConfCat,GWtrends$period)


knitr::kable(table(GWtrends$ConfCat,GWtrends$Measurement,GWtrends$period))




#Put the state on the trend?
#
GWtrends$Calcmedian = GWmedians$median[match(x = paste(GWtrends$LawaSiteID,GWtrends$Measurement),
                                             table = paste(GWmedians$LawaSiteID,GWmedians$Measurement))]
GWtrends$Calcmedian[GWtrends$Calcmedian<=0] <- NA
par(mfrow=c(2,3))
for(meas in unique(GWtrends$Measurement)){
  dfp=GWtrends%>%dplyr::filter(Measurement==meas)
  plot(factor(dfp$ConfCat),dfp$Calcmedian,main=meas,log='y')
}

head(GWmedians%>%filter(!Exclude)%>%select(-Exclude))

