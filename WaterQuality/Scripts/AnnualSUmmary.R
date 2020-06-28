#Annual Summary
rm(list=ls())
library(tidyverse)
source("H:/ericg/16666LAWA/LAWA2019/Scripts/LAWAFunctions.R")
#Pull in land use from LCDB
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')
source('h:/ericg/16666LAWA/LAWA2019/scripts/spineplotEG.R')

Include99s = F
PlotNumbersOnPlots=T
savePlots=F

rec=read_csv("E:/RiverData/RECnz.txt")
rec$groupedREC=rec$LANDCOVER
rec$groupedREC[rec$LANDCOVER%in%c("EF","IF","S","T","W")] <- "forest"
rec$groupedREC[rec$LANDCOVER%in%c("P","B","M")] <- "rural"
rec$groupedREC[rec$LANDCOVER%in%c("U")] <- "urban"

rec%>%group_by(groupedREC)%>%dplyr::summarise(lngh=sum(LENGTH,na.rm=T)/1000,count=n())


#Prep site table ####

riverSiteTable = loadLatestSiteTableRiver()

macroSiteTable = loadLatestSiteTableMacro()
macrositeTableRawLandUse = read.csv("H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/Metadata/SiteTableRawLandUse.csv",stringsAsFactors = F)
macroSiteTable$SWQLanduse[which(is.na(macroSiteTable$SWQLanduse))]=
  tolower(macrositeTableRawLandUse$SWQLanduse[match(macroSiteTable$LawaSiteID[which(is.na(macroSiteTable$SWQLanduse))],
                                                    macrositeTableRawLandUse$LawaSiteID)])
rm(macrositeTableRawLandUse)

macroSiteTable$SWQLanduse[which(macroSiteTable$SWQLanduse%in%c('','unstated'))] <- 
  riverSiteTable$SWQLanduse[match(macroSiteTable$LawaSiteID[which(macroSiteTable$SWQLanduse=='')],
                                  riverSiteTable$LawaSiteID)]
macroSiteTable$SWQLanduse[which(is.na(macroSiteTable$SWQLanduse))] <- tolower(macroSiteTable$Landcover[which(is.na(macroSiteTable$SWQLanduse))])
macroSiteTable$SWQLanduse=tolower(macroSiteTable$SWQLanduse)

#Prep trend table ####
wqtrend=read_csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis",
                          pattern="RiverWQ_Trend*.*",
                          full.names=T,recursive=T,ignore.case=T),1),guess_max = 20000)%>%select(-Median)


#(Headed back to prepWFS to add rec streamorders back tot ribersite table)
wqtrend$Order=siteTable$Order[match(wqtrend$LawaSiteID,siteTable$LawaSiteID)]
wqtrend$Order[which(is.na(wqtrend$Order))]=macroSiteTable$Order[match(wqtrend$LawaSiteID[which(is.na(wqtrend$Order))],
                                                                      macroSiteTable$LawaSiteID)]



#Add land use to missing rows
wqtrend$SWQLanduse <- tolower(wqtrend$SWQLanduse)
table((wqtrend$SWQLanduse))
wqtrend$SWQLanduse[which(is.na(wqtrend$SWQLanduse))] <-
  macroSiteTable$SWQLanduse[match(wqtrend$LawaSiteID[which(is.na(wqtrend$SWQLanduse))],
                                  macroSiteTable$LawaSiteID)]

#Export for erm for um riverJudgementDay and for abi's pivot table ####
if(0){
trendOut=wqtrend
trendOut$nReq = trendOut$period*12*0.9
trendOut$nReq[which(trendOut$frequency=='quarterly')] <- 
  trendOut$period[which(trendOut$frequency=='quarterly')]*4*0.9

trendOut$AnalysisNote[which(trendOut$frequency=='unassessed'|is.na(trendOut$frequency))] <-  
  paste0("NA n=",trendOut$numMonths[which(trendOut$frequency=='unassessed'|is.na(trendOut$frequency))],'/',
         trendOut$nReq[which(trendOut$frequency=='unassessed'|is.na(trendOut$frequency))])
trendOut$CouncilSiteID <- riverSiteTable$CouncilSiteID[match(trendOut$LawaSiteID,riverSiteTable$LawaSiteID)]
trendOut$CouncilSiteID[which(is.na(trendOut$CouncilSiteID))] <- macroSiteTable$CouncilSiteID[match(trendOut$LawaSiteID[which(is.na(trendOut$CouncilSiteID))],
                                                                                                macroSiteTable$LawaSiteID)]
trendOut$SiteID <- riverSiteTable$SiteID[match(trendOut$LawaSiteID,riverSiteTable$LawaSiteID)]
trendOut$SiteID[which(is.na(trendOut$SiteID))] <- macroSiteTable$SiteID[match(trendOut$LawaSiteID[which(is.na(trendOut$SiteID))],
                                                                           macroSiteTable$LawaSiteID)]
trendOut <- trendOut%>%select(Region,Agency,Measurement,period,LawaSiteID,CouncilSiteID,SiteID,
                               nReq,nMeasures,nObs,numMonths,numQuarters,numYears,frequency,Observations,everything(),
                               -standard,-nFirstYear,-nLastYear,-nObs)
trendOut$SWQAltitude=tolower(trendOut$SWQAltitude)
write.csv(trendOut,'h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/TrendResults.csv',row.names = F)
}


# 1)  Sumary statitswics on site counts
length(unique(tolower(wqtrend$LawaSiteID)))  #1456
wqtrend%>%filter(Measurement!="MCI")%>%select(LawaSiteID)%>%unlist%>%unique%>%length  #986
wqtrend%>%filter(Measurement=="MCI")%>%select(LawaSiteID)%>%unlist%>%unique%>%length  #981
MCIsites=wqtrend%>%filter(Measurement=="MCI")%>%select(LawaSiteID)%>%unlist%>%unique
WQsites=wqtrend%>%filter(Measurement!="MCI")%>%select(LawaSiteID)%>%unlist%>%unique
MCIsites[MCIsites%in%WQsites]%>%length #511 in common
MCIsites[!MCIsites%in%WQsites]%>%length #470 unique to MCI
WQsites[!WQsites%in%MCIsites]%>%length  #475 unique to WQ

#Region counts
knitr::kable(wqtrend%>%select(LawaSiteID,Region)%>%distinct%>%select(Region)%>%table)

#Measure counts
knitr::kable(wqtrend%>%select(LawaSiteID,Region,Measurement)%>%distinct%>%select(Region,Measurement)%>%table)

#Duratino counts
knitr::kable(wqtrend%>%select(LawaSiteID,period)%>%distinct%>%select(period)%>%table)

#Land use counts
knitr::kable(wqtrend%>%select(LawaSiteID,SWQLanduse)%>%distinct%>%select(SWQLanduse)%>%table)
knitr::kable(wqtrend%>%select(LawaSiteID,Region,SWQLanduse)%>%distinct%>%select(Region,SWQLanduse)%>%table)

#Order counts
knitr::kable(wqtrend%>%select(LawaSiteID,Order)%>%distinct%>%select(Order)%>%table)
knitr::kable(wqtrend%>%select(LawaSiteID,Region,Order)%>%distinct%>%select(Region,Order)%>%table)


#****************************************************************
if(!Include99s){
  wqtrend$TrendScore[which(wqtrend$TrendScore==(-99))] <- NA
  wqtrend <- wqtrend%>%drop_na(TrendScore)
}else{
  wqtrend$TrendScore[which(is.na(wqtrend$TrendScore))] <- -99
}
#****************************************************************

length(unique(tolower(wqtrend$LawaSiteID)))  #1127
wqtrend%>%filter(Measurement!="MCI")%>%select(LawaSiteID)%>%unlist%>%unique%>%length  #908
wqtrend%>%filter(Measurement=="MCI")%>%select(LawaSiteID)%>%unlist%>%unique%>%length  #550
MCIsites=wqtrend%>%filter(Measurement=="MCI")%>%select(LawaSiteID)%>%unlist%>%unique
WQsites=wqtrend%>%filter(Measurement!="MCI")%>%select(LawaSiteID)%>%unlist%>%unique
MCIsites[MCIsites%in%WQsites]%>%length #331 in common
MCIsites[!MCIsites%in%WQsites]%>%length #219 unique to MCI
WQsites[!WQsites%in%MCIsites]%>%length  #577 unique to WQ

#Region counts
knitr::kable(wqtrend%>%select(LawaSiteID,Region)%>%distinct%>%select(Region)%>%table)

#Measure counts
knitr::kable(wqtrend%>%select(LawaSiteID,Region,Measurement)%>%distinct%>%select(Region,Measurement)%>%table)

#Duratino counts
knitr::kable(wqtrend%>%select(LawaSiteID,period)%>%distinct%>%select(period)%>%table)

#Land use counts
knitr::kable(wqtrend%>%select(LawaSiteID,SWQLanduse)%>%distinct%>%select(SWQLanduse)%>%table)
knitr::kable(wqtrend%>%select(LawaSiteID,Region,SWQLanduse)%>%distinct%>%select(Region,SWQLanduse)%>%table)

#Order counts
knitr::kable(wqtrend%>%select(LawaSiteID,Order)%>%distinct%>%select(Order)%>%table)
knitr::kable(wqtrend%>%select(LawaSiteID,Region,Order)%>%distinct%>%select(Region,Order)%>%table)







#****************************************************************
wqtrend$TrendScore=droplevels(factor(wqtrend$TrendScore,levels=c(-2,-1,0,1,2,-99)))
nTrendClass=length(levels(wqtrend$TrendScore))
wqtrend$ConfCat=factor(wqtrend$ConfCat,levels=c("Very likely degrading","Likely degrading","Indeterminate","Likely improving","Very likely improving" ))
#****************************************************************





#Prep state table ####
wqmedians <- read_csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis",
                               pattern = 'AuditRiverSTATE',
                               full.names = T,recursive=T,ignore.case=T),
                           1),guess_max = 10000)%>%
  filter(ComparisonGroup=='site|All')%>%
  transmute(LawaSiteID=LawaID,
            Measurement=Parameter,
            Median=Median,
            Quartile=factor(StateScore,levels=c("4","3","2","1")),
            revQuartile = factor(Quartile,levels=c("1","2","3","4")))


MCImed=read_csv(tail(dir(path='H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/Analysis',
                         pattern="State",
                         full.names=T,recursive=T,ignore.case=T),1))%>%
  filter(Parameter=="MCI")%>%
  transmute(LawaSiteID=LAWAID,
            Measurement=Parameter,
            Median,
            Quartile=factor(Hmisc::cut2(x=Median,cuts=quantile(Median,probs = c(0,0.25,0.5,0.75,1))),
                            labels=rev(c('1','2','3','4'))),
            revQuartile = factor(Quartile,levels=c("1","2","3","4")))

wqmedians = rbind(wqmedians,MCImed%>%select(names(wqmedians)))

#Now canculate the MCI bad
MCImed$MCIband = Hmisc::cut2(x = MCImed$Median,cuts = c(0,80,100,120,200),levels.mean=T,digits=0)
MCImed$MCIband = factor(MCImed$MCIband,labels=rev(c("A","B","C","D")))
MCImed$MCIband = factor(MCImed$MCIband,levels=c("A","B","C","D"))
MCIband=MCImed
rm(MCImed)
wqmedians$MCIband = MCIband$MCIband[match(wqmedians$LawaSiteID,MCIband$LawaSiteID)]
wqmedians$MCIband[wqmedians$Measurement!="MCI"] <- NA
rm(MCIband)


#Get details on quartile boundaries ####
if(0){
  wqmedians%>%drop_na(Quartile)%>%group_by(Measurement,Quartile)%>%
  dplyr::summarise(upperBound=signif(max(Median,na.rm=T),2),
                                     lowerBound=signif(min(Median,na.rm=T),2))%>%
  as.data.frame%>%write.csv('./annualSummary/QuartileBoundaries_full.csv',row.names = F)
}


#Merge state and trend ####
wqt=left_join(wqtrend,wqmedians,by=c("LawaSiteID","Measurement"))
# rm(wqmedians)
wqt$Measurement <- factor(wqt$Measurement,
                          levels=c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")) 

if(0){
  wqt%>%drop_na(Quartile)%>%group_by(Measurement,Quartile)%>%
    dplyr::summarise(upperBound=signif(max(Median,na.rm=T),2),
                     lowerBound=signif(min(Median,na.rm=T),2))%>%
    as.data.frame%>%write.csv('./annualSummary/QuartileBoundaries_trendOnly.csv',row.names = F)
}


if(Include99s){
  wqt$AnalysisNote[wqt$numMonths<108&wqt$numQuarters<36] <- '<36 quarters & <108 months'
  wqt$AnalysisNote[wqt$nMeasures==0] <- 'no data'
  
  wqt%>%filter(Measurement=="BDISC")%>%select(AnalysisNote)%>%table
  wqt%>%select(AnalysisNote)%>%table%>%knitr::kable(format='rst')
  wqt%>%select(AnalysisNote,Quartile)%>%table%>%knitr::kable(format='rst')
}


uMeasures = levels(wqt$Measurement)
measLab=c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
          'DRP',expression(italic(E.~coli)),'MCI')[c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")%in%
                                                     uMeasures]

#Ten-year trends, quartiles pooled, by parameter ####

tfp = wqt%>%dplyr::filter((period==10))
nMeas=length(unique(tfp$Measurement))

tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/AnnualSummary/TenYearTrendM&Qa.tiff',
     width = 12,height=12,units='in',res=600,compression='lzw',type='cairo')
par(mfrow=c(1,1))
colMPs=-0.5+(1:nMeas)*1.2
tb <- spinePlotEG(factor(tfp$Measurement),
                  factor(tfp$TrendScore),xlab='',ylab='',
                  col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #
                  main="Ten year trends (M&Q)")
dev.off()

#Export data for openLab
write.csv(tfp%>%select(Measurement,TrendScore),'h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/TenYearTrendM&Qa.csv',row.names=F)

tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
mbp <- rbind(rep(0,nMeas),mbp)
mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2

if(0){
tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/AnnualSummary/TenYearTrendM&Qb.tiff',
     width = 12,height=12,units='in',res=600,compression='lzw',type='cairo')
par(mfrow=c(1,1),mar=c(8,5,6,4))
barplot(tbp,main="Ten year trends (M&Q)",las=2,ylab='',xlab='',
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
axis(side = 1,at=colMPs,labels = measLab,las=2)
axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
for(cc in 1:nMeas){
  par(xpd=NA)
  text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=0.75)
  # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
  text(colMPs[cc],1.025,sum(tb[cc,]))
}
dev.off()

tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/AnnualSummary/TenYearTrendM&Qc.tiff',
     width = 12,height=12,units='in',res=600,compression='lzw',type='cairo')
par(mfrow=c(1,1),mar=c(8,5,6,4))
barplot(tbp,main="Ten year trends (M&Q)",las=2,ylab='',xlab='',
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
axis(side = 1,at=colMPs,labels = measLab,las=2)
dev.off()
}

emf('h:/ericg/16666LAWA/LAWA2019/WaterQuality/AnnualSummary/TenYearTrendM&Qd.emf',emfPlus=FALSE)
par(mfrow=c(1,1),mar=c(8,5,6,4))
barplot(tbp,main="",las=2,ylab='',xlab='',
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
axis(side = 1,at=colMPs,labels = measLab,las=2)
axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
par(xpd=NA)
for(cc in 1:nMeas){
  text(colMPs[cc],1.025,sum(tb[cc,]))
}
dev.off()

#Export data for openLab
write.csv(rbind(tbp,t(tb)),'h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/TenYearTrendM&Qd.csv',row.names=F)

library(devEMF)

#Ten year trends, plot per parameter, bar per quartile ####
qabels=c('1st','2nd','3rd','4th')
if(0){
  tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/AnnualSummary/TenYearTrendM&QParamSepa.tiff',
     width = 12,height=12,units='in',res=600,compression='lzw',type='cairo')
par(mfrow=c(3,3),mar=c(4,4,3,2),cex.main=2,cex.lab=2,cex.axis=2)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==measure))%>%
    droplevels
  colMPs=-0.5+(1:4)*1.2
  tb <- spinePlotEG(factor(tfp$Quartile,levels=c("1","2","3","4")),
                    factor(tfp$TrendScore),tol.ylab=0,off=0,ylab='',xlab='',
                    col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #,
                    main=ifelse(measure=="MCI","Ten year trends MCI",paste("Ten year trends (M&Q)",measure)))
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,4),mbp)
  mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
}
dev.off()

tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/AnnualSummary/TenYearTrendM&QParamSepb.tiff',
     width = 12,height=12,units='in',res=600,compression='lzw',type='cairo')
par(mfrow=c(3,3),mar=c(4,2,5,2),cex.main=2,cex.lab=2,cex.axis=2)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==measure))%>%
    droplevels
  colMPs=-0.5+(1:4)*1.2
  tb <- spinePlotEG(factor(tfp$Quartile,levels=c("1","2","3","4")),
                    factor(tfp$TrendScore),tol.ylab=0,off=0,ylab='',xlab='',
                    col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #,
                    main=ifelse(measure=="MCI","Ten year trends MCI",paste("Ten year trends (M&Q)",measure)),doPlot=F)
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,4),mbp)
  mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
  barplot(tbp,main="",las=2,
          col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n')
  title(main = ifelse(measure=="MCI","Ten year trends MCI",paste("Ten year trends (M&Q)",measure)),line = 2)
  axis(side = 1,at=colMPs,labels = qabels,las=2)
  if(PlotNumbersOnPlots){
    for(cc in 1:4){
      par(xpd=NA)
      text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=1.5)
      text(colMPs[cc],1.05,sum(tb[cc,]),cex=1.5)
    }
  }
}
dev.off()

tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/AnnualSummary/TenYearTrendM&QParamSepc.tiff',
     width = 12,height=12,units='in',res=600,compression='lzw',type='cairo')
par(mfrow=c(3,3),mar=c(4,2,5,2),cex.main=2,cex.lab=2,cex.axis=2)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==measure))%>%
    droplevels
  colMPs=-0.5+(1:4)*1.2
  tb <- spinePlotEG(factor(tfp$Quartile,levels=c("1","2","3","4")),
                    factor(tfp$TrendScore),tol.ylab=0,off=0,ylab='',xlab='',
                    col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #,
                    main=ifelse(measure=="MCI","Ten year trends MCI",paste("Ten year trends (M&Q)",measure)),doPlot=F)
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,4),mbp)
  mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
  barplot(tbp,main="",las=2,
          col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n')
  title(main = ifelse(measure=="MCI","Ten year trends MCI",paste("Ten year trends (M&Q)",measure)),line = 2)
  axis(side = 1,at=colMPs,labels = qabels,las=2)
}
dev.off()
}
frex=NULL
emf('h:/ericg/16666LAWA/LAWA2019/WaterQuality/AnnualSummary/TenYearTrendM&QParamSepd.emf',
     emfPlus = F)
par(mfrow=c(3,3),mar=c(3.5,1,3.5,1),cex.main=2,cex.lab=1.5,cex.axis=1.5)
for(measure in seq_along(uMeasures)){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==uMeasures[measure]))%>%
    droplevels
  colMPs=-0.5+(1:4)*1.2
  tb <- spinePlotEG(factor(tfp$Quartile,levels=c("1","2","3","4")),
                    factor(tfp$TrendScore),doPlot=F)
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,4),mbp)
  mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
  frex=rbind(frex,cbind(rep(uMeasures[measure],dim(tbp)[1]),tbp))
  if(uMeasures[measure]=="NH4"){
    tbp=tbp[,-2]
    barplot(tbp,width=c(2,1,1),main='',las=2,space=0.1,
            col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n')
  }else{
  barplot(tbp,main="",las=2,space=0.1,
          col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n')
  }
  title(main = measLab[measure],line = 1.5)
  axis(side = 1,at=colMPs,labels = qabels,las=1,lwd = 0)
}
dev.off()

frex=as.data.frame(frex)
rownames(frex)=NULL
frex[,1]=as.character(frex[,1])
frex[,1]=paste0(frex[,1],c("-2","-1","0","+1","+2"))
frex[,2]=as.numeric(as.character(frex[,2]))
frex[,3]=as.numeric(as.character(frex[,3]))
frex[,4]=as.numeric(as.character(frex[,4]))
frex[,5]=as.numeric(as.character(frex[,5]))

#Export data for openLab
write.csv(frex,'h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/TenYearTrendM&QParamSepd.csv',row.names=F)



#TO Nicole - tables of site numbers per region

rivermacroST = merge(riverSiteTable%>%select(LawaSiteID,Region),
                     macroSiteTable%>%select(LawaSiteID,Region),all=T)

riverSiteTable%>%group_by(Region)%>%dplyr::summarise(n=length(unique(LawaSiteID)))%>%knitr::kable(type='rst')
macroSiteTable%>%group_by(Region)%>%dplyr::summarise(n=length(unique(LawaSiteID)))%>%knitr::kable(type='rst')
rivermacroST%>%group_by(Region)%>%dplyr::summarise(n=length(unique(LawaSiteID)))%>%knitr::kable(type='rst')
