rm(list=ls())
library(tidyverse)
source("H:/ericg/16666LAWA/LAWA2019/Scripts/LAWAFunctions.R")
#Pull in land use from LCDB
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')
source('h:/ericg/16666LAWA/LAWA2019/scripts/spineplotEG.R')

Include99s = F
PlotNumbersOnPlots=T

#Prep site table ####

riverSiteTable = loadLatestSiteTableRiver()
siteTableRawLandUse = read.csv("H:/ericg/16666LAWA/LAWA2019/WaterQuality/Metadata/SiteTableRawLandUse.csv",stringsAsFactors = F)
riverSiteTable$rawWFS=tolower(siteTableRawLandUse$SWQLanduse[match(riverSiteTable$LawaSiteID,siteTableRawLandUse$LawaSiteID)])
riverSiteTable$rawREC=tolower(siteTableRawLandUse$Landcover[match(riverSiteTable$LawaSiteID,siteTableRawLandUse$LawaSiteID)])
rm(siteTableRawLandUse)

macroSiteTable = loadLatestSiteTableMacro()
macrositeTableRawLandUse = read.csv("H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/Metadata/SiteTableRawLandUse.csv",stringsAsFactors = F)
macroSiteTable$rawWFS=tolower(macrositeTableRawLandUse$SWQLanduse[match(macroSiteTable$LawaSiteID,macrositeTableRawLandUse$LawaSiteID)])
macroSiteTable$rawREC=tolower(macrositeTableRawLandUse$Landcover[match(macroSiteTable$LawaSiteID,macrositeTableRawLandUse$LawaSiteID)])
rm(macrositeTableRawLandUse)

# c('if','ef','s','t','w')] <- 'Forest' #indigenous forest, exotic forest, scrub, tussock, wetland
# c('p','b','m')] <- 'Rural'            #pastoral, bare, ?? 
# c('u')] <- 'Urban'                    #urban

riverSiteTable$groupedREC = factor(tolower(riverSiteTable$Landcover))
riverSiteTable$rawWFS = factor(tolower(riverSiteTable$rawWFS),
                               levels=sort(unique(c(tolower(riverSiteTable$rawWFS),tolower(macroSiteTable$rawWFS)))))
riverSiteTable$groupedWFS=factor(tolower(riverSiteTable$SWQLanduse),
                                 levels=sort(unique(c(tolower(riverSiteTable$SWQLanduse),tolower(macroSiteTable$SWQLanduse)))))

macroSiteTable$groupedREC = factor(tolower(macroSiteTable$Landcover))
macroSiteTable$rawWFS = factor(tolower(macroSiteTable$rawWFS),
                               levels=sort(unique(c(tolower(riverSiteTable$rawWFS),tolower(macroSiteTable$rawWFS)))))
macroSiteTable$groupedWFS=factor(tolower(macroSiteTable$SWQLanduse),
                                 levels=sort(unique(c(tolower(riverSiteTable$SWQLanduse),tolower(macroSiteTable$SWQLanduse)))))

rec=read_csv("E:/RiverData/RECnz.txt")
rec$groupedREC=rec$LANDCOVER
rec$groupedREC[rec$LANDCOVER%in%c("EF","IF","S","T","W")] <- "forest"
rec$groupedREC[rec$LANDCOVER%in%c("P","B","M")] <- "rural"
rec$groupedREC[rec$LANDCOVER%in%c("U")] <- "urban"

if(0){
  
  table(rec$LANDCOVER)
  nationwideRepresentation=table(rec$LANDCOVER)/sum(table(rec$LANDCOVER))*100
  signif(nationwideRepresentation,2)%>%knitr::kable(format='rst')
  table(rec$groupedREC)
  table(riverSiteTable$rawREC)
  lawaRepresentation=table(riverSiteTable$rawREC)/sum(table(riverSiteTable$rawREC))*100
  signif(lawaRepresentation,2)%>%knitr::kable(format='rst')
  
  table(riverSiteTable$rawREC)/table(rec$LANDCOVER)*100 #What proportion of the nations each class do we sample
  signif(lawaRepresentation/nationwideRepresentation,2)%>%knitr::kable(format='rst')
  
  # ====  ======  ======  ========
  # Var1   nat   lawa    fold
  # ====  ======  ======  ========
  # B      6.50   0.490   0.075
  # EF     5.00   2.800   0.570
  # IF    24.00   16.000  0.650
  # M      0.66   0.097   0.150
  # P     42.00   66.000  1.600
  # S      5.40   3.000   0.560
  # T     16.00   4.400   0.270
  # U      0.74   7.600   10.000
  # W      0.14   0.097   0.710
  # ====  =======  ===== =======
  
  par(mfrow=c(3,1),cex.lab=1.75,mar=c(6,6,4,2))
  plot(nationwideRepresentation,ylab='Percentage',main='Nationwide representation')
  plot(lawaRepresentation,ylab='Percentage',main='LAWA site representation')
  plot(seq_along(names(lawaRepresentation)),unclass(lawaRepresentation/nationwideRepresentation),
       type='h',frame.plot=F,xaxt='n',lwd=2,log='y',ylab='x-fold',main="LAWA relative representation",
       xlab="")
  mtext(side = 1,at = 1,'bare\n',line=3.5)
  mtext(side = 1,at = 2,'exotic\nforest',line=3)
  mtext(side = 1,at = 3,'indigenous\nforest',line=3)
  mtext(side = 1,at = 4,'misc\n',line=3.5)
  mtext(side = 1,at = 5,'pastoral\n',line=3.5)
  mtext(side = 1,at = 6,'scrubland\n',line=3.5)
  mtext(side = 1,at = 7,'tussock\n',line=3.5)
  mtext(side = 1,at = 8,'urban\n',line=3.5)
  mtext(side = 1,at = 9,'wetland\n',line=3.5)
  abline(h=1,lty=2)
  
  table(rec$LANDCOVER)
  table(riverSiteTable$rawREC) #Raw REC               ex RECLanduse
  table(riverSiteTable$groupedREC)  #Grouped REC      ex Landcover
  table(riverSiteTable$rawWFS) #Raw WFS               ex WFSLanduse
  table(riverSiteTable$groupedWFS) #Grouped WFS       ex SWQLanduse
  table(riverSiteTable$rawREC,riverSiteTable$groupedREC)
  table(riverSiteTable$rawWFS,riverSiteTable$groupedWFS)
  table(riverSiteTable$groupedREC,riverSiteTable$groupedWFS)
}       


#Prep trend table ####
wqtrend=read_csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis",
                          pattern="RiverWQ_Trend*.*",
                          full.names=T,recursive=T,ignore.case=T),1),guess_max = 20000)%>%select(-Median)
if(!Include99s){
  wqtrend$TrendScore[which(wqtrend$TrendScore==(-99))] <- NA
  wqtrend <- wqtrend%>%drop_na(TrendScore)
}else{
  wqtrend$TrendScore[which(is.na(wqtrend$TrendScore))] <- -99
}

wqtrend$TrendScore=droplevels(factor(wqtrend$TrendScore,levels=c(-2,-1,0,1,2,-99)))
nTrendClass=length(levels(wqtrend$TrendScore))
wqtrend$ConfCat=factor(wqtrend$ConfCat,levels=c("Very likely degrading","Likely degrading","Indeterminate","Likely improving","Very likely improving" ))

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


wqmedians$rawWFS = riverSiteTable$rawWFS[match(wqmedians$LawaSiteID,riverSiteTable$LawaSiteID)]
wqmedians$rawWFS[which(is.na(wqmedians$rawWFS))] = macroSiteTable$rawWFS[match(tolower(wqmedians$LawaSiteID[which(is.na(wqmedians$rawWFS))]),tolower(macroSiteTable$LawaSiteID))]
wqmedians$rawREC = riverSiteTable$rawREC[match(tolower(wqmedians$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
wqmedians$rawREC[which(is.na(wqmedians$rawREC))] = macroSiteTable$rawREC[match(tolower(wqmedians$LawaSiteID[which(is.na(wqmedians$rawREC))]),tolower(macroSiteTable$LawaSiteID))]
wqmedians$groupedWFS = riverSiteTable$groupedWFS[match(tolower(wqmedians$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
wqmedians$groupedWFS[which(is.na(wqmedians$groupedWFS))] = macroSiteTable$groupedWFS[match(tolower(wqmedians$LawaSiteID[which(is.na(wqmedians$groupedWFS))]),tolower(macroSiteTable$LawaSiteID))]
wqmedians$groupedREC = riverSiteTable$groupedREC[match(tolower(wqmedians$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
wqmedians$groupedREC[which(is.na(wqmedians$groupedREC))] = macroSiteTable$groupedREC[match(tolower(wqmedians$LawaSiteID[which(is.na(wqmedians$groupedREC))]),tolower(macroSiteTable$LawaSiteID))]
wqmedians$RECAltitude = riverSiteTable$AltitudeCl[match(tolower(wqmedians$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
wqmedians$RECAltitude[is.na(wqmedians$RECAltitude)] = macroSiteTable$Altitude[match(tolower(wqmedians$LawaSiteID[is.na(wqmedians$RECAltitude)]),tolower(riverSiteTable$LawaSiteID))]

wqmedians$rawREC=factor(tolower(wqmedians$rawREC),levels=c("if","w","t","s","ef","p","b","m","u"))


#Get details on quartile boundaries ####
wqmedians%>%drop_na(Quartile)%>%group_by(Measurement,Quartile)%>%
  dplyr::summarise(upperBound=max(Median,na.rm=T),lowerBound=min(Median,na.rm=T))%>%
  as.data.frame%>%write.csv('./annualSummary/QuartileBoundaries.csv',row.names = F)

#Merge state and trend ####
wqt=left_join(wqtrend,wqmedians,by=c("LawaSiteID","Measurement"))
# rm(wqmedians)
wqt$Measurement <- factor(wqt$Measurement,
                          levels=c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")) 


if(Include99s){
  wqt$AnalysisNote[wqt$numMonths<108&wqt$numQuarters<36] <- '<36 quarters & <108 months'
  wqt$AnalysisNote[wqt$nMeasures==0] <- 'no data'
  
  wqt%>%filter(Measurement=="BDISC")%>%select(AnalysisNote)%>%table
  wqt%>%select(AnalysisNote)%>%table%>%knitr::kable(format='rst')
  wqt%>%select(AnalysisNote,Quartile)%>%table%>%knitr::kable(format='rst')
}


measLab = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP','DRP',expression(italic(E. ~ coli)),  'MCI'
)[c("BDISC", "TURB", "TN", "TON", "NH4", "TP", "DRP", "ECOLI", "MCI") %in%unique(wqt$Measurement)]


measLab=c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
          'DRP',expression(italic(E.~coli)),'MCI')[c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")%in%
                                                     unique(wqt$Measurement)]
uMeasures = unique(wqt$Measurement)

newPlot=Cairo::CairoWin()
scratchPlot = Cairo::CairoWin()

#Ten-year trends, quartiles pooled, by parameter ####
tfp = wqt%>%
  dplyr::filter((period==10))#%>%
# drop_na(TrendScore)
nMeas=length(unique(tfp$Measurement))

dev.set(scratchPlot)
par(mfrow=c(1,1))
colMPs=-0.5+(1:nMeas)*1.2
tb <- spinePlotEG(factor(tfp$Measurement),
           factor(tfp$TrendScore),xlab='',ylab='',
           col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #
           main="Ten year trends (M&Q)")
tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
mbp <- rbind(rep(0,nMeas),mbp)
mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2

dev.set(newPlot)
par(mfrow=c(1,1),mar=c(8,5,6,4))
barplot(tbp,main="Ten year trends (M&Q)",las=2,ylab='',xlab='',
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                   'DRP',expression(italic(E.~coli)),"MCI"),las=2)
if(PlotNumbersOnPlots){
  for(cc in 1:nMeas){
    axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
    axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
    par(xpd=NA)
    text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=0.75)
    # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
    text(colMPs[cc],1.025,sum(tb[cc,]))
  }
}



#Ten year trends, plot per parameter, bar per quartile ####
dev.set(scratchPlot)
par(mfrow=c(3,3),mar=c(4,4,2,2),cex.main=2,cex.lab=2,cex.axis=2)
dev.set(newPlot)
par(mfrow=c(3,3),mar=c(6,2,4,1),cex.main=2,cex.lab=2,cex.axis=2)
rabels=c('1st','2nd','3rd','4th')
measLab=c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
          'DRP',expression(italic(E.~coli)),'MCI')[c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")%in%unique(wqt$Measurement)]
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==measure))%>%
    droplevels
  dev.set(scratchPlot)
  colMPs=-0.5+(1:4)*1.2
  tb <- spinePlotEG(factor(tfp$Quartile,levels=c("1","2","3","4")),
                    factor(tfp$TrendScore),tol.ylab=0,off=0,ylab='',xlab='',
                    col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #,
                    main=ifelse(measure=="MCI","Ten year trends MCI",paste("Ten year trends (M&Q)",measure)))
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,4),mbp)
  mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
  dev.set(newPlot)
  barplot(tbp,main="",las=2,
          col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n')
  title(main = ifelse(measure=="MCI","Ten year trends MCI",paste("Ten year trends (M&Q)",measure)),line = 2)
  axis(side = 1,at=colMPs,labels = rabels,las=2)
  if(PlotNumbersOnPlots){
    for(cc in 1:4){
      # axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
      par(xpd=NA)
      text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=1.5)
      text(colMPs[cc],1.05,sum(tb[cc,]),cex=1.5) 
      # text(colMPs[cc],1.05,sum(tfp$Quartile==cc,na.rm=T),cex=1.5) #to do the total number in that quartile, whether it got a trend or not
    }
  }
}


#One with more detail on relationship between state and trend, per parameter ####
signsqrt=function(x){
  signx = sign(x)
  return(signx * sqrt(abs(x)))
}
percentile <- function(x){
  return(as.numeric(factor(Hmisc::cut2(x,cuts = unique(quantile(x,probs=seq(0,1,le=101),na.rm=T))))))
}

dev.set(scratchPlot)
par(mfrow=c(3,3),mar=c(4,4,2,2),cex.main=2,cex.lab=2,cex.axis=2)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter(period==10&Measurement==measure&Percent.annual.change!=0)%>%
    droplevels%>%
    mutate(pmed=percentile(Median),ppec=percentile(Percent.annual.change))
  plot(tfp$pmed,tfp$ppec,
       xlab='',ylab='',main=measure,pch=16,
       col=rainbow(16)[as.numeric(factor(tfp$Region))],asp=1)
  mylm = lm(data=tfp,formula = ppec~pmed*Region)
  print(summary(mylm)$adj.r.squared)
  for(reg in unique(tfp$Region)){
    lines(seq(0,100),predict(mylm,newdata=data.frame(pmed=seq(0,100),Region=reg)),
          col=rainbow(16)[as.numeric(factor(reg,levels=unique(tfp$Region)))],lwd=2)
    # angle is change in y over change in x
    srt = 180/pi*atan(diff(predict(mylm,newdata=data.frame(pmed=c(0,100),Region=reg)))/100)
    rhs = predict(mylm,newdata=data.frame(pmed=min(100,par('usr')[2]),Region=reg))
    text(min(100,par('usr')[2]),rhs,reg,srt=srt,pos = 2,cex=2)
  }
  abline(lwd=4,lm(data=tfp,ppec~pmed))
}


dev.set(scratchPlot)
par(mfrow=c(3,3),mar=c(4,4,2,2),cex.main=2,cex.lab=2,cex.axis=2)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter(period==10&Measurement==measure&Percent.annual.change!=0)%>%
    droplevels%>%
    mutate(pmed=percentile(Median),pprob=percentile(MKProbability))
  plot(tfp$pmed,tfp$pprob,
       xlab='',ylab='',main=measure,pch=16,
       col=rainbow(16)[as.numeric(factor(tfp$Region))],asp=1)
  mylm = lm(data=tfp,formula = pprob~pmed*Region)
  print(summary(mylm)$adj.r.squared)
  for(reg in unique(tfp$Region)){
    lines(seq(0,100),predict(mylm,newdata=data.frame(pmed=seq(0,100),Region=reg)),
          col=rainbow(16)[as.numeric(factor(reg,levels=unique(tfp$Region)))],lwd=2)
    # angle is change in y over change in x
    srt = 180/pi*atan(diff(predict(mylm,newdata=data.frame(pmed=c(0,100),Region=reg)))/100)
    rhs = predict(mylm,newdata=data.frame(pmed=min(100,par('usr')[2]),Region=reg))
    text(min(100,par('usr')[2]),rhs,reg,srt=srt,pos = 2,cex=2)
  }
  abline(lwd=4,lm(data=tfp,pprob~pmed))
}

dev.set(scratchPlot)
par(mfrow=c(3,3),mar=c(4,4,2,2),cex.main=2,cex.lab=2,cex.axis=2)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter(period==10&Measurement==measure&Percent.annual.change!=0)%>%
    droplevels%>%
    mutate(pmed=percentile(Median),ppec=percentile(Percent.annual.change))
  plot(tfp$Median,tfp$Percent.annual.change,
       log='x',xlab='',ylab='',main=measure,pch=16,ylim=c(quantile(tfp$Percent.annual.change,probs=c(0.01,0.99))),
       col=rainbow(16)[as.numeric(factor(tfp$Region))])
  mylm = lm(data=tfp,formula = Percent.annual.change~I(log(Median))*Region)
  print(summary(mylm)$adj.r.squared)
  for(reg in unique(tfp$Region)){
    lines(seq(10^(par('usr')[1]),10^(par('usr')[2])),
          predict(mylm,newdata=data.frame(Median=seq(10^(par('usr')[1]),10^(par('usr')[2])),Region=reg)),
          col=rainbow(16)[as.numeric(factor(reg,levels=unique(tfp$Region)))],lwd=2)
    # angle is change in y over change in x
     srt = 180/pi*atan(diff(predict(mylm,
                                    newdata=data.frame(Median=c(10^(par('usr')[1]),10^(par('usr')[2])),
                                                       Region=reg)))/
                         diff(c(10^(par('usr')[1]),10^(par('usr')[2]))))
    rhs = predict(mylm,newdata=data.frame(Median=10^(par('usr')[2]),Region=reg))
    text(10^(par('usr')[2]),rhs,reg,srt=srt,pos = 2,cex=2)
  }
  abline(lwd=4,lm(data=tfp,Percent.annual.change~I(log(Median))))
}

#And separate by rawWFS ####
if(0){
colMPs=-0.5+(1:length(measLab))*1.2
  newPlot=Cairo::CairoWin()
  
  par(mfrow=c(2,2),mar=c(6,4,4,2))
  rabels=c('Forest','Rural','Urban')
  for(luse in 1:3){
    tfp = wqt%>%
      dplyr::filter((period==10&tolower(groupedWFS)==tolower(rabels[luse])))%>%
      droplevels
    nMeas=length(unique(tfp$Measurement))
    dev.set(scratchPlot)
    par(mfrow=c(1,1))
    tb <- plot(factor(tfp$Measurement),
               factor(tfp$TrendScore),
               col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #,
               main="Ten year trends")
    tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
    mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
    mbp <- rbind(rep(0,nMeas),mbp)
    mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
    dev.set(newPlot)
    barplot(tbp,main=paste(rabels[luse]),las=2,
            col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
    axis(side = 1,at=colMPs,labels = measLab,las=2)
    # axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
    axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
    par(xpd=NA)
    for(cc in 1:nMeas){
      # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
      text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=0.75)
      text(colMPs[cc],1.025,sum(tb[cc,]))
    }
  }
  
  
  #And separate by WFSaltitude ####
  newPlot=Cairo::CairoWin()
  
  par(mfrow=c(1,2),mar=c(6,4,4,2))
  rabels=c('Lowland','Upland')
  for(altd in 1:2){
    tfp = wqt%>%
      dplyr::filter((period==10&tolower(SWQAltitude)==tolower(rabels[altd])))%>%
      droplevels
    nMeas=length(unique(tfp$Measurement))
    dev.set(scratchPlot)
    par(mfrow=c(1,1))
    colMPs=-0.5+(1:nMeas)*1.2
    tb <- plot(factor(tfp$Measurement),
               factor(tfp$TrendScore),
               col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #,
               main="Ten year trends")
    tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
    mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
    mbp <- rbind(rep(0,nMeas),mbp)
    mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
    dev.set(newPlot)
    barplot(tbp,main=paste(rabels[altd]),las=2,
            col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
    axis(side = 1,at=colMPs,labels = measLab,las=2)
    # axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
    axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
    par(xpd=NA)
    for(cc in 1:nMeas){
      # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
      text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=0.75)
      text(colMPs[cc],1.025,sum(tb[cc,]))
    }
  }
  
  
  #And separate by WFSaltitude / rawWFS ####
  newPlot=Cairo::CairoWin()
  
  par(mfrow=c(2,3),mar=c(6,4,4,2),cex.axis=1.5,cex.main=1.5)
  Arabels=c('Lowland','Upland')
  Lurabels=c('Forest','Rural','Urban')
  for(altd in 1:2){
    for(luse in 1:3){
      tfp = wqt%>%
        dplyr::filter((period==10&tolower(SWQAltitude)==tolower(Arabels[altd])&
                         tolower(groupedWFS)==tolower(Lurabels[luse])))%>%
        droplevels
      nMeas=length(unique(tfp$Measurement))
      dev.set(scratchPlot)
      par(mfrow=c(1,1))
      colMPs=-0.5+(1:nMeas)*1.2
      tb <- plot(factor(tfp$Measurement),
                 factor(tfp$TrendScore),
                 col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #,
                 main="Ten year trends")
      tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
      mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
      mbp <- rbind(rep(0,nMeas),mbp)
      mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
      dev.set(newPlot)
      barplot(tbp,main=paste(Arabels[altd],Lurabels[luse]),las=2,
              col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
      axis(side = 1,at=colMPs,labels = measLab,las=2)
      # axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
      axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
      par(xpd=NA)
      for(cc in 1:nMeas){
        # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
        text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,])
        text(colMPs[cc],1.025,sum(tb[cc,]),cex=1.25)
      }
    }
  }
  
}



#Trends compared by groupedREC ####
if(0){
dev.set(scratchPlot)
par(mfrow=c(3,3),mar=c(4,3,3,2),cex.main=2,cex.lab=1.5,cex.axis=1.5)
dev.set(newPlot)
par(mfrow=c(3,3),mar=c(5,3,4,1),cex.main=2,cex.lab=1.5,cex.axis=1.5)
lubels=c("forest","rural","urban")
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==measure))
  dev.set(scratchPlot)
  colMPs=-0.5+(1:3)*1.2
  tb <- spinePlotEG(tfp$groupedREC,
             factor(tfp$TrendScore),off=0,
             col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"),
             main=paste("Ten year trends (M&Q)",measure),xlab='',ylab='')
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,4),mbp)
  mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
  dev.set(newPlot)
  barplot(tbp,main=paste(measure,", ten year trends (M&Q)"),las=2,
          col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') 
  axis(side = 1,at=colMPs,labels = lubels,las=1)
  if(PlotNumbersOnPlots){
    for(cc in 1:3){
      axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
      par(xpd=NA)
      # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
      text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=1.5)
      text(colMPs[cc],1.025,sum(tb[cc,]),cex=1.5)
    }
  }
}
}

#And separate by RECaltitude ####
if(0){
newPlot=Cairo::CairoWin()

par(mfrow=c(1,2),mar=c(6,4,4,2))
rabels=c('Lowland','Upland')
for(altd in 1:2){
  tfp = wqt%>%
    dplyr::filter((period==10&tolower(RECAltitude)==tolower(rabels[altd])))%>%
    droplevels
  nMeas=length(unique(tfp$Measurement))
  dev.set(scratchPlot)
  par(mfrow=c(1,1))
  colMPs=-0.5+(1:nMeas)*1.2
  tb <- plot(factor(tfp$Measurement),
             factor(tfp$TrendScore),
             col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #,
             main="Ten year trends")
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,nMeas),mbp)
  mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
  dev.set(newPlot)
  barplot(tbp,main=paste(rabels[altd]),las=2,
          col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
  axis(side = 1,at=colMPs,labels = measLab,las=2)
  # axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
  axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
  par(xpd=NA)
  for(cc in 1:nMeas){
    # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
    text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=0.75)
    text(colMPs[cc],1.025,sum(tb[cc,]))
  }
}


#And separate by RECaltitude / REClanduse ####
newPlot=Cairo::CairoWin()

par(mfrow=c(2,3),mar=c(6,4,4,2))
Arabels=c('Lowland','Upland')
Lurabels=c('Forest','Rural','Urban')
for(altd in 1:2){
  for(luse in 1:3){
    tfp = wqt%>%
      dplyr::filter((period==10&tolower(RECAltitude)==tolower(Arabels[altd])&
                       tolower(RECLanduse)==tolower(Lurabels[luse])))#%>%
    # droplevels
    nMeas=length(unique(tfp$Measurement))
    dev.set(scratchPlot)
    par(mfrow=c(1,1))
    colMPs=-0.5+(1:nMeas)*1.2
    tb <- plot(factor(tfp$Measurement),
               (tfp$TrendScore),
               col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd"), #,
               main="Tum near trends")
    tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
    mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
    mbp <- rbind(rep(0,nMeas),mbp)
    mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
    dev.set(newPlot)
    barplot(tbp,main=paste(Arabels[altd],Lurabels[luse]),las=2,
            col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
    axis(side = 1,at=colMPs,labels = measLab,las=2)
    # axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
    axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
    par(xpd=NA)
    for(cc in 1:nMeas){
      # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
      text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=0.75)
      text(colMPs[cc],1.025,sum(tb[cc,]))
    }
  }
}
}


#An MCI plots ####
par(mfrow=c(1,1))
plot(wqt$MCIband,wqt$Median,ylab='MCI median',xlab='MCI band')

par(mfrow=c(2,2),mar=c(6,5,1,3.5))
plot(wqt%>%filter(Measurement=="MCI")%>%select(Median)%>%unlist,
     wqt%>%filter(Measurement=="MCI")%>%select(Percent.annual.change)%>%unlist,
     xlab='MCI median',ylab='Trend magnitude')
plot(wqt%>%filter(Measurement=="MCI")%>%select(Median)%>%unlist,
     wqt%>%filter(Measurement=="MCI")%>%select(MKProbability)%>%unlist,
     xlab='MCI median',ylab='Trend probability')
plot(wqt%>%filter(Measurement=="MCI")%>%select(Percent.annual.change)%>%unlist,
     wqt%>%filter(Measurement=="MCI")%>%select(MKProbability)%>%unlist,
     xlab='Trend magnitude',ylab='Trend probability')
plot(wqt%>%filter(Measurement=="MCI")%>%select(TrendScore)%>%unlist,
     wqt%>%filter(Measurement=="MCI")%>%select(Median)%>%unlist,
     ylab='MCI median',xlab='Trend category')
par(mfrow=c(1,1))

par(mfrow=c(2,2),mar=c(6,5,1,3.5))
plot(wqt%>%filter(Measurement=="MCI")%>%select(MCIband)%>%unlist,
     wqt%>%filter(Measurement=="MCI")%>%select(MKProbability)%>%unlist,
     ylab='Probability of decreasing trend',xlab='MCI band')
plot(wqt%>%filter(Measurement=="MCI")%>%select(MCIband)%>%unlist,
     wqt%>%filter(Measurement=="MCI")%>%select(TrendScore)%>%unlist,
     xlab='MCI bands',ylab='Trend category, -2 is degrading')
plot(wqt%>%filter(Measurement=="MCI")%>%select(Quartile)%>%unlist%>%factor,
     wqt%>%filter(Measurement=="MCI")%>%select(MCIband)%>%unlist%>%factor,
     xlab='MCI quartile',ylab='MCI band')
plot(wqt%>%filter(Measurement=="MCI")%>%select(groupedREC)%>%unlist%>%factor,
     wqt%>%filter(Measurement=="MCI")%>%select(MCIband)%>%unlist,xlab='Landuse category',ylab='MCI band')

par(mfrow=c(2,1))
plot(wqt%>%filter(Measurement=="MCI")%>%select(rawWFS),#%>%unlist%>%factor,
     wqt%>%filter(Measurement=="MCI")%>%select(MCIband)%>%unlist,xlab='Landuse category',ylab='MCI band')
plot(wqt%>%filter(Measurement=="MCI")%>%select(groupedWFS),
     wqt%>%filter(Measurement=="MCI")%>%select(MCIband)%>%unlist,xlab='Landuse category',ylab='MCI band')

#Ten-year trends, parameters pooled, by quartile ####
tfp <- wqt%>%filter(period==10)
colMPs=-0.5+(1:4)*1.2
tb <- plot(tfp$revQuartile,tfp$TrendScore)
tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
mbp <- rbind(rep(0,4),mbp)
mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2

par(mfrow=c(1,1),mar=c(5,4.5,6,6),cex.axis=1.55)
barplot(tbp,legend.text = T,main="Ten year trends (M&Q) all parameters",las=2,
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#ddddddFF"),xaxt='n',yaxt='n', #,
        args.legend = list(x='topright',inset=-0.075)) 
axis(side = 1,at=colMPs,labels = c("1st quartile","2nd quartile","3rd quartile","4th quartile"),las=1)
if(PlotNumbersOnPlots){
  axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 1,lwd = 0,lwd.ticks = 0)
  axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 0,lwd.ticks = 1,line=-1)
  par(xpd=NA)
  for(cc in 1:4){
    text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,])
    text(colMPs[cc],1.025,sum(tb[cc,]))
  }
}





#State by landuse ####
par(mfrow=c(2,1),mar=c(6,5,1,3.5),cex.axis=1.5,cex.lab=1.5)
spinePlotEG(wqmedians$rawREC,wqmedians$Quartile,tol.ylab=0,off=0,
            ylab='Quartile state score',yaxt='n',xlab='REC landcover category',col=c("#dd1111FF","#ee4411FF","#11cc11FF","#008800FF"))
# plot(wqmedians$rawWFS,wqmedians$Quartile,tol.ylab=0,off=0,
#      ylab='Quartile state score',yaxt='n',xlab='WFS landcover category',col=c("#dd1111FF","#ee4411FF","#11cc11FF","#008800FF"))
spinePlotEG(wqmedians$groupedREC,wqmedians$Quartile,tol.ylab=0,off=0,
            ylab='Quartile state score',yaxt='n',xlab='REC landcover grouping', col=c("#dd1111FF","#ee4411FF","#11cc11FF","#008800FF"))
# plot(wqmedians$groupedWFS,wqmedians$Quartile,tol.ylab=0,off=0,
#      ylab='Quartile state score',yaxt='n',xlab='WFS landcover grouping', col=c("#dd1111FF","#ee4411FF","#11cc11FF","#008800FF"))

par(mfrow=c(2,1),mar=c(6,5,1,3.5),cex.axis=1.5,cex.lab=1.75)
spinePlotEG(wqmedians$Quartile,wqmedians$rawREC,off=0,tol.ylab=0,
            xlab='Quartile state score',yaxt='n',ylab='REC landcover category',col=terrain.colors(9))
spinePlotEG(wqmedians$Quartile,wqmedians$groupedREC,tol.ylab=0,off=0,
            xlab='Quartile state score',yaxt='n',ylab='Land cover grouping',col=terrain.colors(3))

par(mfrow=c(2,1),mar=c(6,5,1,3.5),cex.axis=1.25,cex.lab=1.75)
spinePlotEG(wqmedians$rawREC,off=0,wqmedians$Quartile,tol.ylab=0,
            ylab='Quartile state score',yaxt='n',xlab='REC landcover category',col=c("#dd1111FF","#ee4411FF","#11cc11FF","#008800FF"))
spinePlotEG(wqmedians$groupedREC,wqmedians$Quartile,tol.ylab=0,off=0,
            ylab='Quartile state score',yaxt='n',xlab='REC landcover grouping',col=c("#dd1111FF","#ee4411FF","#11cc11FF","#008800FF"))

uMeasures = unique(wqmedians$Measurement)
dev.set(scratchPlot)
par(mfrow=c(3,3),mar=c(4,3,2,3),cex.main=2,cex.lab=1.5,cex.axis=1.5)
rabels=c('1st','2nd','3rd','4th')
measLab=c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
          'DRP',expression(italic(E.~coli)),'MCI')[c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")%in%unique(wqt$Measurement)]
par(mfrow=c(3,3))
for(measure in uMeasures){
  if(measure=='PH'){next}
  mfp <- wqmedians%>%filter(Measurement==measure)
  tb <- spinePlotEG(mfp$groupedREC,mfp$Quartile,tol.ylab=0,main=measure,
                    ylab="",yaxt='n',xlab='',col=c("#dd1111FF","#ee4411FF","#11cc11FF","#008800FF"))
}






#MultiDuration ####
if(0){

load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/",
              pattern = "Trend15Year.rData",full.names = T,recursive = T),1),verbose =T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose = T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/",
              pattern = "Trend5Year.rData",full.names = T,recursive = T),1),verbose = T)

if(Include99s){
  trendTable5$TrendScore[is.na(trendTable5$TrendScore)] <- (-99)
  trendTable10$TrendScore[is.na(trendTable10$TrendScore)] <- (-99)
  trendTable15$TrendScore[is.na(trendTable15$TrendScore)] <- (-99)
}else{
  trendTable5$TrendScore[trendTable5$TrendScore==(-99)] <- NA
  trendTable10$TrendScore[trendTable10$TrendScore==(-99)] <- NA
  trendTable15$TrendScore[trendTable15$TrendScore==(-99)] <- NA
  trendTable5 <- trendTable5%>%drop_na(TrendScore)
  trendTable10 <- trendTable10%>%drop_na(TrendScore)
  trendTable15 <- trendTable15%>%drop_na(TrendScore)
}
trendTable5$TrendScore <- droplevels(factor(trendTable5$TrendScore,levels=c(-2,-1,0,1,2,-99)))
trendTable10$TrendScore <- droplevels(factor(trendTable10$TrendScore,levels=c(-2,-1,0,1,2,-99)))
trendTable15$TrendScore <- droplevels(factor(trendTable15$TrendScore,levels=c(-2,-1,0,1,2,-99)))

wqtt5=trendTable5
wqtt10=trendTable10
wqtt15=trendTable15

load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/Analysis/",
              pattern = "Trend15Year.rData",full.names = T,recursive = T),1),verbose =T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose = T)
if(Include99s){
  trendTable10$TrendScore[is.na(trendTable10$TrendScore)] <- (-99)
  trendTable15$TrendScore[is.na(trendTable15$TrendScore)] <- (-99)
}else{
  trendTable10$TrendScore[trendTable10$TrendScore==(-99)] <- NA
  trendTable15$TrendScore[trendTable15$TrendScore==(-99)] <- NA
  trendTable10 <- trendTable10%>%drop_na(TrendScore)
  trendTable15 <- trendTable15%>%drop_na(TrendScore)
}
trendTable10$TrendScore <- droplevels(factor(trendTable10$TrendScore,levels=c(-2,-1,0,1,2,-99)))
trendTable15$TrendScore <- droplevels(factor(trendTable15$TrendScore,levels=c(-2,-1,0,1,2,-99)))

mtt10=trendTable10
mtt15=trendTable15

trendTable5=rbind(wqtt5)
trendTable10=rbind(wqtt10,mtt10)
trendTable15=rbind(wqtt15,mtt15)

tretamble = rbind(trendTable5,trendTable10,trendTable15)
if(0){
par(mfrow=c(3,1),mar=c(5,4,4,4))
t5 <- plot(factor(trendTable5$Measurement,levels=c("BDISC", "DRP", "ECOLI", "NH4", "TN", "TON", "TP", "TURB", "MCI")),
           trendTable5$TrendScore,ylab='',xlab='',
           col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),main="5 Year monthly")
t10 <- plot(factor(trendTable10$Measurement,levels=c("BDISC", "DRP", "ECOLI", "NH4", "TN", "TON", "TP", "TURB", "MCI")),
            trendTable10$TrendScore,ylab='',xlab='',
            col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),main="10 Year (M&Q)")
t15 <- plot(factor(trendTable15$Measurement,levels=c("BDISC", "DRP", "ECOLI", "NH4", "TN", "TON", "TP", "TURB", "MCI")),
            trendTable15$TrendScore,ylab='',xlab='',
            col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),main="15 Year (M&Q)")
t5p <- apply(X = t5,MARGIN = 1,FUN = function(x)x/sum(x))
t10p <- apply(X = t10,MARGIN = 1,FUN = function(x)x/sum(x))
t15p <- apply(X = t15,MARGIN = 1,FUN = function(x)x/sum(x))

m5p <- apply(t5p,MARGIN = 2,FUN=cumsum)
m5p <- rbind(rep(0,dim(m5p)[2]),m5p)
m5p = (m5p[-1,]+m5p[-(dim(m5p)[1]),])/2

m10p <- apply(t10p,MARGIN = 2,FUN=cumsum)
m10p <- rbind(rep(0,dim(m10p)[2]),m10p)
m10p = (m10p[-1,]+m10p[-(dim(m10p)[1]),])/2

m15p <- apply(t15p,MARGIN = 2,FUN=cumsum)
m15p <- rbind(rep(0,dim(m15p)[2]),m15p)
m15p = (m15p[-1,]+m15p[-(dim(m15p)[1]),])/2

colMPs=-0.5+(1:dim(t5p)[2])*1.2

par(mfrow=c(3,1),mar=c(5,4,4,2))
barplot(t5p,main="5 Year",las=2,
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),yaxt='n')
axis(side = 2,at = m5p[,1],labels = colnames(t5),las=2,lty = 0)
for(cc in 1:length(colMPs)){
  text(rep(colMPs[cc],5),m5p[,cc],paste0(t5[cc,],'\n(',round(t5p[,cc]*100,0),'%)'))
}
barplot(t10p,main="10 Year",las=2,
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),yaxt='n')
axis(side = 2,at = m10p[,1],labels = colnames(t10),las=2,lty = 0)
for(cc in 1:length(colMPs)){
  text(rep(colMPs[cc],5),m10p[,cc],paste0(t10[cc,],'\n(',round(t10p[,cc]*100,0),'%)'))
}
barplot(t15p,main="15 Year",las=2,
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),yaxt='n')
axis(side = 2,at = m15p[,1],labels = colnames(t15),las=2,lty = 0)
for(cc in 1:length(colMPs)){
  text(rep(colMPs[cc],5),m15p[,cc],paste0(t15[cc,],'\n(',round(t15p[,cc]*100,0),'%)'))
}
}
}

#Break it down by plot per measure, with bar per duration ####
#Plot it figure per parameter, bar per quartile

if(0){
  colMPs=-0.5+(1:3)*1.2
uMeasures = unique(tretamble$Measurement)
dev.set(newPlot)
par(mfrow=c(3,3),mar=c(4,3,4,1),cex.main=2,cex.lab=2,cex.axis=2)
drabels=c('5y M','10y M&Q','15y M&Q')
measLab=c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
          'DRP',expression(italic(E.~coli)),'MCI')[c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")%in%unique(wqt$Measurement)]
for(measure in uMeasures){
  dev.set(scratchPlot)
  tb <-   spinePlotEG(factor(tretamble$period[tretamble$Measurement==measure],levels=c('5','10','15')),
               factor(tretamble$TrendScore[tretamble$Measurement==measure]),
               ylab='',xlab='',xaxt='n',yaxt='n',off=0,
               col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),main=measure)
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,3),mbp)
  mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
  dev.set(newPlot)
  barplot(tbp,main=measure,las=2,
          col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
  axis(side = 1,at=colMPs,labels = drabels,las=1,padj=1)
  # axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
  if(PlotNumbersOnPlots){
    axis(side = 2,at = seq(0,1,le=11),labels = NA,las=1,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
    par(xpd=NA)
    for(cc in 1:3){
      # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
      text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=1.25)
      text(colMPs[cc],1.025,sum(tb[cc,]),cex=1.5)
    }}
}
}

# DO it only witht hose sites present in all tiem peioreods ####

measurepresn <- tretamble%>%
  group_by(LawaSiteID,Measurement)%>%
  dplyr::summarise(ndurn=length(unique(period)))%>%
  filter(ndurn==3)%>%mutate(siteMeas = paste0(LawaSiteID,Measurement))

measurepresn <- tretamble%>%
  group_by(LawaSiteID,Measurement)%>%
  dplyr::summarise(ndurn=length(unique(period)))%>%
  filter(ndurn==3|(ndurn==2&Measurement=="MCI"))%>%mutate(siteMeas = paste0(LawaSiteID,Measurement))

heritage = tretamble%>%mutate(siteMeas = paste0(LawaSiteID,Measurement))%>%
  filter(siteMeas%in%measurepresn$siteMeas)

colMPs=-0.5+(1:3)*1.2
uMeasures = unique(heritage$Measurement)
dev.set(scratchPlot)
par(mfrow=c(3,3),mar=c(4,3,4,1),cex.main=2,cex.lab=1.5,cex.axis=1.5)
dev.set(newPlot)
par(mfrow=c(3,3),mar=c(4,3,4,1),cex.main=2,cex.lab=1.5,cex.axis=1.5)
drabels=c('5y M','10y M&Q','15y M&Q')
mcabels=c('NA','10y A','15y A')
measLab=c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
          'DRP',expression(italic(E.~coli)),'MCI')[c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")%in%unique(wqt$Measurement)]
for(measure in uMeasures){
  dev.set(scratchPlot)
  tb <-   spinePlotEG(factor(heritage$period[heritage$Measurement==measure],levels=c('5','10','15')),
               factor(heritage$TrendScore[heritage$Measurement==measure]),
               ylab='',xlab='',xaxt='n',yaxt='n',off=0,
               col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),main=measure)
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,3),mbp)
  mbp = (mbp[-1,]+mbp[-(nTrendClass+1),])/2
  dev.set(newPlot)
  barplot(tbp,main=measure,las=2,
          col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF","#dddddd"),yaxt='n',xaxt='n') #,
  axis(side = 1,at=colMPs,labels = if(measure=='MCI')mcabels else drabels,las=1,padj=1)
  # axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
  if(PlotNumbersOnPlots){
    axis(side = 2,at = seq(0,1,le=11),labels = NA,las=1,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
    par(xpd=NA)
    for(cc in 1:3){
      # text(rep(colMPs[cc],nTrendClass),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
      text(rep(colMPs[cc],nTrendClass),mbp[,cc],tb[cc,],cex=1.25)
      text(colMPs[cc],1.025,sum(tb[cc,]),cex=1.5)
    }
  }
}




#A grid, duration by land use ####
tretamble$groupedREC = riverSiteTable$groupedREC[match(tretamble$LawaSiteID,riverSiteTable$LawaSiteID)]
tretamble$groupedREC[which(is.na(tretamble$groupedREC))] = macroSiteTable$groupedREC[match(tretamble$LawaSiteID[which(is.na(tretamble$groupedREC))],
                                                                                           macroSiteTable$LawaSiteID)]

dev.set(newPlot)
par(mfrow=c(3,3),mar=c(6,3,4,1))
dev.set(scratchPlot)
par(mfrow=c(3,3),cex.lab=1,mar=c(7,3,2,1))
colMPs=-0.5+(1:9)*1.2
for(luse in c("forest","rural","urban")){
  for(period in c(5,10,15)){
    dev.set(scratchPlot)
    tp<- spinePlotEG(droplevels(tretamble$Measurement[tretamble$period==period&tretamble$groupedREC==luse]),
              tretamble$TrendScore[tretamble$period==period&tretamble$groupedREC==luse],
              ylab='',xlab='',off=0,las=2,
              col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),
              main=paste(period,"Year, ",luse))
    tpp <- apply(X=tp,MARGIN=1,FUN=function(x)x/sum(x))
    mpp <- apply(tpp,2,cumsum)
    mpp <- rbind(rep(0,9),mpp)
    mpp <- (mpp[-1,]+mpp[-(dim(mpp)[1]),])/2
    dev.set(newPlot)
    barplot(tpp,main=paste(period,"Year,",luse),las=2,
            col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF",'#eeeeeeFF'),yaxt='n')
    axis(side = 2,at = mpp[,1],labels = colnames(tp),las=2,lty = 0)
  }
}






#Change over Trend DUration ####
#Make a plot showing trend breakdown by timespan
# combTrend=read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2019/WaterQuality/Analysis/","RiverWQ_Trend",	recursive = T,full.names = T,ignore.case=T),1),stringsAsFactors = F)
# combTrend <- combTrend%>%filter(TrendScore>(-3))
# combTrend <- combTrend%>%filter(period<16)
# combTrend <- combTrend%>%filter(Measurement!="MCI")
par(mfrow=c(1,1))
trendByDuration=table(tretamble$TrendScore,tretamble$period)
trendByDurationB=t(apply(trendByDuration,1,function(x)x/apply(trendByDuration,2,sum)))
barplot(trendByDurationB,
        col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF"))

tretamble$SiteMeas=paste(tretamble$LawaSiteID,tretamble$Measurement,sep='][')

good15s = which(tretamble$TrendScore==2 & tretamble$period==15 & tretamble$frequency=='monthly')
goodlongMCI = which(tretamble$TrendScore==2 & tretamble$period ==15 & tretamble$Measurement=="MCI")
good15s = c(good15s,goodlongMCI)
bad5s = which(tretamble$TrendScore==-2 & tretamble$period==5 & tretamble$frequency=='monthly')
badshortMCI = which(tretamble$TrendScore==-2 &tretamble$period==10 & tretamble$Measurement=="MCI")
bad5s = c(bad5s,badshortMCI)
rm(goodlongMCI,badshortMCI)

bad15s = which(tretamble$TrendScore==-2 & tretamble$period==15 & tretamble$frequency=='monthly')
badlongMCI = which(tretamble$TrendScore==-2 & tretamble$period ==15 & tretamble$Measurement=="MCI")
bad15s = c(bad15s,badlongMCI)
good5s = which(tretamble$TrendScore==2 & tretamble$period==5 & tretamble$frequency=='monthly')
goodshortMCI = which(tretamble$TrendScore==2 &tretamble$period==10 & tretamble$Measurement=="MCI")
good5s = c(good5s,goodshortMCI)
rm(goodshortMCI,badlongMCI)

worsening = (tretamble$SiteMeas[good15s])[(tretamble$SiteMeas[good15s])%in%(tretamble$SiteMeas[bad5s])]
bettering = (tretamble$SiteMeas[bad15s])[(tretamble$SiteMeas[bad15s])%in%(tretamble$SiteMeas[good5s])]

worsingMeasures=unlist(strsplit(worsening,split = "\\]\\["))[seq(1,length(worsening))*2]
worsingSites=unlist(strsplit(worsening,split = "\\]\\["))[seq(1,length(worsening))*2-1]
knitr::kable(table(worsingMeasures))
knitr::kable(table(riverSiteTable$Agency[match(worsingSites,riverSiteTable$LawaSiteID)]))

betteringMeasures=unlist(strsplit(bettering,split = "\\]\\["))[seq(1,length(bettering))*2]
betteringSites=unlist(strsplit(bettering,split = "\\]\\["))[seq(1,length(bettering))*2-1]
knitr::kable(table(betteringMeasures))
knitr::kable(table(riverSiteTable$Agency[match(betteringSites,riverSiteTable$LawaSiteID)]))

worsening[grep('ecan',worsingSites)]
bettering[grep('ecan',betteringSites)]

source("scripts/PlotMeATrend.R")
#Plot some of the going-each-way ones ####
#These ecan00097 are hte halswell bridge ones
library(devEMF)

devEMF::emf('h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/HalswellDRP.emf',
            emfPlus = F,width=7,height=9)
par(mfrow=c(2,1),xpd=F,mgp=c(2,1,0),mar=c(5,4,4,2))
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "DRP",stopYearIncl = 2018,startYearIncl = 2009,
             mymain="Dissolved Reactive Phosphorus samples for\nHalswell River at McCartneys Bridge",type='b',ylab=expression("g/m"^3),xlab='')
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "DRP",stopYearIncl = 2018,startYearIncl = 2009,
             type='h',doPoints=F,doLegend=F,doCI=F,col='goldenrod',lwd=2,ylimlow=0,
             mymain="Dissolved Reactive Phosphorus samples for\nHalswell River at McCartneys Bridge",xaxt='n',ylab=expression("g/m"^3),xlab='')
text(as.numeric(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),par('usr')[3]-0.025,
     year(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),srt=45,xpd=TRUE)
dev.off()
tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/HalswellDRP.tiff',
     width=7,height=9,units='in',res=1200,compression='lzw',type='cairo')
par(mfrow=c(2,1),xpd=F,mgp=c(2,1,0),mar=c(5,4,4,2))
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "DRP",stopYearIncl = 2018,startYearIncl = 2009,
             mymain="Dissolved Reactive Phosphorus samples for\nHalswell River at McCartneys Bridge",type='b',ylab=expression("g/m"^3),xlab='')
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "DRP",stopYearIncl = 2018,startYearIncl = 2009,
             type='h',doPoints=F,doLegend=F,doCI=F,col='goldenrod',lwd=2,ylimlow=0,
             mymain="Dissolved Reactive Phosphorus samples for\nHalswell River at McCartneys Bridge",xaxt='n',ylab=expression("g/m"^3),xlab='')
text(as.numeric(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),par('usr')[3]-0.025,
     year(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),srt=45,xpd=TRUE)
dev.off()

devEMF::emf('h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/HalswellTON.emf',
            emfPlus = F,width=7,height=9)
par(mfrow=c(2,1),xpd=F,mgp=c(2,1,0),mar=c(5,4,4,2))
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TON",stopYearIncl = 2018,startYearIncl = 2009,
             mymain="Total Oxidised Nitrogen Samples for\nHalswell River at McCartneys Bridge",type='b',ylab=expression("g/m"^3),xlab='')
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TON",stopYearIncl = 2018,startYearIncl = 2009,
             type='h',doPoints=F,doLegend=F,doCI=F,col='goldenrod',lwd=2,ylimlow=0,
             mymain="Total Oxidised Nitrogen Samples for\nHalswell River at McCartneys Bridge",xaxt='n',ylab=expression("g/m"^3),xlab='')
text(as.numeric(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),par('usr')[3]-0.5,
     year(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),srt=45,xpd=TRUE)
dev.off()
tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/HalswellTON.tiff',
     width=7,height=9,units='in',res=1200,compression='lzw',type='cairo')
par(mfrow=c(2,1),xpd=F,mgp=c(2,1,0),mar=c(5,4,4,2))
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TON",stopYearIncl = 2018,startYearIncl = 2009,
             mymain="Total Oxidised Nitrogen Samples for\nHalswell River at McCartneys Bridge",type='b',ylab=expression("g/m"^3),xlab='')
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TON",stopYearIncl = 2018,startYearIncl = 2009,
             type='h',doPoints=F,doLegend=F,doCI=F,col='goldenrod',lwd=2,ylimlow=0,
             mymain="Total Oxidised Nitrogen Samples for\nHalswell River at McCartneys Bridge",xaxt='n',ylab=expression("g/m"^3),xlab='')
text(as.numeric(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),par('usr')[3]-0.5,
     year(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),srt=45,xpd=TRUE)
dev.off()

devEMF::emf('h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/HalswellTN.emf',
            emfPlus = F,width=7,height=6)
 par(mfrow=c(1,1),xpd=F,mgp=c(2,1,0),mar=c(5,4,4,2))
# PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TN",stopYearIncl = 2018,startYearIncl = 2009,
#              mymain="Total Nitrogen Samples for\nHalswell River at McCartneys Bridge",type='b',ylab=expression("g/m"^3),xlab='')
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TN",stopYearIncl = 2018,startYearIncl = 2009,
             type='h',doPoints=F,doLegend=F,doCI=F,col='goldenrod',lwd=2,ylimlow=0,
             mymain="Total Nitrogen Samples for\nHalswell River at McCartneys Bridge",xaxt='n',ylab=expression("g/m"^3),xlab='')
text(as.numeric(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),par('usr')[3]-0.5,
     year(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),srt=45,xpd=TRUE)
dev.off()
tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/HalswellTN.tiff',
     width=7,height=6,units='in',res=1200,compression='lzw',type='cairo')
 par(mfrow=c(1,1),xpd=F,mgp=c(2,1,0),mar=c(5,4,4,2))
# PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TN",stopYearIncl = 2018,startYearIncl = 2009,
#              mymain="Total Nitrogen Samples for\nHalswell River at McCartneys Bridge",type='b',ylab=expression("g/m"^3),xlab='')
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TN",stopYearIncl = 2018,startYearIncl = 2009,
             type='h',doPoints=F,doLegend=F,doCI=F,col='goldenrod',lwd=2,ylimlow=0,
             mymain="Total Nitrogen Samples for\nHalswell River at McCartneys Bridge",xaxt='n',ylab=expression("g/m"^3),xlab='')
text(as.numeric(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),par('usr')[3]-0.5,
     year(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),srt=45,xpd=TRUE)
dev.off()

devEMF::emf('h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/HalswellTURB.emf',
            emfPlus = F,width=7,height=6)
 par(mfrow=c(1,1),xpd=F,mgp=c(2,1,0),mar=c(5,4,4,2))
# PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TURB",stopYearIncl = 2018,startYearIncl = 2009,
#              mymain="Turbidity Samples for\nHalswell River at McCartneys Bridge",type='b',ylab="NTU",xlab='')
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TURB",stopYearIncl = 2018,startYearIncl = 2009,
             type='h',doPoints=F,doLegend=F,doCI=F,col='goldenrod',lwd=2,ylimlow=0,
             mymain="Turbidity Samples for\nHalswell River at McCartneys Bridge",xaxt='n',ylab="NTU",xlab='')
text(as.numeric(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),par('usr')[3]-10,
     year(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),srt=45,xpd=TRUE)
dev.off()
tiff('h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/HalswellTURB.tiff',
     width=7,height=6,units='in',res=1200,compression='lzw',type='cairo')
 par(mfrow=c(1,1),xpd=F,mgp=c(2,1,0),mar=c(5,4,4,2))
# PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TURB",stopYearIncl = 2018,startYearIncl = 2009,
#              mymain="Turbidity Samples for\nHalswell River at McCartneys Bridge",type='b',ylab="NTU",xlab='')
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TURB",stopYearIncl = 2018,startYearIncl = 2009,
             type='h',doPoints=F,doLegend=F,doCI=F,col='goldenrod',lwd=2,ylimlow=0,
             mymain="Turbidity Samples for\nHalswell River at McCartneys Bridge",xaxt='n',ylab="NTU",xlab='')
text(as.numeric(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),par('usr')[3]-10,
     year(seq.Date(from = as.Date('2009-1-1'),to = as.Date('2019-1-1'),by = 'year')),srt=45,xpd=TRUE)
dev.off()

PlotMeATrend(LSIin = 'ecan-00097',measureIn = "ECOLI",stopYearIncl = 2018,startYearIncl = 2014,type='b',ylimlow=90,log='y')
par(mfrow=c(1,1))
PlotMeATrend(LSIin = 'ecan-00097',measureIn = "TON",stopYearIncl = 2018,startYearIncl = 2009)


PlotMeATrend(LSIin = 'ccc-00003',measureIn = "DRP",stopYearIncl = 2018,startYearIncl = 2009,type='b',log='')


par(mfrow=c(3,3))
for(meas in uMeasures){
  with(wqt%>%filter(Measurement==meas),plot(factor(revQuartile),signsqrt(Percent.annual.change),main=meas))
  with(wqt%>%filter(Measurement==meas),anova(newlm <<- lm(Percent.annual.change~Quartile)))
  eval(parse(text=paste0(meas,"lm=newlm")))
}


par(mfrow=c(3,3))
for(meas in uMeasures){
  print(meas)
  with(wqt%>%filter(Measurement==meas),plot(log(Median),signsqrt(Percent.annual.change),main=meas))
  with(wqt%>%filter(Measurement==meas),abline(newlm<<-lm(I(signsqrt(Percent.annual.change))~I(log(Median))),lwd=2,col='red'))
  print(paste("R ",summary(newlm)$adj.r.squared))
  print(paste("p ",anova(newlm)$`Pr(>F)`[1]))
}

par(mfrow=c(1,1))



#Analyse changes from last year ####
load(tail(dir(path="h:/ericg/16666LAWA/2018/WaterQuality/4.Analysis/",
              pattern='Trend10Year.rData',ignore.case=T,recursive = T,full.names = T),1),verbose = T)
load(tail(dir(path="h:/ericg/16666LAWA/2018/WaterQuality/4.Analysis/",
              pattern='Trend10YearQ.rData',ignore.case=T,recursive = T,full.names = T),1),verbose = T)

load(tail(dir(path="h:/ericg/16666LAWA/2018/MacroInvertebrates//4.Analysis/",
              pattern='Trend10Year.rData',ignore.case=T,recursive = T,full.names = T),1),verbose = T)
macro2018=trendTable10  
load(tail(dir(path="h:/ericg/16666LAWA/Lawa2019/MacroInvertebrates/Analysis/",
              pattern='Trend10Year.rData',ignore.case=T,recursive = T,full.names = T),1),verbose = T)
macro2019=trendTable10
rm(trendTable10)


new10=wqtrend%>%filter(period==10)
new10 <- left_join(new10,
                   trendTable10%>%transmute(LawaSiteID=tolower(as.character(LawaSiteID)),Measurement=as.character(parameter),oldMKp=pvalue,oldSenP=Probability,oldSenS=AnnualSenSlope),
                   by=c("LawaSiteID","Measurement"))
plot(new10$MKProbability,new10$oldMKp)
plot(new10$Sen_Probability,new10$oldSenP)
plot(new10$MKProbability,new10$Sen_Probability)
plot(new10$oldMKp,new10$oldSenP)

discFilt = new10%>%filter(Sen_Probability>0.8 & oldSenP<0.2)

wqt$Long = riverSiteTable$Long[match(wqt$LawaSiteID,riverSiteTable$LawaSiteID)]
wqt$Lat = riverSiteTable$Lat[match(wqt$LawaSiteID,riverSiteTable$LawaSiteID)]

wqt$Long[is.na(wqt$Long)] = macroSiteTable$Long[match(wqt$LawaSiteID[is.na(wqt$Long)],macroSiteTable$LawaSiteID)]
wqt$Lat[is.na(wqt$Lat)] = macroSiteTable$Lat[match(wqt$LawaSiteID[is.na(wqt$Lat)],macroSiteTable$LawaSiteID)]

source("k:/R_functions/wgs2nzmg.r")
E_N=wgs2nzmg(wqt$Long,wqt$Lat)
wqt$NZMGE=E_N[,1]
wqt$NZMGN=E_N[,2]
rm(E_N)

library(rgdal)
nzmap <- readOGR('S:/New_S/Basemaps_Vector/NZ_Coastline/nzmg/nzcoast.shp')
plot(nzmap,col=NULL)


par(mfrow=c(3,3),mar=c(0,0,0,0),mgp=c(3,1,0),cex.main=2,cex.lab=2,cex.axis=2,xpd=NA)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==measure))%>%
    droplevels
  plot(tfp$NZMGE,tfp$NZMGN,xlim=range(wqt$NZMGE),ylim=range(wqt$NZMGN),pch=16,cex=0.75,
       col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd")[as.numeric(tfp$TrendScore)],
       xlab='',ylab='',main='',xaxt='n',yaxt='n',bty='n',asp=1)
  text(172.0192,-37.54852,measure,cex=3)
}

par(mfrow=c(3,3),mar=c(0,0,0,0),mgp=c(3,1,0),cex.main=2,cex.lab=2,cex.axis=2,xpd=NA)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==measure))%>%
    droplevels
  plot(tfp$NZMGE,tfp$NZMGN,xlim=range(wqt$NZMGE),ylim=range(wqt$NZMGN),pch=16,cex=0.75,
       col=c("#dd1111FF","#ee4411FF","#11cc11FF","#008800FF")[as.numeric(tfp$Quartile)],
       xlab='',ylab='',main='',xaxt='n',yaxt='n',bty='n',asp=1)
  text(172.0192,-37.54852,measure,cex=3)
}

par(mfrow=c(3,3),mar=c(0,0,0,0),mgp=c(3,1,0),cex.main=2,cex.lab=2,cex.axis=2,xpd=NA)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==measure))%>%
    droplevels
  plot(tfp$NZMGE,tfp$NZMGN,xlim=range(wqt$NZMGE)+c(-50000,300000),ylim=range(wqt$NZMGN),pch=16,cex=0.75,
       col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd")[as.numeric(tfp$TrendScore)],
       xlab='',ylab='',main='',xaxt='n',yaxt='n',bty='n',asp=1)
  points(tfp$NZMGE+400000,tfp$NZMGN,xlim=range(wqt$NZMGE),ylim=range(wqt$NZMGN),pch=16,cex=0.75,
       col=c("#dd1111FF","#ee4411FF","#11cc11FF","#008800FF")[as.numeric(tfp$Quartile)],
       xlab='',ylab='',main='',xaxt='n',yaxt='n',bty='n',asp=1)
  text(2892000,6420000,measure,cex=2)
  plot(nzmap,col=NULL,add=T)
}

fit <- rpart(data=wqt,car::logit(MKProbability)~NZMGE+NZMGN+Measurement+Region+Median+Quartile+SWQLanduse+SWQAltitude+period,
             control=rpart.control(cp=0.001,minbucket=5,xval=5))
fit$variable.importance
 plot(fit)
 text(fit)
wqtpreMK=arm::invlogit(predict(fit))
wqtpreMK=cut(wqtpreMK, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
wqtpreMK=factor(wqtpreMK,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
wqtpreMK=as.data.frame(wqtpreMK)
wqtpreMK$NZMGE=wqt$NZMGE
wqtpreMK$NZMGN=wqt$NZMGN
wqtpreMK$Measurement=wqt$Measurement

devEMF::emf('h:/ericg/16666LAWA/LAWA2019/WaterQuality/annualSummary/SpatialPattern.emf',emfPlus = F,width=7,height=9)
par(mfrow=c(3,3),mar=c(0,0,2,0),xpd=NA)
split(wqtpreMK,f = wqtpreMK$Measurement)%>%
  purrr::map(~{plot(.$NZMGE,.$NZMGN,xlab='',ylab='',xaxt='n',bty='n',yaxt='n',main=unique(.$Measurement),asp=1,pch=16,
                   col=c("#dd0000FF","#ee5500FF","#aaaaaaFF","#00cc00FF","#008800FF","#dddddd")[as.numeric(.$wqtpreMK)])
    with(wqt[as.character(wqt$Measurement)==unique(.$Measurement),],
             points(NZMGE,NZMGN,pch=16,cex=0.5,
                  col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd")[as.numeric(TrendScore)])
         )})
dev.off()
par(mfrow=c(1,1))

library(rpart)
windows()
par(mfrow=c(3,3),mar=c(0,0,0,0),mgp=c(3,1,0),cex.main=2,cex.lab=2,cex.axis=2,xpd=NA)
for(measure in uMeasures){
  tfp = wqt%>%
    dplyr::filter((period==10&Measurement==measure))%>%
    droplevels
  fit <- rpart(data=tfp,MKProbability~NZMGE+NZMGN+Region+Median+Quartile+SWQLanduse+SWQAltitude+period,
               control=rpart.control(cp=0.001,minbucket=5,xval=5))
  tfp$PredMK = predict(fit)
  tfp$Predsc =cut(tfp$PredMK, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                  labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
  tfp$Predsc=factor(tfp$Predsc,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
  plot(tfp$NZMGE,tfp$NZMGN,xlim=range(wqt$NZMGE),ylim=range(wqt$NZMGN),pch=16,
       col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd")[as.numeric(tfp$Predsc)],
       xlab='',ylab='',main=measure,xaxt='n',yaxt='n',bty='n',asp=1)
  with(wqt[as.character(wqt$Measurement)==measure,],
       points(NZMGE,NZMGN,pch=16,cex=0.5,
              col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF","#dddddd")[as.numeric(TrendScore)])
  )
}