#Groudnwater state analysis
rm(list=ls())
library(tidyverse)
source('H:/ericg/16666LAWA/LAWA2019/Scripts/LAWAFunctions.R')
EndYear <- 2018#year(Sys.Date())-1
startYear5 <- EndYear - 5+1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1
plotto=F
applyDataAbundanceFilters=F
# GWdata = read.csv('h:/ericg/16666LAWA/LAWA2019/GW/Data/2019-09-10/MfEExport_20190902.csv',stringsAsFactors = F)
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2019-09-10/GWQImport_20190902.xlsx",sheet="TS Readings")%>%as.data.frame
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2019-10-18/MfEExport_20191016.xlsx",
#                            sheet="Groundwater Details",guess_max = 50000)%>%as.data.frame
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2019-10-24/MfEExport_20191024.xlsx",
#                            sheet="Groundwater Details",guess_max = 50000)%>%as.data.frame
# GWdataold = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2019-10-31/MfEExport_20191031.xlsx",
#                               sheet="Groundwater Details",guess_max = 50000)%>%as.data.frame  #248011
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2019-11-07/MfEExport_20191106.xlsx",
#                            sheet="Groundwater Details",guess_max = 50000)%>%as.data.frame
#215061 of 26
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2019-11-14/MfEExport_20191113.xlsx",
#                            sheet="Groundwater Details",guess_max = 50000)%>%as.data.frame
#212607
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2019-11-22/GWExport-22-11-2019.xlsx",
#                            sheet="Sheet1",guess_max = 50000)%>%as.data.frame
#226406 of 17
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2019-11-25/GWExport-25-11-2019.xlsx",
#                            sheet="Sheet1",guess_max = 50000)%>%as.data.frame
#226379 of 17
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2020-03-06/GWExport-06-03-2020.xlsx",
#                            sheet="Sheet1",guess_max = 50000)%>%as.data.frame
#229001 of 17
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2020-03-13/GWExport-13-03-2020.xlsx",
#                            sheet=1,guess_max = 50000)%>%as.data.frame
#229262 of 17
# GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2020-03-20/GWExport-19-03-2020.xlsx",
#                            sheet=1,guess_max = 50000)%>%as.data.frame
#229114 of 17
GWdata = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2019/GW/Data/2020-03-27/GWExport-27-03-2020.xlsx",
                           sheet=1,guess_max = 50000)%>%as.data.frame
#229086 of 17


#
#6/3/20
#CHeck BOPRC for censored ecoli
GWdata%>%filter(Source=="Bay of Plenty")%>%grepl(pattern = '<',x = .$`Result-prefix`)%>%sum
#BOP censoring indicated in the result-prefix column
#Check WEstCoast ecolie censoring
GWdata%>%filter(Source=="West Coast",Variable_aggregated=="E.coli")%>%
  filter(grepl(pattern = '<',x = .$`Result-prefix`))%>%head
GWdata%>%filter(grepl(pattern='<',x=.$`Result-prefix`))%>%group_by(Source,.drop = F)%>%dplyr::summarise(n=n())
#Also  Auckland and Waikato on result-prefix
#/6/3/20

GWdata <- GWdata%>%mutate(LawaSiteID=`LAWA_ID`,
                          Measurement=`Variable_aggregated`,
                          Region=`Source`,
                          # Date = `Date`,
                          Value=`Result-edited`,
                          siteMeas=paste0(LAWA_ID,'.',Variable_aggregated))
#229086 of 22

#Some LawaSiteIDs went by two different names.  Often they were similarity.
#This finds the commonality and uses that
#It has a side effect of dropping repeated words. Toi Toi St would become Toi St.
# SiteID=split(GWdata,GWdata$LawaSiteID)%>%
#   purrr::map(~paste(Reduce(intersect,x = strsplit(c(.$Site_ID),' ')),collapse=' '))
# GWdata$SiteID=SiteID[GWdata$LawaSiteID]
# ambigSet = GWdata%>%filter(SiteID!=`Site Id`)%>%select(LawaSiteID)%>%distinct%>%unlist
# GWdata%>%filter(LawaSiteID%in%ambigSet)%>%select(LawaSiteID,`Site Id`,`Monitoring Site Id`,SiteID)%>%arrange(LawaSiteID)%>%distinct

# GWdata <- GWdata%>%select(-`Site Id`,-`Monitoring Site Id`)%>%distinct

GWdata <- GWdata%>%distinct
#214254 of 31 11-7
#212401 of 31 11-14
#225242 of 22 11-22
#225898 of 22 11-25
#228933 of 22 13-3-2020
#229104 of 22 20-3-20
#229050 of 22 27-3-20

GWdata$Value[which(GWdata$`Result-prefix`=='<')] <- GWdata$`Result-edited`[which(GWdata$`Result-prefix`=='<')]*0.5
GWdata$Value[which(GWdata$`Result-prefix`=='>')] <- GWdata$`Result-edited`[which(GWdata$`Result-prefix`=='>')]*1.1

if(plotto){
  table(GWdata$Value==GWdata$`Result-edited`)
  table(GWdata$`Result-prefix`==""|is.na(GWdata$`Result-prefix`))
  GWdata[which(GWdata$`Result-prefix`!="" & GWdata$Value==GWdata$`Result-edited`),]
}

GWdata$Censored=FALSE
GWdata$Censored[grepl(pattern = '^[<|>]',GWdata$`Result-prefix`)]=TRUE
GWdata$CenType='not'
GWdata$CenType[grepl(pattern = '^<',GWdata$`Result-prefix`)]='lt'
GWdata$CenType[grepl(pattern = '^>',GWdata$`Result-prefix`)]='gt'

table(GWdata$CenType)

if(plotto){
  table(GWdata$Region,GWdata$Measurement)
}

noData = which(is.na(GWdata$Date)&is.na(GWdata$`Result-raw`))
if(length(noData)>0){
  GWdata <- GWdata[-noData,]
}
rm(noData)
#208767 of 33 11-7
#206960 of 33 11-14
#220201 of 24 11-22
#220465 of 24 11-25
#223521 of 24 6-3-2020
#223836 of 24 13-3-20
#224021 of 24 20-3-20
#224046 of 24 27-3-20

#Conductivity is in different units ####
table(GWdata$Variable_units[GWdata$Variable_aggregated=="Electrical conductivity/salinity"])
table(GWdata$Variable_units[GWdata$Variable_aggregated=="Electrical conductivity/salinity"],
      GWdata$Source[GWdata$Variable_aggregated=="Electrical conductivity/salinity"])
par(mfrow=c(2,1),mar=c(10,4,4,2))
with(GWdata[GWdata$Variable_aggregated=="Electrical conductivity/salinity"&GWdata$Value>0,],
     plot(as.factor(Variable_units),Value,log='y',las=2))
#Set all units to S/m
these = which(GWdata$Variable_units%in%c('µS/cm','us/cm','uS/cm'))
GWdata$Variable_units[these] <- 'µS/cm'

these = which(GWdata$Variable_units%in%c('mS/cm','ms/cm'))
GWdata$Variable_units[these] <- 'µS/cm'
GWdata$Value[these] = GWdata$Value[these]*1000

these = which(GWdata$Variable_units=='mS/m')
GWdata$Variable_units[these] <- 'µS/cm'
GWdata$Value[these] = GWdata$Value[these]*1000/100

these = which(GWdata$Variable_units%in%c('mS/m @25 deg C','mS/m @25°C','mS/m@20C'))
GWdata$Variable_units[these] <- 'µS/cm'
GWdata$Value[these] = GWdata$Value[these]*1000/100

with(GWdata[GWdata$Variable_aggregated=="Electrical conductivity/salinity"&GWdata$Value>0,],
     plot(as.factor(Variable_units),Value,log='y',las=2))
mtext(side = 1,text='µS/cm')

#FreqCheck expects a one called "Date" ####
GWdata$myDate <- as.Date(as.character(GWdata$Date))
GWdata <- GetMoreDateInfo(GWdata)
GWdata$monYear = format(GWdata$myDate,"%b-%Y")
GWdata$quYear = paste0(quarters(GWdata$myDate),'-',format(GWdata$myDate,'%Y'))

write.csv(GWdata,'h:/ericg/16666LAWA/LAWA2019/GW/Data/GWdata.csv',row.names=F)
# GWdata = read.csv('h:/ericg/16666LAWA/LAWA2019/GW/Data/GWdata.csv',stringsAsFactors=F,check.names = F)



#STATE ####
#Carl Hanson 11/9/2019:  We only need state and trend for five parameters: 
#    nitrate nitrogen, chloride, DRP, electrical conductivity and E. coli.
periodYrs=5
GWdataRelevantVariables <- GWdata%>%
  filter(Measurement%in%c("Nitrate nitrogen","Chloride",
                          "Dissolved reactive phosphorus",
                          "Electrical conductivity/salinity",
                          "E.coli","Ammoniacal nitrogen"))%>%
  filter(`Result-raw`!='*')%>%
  dplyr::filter(lubridate::year(myDate)>=(EndYear-periodYrs+1)&lubridate::year(myDate)<=EndYear)%>%  
  dplyr::group_by(LawaSiteID,Measurement,monYear)%>%
  select(-'Result-raw',-'Result-edited',-'Result-metadata',-'Variable')%>%
  dplyr::mutate(Value=mean(Value,na.rm=T),
                Date=first(Date,1),
                myDate=first(myDate,1))%>%
  ungroup%>%distinct

freqs <- split(x=GWdataRelevantVariables,f=GWdataRelevantVariables$siteMeas)%>%purrr::map(~freqCheck(.))%>%unlist
table(freqs)
# freqs
# bimonthly    monthly quarterly     
#        19       190       3547   11-14
#        37       196       3649   11-22
#        36       192       3829   11-25
#        36       192       4000   03-06-2020
#        36       180       3992   03-13-2020
#        32       180       3917   03-20-2020
#        35        180      3994   03-27-2020
#        35        180      3993   04-23-2020
GWdataRelevantVariables$Frequency=freqs[GWdataRelevantVariables$siteMeas]  
rm(freqs)

GWdataRelevantVariables$CenBin = 0
GWdataRelevantVariables$CenBin[GWdataRelevantVariables$CenType=='lt'] = 1
GWdataRelevantVariables$CenBin[GWdataRelevantVariables$CenType=='gt'] = 2



#Calculate state (median) ####
#Get a median value per site/Measurement combo
GWmedians <- GWdataRelevantVariables%>%
  group_by(LawaSiteID,Measurement)%>%
  dplyr::summarise(median=quantile(Value,probs = 0.5,type=5,na.rm=T),
                   MAD = quantile(abs(median-Value),probs=0.5,type=5,na.rm=T),
                   count = n(),
                   minPerYear = min(as.numeric(table(factor(as.character(lubridate::year(myDate)),
                                                            levels=as.character(startYear5:EndYear))))),  
                   nYear = length(unique(Year)),
                   nQuart=length(unique(Qtr)),
                   nMonth=length(unique(Month)),
                   censoredCount = sum(!is.na(`Result-prefix`)),
                   CenType = Mode(CenBin),
                   Frequency=unique(Frequency))%>%
  ungroup%>%
  mutate(censoredPropn = censoredCount/count)

GWmedians$CenType = as.character(GWmedians$CenType)
GWmedians$CenType[GWmedians$CenType==1] <- '<'
GWmedians$CenType[GWmedians$CenType==2] <- '>'
GWmedians$censMedian = GWmedians$median
GWmedians$censMedian[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='<'] <- 
  paste0('<',2*GWmedians$median[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='<'])
GWmedians$censMedian[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='>'] <- 
  paste0('>',GWmedians$median[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='>']/1.1)

#1 is detect, 2 is non-detect
GWmedians$EcoliDetect=NA
GWmedians$EcoliDetect[which(GWmedians$Measurement=="E.coli")] <- "1"  #Detect
GWmedians$EcoliDetect[which(GWmedians$Measurement=="E.coli"&(GWmedians$censoredPropn>0.5|GWmedians$median==0|is.na(GWmedians$median)))] <- "2"  #Non-detect
table(GWmedians$EcoliDetect)   #81 545
GWmedians$EcoliDetectAtAll=NA
GWmedians$EcoliDetectAtAll[which(GWmedians$Measurement=="E.coli")] <- "1"  #Detect
GWmedians$EcoliDetectAtAll[which(GWmedians$Measurement=="E.coli"&(GWmedians$censoredPropn==1|GWmedians$median==0|is.na(GWmedians$median)))] <- "2"  #Non-detect
table(GWmedians$EcoliDetectAtAll) #357 269

table(GWmedians$EcoliDetect,GWmedians$EcoliDetectAtAll)


#3845 of 11 11-7
#3756 of 11 11-14
#3882 of 11 11-22
#4057 of 11 11-25
#4228 of 11 03-06-2020
#4208 of 12 03-13-20
#4208 of 13 03-20-20  Added ecoliDetect
#4209 of 15 03-27-20
#4208 of 16 04-23-2020 added ecoliDetectAtAll

GWmedians$meas = factor(GWmedians$Measurement,labels=c("NH4","Cl","DRP","ECOLI","NaCl","NO3"))


#Plotting
if(plotto){
  with(GWmedians[GWmedians$Frequency=='quarterly',],table(minPerYear,count))
  GWmedians[which(GWmedians$Frequency=='quarterly'&GWmedians$count==20),]
  par(mfrow=c(1,1))
  plot(GWmedians$median,GWmedians$MAD,log='xy',xlab='Median',ylab='MAD',
       col=as.numeric(factor(GWmedians$Measurement)),pch=16,cex=1)
  abline(0,1)
  GWmedians%>%filter(MAD>median)
  plot(GWmedians$median,GWmedians$MAD/GWmedians$median,log='x',xlab='Median',ylab='MAD/Median',
       col=as.numeric(factor(GWmedians$Measurement)),pch=16,cex=1)

  #Show data abundance with original filter cutoffs:
  par(mfrow=c(3,1))
  with(GWmedians%>%filter(Frequency=='monthly'),hist(count,main='monthly'));abline(v=30,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='bimonthly'),hist(count,main='bimonthly',breaks = 20));abline(v=15,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='quarterly'),hist(count,main='quarterly'));abline(v=10,lwd=2,col='red')

  par(mfrow=c(3,1))
  with(GWmedians%>%filter(Frequency=='monthly'),hist(minPerYear,main='monthly'));abline(v=6,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='bimonthly'),hist(minPerYear,main='bimonthly',breaks = 20));abline(v=3,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='quarterly'),hist(minPerYear,main='quarterly'));abline(v=2,lwd=2,col='red')
  
  par(mfrow=c(3,1))
  with(GWmedians%>%filter(Frequency=='monthly'),hist(nMonth,main='monthly'));abline(v=11.5,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='bimonthly'),hist(nMonth,main='bimonthly',breaks=12));abline(v=5,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='quarterly'),hist(nQuart,main='quarterly'));abline(v=3.5,lwd=2,col='red')
}

GWmedians$Exclude<-FALSE
if(applyDataAbundanceFilters){
  GWmedians$Exclude[which(tolower(GWmedians$Frequency)=="bimonthly" & GWmedians$count<(0.5*6*periodYrs))] <- TRUE   #15 ouf ot 6*5 = 30
  GWmedians$Exclude[which(tolower(GWmedians$Frequency)=="monthly" & GWmedians$count<(0.5*12*periodYrs))] <- TRUE    #30 out of 5*12 = 60
  GWmedians$Exclude[which(tolower(GWmedians$Frequency)=="quarterly" & GWmedians$count<(0.5*4*periodYrs))] <- TRUE   #10 out of 5*4 = 20
  table(GWmedians$Exclude)
  # FALSE  TRUE 
  # 2577   1268 11-7
  # 2489   1267 11-14
  # 2473   1409 11-22
  # 2627   1430 11-25
  GWmedians$Exclude[which(tolower(GWmedians$Frequency)=="bimonthly" & GWmedians$minPerYear<(0.5*6))] <- TRUE   #3 out of 6
  GWmedians$Exclude[which(tolower(GWmedians$Frequency)=="monthly" & GWmedians$minPerYear<(0.5*12))] <- TRUE    #6 out of 12
  GWmedians$Exclude[which(tolower(GWmedians$Frequency)=="quarterly" & GWmedians$minPerYear<(0.5*4))] <- TRUE   #2 out of 4
  table(GWmedians$Exclude)
  # FALSE  TRUE 
  # 2086   1759
  # 2096   1660
  # 2009   1873
  # 2154   1903 11-25
  GWmedians$Exclude[which(tolower(GWmedians$Frequency)=='bimonthly' & GWmedians$nMonth<6)] <- TRUE  #Need representation in each bimonth
  GWmedians$Exclude[which(tolower(GWmedians$Frequency)=='monthly' & GWmedians$nMonth<12)] <- TRUE  #Need representation in each month
  GWmedians$Exclude[which(tolower(GWmedians$Frequency)=='quarterly' & GWmedians$nQuart<4)] <- TRUE  #Need representation in each quarter
  table(GWmedians$Exclude)
  # FALSE  TRUE 
  # 2006  1839 
  # 2016  1740
  # 1925  1957
  # 2074  1983 11-25
  
  #Get the monthly ones that were excluded by the above rules, reduce them to quarterly resolution and try again ####
  GWdataReducedTemporalResolution <- GWdata%>%filter(LawaSiteID%in%(GWmedians%>%filter(Exclude&Frequency=="monthly")%>%select(LawaSiteID)%>%unlist)&
                               Measurement%in%(GWmedians%>%filter(Exclude&Frequency=="monthly")%>%select(Measurement)%>%unlist))%>%
    dplyr::filter(lubridate::year(myDate)>=(EndYear-periodYrs+1)&lubridate::year(myDate)<=EndYear)%>%
    dplyr::group_by(LawaSiteID,Measurement,quYear)%>%
    select(-'Result-raw',-'Result-edited',-'Result-metadata',-'Variable',-'monYear',-'Month')%>%
    dplyr::summarise(siteMeas=first(siteMeas),
                     Value=quantile(Value,probs=0.5,type=5,na.rm=T),  #was mean
                     Date=first(Date,1),
                     myDate=first(myDate,1),
                     Qtr = first(Qtr,1),
                     `Result-prefix`=any(!is.na(`Result-prefix`)))%>%
    ungroup%>%distinct
  freqs <- split(x=GWdataReducedTemporalResolution,f=GWdataReducedTemporalResolution$siteMeas)%>%purrr::map(~freqCheck(.))%>%unlist
  table(freqs)
  # freqs
  # quarterly   
  #  191 11-7
  #  191 11-14
  #  202 11-22
  #  196 11-25
  
  GWdataReducedTemporalResolution$Frequency=freqs[GWdataReducedTemporalResolution$siteMeas]  
  rm(freqs)
  
  #Calcualte medians on this set that was reduced from monthly to quarterly
  GWmediansOfReducedTemporalResolution <- GWdataReducedTemporalResolution%>%group_by(LawaSiteID,Measurement)%>%
    dplyr::summarise(median=quantile(Value,probs = 0.5,type=5,na.rm=T),
                     MAD = quantile(abs(median-Value),probs=0.5,type=5,na.rm=T),
                     count = n(),
                     minPerYear = min(as.numeric(table(lubridate::year(myDate)))),
                     nQuart=length(unique(Qtr)),
                     nMonth=NA,
                     censoredCount = sum(`Result-prefix`),
                     Frequency=unique(Frequency))%>%
    ungroup%>%
    mutate(censoredPropn = censoredCount/count)
  
  GWmediansOfReducedTemporalResolution$meas = factor(GWmediansOfReducedTemporalResolution$Measurement,labels=c("NH4","Cl","DRP","ECOLI","NaCl","NO3"))
  #191 of
  #202
  #196
  
  with(GWmediansOfReducedTemporalResolution[GWmediansOfReducedTemporalResolution$Frequency=='quarterly',],table(minPerYear,count))
  GWmediansOfReducedTemporalResolution[which(GWmediansOfReducedTemporalResolution$Frequency=='quarterly'&GWmediansOfReducedTemporalResolution$count==20),]
  
  #Check whether the reduced time resolution set now have enough data to stand in as quarterly or bimonthly, where they failed as monthly
  GWmediansOfReducedTemporalResolution$Exclude<-FALSE
  GWmediansOfReducedTemporalResolution$Exclude[which(tolower(GWmediansOfReducedTemporalResolution$Frequency)=="bimonthly" & GWmediansOfReducedTemporalResolution$count<(0.5*6*periodYrs))] <- TRUE   #15 ouf ot 6*5 = 30
  GWmediansOfReducedTemporalResolution$Exclude[which(tolower(GWmediansOfReducedTemporalResolution$Frequency)=="quarterly" & GWmediansOfReducedTemporalResolution$count<(0.5*4*periodYrs))] <- TRUE   #10 out of 5*4 = 20
  table(GWmediansOfReducedTemporalResolution$Exclude)
  # FALSE  TRUE 
  # 144    47
  # 153    49
  # 147    49 11-25
  GWmediansOfReducedTemporalResolution$Exclude[which(tolower(GWmediansOfReducedTemporalResolution$Frequency)=="bimonthly" & GWmediansOfReducedTemporalResolution$minPerYear<(0.5*6))] <- TRUE   #3 out of 6
  GWmediansOfReducedTemporalResolution$Exclude[which(tolower(GWmediansOfReducedTemporalResolution$Frequency)=="quarterly" & GWmediansOfReducedTemporalResolution$minPerYear<(0.5*4))] <- TRUE   #2 out of 4
  table(GWmediansOfReducedTemporalResolution$Exclude)
  # FALSE  TRUE 
  # 104     87
  # 110     92
  # 104    94
  GWmediansOfReducedTemporalResolution$Exclude[which(tolower(GWmediansOfReducedTemporalResolution$Frequency)=='bimonthly' & GWmediansOfReducedTemporalResolution$nMonth<6)] <- TRUE  #Need representation in each bimonth
  GWmediansOfReducedTemporalResolution$Exclude[which(tolower(GWmediansOfReducedTemporalResolution$Frequency)=='quarterly' & GWmediansOfReducedTemporalResolution$nQuart<4)] <- TRUE  #Need representation in each quarter
  table(GWmediansOfReducedTemporalResolution$Exclude)
  # FALSE  TRUE 
  # 104     87 
  # 110     92
  # 104    92
  
  #Label them as reduced from montnly resolution
  GWmediansOfReducedTemporalResolution$Frequency = paste0(GWmediansOfReducedTemporalResolution$Frequency,'_redFromMonthly')
  GWmediansOfReducedTemporalResolution <- GWmediansOfReducedTemporalResolution[!GWmediansOfReducedTemporalResolution$Exclude,]
  
  
  #Drop these out of the original (unreduced) dataset, to replace with the reduced-resolution substitute
  these = which(paste0(GWmedians$LawaSiteID,GWmedians$Measurement)%in%
                  paste0(GWmediansOfReducedTemporalResolution$LawaSiteID,GWmediansOfReducedTemporalResolution$Measurement))
  GWmedians = GWmedians[-these,]
  GWmedians = rbind(GWmedians,GWmediansOfReducedTemporalResolution)
  rm(GWmediansOfReducedTemporalResolution)
}  # section only done when filtering by data abundance
if(plotto){
  par(mfrow=c(3,3))
  plot(GWmedians$median,GWmedians$MAD/(GWmedians$median*GWmedians$count^0.5),
       log='xy',pch=c(1,16)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
  GWmedians%>%split(GWmedians$Measurement)%>%
    purrr::map(~plot(.$median,.$MAD/(.$median*.$count^0.5),
                     log='xy',pch=c(1,16)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count)))
  
  par(mfrow=c(3,3))
  plot(GWmedians$median,GWmedians$MAD/(GWmedians$median),
       log='xy',lwd=c(2,1)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
  abline(h=1)
  GWmedians%>%split(GWmedians$Measurement)%>%
    purrr::map(~{plot(.$median,.$MAD/(.$median),
                      log='xy',lwd=c(2,1)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count),ylim=c(0.001,5))
      abline(h=1)})
  
  par(mfrow=c(3,3))
  plot(GWmedians$median,GWmedians$MAD,
       log='xy',pch=c(1,16)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
  abline(0,1)
  GWmedians%>%split(GWmedians$Measurement)%>%
    purrr::map(~{plot(.$median,.$MAD,
                      log='xy',pch=c(1,16)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count))
      abline(0,1)})
  
  table(GWmedians$count)
}

GWmedians <- GWmedians%>%filter(!Exclude)
#3845 to 2073  11-7
#3756 to 2072  11-14
#3882 to 1985  11-22
#4057 to 2130  11-25
#4228 unfiltered 03-06-2020
#4208          03-13-20
#4208          03-20-20
#4209          03-27-20
#4208          04-23-20

if(plotto){
  table(GWmedians$count)
  table(GWmedians$count,GWmedians$meas)
  table(GWmedians$count,GWmedians$Frequency)
  
  par(mfrow=c(1,1))
  plot(density(GWmedians$count,from=min(GWmedians$count),adjust=2),xlim=range(GWmedians$count))
  
  
  par(mfrow=c(3,3))
  uMeasures=unique(GWmedians$Measurement)
  for(measure in uMeasures){
    dtp = GWmedians%>%filter(Measurement==measure,median>=0)
    rtp = GWdataRelevantVariables%>%filter(Measurement==measure,Value>=0)
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
  rm(dtp,rtp,measure)
  #pick a site, pick a measurement
  nExample=0
  while(nExample<3){
    site=sample(x = unique(GWmedians$LawaSiteID),size = 1)
    meas=sample(x = unique(GWmedians$Measurement),size = 1)
    if(length(which(GWmedians$LawaSiteID==site&GWmedians$Measurement==meas))>0){
      toPlot = GWdataRelevantVariables%>%filter(LawaSiteID==site&Measurement==meas)%>%select(Value)%>%drop_na%>%unlist
      plot(density(toPlot,na.rm=T,from=0),xlab='',main=paste(site,meas))
      rug(toPlot)
      abline(v = GWmedians%>%filter(LawaSiteID==site&Measurement==meas)%>%select(median))
      nExample=nExample+1
    }
  }
  rm(site,meas)
}


  GWmedians$Source = GWdata$Source[match(GWmedians$LawaSiteID,GWdata$LawaSiteID)]
#Westy coast couldnt get us censor limits, so dont publish their state
# if(Sys.Date()=="2020-03-20"){
  WCEc=which(GWmedians$Source=="West Coast" & GWmedians$Measurement=="E.coli")
  GWmedians$median[WCEc]=NA
  GWmedians$EcoliDetect[WCEc]=NA
  GWmedians$EcoliDetectAtAll[WCEc]=NA
  GWmedians$censoredPropn[WCEc]=NA
  GWmedians$censoredCount[WCEc]=NA
  rm(WCEc)
# }


#Export Median values
dir.create(paste0("h:/ericg/16666LAWA/LAWA2019/GW/Analysis/",format(Sys.Date(),"%Y-%m-%d")))
write.csv(GWmedians,file = paste0('h:/ericg/16666LAWA/LAWA2019/GW/Analysis/',format(Sys.Date(),"%Y-%m-%d"),
                                  '/ITEGWState',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
rm(GWdataRelevantVariables)
if(exists('GWdataReducedTemporalResolution')){rm(GWdataReducedTemporalResolution)}


######################################################################################
#Calculate trends ####
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
foreach(py = c(10,15),.combine=rbind,.errorhandling="stop")%dopar%{
  pn=0.75
  tredspy <- split(GWtrendData,GWtrendData$siteMeas)%>%
    purrr::map(~trendCore(.,periodYrs=py,proportionNeeded=pn)) #Proportion needed is greater or equal
  tredspy <- do.call(rbind.data.frame,tredspy)
  row.names(tredspy)=NULL
  
  # treds10 <- split(GWtrendData,GWtrendData$siteMeas)%>%purrr::map(~trendCore(.,periodYrs=10,proportionNeeded=pn))
  # treds10=do.call(rbind.data.frame,treds10)
  # row.names(treds10)=NULL
  
  return(tredspy)
}->GWtrends
stopCluster(workers)
rm(workers)

#8592 of 39  11-11
#8382 of 39  14-11
#8478 of 39  22-11
#8836 of 39  25-11
#8886 of 39  13-03-2020
#8888 of 39  27-03-2020

GWtrends$Source = GWdata$Source[match(GWtrends$LawaSiteID,GWdata$LawaSiteID)]
#Westy coast couldnt get us censor limits, so dont publish their state
# if(Sys.Date()=="2020-03-20"){
WCEc=which(GWtrends$Source=="West Coast" & GWtrends$Measurement=="E.coli")
GWtrends[WCEc,match(c("KWstat","pvalue",
"nObs","S","VarS","D","tau","Z","p","MKProbability","AnalysisNote","prop.censored",
"prop.unique","no.censorlevels","Median","Sen_VarS","AnnualSenSlope","Intercept","Lci","Uci","Sen_Probability",
"Probabilitymax","Probabilitymin","Percent.annual.change","standard","ConfCat"),names(GWtrends))]=NA
rm(WCEc)
# }


GWtrends$ConfCat <- cut(GWtrends$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                        labels = rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
GWtrends$ConfCat=factor(GWtrends$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
GWtrends$TrendScore=as.numeric(GWtrends$ConfCat)-3
GWtrends$TrendScore[is.na(GWtrends$TrendScore)]<-(NA)

fGWt = GWtrends%>%filter(!grepl('^unassess',GWtrends$frequency)&!grepl('^Insufficient',GWtrends$AnalysisNote))

# 2476 of 41  11-7
# 2515 of 41  11-14
# 2405 of 41  11-22
# 2572 of 41  11-25
# 2585 of 31  03-13-2020
# 2560 of 31  03-27-2020

#    nitrate nitrogen, chloride, DRP, electrical conductivity and E. coli.
# fGWt = fGWt%>%filter(Measurement %in% c("Nitrate nitrogen","Chloride","Dissolved reactive phosphorus",
#                                         "Electrical conductivity/salinity","E.coli"))

# table(GWtrends$proportionNeeded,GWtrends$period)
# knitr::kable(table(fGWt$proportionNeeded,fGWt$period),format='rst')

if(plotto){
  with(fGWt,knitr::kable(table(AnalysisNote,period),format='rst'))
  
  knitr::kable(with(fGWt%>%filter(period==10 & proportionNeeded==0.75)%>%
                      droplevels,table(Measurement,TrendScore)),format='rst')
  
  table(GWtrends$numQuarters,GWtrends$period)
  
  table(GWtrends$Measurement,GWtrends$ConfCat,GWtrends$proportionNeeded,GWtrends$period)
  table(fGWt$Measurement,fGWt$ConfCat,fGWt$proportionNeeded,fGWt$period)
  
  # with(GWtrends[GWtrends$period==5,],table(Measurement,ConfCat))
  with(GWtrends[GWtrends$period==10,],table(Measurement,ConfCat))
  with(GWtrends[GWtrends$period==15,],table(Measurement,ConfCat))
  
  table(GWtrends$Measurement,GWtrends$period)
  table(GWtrends$ConfCat,GWtrends$period)
  
  knitr::kable(table(GWtrends$ConfCat,GWtrends$Measurement,GWtrends$period))
}

#Export Trend values
write.csv(GWtrends,file = paste0('h:/ericg/16666LAWA/LAWA2019/GW/Analysis/',format(Sys.Date(),"%Y-%m-%d"),
                                 '/ITEGWTrend',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
write.csv(fGWt,file = paste0('h:/ericg/16666LAWA/LAWA2019/GW/Analysis/',format(Sys.Date(),"%Y-%m-%d"),
                             '/ITEGWTrendSuccess',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)


# 
# #Put the state on the trend?
# 
# GWtrends$Calcmedian = GWmedians$median[match(x = paste(GWtrends$LawaSiteID,GWtrends$Measurement),
#                                              table = paste(GWmedians$LawaSiteID,GWmedians$Measurement))]
# GWtrends$Calcmedian[GWtrends$Calcmedian<=0] <- NA
# par(mfrow=c(2,3))
# for(meas in unique(GWtrends$Measurement)){
#   dfp=GWtrends%>%dplyr::filter(Measurement==meas)
#   plot(factor(dfp$ConfCat),dfp$Calcmedian,main=meas,log='y')
# }
# 
# head(GWmedians%>%filter(!Exclude)%>%select(-Exclude))
