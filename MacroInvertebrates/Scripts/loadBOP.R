## Load libraries ------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
require(RCurl)

agency='boprc'
setwd("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates")

df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/MetaData/",agency,"Macro_config.csv"),sep=",",stringsAsFactors=FALSE)
Measurements <- subset(df,df$Type=="Measurement")[,1]
# sites <- subset(df,df$Type=="Site")[,1]

#WFS  http://geospatial.boprc.govt.nz/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=getfeature&service=WFS&VERSION=1.1.0&typename=MonitoringSiteReferenceData&srsName=EPSG:4326
# http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?service=SOS&version=2.0.0&request=GetCapabilities

siteTable=loadLatestSiteTableMacro()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

library(parallel)
library(doParallel)
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
})
suppressWarnings(rm(Data))
# for(i in 1:length(sites)){
foreach(i = 1:length(sites),.errorhandling = 'stop',.combine = rbind)%dopar%{
  cat('\n',sites[i],i,'out of',length(sites),'\t')
  for(j in 1:length(Measurements)){
    url <- paste0("http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?",
                  "service=SOS&version=2.0.0&request=GetObservation&",
                  "observedProperty=",Measurements[j],
                  "&featureOfInterest=",sites[i],
                  "&temporalfilter=om:phenomenonTime,P15Y/2020-06-01")
    url <- URLencode(url)
    xmlfile <- ldMWQ(url,agency=agency)
    if(!is.null(xmlfile) && 
       grepl(pattern = sites[i],x = xmlValue(xmlRoot(xmlfile)),ignore.case = T) &&
       !grepl(pattern = 'invalid',x = xmlValue(xmlRoot(xmlfile)),ignore.case = T)){
      #Create vector of times
      time <- sapply(getNodeSet(doc=xmlfile, "//wml2:time"), xmlValue)
      #Create vector of  values
      value <- sapply(getNodeSet(doc=xmlfile, "//wml2:value"), xmlValue)
      if(length(time)!=0){
         cat('got some',Measurements[j],'\t')
        u<-unique(sapply(getNodeSet(xmlfile, path="//wml2:uom"),function(el) xmlGetAttr(el, "code")))
        df <- data.frame(Site=sites[i],Measurement=Measurements[j],time=time,value=value,Units=u, stringsAsFactors = FALSE)
        if(!exists("Data")){
          Data <- df
        } else{
          Data <- rbind.data.frame(Data, df)
        }
      }
    }
  }
  return(Data)
}->Data
stopCluster(workers)
rm(workers)


library(tidyverse)
year2017=read_csv("H:/ericg/16666Lawa/LAWA2020/MacroInvertebrates/Data/BOPRC_Data_for_LAWA_2017_2018.csv")%>%
  drop_na(Aquarius)%>%
  tidyr::gather(key="Measurement",value="Value",c("MCI_Actual","Richness","P_EPT1_r"))%>%
  transmute(Site=Aquarius,Measurement=Measurement,time="2017-12-31T00:00:00.000Z",value=Value,Units="None")
Data=rbind(Data,year2017)
rm(year2017)

con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", agency)

max<-nrow(Data)
#max<-nrows(datatbl)

i<-1
#for each site
while(i<=max){
  s<-Data$Site[i]
  # store first counter going into while loop to use later in writing out sample values
  start<-i
  
  cat(i,Data$Site[i],"\n")   ### Monitoring progress as code runs
  
  while(Data$Site[i]==s){
    #for each measurement
    con$addTag("Measurement",  attrs=c(CouncilSiteID=Data$Site[i]), close=FALSE)
    con$addTag("DataSource",  attrs=c(Name=Data$Measurement[i],NumItems="2"), close=FALSE)
    con$addTag("TSType", "StdSeries")
    con$addTag("DataType", "WQData")
    con$addTag("Interpolation", "Discrete")
    con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
    con$addTag("ItemName", Data$Measurement[i])
    con$addTag("ItemFormat", "F")
    con$addTag("Divisor", "1")
    con$addTag("Units", Data$Units[i])
    con$addTag("Format", "#.###")
    con$closeTag() # ItemInfo
    con$closeTag() # DataSource
    
    # for the TVP and associated measurement water quality parameters
    con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="2"),close=FALSE)
    d<- Data$Measurement[i]
    
    cat("       - ",Data$Measurement[i],"\n")   ### Monitoring progress as code runs
    
    while(Data$Measurement[i]==d&Data$Site[i]==s){
      # for each tvp
      con$addTag("E",close=FALSE)
      con$addTag("T",Data$time[i])
      con$addTag("I1", Data$value[i])
      con$addTag("I2", paste("Units", Data$Units[i], sep="\t"))
      
      con$closeTag() # E
      i<-i+1 # incrementing overall for loop counter
      if(i>max){break}
    }
    # next
    con$closeTag() # Data
    con$closeTag() # Measurement
    
    if(i>max){break}
    # Next
  }
  # store last counter going out of while loop to use later in writing out sample values
  end<-i-1
  
  # Adding WQ Sample Datasource to finish off this Site
  # along with Sample parameters
  con$addTag("Measurement",  attrs=c(CouncilSiteID=Data$Site[start]), close=FALSE)
  con$addTag("DataSource",  attrs=c(Name="WQ Sample", NumItems="1"), close=FALSE)
  con$addTag("TSType", "StdSeries")
  con$addTag("DataType", "WQSample")
  con$addTag("Interpolation", "Discrete")
  con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
  con$addTag("ItemName", "WQ Sample")
  con$addTag("ItemFormat", "S")
  con$addTag("Divisor", "1")
  con$addTag("Units")
  con$addTag("Format", "$$$")
  con$closeTag() # ItemInfo
  con$closeTag() # DataSource
  
  # for the TVP and associated measurement water quality parameters
  con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="1"),close=FALSE)
  # for each tvp
  ## LOAD SAMPLE PARAMETERS
  ## SampleID, ProjectName, SourceType, SamplingMethod and mowsecs
  sample<-Data[start:end,3]
  sample<-unique(sample)
  sample<-sample[order(sample)]
  ## THIS NEEDS SOME WORK.....
  for(a in 1:length(sample)){
    con$addTag("E",close=FALSE)
    con$addTag("T",sample[a])
    #put metadata in here when it arrives
    # con$addTag("I2", paste("QC", QC, sep="\t"))
    con$closeTag() # E
  }
  con$closeTag() # Data
  con$closeTag() # Measurement
}

# saveXML(con$value(), paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"Macro.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"Macro.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"),
          overwrite=T)

