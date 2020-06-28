stop("Convert EBOP to web-fetched data acquisition retrieval\nEmailed Paul Scholes July 19 twenty nineteen")


## Load libraries ------------------------------------------------
require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
#require(ggplot2)
#library(gridExtra)
#library(scales)
#require(tidyr)   ### for reshaping data
#library(readr)
source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')

# source("H:/ericg/16666LAWA/LAWA2020/LAWAScripts/lawa_state_functions.r")

trim <- function (x) gsub("^\\s+|\\s+$", "", x)

## Convert datestring to mow seconds (number of seconds since 1-Jan-1940 00:00)
mowSecs <- function(x){
  s<-strptime("1940-01-01","%Y-%m-%d", tz="GMT")  # USING GMT to avoid daylight time offset as default
  if(nchar(x[1])>=13){                               # arguments assume time based on NZST and NZDT for
    t<-strptime(x,"%d/%m/%Y %H:%M", tz="GMT")     # different parts of the year.
  } else {                                        # Will need to be aware of this for other work.
    t<-strptime(x,"%d/%m/%Y", tz="GMT")
  }
  x<-(t-s)*86400
}

value <- function (val){
  if(grepl(pattern = "^[<>]",x=val,perl = TRUE)){
    x<-gsub(pattern = "^[<>]", replacement = "", x = val)
  } else {x<-val}
  x<-trim(x)
}

nd <- function(val){
  n<-grepl(pattern = "^<",x=val,perl = TRUE)
}


### BAY OF PLENTY'UTF-8-BOM'
fname <- "H:/ericg/16666LAWA/LAWA2020/Lakes/Data/BoPLakesData2017a.csv"
df <- read_csv(fname,guess_max = 20000)
mtRows=which(apply(df,1,function(x)length(x)==sum(is.na(x))))
if(length(mtRows)>0){
  df=df[-mtRows,]
}
rm(mtRows)

df$`site name`[which(df$`site name`=='Site 1')] <- "Lake Rotoma Site 1"

df$mowsecs   <- mowSecs(df$sdate)
#check mowSec output
cat("Number of NAs in derived mowsec field: ",sum(is.na(df$mowsecs)),"out of",length(df$mowsecs),"rows\n")

df$Date.Time <- strptime(df$sdate,"%d/%m/%Y", tz="GMT")
df$DataSource <- df$parameter
df$Qmowsecs   <- df$mowsecs   ## storing original mowsec value should any duplicate samples be found.

df$value      <- as.numeric(sapply(df$`Value (mg/m3)`,value))
df$value[which(is.na(df$value))] <- df$`Value (mg/m3)`[which(is.na(df$value))]
df$nd         <- nd(df$`Uncencored value`)

# reorder data to enable writing to Hilltop File
# Sort by indexing order - Site, Measurement, DateTime
df <- df[order(df$`site name`,df$parameter,df$mowsecs),]

sites<-unique(df$`site name`)
lawa <-unique(df$`LAWA ID`)
measurements<-unique(df$parameter)


## Build XML Document --------------------------------------------
tm<-Sys.time()
cat("Building XML\n")
cat("Creating:",Sys.time()-tm,"\n")

con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", "Bay of Plenty")
#saveXML(con$value(), file="out.xml")

tab <- "\t"

max<-dim(df)[1]

i<-1
#for each site
while(i<=max){
  s<-df$`site name`[i]
  # store first counter going into while loop to use later in writing out sample values
  start<-i
  
  cat(i,df$`site name`[i],"\n")   ### Monitoring progress as code runs
  
  while(df$`site name`[i]==s){
    #for each parameter
    #cat(datatbl$SiteName[i],"\n")
    con$addTag("Measurement",  attrs=c(SiteName=df$`site name`[i]), close=FALSE)
    
    #### I need to join in the DatasourceName to the parameter name here, or perhaps in the qetl CSV
    con$addTag("DataSource",  attrs=c(Name=df$DataSource[i],NumItems="2"), close=FALSE)
    con$addTag("TSType", "StdSeries")
    con$addTag("DataType", "WQData")
    con$addTag("Interpolation", "Discrete")
    con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
    con$addTag("ItemName", df$parameter[i])
    con$addTag("ItemFormat", "F")
    con$addTag("Divisor", "1")
    con$addTag("Units", df$Units[i])
    #con$addTag("Units", "Joking")
    con$addTag("Format", df$Format[i])
    con$closeTag() # ItemInfo
    con$closeTag() # DataSource
    #saveXML(con$value(), file="out.xml")
    
    # for the TVP and associated parameter water quality parameters
    con$addTag("Data", attrs=c(DateFormat="mowsecs", NumItems="2"),close=FALSE)
    d<- df$parameter[i]
    
    cat("       - ",df$parameter[i],"\n")   ### Monitoring progress as code runs
    
    
    while(df$parameter[i]==d){
      
      # remember mowsec (record mowsec at end of while loop) mowsec <- df$mowsecs[i].
      # If next sample has the same mowsec 
      #       then increment mowsec by 1 AND write new result
      
      # Skipping mowsec check for first time through while loop...
      if(i!=start){
        if(df$mowsecs[i]==df$Qmowsecs[i-1]){
          ## If project the same, assume duplicate samples collected. Increment time by 1 second for each one.
          cat("          - duplicate sample time: ", df$Qmowsecs[i-1], " ",df$mowsecs[i], "\n")
          
          #Where sampleID is different, treat as independent sample at that date/time - add 1 second to time.
          df$mowsecs[i] <- df$mowsecs[i-1] + 1
          
        }
      } ## finish of counter check     
      
      # for each tvp
      # Handle Greater than symbols
      if(grepl(pattern = "^>",x =  df$`Uncencored value`[i],perl = TRUE)){
        con$addTag("E",close=FALSE)
        con$addTag("T",df$mowsecs[i])
        con$addTag("I1", gsub(pattern = "^>", replacement = "", x = df$`Uncencored value`[i]))
        con$addTag("I2", paste("$ND",tab,">",tab,
                               "Method",tab,df$Method[i],tab,
                               "Detection Limit",tab,df$DetectionLimit[i],tab,
                               "$QC",tab,df$QualityCode[i],tab,
                               "Result Value",tab,df$RawValue[i],tab,sep=""))
        con$closeTag() # E
        
        # Handle Less than symbols  
      } else if(grepl(pattern = "^<",x =  df$`Uncencored value`[i],perl = TRUE)){
        con$addTag("E",close=FALSE)
        con$addTag("T",df$mowsecs[i])
        con$addTag("I1", gsub(pattern = "^<", replacement = "", x = df$`Uncencored value`[i]))
        con$addTag("I2", paste("$ND",tab,"<",tab,
                               "Method",tab,df$Method[i],tab,
                               "Detection Limit",tab,df$DetectionLimit[i],tab,
                               "$QC",tab,df$QualityCode[i],tab,
                               "Result Value",tab,df$RawValue[i],tab,sep=""))
        con$closeTag() # E
        
        # Handle Asterixes  
      } else if(grepl(pattern = "^\\*",x =  df$`Uncencored value`[i],perl = TRUE)){
        con$addTag("E",close=FALSE)
        con$addTag("T",df$mowsecs[i])
        con$addTag("I1", gsub(pattern = "^\\*", replacement = "", x = df$`Uncencored value`[i]))
        con$addTag("I2", paste("$ND",tab,"*",tab,
                               "Method",tab,df$Method[i],tab,
                               "Detection Limit",tab,df$DetectionLimit[i],tab,
                               "$QC",tab,df$QualityCode[i],tab,
                               "Result Value",tab,df$RawValue[i],tab,sep=""))
        con$closeTag() # E
        
        # Write all other result values  
      } else {
        con$addTag("E",close=FALSE)
        con$addTag("T",df$mowsecs[i])
        if(df$parameter[i]%in%c("Secchi","pH")){
          con$addTag("I1", df$`Value (mg/m3)`[i])
        }else{
          con$addTag("I1", df$`Uncencored value`[i])
        }
        con$addTag("I2", paste("Method",tab,df$Method[i],tab,
                               "Detection Limit",tab,df$DetectionLimit[i],tab,
                               "$QC",tab,df$QualityCode[i],tab,
                               "Result Value",tab,df$RawValue[i],tab,sep=""))
        con$closeTag() # E
      }
      
      #correct<-0
      i<-i+1 # incrementing overall for loop counter
      if(i>max){break}
      
      
    }
    # next
    con$closeTag() # Data
    con$closeTag() # parameter
    
    
    if(i>max){break}
    # Next 
  }
  # store last counter going out of while loop to use later in writing out sample values
  end<-i-1
  
  # Adding WQ Sample Datasource to finish off this Site
  # along with Sample parameters
  con$addTag("Measurement",  attrs=c(SiteName=df$`site name`[start]), close=FALSE)
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
  con$addTag("Data", attrs=c(DateFormat="mowsecs", NumItems="1"),close=FALSE)
  # for each tvp
  if(0){
    ## THIS NEEDS SOME WORK.....
    ## just pulling out mowsecs Depth from, depth to, sample level, sample frequency, laketype
    sample<-df[,match(c("mowsecs","Frequency"),names(df))] ## 
    sample<-sample[start:end,]
    sample<-as.tbl(sample)
    # sample<-distinct(sample,mowsecs)
    
    sample<-sample[order(sample$mowsecs),]
    ## THIS NEEDS SOME WORK.....
    for(a in 1:nrow(sample)){ 
      con$addTag("E",close=FALSE)
      con$addTag("T",sample$mowsecs[a])
      # con$addTag("I1", paste("Depth From",tab,  sample$Depth.from[a], tab, "Depth To", tab, sample$Depth.to[a], tab, "Sample Level", tab,
      #                        sample$Samplelevel..epilimnion..thermocline..hypolimnion.[a], "Sample Frequency",tab, sample$SampleFrequency[a],
      #                        tab, "Sample type", sample$Laketype..Polymictic..Stratified..Brackish.[a], tab,  sep=""))
      con$addTag("I1", paste0("Sample Frequency",tab, sample$Frequency[a]))
      con$closeTag() # E
    }
  }
  con$closeTag() # Data
  con$closeTag() # Measurement    
  
}

cat("Saving: ",Sys.time()-tm,"\n")
saveXML(con$value(), file=paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/boprcLWQ.xml"))
cat("Finished",Sys.time()-tm,"\n")












##############################
#Water Quality version
stop()
## Load libraries ------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
require(RCurl)

agency='boprc'
setwd("H:/ericg/16666LAWA/LAWA2020/Lakes")

siteTable=loadLatestSiteTableLakes()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/MetaData/",agency,"LWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
#WFS  http://geospatial.boprc.govt.nz/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=getfeature&service=WFS&VERSION=1.1.0&typename=MonitoringSiteReferenceData&srsName=EPSG:4326
# http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?service=SOS&version=2.0.0&request=GetCapabilities
Measurements <- subset(df,df$Type=="Measurement")[,1]
Measurements <- as.vector(Measurements)



suppressWarnings(rm(Data))
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\t')
  for(j in 1:length(Measurements)){
    url <- paste0("http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?service=SOS&version=2.0.0&request=GetObservation&",
                  "observedProperty=",Measurements[j],
                  "&featureOfInterest=",sites[i],
                  "&temporalfilter=om:phenomenonTime,P15Y/2020-01-01")
    url <- URLencode(url)
    xmlfile <- ldWQ(url,agency=agency)
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
}

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
    
    while(Data$Measurement[i]==d){
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

# saveXML(con$value(), paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
# saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"SWQ.xml"))
# file.copy(from=paste0("D:/LAWA/2020/",agency,"SWQ.xml"),
          # to=paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))


