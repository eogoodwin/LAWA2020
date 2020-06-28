## Load libraries ------------------------------------------------
require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality/")
source("H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R")

agency='niwa'
tab="\t"

df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/",agency,"SWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
sites=df$Value[which(df$Type=='site')]

# lawaset=c("NH4", "TURB", 
#           "BDISC",  "DRP",
#           "ECOLI",  "TN",
#             "TP",  "TON",  "PH")
# Measurements <- subset(df,df$Type=="Measurement")[,2]
Measurements = c("NH4-N (Dis)","Turbidity (Nephelom)",
                 "Visual Water Clarity","DRP-P",
                 "E Coli","N (Tot)",
                 "P (Tot)","NO3+NO2 as N","pH")
siteTable=loadLatestSiteTableRiver()
# sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])




con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", toupper(agency))
rm(Data)
for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  
  
  for(j in 1:length(Measurements)){
    url <- paste0("https://hydro-sos.niwa.co.nz/?service=SOS&version=2.0.0&request=GetObservation",
                  "&featureOfInterest=",sites[i],
                  "&ObservedProperty=",Measurements[j],
                  "&TemporalFilter=om:phenomenonTime,2005-01-01/2020-01-01")
    url <- URLencode(url)
    
    xmlfile <- ldWQ(url,agency)
    if(!is.null(xmlfile)){
      xmltop<-xmlRoot(xmlfile)
      
      time <- sapply(getNodeSet(doc=xmlfile, "//wml2:time",namespaces=c(wml2="http://www.opengis.net/waterml/2.0")), xmlValue)          #Create vector of  values
      value <- sapply(getNodeSet(doc=xmlfile, "//wml2:value",namespaces=c(wml2="http://www.opengis.net/waterml/2.0")), xmlValue)

      if(length(time)!=0){
        df <- data.frame(time=time,value=value, stringsAsFactors = FALSE)
        
        #Create vector of units
        c<-getNodeSet(xmlfile, path="//wml2:uom",namespaces=c(wml2="http://www.opengis.net/waterml/2.0"))
        u<-unique(sapply(c,function(el) xmlGetAttr(el, "code")))

        df$Site <- sites[i]
        df$Measurement <- Measurements[j]
        if(length(u)>0){
          df$Units <- u
        }else{
          df$Units <- rep("",length(df$Site))
        }
        df <- df[,c(3,4,1,2,5)]
        if(!exists("Data")){
          Data <- df
        } else{
          Data <- rbind.data.frame(Data, df)
        }
      }  
    }
  }
}



con <- xmlOutputDOM("SOS")
con$addTag("Agency", "NIWA")

max<-nrow(Data)

i<-1
#for each site
while(i<=max){
  s<-Data$Site[i]
  # store first counter going into while loop to use later in writing out sample values
  start<-i
  
  cat(i,Data$Site[i],"\n")   ### Monitoring progress as code runs
  
  while(Data$Site[i]==s){
    #for each measurement
    #cat(datatbl$CouncilSiteID[i],"\n")
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
    while(Data$Measurement[i]==d & Data$Site[i]==s){
      # Handle Less than symbols
      if(grepl(pattern = "^\\>",Data$value[i],perl=T)){                   ## GREATER THAN VALUES
        con$addTag("E",close=FALSE)
        con$addTag("T",Data$time[i])
        con$addTag("I1", substr(Data$value[i],2,nchar(Data$value[i])))
        con$addTag("I2", "$ND\t>\t")
        con$closeTag() # E
      } else if(grepl(pattern = "^\\<",Data$value[i],perl=T)){           ## LESS THAN VALUES
        con$addTag("E",close=FALSE)
        con$addTag("T",Data$time[i])
        con$addTag("I1", substr(Data$value[i],2,nchar(Data$value[i])))
        con$addTag("I2", "$ND\t<\t")
        con$closeTag() # E
      } else {                                               ## UNCENSORED VALUES
        con$addTag("E",close=FALSE)
        con$addTag("T",Data$time[i])
        con$addTag("I1", Data$value[i])
        con$addTag("I2", "\t")
        con$closeTag() # E
      }
      i<-i+1 # incrementing overall for loop counter
      if(i>max){break}
    }
    con$closeTag() # Data
    con$closeTag() # Measurement
    if(i>max){break}
  }
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
    con$addTag("I1", "")
    con$closeTag() # E
  }
  con$closeTag() # Data
  con$closeTag() # Measurement
}
# saveXML(con$value(), paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"SWQ.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"SWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
