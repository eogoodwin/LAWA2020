## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
# require(RCurl)
setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality")


agency='arc'
df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/",agency,"SWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
Measurements <- subset(df,df$Type=="Measurement")[,2]
# Measurements <- as.vector(Measurements)
# configsites <- subset(df,df$Type=="Site")[,2]
# configsites <- as.vector(configsites)

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
# sites=configsites

#http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=3
#&Procedure=Sample.Results.LAWA
#&Service=SOS&version=2.0.0
#&request=GetObservation
#&observedProperty=NH3+NH4 as N (mg/l)
#&featureOfInterest=6604
#&temporalfilter=om:phenomenonTime,P25Y/2020-06-30

# http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=3
# &Procedure=Sample.Results.LAWA
# &Service=SOS&version=2.0.0
# &request=GetObservation
# &observedProperty=NH3%2BNH4%20as%20N%20(mg/l)
# &featureOfInterest=6604
# &temporalfilter=om:phenomenonTime,P25Y/2020-06-30

suppressWarnings(rm(Data))
for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?",
                  "datasource=3",
                  "&Procedure=Sample.Results.LAWA",#raw
                  "&service=SOS",
                  "&version=2.0.0",
                  "&request=GetObservation",
                  "&featureOfInterest=",sites[i],
                  "&observedProperty=", URLencode(Measurements[j],reserved=T),
                  "&temporalfilter=om:phenomenonTime,2005-01-01/2020-01-01")
    url <- URLencode(url)
    xmlfile <- ldWQ(url,agency=agency)
    if(!is.null(xmlfile)){
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
  cat('\n')
}

#By this point, we have all the data downloaded from the council, in a data frame called Data.
#The remainder here formats and saves XML. For why. Processes censoring, adds sample metadata from WQ Sample

con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", agency)

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
    cat("       - ",Data$Measurement[i],"\n")   ### Monitoring progress as code runs
    con$addTag("Measurement",  attrs=c(CouncilSiteID=Data$Site[i]), close=FALSE)
    con$addTag("DataSource",  attrs=c(Name=Data$Measurement[i],NumItems="2"), close=FALSE)
    con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
    con$addTag("ItemName", Data$Measurement[i])
    con$addTag("Units", Data$Units[i])
    con$closeTag() # ItemInfo
    con$closeTag() # DataSource
    
    # for the TVP and associated measurement water quality parameters
    con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="2"),close=FALSE)
    d<- Data$Measurement[i]

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
    con$closeTag() # Data
    con$closeTag() # Measurement
    if(i>max){break}
  }
}

# saveXML(con$value(),paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"SWQ.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"SWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
