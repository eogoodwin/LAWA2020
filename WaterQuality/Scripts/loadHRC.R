require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
# require(RCurl)

setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality")
agency='hrc'

df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/",agency,"SWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
# configsites <- subset(df,df$Type=="Site")[,2]
# configsites <- as.vector(configsites)
Measurements <- subset(df,df$Type=="Measurement")[,2]
siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
suppressWarnings(rm(Data))

for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')

  for(j in 1:length(Measurements)){
    url <- paste0("http://tsdata.horizons.govt.nz/boo.hts?service=SOS&agency=LAWA&request=GetObservation",
                 "&FeatureOfInterest=",sites[i],
                 "&ObservedProperty=",Measurements[j],
                 "&TemporalFilter=om:phenomenonTime,2005-01-01,2020-01-01")
    url <- URLencode(url)

    xmlfile <- ldWQ(url,agency)
    if(!is.null(xmlfile)&&!grepl(pattern = "No data|^501",x = xmlValue(xmlRoot(xmlfile)),ignore.case = T)){
      #Create vector of times
      time <- sapply(getNodeSet(doc=xmlfile, "//wml2:time"), xmlValue)          #Create vector of  values
      value <- sapply(getNodeSet(doc=xmlfile, "//wml2:value",namespaces=c(wml2="http://www.opengis.net/waterml/2.0")), xmlValue)
      if(length(time)!=0){
        #Get QC metadata
        xPath <-"//wml2:qualifier"
        c<-getNodeSet(xmlfile,path=xPath)
        QC<-sapply(c,function(el) xmlGetAttr(el, "xlink:title")) 
        #Create dataframe holding both
        df <- as.data.frame(time, stringsAsFactors = FALSE)
        df$value <- value
        #Create vector of units
        myPath<-"//wml2:uom"
        c<-getNodeSet(xmlfile, path=myPath)
        u<-sapply(c,function(el) xmlGetAttr(el, "code"))
        u <-unique(u)
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
  } #j
} #i

# write.csv(Data,paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),file="/",agency,"SWQ.csv"),row.names = F)
# save(Data, file=paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),file="/hrcout.rData"))


con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", "HRC")




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