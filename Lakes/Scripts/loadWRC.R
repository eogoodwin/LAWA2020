require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')


setwd("H:/ericg/16666LAWA/LAWA2020/Lakes")
agency='wrc'

df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Metadata/",agency,"LWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
# configsites <- subset(df,df$Type=="Site")[,1]
# configsites <- as.vector(configsites)
Measurements <- subset(df,df$Type=="Measurement")[,1]

siteTable=loadLatestSiteTableLakes(maxHistory=30)
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
lakeDataColumnLabels=NULL

setwd("H:/ericg/16666LAWA/LAWA2020/Lakes")
if(exists('Data'))rm(Data)

#7July2020 tried using the ARC format to get QC codes, but this appears to be a different installation of Kisters.
# http://envdata.waikatoregion.govt.nz:8080/KiWIS/KiWIS?datasource=0&service=Kisters&type=queryServices&request=getTimeseriesList&station_id=37270
#Got some way toward naviating the database
#Kisters Kiwis help http://kiwis.kisters.de/KiWIS/KiWIS?datasource=0&service=kisters&type=queryServices&request=getrequestinfo 

for(i in 1:length(sites)){
  cat('\n',i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    
    url <- paste0("http://envdata.waikatoregion.govt.nz:8080/KiWIS/KiWIS?datasource=0&service=SOS&agency=LAWA&",
                  "version=2.0&request=GetObservation&procedure=LWQ.Sample.Results.P",
                  "&featureOfInterest=",sites[i],
                  "&observedProperty=",Measurements[j],
					"&temporalfilter=om:phenomenonTime,2004-01-01/2020-01-01")

    url <- URLencode(url)
    xmlfile <- ldLWQ(url,agency)

    
   if(xpathApply(xmlRoot(xmlfile),path="count(//wml2:point)",xmlValue)==0){
     next
   }
    cat(Measurements[j],'\t')
    # browser()
      datAsList = XML::xmlToList(xmlfile)
      newDataColumnLabels = sort(unique(unlist(invisible(
        sapply(
          datAsList$Measurement$Data,
          FUN = function(y) {
            sapply(y,
              FUN = function(x) {
                if ('Name' %in% names(x)) {
                  x[['Name']]
                }
              }
            )
          }
        )
      ))))
      lakeDataColumnLabels=sort(unique(c(lakeDataColumnLabels,newDataColumnLabels)))
      rm(datAsList,newDataColumnLabels)
    #STEP 1: Load wml2:DefaultTVPMetadata to a vector
    xattrs_qualifier         <- xpathSApply(xmlfile, "//wml2:DefaultTVPMeasurementMetadata/wml2:qualifier/@xlink:title")
    if(is.null(xattrs_qualifier)){
      xattrs_qualifier <- ""
    }
    xattrs_uom               <- xpathSApply(xmlfile, "//wml2:DefaultTVPMeasurementMetadata/wml2:uom/@code")
    xattrs_interpolationType <- xpathSApply(xmlfile, "//wml2:DefaultTVPMeasurementMetadata/wml2:interpolationType/@xlink:title")
    xattrs_default           <- c(xattrs_qualifier,xattrs_uom,xattrs_interpolationType)
    names(xattrs_default)    <- c("qualifier","uom","interpolationType")
    rm(xattrs_qualifier,xattrs_uom,xattrs_interpolationType)
    
    #STEP 2: Get wml2:MeasurementTVP metadata values
    xattrs_qualifier         <- xpathSApply(xmlfile, "//wml2:TVPMeasurementMetadata/wml2:qualifier/@xlink:title")
    #If xattrs_qualifier is empty, it means there are no additional qualifiers in this timeseries.
    #Test for Null, and create an empty dataframe as a consequence 
    if(is.null(xattrs_qualifier)){
      df_xattrs <- data.frame(time=character(),qualifier=character())
    } else{
      xattrs_time              <- sapply(getNodeSet(doc=xmlfile, "//wml2:TVPMeasurementMetadata/wml2:qualifier/@xlink:title/../../../../wml2:time"), xmlValue)
      #Store measurementTVPs in dataframe to join back into data after it is all retrieved
      df_xattrs <- as.data.frame(xattrs_time,stringsAsFactors = FALSE)
      names(df_xattrs) <- c("time")
      df_xattrs$qualifier <- xattrs_qualifier
      rm(xattrs_time,xattrs_qualifier)
    }    
    
    #Step3
    
    #Create vector of times
    time <- sapply(getNodeSet(doc=xmlfile, "//wml2:time"), xmlValue)
    #Create vector of  values
    value <- sapply(getNodeSet(doc=xmlfile, "//wml2:value"), xmlValue)
    
    df <- data.frame(time=time,value=value, stringsAsFactors = FALSE)
    rm(time, value)
    
    
    df$Site <- sites[i]
    df$Measurement <- Measurements[j]
    df$Units <- xattrs_default[2]  ## xattrs_default vector contains (qualifier_default, unit, interpolationtype)
    df <- df[,c(3,4,1,2,5)]
    
    # merge in additional qualifiers, if present, from df_xattrs
    if(nrow(df_xattrs)!=0) {
      df <- merge(df,df_xattrs,by="time",all=TRUE)
      df$qualifier[is.na(df$qualifier)] <- xattrs_default[1]
    } else {
      df$qualifier<-xattrs_default[1]
    }
    
    # Remove default metadata attributes for current timeseries
    rm(xattrs_default, df_xattrs)
    
    
    if(!exists("Data")){
      Data <- df
    } else{
      Data <- rbind.data.frame(Data, df)
    }
    
    # Remove current timeseries data frame
    rm(df, xmlfile)
    
    
  }
}


con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", "WRC")


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
    con$addTag("Measurement",  attrs=c(SiteName=Data$Site[i]), close=FALSE)  #Until 21/6/19 this was CouncilSiteID=Data$Site, which yeah it should be, but, consistency with other agencies
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
    if(!is.na(Data$qualifier[i])){    # this will need to be expanded to deal with range of qualifiers
      if(grepl("8202",Data$qualifier[i])){                   ## GREATER THAN VALUES
        con$addTag("E",close=FALSE)
        con$addTag("T",Data$time[i])
        con$addTag("I1", paste0(">",Data$value[i]))
        con$addTag("I2", "$ND\t>\t")
        con$closeTag() # E
      } else if(grepl("16394",Data$qualifier[i])){           ## LESS THAN VALUES
        con$addTag("E",close=FALSE)
        con$addTag("T",Data$time[i])
        con$addTag("I1", paste0("<",Data$value[i]))
        con$addTag("I2", "$ND\t<\t")
        con$closeTag() # E 
      } else {                                               ## UNCENSORED VALUES
        con$addTag("E",close=FALSE)
        con$addTag("T",Data$time[i])
        con$addTag("I1", Data$value[i])
        con$addTag("I2", "\t")
        con$closeTag() # E
      }
      # Write all other result values  
    } else {                                                 ## UNCENSORED VALUES
      con$addTag("E",close=FALSE)
      con$addTag("T",Data$time[i])
      con$addTag("I1", Data$value[i])
      con$addTag("I2", "\t")
      con$closeTag() # E
      
    }
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
con$addTag("Measurement",  attrs=c(SiteName=Data$Site[start]), close=FALSE)#Until 21/6/19 this was CouncilSiteID=Data$Site, which yeah it should be, but, consistency with other agencies
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
  con$addTag("I1", "")
  con$closeTag() # E
}

con$closeTag() # Data
con$closeTag() # Measurement    

}
# saveXML(con$value(), file = paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/MetaData/",format(Sys.Date(),'%Y-%m-%d'),"/",agency,"LWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"LWQ.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"LWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LWQ.xml"))
if(length(lakeDataColumnLabels)>0)write.csv(row.names=F,lakeDataColumnLabels,paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LakeDataColumnLabels.csv"))
