require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)



setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality")
agency='wrc'
df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/",agency,"SWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
# configsites <- subset(df,df$Type=="Site")[,2]
# configsites <- as.vector(configsites)
Measurements <- subset(df,df$Type=="Measurement")[,2]
# Measurements <- as.vector(Measurements)
# Qualifier    <- subset(df,df$Type=="Qualifier")[,2]
# Qualifier    <- as.data.frame(t(as.data.frame(strsplit(Qualifier,split="|",fixed=TRUE))),row.names=FALSE)
# colnames(Qualifier) <- c("Value","Description")

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency=='wrc'])


cat("SiteID\tMeasurementName\tSitesPct\tMeasuresPct\tRERIMP\tWARIMP\n")
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    # Querying procedure=RERIMP.Sample.Results
    # Measurements[j]=gsub(pattern = '\\&',replacement = '%26',x = Measurements[j])
    # Measurements[j]=gsub(pattern = ' ',replacement = '%20',x = Measurements[j])
    url <- paste0("http://envdata.waikatoregion.govt.nz:8080/KiWIS/KiWIS?datasource=0&service=SOS&agency=LAWA&",
				  "version=2.0&request=GetObservation&procedure=RERIMP.Sample.Results.P",
                  "&featureOfInterest=",sites[i],
                  "&observedProperty=", Measurements[j],
                  "&temporalfilter=om:phenomenonTime,2005-01-01/2020-01-01")
    url <- URLencode(url)
    xmlfile <- ldWQ(url,agency)
    
    ## Test returns from URL call
    #  Establish if <om:result> exists, and if so, whether, there is at least 1 <wml2:point>
    countwml2Points<-xpathApply(xmlRoot(xmlfile),path="count(//wml2:point)",xmlValue)
    RERIMP<-countwml2Points
    WARIMP<-0
    
    #  If first URL returns no data, use procedure=WARIMP.Sample.Results
    if(countwml2Points==0){
      url <- paste0("http://envdata.waikatoregion.govt.nz:8080/KiWIS/KiWIS?datasource=0&service=SOS&agency=LAWA&",
					"version=2.0&request=GetObservation&procedure=WARIMP.Sample.Results.P",
                   "&featureOfInterest=",sites[i],
                   "&observedProperty=", Measurements[j],
                   "&temporalfilter=om:phenomenonTime,2005-01-01/2020-01-01")
      url <- URLencode(url)
      xmlfile <- ldWQ(url,agency)

      ## Test returns from URL call
      #  Establish if <om:result> exists, and if so, whether, there is at least 1 <wml2:point>
      countwml2Points<-xpathApply(xmlRoot(xmlfile),path="count(//wml2:point)",xmlValue)
      WARIMP<-countwml2Points
    }
    rm(countwml2Points)  # no longer required.
    
    # cat(sites[i],"\t",Measurements[j],"\t",percent(i/length(sites)),"\t",percent(j/length(Measurements)),"\t",RERIMP, "\t" ,WARIMP ,"\n")
    
    ## At this point, both counts might be zero. If so, go to next for loop
    if(RERIMP==0 & WARIMP==0){
      rm(RERIMP,WARIMP)  # Clears out these variables before next loop
      next
    }
    rm(RERIMP,WARIMP) # variables no longer needed.
    #For Waikato data
    #Default metadata for data qualifiers (such as "<" and ">") are set at the beginning of the time series, and appears to
    #be based on the first value in the timeseries time-value pairs. We need to be aware of what the default qualifier
    #value is, in order to correctly tag values inthe rest of the timeseries.
    
    #The first step is to parse the DefaultTVPMetadata element and store those values
    #The second step is to determine if a <wml2:metadata> element exists with a <wml2:qualifier> child. If it does, 
    #then need to parse that child element and extract the qualifier in order to present the timeseries value
    #properly.
    #Thirdly, retrieve all the data and apply the qualifiers
    
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
qualifiers_added <- unique(Data$qualifier)

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
    
    while(Data$Measurement[i]==d & Data$Site[i]==s){
      # for each tvp
      # Handle Greater than symbols
      
      # Handle Less than symbols  
      if(!is.na(Data$qualifier[i])){    # this will need to be expanded to deal with range of qualifiers
        if(grepl("8202",Data$qualifier[i])){                   ## GREATER THAN VALUES
          con$addTag("E",close=FALSE)
          con$addTag("T",Data$time[i])
          con$addTag("I1", Data$value[i])
          con$addTag("I2", "$ND\t>\t")
          con$closeTag() # E
        } else if(grepl("16394",Data$qualifier[i])){           ## LESS THAN VALUES
          con$addTag("E",close=FALSE)
          con$addTag("T",Data$time[i])
          con$addTag("I1", Data$value[i])
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
# saveXML(con$value(), file = paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/",format(Sys.Date(),'%Y-%m-%d'),"/",agency,"SWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"SWQ.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"SWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
