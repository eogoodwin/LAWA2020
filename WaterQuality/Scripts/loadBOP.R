## Load libraries ------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
require(RCurl)

agency='boprc'
setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality")

df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/",agency,"SWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
Measurements <- subset(df,df$Type=="Measurement")[,2]
Measurements <- as.vector(Measurements)

df <- read.csv("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/Transfers_plain_english_view.txt",sep=",",stringsAsFactors=FALSE)
df <- df%>%filter(Agency=='boprc')%>%select(CallName)

# configsites <- subset(df,df$Type=="Site")[,2]
# configsites <- as.vector(configsites)
#WFS  http://geospatial.boprc.govt.nz/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=getfeature&service=WFS&VERSION=1.1.0&typename=MonitoringSiteReferenceData&srsName=EPSG:4326
# http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?service=SOS&version=2.0.0&request=GetCapabilities

# check is this same list coming through from config now
# Measurements = c("Nitrite Nitrate _as N__LabResult",
#                  "Nitrate _N__LabResult",#"N _Tot__LabResult",
#                  "Ammoniacal N_LabResult",
#                  "DRP_LabResult",
#                  "P _Tot__LabResult",
#                  "Turbidity_Nephelom_LabResult",
#                  "pH_LabResult",
#                  "Water Clarity_LabResult",
#                  "E coli_LabResult")

# <ows:Value>Ammoniacal N_LabResult</ows:Value>
#   <ows:Value>DRP_LabResult</ows:Value>
#   <ows:Value>E coli QT_LabResult</ows:Value>
#   <ows:Value>E coli_LabResult</ows:Value>
#   <ows:Value>Nitrate _N__LabResult</ows:Value>
#   <ows:Value>Turbidity_ Nephelom_LabResult</ows:Value>
  
siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
# Qualifier    <- subset(df,df$Type=="Qualifier")[,2]
# Qualifier    <- as.data.frame(t(as.data.frame(strsplit(Qualifier,split="|",fixed=TRUE))),row.names=FALSE)
# colnames(Qualifier) <- c("Value","Description")


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

#Censor the data there that came in.
# Datasource or Parameter Type	Measurement or Timeseries Name	Units	AQUARIUS Parameter	Detection Limit
# Total Oxidised Nitrogen	Total Oxidised Nitrogen	g/m3	Nitrite Nitrate _as N__LabResult	0.001
# Total Nitrogen	Total Nitrogen	g/m3	N _Tot__LabResult	0.01
# Ammoniacal Nitrogen	Ammoniacal Nitrogen	g/m3	Ammoniacal N_LabResult	0.002
# Dissolved Reactive Phosphorus	DRP	g/m3	DRP_LabResult	0.001
# Total Phosphorus	TP	g/m3	P _Tot__LabResult	0.001
# Turbidity	Turbidity	NTU	Turbidity, Nephelom_LabResult	0.1
# pH	pH	pH units	pH_LabResult	0.2
# Visual Clarity	BDISC	m	Water Clarity_LabResult	0.01
# Escherichia coli	Ecoli	/100 ml	E coli_LabResult	1



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
      # con$addTag("TSType", "StdSeries")
      # con$addTag("DataType", "WQData")
      # con$addTag("Interpolation", "Discrete")
      con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
      con$addTag("ItemName", Data$Measurement[i])
      # con$addTag("ItemFormat", "F")
      # con$addTag("Divisor", "1")
      con$addTag("Units", Data$Units[i])
      # con$addTag("Format", "#.###")
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
    
    # # Adding WQ Sample Datasource to finish off this Site
    # # along with Sample parameters
    # con$addTag("Measurement",  attrs=c(CouncilSiteID=Data$Site[start]), close=FALSE)
    # con$addTag("DataSource",  attrs=c(Name="WQ Sample", NumItems="1"), close=FALSE)
    # con$addTag("TSType", "StdSeries")
    # con$addTag("DataType", "WQSample")
    # con$addTag("Interpolation", "Discrete")
    # con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
    # con$addTag("ItemName", "WQ Sample")
    # con$addTag("ItemFormat", "S")
    # con$addTag("Divisor", "1")
    # con$addTag("Units")
    # con$addTag("Format", "$$$")
    # con$closeTag() # ItemInfo
    # con$closeTag() # DataSource
    # 
    # # for the TVP and associated measurement water quality parameters
    # con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="1"),close=FALSE)
    # # for each tvp
    # ## LOAD SAMPLE PARAMETERS
    # ## SampleID, ProjectName, SourceType, SamplingMethod and mowsecs
    # sample<-Data[start:end,3]
    # sample<-unique(sample)
    # sample<-sample[order(sample)]
    # ## THIS NEEDS SOME WORK.....
    # for(a in 1:length(sample)){
    #   con$addTag("E",close=FALSE)
    #   con$addTag("T",sample[a])
    #   #put metadata in here when it arrives
    #   # con$addTag("I2", paste("QC", QC, sep="\t"))
    #   con$closeTag() # E
    # }
    # con$closeTag() # Data
    # con$closeTag() # Measurement
  }

# saveXML(con$value(), paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"SWQ.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"SWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))

