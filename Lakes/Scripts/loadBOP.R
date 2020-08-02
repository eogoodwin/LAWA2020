## Load libraries ------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
require(RCurl)



agency='boprc'
setwd("H:/ericg/16666LAWA/LAWA2020/Lakes")

df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/MetaData/",agency,"LWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
Measurements <- subset(df,df$Type=="Measurement")[,1]
# sites <- subset(df,df$Type=="Site")[,1]

#WFS  http://geospatial.boprc.govt.nz/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=getfeature&service=WFS&VERSION=1.1.0&typename=MonitoringSiteReferenceData&srsName=EPSG:4326
# http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?service=SOS&version=2.0.0&request=GetCapabilities

 lakeSiteTable=loadLatestSiteTableLakes()
 sites = unique(lakeSiteTable$CouncilSiteID[lakeSiteTable$Agency==agency])
lakeDataColumnLabels=NULL

suppressWarnings(rm(Data))
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\t')
  for(j in 1:length(Measurements)){
    url <- paste0("http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?service=SOS&version=2.0.0&request=GetObservation&",
                  "observedProperty=",Measurements[j],
                  "&featureOfInterest=",sites[i],
                  "&temporalfilter=om:phenomenonTime,P15Y/2020-01-01")
    url <- URLencode(url)
    xmlfile <- ldLWQ(url,agency=agency)
    if(!is.null(xmlfile) && 
       grepl(pattern = sites[i],x = xmlValue(xmlRoot(xmlfile)),ignore.case = T) &&
       !grepl(pattern = 'invalid',x = xmlValue(xmlRoot(xmlfile)),ignore.case = T)){
      # browser()
      datAsList = XML::xmlToList(xmlfile)
      newDataColumnLabels = sort(unique(unlist(invisible(
        sapply(
          datAsList$Measurement$Data,
          FUN = function(y) {
            sapply(
              y,
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
#XML date format ymd e.g. 2015-11-17

#Delivered separately by Lisa Naysmith, email 2/8/twentynineteen
okaro=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/BOPRCLake Okaro 2004_2018.xlsx",sheet=1,na = "<NA>")
okaro=okaro%>%transmute(Site="FI680541_INT",
                        Measurement=parameter,
                        time=sdate,
                        Uvalue=`Uncencored value`,
                        value=Value,
                        Units='ns')%>%as.data.frame
okaro$time=as.character(format.Date(okaro$time,"%Y-%m-%dT00:00:00.000Z"))
okaro$value[!is.na(okaro$`Uncencored value`)]=okaro$`Uncencored value`[!is.na(okaro$`Uncencored value`)]
okaro <- okaro%>%select(-Uvalue)%>%filter(Measurement%in%c("CHLA","ECOLI","NH4","pH","Secchi","TN","TP"))

okaro$value[okaro$Measurement%in%c("NH4","TN","TP")&
              !is.na(as.numeric(okaro$Value))]=okaro$value[okaro$Measurement%in%c("NH4","TN","TP")&
                                                             !is.na(as.numeric(okaro$Value))]/1000
if(any(grepl(okaro$Site[1],siteTable$CouncilSiteID,ignore.case = T))){
  Data=rbind(Data,okaro)
}
rm(okaro)



#Lisa Naysmith points out we can get historic lake data from last year's pull.  Cunning.
bop2018=read.csv(tail(dir(path = 'h:/ericg/16666LAWA/2018/Lakes/1.Imported/',
                          pattern='LakesWithMetadata.csv',
                          recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)%>%
  filter(agency=='boprc')
bop2018$SiteID=toupper(siteTable$CouncilSiteID[match(tolower(bop2018$LawaSiteID),tolower(siteTable$LawaSiteID))])
bop2018 <- bop2018%>%
  drop_na(SiteID)%>%
  transmute(Site=toupper(SiteID),
            Measurement=parameter,
            time=Date,
            value=Value,
            Units='ns')%>%as.data.frame
bop2018$time=format.Date(lubridate::dmy(bop2018$time),"%Y-%m-%dT00:00:00.000Z")

Data=unique(rbind(Data,bop2018))
rm(bop2018)



Data$Measurement[which(Data$Measurement=="TN")] <- "N _Tot__LabResult"
Data$Measurement[which(Data$Measurement=="TP")] <- "P _Tot__LabResult"
Data$Measurement[which(Data$Measurement=="CHLA")] <- "Chloro a_LabResult"
Data$Measurement[which(Data$Measurement=="pH")] <- "pH_LabResult"
Data$Measurement[which(Data$Measurement%in%c("NH4","NH4N"))] <- "Ammoniacal N_LabResult"
Data$Measurement[which(Data$Measurement=="Secchi")] <- "Water Clarity_LabResult"

Data=Data%>%arrange(Site,Measurement)

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

# saveXML(con$value(), paste0("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"LWQ.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"LWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LWQ.xml"),
          overwrite=T)
if(length(lakeDataColumnLabels)>0)write.csv(row.names=F,lakeDataColumnLabels,paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LakeDataColumnLabels.csv"))
