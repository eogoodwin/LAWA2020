## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
# require(RCurl)
setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality")


agency='ac'
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist
Measurements=c(Measurements,'WQ Sample')

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

# http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=3&service=kisters&type=queryServices&request=getrequestinfo 
# http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?service=kisters&type=queryServices&request=getParameterList&datasource=0&format=html&station_no=1043837
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

setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality")
if(exists('Data'))rm(Data)


for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=3&service=Kisters&type=queryServices&request=getWqmSampleValues&format=csv",
                  "&station_no=",sites[i],
                  "&parametertype_name=",Measurements[j],
                  "&period=P25Y&returnfields=station_no,station_name,timestamp,sample_depth,parametertype_name,value_sign,value,unit_name,value_quality")
    url <- URLencode(url)
    url <- gsub(pattern = '\\+',replacement = '%2B',x = url)
    dl=try(download.file(url,destfile="D:/LAWA/2020/tmpWQac.csv",method='curl',quiet=T),silent = T)
    csvfile <- read.csv("D:/LAWA/2020/tmpWQac.csv",stringsAsFactors = F,sep=';')
    if(dim(csvfile)[1]>0){
      cat('got some',Measurements[j],'\t')
      if(!exists("Data")){
        Data <- csvfile
      } else{
        Data <- rbind.data.frame(Data, csvfile)
      }
    }
  }
}
write.csv(Data,file = paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"RawWQ.csv"),row.names = F)

  
  Data <- Data%>%transmute(CouncilSiteID = station_no,
                           Date = format(as_date(timestamp,tz='Pacific/Auckland'),'%d-%b-%y'),
                           Value = value,
                           # Method='',
                           Measurement = parametertype_name,
                           Units = unit_name,
                           Censored = grepl('<|>',value_sign),
                           centype = as.character(factor(value_sign,levels = c('<','---','>'),labels=c('Left','F','Right'))),
                           QC = value_quality)
  lawaset=c("NH4", "TURB", "BDISC",  "DRP",  "ECOLI",  "TN",  "TP",  "TON",  "PH")
  
  Data$Measurement[Data$Measurement == 'NH3+NH4 as N (mg/l)'] <- "NH4"
  Data$Measurement[Data$Measurement == 'Turb'] <- "TURB"
  Data$Measurement[Data$Measurement == 'Black Disk (m)'] <- "BDISC"
  Data$Measurement[Data$Measurement == 'Dis Rx P (mg/l)'] <- "DRP"
  Data$Measurement[Data$Measurement == 'E. coli (CFU/100ml)'] <- "ECOLI"
  Data$Measurement[Data$Measurement == 'Tot N (mg/l)'] <- "TN" 
  Data$Measurement[Data$Measurement == 'Tot P (mg/l)'] <- "TP"
  Data$Measurement[Data$Measurement == 'NO3+NO2 (mg/l)'] <- "TON"
  Data$Measurement[Data$Measurement == 'pH (pH units)'] <- "PH"
  
  Data=merge(Data,siteTable,by='CouncilSiteID')  
  
  # c("PH", "TP", "TN", "NH4", "TON", "BDISC", "TURB", "ECOLI", "DRP")  

  write.csv(Data,file = paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)

# Data=read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),stringsAsFactors = F)

if(0){
suppressWarnings(rm(Data))
for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?",
                  "datasource=3",
                  "&Procedure=Sample.Results.LAWA",
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
}