require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
require(RCurl)
source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
agency='arc'
# readxl::excel_sheets("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx")
ARCdeliveredSecchi=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Secchi")
ARCdeliveredPH=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "pH")
ARCdeliveredNH4=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Ammonical Nitrogen")
ARCdeliveredTN=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Total Nitrogen")
ARCdeliveredTP=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Total Phosphorus")
ARCdeliveredCHL=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Cholorophyll")
ARCdeliveredECOLI=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "E.Coli")
# ARCdeliveredCYANO=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Cyanoall")

ARCdeliveredSecchi$Measurement = "Secchi"
ARCdeliveredPH$Measurement = "pH"
ARCdeliveredNH4$Measurement = "NH4N"
ARCdeliveredTN$Measurement = "TN"
ARCdeliveredTP$Measurement = "TP"
ARCdeliveredCHL$Measurement = "CHLA"
ARCdeliveredECOLI$Measurement = "ECOLI"

ARCdelivered = do.call("rbind",list(ARCdeliveredCHL,  ARCdeliveredECOLI, #ARCdeliveredCYANO,
                                    ARCdeliveredNH4, ARCdeliveredPH, ARCdeliveredSecchi, ARCdeliveredTN, ARCdeliveredTP))
rm(list=ls(pattern="ARCdelivered.+"))
ARCdelivered <- ARCdelivered%>%select(-`Smaple Number`,-`Quality Code`,-SampleType,-SampleFrequency)
# head(ARCdelivered)

ARCtoSave <- ARCdelivered%>%transmute(CouncilSiteID=`Council Site id`,
                                      Date = format(lubridate::ymd(Date),'%d-%m-%Y'),
                                      Value = ReportLabValue,
                                      Method=Method,
                                      Measurement=Measurement,
                                      Censored=!is.na(`Detection Limit`),
                                      centype=ifelse(test = {!is.na(`Detection Limit`)&`Detection Limit`=="<"},
                                                     yes = "Left",no = "FALSE"))
write.csv(ARCtoSave,paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names=F)


cat("ARC delivered by csv, has been assimilated\n")

# http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?service=kisters&type=queryServices&request=getTimeseriesList&datasource=2&format=html&station_no=6301,6303,7605,45001,45003,45011,44616&returnfields=station_name,station_no,ts_id,ts_name,parametertype_name

# agency='arc'
# df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/MetaData/",agency,"LWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
# Measurements <- subset(df,df$Type=="Measurement")[,1]
# Measurements <- as.vector(Measurements)
# # configsites <- subset(df,df$Type=="Site")[,1]
# # configsites <- as.vector(configsites)
# siteTable=loadLatestSiteTableLakes(maxHistory = 30)
# sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
# 
# # sites=configsites
# setwd("H:/ericg/16666LAWA/LAWA2020/Lakes")
# 
# for(i in 1:length(sites)){
#   cat('\n',sites[i],i,'out of',length(sites),'\n')
#   for(j in 1:length(Measurements)){
#     url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=2&service=SOS&version=2.0&request=GetObservation&",
#                   "featureOfInterest=",sites[i],
#                   "&observedProperty=", Measurements[j],
#                   "&Procedure=raw&temporalfilter=om:phenomenonTime,2004-01-01/2020-01-01")
#     url <- URLencode(url)
#     xmlfile <- ldLWQ(url,agency = agency)
#     if(!is.null(xmlfile)){
#       #Create vector of times
#       time <- sapply(getNodeSet(doc=xmlfile, "//wml2:time"), xmlValue)
#       #Create vector of  values
#       value <- sapply(getNodeSet(doc=xmlfile, "//wml2:value"), xmlValue)
#       
#       if(length(time)!=0){
#         cat('got some',Measurements[j],'\t')
#         #Get QC metadata
#         # QC<-sapply(getNodeSet(xmlfile,"//wml2:qualifier"),function(el) xmlGetAttr(el, "xlink:title")) 
#         u<-unique(sapply(getNodeSet(xmlfile, path="//wml2:uom"),function(el) xmlGetAttr(el, "code")))
#         df <- data.frame(Site=sites[i],Measurement=Measurements[j],time=time,value=value,Units=u, stringsAsFactors = FALSE)
#         
#         if(!exists("Data")){
#           Data <- df
#         } else{
#           Data <- rbind.data.frame(Data, df)
#         }
#       }  
#     }
#   }
# }
# 
# #By this point, we have all the data downloaded from the council, in a data frame called Data.
# # write.csv(Data,file = paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LWQ.csv"),row.names = F)
# #The remainder here formats and saves XML. For why. Processes censoring, adds sample metadata from WQ Sample
# # 
# # 
# #----------------
# tm<-Sys.time()
# cat("Building XML\n")
# cat("Creating:",Sys.time()-tm,"\n")
# 
# con <- xmlOutputDOM("Hilltop")
# con$addTag("Agency", "AC")
# #saveXML(con$value(), file="out.xml")
# 
# #-------
# if(length(t)==0){
#   next
# } else{
#   max<-nrow(Data)
#   #max<-nrows(datatbl)
#   
#   i<-1
#   #for each site
#   while(i<=max){
#     s<-Data$Site[i]
#     # store first counter going into while loop to use later in writing out sample values
#     start<-i
#     
#     cat(i,Data$Site[i],"\n")   ### Monitoring progress as code runs
#     
#     while(Data$Site[i]==s){
#       #for each measurement
#       con$addTag("Measurement",  attrs=c(CouncilSiteID=Data$Site[i]), close=FALSE)
#       con$addTag("DataSource",  attrs=c(Name=Data$Measurement[i],NumItems="2"), close=FALSE)
#       con$addTag("TSType", "StdSeries")
#       con$addTag("DataType", "WQData")
#       con$addTag("Interpolation", "Discrete")
#       con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
#       con$addTag("ItemName", Data$Measurement[i])
#       con$addTag("ItemFormat", "F")
#       con$addTag("Divisor", "1")
#       con$addTag("Units", Data$Units[i])
#       con$addTag("Format", "#.###")
#       con$closeTag() # ItemInfo
#       con$closeTag() # DataSource
#       
#       # for the TVP and associated measurement water quality parameters
#       con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="2"),close=FALSE)
#       d<- Data$Measurement[i]
#       
#       cat("       - ",Data$Measurement[i],"\n")   ### Monitoring progress as code runs
#       
#       while(Data$Measurement[i]==d){
#         # for each tvp
#         con$addTag("E",close=FALSE)
#         con$addTag("T",Data$time[i])
#         con$addTag("I1", Data$value[i])
#         con$addTag("I2", paste("Units", Data$Units[i], sep="\t"))
#         
#         con$closeTag() # E
#         i<-i+1 # incrementing overall for loop counter
#         if(i>max){break}
#       }
#       # next
#       con$closeTag() # Data
#       con$closeTag() # Measurement
#       
#       if(i>max){break}
#       # Next 
#     }
#     # store last counter going out of while loop to use later in writing out sample values
#     end<-i-1
#     
#     # Adding WQ Sample Datasource to finish off this Site
#     # along with Sample parameters
#     con$addTag("Measurement",  attrs=c(CouncilSiteID=Data$Site[start]), close=FALSE)
#     con$addTag("DataSource",  attrs=c(Name="WQ Sample", NumItems="1"), close=FALSE)
#     con$addTag("TSType", "StdSeries")
#     con$addTag("DataType", "WQSample")
#     con$addTag("Interpolation", "Discrete")
#     con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
#     con$addTag("ItemName", "WQ Sample")
#     con$addTag("ItemFormat", "S")
#     con$addTag("Divisor", "1")
#     con$addTag("Units")
#     con$addTag("Format", "$$$")
#     con$closeTag() # ItemInfo
#     con$closeTag() # DataSource
#     
#     # for the TVP and associated measurement water quality parameters
#     con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="1"),close=FALSE)
#     # for each tvp
#     ## LOAD SAMPLE PARAMETERS
#     ## SampleID, ProjectName, SourceType, SamplingMethod and mowsecs
#     sample<-Data[start:end,3]
#     sample<-unique(sample)
#     sample<-sample[order(sample)]
#     ## THIS NEEDS SOME WORK.....
#     for(a in 1:length(sample)){ 
#       con$addTag("E",close=FALSE)
#       con$addTag("T",sample[a])
#       #put metadata in here when it arrives
#       # con$addTag("I2", paste("QC", QC, sep="\t"))
#       con$closeTag() # E
#     }
#     con$closeTag() # Data
#     con$closeTag() # Measurement    
#   }
# }
# 
# # saveXML(con$value(), file=paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LWQ.xml"))
# saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"LWQ.xml"))
# file.copy(from=paste0("D:/LAWA/2020/",agency,"LWQ.xml"),
#           to=paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LWQ.xml"),overwrite = T)
