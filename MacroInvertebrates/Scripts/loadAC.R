## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
require(RCurl)
setwd("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/")


agency='ac'


# df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Metadata/",agency,"Macro_config.csv"),sep=",",stringsAsFactors=FALSE)
# Measurements <- subset(df,df$Type=="Measurement")[,1]
# Measurements <- as.vector(Measurements)
# 
# siteTable=loadLatestSiteTableMacro()

# acmacros = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/ARC_LAWA_MCI_2020.xlsx",sheet='Sheet2',skip=1)
acmacros = readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/20200625_LAWA_MCI_AC edit2.xlsx",sheet='Sheet2',skip=3)
                             # 190703_LAWA_MCI_2020.xlsx",sheet='Sheet2',skip=1)
acMacros <- acmacros%>%
  dplyr::select(LAWASiteID,SiteID,Date,"Selected MCI score","Taxonomic Richness","% EPT Richness","QualityCode")%>%
  dplyr::rename(LawaSiteID=LAWASiteID,
         CouncilSiteID=SiteID,
         MCI=`Selected MCI score`,
         `Total Richness`=`Taxonomic Richness`,
         QC=`QualityCode`)%>%
    tidyr::gather(key="Measurement",value="Value",c("MCI","Total Richness","% EPT Richness"))
acMacros$Agency='ac'
acMacros$Date=format(acMacros$Date,'%d-%b-%y')
write.csv(acMacros,
          file=paste0( 'H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),'/',agency,'.csv'),
          row.names=F)

 cat("AC MACROS DELIVERED BY SPREADSHEET")
# 
# sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
# 
# suppressWarnings(rm(Data))
# for(i in 1:length(sites)){
#   cat(sites[i],i,'out of',length(sites),'\n')
#   for(j in 1:length(Measurements)){
#     
#     url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=2&service=SOS&version=2.0&request=GetObservation",
#     "&featureOfInterest=",sites[i],
#     "&observedProperty=", Measurements[j],
#     "&Procedure=raw&temporalfilter=om:phenomenonTime,2005-01-01/2020-01-01")
#     url <- URLencode(url)
#     xmlfile <- ldMWQ(url,agency=agency)
# 
#     #Create vector of times
#     time <- sapply(getNodeSet(doc=xmlfile, "//wml2:time"), xmlValue)
#     #Create vector of  values
#     value <- sapply(getNodeSet(doc=xmlfile, "//wml2:value"), xmlValue)
#     
#     
#     if(length(time!=0)){
#       browser()
#       #Get QC metadata
#       xPath <-"//wml2:qualifier"
#       c<-getNodeSet(xmlfile,path=xPath)
#       QC<-sapply(c,function(el) xmlGetAttr(el, "xlink:title")) 
#       
#       #Create dataframe holding both
#       dataIn <- as.data.frame(time, stringsAsFactors = FALSE)
#       dataIn$value <- value
#       
#       
#       #Create vector of units
#       myPath<-"//wml2:uom"
#       c<-getNodeSet(xmlfile, path=myPath)
#       u<-sapply(c,function(el) xmlGetAttr(el, "code"))
#       u <-unique(u)
#       
#       dataIn$Site <- sites[i]
#       dataIn$Measurement <- Measurements[j]
#       dataIn$Units <- u
#       
#       dataIn <- dataIn[,c(3,4,1,2,5)]
#       
#       
#       
#       if(!exists("Data")){
#         Data <- dataIn
#       } else{
#         Data <- rbind.data.frame(Data, dataIn)
#       }
#       
#     }  
#     
#   }
# }
# 
# 
# 
# #p <- sapply(getNodeSet(doc=xmlfilec ,path="//sos:ObservationOffering/swes:name"), xmlValue)
# 
# #procedure <- c("RERIMP.Sample.Results.P", "WARIMP.Sample.Results.P")
# 
# 
# 
# con <- xmlOutputDOM("Hilltop")
# con$addTag("Agency", "AC")
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
#       con$addTag("I2", paste("QC", QC, sep="\t"))
#       con$closeTag() # E
#     }
#     
#     con$closeTag() # Data
#     con$closeTag() # Measurement    
#     
#   }
# }
# #saveXML(con$value(), file=paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"))
# 
# saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"Macro.xml"))
# file.copy(from=paste0("D:/LAWA/2020/",agency,"Macro.xml"),
#           to=paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"))
