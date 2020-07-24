require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
require(RCurl)
source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')


agency='ac'
# df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/MetaData/",agency,"LWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
# Measurements <- subset(df,df$Type=="Measurement")[,1]
Measurements <- unique(readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Metadata/ARC200626_LakeWQ LAWA KiQS Calls_v2.xlsx",col_types = c(rep('skip',4),'text',rep('skip',3)))%>%as.data.frame)
Measurements <- as.vector(Measurements[,1])
siteTable=loadLatestSiteTableLakes(maxHistory = 30)
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])


setwd("H:/ericg/16666LAWA/LAWA2020/Lakes")
if(exists('Data'))rm(Data)
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=3&service=Kisters&type=queryServices&request=getWqmSampleValues",
                  "&format=csv",
                  "&station_no=",sites[i],
                  "&parametertype_name=",Measurements[j],
                  "&period=P25Y&returnfields=station_no,station_name,timestamp,sample_depth,parametertype_name,value_sign,value,unit_name,value_quality")
    url <- URLencode(url)
    url <- gsub(pattern = '\\+',replacement = '%2B',x = url)
    dl=try(download.file(url,destfile="D:/LAWA/2020/tmpLac.csv",method='curl',quiet=T),silent = T)
    
    csvfile <- read.csv("D:/LAWA/2020/tmpLac.csv",stringsAsFactors = F,sep=';')
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

Data <- Data%>%transmute(CouncilSiteID = station_no,
                         Date = format(as_date(timestamp,tz='Pacific/Auckland'),format='%d-%b-%y'),
                         Value = value,
                         Method='',
                         Measurement = parametertype_name,
                         Censored = grepl('<|>',value_sign),
                         centype = as.character(factor(value_sign,levels = c('<','---','>'),labels=c('Left','F','Right'))),
                         QC = value_quality)

Data$Measurement[Data$Measurement == 'Transpar.-secchi (m)'] <- "Secchi"
Data$Measurement[Data$Measurement == 'Chloro a (mg/l)'] <- "CHLA"
Data$Measurement[Data$Measurement == 'E. coli (CFU/100ml)'] <- "ECOLI"
Data$Measurement[Data$Measurement == 'Tot P (mg/l)'] <- "TP"
Data$Measurement[Data$Measurement == 'NH3+NH4 as N (mg/l)'] <- "NH4N"
Data$Measurement[Data$Measurement == 'Tot N (mg/l)'] <- "TN" 
Data$Measurement[Data$Measurement == 'pH (pH units)'] <- "pH"



# By this point, we have all the data downloaded from the council, in a data frame called Data.
write.csv(Data,file = paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)



if(0){
  agency='ac'
  # readxl::excel_sheets("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx")
  ACdeliveredSecchi=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Secchi")
  ACdeliveredPH=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "pH")
  ACdeliveredNH4=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Ammonical Nitrogen")
  ACdeliveredTN=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Total Nitrogen")
  ACdeliveredTP=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Total Phosphorus")
  ACdeliveredCHL=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Cholorophyll")
  ACdeliveredECOLI=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "E.Coli")
  # ACdeliveredCYANO=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/ARCLakes Data - measurement data LAWA 2019.xlsx",sheet = "Cyanoall")
  
  ACdeliveredSecchi$Measurement = "Secchi"
  ACdeliveredPH$Measurement = "pH"
  ACdeliveredNH4$Measurement = "NH4N"
  ACdeliveredTN$Measurement = "TN"
  ACdeliveredTP$Measurement = "TP"
  ACdeliveredCHL$Measurement = "CHLA"
  ACdeliveredECOLI$Measurement = "ECOLI"
  
  ACdelivered = do.call("rbind",list(ACdeliveredCHL,  ACdeliveredECOLI, #ACdeliveredCYANO,
                                      ACdeliveredNH4, ACdeliveredPH, ACdeliveredSecchi, ACdeliveredTN, ACdeliveredTP))
  rm(list=ls(pattern="ACdelivered.+"))
  ACdelivered <- ACdelivered%>%select(-`Smaple Number`,-`Quality Code`,-SampleType,-SampleFrequency)
  # head(ACdelivered)
  
  ARCtoSave <- ACdelivered%>%transmute(CouncilSiteID=`Council Site id`,
                                        Date = format(lubridate::ymd(Date),'%d-%m-%Y'),
                                        Value = ReportLabValue,
                                        Method=Method,
                                        Measurement=Measurement,
                                        Censored=!is.na(`Detection Limit`),
                                        centype=ifelse(test = {!is.na(`Detection Limit`)&`Detection Limit`=="<"},
                                                       yes = "Left",no = "FALSE"))
  write.csv(ARCtoSave,paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names=F)
  
  cat("AC delivered by csv, has been assimilated\n")
}



# The remainder here formats and saves XML. For why. Processes censoring, adds sample metadata from WQ Sample


# ----------------
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
#           

