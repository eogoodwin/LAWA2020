require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(lubridate)
source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')


setwd("H:/ericg/16666LAWA/LAWA2020/Lakes")
agency='orc'
df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Metadata/",agency,"LWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
# configsites <- subset(df,df$Type=="Site")[,1]
# configsites <- as.vector(configsites)
Measurements <- subset(df,df$Type=="Measurement")[,1]

siteTable=loadLatestSiteTableLakes(maxHistory=30)
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
lakeDataColumnLabels=NULL

#To get metadata into data frame
meta <- read.csv("H:/ericg/16666LAWA/LAWA2020/Lakes/MetaData/LawaLakesWater.csv",sep=",",stringsAsFactors=FALSE)

metaMeasurements <- unique(meta$Measurement)

## Manually matching order of measurement names in the measurement vector to the metaMeasurement vector
# metaMeasurements <- metaMeasurements[c(12,9,7,1,13,2)]
metaMeasurements <- metaMeasurements[c(1,13,12,7,2,9)]

meas <- cbind.data.frame(metaMeasurements,Measurements, stringsAsFactors=FALSE)
names(meas) <- c("Measurement","MeasurementName")
#join meas to meta
meta <- merge(meta,meas,by="Measurement",all = TRUE)

tab="\t"


con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", toupper(agency))

for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    url <- paste0("http://gisdata.orc.govt.nz/hilltop/WQGlobal.hts?service=Hilltop&request=GetData", #&agency=LAWA
                 "&Site=",sites[i],
                 "&Measurement=",Measurements[j],
                 "&From=2004-01-01",
                 "&To=2020-01-01")
    url <- URLencode(url)
    xmlfile <- ldLWQ(url,agency)
    if(!is.null(xmlfile)){
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
      xmltop<-xmlRoot(xmlfile)
      m<-xmltop[['Measurement']]
      # Create new node to replace existing <Data /> node in m
      DataNode <- newXMLNode("Data",attrs=c(DateFormat="Calendar",NumItems="2"))
      
      ## Make new E node
      # Get Time values
      ansTime <- lapply(c("T"),function(var) unlist(xpathApply(m,paste("//",var,sep=""),xmlValue)))
      ansTime <- unlist(ansTime)
      ansValue <- lapply(c("Value"),function(var) unlist(xpathApply(m,paste("//",var,sep=""),xmlValue)))
      ansValue <- unlist(ansValue)
      
      # subset data based on site and measurement and sort on date
      p <- subset(meta, meta$Site.Name==sites[i] & meta$MeasurementName==Measurements[j])
      
      
      if(dim(p)[1]>0){
        p$pDTz <- as.POSIXct(strptime(p$Date.Time,format = "%d/%m/%Y",tz="GMT"))
        p <- p[order(p$pDTz),]
        
        #Concatenate a column with all metadata parameters included and create vector
        p$I2 <- paste("LAWAID", p$LAWAID, "ORC", p$ORC, "Measurement", p$Measurement,
                      "ReportedLabValue", p$ReportedLabValue, "RawValue", p$RawValue, "Depthfrom", p$Depthfrom,
                      "Depthto", p$Depthto, "SampleLevel", p$Samplelevel,
                      "Method", p$Method, "DetectionLimit", p$DetectionLimit, "QualityCode",
                      p$QualityCode, "SampleFrequency", p$SampleFrequency, "LakeType",
                      p$Laketype, sep=tab)
        
        # remove unnecessary variables. Measurement, LAWAID, ORC, Site.Name, Date.Time and the last two pDTz and I2 (
        # all other columns contianed the info noiw in I2
        p<-p[,c(1:5,17:18)]
        
        # ## converting xml to dataframe in order to match datatimes for wq measurement parameters
        # mdata <- xmlToDataFrame(m[['Data']],stringsAsFactors=FALSE)
        # mdata$pDTz <- as.POSIXct(strptime(mdata$T,format = "%Y-%m-%dT%H:%M:%S",tz="GMT"))
        
        mdata=data.frame(T=ansTime,pDTz=as.numeric(ymd_hms(ansTime)),value=ansValue)
        
        mdata <- merge(mdata,p,by="pDTz",all=TRUE)
        mdata <- mdata[complete.cases(mdata$T),]
      }
      
      
      # loop through TVP nodes
      for(N in 1:xmlSize(m[['Data']])){  ## Number of Time series values
        # loop through all Children - T, Value, Parameters ..    
        addChildren(DataNode, newXMLNode(name = "E",parent = DataNode))
        addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "T",ansTime[N]))
        
        #Check for < or > or *
        ## Handle Greater than symbol
        if(grepl(pattern = "^\\>",x =  ansValue[N],perl = TRUE)){
          ansValue[N] <- substr(ansValue[N],2,nchar(ansValue[N]))
          item2 <- paste("$ND",tab,">",tab,sep="")
          
          # Handle Less than symbols  
        } else if(grepl(pattern = "^\\<",x =  ansValue[N],perl = TRUE)){
          ansValue[N] <- substr(ansValue[N],2,nchar(ansValue[N]))
          item2 <- paste("$ND",tab,"<",tab,sep="")
          
          # Handle Asterixes  
        } else if(grepl(pattern = "^\\*",x =  ansValue[N],perl = TRUE)){
          ansValue[N] <- gsub(pattern = "^\\*", replacement = "", x = ansValue[N])
          item2 <- paste("$ND",tab,"*",tab,sep="")
        } else{
          item2 <- ""
        }
        addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I1",ansValue[N]))
        
        ## Checking to see if other elements in <Data> ie. <Parameter> elements
        if(xmlSize(m[['Data']][[N]])>2){    ### Has attributes to add to Item 2
          for(n in 3:xmlSize(m[['Data']][[N]])){      
            #Getting attributes and building string to put in Item 2
            attrs <- xmlAttrs(m[['Data']][[N]][[n]])
            item2 <- paste(item2,attrs[1],tab,attrs[2],tab,sep="")
          }
        }
        addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I2",item2))
      } 
      oldNode <- m[['Data']]
      newNode <- DataNode
      replaceNodes(oldNode, newNode)
      con$addNode(m) 
    }
  }
}
# saveXML(con$value(), paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"LWQ.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"LWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LWQ.xml"))
if(length(lakeDataColumnLabels)>0)write.csv(row.names=F,lakeDataColumnLabels,paste0("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"LakeDataColumnLabels.csv"))
