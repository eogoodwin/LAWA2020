require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)

setwd("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/")
agency='hrc'


df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Metadata/",agency,"Macro_config.csv"),sep=",",stringsAsFactors=FALSE)
# configsites <- subset(df,df$Type=="Site")[,2]
# configsites <- as.vector(configsites)
Measurements <- subset(df,df$Type=="Measurement")[,1]
siteTable=loadLatestSiteTableMacro()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])


suppressWarnings(rm(Data))
for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    url <- paste0("http://tsdata.horizons.govt.nz/boo.hts?service=SOS&agency=LAWA&request=GetObservation",
                  "&FeatureOfInterest=",sites[i],
                  "&ObservedProperty=",Measurements[j],
                  "&TemporalFilter=om:phenomenonTime,2005-01-01,2020-06-01")
    # url <- gsub(" ", "%20", url)
    url <- URLencode(url)
    
    xmlfile <- ldMWQ(url,agency)
    
    if(!is.null(xmlfile)&&!grepl(pattern = "No data|^501",x = xmlValue(xmlRoot(xmlfile)),ignore.case = T)){
      anstime <- sapply(getNodeSet(doc=xmlfile, "//wml2:time"), xmlValue)          #Create vector of  values
      ansvalue <- sapply(getNodeSet(doc=xmlfile, "//wml2:value",namespaces=c(wml2="http://www.opengis.net/waterml/2.0")), xmlValue)
      df=data.frame(time=anstime,value=ansvalue)
      rm(anstime,ansvalue)
      if(length(time)!=0){
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
  }
}


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
  con$addTag("Measurement",  attrs=c(SiteName=Data$Site[start]), close=FALSE)
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


#saveXML(con$value(), file=paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"Macro.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"Macro.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"),
          overwrite = T)
