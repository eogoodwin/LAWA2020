require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)

setwd("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/")

tab="\t"

agency='wcrc'

df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Metadata/",agency,"Macro_config.csv"),sep=",",stringsAsFactors=FALSE)
#  configsites <- subset(df,df$Type=="Site")[,1]
#  configsites <- as.vector(configsites)
Measurements <- subset(df,df$Type=="Measurement")[,1]
siteTable=loadLatestSiteTableMacro()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])




con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", agency)


for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    
    url <- paste0("http://hilltop.wcrc.govt.nz/wq.hts?service=Hilltop&request=GetData",
                 "&Site=",sites[i],
                 "&Measurement=",Measurements[j],
                 "&From=1990-01-01",
                 "&To=2020-06-01")
      url <- URLencode(url)
      xmlfile <- ldMWQ(url,agency)
    
    if(!is.null(xmlfile)){
      xmltop<-xmlRoot(xmlfile)
      
      m<-xmltop[['Measurement']]
      
      
      # Create new node to replace existing <Data /> node in m
      DataNode <- newXMLNode("Data",attrs=c(DateFormat="Calendar",NumItems="2"))
      
      
      #cat(saveXML(DataNode),"\n")
      tab="\t"
      
      
      ## Make new E node
      # Get Time values
      ansTime <- lapply(c("T"),function(var) unlist(xpathApply(m,paste("//",var,sep=""),xmlValue)))
      ansTime <- unlist(ansTime)
      ansValue <- lapply(c("I1"),function(var) unlist(xpathApply(m,paste("//",var,sep=""),xmlValue)))
      ansValue <- unlist(ansValue)
      
      
      ## converting xml to dataframe in order to match datatimes for wq measurement parameters
      mdata <- xmlToDataFrame(m[['Data']],stringsAsFactors=FALSE)
      mdata$pDTz <- as.POSIXct(strptime(mdata$T,format = "%Y-%m-%dT%H:%M:%S",tz="GMT"))
      
      # if(!is.null(p$I2)){
      #   mdata$I2 <- p$I2
      # }
      mdata <- mdata[complete.cases(mdata$T),]
      
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
        
        ## Manually adding supplied parameters
        if(nchar(item2)==0){
          item2 <- mdata$I2[N]
        }else{
          item2 <- paste(item2,tab,mdata$I2[N],tab,sep="")
        }
        
        ## Writing I2 node
        addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I2",item2))
        
      } 
      
      oldNode <- m[['Data']]
      newNode <- DataNode
      replaceNodes(oldNode, newNode)
      
      con$addNode(m) 
      cat("Completed measurement",Measurements[j],"for",sites[i],"\n\n")
    }
  }
}
#saveXML(con$value(), file=paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"Macro.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"Macro.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"))


x <- readLines(paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"))
y <- gsub( "NumItems=\"1\"", "NumItems=\"2\"", x, ignore.case = TRUE  )
y <- gsub( "SimpleTimeSeries", "WQData", y, ignore.case = TRUE  )

writeLines(y,paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"Macro.xml"))






