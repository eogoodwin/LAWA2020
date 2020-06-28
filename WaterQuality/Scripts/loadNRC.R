require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)



setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality")
agency='nrc'
tab='\t'
df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/",agency,"SWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
# configsites <- subset(df,df$Type=="Site")[,2]
# configsites <- as.vector(configsites)
Measurements <- subset(df,df$Type=="Measurement")[,2]

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", toupper(agency))

for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    url <- paste0("http://hilltop.nrc.govt.nz/SOERiverWQ.hts?service=Hilltop&request=GetData&agency=LAWA",
                 "&Site=",sites[i],
                 "&Measurement=",Measurements[j],
                 "&From=2005-01-01",
                 "&To=2020-01-01")
    url <- URLencode(url)
    xmlfile <- ldWQ(url,agency,extra='-g')
    if(!is.null(xmlfile)){
      cat("got",Measurements[j],'\t')
      xmltop<-xmlRoot(xmlfile)
      m<-xmltop[['Measurement']]
      # Create new node to replace existing <Data /> node in m
      DataNode <- newXMLNode("Data",attrs=c(DateFormat="Calendar",NumItems="2"))
      #work on this to make more efficient
      if(Measurements[j]=="WQ Sample"){
        ## Make new E node
        # Get Time values
        ans <- xpathApply(m,"//T",xmlValue)
        ans <- unlist(ans)
        #new bit here
        for(k in 1:length(ans)){
          # Adding the new "E" node
          addChildren(DataNode, newXMLNode(name = "E",parent = DataNode))
          # Adding the new "T" node
          addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "T",ans[k]))
          # Identifying and iterating through Parameter Elements of "m"
          p <- m[["Data"]][[k]] 
          # i'th E Element inside <Data></Data> tags
          c <- length(xmlSApply(p, xmlSize)) # number of children for i'th E Element inside <Data></Data> tags
          for(n in 2:c){   # Starting at '2' and '1' is the T element for time
            #added in if statement to pass over if any date&times with no metadata attached
            if(!is.null(p[[n]])){
              metaName  <- as.character(xmlToList(p[[n]])[1])   ## Getting the name attribute
              metaValue <- as.character(xmlToList(p[[n]])[2])   ## Getting the value attribute
              if(n==2){      # Starting at '2' and '1' is the T element for time
                item1 <- paste(metaName,metaValue,sep=tab)
              } else {
                item1 <- paste(item1,metaName,metaValue,sep=tab)
              }
            }
          }
          # Adding the Item1 node
          addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I1",item1))
        }
        
      } else {
        ## Make new E node
        # Get Time values
        ansTime <- lapply(c("T"),function(var) unlist(xpathApply(m,paste("//",var,sep=""),xmlValue)))
        ansTime <- unlist(ansTime)
        ansValue <- lapply(c("Value"),function(var) unlist(xpathApply(m,paste("//",var,sep=""),xmlValue)))
        ansValue <- unlist(ansValue)
        
        # loop through TVP nodes
        for(N in 1:xmlSize(m[['Data']])){  ## Number of Time series values
          # loop through all Children - T, Value, Parameters ..    
          addChildren(DataNode, newXMLNode(name = "E",parent = DataNode))
          addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "T",ansTime[N]))
          
          #Check for < or > or *
          ## Hand Greater than symbol
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
          
          if(xmlSize(m[['Data']][[N]])>2){    ### Has attributes to add to Item 2
            for(n in 3:xmlSize(m[['Data']][[N]])){      
              #Getting attributes and building string to put in Item 2
              attrs <- xmlAttrs(m[['Data']][[N]][[n]])  
              item2 <- paste(item2,attrs[1],tab,attrs[2],tab,sep="")
            }
          }
          addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I2",item2))
        } 
      }
      oldNode <- m[['Data']]
      newNode <- DataNode
      replaceNodes(oldNode, newNode)
      con$addNode(m) 
    }
  }
  cat('\n')
}

saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"SWQ.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"SWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"),
          overwrite=T)

