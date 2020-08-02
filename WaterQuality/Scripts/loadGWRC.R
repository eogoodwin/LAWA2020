## Load libraries ------------------------------------------------
require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)

setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality")
agency='gwrc'
tab="\t"

Measurements <- read.table("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist
Measurements=c(Measurements,'WQ Sample')

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])


# Well the first yep is debatable, we have codes but not for many parameters, 
# and yes they’re easily retrievable, but not how these guys are suggesting, 
# you just add the highlighted bit to the URL for quality data:
#   
#   http://hilltop.gw.govt.nz/data.hts?Service=Hilltop&Request=GetData
#   &Site=Akatarawa%20River%20at%20Cemetery
#   &Measurement=Stage&From=30/10/2019&To=1/11/2020
#   &tstype=stdqualseries
# 
# Not sure how helpful that is?

# 'http://hilltop.gw.govt.nz/data.hts?Service=Hilltop&Request=GetData&Site=Akatarawa%20River%20at%20Cemetery&Measurement=Stage&From=30/10/2019&To=1/11/2020&tstype=stdqualseries'

con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", toupper(agency))

for(i in 1:length(sites)){
  cat(sites[i],i,'out of ',length(sites),'\n')
  for(j in 1:length(Measurements)){
    url <- paste0("http://hilltop.gw.govt.nz/Data.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",Measurements[j],
                  "&From=2005-01-01&To=2020-01-01")
    url <- URLencode(url)

    xmlback <- ldWQ(url,agency,method='wininet',QC=T) #adds &tstype=stdqualseries
    if(!is.null(xmlback)){
      if(is.list(xmlback)){
        dataxml=xmlback$dataxml
        qcxml=xmlback$qcxml
        # browser()
      }else{
        dataxml=xmlback
      }
      rm(xmlback)
      xmltop<-xmlRoot(dataxml)
      m<-xmltop[['Measurement']]
      # Create new node to replace existing <Data /> node in m
      DataNode <- newXMLNode("Data",attrs=c(DateFormat="Calendar",NumItems="2"))
      
      if(Measurements[j]=="WQ Sample"){
        ## Make new E node
        # Get Time values
        ans <- xpathSApply(m,"//T",xmlValue)
        for(k in 1:length(ans)){
          # Identifying and iterating through Parameter Elements of "m"
          p <- m[["Data"]][[k]] 
          # i'th E Element inside <Data></Data> tags
          c <- length(xmlSApply(p, xmlSize)) # number of children for i'th E Element inside <Data></Data> tags
          if(c>1){
            # Adding the new "E" node
            addChildren(DataNode, newXMLNode(name = "E",parent = DataNode))
            # Adding the new "T" node
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "T",ans[k]))
            for(n in 2:c){   # Starting at '2' and '1' is the T element for time
              #added in if statement to pass over if any date&times with no metadata attached
              metaName  <- as.character(xmlToList(p[[n]])[1])   ## Getting the name attribute
              metaValue <- as.character(xmlToList(p[[n]])[2])   ## Getting the value attribute
              if(n==2){      # Starting at '2' and '1' is the T element for time
                item1 <- paste(metaName,metaValue,sep="\t")     ## Concatenating all pairs separated by tabs. Ack.
              } else {
                item1 <- paste(item1,metaName,metaValue,sep="\t")
              }
            }
            # Adding the Item1 node
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I1",item1))
          }
        }
      } else if(Measurements[j]=="Turbidity (Lab)(X)" & sites[i]=="Horokiri Stream at Snodgrass"){
#Tihs special case was originally (until 7 July 2020) for measurement   Turbidity (Lab) [Turbidity (Lab)(X)] but that's not on te list even
        browser()
                ## Make new E node
        # Get Time values
        ansTime <- xpathSApply(m,"//Parameter[@Name='Lab']/../T",xmlValue)
        ansValue <- xpathSApply(m,"//Parameter[@Name='Lab']/../Value",xmlValue)
        # build temporary dataframe to hold result
        m_df <- data.frame(ansTime=c(ansTime),ansValue=c(ansValue), stringsAsFactors=FALSE)
        
        # Get Time and values for nodes with Quality Codes
        ansTimeQ <- xpathSApply(m,"//QualityCode/../T",xmlValue)
        qualValue <- xpathSApply(m,"//QualityCode",xmlValue)
        # build temporary dataframe to hold result
        q_df <- data.frame(ansTimeQ=c(ansTimeQ),qualValue=c(qualValue), stringsAsFactors=FALSE)
        
        # merge the two data frames by Time
        if(nrow(q_df)==0){
          x_df <- m_df
          x_df$qualValue <- 0 
        } else {
          x_df <- merge(m_df,q_df,by.x="ansTime", by.y = "ansTimeQ",all = TRUE)
        }
        # where there is no quality code, set value to 0
        x_df$qualValue[is.na(x_df$qualValue)] <- 0
        rm(ansTime, ansValue, ansTimeQ, qualValue,m_df,q_df)

        # loop through TVP nodes
        for(N in 1:nrow(x_df)){  ## Number of Time series values
          # Quality codes for GWRC data are not applied against all values. Therefore, only use values
          # that match the specification provided by Mark Heath at GWRC, i.e. Exclude values that are QC400
          if(x_df$qualValue[N]!=400){
            # loop through all Children - T, Value, Parameters ..    
            addChildren(DataNode, newXMLNode(name = "E",parent = DataNode))
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "T",x_df$ansTime[N]))
            
            #Check for < or > or *
            ## Handle Greater than symbol
            if(grepl(pattern = "^\\>",x =  x_df$ansValue[N],perl = TRUE)){
              x_df$ansValue[N] <- substr(x_df$ansValue[N],2,nchar(x_df$ansValue[N]))
              item2 <- paste("$ND",tab,">",tab,sep="")
              # Handle Less than symbols  
            }else if(grepl(pattern = "^\\<",x =  x_df$ansValue[N],perl = TRUE)){
              x_df$ansValue[N] <- substr(x_df$ansValue[N],2,nchar(x_df$ansValue[N]))
              item2 <- paste("$ND",tab,"<",tab,sep="")
              # Handle Asterisks  
            }else if(grepl(pattern = "^\\*",x =  x_df$ansValue[N],perl = TRUE)){
              x_df$ansValue[N] <- gsub(pattern = "^\\*", replacement = "", x = x_df$ansValue[N])
              item2 <- paste("$ND",tab,"*",tab,sep="")
            }else{
              item2 <- ""
            }
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I1",x_df$ansValue[N]))
            item2 <- paste(item2,"$QC",tab,x_df$qualValue[N],tab,sep="")
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I2",item2))
          }
        } 
      } else {
        ## Make new E node
        # Get Time values
        ansTime <- xpathSApply(m,"//T",xmlValue)
        ansValue <- xpathSApply(m,"//Value",xmlValue)
        # build temporary dataframe to hold result
        m_df <- data.frame(ansTime=c(ansTime),ansValue=c(ansValue), stringsAsFactors=FALSE)
        
        # Get Time and values for nodes with Quality Codes
        ansTimeQ <- xpathSApply(m,"//QualityCode/../T",xmlValue) 
        qualValue <- as.numeric(xpathSApply(m,"//QualityCode",xmlValue))
        # build temporary dataframe to hold result
        q_df <- data.frame(ansTimeQ=c(ansTimeQ),qualValue=c(qualValue), stringsAsFactors=FALSE)
        
        # merge the two data frames by Time
        if(nrow(q_df)==0){
          x_df <- m_df
          x_df$qualValue <- 0 
        } else {
          x_df <- merge(m_df,q_df,by.x="ansTime", by.y = "ansTimeQ",all = TRUE)
        }
        # where there is no quality code, set value to 0
        x_df$qualValue[is.na(x_df$qualValue)] <- 0
        rm(ansTime, ansValue, ansTimeQ, qualValue,m_df,q_df)

        # loop through TVP nodes
        for(N in 1:xmlSize(m[['Data']])){  ## Number of Time series values
          # Quality codes for GWRC data are not applied against all values. Therefore, only use values
          # that match the specification provided by Mark Heath at GWRC, i.e. Exclude values that are QC400
          if(x_df$qualValue[N]!=400){
            # loop through all Children - T, Value, Parameters ..    
            addChildren(DataNode, newXMLNode(name = "E",parent = DataNode))
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "T",x_df$ansTime[N]))
            
            #Check for < or > or *
            ## Handle Greater than symbol
            if(grepl(pattern = "^\\>",x =  x_df$ansValue[N],perl = TRUE)){
              x_df$ansValue[N] <- substr(x_df$ansValue[N],2,nchar(x_df$ansValue[N]))
              item2 <- paste("$ND",tab,">",tab,sep="")
              # Handle Less than symbols  
            }else if(grepl(pattern = "^\\<",x =  x_df$ansValue[N],perl = TRUE)){
              x_df$ansValue[N] <- substr(x_df$ansValue[N],2,nchar(x_df$ansValue[N]))
              item2 <- paste("$ND",tab,"<",tab,sep="")
              # Handle Asterisks  
            }else if(grepl(pattern = "^\\*",x =  x_df$ansValue[N],perl = TRUE)){
              x_df$ansValue[N] <- gsub(pattern = "^\\*", replacement = "", x = x_df$ansValue[N])
              item2 <- paste("$ND",tab,"*",tab,sep="")
            }else{
              item2 <- ""
            }
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I1",x_df$ansValue[N]))
            
            if(xmlSize(m[['Data']][[N]])>2){    ### Has attributes to add to Item 2
              for(n in 3:xmlSize(m[['Data']][[N]])){      
                #Getting attributes and building string to put in Item 2
                attrs <- xmlAttrs(m[['Data']][[N]][[n]])  
                item2 <- paste(item2,attrs[1],tab,attrs[2],tab,sep="")
              }
            }
            item2 <- paste(item2,"$QC",tab,x_df$qualValue[N],tab,sep="")
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I2",item2))
          }
        } 
      }
      oldNode <- m[['Data']]
      newNode <- DataNode
      replaceNodes(oldNode, newNode)
      con$addNode(m) 
    }
  }
}
# saveXML(con$value(), paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),'%Y-%m-%d'),"/",agency,"SWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2020/",agency,"SWQ.xml"))
file.copy(from=paste0("D:/LAWA/2020/",agency,"SWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))

