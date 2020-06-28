## SET LOCAL WORKING DIRECTORY
od<-getwd()
setwd("//file/herman/R/OA/08/02/2018/Water Quality/R/Lakes")


## Load libraries ------------------------------------------------
require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)

## Local variables.
f <- c("ac","nrc","wrc","bop","gdc","hbrc","hrc","trc","gwrc","mdc","ncc","tdc","ecan","wcrc",",orc","es")

# XML2CSV
# Converting council data to csv, from XML
# Steps
# 1. Build dataframe from XML file
# 2. Write csv using specified delimiter

#Print #fnum, "Site|Measurement|DateTime|SDate|Value|FWENZID|LAWA ID|Lake Name|Depth From|Depth To"

# Building rest of CSV
# Print #fnum, cat.sitename & "|" & 
              #cat.DefaultMeasurement & "|" & 
              #wq.Time & "|" & 
              #Format(wq.Time, "dd-mmm-yyyy hh:mm") & "|" & 
              #wq.Value & "|" & 
              #wq.Param("FWENZID") & "|" & 
              #wq.Param("LAWAID") & "|" & 
              #wq.Param("Lake Name") & "|" & 
              #wq.Param("Depth From") & "|" & 
              #wq.Param("Depth To")
# i = i + 1
# Debug.Print i




for(i in 1:length(f)){
  fn <- paste(f[i],"LWQ.xml",sep="")   # fn = file name
  if(file.exists(fn)){
    if(!exists("d")){
      xmlfile <- xmlParse(file = fn)
      # Create vector of sitenames
      # cat.SiteName
      sites<-unique(sapply(getNodeSet(xmlfile, path="//Measurement/@SiteName"), as.character))
      if(length(sites)!=0){
        for(j in 1:length(sites)){
          ds <- sapply(getNodeSet(xmlfile, paste("//Measurement[@SiteName='",sites[j],"']/DataSource/@Name",sep="")), as.character)
          ms <- sapply(getNodeSet(xmlfile, paste("//Measurement[@SiteName='",sites[j],"']/DataSource/ItemInfo[@ItemNumber='1']/ItemName",sep="")), xmlValue)
          if(length(ms)!=length(ds)) ms<-ds
          # Drop WQ Sample
          ds <- ds[!ds %in% "WQ Sample"]
          ms <- ms[!ms %in% "WQ Sample"]
          # cat.DefaultMeasurement
          
          for(k in 1:length(ds)){
            #while wq.getNext
            
            Time <- sapply(getNodeSet(xmlfile, paste("//Measurement[@SiteName='",sites[j],"']/DataSource[@Name='",ds[k],"']/../Data/E/T",sep="")), xmlValue)
            Value <- sapply(getNodeSet(xmlfile, paste("//Measurement[@SiteName='",sites[j],"']/DataSource[@Name='",ds[k],"']/../Data/E/I1",sep="")), xmlValue)
            
            if(k==1){
              a <- data.frame(f[i],sites[j],ms[k],Time,Value, stringsAsFactors=FALSE)
              names(a) <- c("Council","Site","Measurement","Time","Value")
            } else {
              b <- data.frame(f[i], sites[j],ms[k],Time,Value, stringsAsFactors=FALSE)
              names(b) <- c("Council","Site","Measurement","Time","Value")
              a <- rbind.data.frame(a,b,stringsAsFactors=FALSE)
            }
          }
          
          if(!exists("d")){
            d <- a
          } else {
            d <- rbind.data.frame(d,a,stringsAsFactors=FALSE)
          }         
          
        }
        write.csv(d,file = paste(f[i],"LWQ-test.txt",sep=""))
        
        if(!exists("e")){
          e <- d
        } else {
          e <- rbind.data.frame(e,d)
        }
        rm(d)
      }
    }
    
  }
  
  
}

save(e,file="rbindLWQfiles.Rdata")


  