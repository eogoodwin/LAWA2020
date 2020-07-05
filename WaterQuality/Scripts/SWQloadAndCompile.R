rm(list=ls())
library(parallel)
library(doParallel)
source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')

try(dir.create(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/", format(Sys.Date(),"%Y-%m-%d"))))

siteTable=loadLatestSiteTableRiver()
rownames(siteTable)=NULL



#Comment these out as councils indicate their data is done.
scriptsToRun = c("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadAC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadBOP.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadECAN.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadES.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadGDC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadGWRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadHBRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadHRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadMDC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadNCC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadNRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadORC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadTDC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadTRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadWCRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadWRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Scripts/loadNIWA.R")

workers <- makeCluster(7)
registerDoParallel(workers)
 clusterCall(workers,function(){
   source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
 })
foreach(i = 1:length(scriptsToRun),.errorhandling = "stop")%dopar%{
  source(scriptsToRun[i])
  return(NULL)
}
stopCluster(workers)
rm(workers)
cat("Done load\n")

#Check all the measurement names are in the transfers table
transfers=read.table("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/transfers_plain_english_view.txt",
                     sep=',',header = T,stringsAsFactors = F)
# transfers$CallName[which(transfers$CallName=="Clarity (Black Disc Field)"&transfers$Agency=='es')] <- "Clarity (Black Disc, Field)"
for(agency in c("arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")){
  df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/",agency,"SWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
  Measurements <- subset(df,df$Type=="Measurement")[,2]
  if(any(!Measurements%in%transfers$CallName[transfers$Agency==agency])){
    cat(agency,'\t',Measurements[!Measurements%in%transfers$CallName[transfers$Agency==agency]])
    print('\n')
  }
}


##############################################################################
#                                 XML to CSV
##############################################################################

agencies=c("arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
lawaset=c("NH4", "TURB", "BDISC",  "DRP",  "ECOLI",  "TN",  "TP",  "TON",  "PH")
workersB <- makeCluster(7)
registerDoParallel(workersB)
clusterCall(workersB,function(){
  library(tidyverse)
  source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
  agencies=c("arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
  lawaset=c("NH4", "TURB", "BDISC",  "DRP",  "ECOLI",  "TN",  "TP",  "TON",  "PH")
  transfers=read.table("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/transfers_plain_english_view.txt",
                       sep=',',header = T,stringsAsFactors = F)
  transfers$CallName[which(transfers$CallName=="Clarity (Black Disc Field)"&transfers$Agency=='es')] <- "Clarity (Black Disc, Field)"
})
# for(i in 1:length(agencies)){
foreach(i = 1:length(agencies),.errorhandling="stop")%dopar%{
  xml2csvRiver(agency=agencies[i],quiet=T,reportCensor=F,reportVars=F,ageCheck=T,maxHistory=20)
  return(NULL)
  }
stopCluster(workersB)
rm(workersB)
##############################################################################
#                                 *****
##############################################################################
cat("Done to CSV")





#19 June 2020
#Check agency and region count per file
 for(agency in c("arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")){
    checkCSVageRiver(agency = agency)
 }
   #   mfl=loadLatestCSVRiver(agency,maxHistory = 3,silent=T)
#    cat(names(mfl),'\n')
#   # print(knitr::kable(table(tolower(mfl$SiteName)==tolower(mfl$CouncilSiteID))))
# }

# 
# #This could checks whether previous downloads pulled datas missing from current download.
# for (agency in c("arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")){
  # agencyFiles = data.frame(name=dir(path = "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
  #                   pattern = paste0('^',agency,'.csv'),
  #                   full.names = T,recursive = T,ignore.case = T),
  #                   nCol = 0,nRow=0,nsite=0,stringsAsFactors = F)
  # for(af in seq_along(agencyFiles$name)){
  #   agencyCSV = read.csv(agencyFiles[af,1],stringsAsFactors=F)
  #   agencyFiles$nCol[af]=dim(agencyCSV)[2]
  #   agencyFiles$nRow[af]=dim(agencyCSV)[1]
  #   agencyFiles$nsite[af]=length(unique(agencyCSV$CouncilSiteID))
  # }





##############################################################################
#                          COMBINE CSVs to COMBO
##############################################################################
#Build the combo ####
  siteTable=loadLatestSiteTableRiver()
  lawaset=c("NH4", "TURB", "BDISC",  "DRP",  "ECOLI",  "TN",  "TP",  "TON",  "PH")
  rownames(siteTable)=NULL
  suppressWarnings(rm(wqdata,"arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc"))
  agencies= c("arc","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
library(parallel)
library(doParallel)
workers=makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
})
startTime=Sys.time()
foreach(agency =1:length(agencies),.combine = rbind,.errorhandling = 'stop')%dopar%{
  mfl=loadLatestCSVRiver(agencies[agency],maxHistory = 30)
  if('CouncilSiteID.1'%in%names(mfl)){
    mfl <- mfl%>%select(-CouncilSiteID.1)
  }
  if(!is.null(mfl)&&dim(mfl)[1]>0){
    if(sum(!tolower(mfl$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID))>0){
      cat('\t',sum(!unique(tolower(mfl$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID)),'CouncilSiteIDs not in site table\n')
      cat('\t',sum(!unique(tolower(mfl$LawaSiteID))%in%tolower(siteTable$LawaSiteID)),'LawaSiteIDs not in site table\n')
    }
    if(!agency%in%c(3)){
    targetSites=tolower(siteTable$CouncilSiteID[siteTable$Agency==agencies[agency]])
    targetCombos=apply(expand.grid(targetSites,tolower(lawaset)),MARGIN = 1,FUN=paste,collapse='')
    currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
    missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]

    if(length(missingCombos)>0){
      agencyFiles = rev(dir(path = "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",
                                        pattern = paste0('^',agencies[agency],'.csv'),
                            full.names = T,recursive = T,ignore.case = T))[-1]
      for(af in seq_along(agencyFiles)){
        agencyCSV = read.csv(agencyFiles[af],stringsAsFactors=F)
        if("Measurement"%in%names(agencyCSV)){
          agencyCSVsiteMeasCombo=paste0(tolower(agencyCSV$CouncilSiteID),tolower(agencyCSV$Measurement))
          if(any(missingCombos%in%unique(agencyCSVsiteMeasCombo))){
            these=agencyCSVsiteMeasCombo%in%missingCombos
            agencyCSV=agencyCSV[these,]
            agencyCSV$Altitude=mfl$Altitude[match(agencyCSV$CouncilSiteID,mfl$CouncilSiteID)]
            agencyCSV$AltitudeCl=mfl$AltitudeCl[match(agencyCSV$CouncilSiteID,mfl$CouncilSiteID)]
            if('SWQLandcover'%in%names(agencyCSV)){
              agencyCSV <- agencyCSV%>%select(-SWQLandcover)
              agencyCSV$SWQLanduse=mfl$SWQLanduse[match(agencyCSV$CouncilSiteID,mfl$CouncilSiteID)]
              agencyCSV$Landcover=mfl$Landcover[match(agencyCSV$CouncilSiteID,mfl$CouncilSiteID)]
            }
            if(!"CouncilSiteID.1"%in%names(agencyCSV)){
              agencyCSV$CouncilSiteID.1 = mfl$CouncilSiteID.1[match(tolower(agencyCSV$CouncilSiteID),tolower(mfl$CouncilSiteID))]
            }
            agencyCSV$LawaSiteID=tolower(agencyCSV$LawaSiteID)
            cat(agency,dim(agencyCSV)[1],'\n')
            mfl=rbind(mfl,agencyCSV[,names(mfl)])
            rm(agencyCSV,these)
            currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
            missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
            if(length(missingCombos)==0){
              break
            }            
          }
        }
        suppressWarnings(rm(agencyCSV))
      }
    }
    rm(missingCombos,targetCombos,currentSiteMeasCombos,targetSites)
    }
    if(agencies[agency]=='niwa'){
      mfl$Value[mfl$Measurement%in%c("DRP","NH4","TN","TP")]=mfl$Value[mfl$Measurement%in%c("DRP","NH4","TN","TP")]/1000
    }
    if(agencies[agency]=='boprc'){
      
      #Censor the data there that came in.
      # Datasource or Parameter Type	Measurement or Timeseries Name	Units	AQUARIUS Parameter	              Detection Limit
      # Total Oxidised Nitrogen	      Total Oxidised Nitrogen	        g/m3	Nitrite Nitrate _as N__LabResult	0.001
      # Total Nitrogen	              Total Nitrogen	                g/m3	N _Tot__LabResult	                0.01
      # Ammoniacal Nitrogen	          Ammoniacal Nitrogen	            g/m3	Ammoniacal N_LabResult	          0.002
      # Dissolved Reactive Phosphorus	DRP	                            g/m3	DRP_LabResult	                    0.001
      # Total Phosphorus	            TP	                            g/m3	P _Tot__LabResult	                0.001
      # Turbidity	                    Turbidity	                      NTU	  Turbidity, Nephelom_LabResult	    0.1
      # pH	                          pH  	                      pH units	pH_LabResult	                    0.2
      # Visual Clarity	              BDISC	                            m	  Water Clarity_LabResult	          0.01
      # Escherichia coli	            Ecoli	                      /100 ml	  E coli_LabResult	                1
      cenTON = which(mfl$Measurement=="TON"&mfl$Value<0.001)
      cenTN =  which(mfl$Measurement=="TN"&mfl$Value<0.01)
      cenNH4 = which(mfl$Measurement=="NH4"&mfl$Value<0.002)
      cenDRP = which(mfl$Measurement=="DRP"&mfl$Value<0.001)
      cenTP =  which(mfl$Measurement=="TP"&mfl$Value<0.001)
      cenTURB =which(mfl$Measurement=="TURB"&mfl$Value<0.1)
      cenPH =  which(mfl$Measurement=="PH"&mfl$Value<0.2)
      cenCLAR =which(mfl$Measurement=="BDISC"&mfl$Value<0.01)
      cenECOLI=which(mfl$Measurement=="ECOLI"&mfl$Value<1)
      if(length(cenTON)>0){
        mfl$Censored[cenTON] <- TRUE
        mfl$CenType[cenTON] <- "Left"
        mfl$Value[cenTON] <- 0.001
      }
      if(length(cenTN)>0){
        mfl$Censored[cenTN] <- TRUE
        mfl$CenType[cenTN] <- "Left"
        mfl$Value[cenTN] <- 0.01
      }
      if(length(cenNH4)>0){
        mfl$Censored[cenNH4] <- TRUE
        mfl$CenType[cenNH4] <- "Left"
        mfl$Value[cenNH4] <- 0.002
      }
      if(length(cenDRP)>0){
        mfl$Censored[cenDRP] <- TRUE
        mfl$CenType[cenDRP] <- "Left"
        mfl$Value[cenDRP] <- 0.001
      }
      if(length(cenTP)>0){
        mfl$Censored[cenTP] <- TRUE
        mfl$CenType[cenTP] <- "Left"
        mfl$Value[cenTP] <- 0.001
      }
      if(length(cenTURB)>0){
        mfl$Censored[cenTURB] <- TRUE
        mfl$CenType[cenTURB] <- "Left"
        mfl$Value[cenTURB] <- 0.1
      }
      if(length(cenPH)>0){
        mfl$Censored[cenPH] <- TRUE
        mfl$CenType[cenPH] <- "Left"
        mfl$Value[cenTON] <- 0.2
      }
      if(length(cenCLAR)>0){
        mfl$Censored[cenCLAR] <- TRUE
        mfl$CenType[cenCLAR] <- "Left"
        mfl$Value[cenCLAR] <- 0.01
      }
      if(length(cenECOLI)>0){
        mfl$Censored[cenECOLI] <- TRUE
        mfl$CenType[cenECOLI] <- "Left"
        mfl$Value[cenECOLI] <- 1
      }
      rm(cenTON,cenTN,cenNH4,cenDRP,cenTP,cenTURB,cenPH,cenCLAR,cenECOLI)
    }
    
  }
  return(mfl)
}->wqdata   #23/6/2020    797484                                          990644 without backfill  1062994 with. 1055269 without ECAN backfill
            #23/6/2020pm  856506
            #25/6/2020    999530
            #3/7/2020    1005143
stopCluster(workers)
rm(workers)

Sys.time()-startTime  #6.7 s

table(wqdata$Agency)
table(wqdata$SWQLanduse,wqdata$SWQAltitude)

wqdata$SWQLanduse[which(is.na(wqdata$SWQLanduse))] <- siteTable$SWQLanduse[match(wqdata$LawaSiteID[which(is.na(wqdata$SWQLanduse))],
                                                                                 siteTable$LawaSiteID)]
wqdata$SWQAltitude[which(!is.na(as.numeric(wqdata$SWQAltitude)))] <- siteTable$SWQAltitude[match(wqdata$LawaSiteID[which(!is.na(as.numeric(wqdata$SWQAltitude)))],
                                                                                                 siteTable$LawaSiteID)]
wqdata$SWQAltitude[which(tolower(wqdata$SWQAltitude)=='unstated')] <- siteTable$SWQAltitude[match(wqdata$LawaSiteID[which(tolower(wqdata$SWQAltitude)=='unstated')],
                                                                                                 siteTable$LawaSiteID)]

wqdata$SWQLanduse=pseudo.titlecase(wqdata$SWQLanduse)
wqdata$SWQAltitude=pseudo.titlecase(wqdata$SWQAltitude)

table(wqdata$SWQLanduse,wqdata$SWQAltitude)

wqdata=unique(wqdata)  #1004961

wqdata$SiteID=trimws(wqdata$SiteID)
wqdata$CouncilSiteID=trimws(wqdata$CouncilSiteID)
wqdata$LawaSiteID=trimws(wqdata$LawaSiteID)
# wqdata$SWQAltitude=tolower(wqdata$SWQAltitude)
wqdata$Altitude=tolower(wqdata$Altitude)
# wqdata$SWQLanduse=tolower(wqdata$SWQLanduse)
wqdata$Landcover=tolower(wqdata$Landcover)
wqdata$Region=tolower(wqdata$Region)
wqdata$Agency=tolower(wqdata$Agency)

wqdata$CenType[wqdata$CenType%in%c("L","Left")] <- "Left"
wqdata$CenType[wqdata$CenType%in%c("R","Right")] <- "Right"

#855695 July8 2019
#932548 July17 2019
#947210 July22 2019
#989969 July 29 2019
#994671 Aug5 2019
#1008571 Aug12 2019
#1043640 Aug 14 2019
#1054395 Aug 19 2019
#1054302 Aug 21 2019
#1054314 Aug 26 2019
#1055056 Sep 9 2019
#797385 Jun 23 2020
#856506 Jun23 pm
#999356 Jun25 2020
#1004961 Jul32020

table(unique(tolower(wqdata$LawaSiteID))%in%tolower(siteTable$LawaSiteID))
table(unique(tolower(wqdata$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID))
wqdata$CouncilSiteID[!tolower(wqdata$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID) & 
                       (wqdata$LawaSiteID)%in%(siteTable$LawaSiteID)] <- 
  siteTable$CouncilSiteID[match(wqdata$LawaSiteID[!tolower(wqdata$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID) & 
                                                    (wqdata$LawaSiteID)%in%(siteTable$LawaSiteID)],siteTable$LawaSiteID)]
# wqdata <- wqdata%>%select(-CouncilSiteID.1)
table(unique(tolower(wqdata$SiteID))%in%tolower(siteTable$SiteID))

#The Lawa CouncilSiteIDs are not in the siteTable as CouncilSiteID
unique(wqdata$CouncilSiteID[!tolower(wqdata$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID)])
unique(wqdata$Agency[!tolower(wqdata$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID)])



wqdata$Symbol=""
wqdata$Symbol[wqdata$CenType=="Left"]='<'
wqdata$Symbol[wqdata$CenType=="Right"]='>'
wqdata$RawValue=paste0(wqdata$Symbol,wqdata$Value)

##################
suppressWarnings(try(dir.create(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d")))))
write.csv(wqdata,paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/AllCouncils.csv"),row.names = F)
# wqdata=read.csv(tail(dir("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data",pattern="AllCouncils.csv",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)
##################


