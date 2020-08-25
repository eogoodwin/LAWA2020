rm(list=ls())
library(parallel)
library(doParallel)
source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')

dir.create(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/", format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

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



doneload=T



##############################################################################
#Check all the measurement names are in the transfers table
##############################################################################
transfers=read.table("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/transfers_plain_english_view.txt",
                     sep=',',header = T,stringsAsFactors = F)
# transfers$CallName[which(transfers$CallName=="Clarity (Black Disc Field)"&transfers$Agency=='es')] <- "Clarity (Black Disc, Field)"
# for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")){
#   df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/MetaData/",agency,"SWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
#   Measurements <- subset(df,df$Type=="Measurement")[,2]
#   if(any(!Measurements%in%transfers$CallName[transfers$Agency==agency])){
#     cat(agency,'\t',Measurements[!Measurements%in%transfers$CallName[transfers$Agency==agency]])
#     print('\n')
#   }
# }


##############################################################################
#                                 XML to CSV
##############################################################################
# agencies=c("ac","boprc","gdc","gwrc","hbrc")

agencies=c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc") 
lawaset=c("NH4", "TURB","TURBFNU", "BDISC",  "DRP",  "ECOLI",  "TN",  "TP",  "TON",  "PH")
transfers=read.table("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Metadata/transfers_plain_english_view.txt",
                     sep=',',header = T,stringsAsFactors = F)
# transfers$CallName[which(transfers$CallName=="Clarity (Black Disc Field)"&transfers$Agency=='es')] <- "Clarity (Black Disc, Field)"

workersB <- makeCluster(7)
registerDoParallel(workersB)
cc=clusterCall(workersB,function(){
  library(tidyverse)
  source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
  agencies=c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc") 
  lawaset=c("NH4", "TURB", "BDISC",  "DRP",  "ECOLI",  "TN",  "TP",  "TON",  "PH")
  })
startTime=Sys.time()
# for(i in 1:length(agencies)){
foreach(i = 1:length(agencies),.errorhandling="stop")%dopar%{
  xml2csvRiver(agency=agencies[i],quiet=T,reportCensor=F,reportVars=F,ageCheck=F,maxHistory=20)
  return(NULL)
  }
stopCluster(workersB)
rm(workersB)
Sys.time()-startTime
rm(startTime)
#15 minutes
##############################################################################
#                                 *****
##############################################################################
cat("Done to CSV")
donexmltocsv=T




#19 June 2020
#Check agency and region count per file
for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")){
  checkXMLageRiver(agency)
  checkCSVageRiver(agency = agency)
}

   #   mfl=loadLatestCSVRiver(agency,maxHistory = 3,silent=T)
#    cat(names(mfl),'\n')
#   # print(knitr::kable(table(tolower(mfl$SiteName)==tolower(mfl$CouncilSiteID))))
# }

# 
# #This could checks whether previous downloads pulled datas missing from current download.
# for (agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")){
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
  suppressWarnings(rm(wqdata,"ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc"))
  agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
library(parallel)
library(doParallel)
  backfill=T
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
  if('centype'%in%names(mfl)){
    names(mfl)[names(mfl)=='centype'] <- 'CenType'
  }
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

    #BACKFILL except canterbury
    if(backfill){
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
            if("Units"%in%names(mfl))agencyCSV$Units=""
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
}->wqdata   #23/6/2020    797484
            #23/6/2020pm  856506
            #25/6/2020    999530
            #3/7/2020    1005143
            #9/7/2020    1040614  1049189
            #16/7/2020   1084848
            #24/7/2020   1102252
            #31/7/2020   1087519
            #07/08/2020  1080963
            #14/8/2020   1116097
stopCluster(workers)
rm(workers)

Sys.time()-startTime  #28s

donecombine=T



 lawaset=c("NH4", "TURB","TURBFNU", "BDISC",  "DRP",  "ECOLI",  "TN",  "TP",  "TON",  "PH")

#Add the TRC data that wont get released until they realise we dont need their flow data to use their WQ data

 TRCextra = read.csv("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/Physicochem_Data_2020_Eric_28July2020.csv",stringsAsFactors = F)
                    # TRCFRODO-#2543515-v1-Physicochem_2019_SEM_Report__NPS-FM_2017_attribute_ammonia_and_nitrate_toxicity.csv",stringsAsFactors = F)
names(TRCextra)[1] <- "CouncilSiteID"
TRCextra <- TRCextra%>%
  filter(trc_parameter_code%in%c("NH3","NH4","TURB","TURBY","TURBYF","BDISC","DRP","ECOL","TN","TP","NO2","NO3","NNN","PH"))%>%
  transmute(CouncilSiteID,
            Date=format(lubridate::ymd(collected_date),format='%d-%b-%y'),
            Value=value_raw,Measurement=trc_parameter_name,
         Units=units,Censored=grepl("<|>",.$prefix_symbol),
         CenType=as.character(factor(.$prefix_symbol,levels=c("","<",">"),labels=c("FALSE","Left",'Right'))),
         QC=0,SiteID=location,LawaSiteID=0,
         NZReach=0,Region="taranaki",Agency='trc',SWQAltitude='unstated',SWQLanduse='unstated',
         Lat=0,Long=0,accessDate="28-Jul-2020",Catchment=parent_catchment,Landcover='unstated',Altitude='unstated',AltitudeCl='unstated')

TRCextra <- TRCextra%>%
  filter(lubridate::dmy(Date)>max(lubridate::dmy(wqdata$Date[wqdata$Agency=='trc'])))
TRCextra$LawaSiteID=siteTable$LawaSiteID[match(tolower(TRCextra$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
TRCextra <- TRCextra%>%drop_na(LawaSiteID)

TRCextra$Measurement[TRCextra$Measurement=="Ammoniacal nitrogen"] <- "NH4"
TRCextra$Measurement[TRCextra$Measurement=="Turbidity"] <- "TURBFNU"
TRCextra$Measurement[TRCextra$Measurement=="Field turbidity measurement"] <- "TURBFNU"
TRCextra$Measurement[TRCextra$Measurement=="Black disc transparency"] <- "BDISC"
TRCextra$Measurement[TRCextra$Measurement=="Dissolved reactive phosphorus"] <- "DRP"
TRCextra$Measurement[TRCextra$Measurement=="E.coli bacteria"] <- "ECOLI"
TRCextra$Measurement[TRCextra$Measurement=="Total nitrogen"] <- "TN"
TRCextra$Measurement[TRCextra$Measurement=="Total phosphorus"] <- "TP"
TRCextra$Measurement[TRCextra$Measurement=="Nitrite/nitrate nitrogen"] <- "TON"
TRCextra$Measurement[TRCextra$Measurement=="pH"] <- "PH"
TRCextra <- TRCextra%>%filter(Measurement%in%lawaset)

TRCextra$NZReach=siteTable$NZReach[match(tolower(TRCextra$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
TRCextra$SWQAltitude=siteTable$SWQAltitude[match(tolower(TRCextra$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
TRCextra$SWQLanduse=siteTable$SWQLanduse[match(tolower(TRCextra$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
TRCextra$Lat=siteTable$Lat[match(tolower(TRCextra$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
TRCextra$Long=siteTable$Long[match(tolower(TRCextra$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
TRCextra$Landcover=siteTable$Landcover[match(tolower(TRCextra$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
TRCextra$Altitude=siteTable$Altitude[match(tolower(TRCextra$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
TRCextra$AltitudeCl=siteTable$AltitudeCl[match(tolower(TRCextra$CouncilSiteID),tolower(siteTable$CouncilSiteID))]

wqdata <- rbind(wqdata,TRCextra)
rm(TRCextra)
#1117454


wqdata$Region[wqdata$Region=="horizons"] <- "manawatū-whanganui"

table(wqdata$Agency,wqdata$QC==0)  #QC data in GWRC, HBRC
table(wqdata$Region)
table(wqdata$Agency)
#    ac  boprc   ecan     es    gdc   gwrc   hbrc    hrc    mdc    ncc   niwa    nrc    orc    tdc    trc   wcrc    wrc 
# 54617  47156 162662  73340  27466  54929  85873 160718   4289  16893 103590  41344  47780  14992  20917  22749  143024 
# 54061  49821 162662  81815  32391  54929  77699 160718  29796  16893 103590  41344  50056  15008  20917  22749  143024
# 54060  49821 162662  81815  32391  54929  77681 160718  29796  16893 103590  41344  50056  15008  20917  22749  143024 
table(wqdata$SWQLanduse,wqdata$SWQAltitude)

wqdata$Units=tolower(wqdata$Units)

unique(wqdata$Units)
wqdata$Units = gsub(pattern = " units|_units",replacement = "",x = wqdata$Units)
wqdata$Units = gsub(pattern = "_",replacement = "",x = wqdata$Units)
wqdata$Units = gsub(pattern = "^g/m.?.",replacement = "g/m3",x = wqdata$Units)
wqdata$Units = gsub(pattern = ".*/ *100ml",replacement = "/100ml",x = wqdata$Units)
wqdata$Units = gsub(pattern = "-n|-p",replacement = "",x = wqdata$Units)
wqdata$Units = gsub(pattern = "/m3.",replacement = "/m3",x = wqdata$Units)
wqdata$Units = gsub(pattern = "meter",replacement = "m",x = wqdata$Units)
wqdata$Units = gsub(pattern = "formazin nephelometric unit",replacement = "fnu",x = wqdata$Units)
unique(wqdata$Units)

table(wqdata$Units,wqdata$Agency)
table(wqdata$Units,wqdata$Measurement)
table(wqdata$Agency,wqdata$Measurement)
table(wqdata$Agency[wqdata$Units=='ntu'&wqdata$Measurement=="TURBFNU"]) #TRC
wqdata$Measurement[wqdata$Units=='ntu'&wqdata$Measurement=="TURBFNU"]<-"TURB"  #Switched in July 2019
wqdata%>%filter(Agency=='trc')%>%select(Measurement,Units)%>%table
wqdata%>%filter(Agency=='wcrc')%>%select(Measurement,Units)%>%table
wqdata%>%filter(Agency=='orc')%>%select(Measurement,Units)%>%table
wqdata%>%filter(Units=='fnu')%>%select(Agency,Measurement)%>%table
wqdata%>%filter(Units=='')%>%select(Agency,Measurement)%>%table
wqdata$Measurement[wqdata$Units=='fnu'&wqdata$Agency=='wcrc'] <- "TURBFNU"
wqdata$Measurement[wqdata$Units=='fnu'] <- "TURBFNU"
wqdata%>%filter(Measurement=="TURBFNU")%>%select(Agency,Units)%>%table

write.csv(table(wqdata$Units,wqdata$Agency),'h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/2020-07-09/unitsagency.csv',row.names = F)
write.csv(table(wqdata$Units,wqdata$Measurement),'h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/2020-07-09/unitsmeasurement.csv',row.names = F)


wqdata$SWQLanduse[which(is.na(wqdata$SWQLanduse))] <- siteTable$SWQLanduse[match(wqdata$LawaSiteID[which(is.na(wqdata$SWQLanduse))],
                                                                                 siteTable$LawaSiteID)]
wqdata$SWQAltitude[which(!is.na(as.numeric(wqdata$SWQAltitude)))] <- siteTable$SWQAltitude[match(wqdata$LawaSiteID[which(!is.na(as.numeric(wqdata$SWQAltitude)))],
                                                                                                 siteTable$LawaSiteID)]
wqdata$SWQAltitude[which(tolower(wqdata$SWQAltitude)=='unstated')] <- siteTable$SWQAltitude[match(wqdata$LawaSiteID[which(tolower(wqdata$SWQAltitude)=='unstated')],
                                                                                                 siteTable$LawaSiteID)]

wqdata$SWQLanduse=pseudo.titlecase(wqdata$SWQLanduse)
wqdata$SWQAltitude=pseudo.titlecase(wqdata$SWQAltitude)

table(wqdata$SWQLanduse,wqdata$SWQAltitude)


wqdata$SiteID=trimws(tolower(wqdata$SiteID))
wqdata$CouncilSiteID=trimws(tolower(wqdata$CouncilSiteID))
wqdata$LawaSiteID=trimws(wqdata$LawaSiteID)
# wqdata$SWQAltitude=tolower(wqdata$SWQAltitude)
wqdata$Altitude=tolower(wqdata$Altitude)
# wqdata$SWQLanduse=tolower(wqdata$SWQLanduse)
wqdata$Landcover=tolower(wqdata$Landcover)
wqdata$Region=tolower(wqdata$Region)
wqdata$Agency=tolower(wqdata$Agency)

wqdata$CenType[wqdata$CenType%in%c("L","Left")] <- "Left"
wqdata$CenType[wqdata$CenType%in%c("R","Right")] <- "Right"

wqdata=unique(wqdata)  #1117473  14Aug 1117289 21Aug 1117279

wqdata%>%group_by(tolower(LawaSiteID))%>%
  dplyr::summarise(agCount=length(unique(Agency)),
                   ags=paste(unique(Agency),collapse=' '),
                   cid=paste(unique(CouncilSiteID),collapse=', '),
                   sid=paste(unique(SiteID),collapse=', '))%>%
  ungroup%>%
  filter(agCount>1)%>%dplyr::select(-agCount)



#Tag the sites that are under NIWA and under an agency
wqdata%>%group_by(tolower(LawaSiteID))%>%
  dplyr::summarise(agCount=length(unique(Agency)),
                   ags=paste(unique(Agency),collapse=' '),
                   cid=paste(unique(CouncilSiteID),collapse=', '))%>%
  ungroup%>%
  filter(agCount>1)%>%dplyr::select(`tolower(LawaSiteID)`)%>%unlist%>%unname->dupSites

wqdata$LawaSiteID[wqdata$LawaSiteID%in%dupSites&wqdata$Agency=='niwa'] <- 
  paste0(wqdata$LawaSiteID[wqdata$LawaSiteID%in%dupSites&wqdata$Agency=='niwa'],"_NIWA")

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
#1017914 Jul92020
#1026489 Jul9 after units and sorting AC and HBRC a bit
#1061942 16Jul2020
#1079348 24Jul2020
#1079547 31july2020
#1073182 7August2020
#1117289 14 August2020
#1117279 21 August 2020

table(unique(tolower(wqdata$LawaSiteID))%in%tolower(siteTable$LawaSiteID))
table(unique(tolower(wqdata$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID))
wqdata$CouncilSiteID[!tolower(wqdata$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID) & 
                       (wqdata$LawaSiteID)%in%(siteTable$LawaSiteID)] <- 
  siteTable$CouncilSiteID[match(wqdata$LawaSiteID[!tolower(wqdata$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID) & 
                                                    (wqdata$LawaSiteID)%in%(siteTable$LawaSiteID)],siteTable$LawaSiteID)]
# wqdata <- wqdata%>%select(-CouncilSiteID.1)
table(unique(tolower(wqdata$SiteID))%in%tolower(siteTable$SiteID))

#The Lawa CouncilSiteIDs are not in the siteTable as CouncilSiteID
unique(wqdata$LawaSiteID[!tolower(wqdata$SiteID)%in%tolower(siteTable$SiteID)])
unique(wqdata$SiteID[!tolower(wqdata$SiteID)%in%tolower(siteTable$SiteID)])
unique(wqdata$CouncilSiteID[!tolower(wqdata$SiteID)%in%tolower(siteTable$SiteID)])
unique(wqdata$Agency[!tolower(wqdata$SiteID)%in%tolower(siteTable$SiteID)])



wqdata$Symbol=""
wqdata$Symbol[wqdata$CenType=="Left"]='<'
wqdata$Symbol[wqdata$CenType=="Right"]='>'
wqdata$RawValue=paste0(wqdata$Symbol,wqdata$Value)

##################
dir.create(paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)
write.csv(wqdata,paste0("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/AllCouncils.csv"),row.names = F)
# wqdata=read.csv(tail(dir("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data",pattern="AllCouncils.csv",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)
##################


