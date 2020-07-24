# ------------------------------
# BATCH LOADER FOR COUNCIL DATA
# ------------------------------
rm(list=ls())
try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2020/Lakes/Data/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)

#This is find out what they had last year?
# ldl=readxl::read_xlsx("h:/ericg/16666LAWA/LAWA2020/Lakes/Data/LakeDownloadData.xlsx",sheet = "Lakes Data")
# by(data = ldl[,7:14],INDICES = ldl$rcid,FUN = function(x)apply(x,2,FUN=function(x)any(!is.na(x))))
# rm(ldl)
## ----------------------------------------------------------------------------,
## Import Lake data to the "1.Imported" folder for 2018

## import destination will be in folder with todays date (created above)
# importDestination <- paste("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",sep="")
source('h:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
siteTable=loadLatestSiteTableLakes()
siteTable$Agency=tolower(siteTable$Agency)


scriptsToRun = c("H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadAC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadBOP.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadECAN.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadES.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadGDC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadGWRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadHBRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadHRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadMDC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadNCC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadNRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadORC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadTDC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadTRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadWCRC.R",
                 "H:/ericg/16666LAWA/LAWA2020/Lakes/Scripts/loadWRC.R")
agencies = c('ac','boprc','ecan','es','gdc','gwrc','hbrc','hrc','mdc','ncc','nrc','orc','tdc','trc','wcrc','wrc')
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')
})
foreach(i = 1:length(scriptsToRun),.errorhandling = "stop")%dopar%{
  if(agencies[i]%in%tolower(siteTable$Agency)) try(source(scriptsToRun[i]))
  return(NULL)
}
stopCluster(workers)
rm(workers)
cat("Done load\n")

 


##############################################################################
#                                 XML to CSV
##############################################################################
lawaset=c("TN","NH4N","TP","CHLA","pH","Secchi","ECOLI")
for(agency in sort(unique(siteTable$Agency))){
  if(agency=='ac'){next} #ARC is saved as csv already, see loadAC.r
  suppressWarnings({rm(forcsv)})
  forcsv=xml2csvLake(agency=agency,maxHistory = 40,quiet=F)
  cat(length(unique(forcsv$Measurement)),paste(unique(forcsv$Measurement),collapse=', '),'\n')
  if(is.null(forcsv)){next}
  #In SWQ this is done by the transfers table file
  forcsv$Measurement[grepl(pattern = 'Transparency|Secchi|Clarity',x = forcsv$Measurement,ignore.case = T)] <- "Secchi"
  forcsv$Measurement[grepl(pattern = 'loroph|CHL|hloro',x = forcsv$Measurement,ignore.case = T)] <- "CHLA"
  forcsv$Measurement[grepl(pattern = 'coli|ecol',x = forcsv$Measurement,ignore.case = T)] <- "ECOLI"
  forcsv$Measurement[grepl(pattern = 'phosphorus|phosphorous|TP|P _Tot|Tot P',x = forcsv$Measurement,ignore.case = T)] <- "TP"
  forcsv$Measurement[grepl(pattern = 'Ammonia|NH4',x = forcsv$Measurement,ignore.case = T)] <- "NH4N"
  forcsv$Measurement[grepl(pattern = 'TN..HRC.|total nitrogen|totalnitrogen|Nitrogen..Total.|N _Tot|Tot N',
                            x = forcsv$Measurement,ignore.case = T)] <- "TN" #Note, might be nitrate nitrogen
  forcsv$Measurement[grepl(pattern = 'ph \\(field\\)|ph \\(lab\\)|pH_Lab|pH \\(pH',x = forcsv$Measurement,ignore.case = T)] <- "pH"

  cat(length(unique(forcsv$Measurement)),paste(unique(forcsv$Measurement),collapse='\t'),'\n')
  cat(agency,'\t\t',lawaset[!lawaset%in%unique(forcsv$Measurement)],'\n') #Missing Measurements
  excess=unique(forcsv$Measurement)[!unique(forcsv$Measurement)%in%lawaset] #Surplus Measurements
  if(length(excess)>0){
    browser()
    forcsv=forcsv[-which(forcsv$Measurement%in%excess),]
  }
  rm(excess)
  prenacount=sum(is.na(forcsv$Value))
  forcsv$Value=as.numeric(forcsv$Value)
  stopifnot(sum(is.na(forcsv$Value))==prenacount)
  rm(prenacount)
  write.csv(forcsv,
            file=paste0( 'H:/ericg/16666LAWA/LAWA2020/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d"),'/',agency,'.csv'),
            row.names=F)
  suppressWarnings({rm(forcsv)})
}


##############################################################################
#                                 COMBO
##############################################################################
#Build the combo ####
siteTable=loadLatestSiteTableLakes()
lawaset=c("TN","NH4N","TP","CHLA","pH","Secchi","ECOLI")
rownames(siteTable)=NULL
suppressWarnings(rm(lakedata,"ac","boprc","ecan","es","gwrc","hbrc","hrc","nrc","orc","trc","wcrc","wrc" ))
agencies= sort(unique(siteTable$Agency))
library(parallel)
library(doParallel)
workers=makeCluster(7)
registerDoParallel(workers)
foreach(agency =1:length(agencies),.combine = rbind,.errorhandling = 'remove')%dopar%{
  mfl=loadLatestCSVLake(agencies[agency],maxHistory = 30)
  if(!is.null(mfl)&&dim(mfl)[1]>0){
    if(agencies[agency]=='boprc'){
      mfl$CouncilSiteID[which(tolower(mfl$CouncilSiteID)=='fi680541')]="fi680541_int"
    }
    if(sum(!tolower(mfl$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID))>0){
      cat('\t',sum(!unique(tolower(mfl$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID)),'CouncilSiteIDs not in site table\n')
      cat('\t',sum(!unique(tolower(mfl$LawaSiteID))%in%tolower(siteTable$LawaSiteID)),'LawaSiteIDs not in site table\n')
    }
    mfl$agency=agencies[agency]
    if(agencies[agency] %in% c('ac','es','wrc','nrc')){ #es mg/L  arc mg/L g/m3             wanted in mg/m3
      mfl$Value[mfl$Measurement=="CHLA"]=mfl$Value[mfl$Measurement=="CHLA"]*1000
    }
    
    targetSites=tolower(siteTable$CouncilSiteID[siteTable$Agency==agencies[agency]])
    targetCombos=apply(expand.grid(targetSites,tolower(lawaset)),MARGIN = 1,FUN=paste,collapse='')
    currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
    missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
    #BackFill
    if(length(missingCombos)>0){
      agencyFiles = rev(dir(path = "H:/ericg/16666LAWA/LAWA2020/Lakes/Data/",
                            pattern = paste0('^',agencies[agency],'.csv'),
                            full.names = T,recursive = T,ignore.case = T))[-1]
      for(af in seq_along(agencyFiles)){
        agencyCSV = read.csv(agencyFiles[af],stringsAsFactors=F)
        if(agencies[agency]=='boprc'){
          agencyCSV$CouncilSiteID[which(tolower(agencyCSV$CouncilSiteID)=='fi680541')]="fi680541_int"
        }
        if("Measurement"%in%names(agencyCSV)){
          agencyCSVsiteMeasCombo=paste0(tolower(agencyCSV$CouncilSiteID),tolower(agencyCSV$Measurement))
          if(any(missingCombos%in%unique(agencyCSVsiteMeasCombo))){
      # browser()
            these=agencyCSVsiteMeasCombo%in%missingCombos
            agencyCSV=agencyCSV[these,]
            agencyCSV$agency=agencies[agency]
            if('QC'%in%names(mfl)&!'QC'%in%names(agencyCSV)){
              agencyCSV$QC <- ''
            }
            mfl=rbind(mfl,agencyCSV[,names(mfl)])
            rm(agencyCSV,these)
            currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
            missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
            if(length(missingCombos)==0){
              break
            }            
          }
          rm(agencyCSVsiteMeasCombo)
        }
        suppressWarnings(rm(agencyCSV))
        
      }
    }
    rm(missingCombos,targetCombos,currentSiteMeasCombos,targetSites)
    
    if(agencies[agency]=='niwa'){
      mfl$Value[mfl$Measurement%in%c("DRP","NH4","TN","TP")]=mfl$Value[mfl$Measurement%in%c("DRP","NH4","TN","TP")]/1000
    }
    
   }
  return(mfl)
}->lakedata 
stopCluster(workers)
rm(workers)
#23Jun 70297
#25Jun 77483
#9July 79582
#24/7/2020 80692

# 
# #Combine and add metadata ####
# combo=data.frame(agency=NA,CouncilSiteID=NA,Date=NA,Value=NA,Method=NA,Measurement=NA,Censored=NA,centype=NA)
# siteTable <- loadLatestSiteTableLakes(maxHistory = 60)
# for(agency in sort(unique(siteTable$Agency))){
#   checkXMLageLakes(agency)
#   forcsv=loadLatestCSVLake(agency,quiet=T,maxHistory = 100)
#   if(!is.null(forcsv)){
#     # cat(agency,'\n',paste(names(forcsv),collapse='\t'),'\n')
#     forcsv$agency=agency
#      if(agency %in% c('ac','es','wrc')){ #es mg/L  arc mg/L g/m3             wanted in mg/m3
#         forcsv$Value[forcsv$Measurement=="CHLA"]=forcsv$Value[forcsv$Measurement=="CHLA"]*1000
#      }
#     b4u=dim(forcsv)[1]
#     forcsv=unique(forcsv)
#     au=dim(forcsv)[1]
#     if(b4u!=au){cat(agency,'shortened by UNIQUE line 129\n')}
#     rm(b4u,au)
#     combo=merge(combo,forcsv[,names(forcsv)%in%names(combo)],all=T)
#   }  
# }
# combo=combo[-(dim(combo)[1]),]
# #55752
# # combo=unique(combo)
  
 
write.csv(lakedata,paste0('h:/ericg/16666LAWA/LAWA2020/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d"),'/LakesCombined.csv'),row.names = F)
# lakedata=read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2020/Lakes/Data/',pattern='LakesCombined',recursive=T,full.names = T,ignore.case = T),1),stringsAsFactors=F)
lakedata$LawaSiteID = siteTable$LawaSiteID[match(tolower(lakedata$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
lakedata$LawaSiteID[which(is.na(lakedata$LawaSiteID))]=siteTable$LawaSiteID[match(tolower(lakedata$CouncilSiteID[is.na(lakedata$LawaSiteID)]),
                                                                     tolower(siteTable$SiteID))]

# lakedata$LawaSiteID[tolower(lakedata$CouncilSiteID) =="omanuka lagoon (composite)"] = siteTable$LawaSiteID[tolower(siteTable$CouncilSiteID) =="omanuka lagoon (composite)"]

lakedata$CouncilSiteID=tolower(lakedata$CouncilSiteID)
siteTable$CouncilSiteID=tolower(siteTable$CouncilSiteID)
lakesWithMetadata=merge(lakedata,siteTable,by="CouncilSiteID",all.x=T,all.y=F)

missingCouncilSiteIDs=unique(lakesWithMetadata$CouncilSiteID.x[is.na(lakesWithMetadata$LawaSiteID)])
missingCouncilSiteIDs%in%siteTable$CouncilSiteID
missingCouncilSiteIDs%in%siteTable$SiteID
missingCouncilSiteIDs%in%siteTable$LawaSiteID

# lakesWithMetadata$CouncilSiteID.x[is.na(lakesWithMetadata$CouncilSiteID.x)] <- 
#   lakesWithMetadata$CouncilSiteID.y[is.na(lakesWithMetadata$CouncilSiteID.x)]


lakesWithMetadata <- lakesWithMetadata%>%
  dplyr::select(-LawaSiteID.y,-agency,-Method)%>%
  dplyr::rename(LawaSiteID=LawaSiteID.x)%>%
  dplyr::select(LawaSiteID,CouncilSiteID,SiteID,Agency,Region,everything())


table(lakesWithMetadata$Agency,lakesWithMetadata$GeomorphicLType)
table(factor(lakesWithMetadata$Agency,levels=c('ac','boprc','ecan','es','gdc','gwrc','hbrc','hrc','mdc','ncc','nrc','orc','tdc','trc','wcrc','wrc')))

suppressWarnings(try(dir.create(paste0('h:/ericg/16666LAWA/LAWA2020/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d")))))
write.csv(lakesWithMetadata,paste0('h:/ericg/16666LAWA/LAWA2020/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d"),'/LakesWithMetadata.csv'),row.names = F)
save(lakesWithMetadata,file = paste0('h:/ericg/16666LAWA/LAWA2020/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d"),'/LakesWithMetadata.rData'))


#    ac boprc  ecan    es   gdc  gwrc  hbrc   hrc   mdc   ncc   nrc   orc   tdc   trc  wcrc   wrc 
# 10145 17501 15967  8244     0  1897  2336  1026     0     0  5627  6629     0  1461  1563  7186 

# 
# for(agency in c("ac","boprc","ecan","es","gwrc","hbrc","hrc","nrc","orc","trc","wcrc","wrc")){
#   forcsv=loadLatestCSVLake(agency,quiet=T,maxHistory = 100)
#   if(!is.null(forcsv)){
#     if(any(missingCouncilSiteIDs%in%forcsv$CouncilSiteID)){
#       cat(agency,'\t')
#     }
#   }
# }
