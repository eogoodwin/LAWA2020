rm(list=ls())
source('H:/ericg/16666LAWA/LAWA2020/Scripts/LAWAFunctions.R')
siteTable=loadLatestSiteTableMacro(maxHistory = 20)

try(shell(paste('mkdir "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/"',format(Sys.Date(),"%Y-%m-%d"),sep=""), translate=TRUE),silent = TRUE)





scriptsToRun = c("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadAC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadBOP.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadECAN.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadES.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadGDC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadGWRC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadHBRC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadHRC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadMDC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadNCC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadNIWA.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadNRC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadORC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadTDC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadTRC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadWCRC.R",
  "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/loadWRC.R")
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



#XML 2 CSV for MACROS ####
lawaset=c("TaxaRichness","MCI","PercentageEPTTaxa")
agencies=c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")
for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
# foreach(agency = 1:length(agencies))
  forcsv=xml2csvMacro(agency,maxHistory = 30,quiet=T)
  if(is.null(forcsv))next
  cat(agency,'\t',paste0(unique(forcsv$Measurement),collapse=', '),'\n')
  if('sqmci'%in%unique(tolower(forcsv$Measurement))){
    forcsv=forcsv[-which(forcsv$Measurement%in%c('SQMCI')),]
    cat(agency,'\t',paste0(unique(forcsv$Measurement),collapse=', '),'\n')
  }
  forcsv$Measurement[grepl(pattern = 'Taxa',x = forcsv$Measurement,ignore.case = T)&
                     !grepl('EPT',forcsv$Measurement,ignore.case = F)] <- "TaxaRichness"
  forcsv$Measurement[grepl(pattern = 'MCI|ate community ind',x = forcsv$Measurement,ignore.case = T)] <- "MCI"
  forcsv$Measurement[grepl(pattern = 'EPT',x = forcsv$Measurement,ignore.case = T)] <- "PercentageEPTTaxa"
  forcsv$Measurement[grepl(pattern = 'Rich',x = forcsv$Measurement,ignore.case = T)] <- "TaxaRichness"
  excess=unique(forcsv$Measurement)[!unique(forcsv$Measurement)%in%lawaset]
  if(length(excess)>0){
browser()
        forcsv=forcsv[-which(forcsv$Measurement%in%excess),]
  }
  rm(excess)
  write.csv(forcsv,
            file=paste0( 'H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),'/',agency,'.csv'),
            row.names=F)
  rm(forcsv)
}




##############################################################################
#                                 *****
##############################################################################
#Build the combo ####
if(exists('macroData')){rm(macroData)}
siteTable=loadLatestSiteTableMacro()
rownames(siteTable)=NULL
lawaset=c("TaxaRichness","MCI","PercentageEPTTaxa")
suppressWarnings(rm(macrodata,"ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc"))
agencies= c("boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","trc","wcrc","wrc")#"tdc",
library(parallel)
library(doParallel)
workers=makeCluster(7)
registerDoParallel(workers)
foreach(agency =1:length(agencies),.combine = rbind,.errorhandling = 'stop')%dopar%{
  mfl=loadLatestCSVmacro(agencies[agency],maxHistory = 30)
  if(!is.null(mfl)&&dim(mfl)[1]>0){
    if(sum(!tolower(mfl$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID))>0){
      cat('\t',sum(!unique(tolower(mfl$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID)),'CouncilSiteIDs not in site table\n')
      cat('\t',sum(!unique(tolower(mfl$LawaSiteID))%in%tolower(siteTable$LawaSiteID)),'LawaSiteIDs not in site table\n')
    }
    
    targetSites=tolower(siteTable$CouncilSiteID[siteTable$Agency==agencies[agency]])
    targetCombos=apply(expand.grid(targetSites,tolower(lawaset)),MARGIN = 1,FUN=paste,collapse='')
    currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
    missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
    
    if(length(missingCombos)>0){
      agencyFiles = rev(dir(path = "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/",
                            pattern = paste0('^',agencies[agency],'.csv'),
                            full.names = T,recursive = T,ignore.case = T))[-1]
      for(af in seq_along(agencyFiles)){
        agencyCSV = read.csv(agencyFiles[af],stringsAsFactors=F)
        if("Measurement"%in%names(agencyCSV)){
          agencyCSVsiteMeasCombo=paste0(tolower(agencyCSV$CouncilSiteID),tolower(agencyCSV$Measurement))
          if(any(missingCombos%in%unique(agencyCSVsiteMeasCombo))){
            browser()
            these=agencyCSVsiteMeasCombo%in%missingCombos
            agencyCSV=agencyCSV[these,]
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
    
        mfl$Agency=agencies[agency]
        if(agencies[agency]=='ac'){
          #Auckland
          mfl$CouncilSiteID=trimws(mfl$CouncilSiteID)
          sort(unique(tolower(mfl$CouncilSiteID))[unique(tolower(mfl$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID)])
          mfl$CouncilSiteID[tolower(mfl$CouncilSiteID)=="avondale @ thuja"] <- "Avondale @ Thuja Pl"
          mfl$Date = format(lubridate::ymd(mfl$Date),'%d-%b-%Y')
           }
        if(agencies[agency]=='es'){
          toCut=which(mfl$CouncilSiteID=="mataura river 200m d/s mataura bridge"&mfl$Date=="21-Feb-2017")
          if(length(toCut)>0){
            mfl=mfl[-toCut,]
          }
          rm(toCut)
        }
        if(agencies[agency]=='tdc'){
          funnyTDCsites=!tolower(mfl$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID)
          cat(length(funnyTDCsites),'\n')
          if(length(funnyTDCsites)>0){
            mfl$CouncilSiteID[funnyTDCsites] <-
              siteTable$CouncilSiteID[match(tolower(mfl$CouncilSiteID[funnyTDCsites]),tolower(siteTable$SiteID))]
          }
          rm(funnyTDCsites)
        }
    
   }
  return(mfl)
}->macroData 
stopCluster(workers)
rm(workers)
#23 Jun 31646
#25Jun 34775
#3July 35560
#9July 35575
# 70693
#24 July 74644
macroData$LawaSiteID = siteTable$LawaSiteID[match(tolower(macroData$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
macroData$LawaSiteID[which(is.na(macroData$LawaSiteID))] = siteTable$LawaSiteID[match(tolower(macroData$CouncilSiteID[which(is.na(macroData$LawaSiteID))]),
                                                                                      tolower(siteTable$SiteID))]
#Add CSV-delivered datasets
acmac=loadLatestCSVmacro(agency = 'ac')%>%select(-QC)
macroData=rbind(macroData[,names(acmac)],acmac) #72571
niwamac = loadLatestCSVmacro(agency='niwa')
macroData=rbind(macroData,niwamac)
#76013

rm(acmac,niwamac)

#This one with rounding is a good way to assign samples to a sampling season.  November/December etc gets rounded forward to the following year 
# macroData$Year[is.na(macroData$Year)] = lubridate::isoyear(lubridate::round_date(lubridate::dmy(macroData$Date[is.na(macroData$Year)]),unit = 'year'))

macroData$Year = lubridate::isoyear(lubridate::dmy(macroData$Date))




write.csv(macroData,paste0('h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),'/MacrosCombined.csv'),row.names = F)
# macroData=read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/',pattern='MacrosCombined.csv',recursive = T,full.names = T),1),stringsAsFactors = F)
names(macroData)
# macroData$CouncilSiteID[grepl('almadale',macroData$CouncilSiteID,ignore.case = T)] <- unique(siteTable$CouncilSiteID[grep('slackline',x = siteTable$SiteID,ignore.case = T)])



# #audit presence of CouncilSiteIDs in sitetable
# table(unique(tolower(macroData$CouncilSiteID))%in%unique(tolower(c(siteTable$SiteID,siteTable$CouncilSiteID))))
# unique((macroData$CouncilSiteID))[!unique(tolower(macroData$CouncilSiteID))%in%unique(tolower(c(siteTable$SiteID,siteTable$CouncilSiteID)))]->unmatched
# 
# # based on CouncilSiteID
# table(unique(macroData$CouncilSiteIDlc)%in%siteTable$CouncilSiteIDlc)
# unique(macroData$agency[!macroData$CouncilSiteIDlc%in%siteTable$CouncilSiteIDlc])
# (unique(macroData$CouncilSiteID[!macroData$CouncilSiteIDlc%in%siteTable$CouncilSiteIDlc])->unmatched)
# table(unique(macroData[,c(1,2)])$agency[unique(macroData[,c(1,2)])$CouncilSiteID%in%unmatched])
# unique(macroData[,c(1,2)])[unique(macroData[,c(1,2)])$CouncilSiteID%in%unmatched,]
# rm(unmatched)
# table(unique(macroData$CouncilSiteIDlc)%in%siteTable$CouncilSiteIDlc)


macroData=merge(macroData,siteTable,by=c("LawaSiteID","Agency"),all.x=T,all.y=F)%>%
  dplyr::select("LawaSiteID","CouncilSiteID.x","SiteID","Region","Agency","Date","Year","Measurement","Value","Lat","Long","Landcover","Altitude")%>%
  dplyr::rename(CouncilSiteID=CouncilSiteID.x)%>%distinct  #Drop macro,and sitnamelc
macroData$Agency=tolower(macroData$Agency)
macroData$Region=tolower(macroData$Region)

agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
table(factor(macroData$Agency,levels=agencies))
table(macroData$Region)
#        ac  boprc  ecan   es  gdc gwrc hbrc  hrc  mdc  ncc  niwa nrc  orc tdc   trc wcrc wrc
#23Jun              8553 2742 1042 1568 2385 2307       976       905 1057      7616 2463 
#25 Jun             8553 2742 1042 1568 2385 2307       976       905 1057      7616 2463 3129
#9July              8949 2994 1042 1568 2537 2307       976       905 1057      7616 2463 3129
#22July 1878 3697   8949 2994 1042 1568 2537 2307    0  976       905 1057   0  7616 2463 3129         
#24July 1878  4039  8949 2997 1042 1568 2537 2307   186 976  3439 905 1057   0  7622 2463 3378 
write.csv(macroData,paste0('h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),
                       '/MacrosWithMetadata.csv'),row.names = F)


