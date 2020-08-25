rm(list=ls())
library(tidyverse)
startYear15 <- lubridate::isoyear(Sys.Date())-15  #2005  #This convention requires >=
startYear10 <- lubridate::isoyear(Sys.Date())-10  #2010
startYear5 <-  lubridate::isoyear(Sys.Date())-5  #2015
EndYear <- lubridate::isoyear(Sys.Date())-1    #2019     #This convention requires <=
source("h:/ericg/16666LAWA/LAWA2020/scripts/lawa_state_functions.R")
source("h:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R")

macroSiteTable=loadLatestSiteTableMacro()


try(dir.create(paste0("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"))))

#Load the latest made (might need to check it works nicely across month folders) 
if(!exists('macroData')){
  macroDataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data",pattern = "MacrosWithMetadata.csv",recursive = T,full.names = T),1)
  cat(as.character(file.info(macroDataFileName)$mtime))
  macroData=read.csv(macroDataFileName,stringsAsFactors = F)
  rm(macroDataFileName)
  # macroData$Date[macroData$Date=="(blank)"]=paste0("01-Jan-",macroData$Year[macroData$Date=="(blank)"])
  macroData$Date=format(dmy(macroData$Date),'%d-%b-%Y')
  macroData$LawaSiteID=tolower(macroData$LawaSiteID)
}

#Bay of Plenty 2017 squeeze it in
#Doesnt add data, and doesnt appear to be needed, for filling LawaSiteIDs as of 14/8/20
# year2017=read_csv("H:/ericg/16666Lawa/LAWA2020/MacroInvertebrates/Data/BOPRC_Data_for_LAWA_2017_2018.csv")%>%
#   drop_na(Aquarius)
# macroData$LawaSiteID[which(is.na(macroData$LawaSiteID))]=tolower(year2017$LAWA_ID[match(tolower(macroData$CouncilSiteID[which(is.na(macroData$LawaSiteID))]),
#                                                                                         tolower(year2017$Aquarius))])

#Output the raw data for ITE
#Output for ITE
write.csv(macroData%>%
            filter(Measurement%in%c("MCI","TaxaRichness","PercentageEPTTaxa"))%>%
            transmute(LAWAID=gsub('_NIWA','',LawaSiteID,ignore.case = T),
                      SiteName = CouncilSiteID,
                      CollectionDate=format(lubridate::dmy(Date),"%Y-%m-%d"),
                      Metric=Measurement,
                      Value=Value)%>%
            group_by(LAWAID,SiteName,CollectionDate,Metric)%>%
            dplyr::summarise(Value=median(Value,na.rm=T,type=5))%>%ungroup,
          file = paste0("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                        "/ITEMacroHistoricData",format(Sys.time(),'%d%b%Y'),".csv"),row.names=F)



macroData$month=lubridate::month(lubridate::dmy(macroData$Date))
macroData$isoYear=lubridate::isoyear(lubridate::dmy(macroData$Date))
macroData=macroData[which(macroData$isoYear>=startYear5 & macroData$isoYear<=EndYear),]

#49506 to 15576 for startYear5 Aug13


macroMeas <- c("PercentageEPTTaxa", "TaxaRichness", "MCI") 

suppressWarnings(rm(macroData_A,macroData_med,macroData_n,lawaMacroData))
for(i in 1:length(macroMeas)){
  
  macroData_A = macroData[tolower(macroData$Measurement)==tolower(macroMeas[i]),]
  # ADD tracking of n, to see how many values the medians were calculated from
  macroData_med <- doBy::summaryBy(formula=Value~LawaSiteID+Measurement+Year,
                             id=~SiteID+CouncilSiteID+Agency+Landcover+Altitude,
                             data=macroData_A, 
                             FUN=quantile, prob=c(0.5), type=5, na.rm=TRUE, keep.name=TRUE)
  macroData_med$LAWAID=macroData_med$LawaSiteID
  macroData_med$LandcoverGroup=macroData_med$Landcover
  macroData_med$AltitudeGroup=macroData_med$Altitude
  macroData_med$Catchment='all'
  macroData_med$Frequency='all'
  
  macroData_n <- doBy::summaryBy(formula=Value~LawaSiteID+Measurement+Year,
                           id=~SiteID+CouncilSiteID+Agency+Region+Landcover+Altitude,
                           data=macroData_A,
                           FUN=length,keep.names=T)
  macroData_med$n=macroData_n$Value

  rm(macroData_n)
  rm(macroData_A)
  gc()
  # Building dataframe to save at the end of this step 
  if(i==1){
    lawaMacroData <- macroData_med
  } else {
    lawaMacroData <- rbind(lawaMacroData,macroData_med)
  }   
  
  if(0){
    # =======================================================
    # Macro State Analysis
    # =======================================================
    
    # All data for the current Measurement is passed through to the StateAnalysis
    # Function.
    #   The output of this function is a data.frame with site medians, 
    # with the grouping variables of Landcover, altitude, catchment and local
    # local authority name. This data.frame forms the basis for calculating
    # State for each site, based on the median of the last sampled values
    #   This step also excludes those sites that meets the following exclusion
    # criteria:
    #
    # Exclusion criteria
    #   - less than 30 samples for monthly samples
    #   - less than 80 percent of samples for bimonthly/quarterly
    
    cat("LAWA Macro State Analysis\t",macroMeas[i],'\n')
    cat("LAWA Macro State Analysis\nCalculating reference quartiles\n")
    
    state <- c("Site","Catchment","Region","NZ")
    level <- c("LandcoverAltitude","Landcover","Altitude","None")
    
    sa11 <- StateAnalysis(df = macroData_med,type = state[1],level = level[1])
    
    sa21 <- StateAnalysis(macroData_med,state[2],level[1])
    sa22 <- StateAnalysis(macroData_med,state[2],level[2])
    sa23 <- StateAnalysis(macroData_med,state[2],level[3])
    sa24 <- StateAnalysis(macroData_med,state[2],level[4])
    
    sa31 <- StateAnalysis(macroData_med,state[3],level[1])
    sa32 <- StateAnalysis(macroData_med,state[3],level[2])
    sa33 <- StateAnalysis(macroData_med,state[3],level[3])
    sa34 <- StateAnalysis(macroData_med,state[3],level[4])
    
    sa41 <- StateAnalysis(macroData_med,state[4],level[1])
    sa42 <- StateAnalysis(macroData_med,state[4],level[2])
    sa43 <- StateAnalysis(macroData_med,state[4],level[3])
    sa44 <- StateAnalysis(macroData_med,state[4],level[4])
    
    cat("LAWA Macro State Analysis\n","Binding ",macroMeas[i]," data together for measurement\n")
    
    if(i==1){
      sa <- rbind(sa11,sa21,sa22,sa23,sa24,sa31,sa32,sa33,sa34,sa41,sa42,sa43,sa44)
    } else {
      sa <- rbind(sa,sa11,sa21,sa22,sa23,sa24,sa31,sa32,sa33,sa34,sa41,sa42,sa43,sa44)
    }
    
    rm(sa11,sa21,sa22,sa23,sa24,sa31,sa32,sa33,sa34,sa41,sa42,sa43,sa44)
  }
}
# rm(state)

# Housekeeping
# - Saving the lawaMacroData table  USED in NOF calculations
save(lawaMacroData,file=paste("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                              "/lawaMacroData",startYear5,"-",EndYear,".RData",sep=""))
# load(file=tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis",pattern = "lawaMacroData",recursive = T,full.names = T),1),verbose = T)


lawaMacroState5yr <- lawaMacroData%>%
  filter(!is.na(LawaSiteID))%>%
  filter(Year>=startYear5)%>%
  dplyr::group_by(LawaSiteID,Measurement)%>%
  dplyr::summarise(Median=quantile(Value,prob=0.5,type=5,na.rm=T),n=n())%>%ungroup
sum(lawaMacroState5yr$n>=3)
sum(lawaMacroState5yr$n>=3)/dim(lawaMacroState5yr)[1]
#0.95  3010 out of 3161
lawaMacroState5yr <- lawaMacroState5yr%>%filter(n>=3)
#3161 to 3010

# lawaMacroState5yr$period=5
write.csv(lawaMacroState5yr%>%transmute(LAWAID=LawaSiteID,
                                        Parameter=Measurement,
                                        Median=Median),
          file=paste0('h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/',
                                        format(Sys.Date(),"%Y-%m-%d"),
                                        '/ITEMacroState',format(Sys.time(),'%d%b%Y'),'.csv'),row.names = F)
# lawaMacroState5yr = read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/',pattern='MACRO_STATE_ForITE|MacroState',recursive = T,full.names = T),1),stringsAsFactors = F)


cat('Stop here!')
stop()
lawaMacroState5yr$MCIband=(cut(lawaMacroState5yr$Median,breaks = c(0,80,100,120,200)))
lawaMacroState5yr$quartile=(cut(lawaMacroState5yr$Median,breaks = quantile(lawaMacroState5yr$Median,probs=c(0,0.25,0.5,0.75,1))))
plot(jitter(lawaMacroState5yr$MCIband),jitter(lawaMacroState5yr$quartile))
table(lawaMacroState5yr$MCIband,lawaMacroState5yr$quartile)

# 
# cat("LAWA Macro State Analysis\nAssigning State Scores\n")
# # ' //   In assigning state scores, the routine needs to process each combination of altitude
# # ' // and Landcover and compare to the National levels for the same combinations.
# # ' //   These combinations are:
# 
# # ' //   National data set - no factors
# # ' //       Each site (all altitude and Landcovers) compared to overall National medians
# 
# # ' //   Single factor comparisons
# # ' //       Each upland site (all Landcovers) compared to upland National medians
# # ' //       Each lowland site (all Landcovers) compared to lowland National medians
# # ' //       Each rural site (all altitudes) compared to rural National medians
# # ' //       Each forest site (all altitudes) compared to forest National medians
# # ' //       Each urban site (all altitudes) compared to urban National medians
# 
# # ' //   Multiple factor comparisons
# # ' //      For each Altitude
# # ' //        Each rural site compared to rural National medians
# # ' //        Each forest site compared to forest National medians
# # ' //        Each urban site compared to urban National medians
# 
# # ' //      For each Landcover
# # ' //        Each upland site compared to upland National medians
# # ' //        Each lowland site compared to lowland National medians
# 
# 
# scope <- c("Site","Catchment","Region") 
# i=1
# # for(i in 1:3){
# 
# ss1 <-   StateScore(df = sa,scopeIn = tolower(scope[i]),altitude = tolower(""),Landcover = tolower(""),wqparam = macroMeas,comparison=1)
# ss21 <-  StateScore(df = sa,scopeIn = tolower(scope[i]),altitude = tolower("Upland"),Landcover = tolower(""),macroMeas,comparison=2)
# ss22 <-  StateScore(df = sa,scopeIn = tolower(scope[i]),altitude = tolower("Lowland"),Landcover = tolower(""),macroMeas,comparison=2)
# ss31 <-  StateScore(df = sa,scopeIn = tolower(scope[i]),altitude = tolower(""),Landcover = tolower("Rural"),macroMeas,comparison=3)
# ss32 <-  StateScore(df = sa,scopeIn = tolower(scope[i]),altitude = tolower(""),Landcover = tolower("Forest"),macroMeas,comparison=3)
# ss33 <-  StateScore(df = sa,scopeIn = tolower(scope[i]),altitude = tolower(""),Landcover = tolower("Urban"),macroMeas,comparison=3)
# ss411 <- StateScore(df = sa,scopeIn = tolower(scope[i]),altitude = tolower("Upland"),Landcover = tolower("Rural"),macroMeas,comparison=4)
# ss412 <- StateScore(df = sa,scopeIn = tolower(scope[i]),altitude = tolower("Upland"),Landcover = "forest",macroMeas,comparison=4)
# 
# # The following line will fail if there are no sites with Upland Urban classification
# # Need to put a test into the StateScore function to return an empty dataframe
# 
# # RE-ENABLE THIS ONCE BOPRC data available
# if(0){
#   ss413 <- StateScore(df = sa,scopeIn=scope[i],altitude = "upland",Landcover = "urban",macroMeas,comparison=4)
#   ss421 <- StateScore(sa,scope[i],"lowland","rural",macroMeas,comparison=4)
#   ss422 <- StateScore(sa,scope[i],"lowland","forest",macroMeas,comparison=4)
#   ss423 <- StateScore(sa,scope[i],"lowland","urban",macroMeas,comparison=4)
# }
# 
# if(i==1){
#   ss <- rbind(ss1,ss21,ss22,ss31,ss32,ss33,ss411,ss412,ss421,ss422,ss423)
# } else{
#   ss <- rbind(ss,ss1,ss21,ss22,ss31,ss32,ss33,ss411,ss412,ss421,ss422,ss423)
# }
# rm(ss1,ss21,ss22,ss31,ss32,ss33,ss411,ss412,ss421,ss422,ss423)
# # }
# 
# 
# write.csv(ss,file=paste("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/state",StartYear,"-",EndYear,".csv",sep=""),row.names = F)
# 
# 
# 
# cat("LAWA Macro State Analysis\nCompleted assigning State Scores\n")
# 
# ss_csv <- read.csv(file=paste("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/state",StartYear,"-",EndYear,".csv",sep=""),header=TRUE,sep=",",quote = "\"")
# 
# ss.1 <- subset(ss_csv,Scope=="Region")
# ss.1$Location <- ss.1$Region
# ss.2 <- subset(ss_csv,Scope=="Catchment")
# ss.2$Location <- ss.2$Catchment
# ss.3 <- subset(ss_csv,Scope=="Site")
# ss.3$Location <- ss.3$LAWAID
# 
# ss.4 <- rbind.data.frame(ss.1,ss.2,ss.3)
# # unique(ss.4$Location)
# 
# ss.5 <- ss.4[c(18,8,2,3,11,17,4,15,16)-1]  # Location, Measurement, Altitude, Landcover, Q50, LAWAState, Region, Scope, StateGroup
# 
# 
# 
# write.csv(ss.5,file=paste("h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/LAWA_STATE_FINAL_",StartYear,"-",EndYear,".csv",sep=""),row.names = F)
# # lawaMacroData_without_niwa <- subset(lawaMacroData,Agency!="NIWA")
# # lawaMacroData_q_without_niwa <- subset(lawaMacroData_q,Agency!="NIWA")
# 
# # write.csv(lawaMacroData_without_niwa,"h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/LAWA_DATA.csv",row.names = F)
# # write.csv(lawaMacroData_without_niwa,"h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/LAWA_RAW_DATA.csv",row.names = F)
# 
# 
