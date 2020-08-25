rm(list=ls())
source('H:/ericg/16666LAWA/LAWA2020/scripts/LAWAFunctions.R')

monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/Metadata/LAWA Masterlist of Umbraco Sites as at 30 July 2020.xlsx',sheet=1)%>%filter(grepl('nrwqn',LAWAID,T))%>%filter(Module=="River Quality")%>%select(Region:Longitude,-SiteType)%>%arrange(LAWAID)%>%as.data.frame
nrwqncodes = read.csv('./Metadata/NRWQNCodes.csv',stringsAsFactors = F)

nrwqncodes$Place[nrwqncodes$Place=="Wairu at Purua"] <- "Wairua at Purua"

# niwast=loadLatestSiteTableRiver()%>%filter(Agency=='niwa')%>%select(CouncilSiteID,SiteID,LawaSiteID,Region,Lat,Long)

head(nrwqncodes)
head(monList)



nrwqncodes$Place <- tolower(gsub('Bridge| at | |@|br.|river','',nrwqncodes$Place,ignore.case = T))
monList$Name <- tolower(gsub('Bridge| at | |@|br.|river','',monList$Name,ignore.case=T))

table(nrwqncodes$Place%in%monList$Name)
table(monList$Name%in%nrwqncodes$Place)

nrwqncodes$monListLawaID = monList$LAWAID[match(nrwqncodes$Place,monList$Name)]


nrwqncodes


allcodes=c(paste0('NRWQN-0000',1:9),paste0('NRWQN-000',10:77))
allcodes[!allcodes%in%nrwqncodes$monListLawaID]->StillMissing

lawaMasterList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/Metadata/LAWA Masterlist of Umbraco Sites as at 30 July 2020.xlsx',sheet=1)
lawaMasterList19 = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/Metadata/LAWA Site Master List.xlsx',sheet=2)

lml=lawaMasterList[grep('nrwqn',lawaMasterList$LAWAID,ignore.case = T),]%>%select(Region,LAWAID,Name,Latitude,Longitude)%>%arrange(LAWAID)%>%as.data.frame
rm(lawaMasterList19,lawaMasterList)


table(StillMissing%in%lml$LAWAID)
lml[lml$LAWAID%in%StillMissing,]

nrwqncodes%>%filter(is.na(monListLawaID))%>%arrange(Place)
