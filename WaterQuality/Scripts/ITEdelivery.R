# Copy Files for ITE


RiverRawData=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/WaterQuality/",
                      pattern="ITERiversRawData",recursive=T,full.names = T,ignore.case = T),1)
RiverState=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/WaterQuality/",
                    pattern="ITERiverState",recursive=T,full.names = T,ignore.case = T),1)
RiverNOF=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/WaterQuality/",
                    pattern="ITERiverNOF",recursive=T,full.names = T,ignore.case = T),1)
RiverTrend=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/WaterQuality/",
                  pattern="ITERiverTrend",recursive=T,full.names = T,ignore.case = T),1)


LakeSiteState=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                       pattern="ITELakeSiteState",recursive=T,full.names = T,ignore.case = T),1)
LakeSiteNOF=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                     pattern="ITELakeSiteNOF[^G]",recursive=T,full.names = T,ignore.case = T),1)
LakeSiteNOFGraph=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                          pattern="ITELakeSiteNOFGraph",recursive=T,full.names = T,ignore.case = T),1)
LakeSiteTrend5=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                        pattern="ITELakeSiteTrend5",recursive=T,full.names = T,ignore.case = T),1)
LakeSiteTrend10=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                         pattern="ITELakeSiteTrend10",recursive=T,full.names = T,ignore.case = T),1)
LakeSiteTrend15=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                         pattern="ITELakeSiteTrend15",recursive=T,full.names = T,ignore.case = T),1)
LakeTLI=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/Lakes/",
                 pattern="ITELakeTLI",recursive=T,full.names = T,ignore.case = T),1)

MacroHistoricData=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/",
                           pattern="ITEMacroHistoric",recursive=T,full.names = T,ignore.case = T),1)
MacroState=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/",
                    pattern="ITEMacroState",recursive=T,full.names = T,ignore.case = T),1)
MacroTrend10=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/",
                      pattern="ITEMacroTrend10",recursive=T,full.names = T,ignore.case = T),1)
MacroTrend15=tail(dir(path="H:/ericg/16666LAWA/LAWA2019/MacroInvertebrates/",
                      pattern="ITEMacroTrend15",recursive=T,full.names = T,ignore.case = T),1)


stopifnot(all(grepl(pattern=format(Sys.Date(),'%Y-%m-%d'),
                    x = c(RiverRawData,RiverState,RiverNOF,RiverTrend,
                          LakeSiteState,LakeSiteNOF,LakeSiteNOFGraph,LakeSiteTrend5,LakeSiteTrend10,LakeSiteTrend15,LakeTLI,
                          MacroHistoricData,MacroState,MacroTrend10,MacroTrend15))))

file.copy(from = RiverRawData,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITERiversRawData.csv",overwrite = T)
file.copy(from = RiverState,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITERiverState.csv",overwrite = T)
file.copy(from = RiverNOF,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITERiverNOF.csv",overwrite = T)
file.copy(from = RiverTrend,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITERiverTrend.csv",overwrite = T)

file.copy(from = LakeSiteState ,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITELakeSiteState.csv",overwrite = T)
file.copy(from = LakeSiteNOF ,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITELakeSiteNOF.csv",overwrite = T)
file.copy(from = LakeSiteNOFGraph ,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITELakeSiteNOFGraph.csv",overwrite = T)
file.copy(from = LakeSiteTrend5 ,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITELakeSiteTrend5.csv",overwrite = T)
file.copy(from = LakeSiteTrend10 ,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITELakeSiteTrend10.csv",overwrite = T)
file.copy(from = LakeSiteTrend15 ,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITELakeSiteTrend15.csv",overwrite = T)
file.copy(from = LakeTLI ,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITELakeTLI.csv",overwrite = T)

file.copy(from = MacroHistoricData,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITEMacroHistoricData.csv",overwrite = T)
file.copy(from = MacroState,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITEMacroState.csv",overwrite = T)
file.copy(from = MacroTrend10,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITEMacroTrend10.csv",overwrite = T)
file.copy(from = MacroTrend15,to = "H:/ericg/16666LAWA/LAWA2019/ITEdelivery/ITEMacroTrend15.csv",overwrite = T)

# and REC data