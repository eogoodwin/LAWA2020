# ======================

setwd("H:/ericg/16666Lawa/Lawa2019/WaterQuality/")


## DATA Import
## Site Tables / Location data
source("H:/ericg/16666Lawa/Lawa2019/WaterQuality/Scripts/SWQPrepWFS.R")
cat("Done prep WFS\n")
## Water Quality data
source("H:/ericg/16666Lawa/Lawa2019/WaterQuality/Scripts/SWQLoadandCompile.R")
cat("Done SWQ load compile\n")
## STATE And TREND Analysis
source("H:/ericg/16666Lawa/Lawa2019/WaterQuality/Scripts/SWQ_state.r")
cat("Done state\n")
source("H:/ericg/16666Lawa/Lawa2019/WaterQuality/Scripts/SWQ_NOF.R")
cat("Done NOF\n")
source("H:/ericg/16666Lawa/Lawa2019/WaterQuality/Scripts/SWQtrend.R")
cat("Done trend\n")
source("H:/ericg/16666Lawa/Lawa2019/WaterQuality/Scripts/SWQAudit.R")
cat("Done audit\n")
source("H:/ericg/16666Lawa/Lawa2019/WaterQuality/Scripts/ITEdelivery.R")
cat("Done package\n")

