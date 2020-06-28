# ======================
rm(list = ls())
setwd("h:/ericg/16666LAWA/LAWA2020/Lakes")


## DATA Import
## Site Tables / Location data
source("H:/ericg/16666Lawa/LAWA2020/Lakes/Scripts/LWQprepWFS.R")
cat("Done Prep WFS\n")
source("H:/ericg/16666Lawa/LAWA2020/Lakes/Scripts/LWQloadAndCompile.R")
cat("Done load and compile\n")
source("H:/ericg/16666Lawa/LAWA2020/Lakes/Scripts/LWQ_state.r")
cat("Done state\n")
source("H:/ericg/16666Lawa/LAWA2020/Lakes/Scripts/LWQ_NOF.R")
cat("Done nof\n")
source("H:/ericg/16666Lawa/LAWA2020/Lakes/Scripts/LWQtrend.R")
cat("Done trend\n")
source("H:/ericg/16666Lawa/LAWA2020/Lakes/Scripts/LWQAudit.R")
cat("Done audit\n")

