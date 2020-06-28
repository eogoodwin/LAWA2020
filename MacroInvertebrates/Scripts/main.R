# ======================
rm(list = ls())
setwd("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates")

## DATA Import
## Site Tables / Location data
source("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/Macro_PrepWFS.R")
cat('done prep WFS\n')
## Mancroivertebrant data
source("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/Macro_loadAndCompile.R")
cat('done load Mancros\n')
source("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/Macro_state.r")
cat('done state\n')
source("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/Macro_trend.R")
cat('done trend\n')
source("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Scripts/Macro_Audit.R")
cat('done audit Mancros\n')



