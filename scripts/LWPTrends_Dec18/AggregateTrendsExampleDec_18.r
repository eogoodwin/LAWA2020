##########################################################
#
#  EXAMPLE usage of LWP-Trends aggregation functions
#
##########################################################

#This file provides an example applciations of the LWP-Trends code
# for trend aggregations

# Date: Dec 2018
# By Ton Snelder and Caroline Fraser

##########################################################

rm(list = ls())                                               # clear the memory
##########################################################
# 0.  Prepration
##########################################################
# Read in required packages
##########################################################
require(plyr)
library(gplots) # for making colour ramp
library(ggplot2)

##########################################################
# Read in functions 
##########################################################
dir.code<-getwd() #UPDATE TO LOCAL DIRECTORY
setwd(dir.code)
source("LWPTrends_v1811.r")

##########################################################
# Read in Example dataset
##########################################################
#The example dataset is the output from the trend analysis
# functions for many site and variable combinations
AllTrends10FA  <- read.csv(file ="AllTrends10FA.csv")

# Define those variables for which decreasing is degrading  
Reverse =  c("Clar", "CLAR", "Clarity", "MCI", "%EPT", "%EPT_Taxa", "QMCI") # variables for which decreasing = degrading  

# Assign confidence categories
AllTrends10FA$DirectionConf <- ImprovementConfCat(x=AllTrends10FA, Reverse=Reverse)

##########################################################
# Make stacked bar chart of confidence categories
##########################################################
#Remove the Not analysed samples to plot the categories
AllTrends10FA2<-AllTrends10FA[AllTrends10FA$DirectionConf!="Not Analysed",]

ConfCats<-ddply(AllTrends10FA2, "npID", function(x) 100*table(x$DirectionConf)/sum(table(x$DirectionConf)), .drop = F)
rowSums(ConfCats[,2:10]) #Check to see that each npID sums to 100%

# stack to make a plot
ConfCatsStack <- stack(ConfCats[, 2:10])
ConfCatsStack$ind <- factor(as.character(ConfCatsStack$ind), levels=names(ConfCats)[2:10])
ConfCatsStack$npID <- ConfCats$npID

#Assign a color ramp for teh plot
myCols <- (colorRampPalette(c("red", "yellow", "green"))(n = 9))  #  red yellow green colour scale 

graphics.off(); x11(height = 7, width = 10) 
ggplot(data=ConfCatsStack, aes(x=npID, y=values, fill=ind)) + 
  geom_bar(stat="identity") + scale_fill_manual(values=myCols)+
  ylab("Proportion of sites (%)")+coord_cartesian(ylim= c(0,100),expand=FALSE) + theme_bw()


##########################################################
# Calculate Probability of Improving Trends
##########################################################
PIT <- ddply(AllTrends10FA, "npID", function(x) AnalysePIT(x,Reverse=Reverse)) # this evaluates the PIT statistic analytically

# calculate the 95% CIs
PIT$uCI <- PIT$PIT + 1.96*PIT$sdPIT
PIT$uCI[PIT$uCI>100]<- 100
PIT$lCI <- PIT$PIT - 1.96*PIT$sdPIT
PIT$lCI[PIT$lCI<0]<- 0

# make a plot
graphics.off();x11();ggplot(PIT, aes(x=npID,y=PIT))+ylab("Proportion improved sites (%)")+ #+xlim(c(1994, 2014))
  geom_point(size=2) + #ggtitle("10-year LAWA trends")+  #+coord_cartesian(ylim=c(-30,30))+geom_hline(yintercept = 0,col="red") 
  geom_errorbar(data = PIT, aes(ymin=lCI, ymax=uCI), lwd=1, width=.1, position=position_dodge(0)) 

