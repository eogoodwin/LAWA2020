##########################################################
#
#  EXAMPLE usage of LWP-Trends functions
#
##########################################################

#This file provides three example applciations of the LWP-Trends code
#The three applications are for:
#   1. Monthly observations (a) Seasonal and (b) non-seasonal
#   2. Monthly observations with gaps (so analysed quarterly)
#   3. Annual Observations


# Date: Dec 2018
# By Ton Snelder and Caroline Fraser
##########################################################

rm(list = ls())                                               # clear the memory
##########################################################
# 0.  Prepration
##########################################################
# Read in required packages
##########################################################
require(dplyr)

##########################################################
# Read in functions and sample data
##########################################################

dir.code<-getwd() #UPDATE TO LOCAL DIRECTORY
setwd(dir.code)

source("LWPTrends_v1811.r")
load("LWPTrends_ExampleDataDec18.rdata")

#########################################################
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#         EXAMPLE 1a: Seasonal Monthly Observations
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#########################################################
#  Preliminary Setup
#########################################################
#Format myDate
WQData_Ex1a$myDate <- as.Date(as.character(WQData_Ex1a$sdate),"%Y-%m-%d")

#Add on seasons and extra date information
WQData_Ex1a<-GetMoreDateInfo(WQData_Ex1a)
WQData_Ex1a$Season<-WQData_Ex1a$Month
SeasonString<-levels(WQData_Ex1a$Season)

#Process censored values
NewValues <- RemoveAlphaDetect(WQData_Ex1a$Value)
WQData_Ex1a <- cbind.data.frame(WQData_Ex1a, NewValues)

summary(WQData_Ex1a) # note there are 40 censored values (Censored == TRUE)

#########################################################
# Inspect Data
#########################################################
#Make time series plot
x11(width=13,height=15);layout(matrix(c(1,1,2,3),2,2,byrow=TRUE)) #THis is to create three plots in one pane

InspectData(WQData_Ex1a, StartYear = 1997, EndYear = 2017, FlowtoUse = "finalQ",
                   plotType = "TimeSeries", main= "Ex 1a Time Series of Monthly Data")

#Make matrix plot of data values
InspectData(WQData_Ex1a, StartYear = 1997, EndYear = 2017, plotType = "Matrix",
                   PlotMat="RawData",main="Matrix of Values: monthly data")

#Make matrix plot demonstrating distribution of censored data
InspectData(WQData_Ex1a, StartYear = 1997, EndYear = 2017, plotType = "Matrix",
                   PlotMat="Censored",main="Matrix of censoring: monthly data")

##########################################################
# Seasonality Test on Raw Data
##########################################################
graphics.off(); x11(width=9,height=11);layout(matrix(c(1,2),2,1,byrow=TRUE)) #This is creates 2 plots in one pane
#Conduct a seasonlity test
SeasonalityTest(WQData_Ex1a,main="Example 1a: Raw Monthly Data")
#The raw data is strongly seasonal

##########################################################
# Perform Trend Tests on Raw Data
##########################################################

SeasonalTrendAnalysis(WQData_Ex1a,mymain="Ex 1a Raw Trend: Monthly Data",doPlot=T)

#Examine difference in the trend assessment if all values below the highest censored level 
#are set to the highest censored level (and assumed to be censored)
x11();SeasonalTrendAnalysis(WQData_Ex1a,mymain="Ex1a Raw Trend: Monthly Data - high censor filter",
                               doPlot=T, HiCensor=TRUE)

##########################################################
#  Flow Adjustment
##########################################################

x11();FlowAdjusted<-AdjustValues(WQData_Ex1a, method = c("Gam", "LogLog", "LOESS"), ValuesToAdjust = "RawValue", 
                         Covariate = "finalQ", Span = c(0.7), doPlot = T,plotpval=T)
head(FlowAdjusted)

#Select which flow adjustment to use (note, this could be different for every site/variable combination)
WQData_Ex1a<-merge(WQData_Ex1a,FlowAdjusted[,c("myDate","LOESS0.7")])

##########################################################
# Perform Trend Tests on Flow Adjusted Data
##########################################################
x11(width=9,height=11);layout(matrix(c(1,2),2,1,byrow=TRUE)) #THis is just to create 2 plots in one pane
#Check to see that the flow adjusted values are still seasonal
SeasonalityTest(WQData_Ex1a,ValuesToUse="LOESS0.7",main="Example 1a: Flow Adj. Monthly Data")

#the flow adjustment 
t(SeasonalTrendAnalysis(WQData_Ex1a,ValuesToUse="LOESS0.7",mymain="Example 1a: Flow Adjusted Trend: Monthly Data",doPlot=T))

#########################################################

#########################################################
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#         EXAMPLE 1b: NonSeasonal Monthly Observations
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#########################################################
#  Preliminary Setup
#########################################################
#Format myDate
WQData_Ex1b$myDate <- as.Date(as.character(WQData_Ex1b$sdate),"%Y-%m-%d")

#Add on seasons and extra date information - NOTE this dataset is based on Water Years
WQData_Ex1b<-GetMoreDateInfo(WQData_Ex1b,firstMonth = 7)
WQData_Ex1b$Season<-WQData_Ex1b$Month
SeasonString<-levels(WQData_Ex1b$Season)

#Process censored values
NewValues <- RemoveAlphaDetect(WQData_Ex1b$Value)
WQData_Ex1b <- cbind.data.frame(WQData_Ex1b, NewValues)

summary(WQData_Ex1b) # note there are no censored values (Censored == TRUE)

#########################################################
# Inspect Data
#########################################################
#Make time series plot
x11(width=13,height=15);layout(matrix(c(1,1,2,3),2,2,byrow=TRUE)) #THis is to create three plots in one pane

InspectData(WQData_Ex1b, StartYear = 1996, EndYear = 2017, Year="Year",
            plotType = "TimeSeries", main= "Ex 1b Time Series of Monthly Data")

#Make matrix plot of data values
InspectData(WQData_Ex1b, StartYear = 1997, EndYear = 2017, plotType = "Matrix", Year="CustomYear",
            PlotMat="RawData",main="Matrix of Values: monthly data")

#Make matrix plot demonstrating distribution of censored data
InspectData(WQData_Ex1b, StartYear = 1997, EndYear = 2017, plotType = "Matrix", Year="CustomYear",
            PlotMat="Censored",main="Matrix of censoring: monthly data")

##########################################################
# Seasonality Test on Raw Data
##########################################################
graphics.off(); x11(width=9,height=11);layout(matrix(c(1,2),2,1,byrow=TRUE)) #THis is just to create 2 plots in one pane
#Conduct a seasonlity test
SeasonalityTest(WQData_Ex1b,main="Example 1b: Raw Monthly Data")
#The raw data is NOT seasonal

##########################################################
# Perform Trend Tests on Raw Data
##########################################################
NonSeasonalTrendAnalysis(WQData_Ex1b,mymain="Ex 1b Raw Trend: Monthly Data",Year="CustomYear",doPlot=T)

##########################################################
#  Flow Adjustment
##########################################################
x11();FlowAdjusted<-AdjustValues(WQData_Ex1b, method = c("Gam", "LogLog", "LOESS"), ValuesToAdjust = "RawValue", 
                                 Covariate = "finalQ", Span = c(0.7, 0.9), doPlot = T,plotpval=T)
head(FlowAdjusted)

#All Flow-Concentration relationships are very weak, so have not proceeded with flow adjustment

#########################################################
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#         EXAMPLE 2: Limited Monthly Observations (Quarterly analysis)
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#########################################################
# Preliminary Setup
#########################################################
#Format myDate
WQData_Ex2$myDate <- as.Date(as.character(WQData_Ex2$sdate),"%Y-%m-%d")

#Add on seasons and extra date informaiton - NOTE, using a custom year from Jul-Jun
WQData_Ex2<-GetMoreDateInfo(WQData_Ex2,firstMonth = 7)
WQData_Ex2$Season<-WQData_Ex2$Qtr
SeasonString<-levels(WQData_Ex2$Season)

#Process censored values
NewValues <- RemoveAlphaDetect(WQData_Ex2$Value)
WQData_Ex2 <- cbind.data.frame(WQData_Ex2, NewValues)

#########################################################
# Inspect Data
#########################################################
#Make time series plot
x11(width=9,height=11);layout(matrix(c(1,1,2,3),2,2,byrow=TRUE)) #THis is to create three plots in one pane

InspectData(WQData_Ex2, StartYear = 1996, EndYear = 2017, 
            plotType = "TimeSeries", main= "Ex 2 Time Series of Quarterly Data")

#Make matrix plot of data values
InspectData(WQData_Ex2, StartYear = 1997, EndYear = 2017, Year = "CustomYear", plotType = "Matrix",
            PlotMat="RawData",main="Matrix of Values: Quarterly data")

#Make matrix plot demonstrating distribution of censored data
InspectData(WQData_Ex2, StartYear = 1997, EndYear = 2016, Year = "CustomYear",plotType = "Matrix",
            PlotMat="Censored",main="Matrix of censoring: Quarterly data")

##########################################################
# Seasonality Test on Raw Data
##########################################################
x11(width=9,height=11);layout(matrix(c(1,2),2,1,byrow=TRUE)) #THis is just to create 2 plots in one pane
#Conduct a seasonlity test
SeasonalityTest(WQData_Ex2,ValuestoUse="RawValue",main="Example 2: Raw Quarterly Data")
#The raw data is not seasonal

##########################################################
# Perform Trend Tests on Raw Data
##########################################################
NonSeasonalTrendAnalysis(WQData_Ex2,mymain="Ex 2 Raw Trend: Quarterly Data",
                         Year="CustomYear",doPlot=T)

##########################################################
#  Explore Flow Adjustment
##########################################################

x11();FlowAdjusted<-AdjustValues(WQData_Ex2, method = c("Gam", "LogLog", "LOESS"), ValuesToAdjust = "RawValue", 
                                 Covariate = "finalQ", Span = c(0.7, 0.9), doPlot = T,plotpval = TRUE)
#Select Loglog
#Select which flow adjustment to use (note, this could be different for every site/variable combination)
WQData_Ex2<-merge(WQData_Ex2,FlowAdjusted[,c("myDate","LogLog")])
names(WQData_Ex2)[names(WQData_Ex2)=="LogLog"]<-"FlowAdjusted"


##########################################################
# Perform Trend Tests on Flow Adjusted Data
##########################################################
x11(width=9,height=11);layout(matrix(c(1,2),2,1,byrow=TRUE)) #THis is just to create 2 plots in one pane
#Check to see that the flow adjusted values are still seasonal
SeasonalityTest(WQData_Ex2,ValuesToUse="FlowAdjusted",main="Example 2: Flow Adj. Quarterly Data")
#the flow adjusted data still non-seasonal
NonSeasonalTrendAnalysis(WQData_Ex2,ValuesToUse="FlowAdjusted",
                         mymain="Ex 2 Flow Adjusted Trend: Quarterly Data",Year="CustomYear",doPlot=T)

#########################################################

#########################################################
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#         EXAMPLE 3: Annual Data
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#########################################################
# Preliminary Setup
#########################################################
#Format myDate
WQData_Ex3$myDate <- as.Date(as.character(WQData_Ex3$sdate),"%Y-%m-%d")

#Add on seasons and extra date informaiton - NOTE, using a custom year from Jul-Jun
WQData_Ex3<-GetMoreDateInfo(WQData_Ex3,firstMonth = 7)
WQData_Ex3$Season<-factor(WQData_Ex3$CustomYear,levels=sort(unique(WQData_Ex3$CustomYear)))
SeasonString<-levels(WQData_Ex3$Season)

#Process censored values  (NOTE THERE AREN'T ACTUALLY ANY CENSORED VALUES IN THIS DATASET)
NewValues <- RemoveAlphaDetect(WQData_Ex3$Value)
WQData_Ex3 <- cbind.data.frame(WQData_Ex3, NewValues)

#########################################################
# Inspect Data
#########################################################
#Make time series plot
x11();InspectData(WQData_Ex3, StartYear = 1999, EndYear = 2017, Year = "CustomYear", 
            plotType = "TimeSeries", main= "Time Series of Annual Data")

#NOte: no matrix plots created as there aren't 2 dimensions
##########################################################
# Perform Trend Tests on Raw Data
##########################################################
x11();NonSeasonalTrendAnalysis(WQData_Ex3,mymain="Ex 2 Raw Trend: Annual Data",
                               Year="CustomYear",doPlot=T,legend.pos="bottom")


