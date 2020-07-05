#Investigation into searching the xmlfile for evidence of QA/QC codes.
#XML file converted to R list first:

WQList=XML::xmlToList(xmlfile)
phosList=XML::xmlToList(xmlfile)

#Then to search a single element of that large list, you'd:

invisible(sapply(
  names(sapply(phosList$Measurement$Data[[30]], names)),
  FUN = function(x)
    if('Name' %in% names(phosList$Measurement$Data[[30]][[x]])) {
      cat(phosList$Measurement$Data[[30]][[x]]['Name'], '\n')
    }
))

#But no, because if a name is reused, you only ever get the first one, so you can't do it by name.
invisible(sapply(
  seq_len(length(phosList$Measurement$Data[[30]])),
  FUN = function(x)
    if('Name' %in% names(phosList$Measurement$Data[[30]][[x]])) {
      cat(phosList$Measurement$Data[[30]][[x]]['Name'], '\n')
    }
))

#Right, that's better  Can that not be simplified though?
invisible(sapply(phosList$Measurement$Data[[30]],FUN=function(x){
  if('Name'%in%names(x)){
    cat(x[['Name']],'\n')
  }
}))


#But I want to search each element of that large list
#So we need another level of recursion.
invisible(sapply(phosList$Measurement$Data,FUN=function(y){
  sapply(y,FUN=function(x){
    if('Name'%in%names(x)){
      cat(x[['Name']],'\n')
    }
  })
}))

# and, rather than print them out, can we pack them into a vector?
listnames = sort(unique(unlist(invisible(
  sapply(
    phosList$Measurement$Data,
    FUN = function(y) {
      sapply(
        y,
        FUN = function(x) {
          if ('Name' %in% names(x)) {
            x[['Name']]
          }
        }
      )
    }
  )
))))
# [1] "Analysts Comments"      "AnalyteCode"            "AnalyteName"            "Archived"               "Cost"                  
# [6] "Cost Code"              "CouncilSampleID"        "Detection Limit"        "Field Technician"       "Lab"                   
# [11] "Lab Method"             "Lab Sample ID"          "LabsTestMethod"         "LabTestID"              "Lawa Type"             
# [16] "Lower Confidence Limit" "Meter Number"           "Method"                 "MethodText"             "Project"               
# [21] "Rain"                   "Rain Previously"        "Result Value"           "Run Number"             "Sample Comment"        
# [26] "Sample Frequency"       "Standard Uncertainty"   "TestMethodName"         "Upper Confidence Limit" "Water Clarity"         
# [31] "Water Colour"           "Wind Direction"         "Wind Strength"     


#And to get all unique values of one of those types,
sort(unique(unlist(invisible(
  sapply(
    phosList$Measurement$Data,
    FUN = function(y) {
      sapply(
        y,
        FUN = function(x) {
          if ('Name' %in% names(x)) {
            if (x[['Name']] == "Analysts Comments") {
              x[['Value']]
            }
          }
        }
      )
    }
  )
))))

#Or all values in the xml
sort(unique(unlist(invisible(
  sapply(
    phosList$Measurement$Data,
    FUN = function(y) {
      sapply(
        y,
        FUN = function(x) {
          if ('Name' %in% names(x)) {
            x[['Value']]
          }
        }
      )
    }
  )
))))









#HRC Version
#Then to search a single element of that large list, you'd:

invisible(sapply(
  names(sapply(datAsList$
                 observationMember$
                 OM_Observation$
                 result$
                 MeasurementTimeseries$
                 defaultPointMetadata$
                 DefaultTVPMeasurementMetadata, names)),
  FUN = function(x)
    if('title' %in% names(datAsList$observationMember$OM_Observation$result$MeasurementTimeseries$defaultPointMetadata$DefaultTVPMeasurementMetadata[[x]])) {
             cat(datAsList$observationMember$
             OM_Observation$result$MeasurementTimeseries$
             defaultPointMetadata$DefaultTVPMeasurementMetadata[[x]][['title']], '\n')
    }
))

#But no, because if a name is reused, you only ever get the first one, so you can't do it by name.
invisible(sapply(
  seq_len(length(datAsList$observationMember$OM_Observation$result$MeasurementTimeseries$defaultPointMetadata$DefaultTVPMeasurementMetadata)),
  FUN = function(x)
    if('title' %in% names(datAsList$observationMember$OM_Observation$result$MeasurementTimeseries$defaultPointMetadata$DefaultTVPMeasurementMetadata[[x]])) {
      cat(datAsList$observationMember$OM_Observation$result$MeasurementTimeseries$defaultPointMetadata$DefaultTVPMeasurementMetadata[[x]]['title'], '\n')
    }
))

#Right, that's better  Can that not be simplified though?
invisible(sapply(datAsList$observationMember$OM_Observation$result$MeasurementTimeseries$defaultPointMetadata$DefaultTVPMeasurementMetadata,FUN=function(x){
  if('title'%in%names(x)){
    cat(x[['title']],'\n')
  }
}))


#But I want to search each element of that large list
#So we need another level of recursion.
invisible(sapply(datAsList$observationMember$OM_Observation,FUN=function(y){
  sapply(y,FUN=function(x){
    if('title'%in%names(x)){
      cat(x[['title']],'\n')
    }
  })
}))

# and, rather than print them out, can we pack them into a vector?
listnames = sort(unique(unlist(invisible(
  sapply(datAsList$observationMember$OM_Observation,FUN = function(y) {
      sapply(y,FUN = function(x) {
          if ('title' %in% names(x)) {
            x[['title']]
          }
        })
    })
))))
# [1] "Analysts Comments"      "AnalyteCode"            "AnalyteName"            "Archived"               "Cost"                  
# [6] "Cost Code"              "CouncilSampleID"        "Detection Limit"        "Field Technician"       "Lab"                   
# [11] "Lab Method"             "Lab Sample ID"          "LabsTestMethod"         "LabTestID"              "Lawa Type"             
# [16] "Lower Confidence Limit" "Meter Number"           "Method"                 "MethodText"             "Project"               
# [21] "Rain"                   "Rain Previously"        "Result Value"           "Run Number"             "Sample Comment"        
# [26] "Sample Frequency"       "Standard Uncertainty"   "TestMethodName"         "Upper Confidence Limit" "Water Clarity"         
# [31] "Water Colour"           "Wind Direction"         "Wind Strength"     


#And to get all unique values of one of those types,
sort(unique(unlist(invisible(
  sapply(
    datAsList$observationMember$OM_Observation,
    FUN = function(y) {
      sapply(
        y,
        FUN = function(x) {
          if ('Name' %in% names(x)) {
            if (x[['Name']] == "Analysts Comments") {
              x[['Value']]
            }
          }
        }
      )
    }
  )
))))

#Or all values in the xml
sort(unique(unlist(invisible(
  sapply(
    datAsList$observationMember$OM_Observation,
    FUN = function(y) {
      sapply(
        y,
        FUN = function(x) {
          if ('Name' %in% names(x)) {
            x[['Value']]
          }
        }
      )
    }
  )
))))