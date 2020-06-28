#Investigation into searching the xmlfile for evidence of QA/QC codes.
#XML file converted to R list first:

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