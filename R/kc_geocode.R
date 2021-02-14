#' @title King County geocoder - find address candidates
#' 
#' @description \code{kc_geocode} geocodes a single address using KCGIS's ArcGIS
#' geocoder web service.
#' 
#' @details This function takes a single address and runs it through King County 
#' GIS's ArcGIS four-county geocoder web service, which covers King, Pierce, 
#' Snohomish, and Kitspap Counties. Locators in this service include street networks,
#' transit, andparcels, and ZIP codes.
#' Users can input either a single-line address (preferred as it seems to yield 
#' fewer false matches) or separate street, city, and ZIP values. 
#' Returns lon/lat (in EPSG 2926 or NAD_1983_HARN_StatePlane_Washington_North_FIPS_4601_Feet 
#' projection), score (accuracy of match for that locator), locName (name of the 
#' locator that returned the result), matchAddr (complete address returned), and 
#' addressType (match level for the result).
#' 
#' @param singleline A concatenated address that consists of street, city, and ZIP
#' in a single field
#' @param street The street portion of an address (if specifying address separately)
#' @param city The city portion of an address (if specifying address separately)
#' @param zip The ZIP portion of an address (if specifying address separately)
#' @param max_return The maximum number of matches to return (default is 10)
#' @param best_result Whether or not to return the single best result for the 
#' input. Default hierarchy is PointAddress, StreetAddress, then Postal. Multiple 
#' options within an address type are decided by score.
#'
#' @examples
#' \dontrun{
#' kc_geocode(singleline = "401 5TH AVE, SEATTLE, 98105", best_result = F)
#' kc_geocode(street = "401 5TH AVE", city = "SEATTLE", zip = "98105")
#' }
#' 
#' @importFrom rlang .data
#' 
#' @export

# CODE ADAPTED FROM HERE: https://github.com/cengel/ArcGIS_geocoding/blob/master/SUL_gcFunctions.R
#
##################################
## Single Line Geocode Function ##
##################################
# The function takes one address at a time as ONE of the following:
# - one string (singleline) (if this is non-null, used by default)
# - multiple address components (street, city, zip)
#  Other options:
# - max_return = limit the number of possible matches (most addresses return <=5 anyway)
# - best_result = return only the best possible match
#
# The function returns:
# lon, lat -    The primary x/y coordinates of the address returned by the geocoding service. 
#               Has the NAD_1983_HARN_StatePlane_Washington_North_FIPS_4601_Feet projection. 
#               Can use this proj4string:
#               +proj=lcc +lat_1=47.5 +lat_2=48.73333333333333 +lat_0=47 +lon_0=-120.8333333333333 
#               +x_0=500000.0000000001 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=us-ft +no_defs
# score -       The accuracy of the address match between 0 and 100.
#               Note that ZIP centroid matches have a score of 100.
# locName -     Geolocator used to match the address/
# matchAddr -   Complete address returned for the geocode request.
# addressType - The match level for a geocode request. "PointAddress" is typically the 
#               most spatially accurate match level. "StreetAddress" differs from PointAddress 
#               because the house number is interpolated from a range of numbers. "StreetName" is similar,
#               but without the house number.

kc_geocode <- function(singleline = NULL, 
                       street = NULL, 
                       city = NULL, 
                       zip = NULL,
                       max_return = 10, 
                       best_result = T) {
  
  # Determine whether to use singleline or not
  if (!is.null(singleline)) {
    single <- TRUE
  } else {
    single <- FALSE
  }
  
  # Set up constants (note, server being moved to production so this will be updated)
  server <- "https://api.kingcounty.gov/gis/v1/4co/findAddressCandidates"
  
  # Create URL and record of input address
  if (single == T) {
    url <- utils::URLencode(paste0(server, "?singleLine=", singleline, "&maxLocations=", max_return, "&f=json&outFields=*"))
    input_text <- singleline
  } else {
    url <- utils::URLencode(paste0(server, "?Street=", street, "&City=", city, "&ZIP=", zip,
                                   "&maxLocations=", max_return, "&f=json&outFields=*"))
    input_text <- paste(street, city, zip, sep = ", ")
  }

  # Submit to REST API
  rawdata <- httr::GET(url)
  
  # Parse JSON
  result <- httr::content(rawdata, "parsed", "application/json")
  
  # Pull out all results if they exist
  if (length(result$candidates) >= 1) {
    result_parsed <- dplyr::bind_rows(lapply(seq(1:length(result$candidates)), function(x) {
      
      output <- data.frame(lon = as.numeric(result[["candidates"]][[x]][["location"]][["x"]]),
                           lat = as.numeric(result[["candidates"]][[x]][["location"]][["y"]]),
                           score = result[["candidates"]][[x]][["score"]], 
                           locName = result[["candidates"]][[x]][["attributes"]][["Loc_name"]],
                           #status = result[["candidates"]][[x]][["attributes"]][["Status"]],
                           matchAddr = result[["candidates"]][[x]][["attributes"]][["Match_addr"]],
                           addressType = result[["candidates"]][[x]][["attributes"]][["Addr_type"]],
                           stringsAsFactors = F)
    }))
    
    # Pull out the best address if desired
    if (best_result == T) {
      if (nrow(result_parsed[result_parsed$addressType == "PointAddress", ]) >= 1) {
        result_parsed <- dplyr::filter(result_parsed, addressType == "PointAddress")
      } else if (nrow(result_parsed[result_parsed$addressType == "StreetAddress", ]) >= 1) {
        result_parsed <- dplyr::filter(result_parsed, addressType == "StreetAddress")
      } else if (nrow(result_parsed[result_parsed$addressType == "Postal", ]) >= 1) {
        result_parsed <- dplyr::filter(result_parsed, addressType == "Postal")
      }
      
      result_parsed <- dplyr::arrange(result_parsed, -score)
      result_parsed <- dplyr::slice(result_parsed, 1)
    }
    
    # Otherwise return all results
    result_parsed <- cbind(input_addr = rep(input_text, nrow(result_parsed)),
                           result_parsed, stringsAsFactors = F)
  } else { # If no results, just return input address
    result_parsed <- data.frame(input_addr = input_text,
                                lon = NA_real_, lat = NA_real_, score = NA_real_,
                                locName = NA_character_, matchAddr = NA_character_, 
                                addressType = NA_character_,
                                stringsAsFactors = F)
  }
  
  return(result_parsed)
}



#' @title King County geocoder - batch geocoder
#' 
#' @description \code{kc_geocode_batch} geocodes multiple addresses using KCGIS's ArcGIS
#' geocoder web service.
#' 
#' @details This function takes up to 1,000 addresses and runs them through King County 
#' GIS's ArcGIS four-county geocoder web service, which covers King, Pierce, 
#' Snohomish, and Kitspap Counties. Locators in this service include street networks,
#' transit, andparcels, and ZIP codes.
#' Users can input either a single-line address (preferred as it seems to yield 
#' fewer false matches) or separate street, city, and ZIP values. 
#' Returns lon/lat (in EPSG 2926 or NAD_1983_HARN_StatePlane_Washington_North_FIPS_4601_Feet 
#' projection), score (accuracy of match for that locator), locName (name of the 
#' locator that returned the result), matchAddr (complete address returned), and 
#' addressType (match level for the result).
#' 
#' @param id A unique ID that can be used to link results to original input 
#' (required, must be numeric).
#' @inheritParams kc_geocode
#'
#' @examples
#' \dontrun{
#' kc_geocode_batch(id = 1:3,
#' singleline = c("401 5TH AVE, SEATTLE, 98105", 
#' "14350 SE EASTGATE WAY, BELLEVUE, 98007",
#' "33431 13TH PLACE S, FEDERAL WAY, 98003")
#' }
#' 
#' @importFrom rlang .data
#' 
#' @export


#######################################
## Multi Line Batch Geocode Function ##
#######################################
# The function takes:
# - ID variable to identify records, must be numeric and should be unique
# - one string (singleline) (if this is non-null, used by default)
# - multiple address components (street, city, zip)

#
# It can take a maximum of 1000 adresses. If more, it returns an error.
#
# The function returns a data frame with the following fields:
# ID -          Result ID can be used to join the output fields in the response to the attributes 
#               in the original address table.
# lon, lat -    The primary x/y coordinates of the address returned by the geocoding service in WGS84 
# score -       The accuracy of the address match between 0 and 100.
# locName -     The component locator used to return a particular match result
# status -      Whether a batch geocode request results in a match (M), tie (T), or unmatch (U)
# matchAddr -   Complete address returned for the geocode request.
# addressType - The match level for a geocode request. "PointAddress" is typically the 
#               most spatially accurate match level. "StreetAddress" differs from PointAddress 
#               because the house number is interpolated from a range of numbers. "StreetName" is similar,
#               but without the house number.

kc_geocode_batch <- function(id, 
                             singleline = NULL, 
                             street = NULL, 
                             city = NULL,  
                             zip = NULL) {
  
  # Determine whether to use singleline or not
  if (!is.null(singleline)) {
    single <- TRUE
  } else {
    single <- FALSE
  }
  
  # Set up constants 
  server <- "https://api.kingcounty.gov/gis/v1/4co/geocodeAddresses"
  
  # check if we have more than 1000, if so stop.
  if (length(id) > 1000){
    message("length is: ", length(id))
    stop("Can only process up to 1000 addresses at a time.")}
  
  # check if id is numeric, either a real number or a string number
  if(!all(grepl("^[0-9]{1,}$", id))){ # HT: https://stackoverflow.com/a/48954452/2630957
    #if (!is.numeric(id)) {
    stop("id variable needs to be a number")
  }
  
  # Make input data frame
  if (single == T) {
    input_df <- data.frame(OBJECTID = id,  # we need the id to be called OBJECTID
                           SingleLine = singleline)
  } else {
    input_df <- data.frame(OBJECTID = id,  # we need the id to be called OBJECTID
                           Street = street,
                           City = city,
                           Zip = zip)
    
    # Set missing ZIP codes to empty strings
    input_df$Zip <- ifelse(is.na(input_df$Zip), '', as.character(input_df$Zip))
  }
  
  
  # Make JSON
  tmp_list <- apply(input_df, 1, function(i) list(attributes = as.list(i)))
  
  # need to coerce OBJECTID to numeric
  tmp_list <- lapply(tmp_list, function(i) { 
    i$attributes$OBJECTID <- as.numeric(i$attributes$OBJECTID); 
    i})
  
  adr_json <- rjson::toJSON(list(records = tmp_list))
  adr_json_enc <- utils::URLencode(adr_json, reserved = TRUE)
  
  # Submit
  req <- httr::POST(
    url = server, 
    body = list(addresses = adr_json, f = "json"),
    #body = list(addresses = adr_json_enc, f = "json"),
    encode = "form")
  
  # Error check
  httr::stop_for_status(req)
  
  # Process and parse
  res <- httr::content(req, "parsed", "application/json")
  
  if ("error" %in% names(res)) {
    message("Could not resolve error, try again later")
  }
  
  output_df <- data.frame()
  for (i in seq_len(length(res$locations))){
    d <- with(res$locations[[i]], {data.frame(ID = attributes$ResultID,
                                              lon = as.numeric(location$x),
                                              lat = as.numeric(location$y),
                                              score = score, 
                                              locName = attributes$Loc_name,
                                              status = attributes$Status,
                                              matchAddr = attributes$Match_addr,
                                              #side = attributes$Side,
                                              addressType = attributes$Addr_type)})
    output_df <- rbind(output_df, d)
  }
  
  return(output_df)
}
