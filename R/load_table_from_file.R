#' @title Load a data file to a SQL table
#' 
#' @description \code{load_table_from_file} loads a file data to a SQL table using specified variables or a YAML config file.
#' 
#' @details This function loads a data file to an already existing SQL table using 
#' specified variables or a YAML configuration file. The function is essentially a 
#' wrapper for the SQL bulk copy program (BCP) utility using integrated authorization. 
#' Users can specify some input functions (e.g., to_table) and rely on the config file 
#' for the rest of the necessary information. 
#' For all arguments that could be specified or come from a YAML file, the hierarchy is 
#' specified > argument under server in YAML > argument not under server in YAML. 
#' Note that arguments that should not vary between servers (e.g., row_terminator) 
#' should not be listed under a server in the YAML file.
#' 
#' ## Example YAML file with no server or individual years
#' (Assume the indentation is appropriate)
#' ```
#' to_schema: raw
#' to_table: mcaid_elig
#' *optional other components like a qa_schema and qa_table, index name, vars, etc.*
#' server_path: KCITSQLABCD51
#' db_name: PHClaims
#' file_path: //path123/importdata/Data/kc_elig_20210519.txt
#' field_term: \t
#' row_term: \n
#' ```
#' 
#' ## Example YAML file with servers (phclaims, hhsaw) and individual years
#' (Assume the indentation is appropriate)
#' ```
#' phclaims:
#'     to_schema: raw
#'     to_table: mcaid_elig
#'     server_path: KCITSQLABCD51
#'     db_name: PHClaims
#' hhsaw:
#'     to_schema: raw
#'     to_table: mciad_elig
#'     server_path: kcitazxyz20.database.windows.net
#'     db_name: hhs_analytics_workspace
#' *optional other components like a qa_schema and qa_table, index name, vars, etc.*
#' field_term: \t
#' row_term: \n
#' years:
#'     2014
#'     2015
#'     2016
#' 2014:
#'     file_path: //path123/importdata/Data/kc_elig_2014.txt
#'     field_term: \|
#'     row_term: \r
#' 2015:
#'     file_path: //path123/importdata/Data/kc_elig_2015.txt  
#' 2016:
#'     file_path: //path123/importdata/Data/kc_elig_2016.txt
#'     field_term: \0
#'     row_term: \0
#' ````
#' 
#' @param conn SQL server connection created using \code{odbc} package.
#' @param server Name of server being used (only applies if using a YAML file). 
#' Useful if the same table is loaded to multiple servers but with different names 
#' or schema. Note that this is different from the *server_path* argument that is 
#' used as part of the BCP command; the *server* argument can be any name a user 
#' wants whereas *server_path* must be the actual server name.
#' @param overall Load a single table instead of a table for each calendar year. 
#' Mutually exclusive with *ind_yr* option. Default is TRUE.
#' @param ind_yr Load multiple tables, one for each calendar year, with a year suffix 
#' on each table name (e.g., mcaid_elig_2014). Mutually exclusive with *overall* option. 
#' If using this option, the list of years should be provided via the *years* argument or 
#' a *years* variable in the YAML file. Default is FALSE.
#' @param years Vector of individual years to make tables for (if not using YAML input).
#' @param combine_yr Union year-specific files into a single table. Only applies 
#' if ind_yr = T. Default is FALSE.
#' @param config Name of object in global environment that contains configuration 
#' information. Use one of `config`, `config_url`, or `config_file`. 
#' Should be in a YAML format with at least the following variables: 
#' *to_schema*, *to_table*, *server_path*, *db_name*, and *file_path*, with possibly 
#' *field_term*, *row_term*, and *first_row*. 
#' *to_schema* and *to_table*, *server_path*, and *db_name* should all be nested under 
#' the server name if applicable, other variables should not (but might be nested 
#' under a calendar year).
#' @param config_url URL of a YAML config file. Use one of `config`, `config_url`, or 
#' `config_file`. Note the requirements under `config`.
#' @param config_file File path of a YAML config file. Use one of `config`, `config_url`, or 
#' `config_file`. Note the requirements under `config`.
#' @param to_schema Name of the schema to apply the index to (if not using YAML input).
#' @param to_table Name of the table to apply the index to (if not using YAML input).
#' @param server_path Name of the SQL server to connect to (if not using YAML input).
#' @param db_name Name of the database to use (if not using YAML input).
#' @param file_path File path of data to be loaded (if not using YAML input). If 
#' ind_yr = T, this should be a named vector with the format 
#' *c("2014" = "//path1/folder1/file1.ext", "2015" = "//path1/folder1/file2.ext")* 
#' where the name matches the years to load.
#' @param field_term Field terminator in the data (if not using YAML input). If 
#' using ind_yr = T and the terminator differs between calendar years, this should 
#' be a named vector with the format *c("overall" = "\t", "2014" = "\|", "2016" = "\0")* 
#' where "overall" supplies the default terminator and the other names match the years 
#' that differ. Do not use a named vector if overall = T or if there is no variation 
#' between years. The BCP default is \\t.
#' @param row_term Row terminator in the data (if not using YAML input). If 
#' using ind_yr = T and the terminator differs between calendar years, this should 
#' be a named vector with the format *c("overall" = "\n", "2014" = "\r", "2016" = "\0")* 
#' where "overall" supplies the default terminator and the other names match the years 
#' that differ. Do not use a named vector if overall = T or if there is no variation 
#' between years. The BCP default is \\n.
#' @param first_row Row number of the first line of data (if not using YAML input). 
#' Default is 2 (assumes a header row). Currently this must be the same for all years.
#' @param truncate Truncate existing table prior to loading. Default is TRUE. 
#' @param drop_index Drop any existing indices prior to loading data. This can speed 
#' loading times substantially. Use \code{add_index} to restore the index after. Default is TRUE.
#' @param test_schema Write to a temporary/development schema when testing out table creation. 
#' Will use the to_schema (specified or in the YAML file) to make a new table name of  
#' {to_schema}_{to_table}. Schema must already exist in the database. Most useful 
#' when the user has an existing YAML file and does not want to overwrite it. 
#' Only 1,000 rows will be loaded to each table. Default is NULL.
#'
#' @examples
#' \dontrun{
#' load_table(conn = db_claims, server = "hhsaw", config = load_config)
#' load_table(conn = db_claims, server = "phclaims", 
#' config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/load_mcaid_raw.R",
#' overall = F, ind_yr = T)
#' }
#' 
#' @export
#' @md

load_table_from_file <- function(conn,
                                 server = NULL,
                                 overall = T,
                                 ind_yr = F,
                                 years = NULL,
                                 combine_yr = F,
                                 config = NULL,
                                 config_url = NULL,
                                 config_file = NULL,
                                 to_schema = NULL,
                                 to_table = NULL,
                                 server_path = NULL,
                                 db_name = NULL,
                                 file_path = NULL,
                                 field_term = NULL,
                                 row_term = NULL,
                                 first_row = 2,
                                 truncate = T,
                                 drop_index = T,
                                 test_schema = NULL) {
  
  
  # INITIAL ERROR CHECK ----
  # Check if the config provided is a local file or on a webpage
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either alocal config object, config_url, or config_file but only one")
  }
  
  # Check that the yaml config file exists in the right format
  if (!is.null(config_file)) {
    # Check that the yaml config file exists in the right format
    if (file.exists(config_file) == F) {
      stop("Config file does not exist, check file name")
    }
    
    if (configr::is.yaml.file(config_file) == F) {
      stop(glue("Config file is not a YAML config file. ", 
                "Check there are no duplicate variables listed"))
    }
  }
  
  # Check that something will be run (but not both things)
  if (overall == F & ind_yr == F) {
    stop("At least one of 'overall and 'ind_yr' must be set to TRUE")
  }
  
  if (overall == T & ind_yr == T) {
    stop("Only one of 'overall and 'ind_yr' can be set to TRUE")
  }
  
  
  # READ IN CONFIG FILE ----
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }

  # Make sure a valid URL was found
  if ('404' %in% names(table_config)) {
    stop("Invalid URL for YAML file")
  }
  
  
  # VARIABLES ----
  ## to_schema ----
  if (is.null(to_schema)) {
    if (!is.null(table_config[[server]][["to_schema"]])) {
      to_schema <- table_config[[server]][["to_schema"]]
    } else if (!is.null(table_config$to_schema)) {
      to_schema <- table_config$to_schema
    }
  }
  
  ## to_table ----
  if (is.null(to_table)) {
    if (!is.null(table_config[[server]][["to_table"]])) {
      to_table <- table_config[[server]][["to_table"]]
    } else if (!is.null(table_config$to_table)) {
      to_table <- table_config$to_table
    }
  }
  
  
  ## server_path ----
  if (is.null(server_path)) {
    if (!is.null(table_config[[server]][["server_path"]])) {
      server_path <- table_config[[server]][["server_path"]]
    } else if (!is.null(table_config$server_path)) {
      server_path <- table_config$server_path
    }
  }
  
  ## db_name ----
  if (is.null(db_name)) {
    if (!is.null(table_config[[server]][["db_name"]])) {
      db_name <- table_config[[server]][["db_name"]]
    } else if (!is.null(table_config$db_name)) {
      db_name <- table_config$db_name
    }
  }


  if (ind_yr == T & combine_yr == T) {
    # Use unique in case variables are repeated
    #combine_years <- as.list(sort(unique(table_config$combine_years)))
    combine_years <- as.list(sort(unique(table_config$years)))
  }
  
  
  # TEST MODE ----
  # Alert users they are in test mode
  if (!is.null(test_schema)) {
    message("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO ", toupper(test_schema), " SCHEMA")
    test_msg <- " (function is in test mode, only 1,000 rows will be loaded)"
    to_table <- glue::glue("{to_schema}_{to_table}")
    to_schema <- test_schema
    load_rows <- " -L 1001 "
  } else {
    test_msg <- ""
    load_rows <- ""
  }
  
  
  # SET UP A FUNCTION FOR COMMON ACTIONS ----
  # Both the overall load and year-specific loads use a similar set of code
  loading_process <- function(conn_inner = conn,
                              to_schema_inner = to_schema,
                              to_table_inner = to_table,
                              server_path_inner = server_path,
                              db_name_inner = db_name,
                              file_path_inner = file_path,
                              field_term_inner = field_term,
                              row_term_inner = row_term,
                              first_row_inner = first_row,
                              load_rows_inner = load_rows,
                              truncate_inner = truncate,
                              drop_index_inner = drop_index,
                              test_msg_inner = test_msg) {
    
    # Add message to user
    message(glue('Loading [{to_schema_inner}].[{to_table_inner}] table(s) ',
                 ' from {file_path_inner} {test_msg_inner}'))
    
    ## Truncate existing table if desired ----
    if (truncate_inner == T) {
      DBI::dbExecute(conn_inner, 
                     glue::glue_sql("TRUNCATE TABLE {`to_schema_inner`}.{`to_table_inner`}", 
                                    .con = conn_inner))
    }
    
    ## Remove existing index if desired (and an index exists) ----
    if (drop_index_inner == T) {
      # This code pulls out the index name
      existing_index <- DBI::dbGetQuery(conn_inner, 
                                        glue::glue_sql("SELECT DISTINCT a.index_name
                     FROM
                     (SELECT ind.name AS index_name
                       FROM
                       (SELECT object_id, name, type_desc FROM sys.indexes
                         WHERE type_desc LIKE 'CLUSTERED%') ind
                       INNER JOIN
                       (SELECT name, schema_id, object_id FROM sys.tables
                         WHERE name = {to_table_inner}) t
                       ON ind.object_id = t.object_id
                       INNER JOIN
                       (SELECT name, schema_id FROM sys.schemas
                         WHERE name = {to_schema_inner}) s
                       ON t.schema_id = s.schema_id) a", 
                                                       .con = conn_inner))
      
      if (nrow(existing_index) != 0) {
        lapply(seq_along(existing_index), function(i) {
          DBI::dbExecute(conn_inner,
                         glue::glue_sql("DROP INDEX {`existing_index[['index_name']][[i]]`} 
                                        ON {`to_schema_inner`}.{`to_table_inner`}", 
                                        .con = conn_inner))
        })
      }
    }
    
    ## Pull out parameters for BCP load ----
    if (!is.null(field_term_inner)) {
      field_term <- paste0("-t ", field_term_inner)
    } else {
      field_term <- ""
    }
    
    if (!is.null(row_term_inner)) {
      row_term <- paste0("-r ", row_term_inner)
    } else {
      row_term <- ""
    }
    
    
    
    ## Set up BCP arguments and run BCP ----
    bcp_args <- c(glue(' {to_schema_inner}.{to_table_inner} IN ', 
                       ' "{file_path_inner}" -d {db_name_inner} ',
                       ' {field_term} {row_term} -C 65001 -F {first_row_inner} ',
                       ' -S {server_path_inner} -T -b 100000 {load_rows_inner} -c '))
    
    print(bcp_args)
    system2(command = "bcp", args = c(bcp_args))
  }
  
  
  # OVERALL TABLE ----
  if (overall == T) {
    ## Pull out table-specific variables ----
    ### file_path ----
    if (is.null(file_path)) {
      if (!is.null(server)) {
        file_path <- table_config[[server]][["file_path"]]
      } else if (!is.null(table_config$file_path)) {
        file_path <- table_config$file_path
      }
    }
    
    ### field_term ----
    if (is.null(field_term)) {
      if (!is.null(table_config[[server]][["field_term"]])) {
        field_term <- table_config[[server]][["field_term"]]
      } else if (!is.null(table_config$field_term)) {
        field_term <- table_config$field_term
      }
    }
    
    ### row_term ----
    if (is.null(row_term)) {
      if (!is.null(table_config[[server]][["row_term"]])) {
        row_term <- table_config[[server]][["row_term"]]
      } else if (!is.null(table_config$row_term)) {
        row_term <- table_config$row_term
      }
    }
    
    ### first_row ----
    # Order is a bit different because a default is provided
    if (!is.null(table_config[[server]][["first_row"]])) {
      first_row <- table_config[[server]][["first_row"]]
    } else if (!is.null(table_config$first_row)) {
      first_row <- table_config$first_row
    }
    
    
    ## Run loading function, can use defaults for everything ----
    loading_process()
  }
  
  
  # CALENDAR YEAR TABLES ----
  ### NB Need to redo this section to work with servers
  # Not currently an issue since partial loads don't use the individual years piece
  
  if (ind_yr == T) {
    # Use unique in case years are repeated
    if (!is.null(years)) {
      years <- sort(unique(years))
    } else {
      years <- sort(unique(table_config$years))
    }
    
    message(glue::glue("Loading calendar year [{to_schema}].[{to_table}] tables", test_msg))
    
    lapply(years, function(x) {
      ## Pull out table-specific variables ----
      ### to_table ----
      to_table <- paste0(to_table, "_", x)
      
      ### file_path ----
      if (!is.null(file_path)) {
        file_path <- file_path[[x]]
      } else if ("file_path" %in% names(table_config[[x]])) {
        file_path <- table_config[[x]][["file_path"]]
      } else {
        warning("No file name supplied for CY ", x, ". 
                Specify in function arguments or the YAML file (see examples in ?load_table_from_file).")
      }
      
      ### field_term ----
      if (x %in% names(field_term)) {
        field_term <- field_term[[x]]
      } else if ("overall" %in% names(field_term)) {
        field_term <- field_term[["overall"]]
      } else if (!is.null(table_config[[x]][["field_term"]])) {
        field_term <- table_config[[x]][["field_term"]]
      } else if (!is.null(table_config$field_term)) {
        field_term <- table_config$field_term
      } else {
        field_term <- NULL
      }
      
      ### row_term ----
      if (x %in% names(row_term)) {
        row_term <- row_term[[x]]
      } else if ("overall" %in% names(row_term)) {
        row_term <- row_term[["overall"]]
      } else if (!is.null(table_config[[x]][["row_term"]])) {
        row_term <- table_config[[x]][["row_term"]]
      } else if (!is.null(table_config$row_term)) {
        row_term <- table_config$row_term
      } else {
        row_term <- NULL
      }
      
      ### first_row ----
      # Order is a bit different because a default is provided
      if (!is.null(table_config[[x]][["first_row"]])) {
        first_row <- table_config[[x]][["first_row"]]
      } else if (!is.null(table_config$first_row)) {
        first_row <- table_config$first_row
      }
      
      
      ## Run loading function, can use defaults for everything ----
      loading_process()
    })
  }
  
  
  # COMBINE INDIVIDUAL YEARS ----
  if (combine_yr == T) {
    message("Combining years into a single table")
    if (truncate == T) {
      # Remove data from existing combined table if desired
      dbGetQuery(conn, glue::glue_sql("TRUNCATE TABLE {`to_schema`}.{`table_name`}", 
                                      .con = conn))
    }
    
    if (add_index == T) {
      # Remove index from combined table if it exists
      # This code pulls out the clustered index name
      index_name <- dbGetQuery(conn, 
                               glue::glue_sql("SELECT DISTINCT a.index_name
                                                FROM
                                                (SELECT ind.name AS index_name
                                                  FROM
                                                  (SELECT object_id, name, type_desc FROM sys.indexes
                                                    WHERE type_desc LIKE 'CLUSTERED%') ind
                                                  INNER JOIN
                                                  (SELECT name, schema_id, object_id FROM sys.tables
                                                    WHERE name = {`table`}) t
                                                  ON ind.object_id = t.object_id
                                                  INNER JOIN
                                                  (SELECT name, schema_id FROM sys.schemas
                                                    WHERE name = {`schema`}) s
                                                  ON t.schema_id = s.schema_id) a",
                                              .con = conn,
                                              table = dbQuoteString(conn, table_name),
                                              schema = dbQuoteString(conn, to_schema)))[[1]]
      
      if (length(index_name) != 0) {
        dbGetQuery(conn_inner,
                   glue::glue_sql("DROP INDEX {`index_name`} ON 
                                  {`to_schema`}.{`table_name`}", .con = conn))
      }
    }
    
    
    # Need to find all the columns that only exist in some years
    # First find common variables
    # Set up to work with old and new YAML config styles
    if (!is.null(names(table_config$vars))) {
      all_vars <- unlist(names(table_config$vars))
    } else {
      all_vars <- unlist(table_config$vars)  
    }
    
    # Now find year-specific ones and add to main list
    lapply(combine_years, function(x) {
      table_name_new <- paste0("table_", x)
      add_vars_name <- paste0("vars_", x)
      
      if (!is.null(names(table_config$vars))) {
        all_vars <<- c(all_vars, unlist(names(table_config[[table_name_new]][[add_vars_name]])))
      } else {
        all_vars <<- c(all_vars, unlist(table_config[[table_name_new]][[add_vars_name]]))
      }
    })
    # Make sure there are no duplicate variables
    all_vars <- unique(all_vars)
    
    
    # Set up SQL code to load columns
    sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`table_name`} WITH (TABLOCK) 
                                    ({`vars`*}) 
                                    SELECT {`vars`*} FROM (", 
                                  .con = conn,
                                  vars = all_vars)
    
    # For each year check which of the additional columns are present
    lapply(seq_along(combine_years), function(x) {
      table_name_new <- paste0(table_name, "_", combine_years[x])
      config_name_new <- paste0("table_", combine_years[x])
      add_vars_name <- paste0("vars_", combine_years[x])
      if (!is.null(names(table_config$vars))) {
        year_vars <- c(unlist(names(table_config$vars)), 
                       unlist(names(table_config[[config_name_new]][[add_vars_name]])))
      } else {
        year_vars <- c(unlist(table_config$vars), unlist(table_config[[config_name_new]][[add_vars_name]]))
      }
      
      matched_vars <- match(all_vars, year_vars)
      
      vars_to_load <- unlist(lapply(seq_along(matched_vars), function(y) {
        if (is.na(matched_vars[y])) {
          var_x <- paste0("NULL AS ", all_vars[y])
        } else {
          var_x <- all_vars[y]
        }
      }))
      
      # Add to main SQL statement
      if (x < length(combine_years)) {
        sql_combine <<- glue::glue_sql("{`sql_combine`} SELECT {`vars_to_load`*}
                                         FROM {`to_schema`}.{`table`} UNION ALL ",
                                       .con = conn,
                                       table = table_name_new)
      } else {
        sql_combine <<- glue::glue_sql("{`sql_combine`} SELECT {`vars_to_load`*}
                                         FROM {`to_schema`}.{`table`}) AS tmp",
                                       .con = conn,
                                       table = table_name_new)
      }
      
    })
    
    dbGetQuery(conn, sql_combine)
    
    if (add_index == T) {
      if (!exists("add_index_f")) {
        devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
      }
      message("Adding index")
      add_index_f(conn = conn, table_config = table_config, test_mode = test_mode)
    }
  }
}
