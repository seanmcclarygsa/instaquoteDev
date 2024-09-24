# Config file properties explained
The following are possible properties in a pandas_etl config file. The files in this directory have examples of all of these config properties. Note the pandas function that
is called as the underlying implementation is also referenced if there is one in the writeups below.  Properties of the pandas methods that are not currently exposed could be if the capabilities are deemed useful (for example all dateaframe.to_csv(...) properties are not configurable from the config file.

Properties can occur in any order in the config file. The top level properties are source, destination, date_coercion, and header.

* **source** - Configuration properties for determining how to read the data which can later be written using the 'destination' section. 
Data read from the source section will first be read into a pandas dataframe before being written to the 'destination'.
  * **type** - Source type of data that is to be read from. Each source type will have other specific configuration properties.
     *  **db** - database query or table. [Implementing pandas function](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html)
         *  query - A query (select * from table) or table name (my_table) to get the data from.
         *  url - Connection string to connect to the database.
         *  debug - If True then write database debug information to the log. Values: True/False(default)
     *  **googlesheets** - Read data from a GoogleSheet. [Implementing googlesheets api function](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get).  Note pandas does not have the ability to connect to googlesheets natively
         * spreadsheet_id - Id of the googlesheet. 
         * sheet - Google sheet name along with optional A1 notation (see link above for info on A1 notation)
         * credentials_file - A google service account credentials file used to log in to the google sheet.
         * header_index - The row index in the spreadsheet where the header starts (row 1 would be index 0) (default is 0)
         * data_index -  The row index in the spreadsheet where the data starts (row 1 would be index 0) (default is 0)
     *  **excel** - Read data from excel. [Implementing pandas function](https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html)
         * file - Excel spreadsheet file to read in
         * sheet - Excel sheet to read in from the excel file
     *  **csv** - Read any delimited file, but defaults to ',' delimited (CSV).
         * file - File to read. It is passed to the pandas variable 'filepath_or_buffer' - From pandas documentation...
             Any valid string path is acceptable. The string could be a URL. 
            Valid URL schemes include http, ftp, s3, gs, and file. For file URLs, 
            a host is expected. A local file could be: file://localhost/path/to/table.csv
            Note compressed formats are also supported  - ‘.gz’, ‘.bz2’, ‘.zip’, ‘.xz’, ‘.zst’, ‘.tar’, ‘.tar.gz’, ‘.tar.xz’ 
            or ‘.tar.bz2’ (otherwise no compression). See 'compression' parameter in the pandas_read_csv documentation for more
            on compression. Also note for certain types such as s3 other libraries may need to be installed (such as s3fs)
         * delimiter - Specified delimeter in file. default is ',' 
         * header - Row of data file that has header file.  Default is 0 
         * chunksize - Can increase performance of larger csv file. Default is None 
     *  **parquet** - Read data from a parquet file
         * file - The name and location of the parquet file. From pandas documentation...
            The string could be a URL. Valid URL schemes include http, ftp, s3, gs, and file. For file URLs, a host is expected. A local 
            file could be: file://localhost/path/to/table.parquet. A file URL can also be a path to a directory that contains multiple 
            partitioned parquet files. Both pyarrow and fastparquet support paths to directories as well as file URLs. A directory path 
            could be: file://localhost/path/to/tables or s3://bucket/partition_dir.
         * columns - If not None, only these columns will be read from the file. example: ["colA", "colB", "colZ"]. The default is None (so all parquet 
          columns are read
     *  **memory** - Uses a simple hardcoded in memory structure. Used only for demo and test purposes.
* **destination** - Configuration properties for determining how to write the data to a destination.
  * **type** - Source type of data that is to be written to. Each source type will have other specific configuration properties.
     *  **db** - database table to write to. [Implementing pandas function](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html)
         *  table - A table name (my_table) to write to.
         *  url - Connection string to connect to the database.
         *  debug - If True then write database debug information to the log. Values: True/False(default)
         *  if_exists - What to do if the table already exists...Values: fail, replace, or append(default)
         *  method - Controls the SQL insertion clause used.  Values: single(default), multi (more efficient)
         *  chunksize - Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once. 
     *  **googlesheets** - Write data to a GoogleSheet. [Implementing googlesheets api function](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append#InsertDataOption).  Note pandas does not have the ability to connect to googlesheets natively
         * spreadsheet_id - Id of the googlesheet. 
         * sheet - Google sheet name to write to. It can use A1 notation.
         * credentials_file - A google service account credentials file used to log in to the google sheet.
         * overwrite_or_append - OVERWRITE(default) or APPEND rows
         * value_input_option - Values: USER_ENTERED(default), RAW, INPUT_VALUE_OPTION_UNSPECIFIED
     *  **excel** - Write data to excel. [Implementing pandas function](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_excel.html) and [this one](https://pandas.pydata.org/docs/reference/api/pandas.ExcelWriter.html#pandas.ExcelWriter)
         * file - Excel spreadsheet file to write to
         * sheet - Excel sheet to write to in the spreadshet
         * datetime_format - Format of any datetimes columns that are written
         * datae_format - Format of any date columns that are written
         * mode - 'a' or 'w'. File mode to use (write or append). 'w' is the default. Note append doesn't work with the XlsxWriter python library but works with the openpyxl library. Note both libraries can 
         be installed at the same time. See [pandas documentation](https://pandas.pydata.org/docs/reference/api/pandas.ExcelWriter.html#pandas.ExcelWriter) for more about these properties. Append mode has these 2 other properties..
           * if_sheet_exists - What to do if the sheet already exists - {‘error’, ‘new’, ‘replace’, ‘overlay’}, default ‘error’
           * header - Indicates whether or not to write the column headers to the sheet.  In the case of an append it is assumed the header is already in the destination sheet (i.e. header = False).  Possible values are True and False and the default is False.
     *  **csv** - Write to a delimited file. [Implementing pandas function](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html)
         * file - File name to write to (also supports other formats such as AWS S3. See link above)
         * delimiter - delimiter between columns (',' is default, escapes work such as '/t')
         * float_format - format for any float numbers written to the file. Optional field. Example: %.2f
         * date_format - format for any dates written to the file. Optional field. Example: %m-%d-%Y
         * columns - Array list of columns to write to the file. Example: ["col1","col3",col5"] (Note col2,col4 and any col's after col5 wouldn't be written in this example.
         * index - Write a row number to the file
         * index_label - Provide a column label for the index/row column.
         * chunksize - Write the file in batches.  The default is to write as one batch.  
     *  **parquet** - Read data from a parquet file
         * file - Path to the parquet output file. From pandas documentation...
            Path to the parquet output file.  If a string or path, it will be used as Root Directory path when writing a 
            partitioned dataset. The pandas documentation doesn't state whether s3, gs etc. can be used as the parquet read function above
            so that is not clear. 
     *  **noop** - Type to use if no destination is being written to.
* **date_coercion** - Attempts to force all values to a date type. If it fails values are left as is. Values: True(default)/False
* **header** - Columns and formatters are mutually exclusive i.e. only one can be used at a time.
  * columns - Override column names that are in the source data. Example: ["cola","colb","colc","cold"]
  * formatters - Take the original column names in the source data and format them. For example replace spaces with underscores via a regular expression (example 'my col name' becomes 'my_col_name'). The following formatters are currently supported.
      * type - Note any number of the following types can be applied and they are applied to the column in the given order.
          *  uppercase - Converts a column name to uppercase. Example: mycolname->MYCOLNAME
          *  lowercase - Converts a column name to lowercase. Example: MYCOLNAME->mycolname
          *  width - Truncate the column name to the specified number of characters. Example if width=5: mycolname->mycol
          *  replace - Specify valid column name characters and replace all others with the replacement string.
              *  valid_regex - Valid characters from a regular expression. All others will be replaced. Example: "[^A-Za-z0-9_]+"  
              *  replace - Replace any characters that don't match the above regular expressing with this string: Example: "-"
