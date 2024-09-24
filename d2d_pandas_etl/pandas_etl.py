import sys
import os
import datetime
import re
import logging
from logging.config import fileConfig

import pandas as pd
from pandas import DataFrame
from pandas._libs.tslibs.nattype import NaTType
from sqlalchemy import create_engine, text

import googlesheets
import utils
from exceptions import UnsupportedDataDestination, UnsupportedDataSource
import math #done by satish

"""
    Program that uses a config file that contains a 'source' (excel file, google sheet, database etc) and takes this tabular
    data and copies/etl's it to the 'destination' (source->destination).  If in the pandas directory sample
    config files are kept in 'pandas/resources' and they may be invoked in the following way:

        * python3 pandas_etl.py resources/memory_to_sqllitedb_config.json
        * python3 pandas_etl.py resources/excel_to_sqllitedb_config.json
        * python3 pandas_etl.py resources/googlesheet_to_sqllitedb_config.json
        * python3 pandas_etl.py resources/googlesheet_to_mysqldb_config.json
        * python3 pandas_etl.py resources/excel_to_excel_config.json

    Look into the resources directory for other config files.  In general data sources can be
        * database/db
        * googlesheets
        * excel
        * memory (for debugging. It uses a hardcoded dataset)

    Destination data sinks:
        * database/db
        * googlesheets
        * excel

    Pandas supports other datasets such as Parquet, csv, json and so those could also easily be added
    as a source or destination.


    Here is further documentation on the source/destination types pandas allows:
    https://pandas.pydata.org/pandas-docs/stable/reference/frame.html#serialization-io-conversion

"""
fileConfig('logging_conf.ini', disable_existing_loggers=False)
log = logging.getLogger(__name__)
DEBUG_LEVEL = 10 # logging debug level from ini config file

def _noop(dataframe: DataFrame) -> DataFrame:
    """ Default implementation of a function that takes a dataframe as an argument.

     It is automatically called in between getting the source datasource and writing to the destination datasource.  This is
     a no operation method that doesn't have any effect.
    """
    return dataframe


def _clean_for_googlesheets(value):
#    print(value)
#    print(type(value))
    if isinstance(value, NaTType):
        return None
    elif isinstance(value, float) and math.isnan(value): #Satish had added these 2 lines
        return 0.0
    elif isinstance(value, datetime.date):
        return str(value)
    else:
        return value


class PandasEtl:
    memory_data = [
        ["obj_col", "int_col", "float_col", "date_col"],
        ["joe", 10, 10.5, "12/20/2000"],
        ["ed", 20, 20.5, "12/21/2000"],
        ["al", 30, 30.5, None],
    ]

    def __init__(self, config_arg):
        self.config = config_arg

    def run(self, dataframe_callback=_noop):
        """
        Method that performs the ETL (move data from source to destination) defined in the config file.

        This method can be passed a function that allows the caller to manipulate the DataFrame in any
        way they want before the changed DataFrame is saved to the destination. For example the following
        function would add a new column to the passed in DataFrame.<br>
            def add_column(dataframe: DataFrame) -> DataFrame:
                &nbsp;&nbsp;&nbsp;dataframe['newcol']=22
                &nbsp;&nbsp;&nbsp;return dataframe

        :param function dataframe_callback: A function that takes a DataFrame as an argument and returns a DataFrame
            after it has been manipulated. An example call providing the above 'add_column' function:
                etl.run(dataframe_callback=add_column)
        """        
        self.to_destination(dataframe_callback(self.fix_header(self.from_source())))

    def from_source(self):
        source_type = self.config['source']['type']
        log.info(f"source.type: {source_type}")
        # date_coercion defaults to true for all source types.
        date_coercion = self.config['source'].get('date_coercion', True)

        if source_type == "memory":
            data = self.memory_data.copy()
            header = data.pop(0)
            dataframe = utils.to_pandas(data, header, strings_to_dates=date_coercion)
        elif source_type == "db":
            query = self.config['source']['query']
            log.info(f" source.query: {query}")
            engine = create_engine(self.config['source']['url'], echo=self._is_debug())
            try:
                dataframe = pd.read_sql(query, engine)
            except Exception as e:
                print("An error occured", str(e))
                print("Trying to use connect instead ...")
                query = text(query)
                conn= engine.connect()
                dataframe = pd.read_sql_query(query, conn)
                conn.close()
            if date_coercion:
                dataframe = utils.dataframe_date_coercion(dataframe)
        elif source_type == "excel":
            excel_file = self.config['source']['file']
            sheet = self.config['source']['sheet']
            log.info(f" source.file: {excel_file}, source.sheet: {sheet}")
            dataframe = pd.read_excel(io=excel_file, sheet_name=sheet, header=0)
            if date_coercion:
                dataframe = utils.dataframe_date_coercion(dataframe)            
        elif source_type == "googlesheets":
            dataframe = self._get_googlesheet_dataframe()
            if date_coercion:
                dataframe = utils.dataframe_date_coercion(dataframe)  
        elif source_type == "csv":
            dataframe = self. _from_csv()              
        elif source_type == "parquet":
            dataframe = self. _from_parquet()          
        else:
            raise UnsupportedDataSource(f"The following 'source.type' is not supported: {source_type}")

        return dataframe

    def fix_header(self, dataframe):
        """ This method can change the header columns of the dataframe, either by getting a hardcoded
        header from the config file (under source.header.columns) or it can format the existing column.
        Currently the following are supported and here is an example of how each are called. They will
        all be called in the order they appear in the config file.
            "header" : {
                "formatters" : [
                    # convert the column names to upper case
                    {"type": "uppercase"},
                    # convert the column names to lower case
                    {"type": "lowercase"},
                    # replace any invalid characters with the given string.  Note the reg exp lists valid chars
                    {"type": "replace", "valid_regex":"[^A-Za-z0-9_]+", "replace_with": "X"},
                    # truncates the column name to 2 characters
                    {"type": "width", "size": 2}
                ]
            }
        :param dataframe:
        """
        # if the user defined a header in the config file then use it to override the existing one.
        header = self._get_column_header()

        if header:
            dataframe.columns = header
        elif ('header' in self.config['source']) and ('formatters' in self.config['source']['header']):
            formatters = self.config['source']['header']['formatters']
            for formatter in formatters:
                if 'type' in formatter:
                    formatter_type = formatter['type']
                    if formatter_type.upper() == 'UPPERCASE':
                        # note the following converts ALL column names to upper case. The syntax is powerful and
                        # succinct and is a feature of pandas.
                        dataframe.columns = dataframe.columns.str.upper()
                    elif formatter_type.upper() == 'LOWERCASE':
                        dataframe.columns = dataframe.columns.str.lower()
                    elif formatter_type.upper() == 'WIDTH': # truncate excess characters from column
                        size = formatter['size']
                        dataframe.columns = dataframe.columns.str[:size]
                    elif formatter_type.upper() == 'REPLACE':
                        # replace all characters that are NOT in the regex string using replace_with
                        regex = formatter['valid_regex']
                        replace_with = formatter['replace_with']
                        dataframe.columns = ([re.sub(regex, replace_with, str(column_name))
                                             for column_name in dataframe.columns])
                        
        return dataframe

    def to_destination(self, dataframe):
        dest_type = self.config['destination']['type']
        log.info(f"destination.type: {dest_type}")

        if dest_type == "db":
            table = self.config['destination']['table']
            log.info(f" destination.table: {table}")
            engine = create_engine(self.config['destination']['url'], echo=self._is_debug())
            utils.to_db(engine=engine,
                        dataframe=dataframe,
                        table_name=table,
                        if_exists=self.config['destination'].get('if_exists', 'append'),
                        method=self.config['destination'].get('method', 'single'),
                        chunksize =  self.config['destination'].get('chunksize', None),
                        debug=self._is_debug())
        elif dest_type == "excel":
            excel_file = self.config['destination']['file']
            sheet = self.config['destination']['sheet']
            mode = self.config['destination'].get('mode', 'w') # w=write, a=append,  default is ‘w’  
            datetime_format = self.config['destination'].get('datetime_format', None)
            date_format = self.config['destination'].get('date_format', None)
            log.info(f" destination.file: {excel_file}, destination.sheet: {sheet}, destination.mode: {mode}, destination.datetime_format: {datetime_format}, destination.date_format: {date_format}")

            # append mode can't create a file so if there isn't a file yet simply write the file.
            if mode == 'w' or (mode == 'a' and os.path.exists(excel_file) is False):
                 with pd.ExcelWriter(excel_file,
                                mode = 'w',
                                datetime_format = datetime_format,
                                date_format = date_format) as writer:
                      dataframe.to_excel(excel_writer = writer, sheet_name = sheet, index = False)
            # note if the file already exists and append mode is selcted then the following is done to add new rose to the file (append)
            elif mode == 'a': # note append doesn't work with the XlsxWriter python library but works with the openpyxl library
                self._append_excel(dataframe, excel_file, sheet, date_format, datetime_format)    
            else:
                raise UnsupportedDataDestination(f"The following 'destination.mode' is not supported: {mode}")
        elif dest_type == "googlesheets":
            spreadsheet_id = self.config['destination']['spreadsheet_id']
            sheet = self.config['destination']['sheet']
            overwrite_or_append = self.config['destination']['overwrite_or_append']
            value_input_option =  self.config['destination'].get('value_input_option', "USER_ENTERED")
            log.info(
                f" destination.spreadsheet_id: {spreadsheet_id}, destination.sheet: {sheet}, destination.overwrite_or_append: {overwrite_or_append}")

            spreadsheet = googlesheets.GoogleSheet(self.config['destination']['credentials_file'])
            # Change types that google sheets does not support (pandas dates for example) into ones it does
            dataframe = dataframe.applymap(lambda value: _clean_for_googlesheets(value))
            # Change pandas data into a format that google spreadsheets api supports (header, and data in a list)
            #  Note might also be good for doing the following conversion: dataframe.to_dict("split"))
            data = [dataframe.columns.values.tolist()]  # header
            data.extend(dataframe.values.tolist())  # data/rows
            results = spreadsheet.put_data(spreadsheet_id, sheet, data, value_input_option=value_input_option, overwrite_or_append=overwrite_or_append)
            log.info(results)
        elif dest_type == "csv":
            self. _to_csv(dataframe)
        elif dest_type == "parquet":
            self._to_parquet(dataframe)
        elif dest_type == "noop":
            log.info("The destination is 'noop'. The data will not be written to a destination.")
        else:
            raise UnsupportedDataDestination(f"The following 'destination.type' is not supported: {dest_type}")


    def _get_googlesheet_dataframe(self):
        spreadsheet_id = self.config['source']['spreadsheet_id']
        sheet = self.config['source']['sheet']
        log.info(f" source.spreadsheet_id: {spreadsheet_id}, source.sheet: {sheet}")
        spreadsheet = googlesheets.GoogleSheet(self.config['source']['credentials_file'])
        all_rows = spreadsheet.get_data(spreadsheet_id, sheet)

        header = self._get_googlesheets_header(all_rows)
        data = self._get_googlesheets_data(all_rows)
        # if data_coercion is needed it will instead be called from a higher level.
        dataframe = utils.to_pandas(data, header, strings_to_dates=False)
        return dataframe

    def _get_googlesheets_header(self, all_rows):
        # a defined column header takes precedence over header_index
        # Note getting 'columns' from the config sheet  is also done for all dataframes later in the code,
        # but it is also done here as sonetimes if the header in the sheet isn't defined a header_index
        # could throw an exception, so we want to avoid that code from happening here.
        header = self._get_column_header()
        if header:
            return header
        else:
            # header_index - The row index in the spreadsheet where the header starts (row 1 would be index 0)
            header_index = self.config['source'].get('header_index', 0)
            return all_rows[header_index]

    def _get_googlesheets_data(self, all_rows):
        # data_index -  The row index in the spreadsheet where the data starts (row 1 would be index 0)
        data_index = self.config['source'].get('data_index', 1)
        data = all_rows[data_index:]
        return data

    def _get_column_header(self):
        if ('header' in self.config['source']) and ('columns' in self.config['source']['header']):
            return self.config['source']['header']['columns']
        else:
            return None
  
    def _from_csv(self):
       """  
        Read a delimited file (defaults to ',') into a pandas dataframe.

        pandas.read_csv - As values are simply passed on to this pandas method see its documentation for further information 
        on the allowed parameters - https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html#pandas.read_csv
        
        csv_file is passed to the pandas variable 'filepath_or_buffer' - From pandas documentation...
            Any valid string path is acceptable. The string could be a URL. 
            Valid URL schemes include http, ftp, s3, gs, and file. For file URLs, 
            a host is expected. A local file could be: file://localhost/path/to/table.csv
            Note compressed formats are also supported  - ‘.gz’, ‘.bz2’, ‘.zip’, ‘.xz’, ‘.zst’, ‘.tar’, ‘.tar.gz’, ‘.tar.xz’ 
            or ‘.tar.bz2’ (otherwise no compression). See 'compression' parameter in the pandas_read_csv documentation for more
            on compression. Also note for certain types such as s3 other libraries may need to be installed (such as s3fs)
        delimiter - Specified delimeter in file. default is ',' 
        header - Row of data file that has header file.  Default is 0 
        chunksize - Can increase performance of larger csv file. Default is None 
       """
       csv_file = self.config['source']['file']
       delimiter =  self.config['source'].get('delimiter', ",")
       header = self.config['source'].get('header', 0)
       chunksize =  self.config['source'].get('chunksize', None) # Number of rows to write at a time
       dataframe = pd.read_csv(filepath_or_buffer=csv_file, delimiter=delimiter, header=header, chunksize=chunksize)
       
       log.info(f" source config properties - source.file: {csv_file}, source.delimiter: '{delimiter}', source.header: '{header}', source.chunksize: {chunksize}")

       return dataframe
   
        
    def _to_csv(self, dataframe):
        # See the following for further information on pandas writing to a csv - https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html#pandas.DataFrame.to_csv
        #  Note all parameters to the df.to_csv are not currently exposed in the json file, however the others could be added if needed.
        csv_file = self.config['destination']['file']
        delimiter =  self.config['destination'].get('delimiter', ",")
        float_format = self.config['destination'].get('float_format', None)
        date_format =  self.config['destination'].get('date_format', None)
        columns = self.config['destination'].get('columns', None) # Sequence of columns to write
        index = self.config['destination'].get('index', False) # Write the row index values
        index_label = self.config['destination'].get('index_label', "index")
        chunksize =  self.config['destination'].get('chunksize', None) # Number of rows to write at a time
      
        log.info(f" destination config properties1 - destination.file: {csv_file}, destination.delimiter: '{delimiter}', destination.float_format: {float_format}")
        log.info(f" destination config properties2 - destination.columns: {columns}, destination.index: {index}, destination.index_label: {index_label}, destination.date_format: {date_format}")
        log.info(f" destination config properties3 - destination.chunksize: {chunksize}")

        dataframe.to_csv(csv_file, sep=delimiter, float_format=float_format, columns=columns, index=index, index_label=index_label, date_format=date_format, chunksize=chunksize)


    def _from_parquet(self):
       """  
        Read a parquet file into a pandas dataframe.
        
        Note a python package/library that can read parquet must be installed for this to work.  From the pandas documentation 
        pyarrow and fastparquet are supported.
        
        pandas.read_parquet - As values are simply passed on to this pandas method see its documentation for further information 
        on the allowed parameters - https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html
        
        path - From pandas documentation...
            The string could be a URL. Valid URL schemes include http, ftp, s3, gs, and file. For file URLs, a host is expected. A local 
            file could be: file://localhost/path/to/table.parquet. A file URL can also be a path to a directory that contains multiple 
            partitioned parquet files. Both pyarrow and fastparquet support paths to directories as well as file URLs. A directory path 
            could be: file://localhost/path/to/tables or s3://bucket/partition_dir.
        columns - If not None, only these columns will be read from the file. example: ["colA", "colB", "colZ"]. default=None
        
       """
       path = self.config['source']['file']
       columns =  self.config['source'].get('columns', None)
       dataframe = pd.read_parquet(path=path, columns=columns)
       
       log.info(f" source config properties - source.file: {path}, source.columns: '{columns}'")

       return dataframe
   
    
    def _to_parquet(self, dataframe):
        """  
         Write a parquet file from a pandas dataframe.
        
         Note a python package/library that can write parquet must be installed for this to work.  From the pandas documentation 
         pyarrow and fastparquet are supported.
        
         pandas.dataframe.to_parquet - As values are simply passed on to this pandas method see its documentation for further information 
         on the allowed parameters - https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html
        
         path - From pandas documentation...
            Path to the parquet output file.  If a string or path, it will be used as Root Directory path when writing a 
            partitioned dataset. The pandas documentation doesn't state whether s3, gs etc. can be used as the parquet read function above
            so that is not clear. 
        
       """
        path = self.config['destination']['file']      
        log.info(f" destination config properties - destination.file: {path}")
        dataframe.to_parquet(path=path)

    def _append_excel(self, dataframe, excel_file, sheet, date_format, datetime_format):
        if_sheet_exists = self.config['destination'].get('if_sheet_exists', "overlay")
        header = self.config['destination'].get('header', False)
        log.info(f" destination.if_sheet_exists: {if_sheet_exists}, destination.header: {header}")
        with pd.ExcelWriter(excel_file,
              mode = 'a',
              engine='openpyxl', # note xlsxwriter does not support append mode so switching engines
              if_sheet_exists = if_sheet_exists,
              datetime_format = datetime_format,
              date_format = date_format) as writer:
            startrow=writer.sheets[sheet].max_row
            dataframe.to_excel(excel_writer = writer, startrow = startrow, header = header, sheet_name = sheet, index = False)

    
    def _is_debug(self):
        return log.getEffectiveLevel() == DEBUG_LEVEL or self.config.get("debug", False)


if __name__ == '__main__':
    # Example: python3 pandas_etl.py resources/memory_to_sqllitedb_config.json
    if len(sys.argv) == 2:
        
        try:
            configuration_file = sys.argv[1]
            config = utils.load_json(configuration_file)
            log.debug(config)
            etl = PandasEtl(config)
            etl.run()
        except Exception:
            log.exception("pandas_etl exception")
    else:
        print(
            "Pass a configuration file that defines the source of where to get tabular data from and the destination of where to copy it to.")
        print("Example: python pandas_etl.py resources/memory_to_sqllitedb_config.json")
