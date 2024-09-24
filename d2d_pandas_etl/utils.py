import json
import os
from string import Template

import pandas as pd
from dateutil.parser import parse as parse_date
from sqlalchemy import create_engine
import logging


"""
Utilities for 
    * Converting tabular data to a Pandas DataFrame
    * Saving Pandas DataFrames to a database
    * Converting strings to dates
"""


def date_coercion(value: object, fuzzy: bool = False) -> object:
    """
    Return a date if the value passed can be converted to a date otherwise return the passed value unchanged.

    :return: date or original value of any type
    :rtype: object
    :param value: any object type, but if it is a string the code tries to convert it to a date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        # parse any string except numbers i.e.: "10", "10.5".  These will normally parse to dates but we
        # don't want to do that.
        if isinstance(value, str) and not is_str_number(value):
            return parse_date(value, fuzzy=fuzzy)
        else:
            # if not isinstance(string, str) or string.isnumeric() or isfloat(string):
            return value
    # Note regular strings such 'hello' would trigger this and it is so common opting not to log it.
    except ValueError:        
        return value
    except Exception as e:
        logging.warning(f"The value: '{value}' caused the following date_coercion exception: {e}")
        return value


def is_str_number(value: str) -> bool:
    """
        Test to see if a passed in string is a float (ex: "200", "22.34". Non string arguments will return false.

        :param value: Any object
        :return: True if the value is a string that can be converted to a number
        :rtype: bool
    """
    try:
        if not isinstance(value, str):
            return False
        elif value.isnumeric():
            return True
        else:
            float(value)
            return True
    except ValueError:
        return False


def to_pandas(rows, header=None, strings_to_dates=True):
    """
        Convert lists of rows to a Pandas DataFrame.  DataFrames are flexible and powerful way to manipulate tabular data.

        :param rows: Rows containing any number of cells as a list of lists. Rows should not contain a header. Example:
            [["joe",22,"12/20/1975"],["bill",33,"08/25/1980"]
        :param header: Column header in list form for the data or None. Example: ["name","age","birthday"]
        :param strings_to_dates: True if dates strings should be converted to a 'date' type. The parsing mechanism
            is quite flexible so many date formats are automatically converted if True. Example: "12/20/200" would be
            converted from a string to a date if True. It will be left alone and returned as a string if this parameter
            is False.
        :return: A Pandas DataFrame
    """
    df = pd.DataFrame(data=rows, columns=header)
    if strings_to_dates:
        # Change all dataframe string values of format date (for example: "12/10/2013" to actual dates.
        # Leave all other cells alone.
        return dataframe_date_coercion(df)
    else:
        return df

"""
    Currently applies date coercion to all values in all columns of the dataframe. This could potentially result in some values
    in a column being converted to dates whie others are not and so having mismatched data types in a column.  
    For example '3M 45' is converted to a date in python but in one case this was supposed to be a string key 
    in the data and so it caused an error. At some point it might be worth it to look into more selectively 
    applying this to columns.
"""
def dataframe_date_coercion(df):
    return df.applymap(lambda cell_value: date_coercion(cell_value))


def to_db(dataframe, table_name, schema=None, chunksize=None, engine=None, if_exists='append', method=None, debug=False):
    """
    Save a Pandas DataFrame to a database.  Note a table will be created if it doesn't already exist.
    Records are currently appended if the table already exists.

    :param dataframe: Pandas DataFrame tabular data
    :param table_name: Table to save the data into
    :param schema: DB Schema to save to.  If not provide it will use the default schema
    :param chunksize: Batch commit size.
    :param engine: Connection to a specific database type (for example mysql, sqllite, postgress). If engine isn't
        passed then a sqllite internal database will be used.
    :param if_exists: Possible values ‘fail’, ‘replace’, ‘append’ default 'append'
        'fail' - fail if table exists, 'replace' - drop table before inserts, 'append' - append rows
        'overwrite' - delete existing rows before inserting new rows. All but 'overwrite' are standard arguments
        as defined for https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html and merely 
        passed on.
    :param method:  Possible values: None, ‘multi’, 'single', callable. See 
         https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html for documentation as
         this argument is merely passed on excelp for single which is an alias for 'None' and so does
         the same thing.
    :param debug: If true then turn on db logging level to DEBUG
    :return: No return value
    """
    if engine is None:
        engine = create_engine('sqlite://', echo=debug)
        
    if method == 'single':
        method = None
        
    if if_exists == 'overwrite':
        _overwrite(dataframe, table_name, schema, chunksize, engine, if_exists, method, debug)
    else:
        dataframe.to_sql(name=table_name, con=engine, schema=schema, chunksize=chunksize, if_exists=if_exists, method=method, index=False)


def _overwrite(dataframe, table_name, schema, chunksize, engine, if_exists, method, debug):
    """
     Add ability to first delete rows from the table. Note the table has to already exist (if needed a table existance check could be 
     added to this code). Note delete and insert are in one transaction.
    """
    connection = engine.connect()
    trans = connection.begin()
    try:
        connection.execute('delete from ' + table_name)
        dataframe.to_sql(name=table_name, con=connection, schema=schema, chunksize=chunksize, if_exists="append", method=method, index=False)
        trans.commit()
    except:
        trans.rollback()
        raise
    
    
def load_json(filename, var_subst_func=None):
    """
        Load a json file into a python dictionary and performs variable substitution on the file
        contents (i.e ${USER}, ${PASSWORD} could be done in the file or your own approach to variable
        substitution can be used by passsing in 'var_subst_func' that will accept a version of the
        json file as a string.

        :param filename: json file
        :param var_subst_func:
        :return: dictionary of json file
    """
    with open(filename) as file:
        json_str = file.read()

    if var_subst_func is None:
        var_subst_func = _substitute_os_vars

    return json.loads(var_subst_func(json_str))


def _substitute_os_vars(json_str):
    # replace OS variable references with their values.  Can use $xxx or ${xxx} if no whitespace follows
    # Example { "key": "Hi${USER} Hi $USER, Hi${PASSWORD} or Hi $PASSWORD"}
    return Template(json_str).substitute(os.environ)

