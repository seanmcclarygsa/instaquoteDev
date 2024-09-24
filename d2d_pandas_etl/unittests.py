from unittest import TestCase
import os.path

import pandas as pd
from sqlalchemy import create_engine
import datetime

import utils
import googlesheets
import pandas_etl

"""
    * Tests for converting various tabular data sets to a Pandas dataframe. For example: excel, lists
    * Tests for saving Pandas DataFrames to a database.
    * Tests for other utility methods such as data coercion
    
    For 2 of the tests to succeed mysql must be running and the following schema created:
        CREATE SCHEMA IF NOT EXISTS delme;
        
    The above command can be executed from mysqlworkbench.
    
    I ran mysql in docker for the tests.  Here is a command to run mysql in docker.  Docker is not required. 
    You just need mysql to be running locally.  Here is the docker mysql command I used:
        docker run --name delmex1 --rm  -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root -d mysql

            
    To run these tests: python -m unittest unittests.py
    
    A successful run should have results similar to the following depending on how many tests there are.
        Ran 17 tests in 12.593s

        OK

    Assertions - https://www.kite.com/python/docs/unittest.TestCase
"""
# mysql isn't always running so tests can be enabled/disabled
RUN_MYSQL_TESTS = True

class UnitTests(TestCase):
    data = [
        ["obj_col", "int_col", "float_col", "date_col"],
        ["joe", 10, 10.5, "12/20/2000"],
        ["ed", 20, 20.5, "12/21/2000"],
        ["al", 30, 30.5, None],
    ]
    creds_file = "/Users/steventsouza/.kettle/client_secret.json"

    def test_is_str_number(self):
        self.assertTrue(utils.is_str_number("1.5"))
        self.assertTrue(utils.is_str_number("1"))
        self.assertFalse(utils.is_str_number(1))  # false - only strings can be true
        self.assertFalse(utils.is_str_number(1.5))
        self.assertFalse(utils.is_str_number("joe"))
        self.assertFalse(utils.is_str_number(None))
        # added the following test due to a bug
        self.assertFalse(utils.is_str_number("3M 60410003036"))

    def test_date_coercion(self):
        #  datetime(y,m,d)
        self.assertEqual(datetime.datetime(2013, 12, 2), utils.date_coercion("2013-12-2"))
        self.assertEqual(datetime.datetime(1962, 5, 15), utils.date_coercion("05/15/1962"))
        self.assertEqual("1999", utils.date_coercion("1999"))
        self.assertEqual("10.5", utils.date_coercion("10.5"))
        self.assertEqual("joe", utils.date_coercion("joe"))
        self.assertEqual(2000, utils.date_coercion(2000))
        self.assertEqual(datetime.datetime(2021, 1, 14, 20), utils.date_coercion("Thursday, January 14, 2021 at 8 pm"))
        self.assertIsNone(utils.date_coercion(None))
        # note errors below just return the string unchanged.
        # The following test converts to todays date at 3 min 45 seconds. Added test for bug found
        assert type(utils.date_coercion("3M 45")) is datetime.datetime
        # doesn't convert date correctly as number is too big
        self.assertEqual("3M 60410003036", utils.date_coercion("3M 60410003036"))
        # xx makes it not convert.
        self.assertEqual("xx/15/1962", utils.date_coercion("xx/15/1962"))


    def test_to_pandas_no_string_to_date_coercion(self):
        data = self.data.copy()
        header = data.pop(0)
        df = utils.to_pandas(data, header, strings_to_dates=False)
        print(df)

        # note there may be better ways to do this.  Comparing if fields are of expected types
        col_types = df.select_dtypes(["object"]).columns.tolist()
        self.assertListEqual(["obj_col", "date_col"], col_types)

        col_types = df.select_dtypes(["int64"]).columns.tolist()
        self.assertListEqual(["int_col"], col_types)

        col_types = df.select_dtypes(["float64"]).columns.tolist()
        self.assertListEqual(["float_col"], col_types)

    def test_to_pandas_with_string_to_date_coercion(self):
        data = self.data.copy()
        header = data.pop(0)
        df = utils.to_pandas(data, header)
        print(df)

        # note there may be better ways to do this.  Comparing if fields are of expected types
        col_types = df.select_dtypes(["object"]).columns.tolist()
        self.assertListEqual(["obj_col"], col_types)

        col_types = df.select_dtypes(["int64"]).columns.tolist()
        self.assertListEqual(["int_col"], col_types)

        col_types = df.select_dtypes(["float64"]).columns.tolist()
        self.assertListEqual(["float_col"], col_types)

        col_types = df.select_dtypes(["datetime64"]).columns.tolist()
        self.assertListEqual(["date_col"], col_types)

    def test_to_pandas_noheader_with_string_to_date_coercion(self):
        data = self.data.copy()
        header = data.pop(0)
        df = utils.to_pandas(data)
        print(df)

        # note there may be better ways to do this.  Comparing if fields are of expected types
        col_types = df.select_dtypes(["object"]).columns.tolist()
        self.assertListEqual([0], col_types)

        col_types = df.select_dtypes(["int64"]).columns.tolist()
        self.assertListEqual([1], col_types)

        col_types = df.select_dtypes(["float64"]).columns.tolist()
        self.assertListEqual([2], col_types)

        col_types = df.select_dtypes(["datetime64"]).columns.tolist()
        self.assertListEqual([3], col_types)

    def test_to_sqlite_db_string_to_date_coercion(self):
        """
        Creates the following table if it doesn't exist or uses the existing one if it does.
        This example converts the string formatted date columns to DATETIME

        CREATE TABLE mytable (
	        obj_col TEXT,
	        int_col BIGINT,
	        float_col FLOAT,
	        date_col DATETIME
        )
        """
        MYTABLE = "mytable"
        data = self.data.copy()
        header = data.pop(0)
        # defaults to convert any string cells to dates if they can be converted
        df = utils.to_pandas(data, header)
        engine = create_engine('sqlite://', echo=True)
        utils.to_db(engine=engine,
                    dataframe=df,
                    table_name=MYTABLE)

        table_results = engine.execute(f"SELECT * FROM {MYTABLE}").fetchall()
        self.assertEqual(3, len(table_results))

    def test_to_sqlite_db_no_string_to_date_coercion(self):
        """
        Creates the following table if it doesn't exist or uses the existing one if it does.
        This example does not convert the string formatted date columns to DATETIME

        CREATE TABLE mytable (
            obj_col TEXT,
            int_col BIGINT,
            float_col FLOAT,
            date_col TEXT
        )
        """
        MYTABLE = "mytable"
        data = self.data.copy()
        header = data.pop(0)
        df = utils.to_pandas(data, header, strings_to_dates=False)
        engine = create_engine('sqlite://', echo=True)
        utils.to_db(engine=engine,
                    dataframe=df,
                    table_name=MYTABLE)

        table_results = engine.execute(f"SELECT * FROM {MYTABLE}").fetchall()
        self.assertEqual(3, len(table_results))

    def test_to_sqlite_db_no_string_to_date_coercion_multi(self):
        """
        Creates the following table if it doesn't exist or uses the existing one if it does.
        This example does not convert the string formatted date columns to DATETIME
        and it uses the multi insert method

        CREATE TABLE mytable (
            obj_col TEXT,
            int_col BIGINT,
            float_col FLOAT,
            date_col TEXT
        )
        """
        MYTABLE = "mytable"
        data = self.data.copy()
        header = data.pop(0)
        df = utils.to_pandas(data, header, strings_to_dates=False)
        engine = create_engine('sqlite://', echo=True)
        utils.to_db(engine=engine,
                    dataframe=df,
                    method='multi',
                    table_name=MYTABLE)

        table_results = engine.execute(f"SELECT * FROM {MYTABLE}").fetchall()
        self.assertEqual(3, len(table_results))
        
    def test_to_mysql_db_string_to_date_coercion(self):
        """
        Creates the following table if it doesn't exist or uses the existing one if it does.
        This example converts the string formatted date columns to DATETIME. If dates aren't
        coerced note that the inserts would fail the following table structure as a string of
        format '12/20/2020' is not considered a date.

            CREATE TABLE mytable (
                obj_col TEXT,
                int_col BIGINT,
                float_col FLOAT(53),
                date_col DATETIME
            )

        """
        if RUN_MYSQL_TESTS:
            MYTABLE = "mytable_pandasetl_datecoercion1"
            data = self.data.copy()
            header = data.pop(0)
            # defaults to convert any string cells to dates if they can be converted
            df = utils.to_pandas(data, header)
            # username: root, password: root, database: delme.  All of these can be anything as long
            # as they allow you to connect to the mysql instance.
            engine = create_engine('mysql+pymysql://root:root@localhost/delme', echo=True)
            utils.to_db(engine=engine,
                        dataframe=df,
                        table_name=MYTABLE)

            table_results = engine.execute(f"SELECT * FROM {MYTABLE}").fetchall()
            print(table_results)
            # For mysql test this is a permanent table and will append so it may have greater than 3 rows.
            self.assertLessEqual(3, len(table_results))
        else:
            print("This test is disabled")

    def test_to_mysql_db_no_string_to_date_coercion(self):
        """
        Creates the following table if it doesn't exist or uses the existing one if it does.
        This example does not convert the string formatted date columns to DATETIME. It leaves them as
        text.

            CREATE TABLE mytable_text (
                obj_col TEXT,
                int_col BIGINT,
                float_col FLOAT(53),
                date_col TEXT
            )

        """
        if RUN_MYSQL_TESTS:
            MYTABLE = "mytable_pandasetl_text"
            data = self.data.copy()
            header = data.pop(0)
            df = utils.to_pandas(data, header, strings_to_dates=False)
            # username: root, password: root, database: delme.  All of these can be anything as long
            # as they allow you to connect to the mysql instance.
            engine = create_engine('mysql+pymysql://root:root@localhost/delme', echo=True)
            utils.to_db(engine=engine,
                        dataframe=df,
                        table_name=MYTABLE)

            table_results = engine.execute(f"SELECT * FROM {MYTABLE}").fetchall()
            print(table_results)
            # For mysql test this is a permanent table and will append so it may have greater than 3 rows.
            self.assertLessEqual(3, len(table_results))
        else:
            print("This test is disabled")

    def test_excel_to_sqlite_db_string_to_date_coercion(self):
        """
        Based on the data in the excel spreadsheet the following table is created if it doesn't exist or use
        the existing one if it does exist. This example converts the string formatted date columns to DATETIME

            CREATE TABLE mytable (
                obj_col TEXT,
                int_col BIGINT,
                float_col FLOAT,
                date_col DATETIME
            )
        """
        MYTABLE = "mytable_pandasetl_datecoercion2"
        df = pd.read_excel(io="resources/python_excel_test.xlsx", sheet_name="Sheet1", header=0)
        print(f"Excel sheet dataFrame: \n{df}")
        self.assertEqual(60, df["int_col"].sum())

        engine = create_engine('sqlite://', echo=True)
        utils.to_db(engine=engine,
                    dataframe=df,
                    table_name=MYTABLE)

        table_results = engine.execute(f"SELECT * FROM {MYTABLE}").fetchall()
        print(table_results)
        self.assertEqual(3, len(table_results))

    def test_google_sheets_to_db_string_to_date_coercion(self):
        """
            This test requires access to a google service account secrets json file.  This file won't be checked
            into source control so the test can be disabled.
        """
        if os.path.isfile(self.creds_file):
            MYTABLE = "mytable_googlesheet"
            spreadsheet_id = "18VF0mB6usVkorgCULDqd4Ib9UrWf8MnaqYxrtnYcsvo"
            sheet = "Sheet1"  # A1 notation
            spreadsheet = googlesheets.GoogleSheet(self.creds_file)

            spreadsheet.get_modified_time(spreadsheet_id)
            self.assertIsNotNone(spreadsheet.get_modified_time(spreadsheet_id))
            data = spreadsheet.get_data(spreadsheet_id, sheet)
            print(data)
            header = data.pop(0)
            print(header)

            df = utils.to_pandas(data, header)
            engine = create_engine('sqlite://', echo=True)
            utils.to_db(engine=engine,
                        dataframe=df,
                        table_name=MYTABLE)

            table_results = engine.execute(f"SELECT * FROM {MYTABLE}").fetchall()
            print(table_results)
            self.assertLess(1, len(table_results))

    def test_google_sheets_overwrite(self):
        """
            This test requires access to a google service account secrets json file.  This file won't be checked
            into source control so the test can be disabled.
        """
        if os.path.isfile(self.creds_file):
            spreadsheet_id = "18VF0mB6usVkorgCULDqd4Ib9UrWf8MnaqYxrtnYcsvo"
            # TODO Note as is currently written this sheet must exist before it is executed.
            sheet = "PythonDestination"  # A1 notation
            spreadsheet = googlesheets.GoogleSheet(self.creds_file)
            results = spreadsheet.put_data(spreadsheet_id, sheet, self.data)
            print(results)
            self.assertEqual(4, results['updates']['updatedRows'])

    def test_google_sheets_append(self):
        """
            This test requires access to a google service account secrets json file.  This file won't be checked
            into source control so the test can be disabled.
        """        

        if os.path.isfile(self.creds_file):
            spreadsheet_id = "18VF0mB6usVkorgCULDqd4Ib9UrWf8MnaqYxrtnYcsvo"
            # TODO Note as is currently written this sheet must exist before it is executed.
            sheet = "PythonDestination"  # A1 notation
            spreadsheet = googlesheets.GoogleSheet(self.creds_file)
            results = spreadsheet.put_data(spreadsheet_id, sheet, self.data, overwrite_or_append="APPEND")
            print(results)
            self.assertEqual(4, results['updates']['updatedRows'])

    def test_load_csv(self):
        json_config = utils.load_json("resources/csv_to_csv_config_defaults.json")
        etl = pandas_etl.PandasEtl(json_config)
        df = etl.from_source()
        row_count = df.shape[0]
        self.assertEqual(3, row_count)

    def test_load_parquet(self):
        json_config = utils.load_json("resources/parquet_to_csv_config_defaults.json")
        etl = pandas_etl.PandasEtl(json_config)
        df = etl.from_source()
        row_count = df.shape[0]
        self.assertEqual(1000, row_count)
            
    def test_load_json(self):
        json = utils.load_json("resources/memory_to_sqllitedb_config.json")
        print(json)
        self.assertIsNotNone(json.get("source"))
        self.assertEqual("memory", json["source"]["type"])

    def test_load_json_provide_var_subst(self):
        json = utils.load_json("resources/mysqldb_to_sqllitedb_osvar_subst_config.json",
                               var_subst_func=self._var_subst)
        print('after var substitution')
        print(json)
        self.assertEqual("jsonfile", json["new"])
        
    def test_PandasEtl_debug(self):
        etl = pandas_etl.PandasEtl({})
        self.assertEqual(False, etl._is_debug())

        etl = pandas_etl.PandasEtl({"debug": False})
        self.assertEqual(False, etl._is_debug())
        
        etl = pandas_etl.PandasEtl({"debug": True})
        self.assertEqual(True, etl._is_debug())
        
    # note this method could also be a function and work in the test
    def _var_subst(self, json_str):
        print("in _var_subst. original json")
        print(json_str)
        return '{"new": "jsonfile"}'
