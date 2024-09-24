# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10, 2022

@author: SatishVenkataraman
"""

import re
import logging

def validate_nsn(nsn_input):
    original_nsn = nsn_input  # Keep a copy of the original input for logging
    
    # Trim any leading or trailing spaces
    nsn_input = nsn_input.strip()
    
    # Check for leading/trailing spaces
    if original_nsn != nsn_input:
        logging.warning(f"NSN input had leading or trailing spaces: '{{original_nsn}}'")
    
    # Remove any non-digit characters (spaces, special characters, etc.)
    cleaned_nsn = re.sub(r'\D', '', nsn_input)
    
    # Log if non-digit characters were removed
    if len(cleaned_nsn) != len(nsn_input):
        logging.warning(f"NSN input contained non-digit characters: '{{original_nsn}}'")

    # Check if the cleaned NSN has exactly 13 digits
    if len(cleaned_nsn) == 13:
        logging.info(f"Valid NSN after cleaning: '{{cleaned_nsn}}'")
        return cleaned_nsn
    else:
        # Log specific cases of too many or too few digits
        if len(cleaned_nsn) > 13:
            raise ValueError(f"Invalid NSN: '{{original_nsn}}'. NSN contains too many digits after cleaning. Should be 13 digits.")
        elif len(cleaned_nsn) < 13:
            raise ValueError(f"Invalid NSN: '{{original_nsn}}'. NSN contains too few digits after cleaning. Should be 13 digits.")
import sys
from pandas import DataFrame
from d2d_pandas_etl import pandas_etl as pe
from d2d_pandas_etl import utils as ut
import sqlite3

from commonUtils import extn_utils as eut
from commonUtils import myConstants as mc
from commonUtils import send_emails as em
import os
import sqlalchemy as sa
import logging
from datetime import datetime
import math
import time
from googleapiclient.errors import HttpError
#import gspread as gs

#The following code is to ensure there is no read timeout on the google sheets.
# Try increasing it if you have issues.
import socket
socket.setdefaulttimeout(5*60000) #60000 milli seconds = 60 secs = 1 min

DATE_FORMAT = "%m/%d/%Y"
FGCOLOR=mc.FGCOLOR
RESET=mc.RESET


class FileCountMismatchError(Exception):
    pass


def execute_with_retry_etl(etl_object, fix_data, max_retries=5, sleep_factor=4):
    """
    Execute the etl.run method with retry logic.

    Parameters:
    - etl_object: An instance of YourETLObject.
    - fix_data: The data to be passed to etl.run.
    - max_retries: Maximum number of retries.
    - sleep_factor: Factor to multiply the sleep duration between retries.

    Returns:
    The result of the etl.run method if successful.
    """
    for retry in range(max_retries):
        try:
            logging.info(f"Attempt {retry + 1} begun")
            result = etl_object.run(fix_data)
            logging.info(f"Attempt {retry + 1} completed successfully")
            return result
        except HttpError as e:  # Replace with the actual exception type raised by etl.run
            if e.resp.status == 504 or e.resp.status == 503:
                # Service unavailable, retry after a delay
                wait_time = sleep_factor ** retry
                print(f"Received {e.resp.status} error. Attempt# {retry + 1} of {max_retries}.  Retrying in {wait_time} seconds...Details {e}")
                time.sleep(wait_time)
            elif e.resp.status == 500:
                wait_time = sleep_factor ** retry
                print(f"Received 500 error. Attempt# {retry + 1} of {max_retries}.  Retrying in {wait_time} seconds...Details {e}")
                time.sleep(wait_time)
            else:
                # Re-raise the exception if it's not 504, 503, or 500
                logging.info(f"In the Else Block of the retry area. Attempt {retry + 1} Error status code:  {e.resp.status}")
                raise
        except Exception as e:
            logging.info(f"Inside Exception catch")
            logging.info(f"exception logged. Retry: {retry + 1}, max_retries: {max_retries}")
            if retry == max_retries - 1:
                raise Exception("500 Error - Retry attempts failed.") # If this is the last retry attempt, re-raise the exception
            else:
                logging.info(f"Exception logged - LAST ATTEMPT. Retry: {retry}, max_retries: {max_retries}")
                # All retries failed, raise the last encountered exception
                raise Exception("All retry attempts failed.")
        except:
                logging.info(f"Final Exception Block");
				
 

# This function counts the number of files in a folder
def countFilesInFolder(folder_path):
    try:
        # Get the list of files in the specified folder
        files = os.listdir(folder_path)
        # Count the number of files
        file_count = len(files)
        return file_count
    except OSError as e:
        print(f"Error accessing folder: {e}")
        return None


#  This function is used to clean up the data
#  Clean tabs from every column if they exist
#  Ensure columns that are supposed to be strings do not have integers etc
#  Remove trailing spaces
#  Remove special characters
def fixData(dataframe: DataFrame) -> DataFrame:
    global processingStatus
    try:
     #dataframe.str.replace(to_replace ='Stanley', value = 'Steve', 
     #regex = True) #did not work
     #dataframe['MFRNAME'] = dataframe['MFRNAME'].replace(
     #to_replace ='Stanley', value = 'Steve', regex = True
     #)
     #dataframe['new_col'] = "hello" # Adds a new column
     #dataframe.dropna(subset=['UNITPRICE', 'MFRNAME', 'MFRPN', 'UOI'],
     #inplace=True)
     #print(stringColumns)
     #itemType = os.getenv("ItemType")
     #logging.info(f"String Columns = {stringColumns}")
     #logging.info(f"dropRowsWhenColumnsEmpty = {dropRowsWhenColumnsEmpty}")
        if dropRowsWhenColumnsEmpty is not None:
            eut.dropEmptyRowsforColumns(dataframe, dropRowsWhenColumnsEmpty)
        if len(dataframe.index) > 0:
            if stringColumns is not None:
                for x in stringColumns:
                    #Make specified columns as strings
                    dataframe[x] = dataframe[x].apply(lambda _: str(_))
                    #df_obj = dataframe.select_dtypes(['object'])
                    #dataframe[df_obj.columns] = df_obj.apply(
                    #lambda x: x.str.strip()
                    #)
                    #Ltrim and rtrim the string columns
                    dataframe[x] = dataframe[x].str.strip()
            #Make specified columns as integers
            if integerColumns is not None:
                for x in integerColumns:
                    dataframe[x] = dataframe[x].apply(
                        lambda _: int(_)
                        if not (isinstance(_, str) or math.isnan(_)) else _
                        )
             #Round specified columns to 2 decimals
            if decimalColumns is not None:
                for x in decimalColumns:
                    dataframe[x] = dataframe[x].apply(
                        lambda _: round(_,2)
                        if not (isinstance(_, str) or math.isnan(_)) else _
                        )
    #        dataframe['UNITPRICE'] = dataframe['UNITPRICE'].round(2)
            if droptab:
                #replacing tabs in every column
                dataframe = dataframe.replace('\t',' ',regex=True)
        if categoryName == "quotes":
            #add the itemType to the data frame
            dataframe["ITEMTYPE"] = itemType
        return dataframe
    except Exception as e:
        processingStatus = "Failed while cleaning data"
        logging.exception(f"Could not clean data.  Try again... :${e}")
        sendEmail("Instaquote Error", f"Python Error encountered when cleaning data. Error:${e}.")


# This method is used to clean up any data from the sql server staging table
# prior to being pushed to the staging google sheet
def fixStagingData(dataframe: DataFrame) -> DataFrame:
    timestampStr = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
    dataframe.fillna('', inplace=True)
    dataframe['LASTUPDATED']=""
    dataframe.at[0,'LASTUPDATED'] = timestampStr
    return dataframe


def fixExcludedData(dataframe: DataFrame) -> DataFrame:
    timestampStr = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
    dataframe.fillna('', inplace=True)
    dataframe['LASTUPDATED']=""
    dataframe.at[0,'LASTUPDATED'] = timestampStr
    return dataframe


def updateLogTable(entryType, entryMessage):
    engine = sa.create_engine(url)
    connection = engine.raw_connection()
    try:
        cursor_obj = connection.cursor()
        cursor_obj.execute(
            spLogName+" ?,?", entryType, entryMessage
            )
        cursor_obj.close()
        connection.commit()
    except Exception:
        logging.ERROR("{FGCOLOR}Error Updating the SQL Log !!!{Style.RESET_ALL}")
        processingStatus = "Failed when updating SQL Log"
        sendEmail("Instaquote Error", "Error encountered when updating the SQL Log.")
    finally:
        connection.close()
        logging.info("SQL Log Updated")


def sendEmail(msgSubject, msgContent):
    global toList, ccList, subject
    toList = 'instaquote.support.team@gsa.gov'
    ccList = 'curtis.gathright@gsa.gov'
    subject = msgSubject
    content = msgContent
    try:
        service = em.gmail_authenticate()
        #send_message(service, args.destination, args.Cc, args.subject, args.body, arg.files)
        em.send_message(service, toList, ccList, subject, content)
    except Exception as e:
        logging.exception(f"Sending Email Error: {e}")
        updateLogTable("Python Error",f"Email Generation Error: {e}")
        
    
class QuoteProcessingOrchestrator:
    def __init__(self, processType, configFromUI):
        self.configFromUI = configFromUI
        self.processType = processType

    def setup(self):
        global is_conversion_needed, is_copy_needed, is_sp_needed
        global is_retrieveProcessedData_needed, is_clean_needed, json_gs_nsn
        global json_gs_pn, json_xl_nsn, json_xl_pn, src_dir_quotes
        global src_dir_mappings, dest_dir_quotes, dest_dir_mappings, file_ext
        global json_data_to_gs, json_excluded_data_to_gs, json_mapping, processingStatus
        global processing_scope, gaca_config, query, spname, spLogName, url
        processingStatus = 'OK'
        if self.processType == "LoadAndProcessQuotes":
            gaca_config = ut.load_json(
                "resources/extn/test_LoadAndProcess_GACA_googlesheet_to_tsv_config.json"
            )
            moduleList = gaca_config['onlyErrorLogging'].get('moduleList')
            url = gaca_config['spexec']['url']
            spLogName = gaca_config['spexec']['spLogName']
            # path to destination directory
            dest_dir_quotes = gaca_config['copy'].get('dest_dir_quotes')
            # path to destination directory
            dest_dir_mappings = gaca_config['copy'].get('dest_dir_mappings')
            updateLogTable("Python Started","Python Process Begun")
            if moduleList != "":
                moduleList = moduleList.split(",")
                eut.setLoggingLevelForModules(moduleList,logging.ERROR)
                #Getting and storing some values from the GACA Config
                if self.configFromUI is not None:
                    is_conversion_needed = self.configFromUI.get("isConversionNeeded")
                    is_copy_needed = self.configFromUI.get("isCopyNeeded")
                    is_sp_needed = self.configFromUI.get("isRunStoredProcNeeded")
                    is_retrieveProcessedData_needed = self.configFromUI.get("isUploadDBTOGSNeeded")
                    processing_scope = self.configFromUI.get('scope_complete_partial').upper()
                else:
                    is_conversion_needed = gaca_config['conversion'].get('isneeded')
                    is_copy_needed = gaca_config['copy'].get('isneeded')
                    is_sp_needed = gaca_config['spexec'].get('isneeded')
                    is_retrieveProcessedData_needed = gaca_config['retrieveProcessedData'].get('isneeded')
                    processing_scope = gaca_config['processing'].get('scope_complete_partial').upper()
                    # path to output directory which will need to be cleaned
                    # out for a complete.
                src_dir_quotes = gaca_config['copy'].get('src_dir_quotes')
                if is_conversion_needed:
                    json_gs_nsn = gaca_config['conversion'].get('json_gs_nsn')
                    json_gs_pn = gaca_config['conversion'].get('json_gs_pn')
                    json_xl_nsn = gaca_config['conversion'].get('json_xl_nsn')
                    json_xl_pn = gaca_config['conversion'].get('json_xl_pn')
                    json_mapping = gaca_config['conversion'].get('json_mapping')
                is_clean_needed = gaca_config['clean'].get('isneeded')
                json_data_to_gs = gaca_config['retrieveProcessedData'].get('json_data_to_gs')
                json_excluded_data_to_gs = gaca_config['retrieveProcessedData'].get('json_excluded_data_to_gs')
                #processing_type = gaca_config['spexec'].get('processing_type')
                if is_copy_needed:
                    # file extensions
                    file_ext = gaca_config['copy'].get('file_ext')
                    # path to source directory
                    src_dir_quotes = gaca_config['copy'].get('src_dir_quotes')
                    # path to source directory
                    src_dir_mappings = gaca_config['copy'].get('src_dir_mappings')

                #checking if the Z:drive can be found.
                #This is needed for copying files
                    try:
                        logging.info(f"{FGCOLOR}Cleaning up the destination folder {dest_dir_quotes} and {dest_dir_mappings}...{RESET}")
                        eut.deleteFolderContents(dest_dir_quotes)
                        eut.deleteFolderContents(dest_dir_mappings)
                    except FileNotFoundError:
                        logging.ERROR(f"{FGCOLOR}Failed Connecting to Z: drive.  Pls get on VPN and connect to Z: !!!{RESET}")
                        sendEmail("Instaquote Error", "Python Error connecting to Z: Drive.")
                if is_sp_needed:
                    spname = gaca_config['spexec']['spname']
            elif self.processType == "UploadStandingOrders":
                gaca_config = ut.load_json("resources/extn/test_Upload_sqlserver_to_googlesheet_config.json")
                query = gaca_config['source'].get('query')

    #Read the google sheet Instaquote Processor List Test enviorment
    #(https://docs.google.com/spreadsheets/d/1dA7p-SeQI0o8Q5GqAqxbIwwZqoOuhugV18JykXDXmAY/edit?gid=0#gid=0)
    #iterate through it line by line
    #get values in the sheet as variables
    #pass those variables to the config file to
    #read relevant googlesheets and save as tsv
    def loadAndProcessQuotes(self):
        global dropRowsWhenColumnsEmpty, itemType, stringColumns
        global decimalColumns, integerColumns, droptab, processingStatus
        global categoryName, spname, url, expectedFileCount, actualFileCount
        actualFileCount = 0
        expectedFileCount = 0
        #processingStatus = "OK"
        updateLogTable("Python Informational", "Python loadAndProcessQuotes Begun")
        # This is use when the processing_scope is partial alone.
        file_list = []
        if processingStatus == "OK":
            if is_conversion_needed:
                if processing_scope == "COMPLETE":
                    #Deleting contents from the Source directory - Output
                    eut.deleteFolderContents(src_dir_quotes)
                logging.info(f"{FGCOLOR}Getting data from the GACA Config Google Sheet  ...{RESET}")
                ##Added
                gaca_config["source"]["sheet"] = gaca_config['source'].get('sheet_list')
                lookUpValue = pe.PandasEtl(gaca_config).from_source()
                gaca_config["source"]["sheet"] = lookUpValue.columns[0]
                ##Until here
                etl = pe.PandasEtl(gaca_config)
                #iterate through each row in GACA sheet and process it
                df = etl.from_source() #Get the data from the GACA Spreadsheet
                #Iterate through each row of the GACA Sheet
                for index, row in df.iterrows():
                    if row['Process'] == 'Yes':
                        expectedFileCount = expectedFileCount + 1
                        vendor = row['Vendor']
                        sourceType = row['SourceType']
                        itemType = row['ItemType']
                        sheetName = row['SheetName']
                        os.environ["Vendor"] = vendor
                        os.environ["SourceType"] = sourceType
                        #os.environ["ItemType"] = itemType
                        os.environ["SheetName"] = sheetName
                        if sourceType == "googlesheets":
                            googlesheetID = row['GoogleSheetID']
                            os.environ["GoogleSheetID"]=googlesheetID
                            if itemType == "PN":
                                configjson = json_gs_pn
                            elif itemType == "NSN":
        try:
            nsn_input = row['NSN']  # Get NSN from the row
            valid_nsn = validate_nsn(nsn_input)  # Validate the NSN
            row['NSN'] = valid_nsn  # Replace the NSN in the row with the validated one

        except ValueError as e:
            logging.error(f"NSN validation error at row {index}: {e}")
            sendEmail("NSN Validation Error", f"Error at row {index}: {e}")
            continue  # Skip the row if NSN is invalid
                                configjson = json_gs_nsn
                        elif sourceType == "excel":
                            filePath = row['FilePath']
                            os.environ["FilePath"] = filePath
            #                print(filePath)
                            if itemType == "PN":
                                configjson = json_xl_pn
                            elif itemType == "NSN":
        try:
            nsn_input = row['NSN']  # Get NSN from the row
            valid_nsn = validate_nsn(nsn_input)  # Validate the NSN
            row['NSN'] = valid_nsn  # Replace the NSN in the row with the validated one

        except ValueError as e:
            logging.error(f"NSN validation error at row {index}: {e}")
            sendEmail("NSN Validation Error", f"Error at row {index}: {e}")
            continue  # Skip the row if NSN is invalid
                                configjson = json_xl_nsn
                        #Loads the config and substitute variables with values
                        config = ut.load_json(configjson)
                        if config is not None:
                            try:
                                etl = pe.PandasEtl(config)
                                if is_clean_needed:
                                    logging.info(f"{FGCOLOR}Cleaning the data for {vendor}_{itemType}...{RESET}")
                                    categoryName = config['category'].get('name')
                                    stringColumns = config['cleandetails'].get('stringColumns')
                                    decimalColumns = config['cleandetails'].get('decimalColumns')
                                    integerColumns = config['cleandetails'].get('integerColumns')
                                    dropRowsWhenColumnsEmpty = config['cleandetails'].get('dropRowsWhenColumnsEmpty')
                                    droptab = config['cleandetails'].get('droptab')
                                    #Get the data from the googlesheet,
                                    #Do clean up of the data before calling
                                    #the conversion to the destination format
                                    #etl.run(fixData)
                                    result = execute_with_retry_etl(etl, fixData,6 ,4);
                                    logging.info(result)

                                else:
                                    etl.run()
                                logging.info(f"{FGCOLOR}Completed creating the file for {vendor}_{itemType}...{RESET}")
                                #if processing is partial, keep track
                                #of the files that were generated
                                if processing_scope != "COMPLETE":
                                    fileName = config['destination'].get('file')
                                    fileName = fileName.rsplit('/', 1)[-1]
                                    file_list.append(fileName)
                            except Exception as e:
                                logging.exception(f"QuoteProcessingOrchestrator exception :${e}")
                                processingStatus = "Failed at Processing"
                                updateLogTable("Python Error",f"QuoteProcessingOrchestrator Processing exception index: {index} Error :{e}")
                                sendEmail("Instaquote Error", f"Python Error encountered during Processing at record {index}. Error: {e}.")
        # Once all the tsv files have been generated,
        # copy them to the SQL Server
            if processingStatus == "OK":
                if is_copy_needed:
                #checking if the Z:drive can be found.
                #This is needed for copying files
                    try:
                        #                    ld = os.listdir(dest_dir)
                        #                    print (ld)
                        #                        logging.info(f"{FGCOLOR}Cleaning up the
                        #                        destination folder {dest_dir} ...{RESET}"
                        #                        )
                        #                        eut.deleteFolderContents(dest_dir)
                        dest_dir = dest_dir_quotes
                        src_dir = src_dir_quotes
                        logging.info(f"{FGCOLOR}Copying files from {src_dir} to {dest_dir} ...{RESET}")
                        if processing_scope == "COMPLETE":
                            eut.copyFromSourceFolderToDestFolder(src_dir,dest_dir,file_ext)
                        else:
                            eut.copySelectedFilesFromSourceFolderToDestFolder(src_dir,dest_dir,file_list,file_ext)

                        # Count files in location and verify that it matches the number of files expected

                        actualFileCount = countFilesInFolder(dest_dir)
                        logging.info(f"{FGCOLOR}Checking File count. There should be {expectedFileCount} files")
                        if actualFileCount is not None:
                            if actualFileCount == expectedFileCount:
                                logging.info(f"{FGCOLOR}The number of files in the folder is as expected: {actualFileCount}")
                            else:
                                raise FileCountMismatchError(
                                    f"Number of files mismatch! Expected: {expectedFileCount}, Actual: {actualFileCount}")

                        else:
                            raise FileCountMismatchError("Unable to determine the number of files due to an error.")

                    except FileCountMismatchError as e:
                        print(f"File count verification failed: {e}")
                        logging.exception(f"QuoteProcessingOrchestrator exception :${e}")
                        processingStatus = "File Mismatch"
                        updateLogTable("Python Error",
                                       f"QuoteProcessingOrchestrator File Mismatch Processing exception index: {index} Error :{e}")
                        sendEmail("Instaquote Error",
                                  f"Python Error File Mismatch encountered during Processing at record {index}. Error: {e}.")
                        # Handle the exception as needed
                    except FileNotFoundError:
                        logging.ERROR(f"{FGCOLOR}Failed Connecting to Z: drive.  Pls get on VPN and connect to Z: !!!{RESET}")
                        processingStatus = "Failed at Copying"
                        updateLogTable("Python Error","Failed Connecting to Z: Drive")
                        sendEmail("Instaquote Error", "Python Error encountered when connecting to Z:")    
    #        print (processingStatus)
            #Call the Stored Procedure on the SQL Server
            if processingStatus == "OK":
                if is_sp_needed:
                    dest_dir = dest_dir_quotes
                    ld = os.listdir(dest_dir)
        #            print(ld)
    #                    spname = gaca_config['spexec']['spname']
                    logging.info(f"{FGCOLOR}Now calling the stored procedure {spname} for matching quotes against standing orders within the SQL Server ...{RESET}")
                    engine = sa.create_engine(url)
                    params = ",".join(ld)
    #                logging.info(f"{params}")
                    print(f"{params}")
                    results = ""
                    resultString = ""
                    connection = engine.raw_connection()
                    try:
                        cursor_obj = connection.cursor()
                        cursor_obj.execute(
                            spname+" ?,?,?", params, processing_scope,results
                            )
                        row = cursor_obj.fetchone()
                        resultString = row[0]
                        cursor_obj.close()
                        connection.commit()
                    except Exception:
                        logging.ERROR("{FGCOLOR}Running the stored procedure failed !!!{Style.RESET_ALL}")
                        processingStatus = "Failed when running Stored Procedure"
                        updateLogTable("Python Error", "Failed Running Stored Procedure via Python")
                        sendEmail("Instaquote Error", "Python Error thrown when running the Stored Procedure. See SQL Logs.")
                    finally:
                        connection.close()
                        logging.info(f"{FGCOLOR}{resultString}{RESET}")
            #Get the contents from the staging table and put it on the staging
            #tab within the RPA BOT Google Sheet
            if processingStatus == "OK":
                if is_retrieveProcessedData_needed:
                    try:
                        configStagingDataToGS = ut.load_json(json_data_to_gs)
                        etl_stagingData = pe.PandasEtl(configStagingDataToGS)
                        #spreadsheet_id = etl_stagingData.config['destination']['spreadsheet_id']
                        etl_stagingData.run(fixStagingData)
                        logging.info(f"{FGCOLOR}Completed moving data to the Staging tab for RPA BOT ...{RESET}")
                    except Exception:
                        logging.ERROR(f"{FGCOLOR}Issue when copying staging data to the Staging Google Sheet{RESET}")
                        processingStatus = "Failed when copying Staging Data to Staging Google Sheet"
                        updateLogTable("Python Error","Failed Copying Staging data to the Staging Google Sheet")
                        sendEmail("Instaquote Error", "Python Error encountered when updating the Staging Google Sheet.")
            # Get the contents for EXCLUDED Quotes table and put it on the appropriate staging tab
            # tab within the RPA BOT Google Sheet
            if processingStatus == "OK":
                if is_retrieveProcessedData_needed:
                    try:
                        configExcludedDataToGS = ut.load_json(json_excluded_data_to_gs)
                        etl_excludedData = pe.PandasEtl(configExcludedDataToGS)
                        etl_excludedData.run(fixExcludedData)
                        logging.info(
                            f"{FGCOLOR}Completed moving data to the Excluded Quotes tab for RPA BOT ...{RESET}")
                    except Exception:
                        logging.ERROR(
                            f"{FGCOLOR}Issue when copying staging data to the Excluded Quotes tab of Google Sheet{RESET}")
                        processingStatus = "Failed when copying Staging Data to Excluded Quotes tab of Google Sheet"
                        updateLogTable("Python Error",
                                       "Failed Copying Staging data to the Excluded Quotes tab of Google Sheet")
                        sendEmail("Instaquote Error",
                                  "Python Error encountered when updating the Excluded Quotes tab of Google Sheet.")

    def loadMappingData(self):
        print("Starting to Load Mapping Data")
        global dropRowsWhenColumnsEmpty, integerColumns, stringColumns
        global decimalColumns, droptab, categoryName
        gaca_config["source"]["sheet"] = gaca_config['source'].get('sheet_mappingList')
        lookUpValue = pe.PandasEtl(gaca_config).from_source()
        gaca_config["source"]["sheet"] = lookUpValue.columns[0]
        ##Until here
        etl = pe.PandasEtl(gaca_config)
        #        configjson = json_mapping
        # Get the data from the GACA Spreadsheet
        df = etl.from_source()
        # Iterate through each row of the GACA Sheet
        for index, row in df.iterrows():
            if row['Process'] == 'Yes':
                name = row['Name']
                googlesheetID = row['GoogleSheetID']
                sheetName = row['SheetName']
                sourceType = row['SourceType']
                json = row['json']
                os.environ['Name'] = name
                os.environ["GoogleSheetID"]=googlesheetID
                os.environ["SheetName"] = sheetName
                os.environ["SourceType"] = sourceType
                os.environ["json"] = json
                #Loads the config json and substitute variables with values
                config = ut.load_json(json)
                if config is not None:
                    try:
                        etl = pe.PandasEtl(config)
                        categoryName = config['category'].get('name')
                        stringColumns = config['cleandetails'].get('stringColumns')
                        decimalColumns = config['cleandetails'].get('decimalColumns')
                        integerColumns = config['cleandetails'].get('integerColumns')
                        dropRowsWhenColumnsEmpty = config['cleandetails'].get('dropRowsWhenColumnsEmpty')
                        #Get the data from the googlesheet,
                        #Do clean up of the data before calling the conversion
                        #to the destination format
                        droptab = config['cleandetails'].get('droptab')
                        etl.run(fixData)
                    except Exception:
                        print("Errored out Loading Mapping Data")
                        updateLogTable("Python Error", "Error loading Mapping Data")
                        sendEmail("Instaquote Error", "Python Error encountered when loading Mapping Data.")
        try:
            dest_dir = dest_dir_mappings
            src_dir = src_dir_mappings
            logging.info(f"{FGCOLOR}Copying files from {src_dir} to {dest_dir} ...{RESET}")
            eut.copyFromSourceFolderToDestFolder(src_dir,dest_dir,file_ext)
        except FileNotFoundError:
            logging.ERROR(f"{FGCOLOR}Failed Connecting to Z: drive.  Pls get on VPN and connect to Z: !!!{RESET}")
            updateLogTable("Python Error","Error Connecting to Z: Drive")
            sendEmail("Instaquote Error", "Python Error encountered connecting to Z: Drive.")


'''
def uploadStandingOrders():
    global processingStatus
    processingStatus = "OK"
    #Get data from database (WIP)
#    MYTABLE = "mytable"
#    data = self.data.copy()
#    header = data.pop(0)
    # defaults to convert any string cells to dates if they can be converted
#    df = utils.to_pandas(data, header)
    engine = sa.create_engine(gaca_config['source']['url'])
    print(query)
    table_results = engine.execute(f"{query}").fetchall()
    print (table_results)
#    self.assertEqual(3, len(table_results))
    #iterate through each of the google sheets
    #populate the google sheets
'''
#This is the starting of the program
if __name__ == '__main__':
    # Example: python MT_QuoteProcessingOrchestrator.py LoadAndProcess
    # Example: python MT_QuoteProcessingOrchestrator.py LoadStandingOrders
    if len(sys.argv) >= 2:
        try:
            action = sys.argv[1]
            etl = QuoteProcessingOrchestrator(action, None)
            etl.setup()
            if action == "LoadAndProcessQuotes":
#                etl.processingStatus = "OK" # Start off on the right foot
                etl.loadMappingData()
                etl.loadAndProcessQuotes()
        except Exception as e:
            processingStatus = "failed"
            logging.exception(f"QuoteProcessingOrchestrator exception :{e}")
            updateLogTable("Python Error",f"QuoteProcessingOrchestrator exception :{e}")
            sendEmail("Instaquote Error", f"Python Error encountered within QuoteProcessingOrchestrator. Error:${e}")
        finally:
            if processingStatus != "OK":
                logging.error("***********")
                logging.error(f"{FGCOLOR}{processingStatus}{RESET}")
                logging.error("***********")
            updateLogTable("Python Completed",f"FinalStatus:${processingStatus}")
            sendEmail("Instaquote Python Ingest Processing Completed", f"IQ Python Ingest Processing Completed. FinalStatus:{processingStatus}")
    else:
        print("Pass an argument.")
        print("Example: python QuoteProcessingOrchestrator.py LoadAndProcessQuotes (or) python QuoteProcessingOrchestrator.py LoadStandingOrders")
    sys.exit()
