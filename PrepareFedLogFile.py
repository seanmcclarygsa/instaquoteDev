# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 09:36:18 2023

@author: CurtisMGathright
"""

# This file updates the FEDLOG FLIS file pulled from 
# specifications created in the FEDLOG_FLIS_TO_IQ.TDE file
# in the resources/FEDLOG folder. 
# The file created should be placed in the location described 
# in the Convert_FedLogFile_and_UploadToSQLServer.json config 
# file.
# 
# The file is converted from FLIS format to IQ format locally
# and  uploaded to the designated output folder.
# Until another process/source is found, this process 
# should be run manually monthly upon update of FLIS data.
# See related doc files in Resources/FEDLOG Folder.

import sys
from commonUtils import myConstants as mc
import logging
from d2d_pandas_etl import utils as ut
from commonUtils import extn_utils as eut
import csv


#The following code is to ensure there is no read timeout on the google sheets.
# Try increasing it if you have issues.
import socket
socket.setdefaulttimeout(5*60000) #60000 milli seconds = 60 secs = 1 min

DATE_FORMAT = "%m/%d/%Y"
FGCOLOR=mc.FGCOLOR
RESET=mc.RESET

 #Convert the file provided by FedLog to what IQ expects
 #Concatenate the FSC and NIIN
 #Remove any items with an RNVC of 9
 #Remove RNCC and RNVC
 #Strip quotes from the file
 #Rename headers

class PrepareFedLogFile:
    def __init__(self, inputFileName):
        self.inputFileName = inputFileName
        
    def setup(self):
        global is_conversion_needed, is_copy_needed
        global src_dir_mappings, dest_dir_mappings, src_file, dest_file, file_ext
        global gaca_config
        
        gaca_config = ut.load_json(
            "resources/extn/Convert_FedLogFile_and_UploadToSQLServer.json"
            )
        moduleList = gaca_config['onlyErrorLogging'].get('moduleList')
        if moduleList != "":
            moduleList = moduleList.split(",")
            eut.setLoggingLevelForModules(moduleList,logging.ERROR)
            is_copy_needed = gaca_config['copy'].get('isneeded')
            if(is_copy_needed):
                #set the input (original) file
                orig_dir_mappings = gaca_config['copy'].get('orig_dir_mappings')
                orig_file = orig_dir_mappings + self.inputFileName
                # path to source directory for converted file
                src_dir_mappings = gaca_config['copy'].get('src_dir_mappings')
                src_file = src_dir_mappings + gaca_config['copy'].get('src_file_name')
                #path to destination directory for converted file
                dest_dir_mappings = gaca_config['copy'].get('dest_dir_mappings')   
                dest_file = dest_dir_mappings + gaca_config['copy'].get('output_file_name')
                file_ext = gaca_config['copy'].get('file_ext')
                self.origFileName = orig_file
                self.copyFileName = src_file
                self.outputFileName = dest_file
                #This is needed for copying files
                try:
                    logging.info(f"{FGCOLOR}Cleaning up the destination file {dest_file}...{RESET}")
                    eut.rmfiles(dest_file)
                except FileNotFoundError:
                    logging.ERROR(f"{FGCOLOR}Failed Connecting to Z: drive.  Pls get on VPN and connect to Z: !!!{RESET}")
    def prepareAndCopyFile(self):    
        global src_dir, dest_dir
        with open(self.origFileName, 'r') as file:
            reader = csv.reader(file)
            rows = list(reader)

        # Remove rows with "9" in the fourth column
        rows = [row for row in rows if row[3] != "9"]

        # Concatenate first two columns and remove third and fourth columns
        rows = [[row[0] + row[1], row[4], row[5]] for row in rows]

        # Remove quotes surrounding values
        rows = [[value.strip('"') for value in row] for row in rows]

        # Rename header row values
        rows[0] = ["NSN", "CAGE_CODE", "PART_NUMBER"]

        with open(self.copyFileName, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
        
        #copy file to Z: Location
        #checking if the Z:drive can be found.
        try:
            #ld = os.listdir(dest_dir)
            #print (ld)
            #logging.info(f"{FGCOLOR}Cleaning up the
            #destination folder {dest_dir} ...{RESET}"
            #)
            #eut.deleteFolderContents(dest_dir)
            dest_dir = dest_dir_mappings
            src_dir = src_dir_mappings
        
            logging.info(f"{FGCOLOR}Copying files from {src_dir} to {dest_dir} ...{RESET}")
            if is_copy_needed:
                eut.copyFromSourceFolderToDestFolder(src_dir,dest_dir,file_ext)
        except FileNotFoundError:
            logging.ERROR(f"{FGCOLOR}Failed Connecting to Z: drive.  Pls get on VPN and connect to Z: !!!{RESET}")

if __name__ == '__main__':
    # Example usage
    global input_file, output_file
    # Example: python PrepareFedLogFile.py inputFileName.csv
    if len(sys.argv) >= 2:
        try:
            input_file = sys.argv[1]
            etl = PrepareFedLogFile(input_file)
            etl.setup()
            etl.prepareAndCopyFile()
            logging.info("Completed Process")
        except Exception as e:
            logging.exception(f"PrepareFedLogFile exception :${e}")
    else:
        print("Pass an argument.")
        print("Example: python PrepareFedLogFile.py inputFileName.csv")
    sys.exit()